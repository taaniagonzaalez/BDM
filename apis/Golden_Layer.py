import pandas as pd
import numpy as np
import logging
import io
from minio import Minio

# ----------------------------
# Configuration
# ----------------------------
MINIO_ENDPOINT = "host.docker.internal:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "bdm-project-upc"

YELP_PATH = "raw/batch/yelp/yelp_restaurants.parquet"
FOURSQUARE_PATH = "raw/batch/foursquare/foursquare_restaurants.parquet"

OUTPUT_PATH_ALL = "golden_layer/restaurants_kpi.parquet"
OUTPUT_PATH_CUISINE = "golden_layer/kpi_cuisine.parquet"
OUTPUT_PATH_SOURCE = "golden_layer/kpi_source_rating.parquet"

# ----------------------------
# MinIO Client Setup
# ----------------------------
def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def read_parquet_from_minio(client, object_path):
    response = client.get_object(BUCKET_NAME, object_path)
    data = response.read()
    response.close()
    return pd.read_parquet(io.BytesIO(data))

def write_parquet_to_minio(client, df, object_path):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    client.put_object(BUCKET_NAME, object_path, buffer, length=buffer.getbuffer().nbytes)

# ----------------------------
# KPI Processing Logic
# ----------------------------
def main():
    client = get_minio_client()

    # Read data
    yelp_df = read_parquet_from_minio(client, YELP_PATH)
    fsq_df = read_parquet_from_minio(client, FOURSQUARE_PATH)

    yelp_df["source"] = "Yelp"
    fsq_df["source"] = "Foursquare"

    # Align schema
    columns = ["name", "direction", "number", "email", "rating", "comments", "open_hours", "type", "key_words", "source"]
    df_all = pd.concat([yelp_df[columns], fsq_df[columns]], ignore_index=True)
    df_all["number"] = df_all["number"].astype(str)

    # Cuisine KPI: explode `type` column
    df_keywords = df_all.dropna(subset=["type"]).copy()
    df_keywords["type"] = df_keywords["type"].str.lower().str.split(",")
    df_keywords = df_keywords.explode("type")
    df_keywords["type"] = df_keywords["type"].str.strip()

    cuisine_kpi = (
        df_keywords.groupby("type")
        .agg(
            restaurant_count=("type", "count"),
            avg_rating=("rating", "mean")
        )
        .reset_index()
        .sort_values("restaurant_count", ascending=False)
    )

    # Rating KPI by source
    rating_kpi = (
        df_all.groupby("source")
        .agg(
            avg_rating=("rating", "mean"),
            total_restaurants=("source", "count")
        )
        .reset_index()
    )

    # Popularity score (simplified as rating for now)
    df_all["popularity_score"] = df_all["rating"]

    # Write all outputs to MinIO
    write_parquet_to_minio(client, df_all, OUTPUT_PATH_ALL)
    write_parquet_to_minio(client, cuisine_kpi, OUTPUT_PATH_CUISINE)
    write_parquet_to_minio(client, rating_kpi, OUTPUT_PATH_SOURCE)
    

    print("Golden Layer KPIs saved successfully (pandas version).")

if __name__ == "__main__":
    main()
