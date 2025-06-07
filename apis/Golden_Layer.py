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

RESTAURANTS_PATH = "silver_layer/combined_restaurants.parquet"
SEARCHES_PATH = "silver_layer/user_searches.parquet"
REGISTRATION_PATH = "silver_layer/user_registrations.parquet"

OUTPUT_PATH_ALL = "golden_layer/restaurants_kpi.parquet"
OUTPUT_KPI_1 = "golden_layer/kpi_1.parquet"
OUTPUT_KPI_2 = "golden_layer/kpi_2.parquet"
OUTPUT_KPI_3 = "golden_layer/kpi_3.parquet"
OUTPUT_KPI_4 = "golden_layer/kpi_4.parquet"
OUTPUT_KPI_5 = "golden_layer/kpi_5.parquet"

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
    df_restaurant = read_parquet_from_minio(client, RESTAURANTS_PATH)
    df_searches = read_parquet_from_minio(client, SEARCHES_PATH)
    df_registrations = read_parquet_from_minio(client, REGISTRATION_PATH)
    # Align schema
    columns = ["name", "direction", "number", "email", "rating", "comments", "open_hours", "type", "key_words", "source"]


    # Cuisine KPI: explode `type` column
    df_keywords = df_restaurant.dropna(subset=["type"]).copy()
    df_keywords["type"] = df_keywords["type"].str.lower().str.split(",")
    df_keywords = df_keywords.explode("type")
    df_keywords["type"] = df_keywords["type"].str.strip()

    # KPI 1

    kpi_1 = (
        df_keywords.groupby("type")
        .agg(
            restaurant_count=("type", "count"),
            avg_rating=("rating", "mean")
        )
        .reset_index()
        .sort_values("restaurant_count", ascending=False)
    )

    # KPI 2

    kpi_2 = (
        df_keywords.sort_values(['type', 'rating'], ascending=[True, False])
        .groupby('type')
        .head(5)
        .reset_index(drop=True)
    )

    # KPI 3

    kpi_3 = (
        df_keywords.sort_values(['rating'], ascending=[False])
        .head(10)
        .reset_index(drop=True)
    )

    # KPI 4

    kpi_4 = (
        df_searches.groupby(['theme', 'user_id'])
        .agg(
            searches_count = ("search_date", "count")
        )
        .reset_index()
    )

    # KPI 5

    # Convert to datetime
    df_registrations['register_date'] = pd.to_datetime(df_registrations['register_date'])

    # Group by day (you can also use 'W' for week or 'M' for month)
    kpi_5 = df_registrations.groupby(df_registrations['register_date'].dt.date).size().reset_index(name='user_count')

    # Optional: sort by date
    kpi_5 = kpi_5.sort_values(by='register_date')

    # Write all outputs to MinIO
    write_parquet_to_minio(client, kpi_1, OUTPUT_KPI_1)
    write_parquet_to_minio(client, kpi_2, OUTPUT_KPI_2)
    write_parquet_to_minio(client, kpi_3, OUTPUT_KPI_3)
    write_parquet_to_minio(client, kpi_4, OUTPUT_KPI_4)
    write_parquet_to_minio(client, kpi_5, OUTPUT_KPI_5)

    

    print("Golden Layer KPIs saved successfully.")

if __name__ == "__main__":
    main()
