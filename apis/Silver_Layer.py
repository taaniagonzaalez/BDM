import pandas as pd
import numpy as np
from minio import Minio
import logging
import io
import uuid
import json

# ----------------------------
# Configuration
# ----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = "host.docker.internal:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "bdm-project-upc"

FOURSQUARE_PATH = "raw/batch/foursquare/foursquare_restaurants.parquet"
YELP_PATH = "raw/batch/yelp/yelp_restaurants.parquet"
BCN_PATH = "raw/batch/bcn/bcn_restaurants.parquet"

SILVER_LAYER_PATH = "silver_layer/combined_restaurants.parquet"
SILVER_LAYER_PATH1 = "silver_layer/searches.parquet"
SILVER_LAYER_PATH2 = "silver_layer/registrations.parquet"

# ----------------------------
# Helpers
# ----------------------------
def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def read_parquet_from_minio(client, bucket_name, object_path):
    try:
        logger.info(f"Reading {object_path} from MinIO bucket {bucket_name}...")
        response = client.get_object(bucket_name, object_path)
        data = response.read()
        response.close()
        response.release_conn()
        return pd.read_parquet(io.BytesIO(data))
    except Exception as e:
        logger.error(f"Failed to read {object_path}: {e}")
        raise

def write_parquet_to_minio(client, df, bucket_name, object_path):
    try:
        logger.info(f"Writing to {object_path} in MinIO bucket {bucket_name}...")
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        client.put_object(bucket_name, object_path, buffer, length=buffer.getbuffer().nbytes)
        logger.info(f"Successfully wrote to {object_path}")
    except Exception as e:
        logger.error(f"Failed to write {object_path}: {e}")
        raise

def load_json_partitioned_by_date(fs):
    """
    Carga archivos JSON desde una estructura particionada por date=YYYY-MM-DD/
    (solo para SEARCHES)
    """
    files = fs.list_objects('bdm-project-upc', prefix='raw/streaming/event_type=search/date=', recursive=True)

    dfs = []
    for file in files:
        date_part = file.split("date=")[1].split("/")[0]
        with fs.open(file, 'r') as f: #Add commentMore actions
            json_objects = [json.loads(line) for line in f.readlines()]
            df = pd.DataFrame(json_objects)
            df["date"] = date_part
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def load_json_flat_folder(fs):
    """
    Carga todos los archivos JSON directamente de una carpeta sin subcarpetas (para REGISTRATIONS)
    """
    files = fs.list_objects('bdm-project-upc', prefix='raw/streaming/event_type=registration', recursive=True)

    data = []
    for file in files:
        with fs.open(file, 'r') as f:
            data.append(json.load(f))
    return pd.DataFrame(data) if data else pd.DataFrame()

# ----------------------------
# Main Transformation Logic
# ----------------------------
def main():
    client = get_minio_client()

    # Read all datasets
    df_sources = []
    for source, path in {
        "Foursquare": FOURSQUARE_PATH,
        "Yelp": YELP_PATH,
        "Barcelona_API": BCN_PATH
    }.items():
        try:
            df = read_parquet_from_minio(client, BUCKET_NAME, path)
            df_sources.append(df)
        except Exception:
            logger.warning(f"Skipping {source} due to read failure.")

    # Combine datasets
    logger.info("Combining datasets...")
    combined_df = pd.concat(df_sources, ignore_index=True)

    # Convert array/list columns to tuples (to handle deduplication)
    for col in combined_df.columns:
        if combined_df[col].apply(lambda x: isinstance(x, (np.ndarray, list))).any():
            combined_df[col] = combined_df[col].apply(lambda x: tuple(x) if isinstance(x, (np.ndarray, list)) else x)

    # Drop duplicates
    logger.info("Dropping duplicates...")
    combined_df.drop_duplicates(inplace=True)

    # Add UUIDs
    logger.info("Assigning UUIDs to each restaurant...")
    combined_df.insert(0, 'restaurant_id', [str(uuid.uuid4()) for _ in range(len(combined_df))])

    # Fix column types if needed
    if 'number' in combined_df.columns:
        combined_df['number'] = combined_df['number'].astype(str)

    # Save final output
    write_parquet_to_minio(client, combined_df, BUCKET_NAME, SILVER_LAYER_PATH)

    #--------------------------------------------------------------------------------------

    # Cargar SEARCHES desde estructura particionada por fecha
    df_searches = load_json_partitioned_by_date(client)
    df_searches = df_searches.rename(columns={
        "search_date": "timestamp",
        "theme": "search_theme"
    })

    # Cargar REGISTRATIONS desde ruta plana
    df_registrations = load_json_flat_folder(client)
    df_registrations = df_registrations.rename(columns={
        "register_date": "timestamp",
        "name": "user_name"
    })

    # Ver ejemplos
    print("SEARCHES:")
    print(df_searches.head())

    print("\nREGISTRATIONS:")
    print(df_registrations.head())

    # Guardar como silver en Parquet
    write_parquet_to_minio(client, df_searches, BUCKET_NAME, SILVER_LAYER_PATH1)
    write_parquet_to_minio(client, df_registrations, BUCKET_NAME, SILVER_LAYER_PATH2)
    logger.info("Silver layer successfully updated.")

if __name__ == "__main__":
    main()
