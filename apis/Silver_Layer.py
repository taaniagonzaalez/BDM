import pandas as pd
import numpy as np
from minio import Minio
import logging
import io
import uuid  # For generating unique IDs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants for MinIO connection and file paths
MINIO_ENDPOINT = "host.docker.internal:9000"  # adjust your MinIO endpoint
MINIO_ACCESS_KEY = "minio"  # replace with your access key
MINIO_SECRET_KEY = "minio123"  # replace with your secret key
BUCKET_NAME = "bdm-project-upc"
FOURSQUARE_PATH = "raw/batch/foursquare/foursquare_restaurants.parquet"
YELP_PATH = "raw/batch/yelp/yelp_restaurants.parquet"
SILVER_LAYER_PATH = "silver_layer/combined_restaurants.parquet"

def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # Change to True if you use HTTPS
    )

def read_parquet_from_minio(client, bucket_name, object_path):
    try:
        logger.info(f"Reading {object_path} from MinIO bucket {bucket_name}...")
        response = client.get_object(bucket_name, object_path)
        data = response.read()
        response.close()
        response.release_conn()
        
        buffer = io.BytesIO(data)
        df = pd.read_parquet(buffer)
        logger.info(f"Successfully read {object_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to read {object_path} from MinIO: {e}")
        raise

def write_parquet_to_minio(client, df, bucket_name, object_path):
    try:
        logger.info(f"Writing data to {object_path} in MinIO bucket {bucket_name}...")
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        client.put_object(bucket_name, object_path, buffer, length=buffer.getbuffer().nbytes)
        logger.info(f"Successfully wrote data to {object_path}")
    except Exception as e:
        logger.error(f"Failed to write data to {object_path} in MinIO: {e}")
        raise

def main():
    client = get_minio_client()

    # Read datasets
    df_foursquare = read_parquet_from_minio(client, BUCKET_NAME, FOURSQUARE_PATH)
    df_yelp = read_parquet_from_minio(client, BUCKET_NAME, YELP_PATH)

    # Combine datasets
    logger.info("Combining Foursquare and Yelp datasets...")
    combined_df = pd.concat([df_foursquare, df_yelp], ignore_index=True)

    # Convert array or list columns to tuples for drop_duplicates compatibility
    logger.info("Converting array/list columns to tuples for deduplication...")
    for col in combined_df.columns:
        if combined_df[col].apply(lambda x: isinstance(x, (np.ndarray, list))).any():
            combined_df[col] = combined_df[col].apply(lambda x: tuple(x) if isinstance(x, (np.ndarray, list)) else x)

    # Drop duplicates
    logger.info("Dropping duplicates from combined dataset...")
    combined_df.drop_duplicates(inplace=True)

    logger.info(f"Combined dataset shape after deduplication: {combined_df.shape}")

    # Assign unique IDs (UUID4) to each restaurant
    logger.info("Assigning unique IDs to each restaurant...")
    combined_df.insert(0, 'restaurant_id', [str(uuid.uuid4()) for _ in range(len(combined_df))])

    # Ensure 'number' column is string to avoid pyarrow type conversion errors
    if 'number' in combined_df.columns:
        combined_df['number'] = combined_df['number'].astype(str)

    # Write combined DataFrame with IDs back to MinIO
    write_parquet_to_minio(client, combined_df, BUCKET_NAME, SILVER_LAYER_PATH)

if __name__ == "__main__":
    main()
