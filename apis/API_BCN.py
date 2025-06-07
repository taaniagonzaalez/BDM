import requests
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import os

# ----------------------------
# Configuration
# ----------------------------
MINIO_ENDPOINT = "host.docker.internal:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "bdm-project-upc"
OBJECT_PATH = "raw/batch/barcelona/barcelona_raw_restaurants.csv"

# ----------------------------
# Download Function
# ----------------------------
def download_bcn_file():
    file_url = "https://opendata-ajuntament.barcelona.cat/data/dataset/b4d2cc2f-67dc-481a-a7cb-1999fd0d5740/resource/bce0486e-370e-4a72-903f-024ba8902ae1/download"
    response = requests.get(file_url)
    response.raise_for_status()
    return response.content

# ----------------------------
# Upload Function
# ----------------------------
def upload_to_minio(content, bucket, object_name):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # Ensure the bucket exists
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    # Save file locally before uploading (MinIO requires a file path or stream)
    local_path = "/tmp/barcelona_raw_restaurants.csv"
    with open(local_path, "wb") as f:
        f.write(content)

    client.fput_object(bucket, object_name, local_path, content_type="text/csv")
    print(f"[MinIO] File uploaded to s3://{bucket}/{object_name}")

# ----------------------------
# Main
# ----------------------------
def main():
    content = download_bcn_file()
    upload_to_minio(content, BUCKET_NAME, OBJECT_PATH)

if __name__ == "__main__":
    main()
