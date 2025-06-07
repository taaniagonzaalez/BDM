import os
import pandas as pd
from minio import Minio

# ----------------------------
# Configuration
# ----------------------------
MINIO_ENDPOINT = "host.docker.internal:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "bdm-project-upc"
FOLDERS_TO_READ = [
    "silver_layer",
    "golden_layer"
]
LOCAL_OUTPUT_DIR = "./dashboard_data"

# ----------------------------
# Connect to MinIO
# ----------------------------
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

if not os.path.exists(LOCAL_OUTPUT_DIR):
    os.makedirs(LOCAL_OUTPUT_DIR)

# ----------------------------
# Download and Convert Parquet to CSV
# ----------------------------
for folder in FOLDERS_TO_READ:
    print(f"Looking for file on: {folder}/")

    objects = client.list_objects(BUCKET_NAME, prefix=folder, recursive=True)
    for obj in objects:
        if obj.object_name.endswith(".parquet"):
            file_name = obj.object_name.split("/")[-1].replace(".parquet", ".csv")
            local_parquet = f"/tmp/{file_name}.parquet"
            local_csv = os.path.join(LOCAL_OUTPUT_DIR, file_name)

            # Download parquet file
            client.fget_object(BUCKET_NAME, obj.object_name, local_parquet)
            print(f"Downloading: {obj.object_name}")

            # Convert to CSV
            try:
                df = pd.read_parquet(local_parquet)
                df.to_csv(local_csv, index=False)
                print(f"CSV: {local_csv}")
            except Exception as e:
                print(f"Error reading {local_parquet}: {e}")
            