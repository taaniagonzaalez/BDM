from kafka import KafkaConsumer
from minio import Minio
import json
import os
import time
from datetime import datetime

# ----------------------------
# Configuration
# ----------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
BUCKET_NAME = "bdm-project-upc"
MINIO_ENDPOINT = "host.docker.internal:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

# ----------------------------
# Kafka Consumer
# ----------------------------
consumer = KafkaConsumer(
    'user_search',
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='user-search-group'
)

# ----------------------------
# MinIO Client
# ----------------------------
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

if not minio_client.bucket_exists(BUCKET_NAME):
    minio_client.make_bucket(BUCKET_NAME)

# ----------------------------
# Main Loop
# ----------------------------
if __name__ == "__main__":
    print("[Consumer] Listening for messages...")
    for message in consumer:
        event = message.value
        print(f"[Consumer] Event received: {event}")

        user_id = event.get("user_id", "unknown")
        search_date = event.get("search_date", datetime.utcnow().isoformat())

        register_date, hour_date = search_date.split("T")
        s3_key = f"raw/streaming/event_type=search/date={register_date}/user_{user_id}_{hour_date}.json"

        # Convert data to JSON and upload to MinIO
        content = json.dumps(event, ensure_ascii=False).encode("utf-8")

        minio_client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=s3_key,
            data=io.BytesIO(content),
            length=len(content),
            content_type='application/json'
        )

        print(f"[MinIO] Saved event to: s3://{BUCKET_NAME}/{s3_key}")
        time.sleep(20)
