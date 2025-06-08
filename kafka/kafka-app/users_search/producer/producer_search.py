from kafka import KafkaProducer
from minio import Minio
import json
import uuid
import random
from datetime import datetime
import time
import os

# ----------------------------
# Kafka Producer Configuration
# ----------------------------
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

user_search_topic = 'user_search'

# ----------------------------
# MinIO Configuration
# ----------------------------
MINIO_ENDPOINT = "host.docker.internal:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "bdm-project-upc"

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# ----------------------------
# Get Existing User IDs
# ----------------------------
prefix = 'raw/streaming/event_type=registration'
user_ids = []

objects = client.list_objects(BUCKET_NAME, prefix=prefix, recursive=True)
for obj in objects:
    key = obj.object_name
    if key.startswith(prefix + "/user_") and key.endswith(".json"):
        user_id = key.split("user_")[1].split(".json")[0]
        user_ids.append(user_id)

# ----------------------------
# Simulate and Send User Searches
# ----------------------------
def user_search(user_id, producer):      
    min_lat = 41.36
    max_lat = 41.42
    min_lon = 2.14
    max_lon = 2.21
    theme = ["Tapas", "Japanese", "Italian", "Mexican", "Chinese", "Indian", "French", "American", "Vegan", "Thai", "Spanish"]

    while True:
        user = {
            "user_id": user_id,
            "coordinates": {
                "latitude": round(random.uniform(min_lat, max_lat), 6),
                "longitude": round(random.uniform(min_lon, max_lon), 6)
            },
            "theme": random.choice(theme),
            "search_date": datetime.now().isoformat()
        }
        producer.send(user_search_topic, value=user)
        print(f"[Producer] Search sent → {user}")
        time.sleep(20)

# ----------------------------
# Main Execution
# ----------------------------
if __name__ == "__main__":
    if not user_ids:
        print("[Producer] No user IDs found in MinIO. Please register users first.")
    else:
        user_search(random.choice(user_ids), producer)
