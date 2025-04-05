from kafka import KafkaProducer
import json
import uuid
import random
from datetime import datetime
import time
import os
import boto3


producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
user_search_topic = 'user_search'
"""
Get user_id: normally, when the user uses the app they have their user_id that will be sent with the request
"""

s3 = boto3.client('s3')

bucket_name = os.getenv("AWS_BUCKET_NAME", "bdm-project-upc")
current_day = datetime.now().isoformat().split("T")[0]
prefix = f'raw/streaming/event_type=registration/date={current_day}'
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

user_ids = []

for obj in response['Contents']:
    key = obj['Key'] 
    if key.startswith(prefix + "/user_") and key.endswith(".json"):
        user_id = key.split("user_")[1].split(".json")[0]
        user_ids.append(user_id)



def user_search(user_id, producer):      
    min_lat = 41.36
    max_lat = 41.42
    min_lon = 2.14
    max_lon = 2.21
    theme = ["Tapas", "Japanese", "Italian", "Mexican", "Chinese", "Indian", "French", "American", "Vegan", "Thai", "Spanish"]
    while True:
        user_id = str(uuid.uuid4())             # Here has to go a real user_id
        user = {"user_id": user_id,
                "coordinates":{
                    "latitude": random.uniform(min_lat, max_lat),
                    "longitude": random.uniform(min_lon, max_lon)
                },
                "theme": random.choice(theme),
                "search_date": datetime.now().isoformat()
                }
        producer.send(user_search_topic, value = user)
        print(f"Search sent:    {user['user_id']}   {user['coordinates']['latitude']}   {user['coordinates']['latitude']}     {user['theme']}     {user['search_date']}")
        time.sleep(20)

if __name__ == "__main__":
    ## Cojer de s3 los datos del cliente para el user_id
    #user_search(user_id, producer)
    user_search(random.choice(user_ids), producer)



