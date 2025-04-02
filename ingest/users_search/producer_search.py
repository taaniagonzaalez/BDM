from kafka import KafkaProducer
import json
import uuid
import random
from datetime import datetime
import time

producer = KafkaProducer(
                bootstrap_servers='localhost:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )

def user_search(user_id, producer):      
    min_lat = 41.36
    max_lat = 41.42
    min_lon = 2.14
    max_lon = 2.21
    theme = ["Tapas", "Japanese", "Italian", "Mexican", "Chinese", "Indian", "French", "American", "Vegan", "Thai", "Spanish"]
    while True:
        user = {"user_id": user_id,
                "coordinates":{
                    "latitude": random.uniform(min_lat, max_lat),
                    "longitude": random.uniform(min_lon, max_lon)
                },
                "theme": random.choice(theme),
                "search_date": datetime.now().isoformat()
                }
        producer.send(user_search_topic, value = user)
        time.sleep(30)

if __name__ == "__main__":
    ## Cojer de s3 los datos del cliente para el user_id
    user_search(user_id, producer)



