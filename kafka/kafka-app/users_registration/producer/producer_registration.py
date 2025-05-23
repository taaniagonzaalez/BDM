from kafka import KafkaProducer
import json
import uuid
import random
from datetime import datetime
import time
import sys
import os

user_registration_topic = 'user_registration'

def user_register(producer):
    names = ["Alice", "Bob", "Charlie", "Diana", "Ethan", "Fiona"]
    while True:
        user = {
            "user_id": str(uuid.uuid4()),
            "name": random.choice(names),
            "register_date": datetime.now().isoformat()
        }

        producer.send(user_registration_topic, value = user)
        producer.flush()
        print(f"User registered: {user['user_id']}    {user['name']}    {user['register_date']}")
        time.sleep(20)


if __name__ == "__main__":
    
    # Kafka config
    producer = KafkaProducer(
                    bootstrap_servers='kafka:9092',
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                    )
    
    user_register(producer)
    