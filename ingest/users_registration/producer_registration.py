from kafka import KafkaProducer
import json
import uuid
import random
from datetime import datetime
import time
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from start_kafka import *

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

        time.sleep(20)


if __name__ == "__main__":
    
    # Kafka config
    kafka_driver.create_topic(user_registration_topic)
    producer = KafkaProducer(
                    bootstrap_servers='localhost:9092',
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                    )
    
    user_register(producer)
    