from datetime import datetime
import uuid
import random

def register_users(producer, topic):
    names = ["Alice", "Bob", "Charlie", "Diana", "Ethan", "Fiona", "George", "Hannah", "Ivan", "Julia"]

    while True:
        user = {
            "user_id": str(uuid.uuid4()),
            "name": random.choice(names),
            "register_date": datetime.now().isoformat()
        }

        kafka.enviar_a_kafka(producer, topic,user)
        print(f"Registered: {user}")
        time.sleep(10)     

def user_search(user_id, coordinates = "41.376484, 2.135738"):
    # falta que se generen diferentes searches
    categories = ["Tapas", "Japanese", "Italian", "Mexican", "Chinese", "Indian", "French", "American", "Vegan", "Thai", "Spanish"]
    return{"user_id": user_id,
           "timestamp": datetime.now(), 
           "coordinates": coordinates, 
           "theme": random.choice(categories)}

        