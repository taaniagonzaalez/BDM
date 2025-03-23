from API_GM import * 
from API_BCN import *
from Kafka import *
import pandas as pd
import socket

review_creator_path = 'utilities/review_creator.json'

with open(review_creator_path, "r", encoding="utf-8") as file:
    review_creator = json.load(file)

def get_restaurant_info(i):
    api = API_GM(i, review_creator)
    return {
        "restaurant": i,
        "status": api.get_current_status(),
        "people": api.get_number_people()
    }


def wait_for_kafka(host="localhost", port=9092, timeout=30):
    print("Esperando a que Kafka esté disponible...")
    for _ in range(timeout):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((host, port))
                print("✅ Kafka está listo.")
                return True
            except Exception:
                time.sleep(1)
    print("❌ Kafka no está disponible después de esperar.")
    return False

# Define la ruta base donde tienes Kafka instalado
KAFKA_DIR = "/Users/tania/kafka"  # cambia esto a tu ruta real

path_bcn = "/Users/tania/Desktop/Master/2S/BDM/P1/BDM/dataframes/restaurants_bcn.csv"

kafka_topic = "Restaurant_status"

df_bcn = pd.read_csv(path_bcn)

kafka_conf = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "googlemaps_producer"
}

if __name__ == "__main__":

    kafka = kafka_utilities(KAFKA_DIR)
    print("Iniciando Zookeeper...")
    zk_proc = kafka.start_zookeeper()
    
    time.sleep(5)  # esperar a que Zookeeper se inicie antes de Kafka
    
    print("Iniciando Kafka...")
    kafka_proc = kafka.start_kafka()

    wait_for_kafka()

    print("Creando tópico de Kafka...")
    kafka.create_topic(kafka_topic)

    time.sleep(2)

    restaurants = list(df_bcn['name'])

    
    results = [get_restaurant_info(i) for i in restaurants[:3]]
    if results:
        producer = Producer(kafka_conf)
        mensajes = [
            f"Nombre: {r['restaurant']}, Status: {r['status']}, People: {r['people']}"
            for r in results
        ]
        kafka.enviar_a_kafka(producer, kafka_topic, mensajes)
        for mensaje in mensajes:
            print(f"Enviado a Kafka: {mensaje}")







    # Get data from bcn dataset
    # Get reviews from each restaurant from fake API
    # Save data using Delta Lake and Hadoop
    
    









