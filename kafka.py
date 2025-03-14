import requests
import subprocess
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Configuración de la API de Foursquare
FOURSQUARE_API_KEY = "fsq33IomRNg9y+33yeWoBDYszC3kkKYDEysBR3/Wyf8kJC0="
FOURSQUARE_BASE_URL = "https://api.foursquare.com/v3/places/search"

# Configuración de Kafka
KAFKA_DIR = "/kafka"
kafka_conf = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "foursquare_producer"
}
kafka_topic = "foursquare_restaurants"

def start_zookeeper():
    """Inicia Zookeeper en segundo plano en Windows."""
    cmd = f"{KAFKA_DIR}/bin/windows/zookeeper-server-start.bat {KAFKA_DIR}/config/zookeeper.properties"
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def start_kafka():
    """Inicia Kafka en segundo plano en Windows."""
    cmd = f"{KAFKA_DIR}/bin/windows/kafka-server-start.bat {KAFKA_DIR}/config/server.properties"
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def create_topic(topic_name, kafka_bootstrap_servers="localhost:9092"):
    """Crea un tópico en Kafka si no existe."""
    admin_client = AdminClient({"bootstrap.servers": kafka_bootstrap_servers})
    topic_list = [NewTopic(topic_name, num_partitions=1, replication_factor=1)]
    future = admin_client.create_topics(topic_list)
    
    for topic, f in future.items():
        try:
            f.result()  # Bloquea hasta que se complete
            print(f"Tópico '{topic}' creado exitosamente.")
        except Exception as e:
            print(f"Advertencia: {e}")

def obtener_restaurantes(ciudad):
    """Obtiene restaurantes de Foursquare."""
    headers = {
        "Accept": "application/json",
        "Authorization": FOURSQUARE_API_KEY
    }
    params = {"query": "restaurant", "near": ciudad, "limit": 5}
    try:
        respuesta = requests.get(FOURSQUARE_BASE_URL, headers=headers, params=params)
        respuesta.raise_for_status()
        return respuesta.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"Error en la API de Foursquare: {e}")
        return []

def enviar_a_kafka(producer, topic, mensajes):
    """Envía mensajes a Kafka."""
    for mensaje in mensajes:
        producer.produce(topic, mensaje.encode("utf-8"))
    producer.flush()

if __name__ == "__main__":
    print("Iniciando Zookeeper...")
    zk_process = start_zookeeper()
    time.sleep(5)

    print("Iniciando Kafka...")
    kafka_process = start_kafka()
    time.sleep(10)

    print("Creando tópico de Kafka...")
    create_topic(kafka_topic)

    ciudad = "Barcelona"
    datos = obtener_restaurantes(ciudad)

    if datos:
        producer = Producer(kafka_conf)
        mensajes = [
            f"Nombre: {r.get('name', 'N/A')}, Dirección: {r.get('location', {}).get('formatted_address', 'N/A')}"
            for r in datos
        ]
        enviar_a_kafka(producer, kafka_topic, mensajes)
        for mensaje in mensajes:
            print(f"Enviado a Kafka: {mensaje}")