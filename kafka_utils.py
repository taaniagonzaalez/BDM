import subprocess
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
KAFKA_DIR = "/kafka"
kafka_conf = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "foursquare_producer"
}

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

def enviar_a_kafka(topic, mensajes):
    """Envía mensajes a Kafka."""
    producer = Producer(kafka_conf)
    for mensaje in mensajes:
        producer.produce(topic, mensaje.encode("utf-8"))
    producer.flush()