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
    cmd = f"{KAFKA_DIR}/bin/windows/zookeeper-server-start.bat {KAFKA_DIR}/config/zookeeper.properties"
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def start_kafka():
    cmd = f"{KAFKA_DIR}/bin/windows/kafka-server-start.bat {KAFKA_DIR}/config/server.properties"
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def create_topic(topic_name, kafka_bootstrap_servers="localhost:9092"):
    admin_client = AdminClient({"bootstrap.servers": kafka_bootstrap_servers})
    topic_list = [NewTopic(topic_name, num_partitions=1, replication_factor=1)]
    future = admin_client.create_topics(topic_list)

    for topic, f in future.items():
        try:
            f.result()
            print(f"Tópico '{topic}' creado exitosamente.")
        except Exception as e:
            print(f"Advertencia: {e}")

def enviar_a_kafka(producer, topic, mensajes):
    for mensaje in mensajes:
        producer.produce(topic, mensaje.encode("utf-8"))
    producer.flush()
