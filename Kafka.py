import subprocess
import platform
import os
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

class kafka_utilities():

    def __init__(self,kafka_path):
        self.kafka_path = kafka_path
    

    def start_zookeeper(self):
        """Inicia Zookeeper en segundo plano según el sistema operativo."""
        system = platform.system()
        
        if system == "Windows":
            cmd = f"{self.kafka_path}\\bin\\windows\\zookeeper-server-start.bat {self.kafka_path}\\config\\zookeeper.properties"
        else:
            cmd = f"{self.kafka_path}/bin/zookeeper-server-start.sh {self.kafka_path}/config/zookeeper.properties"

        return subprocess.Popen(cmd, shell=True)

    def start_kafka(self):
        """Inicia Kafka en segundo plano según el sistema operativo."""
        system = platform.system()
        
        if system == "Windows":
            cmd = f"{self.kafka_path}\\bin\\windows\\kafka-server-start.bat {self.kafka_path}\\config\\server.properties"
        else:
            cmd = f"{self.kafka_path}/bin/kafka-server-start.sh {self.kafka_path}/config/server.properties"

        return subprocess.Popen(cmd, shell=True)

    def create_topic(self,topic_name, kafka_bootstrap_servers="localhost:9092"):
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
    
    def enviar_a_kafka(self,producer, topic, mensajes):
        """Envía mensajes a Kafka."""
        for mensaje in mensajes:
            producer.produce(topic, mensaje.encode("utf-8"))
        producer.flush()