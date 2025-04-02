from ingest.apis.API_GM import * 
from ingest.apis.API_BCN import *
from ingest.start_kafka import *
from ingest.apis.users import *
import pandas as pd



# Define la ruta base donde tienes Kafka instalado
KAFKA_DIR = "/Users/tania/kafka"  # cambia esto a tu ruta real

# Al final del día esto se eliminará porque estará guardado en otra carpeta
path_bcn = "/Users/tania/Desktop/Master/2S/BDM/P1/BDM/dataframes/restaurants_bcn.csv"
df_bcn = pd.read_csv(path_bcn)


kafka_topic_status = "restaurant_status"
kafka_topic_review = "restaurant_review"
kafka_topic_user = "user_search"


kafka_conf = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "googlemaps_producer"
}

if __name__ == "__main__":

    kafka = kafka_utilities(KAFKA_DIR)
    ## CONFIGURAR KAFKA Y CREAR TOPICOS ##
    print("Iniciando Zookeeper...")
    zk_proc = kafka.start_zookeeper()
    
    time.sleep(5)  # esperar a que Zookeeper se inicie antes de Kafka
    
    print("Iniciando Kafka...")
    kafka_proc = kafka.start_kafka()

    kafka.wait_for_kafka()

    print("Creando tópicos de Kafka...")
    kafka.create_topic(kafka_topic_review)
    kafka.create_topic(kafka_topic_status)
    kafka.create_topic(kafka_topic_user)

    time.sleep(4)


    ## Obtener los datos ##

    results_comments = [get_restaurant_review(i) for i in df_bcn[:3]]
    results_status = [get_restaurant_info(i) for i in df_bcn[:3]]
    results_user = []
    # Aquí debe ir las coordenadas de usuario

    ## Enviar los datos a Kafka ##
    if results_status:
        producer = Producer(kafka_conf)
        mensajes = [
            f"Nombre: {r['restaurant']}, Status: {r['status']}, People: {r['people']}"
            for r in results_status
        ]
        kafka.enviar_a_kafka(producer, kafka_topic_status, mensajes)
        for mensaje in mensajes:
            print(f"Enviado a Kafka: {mensaje}")







    # Get data from bcn dataset
    # Get reviews from each restaurant from fake API
    # Save data using Delta Lake and Hadoop
    
    









