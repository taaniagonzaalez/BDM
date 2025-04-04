import time
from kafka_utils import start_zookeeper, start_kafka, create_topic, enviar_a_kafka, kafka_conf
from data_sources import obtener_restaurantes_foursquare, obtener_fotos_restaurante_foursquare, obtener_info_google
from delta_utils import iniciar_spark, guardar_en_delta
from confluent_kafka import Producer

ciudad = "Barcelona"
kafka_topic = "foursquare_restaurants"
DELTA_PATH = "./delta_output"

print("Iniciando Zookeeper...")
zk_process = start_zookeeper()
time.sleep(5)

print("Iniciando Kafka...")
kafka_process = start_kafka()
time.sleep(10)

print("Creando tópico de Kafka...")
create_topic(kafka_topic)

print("Consultando Foursquare...")
datos = []
datos_fsq = obtener_restaurantes_foursquare(ciudad)

for r in datos_fsq:
    fsq_id = r.get("fsq_id")
    r["photo_urls"] = obtener_fotos_restaurante_foursquare(fsq_id)

    nombre = r.get("name")
    direccion = r.get("location", {}).get("formatted_address", "")
    r.update(obtener_info_google(nombre, direccion))
    datos.append(r)

if datos:
    producer = Producer(kafka_conf)
    mensajes = [
        f"Nombre: {r.get('name', 'N/A')}, Dirección: {r.get('location', {}).get('formatted_address', 'N/A')}"
        for r in datos
    ]
    enviar_a_kafka(producer, kafka_topic, mensajes)
    for r in datos:
        print("\n====================")
        print(f"Nombre: {r.get('name')}")
        print(f"Dirección: {r.get('location', {}).get('formatted_address')}")
        print(f"Fotos Foursquare: {r.get('photo_urls')}")
        print(f"Fotos Google: {r.get('google_photo_urls')}")
        print(f"Rating Google: {r.get('google_rating')} ({r.get('google_reviews')} opiniones)")
        print(f"Horarios: {r.get('google_opening_hours')}")
        print(f"Comentarios: {r.get('google_comments')}")

    spark = iniciar_spark()
    guardar_en_delta(spark, datos, DELTA_PATH, "Restaurantes")
    spark.stop()
