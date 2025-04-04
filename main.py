import time
from kafka_utils import start_zookeeper, start_kafka, create_topic, enviar_a_kafka
from api_utils import obtener_restaurantes
from delta_utils import iniciar_spark, guardar_en_delta

KAFKA_TOPIC = "foursquare_restaurants"
DELTA_PATH = "/tmp/delta_lake/restaurants"

if __name__ == "__main__":
    print("Iniciando Zookeeper...")
    zk_process = start_zookeeper()
    time.sleep(5)

    print("Iniciando Kafka...")
    kafka_process = start_kafka()
    time.sleep(10)

    print("Creando tópico de Kafka...")
    create_topic(KAFKA_TOPIC)

    # Buscar restaurantes
    ciudad = "Barcelona"
    datos = obtener_restaurantes(ciudad)

    if datos:
        # Enviar a Kafka
        mensajes = [
            f"Nombre: {r.get('name', 'N/A')}, Dirección: {r.get('location', {}).get('formatted_address', 'N/A')}, Fotos: {', '.join(r.get('photos', []))}"
            for r in datos
        ]
        enviar_a_kafka(KAFKA_TOPIC, mensajes)
        print(f"Enviado a Kafka: {mensajes}")
        # Guardar en Delta Lake
        print("Guardando en Delta Lake...")
        spark = iniciar_spark()
        guardar_en_delta(spark, datos, DELTA_PATH, "Foursquare")

        for mensaje in mensajes:
            print(f"Enviado a Kafka: {mensaje}")

