import requests
from confluent_kafka import Producer

# Configuración de la API de Foursquare
FOURSQUARE_API_KEY = "fsq33IomRNg9y+33yeWoBDYszC3kkKYDEysBR3/Wyf8kJC0="
FOURSQUARE_BASE_URL = "https://api.foursquare.com/v3/places/search"

# Configuración del productor de Kafka
kafka_conf = {
    "bootstrap.servers": "localhost:9092",  # Dirección del broker de Kafka
    "client.id": "foursquare_producer"
}
kafka_topic = "foursquare_restaurants"

# Función para obtener restaurantes de Foursquare
def obtener_restaurantes(ciudad):
    headers = {
        "Accept": "application/json",
        "Authorization": FOURSQUARE_API_KEY
    }
    params = {
        "query": "restaurant",
        "near": ciudad,
        "limit": 5  # Número de resultados a obtener
    }
    respuesta = requests.get(FOURSQUARE_BASE_URL, headers=headers, params=params)
    
    if respuesta.status_code == 200:
        return respuesta.json().get("results", [])
    else:
        print(f"Error al obtener datos: {respuesta.status_code}")
        return None

# Función para enviar datos a Kafka
def enviar_a_kafka(producer, topic, mensaje):
    producer.produce(topic, mensaje.encode("utf-8"))
    producer.flush()

if __name__ == "__main__":
    ciudad = "Barcelona"
    datos = obtener_restaurantes(ciudad)
    if datos:
        producer = Producer(kafka_conf)
        for restaurante in datos:
            nombre = restaurante.get("name", "N/A")
            direccion = restaurante.get("location", {}).get("formatted_address", "N/A")
            mensaje = f"Nombre: {nombre}, Dirección: {direccion}"
            enviar_a_kafka(producer, kafka_topic, mensaje)
            print(f"Enviado a Kafka: {mensaje}")