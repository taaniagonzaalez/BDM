from kafka import KafkaProducer
from create_kafka_topics.kafka_topics import create_topic, foursquare_topic
from api.API_FOURSQUARE import obtener_restaurantes_foursquare, obtener_fotos_restaurante_foursquare
import json
import os
import boto3

ciudad = "Barcelona"
nombre_archivo_s3 = "restaurantes_barcelona.json"

print("Creando tópico de Kafka (si no existe)...")
create_topic(foursquare_topic)

print("Consultando Foursquare...")
datos = []
datos_fsq = obtener_restaurantes_foursquare(ciudad)

for r in datos_fsq:
    fsq_id = r.get("fsq_id")
    r["photo_urls"] = obtener_fotos_restaurante_foursquare(fsq_id)
    datos.append(r)

if datos:
    producer = KafkaProducer(bootstrap_servers="kafka:9092", value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    for r in datos:
        mensaje = {
            "nombre": r.get("name", "N/A"),
            "direccion": r.get("location", {}).get("formatted_address", "N/A"),
            "fotos": r.get("photo_urls", [])
        }
        producer.send(foursquare_topic, mensaje)

    producer.flush()

    for r in datos:
        print("\n====================")
        print(f"Nombre: {r.get('name')}")
        print(f"Dirección: {r.get('location', {}).get('formatted_address')}")
        print(f"Fotos Foursquare: {r.get('photo_urls')}")

    # Guardar en S3
    s3 = boto3.client("s3")
    bucket_name = os.getenv("AWS_BUCKET_NAME", "bdm-project-upc")
    contenido = json.dumps(datos, indent=2)
    ruta_s3 = f"restaurantes/{nombre_archivo_s3}"
    try:
        s3.put_object(Body=contenido, Bucket=bucket_name, Key=ruta_s3)
        print(f"✅ Datos guardados en S3: s3://{bucket_name}/{ruta_s3}")
    except Exception as e:
        print(f"❌ Error al guardar en S3: {e}")