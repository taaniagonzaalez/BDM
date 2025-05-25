import requests
import json
import pandas as pd
from minio import Minio
from pyspark.sql import SparkSession
import os

# ----------------------------
# Configuration
# ----------------------------
FOURSQUARE_API_KEY = "fsq33IomRNg9y+33yeWoBDYszC3kkKYDEysBR3/Wyf8kJC0="
FOURSQUARE_BASE_URL = "https://api.foursquare.com/v3/places/search"
FOURSQUARE_PHOTOS_URL = "https://api.foursquare.com/v3/places/{fsq_id}/photos"

MINIO_ENDPOINT = "host.docker.internal:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "bdm-project-upc"
RAW_PATH = "raw/batch/foursquare/foursquare_restaurants.parquet"

# ----------------------------
# Foursquare API functions
# ----------------------------
def obtener_restaurantes_foursquare(ciudad):
    headers = {
        "Accept": "application/json",
        "Authorization": FOURSQUARE_API_KEY
    }
    params = {"query": "restaurant", "near": ciudad, "limit": 50}
    try:
        response = requests.get(FOURSQUARE_BASE_URL, headers=headers, params=params)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"Error en la API de Foursquare: {e}")
        return []

def obtener_fotos_restaurante_foursquare(fsq_id, cantidad=1):
    headers = {
        "Accept": "application/json",
        "Authorization": FOURSQUARE_API_KEY
    }
    url = FOURSQUARE_PHOTOS_URL.format(fsq_id=fsq_id)
    params = {"limit": cantidad}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        fotos = response.json()
        return [f"{f['prefix']}original{f['suffix']}" for f in fotos] if fotos else []
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener fotos para {fsq_id}: {e}")
        return []

# ----------------------------
# Data Transformation
# ----------------------------
def transformar_datos(resultados):
    datos_transformados = []
    for r in resultados:
        loc = r.get("location", {})
        datos_transformados.append({
            "name": r.get("name", ""),
            "direction": ", ".join(filter(None, [
                loc.get("address"),
                loc.get("locality"),
                loc.get("postcode"),
                loc.get("country")
            ])),
            "number": r.get("tel", "0"),  # If telephone available, convert to int
            "email": r.get("email", ""),
            "rating": float(r.get("rating", 0.0)) if "rating" in r else 0.0,
            "comments": r.get("description", ""),
            "open_hours": r.get("hours", {}).get("display", ""),
            "type": r.get("categories", [{}])[0].get("name", ""),
            "key_words": [c.get("name") for c in r.get("categories", [])],
        })
    return datos_transformados

# ----------------------------
# Save to MinIO (Parquet)
# ----------------------------
def guardar_en_minio_parquet(datos):
    df = pd.DataFrame(datos)
    parquet_file = "/tmp/restaurants.parquet"
    df.to_parquet(parquet_file, index=False)

    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    client.fput_object(BUCKET_NAME, RAW_PATH, parquet_file)
    print(f"Archivo guardado en MinIO: s3://{BUCKET_NAME}/{RAW_PATH}")

# ----------------------------
# Optional: Register Iceberg table (Spark)
# ----------------------------
def registrar_tabla_iceberg():
    spark = SparkSession.builder \
        .appName("IcebergMinIOExample") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", f"s3a://{BUCKET_NAME}/iceberg-tables") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    df = spark.read.parquet(f"s3a://{BUCKET_NAME}/{RAW_PATH}")
    df.writeTo("my_catalog.restaurants").using("iceberg").createOrReplace()
    print("Tabla Iceberg creada o actualizada.")

#Temporal function to visualize data
def visualizar_datos():
    # Setup MinIO client
    client = Minio("host.docker.internal:9000", access_key="minio", secret_key="minio123", secure=False)
    bucket = "bdm-project-upc"
    object_path = "raw/batch/foursquare/foursquare_restaurants.parquet"
    local_file = "/tmp/foursquare_restaurants.parquet"

    # Download Parquet from MinIO
    try:
        client.fget_object(bucket, object_path, local_file)
        print(f"Archivo descargado desde MinIO: {local_file}")
    except Exception as e:
        print(f"Error descargando desde MinIO: {e}")
        return

    # Load and display with pandas
    try:
        df = pd.read_parquet(local_file)
        pd.set_option("display.max_columns", None)
        print("\n Vista previa de los datos:")
        print(df.head(20))
    except Exception as e:
        print(f"Error al cargar con pandas: {e}")

# ----------------------------
# Main Function
# ----------------------------
""" def main():
    ciudad = "Barcelona"
    print("Consultando Foursquare...")
    datos_fsq = obtener_restaurantes_foursquare(ciudad)

    for r in datos_fsq:
        fsq_id = r.get("fsq_id")
        r["photo_urls"] = obtener_fotos_restaurante_foursquare(fsq_id)

    datos_formateados = transformar_datos(datos_fsq)
    guardar_en_minio_parquet(datos_formateados)
    registrar_tabla_iceberg() """

#Temporal main to visualize data
def main():
    ciudad = "Barcelona"
    nombre_archivo_s3 = "foursquare_restaurants.parquet"

    print("Consultando Foursquare...")
    datos = []
    datos_fsq = obtener_restaurantes_foursquare(ciudad)

    for r in datos_fsq:
        fsq_id = r.get("fsq_id")
        r["photo_urls"] = obtener_fotos_restaurante_foursquare(fsq_id)
        datos.append(r)

    # Format data to your required schema
    formateados = []
    for r in datos:
        loc = r.get("location", {})
        direccion = ", ".join(filter(None, [
            loc.get("address"), loc.get("locality"), loc.get("region"),
            loc.get("postcode"), loc.get("country")
        ]))
        formateados.append({
            "name": r.get("name", ""),
            "direction": direccion,
            "number": r.get("tel", 0),
            "email": "",  # not available from Foursquare
            "rating": r.get("rating", 0.0),
            "comments": "",  # not available
            "open_hours": "",  # not available
            "type": ", ".join([c["name"] for c in r.get("categories", [])]) if r.get("categories") else "",
            "key_words": [c["name"] for c in r.get("categories", [])] if r.get("categories") else []
        })

    # Save as Parquet locally
    import pandas as pd
    df = pd.DataFrame(formateados)
    local_file = "/tmp/foursquare_restaurants.parquet"
    df.to_parquet(local_file, index=False)

    # Upload to MinIO
    from minio import Minio
    client = Minio("host.docker.internal:9000", "minio", "minio123", secure=False)
    bucket = "bdm-project-upc"
    object_path = f"raw/batch/foursquare/{nombre_archivo_s3}"
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    client.fput_object(bucket, object_path, local_file)
    print(f"Datos guardados en MinIO: s3://{bucket}/{object_path}")

    # Show data
    visualizar_datos()



if __name__ == "__main__":
    main()