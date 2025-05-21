import requests
import json
import pandas as pd
from minio import Minio
from pyspark.sql import SparkSession
import os

# ----------------------------
# Configuration
# ----------------------------
YELP_API_KEY = "F-kq1RwUQWqO7076MLzV0-m2zrFvC6BB0sTMQizIjmbZwzIuqWiNMPAY9m5XuW8KKlOWSPYx16rbMFPyzlJHTlMTzYnpxDg1sMvb5pLY21MhW9KCCpOerMy50yktaHYx"
YELP_BASE_URL = "https://api.yelp.com/v3/businesses/search"
YELP_PHOTOS_URL = "https://api.yelp.com/v3/businesses/{id}"

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "bdm-project-upc"
RAW_PATH = "raw/batch/yelp/yelp_restaurants.parquet"

# ----------------------------
# Yelp API functions
# ----------------------------
def obtener_restaurantes_yelp(ciudad):
    headers = {
        "Authorization": f"Bearer {YELP_API_KEY}"
    }
    params = {
        "term": "restaurants",
        "location": ciudad,
        "limit": 50
    }
    try:
        response = requests.get(YELP_BASE_URL, headers=headers, params=params)
        response.raise_for_status()
        return response.json().get("businesses", [])
    except requests.exceptions.RequestException as e:
        print(f"Error en la API de Yelp: {e}")
        return []

def obtener_fotos_restaurante_yelp(business_id, cantidad=1):
    # Yelp business detail endpoint has photos array
    headers = {
        "Authorization": f"Bearer {YELP_API_KEY}"
    }
    url = YELP_PHOTOS_URL.format(id=business_id)
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        photos = data.get("photos", [])
        return photos[:cantidad]
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener fotos para {business_id}: {e}")
        return []

# ----------------------------
# Data Transformation
# ----------------------------
def transformar_datos_yelp(resultados):
    datos_transformados = []
    for r in resultados:
        location = r.get("location", {})
        datos_transformados.append({
            "name": r.get("name", ""),
            "direction": ", ".join(filter(None, [
                location.get("address1"),
                location.get("city"),
                location.get("zip_code"),
                location.get("country")
            ])),
            "number": r.get("phone", ""),  # phone as string
            "email": "",  # Yelp does not provide email in API
            "rating": float(r.get("rating", 0.0)),
            "comments": "",  # Not provided
            "open_hours": "",  # Requires extra API calls, skipping for now
            "type": ", ".join(r.get("categories", [{}])[0].get("title", "") if r.get("categories") else ""),
            "key_words": [c.get("title") for c in r.get("categories", [])] if r.get("categories") else [],
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

# ----------------------------
# Temporal function to visualize data
# ----------------------------
def visualizar_datos():
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    bucket = BUCKET_NAME
    object_path = RAW_PATH
    local_file = "/tmp/yelp_restaurants.parquet"

    try:
        client.fget_object(bucket, object_path, local_file)
        print(f"Archivo descargado desde MinIO: {local_file}")
    except Exception as e:
        print(f"Error descargando desde MinIO: {e}")
        return

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
def main():
    ciudad = "Barcelona"
    nombre_archivo_s3 = "yelp_restaurants.parquet"

    print("Consultando Yelp...")
    datos = []
    datos_yelp = obtener_restaurantes_yelp(ciudad)

    for r in datos_yelp:
        business_id = r.get("id")
        r["photo_urls"] = obtener_fotos_restaurante_yelp(business_id)
        datos.append(r)

    formateados = []
    for r in datos:
        location = r.get("location", {})
        direccion = ", ".join(filter(None, [
            location.get("address1"),
            location.get("city"),
            location.get("state"),
            location.get("zip_code"),
            location.get("country")
        ]))
        formateados.append({
            "name": r.get("name", ""),
            "direction": direccion,
            "number": r.get("phone", ""),
            "email": "",  # not available from Yelp
            "rating": r.get("rating", 0.0),
            "comments": "",  # not available
            "open_hours": "",  # not available in this scope
            "type": ", ".join([c["title"] for c in r.get("categories", [])]) if r.get("categories") else "",
            "key_words": [c["title"] for c in r.get("categories", [])] if r.get("categories") else []
        })

    # Save as Parquet locally
    df = pd.DataFrame(formateados)
    local_file = "/tmp/yelp_restaurants.parquet"
    df.to_parquet(local_file, index=False)

    # Upload to MinIO
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    bucket = BUCKET_NAME
    object_path = f"raw/batch/yelp/{nombre_archivo_s3}"
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    client.fput_object(bucket, object_path, local_file)
    print(f"Datos guardados en MinIO: s3://{bucket}/{object_path}")

    # Show data
    visualizar_datos()

if __name__ == "__main__":
    main()
