import requests
import pandas as pd
from minio import Minio
from pyspark.sql import SparkSession

# ----------------------------
# Configuration
# ----------------------------
YELP_API_KEY = "F-kq1RwUQWqO7076MLzV0-m2zrFvC6BB0sTMQizIjmbZwzIuqWiNMPAY9m5XuW8KKlOWSPYx16rbMFPyzlJHTlMTzYnpxDg1sMvb5pLY21MhW9KCCpOerMy50yktaHYx"
YELP_BASE_URL = "https://api.yelp.com/v3/businesses/search"
YELP_BUSINESS_URL = "https://api.yelp.com/v3/businesses/{id}"

MINIO_ENDPOINT = "host.docker.internal:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "bdm-project-upc"
RAW_PATH = "raw/batch/yelp/yelp_restaurants.parquet"

# ----------------------------
# Yelp API functions
# ----------------------------
def obtener_restaurantes_yelp(ciudad):
    headers = {"Authorization": f"Bearer {YELP_API_KEY}"}
    params = {"term": "restaurants", "location": ciudad, "limit": 50}
    try:
        response = requests.get(YELP_BASE_URL, headers=headers, params=params)
        response.raise_for_status()
        return response.json().get("businesses", [])
    except requests.exceptions.RequestException as e:
        print(f"Error en la API de Yelp: {e}")
        return []

def obtener_fotos_restaurante_yelp(business_id, cantidad=1):
    headers = {"Authorization": f"Bearer {YELP_API_KEY}"}
    url = YELP_BUSINESS_URL.format(id=business_id)
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        photos = response.json().get("photos", [])
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
        direccion = ", ".join(filter(None, [
            location.get("address1"),
            location.get("city"),
            location.get("state"),
            location.get("zip_code"),
            location.get("country")
        ]))
        datos_transformados.append({
            "name": r.get("name", ""),
            "direction": direccion,
            "number": r.get("phone", ""),
            "email": "",  # Yelp API doesn't provide email
            "rating": float(r.get("rating", 0.0)),
            "comments": "",  # Not available via Yelp API
            "open_hours": "",  # Requires extra API calls, skipping for now
            "type": ", ".join([c.get("title", "") for c in r.get("categories", [])]) if r.get("categories") else "",
            "key_words": [c.get("title") for c in r.get("categories", [])] if r.get("categories") else [],
            "photo_urls": obtener_fotos_restaurante_yelp(r.get("id"))
        })
    return datos_transformados

# ----------------------------
# Save to MinIO (Parquet)
# ----------------------------
def guardar_en_minio_parquet(datos, bucket=BUCKET_NAME, object_path=RAW_PATH):
    df = pd.DataFrame(datos)
    local_file = "/tmp/yelp_restaurants.parquet"
    df.to_parquet(local_file, index=False)

    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    client.fput_object(bucket, object_path, local_file)
    print(f"Archivo guardado en MinIO: s3://{bucket}/{object_path}")

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
# Main Function
# ----------------------------
def main():
    ciudad = "Barcelona"
    print("Consultando Yelp...")
    resultados_raw = obtener_restaurantes_yelp(ciudad)
    datos_transformados = transformar_datos_yelp(resultados_raw)

    guardar_en_minio_parquet(datos_transformados)

    print("Proceso finalizado. Datos transformados y almacenados en MinIO.")

if __name__ == "__main__":
    main()
