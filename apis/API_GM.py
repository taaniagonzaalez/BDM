import pandas as pd
import random
import json
from datetime import datetime
from minio import Minio
import os

# ----------------------------
# Configuration
# ----------------------------
MINIO_ENDPOINT = "host.docker.internal:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "bdm-project-upc"
INPUT_CSV_PATH = "/opt/airflow/apis/Restaurant_reviews.csv"
RAW_PATH_TEMPLATE = "raw/batch/googlemaps/date={date}/restaurant_{name}.json"

# ----------------------------
# Review Generator
# ----------------------------
class ReseñasGoogleMaps:
    def __init__(self):
        self.df_reviews = pd.read_csv(INPUT_CSV_PATH)
    
    def generar_review(self):
        for column in ["Review", "text"]:
            if column in self.df_reviews.columns:
                return self.df_reviews.sample(n=1).iloc[0][column]
        raise ValueError("No se encontró ninguna columna válida para las reseñas ('Review' o 'text').")
    
    def generar_multiples_reviews(self, cantidad=10):
        return [self.generar_review() for _ in range(cantidad)]

# ----------------------------
# Save to MinIO (JSON)
# ----------------------------
def guardar_reviews_en_minio(nombre_restaurante, reviews_json):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    date_str = datetime.now().strftime("%Y-%m-%d")
    s3_path = RAW_PATH_TEMPLATE.format(date=date_str, name=nombre_restaurante.replace(" ", "_"))
    local_tmp_path = f"/tmp/{nombre_restaurante.replace(' ', '_')}.json"

    with open(local_tmp_path, "w", encoding="utf-8") as f:
        json.dump(reviews_json, f, ensure_ascii=False)

    client.fput_object(
        bucket_name=BUCKET_NAME,
        object_name=s3_path,
        file_path=local_tmp_path,
        content_type="application/json"
    )

    print(f"[MinIO] Reseñas guardadas en s3://{BUCKET_NAME}/{s3_path}")

# ----------------------------
# Main Function
# ----------------------------
def main():
    print("Generando reseñas simuladas de Google Maps...")

    # Simulated restaurant names (adjust or fetch dynamically later)
    nombres_restaurantes = ["Beyond Flavours", "Paradise", "Flechazo", "Shah Ghouse Hotel & Restaurant", "Over The Moon Brew Company", "eat.fit","Hyper Local","Cream Stone","Sardarji's Chaats & More","Barbeque Nation","AB's - Absolute Barbecues"]

    generador = ReseñasGoogleMaps()

    for nombre in nombres_restaurantes:
        reseñas = generador.generar_multiples_reviews()
        guardar_reviews_en_minio(nombre, reseñas)

    print("Proceso finalizado. Reseñas almacenadas en MinIO.")

if __name__ == "__main__":
    main()
