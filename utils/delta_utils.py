from pyspark.sql import SparkSession
from delta import *
import os

def iniciar_spark():
    builder = SparkSession.builder \
        .appName("FoursquareDelta") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()

def guardar_en_delta(spark, datos, ruta, api):
    print("Guardando en Delta Lake...")
    for r in datos:
        r["photo_urls"] = ", ".join(r.get("photo_urls", []))
    df = spark.createDataFrame(datos)
    ruta_final = os.path.join(ruta, api)
    df.write.format("delta").mode("overwrite").save(ruta_final)