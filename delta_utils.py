import pyspark.sql.functions as F
from delta import *
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

def iniciar_spark():
    # Set custom working directory for Spark
    custom_cache_dir = "C:/Users/edelg/OneDrive/Escritorio/Project/BDM/spark_cache"
    os.environ["SPARK_LOCAL_DIRS"] = custom_cache_dir
    os.environ["SPARK_WORKDIR"] = custom_cache_dir
    """Inicializa una sesión de Spark sin necesidad de Hadoop en Windows."""
    spark = SparkSession.builder \
        .appName("MyDeltaLakeApp") \
        .config("spark.local.dir", custom_cache_dir) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:3.0.0") \
        .getOrCreate()
    
    return spark

def guardar_en_delta(spark, datos, delta_path, api_name):
    """Guarda datos en Delta Lake."""
    if not datos:
        print("No hay datos para guardar.")
        return
    
    # Ensure `datos` is a list of dictionaries
    structured_data = [
        {
            "name": r.get("name", "N/A"),
            "address": r.get("location", {}).get("formatted_address", "N/A"),
            "api_source": api_name
        } for r in datos
    ]

    # Define schema explicitly
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("api_source", StringType(), True)
    ])

    df = spark.createDataFrame(structured_data, schema=schema)

    df.write.format("delta").mode("append").save(delta_path)
    print("Datos guardados en Delta Lake correctamente.")

def leer_desde_delta(spark, ruta):
    """Lee los datos almacenados en Delta Lake."""
    return spark.read.format("delta").load(ruta)
