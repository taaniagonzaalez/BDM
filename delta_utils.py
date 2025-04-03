import pyspark.sql.functions as F
from delta import *
from pyspark.sql import SparkSession

def iniciar_spark():
    """Inicializa Spark con soporte para Delta Lake."""
    spark = (
        SparkSession.builder.appName("DeltaLakeExample")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    return spark

def guardar_en_delta(spark, datos, ruta, api_nombre):
    """Guarda los datos en Delta Lake, separados por API."""
    df = spark.createDataFrame(datos)
    df = df.withColumn("api_origen", F.lit(api_nombre))  # Agregar columna con el nombre de la API
    df.write.format("delta").mode("append").save(ruta)

def leer_desde_delta(spark, ruta):
    """Lee los datos almacenados en Delta Lake."""
    return spark.read.format("delta").load(ruta)
