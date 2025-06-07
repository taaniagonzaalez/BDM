from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, split, explode, trim, mean, count

def get_spark_session():
    return SparkSession.builder \
        .appName("GoldenLayerKPI") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://bdm-project-upc/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://host.docker.internal:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def main():
    spark = get_spark_session()

    # Leer la tabla cleaned (fuente única de verdad)
    df_all = spark.table("my_catalog.cleaned_layer.restaurants")

    # Popularity score (provisional = rating)
    df_all = df_all.withColumn("popularity_score", col("rating"))

    # Guardar la tabla base con popularity_score en la golden layer
    df_all.writeTo("my_catalog.golden_layer.restaurants_kpi").overwritePartitions()

    # --------------------------
    # KPI 1: Por tipo de cocina
    # --------------------------
    cuisine_df = (
        df_all
        .filter(col("type").isNotNull())
        .withColumn("type", lower(col("type")))
        .withColumn("type", split(col("type"), ","))
        .withColumn("type", explode(col("type")))
        .withColumn("type", trim(col("type")))
    )

    cuisine_kpi = (
        cuisine_df
        .groupBy("type")
        .agg(
            count("*").alias("restaurant_count"),
            mean("rating").alias("avg_rating")
        )
    )

    cuisine_kpi.writeTo("my_catalog.golden_layer.kpi_cuisine").overwrite()

    # --------------------------
    # KPI 2: Rating por fuente
    # --------------------------
    rating_kpi = (
        df_all
        .groupBy("source")
        .agg(
            mean("rating").alias("avg_rating"),
            count("*").alias("total_restaurants")
        )
    )

    rating_kpi.writeTo("my_catalog.golden_layer.kpi_source_rating").overwrite()

    print("Golden Layer KPIs written successfully from cleaned_layer.restaurants.")

if __name__ == "__main__":
    main()