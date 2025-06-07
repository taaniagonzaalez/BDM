from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lower, regexp_replace, soundex, levenshtein,
    col, udf
)
from pyspark.sql.types import StringType
import uuid

def get_spark_session():
    return SparkSession.builder \
        .appName("silver_layer") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://bdm-project-upc/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://host.docker.internal:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

# UDF para generar UUID
uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

def normalize(df, col_name):
    return df.withColumn(
        "norm_name",
        lower(regexp_replace(col(col_name), "[^a-z0-9áéíóúñü]", ""))
    )

def run_job():
    spark = get_spark_session()

    # 1. Leer datos de MinIO
    df1 = spark.read.parquet("s3a://bdm-project-upc/raw/batch/foursquare/foursquare_restaurants.parquet")
    df2 = spark.read.parquet("s3a://bdm-project-upc/raw/batch/yelp/yelp_restaurants.parquet")

    # 2. Normalizar nombres
    df1_norm = normalize(df1, "name").withColumn("soundex_name", soundex(col("norm_name")))
    df2_norm = normalize(df2, "name").withColumn("soundex_name", soundex(col("norm_name")))

    # 3. Comprobar si la tabla Iceberg ya existe
    tablas = spark.sql("SHOW TABLES IN my_catalog.silver_layer").rdd.map(lambda r: r.tableName).collect()
    if "restaurants" in tablas:
        # 3a. Cargar tabla existente
        existing = spark.table("my_catalog.silver_layer.restaurants")

        # 3b. Unir por fuzzy match: misma soundex & Levenshtein <= 3
        joined = (
            df2_norm.alias("nuevos")
            .join(
                df1_norm.alias("exist"),
                ( col("nuevos.soundex_name") == col("exist.soundex_name") ) &
                ( levenshtein(col("nuevos.norm_name"), col("exist.norm_name")) <= 3 ),
                how="left"
            )
            .select(
                col("nuevos.*"),
                col("exist.restaurant_id").alias("matched_id"),
                col("exist.norm_name").alias("matched_norm_name")
            )
        )

        # 3c. Separar matcheados vs no matcheados
        matched = (
            joined.filter(col("matched_id").isNotNull())
                  .withColumnRenamed("matched_id", "restaurant_id")
                  .drop("matched_norm_name", "soundex_name", "norm_name")
        )
        unmatched = (
            joined.filter(col("matched_id").isNull())
                  .withColumn("restaurant_id", uuid_udf())
                  .drop("matched_id", "soundex_name", "norm_name", "matched_norm_name")
        )

        # 3d. Reconstruir DataFrame completo de nuevos (df2) con restaurant_id asignado
        final_new = unmatched.select("restaurant_id", *df2.columns)
        final_matched = matched.select("restaurant_id", *df2.columns)

        # 3e. Insertar solo los no existentes en Iceberg (final_new)
        if "number" in final_new.columns:
            final_new = final_new.withColumn("number", col("number").cast("string"))

        final_new.writeTo("my_catalog.silver_layer.restaurants").append()

    else:
        # 4. Primera vez: creamos la tabla desde cero
        combined = df1.unionByName(df2).dropDuplicates()
        combined = combined.withColumn("restaurant_id", uuid_udf())

        if "number" in combined.columns:
            combined = combined.withColumn("number", col("number").cast("string"))

        combined.writeTo("my_catalog.silver_layer.restaurants") \
                .using("iceberg") \
                .tableProperty("format-version", "2") \
                .create()

if __name__ == "__main__":
    run_job()
