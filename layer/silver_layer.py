from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, regexp_replace, soundex, levenshtein
)

def get_spark_session():
    return SparkSession.builder \
        .appName("merge_restaurants_by_name") \
        .config("spark.sql.catalog.cleaned", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.cleaned.type", "hadoop") \
        .config("spark.sql.catalog.cleaned.warehouse", "s3a://cleaned/iceberg") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://bdm-project-upc/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def normalize(df, col_name):
    return df.withColumn("norm_name", lower(regexp_replace(col(col_name), "[^a-z0-9áéíóúñü]", "")))

def run_job():
    spark = get_spark_session()

    # === 1. Cargar y preparar el CSV del Ayuntamiento ===
    df_raw = spark.read.option("header", True)\
        .option("inferSchema", True)\
        .option("encoding", "UTF-16LE")\
        .option("delimiter", "\t")\
        .csv("s3a://raw/batch/barcelona/barcelona_raw_restaurants.csv")

    df_raw = df_raw.toDF(*[c.replace('\ufeff', '').strip() for c in df_raw.columns])
    df_raw = (
        df_raw
        .withColumnRenamed("adress_road_name", "road_name")
        .withColumnRenamed("adress_zip_code", "zip")
        .withColumnRenamed("adress_neighbourhood_name", "neighbourhood")
        .withColumnRenamed("adress_town", "town")
        .withColumnRenamed("values_values", "telephone")
    )

    df_filtered = df_raw.filter(col("secondary_filters_name") == "Restaurants")

    df_cleaned = df_filtered.select(
        col("name"),
        col("road_name"),
        col("zip"),
        col("addresses_start_street_number"),
        col("addresses_end_street_number"),
        col("neighbourhood"),
        col("town").alias("city"),
        col("telephone"),
        col("secondary_filter_name").alias("category")
    )

    df_cleaned.writeTo("cleaned.iceberg.restaurants") \
        .using("iceberg") \
        .tableProperty("format-version", "2") \
        .createOrReplace()

    # === 2. Normalizar tabla cleaned para hacer fuzzy match ===
    df_bcn_norm = normalize(df_cleaned, "name").withColumn("soundex_name", soundex(col("norm_name")))

    # === 3. Cargar Foursquare + Yelp ===
    df1 = spark.read.parquet("s3a://bdm-project-upc/raw/batch/foursquare/foursquare_restaurants.parquet")
    df2 = spark.read.parquet("s3a://bdm-project-upc/raw/batch/yelp/yelp_restaurants.parquet")
    df_ext = df1.unionByName(df2)
    df_ext_norm = normalize(df_ext, "name").withColumn("soundex_name", soundex(col("norm_name")))

    # === 4. Fuzzy join: soundex + levenshtein ===
    joined = (
        df_ext_norm.alias("new")
        .join(
            df_bcn_norm.alias("bcn"),
            (col("new.soundex_name") == col("bcn.soundex_name")) &
            (levenshtein(col("new.norm_name"), col("bcn.norm_name")) <= 3),
            how="left"
        )
        .select("new.*", "bcn.restaurant_id")
    )

    # === 5. Añadir norm_name para MERGE seguro por nombre limpio
    final = normalize(joined.drop("restaurant_id"), "name")

    # === 6. Crear tabla si no existe o hacer merge por nombre ===
    spark.sql("CREATE DATABASE IF NOT EXISTS my_catalog.silver_layer")
    tablas = spark.sql("SHOW TABLES IN my_catalog.silver_layer").rdd.map(lambda r: r.tableName).collect()

    if "restaurants" not in tablas:
        # Primera vez: crear tabla con UUID para todos
        from pyspark.sql.functions import expr
        final = final.withColumn("restaurant_id", expr("uuid()")).drop("norm_name")
        final.writeTo("my_catalog.silver_layer.restaurants") \
            .using("iceberg") \
            .tableProperty("format-version", "2") \
            .create()
    else:
        final.createOrReplaceTempView("updates")
        spark.sql("""
            MERGE INTO my_catalog.silver_layer.restaurants AS target
            USING updates AS source
            ON LOWER(REGEXP_REPLACE(target.name, '[^a-z0-9áéíóúñü]', '')) = source.norm_name
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            SELECT uuid() AS restaurant_id, * EXCEPT(norm_name) FROM source
        """)

if __name__ == "__main__":
    run_job()
