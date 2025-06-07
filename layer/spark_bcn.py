from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap
import sys
if __name__ == "__main__":
  
  print("Initializing Spark session...")
  spark = SparkSession.builder \
      .appName("CSV to Iceberg") \
      .config("spark.sql.catalog.cleaned", "org.apache.iceberg.spark.SparkCatalog") \
      .config("spark.sql.catalog.cleaned.type", "hadoop") \
      .config("spark.sql.catalog.cleaned.warehouse", "s3a://cleaned/iceberg") \
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
      .config("spark.hadoop.fs.s3a.access.key", "minio") \
      .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
      .config("spark.hadoop.fs.s3a.path.style.access", "true") \
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
      .getOrCreate()
  spark.sparkContext.setLogLevel("INFO")
  print("Reading raw file...")
  df_raw = spark.read.option("header", True).option("inferSchema", True).option("encoding", "UTF-16LE").option("delimiter", "\t").csv("s3a://raw/batch/barcelona/barcelona_raw_restaurants.csv")
  df_raw = df_raw.toDF(*[c.replace('\ufeff', '').strip() for c in df_raw.columns])
  print(df_raw.columns)

  # df_renamed = (
  #     df_raw
  #     .withColumnRenamed("adress_road_name", "road_name")
  #     .withColumnRenamed("adress_zip_code", "zip")
  #     .withColumnRenamed("adress_neighbourhood_name", "neighbourhood")
  #     .withColumnRenamed("adress_town", "town")
  #     .withColumnRenamed("values_values", "telephone")
  # )

  print("Processing file...")
  df_filtered = df_raw.filter(df_raw["secondary_filters_name"] == "Restaurants")
  
  df_selected = df_filtered.select(
      col("name"),
      col("addresses_road_name").alias("road_name"),
      col("addresses_zip_code").alias("zip"),
      col("addresses_start_street_number"),
      col("addresses_end_street_number"),
      col("addresses_neighborhood_name").alias("neighbourhood"),
      col("addresses_town").alias("city"),
      col("values_value").alias("telephone"),
      col("secondary_filter_name"),
      )



  df_capitalized = df_selected.withColumn("city", initcap("city"))

  print("Saving data in Iceberg format...")
  df_capitalized.writeTo("cleaned.iceberg.restaurants") \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .createOrReplace()

  df_capitalized.show()