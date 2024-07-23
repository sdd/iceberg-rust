from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()

spark.read.parquet("/home/iceberg/raw_data/yellow_tripdata_2024-01.parquet").writeTo("nyc.taxis").append()
