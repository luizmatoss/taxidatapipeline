from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NYCTaxiDataPipeline") \
    .master("local[*]") \
    .getOrCreate()

# Test Spark session
print(spark.version)
