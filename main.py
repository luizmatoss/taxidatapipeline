import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from data_ingestion import DataIngestion
from data_transformation import DataTransformation
from data_validation import DataValidation
from data_deduplication import DataDeduplication
from data_aggregation import DataAggregation

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NYCTaxiDataPipeline") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()
    

# Define file paths
load_dotenv() # Load environment variables
bronze_path = os.getenv("BRONZE_PATH")
silver_path = os.getenv("SILVER_PATH")
gold_path = os.getenv("GOLD_PATH")

# Initialize all pipeline components
ingestion = DataIngestion(spark, bronze_path)
transformation = DataTransformation(spark, silver_path)
validation = DataValidation(spark, silver_path)
deduplication = DataDeduplication(spark, silver_path)
aggregation = DataAggregation(spark, gold_path)

# 1. Import Data
green_df, yellow_df = ingestion.import_data()

# 2. Standardize Data
standardized_green_df = transformation.standardize_column_names(green_df, "Green")
standardized_yellow_df = transformation.standardize_column_names(yellow_df, "Yellow")

# 3. Validate Data
valid_data_df, invalid_data_df = validation.validate_data(standardized_green_df.unionByName(standardized_yellow_df), "combined")

# 4. Deduplicate Data
deduped_data = deduplication.dedupe_data(valid_data_df, "combined")

# 5. Shape and Save to Gold
locations_data, vendors_data = aggregation.shape_data_to_gold(deduped_data)

# 6. Save to Silver
transformed_data = deduplication.dedupe_data(deduped_data, "combined")
