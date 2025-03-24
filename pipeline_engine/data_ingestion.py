from pyspark.sql import SparkSession
import logging

class DataIngestion:
    def __init__(self, spark, bronze_path):
        self.spark = spark
        self.bronze_path = bronze_path
        self.logger = logging.getLogger("DataIngestionLogger")

    def import_data(self):
        try:
            self.logger.info("Starting data import...")
            green_df = self.spark.read.option("header", "true").csv(f"{self.bronze_path}/green_tripdata_2021_01.csv")
            yellow_df = self.spark.read.option("header", "true").csv(f"{self.bronze_path}/yellow_tripdata_2021_01.csv")
            self.logger.info(f"Loaded Green dataset with {green_df.count()} records.")
            self.logger.info(f"Loaded Yellow dataset with {yellow_df.count()} records.")
            return green_df, yellow_df
        except Exception as e:
            self.logger.error(f"Error importing data: {e}")
            raise
