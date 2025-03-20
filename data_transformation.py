from pyspark.sql.functions import col
import logging

class DataTransformation:
    def __init__(self, spark, silver_path):
        self.spark = spark
        self.silver_path = silver_path
        self.logger = logging.getLogger("DataTransformationLogger")

    def standardize_column_names(self, df, dataset_name):
        try:
            self.logger.info(f"Standardizing column names for {dataset_name} dataset...")
            if dataset_name == "Green":
                df = df.select(
                    col("VendorID").alias("VendorId"),
                    col("lpep_pickup_datetime").alias("PickUpDateTime"),
                    col("lpep_dropoff_datetime").alias("DropOffDateTime"),
                    col("PULocationID").alias("PickUpLocationId"),
                    col("DOLocationID").alias("DropOffLocationId"),
                    col("passenger_count").alias("PassengerCount"),
                    col("trip_distance").alias("TripDistance"),
                    col("tip_amount").alias("TipAmount"),
                    col("total_amount").alias("TotalAmount")
                )
            elif dataset_name == "Yellow":
                df = df.select(
                    col("VendorID").alias("VendorId"),
                    col("tpep_pickup_datetime").alias("PickUpDateTime"),
                    col("tpep_dropoff_datetime").alias("DropOffDateTime"),
                    col("PULocationID").alias("PickUpLocationId"),
                    col("DOLocationID").alias("DropOffLocationId"),
                    col("passenger_count").alias("PassengerCount"),
                    col("trip_distance").alias("TripDistance"),
                    col("tip_amount").alias("TipAmount"),
                    col("total_amount").alias("TotalAmount")
                )
            return df
        except Exception as e:
            self.logger.error(f"Error in standardizing columns: {e}")
            raise
