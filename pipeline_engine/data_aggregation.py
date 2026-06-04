from pyspark.sql.functions import col, sum, avg
import logging

class DataAggregation:
    def __init__(self, spark, gold_path):
        self.spark = spark
        self.gold_path = gold_path
        self.logger = logging.getLogger("DataAggregationLogger")
        
    def shape_data_to_gold(self, df):
        """
        Shape the data into the final format and save as CSV in the Gold directory.
        """
        try:
            # File 1 - Locations
            locations_df = df.groupBy("PickUpLocationId", "DropOffLocationId").agg(
                sum(col("TotalAmount").cast("float")).alias("TotalFare"),
                sum(col("TipAmount").cast("float")).alias("TotalTips"),
                avg(col("TripDistance")).alias("AvgPickUpDistance"),
                avg(col("TripDistance")).alias("AvgDropOffDistance")
            )
            locations_df.write.mode("overwrite").csv(f"{self.gold_path}/locations_data.csv", header=True)

            # File 2 - Vendors
            vendors_df = df.groupBy("VendorId").agg(
                sum(col("TotalAmount").cast("float")).alias("TotalFare"),
                sum(col("TipAmount").cast("float")).alias("TotalTips"),
                avg(col("TotalAmount").cast("float")).alias("AvgFare"),
                avg(col("TipAmount").cast("float")).alias("AvgTip")
            )
            vendors_df.write.mode("overwrite").csv(f"{self.gold_path}/vendors_data.csv", header=True)

            self.logger.info("Locations and Vendors data saved to Gold directory.")
            return locations_df, vendors_df
        except Exception as e:
            self.logger.error(f"Error shaping data to Gold: {e}")
            raise