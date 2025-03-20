from pyspark.sql.functions import when
import logging

class DataValidation:
    def __init__(self, spark, silver_path):
        self.spark = spark
        self.silver_path = silver_path
        self.logger = logging.getLogger("DataValidationLogger")

    def validate_data(self, df, dataset_name):
        try:
            self.logger.info(f"Validating {dataset_name} dataset...")
            valid_data_df = df.filter(df["PassengerCount"] > 0).withColumn(
                "VendorId", when(df["VendorId"].isNull(), 999).otherwise(df["VendorId"])
            )
            invalid_data_df = df.subtract(valid_data_df)
            valid_data_df.write.mode("overwrite").parquet(f"{self.silver_path}/valid_{dataset_name}_data.parquet")
            invalid_data_df.write.mode("overwrite").csv(f"{self.silver_path}/invalid_{dataset_name}_data.csv")
            return valid_data_df, invalid_data_df
        except Exception as e:
            self.logger.error(f"Error validating data: {e}")
            raise
