import logging


class DataDeduplication:
    def __init__(self, spark, silver_path):
        self.spark = spark
        self.silver_path = silver_path
        self.logger = logging.getLogger("DataDeduplicationLogger")

    def dedupe_data(self, df, dataset_name):
        try:
            self.logger.info(f"Deduplicating {dataset_name} dataset...")
            deduped_df = df.dropDuplicates(
                [
                    "PickUpLocationId",
                    "PickUpDateTime",
                    "DropOffDateTime",
                    "DropOffLocationId",
                    "VendorId",
                ]
            )
            deduped_df.write.mode("overwrite").parquet(
                f"{self.silver_path}/deduped_{dataset_name}_data.parquet"
            )
            return deduped_df
        except Exception as e:
            self.logger.error(f"Error deduplicating data: {e}")
            raise
