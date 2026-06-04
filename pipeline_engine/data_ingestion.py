import glob
import logging
import os

class DataIngestion:
    def __init__(self, spark, bronze_path):
        self.spark = spark
        self.bronze_path = bronze_path
        self.logger = logging.getLogger("DataIngestionLogger")

    def _resolve_source_file(self, dataset_prefix):
        expected_csv = os.path.join(self.bronze_path, f"{dataset_prefix}_tripdata_2021_01.csv")
        expected_parquet = os.path.join(self.bronze_path, f"{dataset_prefix}_tripdata_2021_01.parquet")

        if os.path.exists(expected_csv):
            return expected_csv
        if os.path.exists(expected_parquet):
            return expected_parquet

        csv_candidates = sorted(glob.glob(os.path.join(self.bronze_path, f"{dataset_prefix}_tripdata_*.csv")))
        parquet_candidates = sorted(glob.glob(os.path.join(self.bronze_path, f"{dataset_prefix}_tripdata_*.parquet")))

        if parquet_candidates:
            return parquet_candidates[-1]
        if csv_candidates:
            return csv_candidates[-1]

        raise FileNotFoundError(f"No source file found for dataset: {dataset_prefix}")

    def _read_source(self, file_path):
        if file_path.endswith(".parquet"):
            return self.spark.read.parquet(file_path)

        return (
            self.spark.read
            .option("header", "true")
            .option("mode", "FAILFAST")
            .option("enforceSchema", "true")
            .csv(file_path)
        )

    def import_data(self):
        try:
            self.logger.info("Starting data import...")
            green_file_path = self._resolve_source_file("green")
            yellow_file_path = self._resolve_source_file("yellow")

            green_df = self._read_source(green_file_path)
            yellow_df = self._read_source(yellow_file_path)

            self.logger.info(f"Loaded Green dataset with {green_df.count()} records.")
            self.logger.info(f"Loaded Yellow dataset with {yellow_df.count()} records.")
            return green_df, yellow_df
        except Exception as e:
            self.logger.error(f"Error importing data: {e}")
            raise
