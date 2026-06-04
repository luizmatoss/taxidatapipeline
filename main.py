import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pipeline_engine.data_ingestion import DataIngestion
from pipeline_engine.data_transformation import DataTransformation
from pipeline_engine.data_validation import DataValidation
from pipeline_engine.data_deduplication import DataDeduplication
from pipeline_engine.data_aggregation import DataAggregation

def create_spark_session():
    return SparkSession.builder \
        .appName("NYCTaxiDataPipeline") \
        .master("local[*]") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .getOrCreate()


def run_pipeline():
    spark = create_spark_session()

    load_dotenv()
    bronze_path = os.getenv("BRONZE_PATH")
    silver_path = os.getenv("SILVER_PATH")
    gold_path = os.getenv("GOLD_PATH")

    ingestion = DataIngestion(spark, bronze_path)
    transformation = DataTransformation(spark, silver_path)
    validation = DataValidation(spark, silver_path)
    deduplication = DataDeduplication(spark, silver_path)
    aggregation = DataAggregation(spark, gold_path)

    green_df, yellow_df = ingestion.import_data()

    standardized_green_df = transformation.standardize_column_names(green_df, "Green")
    standardized_yellow_df = transformation.standardize_column_names(yellow_df, "Yellow")

    combined_df = standardized_green_df.unionByName(standardized_yellow_df)
    valid_data_df, _ = validation.validate_data(combined_df, "combined")

    deduped_data = deduplication.dedupe_data(valid_data_df, "combined")
    aggregation.shape_data_to_gold(deduped_data)


if __name__ == "__main__":
    run_pipeline()
