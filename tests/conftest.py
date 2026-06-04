import sys
import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(scope="session")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[1]")
        .appName("taxi-pipeline-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def green_schema():
    return StructType(
        [
            StructField("VendorID", StringType(), True),
            StructField("lpep_pickup_datetime", StringType(), True),
            StructField("lpep_dropoff_datetime", StringType(), True),
            StructField("PULocationID", StringType(), True),
            StructField("DOLocationID", StringType(), True),
            StructField("passenger_count", StringType(), True),
            StructField("trip_distance", StringType(), True),
            StructField("tip_amount", StringType(), True),
            StructField("total_amount", StringType(), True),
        ]
    )


@pytest.fixture
def yellow_schema():
    return StructType(
        [
            StructField("VendorID", StringType(), True),
            StructField("tpep_pickup_datetime", StringType(), True),
            StructField("tpep_dropoff_datetime", StringType(), True),
            StructField("PULocationID", StringType(), True),
            StructField("DOLocationID", StringType(), True),
            StructField("passenger_count", StringType(), True),
            StructField("trip_distance", StringType(), True),
            StructField("tip_amount", StringType(), True),
            StructField("total_amount", StringType(), True),
        ]
    )


@pytest.fixture
def standardized_schema():
    return StructType(
        [
            StructField("VendorId", IntegerType(), True),
            StructField("PickUpDateTime", StringType(), True),
            StructField("DropOffDateTime", StringType(), True),
            StructField("PickUpLocationId", StringType(), True),
            StructField("DropOffLocationId", StringType(), True),
            StructField("PassengerCount", IntegerType(), True),
            StructField("TripDistance", StringType(), True),
            StructField("TipAmount", StringType(), True),
            StructField("TotalAmount", StringType(), True),
        ]
    )
