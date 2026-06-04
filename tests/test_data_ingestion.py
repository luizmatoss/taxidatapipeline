import os
import pytest

from pipeline_engine.data_ingestion import DataIngestion


def test_file_paths(tmp_path, monkeypatch):
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"
    gold_path = tmp_path / "gold"

    bronze_path.mkdir()
    silver_path.mkdir()
    gold_path.mkdir()

    (bronze_path / "green_tripdata_2021_01.csv").write_text("header\n", encoding="utf-8")
    (bronze_path / "yellow_tripdata_2021_01.csv").write_text("header\n", encoding="utf-8")

    monkeypatch.setenv("BRONZE_PATH", str(bronze_path))
    monkeypatch.setenv("SILVER_PATH", str(silver_path))
    monkeypatch.setenv("GOLD_PATH", str(gold_path))

    assert os.path.exists(os.getenv("BRONZE_PATH"))
    assert os.path.exists(os.getenv("SILVER_PATH"))
    assert os.path.exists(os.getenv("GOLD_PATH"))

    green_data_path = os.path.join(os.getenv("BRONZE_PATH"), "green_tripdata_2021_01.csv")
    yellow_data_path = os.path.join(os.getenv("BRONZE_PATH"), "yellow_tripdata_2021_01.csv")

    assert os.path.exists(green_data_path)
    assert os.path.exists(yellow_data_path)


def test_import_data_reads_green_and_yellow(spark, tmp_path):
    bronze_path = tmp_path / "bronze"
    bronze_path.mkdir()

    green_csv = (
        "VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,PULocationID,DOLocationID,"
        "passenger_count,trip_distance,tip_amount,total_amount\n"
        "2,2021-01-01 10:00:00,2021-01-01 10:15:00,42,43,1,2.5,0.5,15.0\n"
    )
    yellow_csv = (
        "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,PULocationID,DOLocationID,"
        "passenger_count,trip_distance,tip_amount,total_amount\n"
        "1,2021-01-01 11:00:00,2021-01-01 11:20:00,138,239,2,3.2,1.2,20.0\n"
    )

    (bronze_path / "green_tripdata_2021_01.csv").write_text(green_csv, encoding="utf-8")
    (bronze_path / "yellow_tripdata_2021_01.csv").write_text(yellow_csv, encoding="utf-8")

    ingestion = DataIngestion(spark, str(bronze_path))
    green_df, yellow_df = ingestion.import_data()

    assert green_df.count() == 1
    assert yellow_df.count() == 1
    assert "VendorID" in green_df.columns
    assert "VendorID" in yellow_df.columns


def test_import_data_raises_when_required_file_missing(spark, tmp_path):
    bronze_path = tmp_path / "bronze"
    bronze_path.mkdir()

    green_csv = (
        "VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,PULocationID,DOLocationID,"
        "passenger_count,trip_distance,tip_amount,total_amount\n"
        "2,2021-01-01 10:00:00,2021-01-01 10:15:00,42,43,1,2.5,0.5,15.0\n"
    )
    (bronze_path / "green_tripdata_2021_01.csv").write_text(green_csv, encoding="utf-8")

    ingestion = DataIngestion(spark, str(bronze_path))

    with pytest.raises(Exception):
        ingestion.import_data()