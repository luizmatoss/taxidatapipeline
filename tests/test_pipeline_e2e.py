import pytest

import main


def test_run_pipeline_writes_silver_and_gold_outputs(tmp_path, monkeypatch):
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"
    gold_path = tmp_path / "gold"

    bronze_path.mkdir()
    silver_path.mkdir()
    gold_path.mkdir()

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

    monkeypatch.setenv("BRONZE_PATH", str(bronze_path))
    monkeypatch.setenv("SILVER_PATH", str(silver_path))
    monkeypatch.setenv("GOLD_PATH", str(gold_path))

    main.run_pipeline()

    assert (silver_path / "valid_combined_data.parquet").exists()
    assert (silver_path / "invalid_combined_data.csv").exists()
    assert (silver_path / "deduped_combined_data.parquet").exists()
    assert (gold_path / "locations_data.csv").exists()
    assert (gold_path / "vendors_data.csv").exists()

    spark = main.create_spark_session()

    valid_df = spark.read.parquet(str(silver_path / "valid_combined_data.parquet"))
    deduped_df = spark.read.parquet(str(silver_path / "deduped_combined_data.parquet"))
    locations_df = (
        spark
        .read.option("header", "true")
        .csv(str(gold_path / "locations_data.csv"))
    )
    vendors_df = (
        spark
        .read.option("header", "true")
        .csv(str(gold_path / "vendors_data.csv"))
    )

    assert valid_df.columns == [
        "VendorId",
        "PickUpDateTime",
        "DropOffDateTime",
        "PickUpLocationId",
        "DropOffLocationId",
        "PassengerCount",
        "TripDistance",
        "TipAmount",
        "TotalAmount",
    ]
    assert deduped_df.columns == valid_df.columns
    assert valid_df.count() == 2
    assert deduped_df.count() == 2

    valid_rows = {
        (row.VendorId, row.PickUpLocationId, row.DropOffLocationId, row.PassengerCount, row.TotalAmount)
        for row in valid_df.collect()
    }
    assert valid_rows == {
        (1, "138", "239", 2, 20.0),
        (2, "42", "43", 1, 15.0),
    }

    assert locations_df.columns == [
        "PickUpLocationId",
        "DropOffLocationId",
        "TotalFare",
        "TotalTips",
        "AvgPickUpDistance",
        "AvgDropOffDistance",
    ]
    assert locations_df.count() == 2

    location_rows = {
        (row.PickUpLocationId, row.DropOffLocationId): (float(row.TotalFare), float(row.TotalTips))
        for row in locations_df.collect()
    }
    assert location_rows[("138", "239")] == (pytest.approx(20.0), pytest.approx(1.2))
    assert location_rows[("42", "43")] == (pytest.approx(15.0), pytest.approx(0.5))

    assert vendors_df.columns == ["VendorId", "TotalFare", "TotalTips", "AvgFare", "AvgTip"]
    assert vendors_df.count() == 2

    vendor_rows = {
        row.VendorId: (float(row.TotalFare), float(row.TotalTips), float(row.AvgFare), float(row.AvgTip))
        for row in vendors_df.collect()
    }
    assert vendor_rows["1"] == (pytest.approx(20.0), pytest.approx(1.2), pytest.approx(20.0), pytest.approx(1.2))
    assert vendor_rows["2"] == (pytest.approx(15.0), pytest.approx(0.5), pytest.approx(15.0), pytest.approx(0.5))


def test_run_pipeline_writes_invalid_silver_contract(tmp_path, monkeypatch):
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"
    gold_path = tmp_path / "gold"

    bronze_path.mkdir()
    silver_path.mkdir()
    gold_path.mkdir()

    green_csv = (
        "VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,PULocationID,DOLocationID,"
        "passenger_count,trip_distance,tip_amount,total_amount\n"
        "2,2021-01-01 10:00:00,2021-01-01 10:15:00,42,43,0,1.0,0.1,9.0\n"
    )
    yellow_csv = (
        "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,PULocationID,DOLocationID,"
        "passenger_count,trip_distance,tip_amount,total_amount\n"
        "1,2021-01-01 11:00:00,2021-01-01 11:20:00,138,239,1,3.2,1.2,20.0\n"
    )

    (bronze_path / "green_tripdata_2021_01.csv").write_text(green_csv, encoding="utf-8")
    (bronze_path / "yellow_tripdata_2021_01.csv").write_text(yellow_csv, encoding="utf-8")

    monkeypatch.setenv("BRONZE_PATH", str(bronze_path))
    monkeypatch.setenv("SILVER_PATH", str(silver_path))
    monkeypatch.setenv("GOLD_PATH", str(gold_path))

    main.run_pipeline()

    spark = main.create_spark_session()
    invalid_df = spark.read.csv(str(silver_path / "invalid_combined_data.csv"))

    assert invalid_df.columns == [
        "_c0",
        "_c1",
        "_c2",
        "_c3",
        "_c4",
        "_c5",
        "_c6",
        "_c7",
        "_c8",
    ]
    assert invalid_df.count() == 1

    row = invalid_df.collect()[0]
    assert row._c0 == "2"
    assert row._c3 == "42"
    assert row._c4 == "43"
    assert row._c5 == "0"
    assert row._c8 == "9.0"