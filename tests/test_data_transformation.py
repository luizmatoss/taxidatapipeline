from pipeline_engine.data_transformation import DataTransformation
import pytest


def test_standardize_green_columns(spark, green_schema):
    data = [
        (
            "2",
            "2021-01-01 10:00:00",
            "2021-01-01 10:15:00",
            "42",
            "43",
            "1",
            "2.5",
            "0.50",
            "15.25",
        )
    ]
    df = spark.createDataFrame(data, green_schema)
    transformation = DataTransformation(spark, "unused")

    result = transformation.standardize_column_names(df, "Green")

    assert result.columns == [
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
    assert result.count() == 1


def test_standardize_yellow_columns(spark, yellow_schema):
    data = [
        (
            "1",
            "2021-01-02 14:30:00",
            "2021-01-02 14:45:00",
            "138",
            "239",
            "2",
            "3.2",
            "1.25",
            "22.50",
        )
    ]
    df = spark.createDataFrame(data, yellow_schema)
    transformation = DataTransformation(spark, "unused")

    result = transformation.standardize_column_names(df, "Yellow")

    assert result.count() == 1
    assert "PickUpDateTime" in result.columns
    assert "DropOffDateTime" in result.columns


def test_standardize_keeps_nulls_and_row_count(spark, green_schema):
    data = [
        (
            "2",
            "2021-01-01 10:00:00",
            None,
            "42",
            None,
            "1",
            None,
            "0.50",
            "15.25",
        ),
        (
            "1",
            "2021-01-01 11:00:00",
            "2021-01-01 11:15:00",
            "43",
            "44",
            "2",
            "3.0",
            "1.00",
            "20.00",
        ),
    ]
    df = spark.createDataFrame(data, green_schema)
    transformation = DataTransformation(spark, "unused")

    result = transformation.standardize_column_names(df, "Green")

    assert result.count() == 2
    first = result.orderBy("PickUpDateTime").collect()[0]
    assert first.DropOffDateTime is None
    assert first.DropOffLocationId is None
    assert first.TripDistance is None


def test_standardize_empty_dataframe(spark, green_schema):
    df = spark.createDataFrame([], green_schema)
    transformation = DataTransformation(spark, "unused")

    result = transformation.standardize_column_names(df, "Green")

    assert result.count() == 0
    assert result.columns == [
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


def test_standardize_unsupported_dataset_raises(spark, green_schema):
    df = spark.createDataFrame([], green_schema)
    transformation = DataTransformation(spark, "unused")

    with pytest.raises(ValueError, match="Unsupported dataset name"):
        transformation.standardize_column_names(df, "Blue")
