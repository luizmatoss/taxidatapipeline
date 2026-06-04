from pyspark.sql.readwriter import DataFrameWriter

from pipeline_engine.data_aggregation import DataAggregation


def _mock_csv(monkeypatch):
    def fake_csv(self, path, header=None):
        return None

    monkeypatch.setattr(DataFrameWriter, "csv", fake_csv)


def _input_df(spark):
    schema = [
        "VendorId",
        "PickUpLocationId",
        "DropOffLocationId",
        "PassengerCount",
        "TripDistance",
        "TipAmount",
        "TotalAmount",
        "PickUpDateTime",
        "DropOffDateTime",
    ]
    data = [
        (
            "1",
            "42",
            "43",
            1,
            "2.5",
            "0.50",
            "10.00",
            "2021-01-01 10:00:00",
            "2021-01-01 10:15:00",
        ),
        (
            "1",
            "42",
            "43",
            1,
            "2.5",
            "1.50",
            "20.00",
            "2021-01-01 11:00:00",
            "2021-01-01 11:15:00",
        ),
    ]
    return spark.createDataFrame(data, schema=schema)


def test_shape_data_returns_expected_output_columns(spark, monkeypatch):
    _mock_csv(monkeypatch)
    aggregation = DataAggregation(spark, "unused")

    locations_df, vendors_df = aggregation.shape_data_to_gold(_input_df(spark))

    assert locations_df.columns == [
        "PickUpLocationId",
        "DropOffLocationId",
        "TotalFare",
        "TotalTips",
        "AvgPickUpDistance",
        "AvgDropOffDistance",
    ]
    assert vendors_df.columns == [
        "VendorId",
        "TotalFare",
        "TotalTips",
        "AvgFare",
        "AvgTip",
    ]


def test_shape_data_idempotent_contract_with_repeat_calls(spark, monkeypatch):
    _mock_csv(monkeypatch)
    aggregation = DataAggregation(spark, "unused")
    df = _input_df(spark)

    first_locations, first_vendors = aggregation.shape_data_to_gold(df)
    second_locations, second_vendors = aggregation.shape_data_to_gold(df)

    assert first_locations.count() == second_locations.count()
    assert first_vendors.count() == second_vendors.count()


def test_shape_data_vendor_totals_contract(spark, monkeypatch):
    _mock_csv(monkeypatch)
    aggregation = DataAggregation(spark, "unused")

    _, vendors_df = aggregation.shape_data_to_gold(_input_df(spark))
    row = vendors_df.collect()[0]

    assert float(row.TotalFare) == 30.0
    assert float(row.TotalTips) == 2.0
