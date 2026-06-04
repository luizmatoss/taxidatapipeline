import pytest
from pyspark.sql.readwriter import DataFrameWriter
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pipeline_engine.data_aggregation import DataAggregation


def _mock_csv(monkeypatch):
	def fake_csv(self, path, header=None):
		return None

	monkeypatch.setattr(DataFrameWriter, "csv", fake_csv)


def _base_rows():
	return [
		("1", "42", "43", 1, "2.5", "0.50", "15.25", "2021-01-01 10:00:00", "2021-01-01 10:15:00"),
		("1", "42", "43", 2, "3.5", "1.00", "18.50", "2021-01-01 11:00:00", "2021-01-01 11:15:00"),
		("2", "44", "45", 1, "5.0", "2.00", "30.00", "2021-01-01 12:00:00", "2021-01-01 12:15:00"),
	]


def test_shape_data_groups_location_and_vendor(spark, monkeypatch):
	_mock_csv(monkeypatch)
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
	df = spark.createDataFrame(_base_rows(), schema=schema)
	aggregation = DataAggregation(spark, "unused")

	locations_df, vendors_df = aggregation.shape_data_to_gold(df)

	assert locations_df.count() == 2
	assert vendors_df.count() == 2


def test_shape_data_location_totals_are_correct(spark, monkeypatch):
	_mock_csv(monkeypatch)
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
	df = spark.createDataFrame(_base_rows(), schema=schema)
	aggregation = DataAggregation(spark, "unused")

	locations_df, _ = aggregation.shape_data_to_gold(df)
	row = locations_df.filter(
		(locations_df.PickUpLocationId == "42") & (locations_df.DropOffLocationId == "43")
	).collect()[0]

	assert float(row.TotalFare) == pytest.approx(33.75)
	assert float(row.TotalTips) == pytest.approx(1.50)
	assert float(row.AvgPickUpDistance) == pytest.approx(3.0)
	assert float(row.AvgDropOffDistance) == pytest.approx(3.0)


def test_shape_data_empty_dataframe(spark, monkeypatch):
	_mock_csv(monkeypatch)
	schema = StructType(
		[
			StructField("VendorId", StringType(), True),
			StructField("PickUpLocationId", StringType(), True),
			StructField("DropOffLocationId", StringType(), True),
			StructField("PassengerCount", IntegerType(), True),
			StructField("TripDistance", StringType(), True),
			StructField("TipAmount", StringType(), True),
			StructField("TotalAmount", StringType(), True),
			StructField("PickUpDateTime", StringType(), True),
			StructField("DropOffDateTime", StringType(), True),
		]
	)
	df = spark.createDataFrame([], schema=schema)
	aggregation = DataAggregation(spark, "unused")

	locations_df, vendors_df = aggregation.shape_data_to_gold(df)

	assert locations_df.count() == 0
	assert vendors_df.count() == 0