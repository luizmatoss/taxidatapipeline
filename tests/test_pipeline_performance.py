import time

import pytest
from pyspark.sql.readwriter import DataFrameWriter

from pipeline_engine.data_aggregation import DataAggregation
from pipeline_engine.data_deduplication import DataDeduplication
from pipeline_engine.data_transformation import DataTransformation
from pipeline_engine.data_validation import DataValidation


pytestmark = pytest.mark.performance


def _mock_writers(monkeypatch):
	def fake_parquet(self, path):
		return None

	def fake_csv(self, path, header=None):
		return None

	monkeypatch.setattr(DataFrameWriter, "parquet", fake_parquet)
	monkeypatch.setattr(DataFrameWriter, "csv", fake_csv)


def test_transformation_performance_sanity(spark, green_schema):
	transformation = DataTransformation(spark, "unused")
	data = [
		(
			"1",
			f"2021-01-01 {i % 24:02d}:00:00",
			f"2021-01-01 {i % 24:02d}:15:00",
			str(i % 250),
			str((i + 1) % 250),
			"1",
			"2.5",
			"0.5",
			"10.0",
		)
		for i in range(3000)
	]
	df = spark.createDataFrame(data, green_schema)

	started = time.perf_counter()
	output_count = transformation.standardize_column_names(df, "Green").count()
	elapsed = time.perf_counter() - started

	assert output_count == 3000
	assert elapsed < 20


def test_validation_and_dedup_performance_sanity(spark, standardized_schema, monkeypatch):
	_mock_writers(monkeypatch)
	validation = DataValidation(spark, "unused")
	dedup = DataDeduplication(spark, "unused")
	data = [
		(
			i % 2 + 1,
			f"2021-01-01 {i % 24:02d}:00:00",
			f"2021-01-01 {i % 24:02d}:15:00",
			str(i % 120),
			str((i + 1) % 120),
			1,
			"3.0",
			"1.0",
			"15.0",
		)
		for i in range(5000)
	]
	df = spark.createDataFrame(data, standardized_schema)

	started = time.perf_counter()
	valid_df, _ = validation.validate_data(df, "perf")
	deduped_df = dedup.dedupe_data(valid_df, "perf")
	dedup_count = deduped_df.count()
	elapsed = time.perf_counter() - started

	assert dedup_count > 0
	assert elapsed < 25


def test_aggregation_performance_sanity(spark, monkeypatch):
	_mock_writers(monkeypatch)
	aggregation = DataAggregation(spark, "unused")
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
			str(i % 2 + 1),
			str(i % 200),
			str((i + 1) % 200),
			1,
			"2.5",
			"0.5",
			"10.0",
			f"2021-01-01 {i % 24:02d}:00:00",
			f"2021-01-01 {i % 24:02d}:15:00",
		)
		for i in range(4000)
	]
	df = spark.createDataFrame(data, schema=schema)

	started = time.perf_counter()
	locations_df, vendors_df = aggregation.shape_data_to_gold(df)
	total_groups = locations_df.count() + vendors_df.count()
	elapsed = time.perf_counter() - started

	assert total_groups > 0
	assert elapsed < 25