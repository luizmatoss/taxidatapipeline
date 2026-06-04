from pyspark.sql.readwriter import DataFrameWriter

from pipeline_engine.data_validation import DataValidation


def _mock_writes(monkeypatch):
	def fake_parquet(self, path):
		return None

	def fake_csv(self, path):
		return None

	monkeypatch.setattr(DataFrameWriter, "parquet", fake_parquet)
	monkeypatch.setattr(DataFrameWriter, "csv", fake_csv)


def test_validate_splits_valid_and_invalid_rows(spark, standardized_schema, monkeypatch):
	_mock_writes(monkeypatch)
	validation = DataValidation(spark, "unused")
	data = [
		(1, "2021-01-01 10:00:00", "2021-01-01 10:15:00", "42", "43", 1, "2.5", "0.50", "15.25"),
		(2, "2021-01-01 11:00:00", "2021-01-01 11:15:00", "44", "45", 0, "3.0", "0.25", "12.00"),
		(2, "2021-01-01 12:00:00", "2021-01-01 12:15:00", "46", "47", 2, "4.0", "1.25", "21.00"),
	]
	df = spark.createDataFrame(data, standardized_schema)

	valid_df, invalid_df = validation.validate_data(df, "test")

	assert valid_df.count() == 2
	assert invalid_df.count() == 1


def test_validate_imputes_null_vendor_in_valid_data(spark, standardized_schema, monkeypatch):
	_mock_writes(monkeypatch)
	validation = DataValidation(spark, "unused")
	data = [
		(None, "2021-01-01 10:00:00", "2021-01-01 10:15:00", "42", "43", 1, "2.5", "0.50", "15.25"),
		(2, "2021-01-01 11:00:00", "2021-01-01 11:15:00", "44", "45", 3, "3.0", "1.25", "20.00"),
	]
	df = spark.createDataFrame(data, standardized_schema)

	valid_df, _ = validation.validate_data(df, "test")
	vendor_ids = sorted([row.VendorId for row in valid_df.collect()])

	assert vendor_ids == [2, 999]


def test_validate_empty_dataframe(spark, standardized_schema, monkeypatch):
	_mock_writes(monkeypatch)
	validation = DataValidation(spark, "unused")
	df = spark.createDataFrame([], standardized_schema)

	valid_df, invalid_df = validation.validate_data(df, "test")

	assert valid_df.count() == 0
	assert invalid_df.count() == 0