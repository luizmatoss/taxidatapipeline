from pyspark.sql.readwriter import DataFrameWriter

from pipeline_engine.data_deduplication import DataDeduplication


def _mock_parquet(monkeypatch):
    def fake_parquet(self, path):
        return None

    monkeypatch.setattr(DataFrameWriter, "parquet", fake_parquet)


def test_dedupe_removes_key_duplicates(spark, standardized_schema, monkeypatch):
    _mock_parquet(monkeypatch)
    dedup = DataDeduplication(spark, "unused")
    data = [
        (
            1,
            "2021-01-01 10:00:00",
            "2021-01-01 10:15:00",
            "42",
            "43",
            1,
            "2.5",
            "0.50",
            "15.25",
        ),
        (
            1,
            "2021-01-01 10:00:00",
            "2021-01-01 10:15:00",
            "42",
            "43",
            2,
            "2.5",
            "9.99",
            "99.99",
        ),
        (
            2,
            "2021-01-01 11:00:00",
            "2021-01-01 11:15:00",
            "44",
            "45",
            1,
            "3.0",
            "1.00",
            "20.00",
        ),
    ]
    df = spark.createDataFrame(data, standardized_schema)

    result = dedup.dedupe_data(df, "test")

    assert result.count() == 2


def test_dedupe_keeps_non_duplicate_rows(spark, standardized_schema, monkeypatch):
    _mock_parquet(monkeypatch)
    dedup = DataDeduplication(spark, "unused")
    data = [
        (
            1,
            "2021-01-01 10:00:00",
            "2021-01-01 10:15:00",
            "42",
            "43",
            1,
            "2.5",
            "0.50",
            "15.25",
        ),
        (
            1,
            "2021-01-01 10:00:00",
            "2021-01-01 10:15:00",
            "99",
            "43",
            1,
            "2.5",
            "0.50",
            "15.25",
        ),
    ]
    df = spark.createDataFrame(data, standardized_schema)

    result = dedup.dedupe_data(df, "test")

    assert result.count() == 2


def test_dedupe_empty_dataframe(spark, standardized_schema, monkeypatch):
    _mock_parquet(monkeypatch)
    dedup = DataDeduplication(spark, "unused")
    df = spark.createDataFrame([], standardized_schema)

    result = dedup.dedupe_data(df, "test")

    assert result.count() == 0
