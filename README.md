# Taxi Data Pipeline

## Overview
This project implements a data pipeline for processing NYC taxi trip data. The pipeline follows the **Medallion Architecture** (Bronze, Silver, and Gold stages) to process raw data, transform it, validate it, and create aggregated reports. The pipeline is implemented in **Python** and leverages **PySpark** for large-scale data processing.

## Features
- **Data Ingestion**: Load raw trip data from CSV or Parquet files into the pipeline.
- **Data Transformation**: Standardize column names and ensure a consistent schema across datasets.
- **Data Validation**: Validate the dataset using predefined rules (e.g., ensuring vendor IDs and passenger counts are present).
- **Data Deduplication**: Remove duplicate records from the dataset.
- **Data Aggregation**: Create summary reports such as total fares, tips, and average trip distances.

## Folder Structure
This project follows a **Medallion Architecture** with the following directory structure:
- `data/bronze/`: Contains raw source files (for example `green_tripdata_2026-04.parquet`, `yellow_tripdata_2026-04.parquet`, or matching CSV files).
- `data/silver/`: Contains the transformed data with standardized columns.
- `data/gold/`: Contains the final aggregated datasets used for reporting.
- `pipeline_engine/`: The folder containing the main pipeline code, including data ingestion, transformation, validation, and deduplication functions.
- `tests/`: Contains unit tests for the pipeline's functionality.

## Supported Input Naming
The ingestion stage auto-discovers files in `data/bronze/` using these patterns:
- `green_tripdata_*.parquet` or `green_tripdata_*.csv`
- `yellow_tripdata_*.parquet` or `yellow_tripdata_*.csv`

If `green_tripdata_2021_01.*` or `yellow_tripdata_2021_01.*` is present it is used first; otherwise the latest matching file is selected.

## Prerequisites
- Python 3.10 or higher
- Java 8+ or Java 11+ (required by Spark)

Install required dependencies:

```bash
pip install -r requirements.txt
```

## Setup and Running the Pipeline

### 1. Prepare the Data
Download NYC taxi data and place files in `data/bronze/`.

Example accepted files:
- `data/bronze/green_tripdata_2026-01.parquet`
- `data/bronze/yellow_tripdata_2026-01.parquet`

### 2. Run the Pipeline
To execute the entire pipeline:

```bash
python main.py
```

By default, the pipeline uses:
- `data/bronze` for input
- `data/silver` for validated/deduplicated outputs
- `data/gold` for aggregated outputs

You can override paths with environment variables:
- `BRONZE_PATH`
- `SILVER_PATH`
- `GOLD_PATH`

### 3. Run the Tests

```bash
pytest tests/
```

Or run all tests:

```bash
pytest -q
```

## Inspecting Results

After a successful run, inspect outputs in:
- `data/silver/valid_combined_data.parquet`
- `data/silver/invalid_combined_data.csv`
- `data/silver/deduped_combined_data.parquet`
- `data/gold/locations_data.csv`
- `data/gold/vendors_data.csv`

## GitHub Actions

This repository uses GitHub Actions to automatically run tests on code pushes and pull requests. The action runs the tests in the tests/ directory, helping to ensure code quality and correctness.

## Contribution Guidelines

Contributions are welcome! Feel free to fork the repository and submit pull requests. When contributing, please:

- Ensure the code is well-documented.
- Write tests for new features or bug fixes.
- Follow the existing code style and structure.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
