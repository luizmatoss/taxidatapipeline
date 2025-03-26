# Taxi Data Pipeline

## Overview
This project implements a data pipeline for processing NYC taxi trip data. The pipeline follows the **Medallion Architecture** (Bronze, Silver, and Gold stages) to process raw data, transform it, validate it, and create aggregated reports. The pipeline is implemented in **Python** and leverages **PySpark** for large-scale data processing.

## Features
- **Data Ingestion**: Load raw trip data from CSV files into the pipeline.
- **Data Transformation**: Standardize column names and ensure a consistent schema across datasets.
- **Data Validation**: Validate the dataset using predefined rules (e.g., ensuring vendor IDs and passenger counts are present).
- **Data Deduplication**: Remove duplicate records from the dataset.
- **Data Aggregation**: Create summary reports such as total fares, tips, and average trip distances.

## Folder Structure
This project follows a **Medallion Architecture** with the following directory structure:
- `data/bronze/`: Contains the raw data files (e.g., `green_tripdata_2021_01.csv`, `yellow_tripdata_2021_01.csv`).
- `data/silver/`: Contains the transformed data with standardized columns.
- `data/gold/`: Contains the final aggregated datasets used for reporting.
- `pipeline_engine/`: The folder containing the main pipeline code, including data ingestion, transformation, validation, and deduplication functions.
- `tests/`: Contains unit tests for the pipeline's functionality.
- Download the raw data from the NYC Open Data Portal

## Prerequisites
- Python 3.10 or higher
- Install required dependencies:
  ```bash
  pip install -r requirements.txt
  
Setup and Running the Pipeline

Prepare the Data:
Download the raw data from the NYC Open Data Portal and place it in the data/bronze/ directory.

Run the Pipeline:
To execute the entire pipeline, run the following command:
python main.py

Running the Tests:
To run the tests and ensure everything is working correctly:
pytest tests/

GitHub Actions

This repository uses GitHub Actions to automatically run tests on code pushes and pull requests. The action runs the tests in the tests/ directory, helping to ensure code quality and correctness.

Contribution Guidelines

Contributions are welcome! Feel free to fork the repository and submit pull requests. When contributing, please:

Ensure the code is well-documented.
Write tests for new features or bug fixes.
Follow the existing code style and structure.
License

This project is licensed under the MIT License. See the LICENSE file for details.
