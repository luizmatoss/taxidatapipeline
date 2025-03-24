import os
import sys
import pytest
from dotenv import load_dotenv  # Import dotenv to load environment variables

# Load environment variables from .env file
load_dotenv()

# Get the root directory from the environment variable
root_dir = os.getenv('ROOT_DIR')
if root_dir:
    sys.path.append(root_dir)  # Add the root directory to sys.path
else:
    raise EnvironmentError("ROOT_DIR environment variable is not set in the .env file.")

# Import DataIngestion after updating sys.path
from pipeline_engine.data_ingestion import DataIngestion  # Ensure this import is necessary

def test_file_paths():
    # Get paths from environment variables
    bronze_path = os.getenv('BRONZE_PATH')
    silver_path = os.getenv('SILVER_PATH')
    gold_path = os.getenv('GOLD_PATH')

    # Example: Check if the bronze directory exists
    assert os.path.exists(bronze_path), f"Bronze directory is missing: {bronze_path}"

    # Example: Check if the silver directory exists
    assert os.path.exists(silver_path), f"Silver directory is missing: {silver_path}"

    # Example: Check if the gold directory exists
    assert os.path.exists(gold_path), f"Gold directory is missing: {gold_path}"

    # Example: Check if specific files exist in the bronze directory
    green_data_path = os.path.join(bronze_path, 'green_tripdata_2021_01.csv')
    yellow_data_path = os.path.join(bronze_path, 'yellow_tripdata_2021_01.csv')

    assert os.path.exists(green_data_path), f"Green data file is missing: {green_data_path}"
    assert os.path.exists(yellow_data_path), f"Yellow data file is missing: {yellow_data_path}"