"""
import pytest
from pipeline_engine.data_aggregation import DataAggregation  # Replace with the actual module name

def test_aggregation():
    # Mock or create sample transformed_data for testing
    transformed_data = [
        {"VendorID": 1, "TotalFare": 10.5, "LocationID": 101},
        {"VendorID": 2, "TotalFare": 15.0, "LocationID": 102},
        {"VendorID": 1, "TotalFare": 20.0, "LocationID": 101},
    ]

    # Call the function to be tested
    locations_data, vendors_data = pipeline.shape_data_to_gold(transformed_data)

    # Verify the shape of the aggregated data
    assert locations_data.count() > 0, "Locations data is empty"
    assert vendors_data.count() > 0, "Vendors data is empty"

    # Verify that specific fields are correctly aggregated
    assert "TotalFare" in locations_data.columns, "TotalFare column missing in locations data"
    assert "TotalFare" in vendors_data.columns, "TotalFare column missing in vendors data"
    
    """