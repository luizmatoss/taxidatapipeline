"""

def test_data_validation():
    valid_data_df, invalid_data_df = pipeline.validate_data(transformed_data, "combined")
    
    # Validate that no valid records have zero passenger count
    assert valid_data_df.filter(valid_data_df["PassengerCount"] > 0).count() == valid_data_df.count(), "Invalid passenger count in valid data"
    
    # Assert that invalid data contains only invalid records
    assert invalid_data_df.count() > 0, "No invalid records found
    
""""
