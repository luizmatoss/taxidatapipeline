"""
def test_column_standardization():
    # Assuming we have a DataFrame 'df' from the transformation function
    transformed_df = pipeline.standardize_column_names(green_df, "Green")

    # Assert column names are standardized
    assert "PickUpDateTime" in transformed_df.columns, "PickUpDateTime column missing"
    assert "DropOffDateTime" in transformed_df.columns, "DropOffDateTime column missing"

    # Assert no null values in critical columns
    assert transformed_df.filter(transformed_df['PickUpDateTime'].isNull()).count() == 0, "Null values found in PickUpDateTime"
    
"""
