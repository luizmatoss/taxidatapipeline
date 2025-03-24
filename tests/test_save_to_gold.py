"""

def test_save_to_gold():
    locations_data, vendors_data = pipeline.shape_data_to_gold(transformed_data)
    
    # Check if the gold data files are saved correctly
    assert os.path.exists("/path/to/gold/locations_data.csv"), "Locations data not saved"
    assert os.path.exists("/path/to/gold/vendors_data.csv"), "Vendors data not saved"
    
"""
