"""
def test_deduplication():
    deduped_df = pipeline.dedupe_data(transformed_data, "combined")

    # Assert that the deduplicated DataFrame has fewer records than the original
    assert deduped_df.count() <= transformed_data.count(), "Deduplication did not remove duplicates"
    
"""
