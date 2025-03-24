"""
import time

def test_pipeline_performance():
    start_time = time.time()
    
    # Run the entire pipeline (or key sections like ingestion and transformation)
    green_df, yellow_df = pipeline.import_data()
    transformed_data = pipeline.transform_data(green_df, yellow_df)
    
    # Record the time taken
    elapsed_time = time.time() - start_time
    print(f"Pipeline execution time: {elapsed_time:.2f} seconds")
    
    # Assert that the pipeline completes within a reasonable time
    assert elapsed_time < 60, "Pipeline took too long to execute"
    
"""
