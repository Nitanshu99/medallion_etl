"""This module provides functions to save DataFrames to the Silver layer."""
import os
import pandas as pd

# Define the output path
SILVER_PATH = 'data/silver'

def save_to_silver(df: pd.DataFrame, table_name: str):
    """
    Saves a DataFrame to the Silver layer in Parquet format.
    """
    # Ensure the silver directory exists
    os.makedirs(SILVER_PATH, exist_ok=True)

    output_file = os.path.join(SILVER_PATH, f"{table_name}.parquet")

    print(f"Saving {table_name} to {output_file}...")
    df.to_parquet(output_file, index=False)
    print(f"Successfully saved {table_name}.")
# End of file
