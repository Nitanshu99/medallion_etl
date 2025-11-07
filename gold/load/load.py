"""This module contains functions to load data into the Gold layer."""
import os

def save_to_gold(df, filename, gold_path="data/gold"):
    """
    Saves the given DataFrame to a .parquet file in the gold layer directory.
    """
    # Ensure the target directory exists
    os.makedirs(gold_path, exist_ok=True)

    file_path = os.path.join(gold_path, filename)
    df.to_parquet(file_path, index=False, engine='pyarrow')
    print(f"Successfully loaded: {file_path}")
