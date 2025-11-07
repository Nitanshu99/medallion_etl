"""This module contains functions to extract data from the Silver layer."""
import glob
import os
import sys
import pandas as pd

def read_silver_data(silver_path="data/silver"):
    """
    Reads all .parquet files from the specified silver layer directory
    and returns them in a dictionary of DataFrames.
    """
    print(f"Extracting data from {silver_path}...")
    search_path = os.path.join(silver_path, "*.parquet")
    files = glob.glob(search_path)

    if not files:
        print(f"Error: No .parquet files found in {silver_path}", file=sys.stderr)
        return None

    dataframes = {}
    for file_path in files:
        # Get filename without extension to use as dict key
        file_name = os.path.basename(file_path).replace(".parquet", "")
        dataframes[file_name] = pd.read_parquet(file_path)

    if dataframes:
        print(f"Successfully extracted: {list(dataframes.keys())}")

    return dataframes
