"""This module provides functionality to transform CSV files to DataFrames."""
import os
from pathlib import Path
import pandas as pd

# Get the project root (medallion_etl)
project_root = Path(__file__).resolve().parents[2]
os.chdir(project_root)

# Reads "data/bronze/raw/" directory for CSV files and stores the names in a list.
# That list is now a global variable.
csv_files = [f for f in os.listdir("data/bronze/raw/local") if f.endswith('.csv')]
globals()['csv_files'] = csv_files

def transform_csv(file_name: str) -> pd.DataFrame:
    """
    Transform a CSV file to a DataFrame.

    Args:
        file_name (str): The name of the CSV file.

    Returns:
        pd.DataFrame: The transformed data as a DataFrame.
    """
    df_name = file_name.replace('.csv', '')
    df_internal = pd.read_csv(f"data/bronze/raw/local/{file_name}")
    globals()[f"df_{df_name}"] = df_internal
    return df_internal

# Execute transformation for all CSV files found
if __name__ == "__main__":
    print(f"CSV files found: {csv_files}")
    for file in csv_files:
        df = transform_csv(file)
        print(f"\nLoaded {file}:")
        print(df.head())
