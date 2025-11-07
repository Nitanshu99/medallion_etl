"""This module provides functionality to transform JSONL files to DataFrames."""
import os
from pathlib import Path
import pandas as pd

# Get the project root
project_root = Path(__file__).resolve().parents[2]
os.chdir(project_root)

# List all JSONL files in data/bronze/raw/azure
jsonl_files = [f for f in os.listdir("data/bronze/raw/azure") if f.endswith('.jsonl')]
globals()['jsonl_files'] = jsonl_files

def transform_jsonl(file_name: str) -> pd.DataFrame:
    """
    Transform a JSONL file to a DataFrame.

    Args:
        file_name (str): The name of the JSONL file.

    Returns:
        pd.DataFrame: The transformed data as a DataFrame.
    """
    df_name = file_name.replace('.jsonl', '')
    df_internal = pd.read_json(f"data/bronze/raw/azure/{file_name}", lines=True)
    globals()[f"df_{df_name}"] = df_internal
    return df_internal

# Execute transformation for all JSONL files found
if __name__ == "__main__":
    print(f"JSONL files found: {jsonl_files}")
    for file in jsonl_files:
        df = transform_jsonl(file)
        print(f"\nLoaded {file}:")
        print(df.head())
# End-of-file (EOF)
