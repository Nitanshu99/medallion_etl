"""This module loads extracted DataFrames to Parquet files."""
import os
from bronze.transform.csv_transformer import transform_csv, csv_files

# Create parquet directory if it doesn't exist
os.makedirs("data/bronze/parquet", exist_ok=True)

def load_bronze_csv(file_name: str) -> None:
    """Load CSV to Parquet file."""
    df = transform_csv(file_name)
    parquet_name = file_name.replace('.csv', '.parquet')
    df.to_parquet(f"data/bronze/parquet/{parquet_name}", index=False)
    print(f"Loaded {file_name} to {parquet_name}")

if __name__ == "__main__":
    for file in csv_files:
        load_bronze_csv(file)
# End-of-file (EOF)
