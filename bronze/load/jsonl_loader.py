"""This module loads transformed JSONL DataFrames to Parquet files."""
import os
from bronze.transform.jsonl_transformer import transform_jsonl, jsonl_files

# Create parquet directory if it doesn't exist
os.makedirs("data/bronze/parquet", exist_ok=True)

def load_bronze_jsonl(file_name: str) -> None:
    """Load JSONL to Parquet file."""
    df = transform_jsonl(file_name)
    parquet_name = file_name.replace('.jsonl', '.parquet')
    df.to_parquet(f"data/bronze/parquet/{parquet_name}", index=False)
    print(f"Loaded {file_name} to {parquet_name}")

if __name__ == "__main__":
    for file in jsonl_files:
        load_bronze_jsonl(file)
# End-of-file (EOF)
