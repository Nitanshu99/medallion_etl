"""Handles DuckDB connection and view creation."""
import os
import sys
import glob
import duckdb

def connect_and_create_views(layer_name: str, data_path: str):
    """
    Validates paths, connects to DuckDB, and creates views for parquet files.
    
    Returns:
        (duckdb.Connection, list[str]): The connection and list of table names.
    """
    # Check if the directory exists
    if not os.path.exists(data_path):
        print(f"Error: Directory not found: '{data_path}'")
        print("Please run the Dagster pipeline first to generate data.")
        sys.exit(1)

    # Find all parquet files in the directory
    search_pattern = os.path.join(data_path, "*.parquet")
    parquet_files = glob.glob(search_pattern)

    if not parquet_files:
        print(f"Error: No .parquet files found in '{data_path}'")
        sys.exit(1)

    # Connect to an in-memory DuckDB database
    con = duckdb.connect(database=':memory:')

    print("--- 🦆 Medallion Query Interface ---")
    print(f"Connected to {layer_name.title()} layer at: {data_path}\n")
    print("Available tables (views):")

    # Create a view for each parquet file
    table_names = []
    for file_path in parquet_files:
        # 'data/gold/aov_by_store_month.parquet' -> 'aov_by_store_month'
        file_name = os.path.basename(file_path)
        table_name = file_name.replace(".parquet", "")
        table_names.append(table_name)

        # Create a view that scans the parquet file
        con.execute(f"""
            CREATE OR REPLACE VIEW "{table_name}" AS
            SELECT * FROM parquet_scan('{file_path}');
        """)
        print(f"  - {table_name}")

    return con, table_names
