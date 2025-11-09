"""Interactive SQL query REPL for Medallion ETL parquet files."""
import argparse
import os
import sys
import glob
import pandas as pd
import duckdb

# Pandas config is no longer needed, but we'll keep it
# in case a query *is* narrow enough.
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000) # Set a large width
pd.set_option('display.max_colwidth', None)


# Define the default paths based on your project structure
#
PATH_CONFIG = {
    "bronze": "data/bronze/parquet",
    "silver": "data/silver",
    "gold": "data/gold"
}

def start_query_repl(layer_name: str, data_path: str):
    """
    Starts an interactive Read-Eval-Print Loop (REPL) for querying
    parquet files in the specified directory.
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

    print("\n---")
    print("Type your SQL query and press Enter.")
    if table_names:
        print(f"e.g., 'SELECT * FROM {table_names[0]} LIMIT 5;'")
    print("Type 'q' or 'exit' to quit.")
    print("Type '.vertical' or '.horizontal' to change display (Default: VERTICAL).")
    print("---\n")

    # Start the query loop
    vertical_mode = True # Default to readable vertical mode

    while True:
        try:
            query = input("sql> ").strip()

            if query.lower() in ['q', 'exit', '.exit']:
                break

            if query.lower() == '.vertical':
                vertical_mode = True
                print("Display mode set to: VERTICAL")
                continue

            if query.lower() == '.horizontal':
                vertical_mode = False
                print("Display mode set to: HORIZONTAL")
                continue

            if not query:
                continue

            # Execute the query
            result = con.sql(query)

            # If it's a SELECT statement, print the result
            if result:
                df = result.df()
                if df.empty:
                    print("(No results)")
                    continue

                if vertical_mode:
                    # --- VERTICAL MODE ---
                    # Print each row one by one, like a form
                    total_rows = len(df)
                    for row_num, (_, row) in enumerate(df.iterrows(), 1):
                        print(f"---[ Row {row_num} / {total_rows} ]---")
                        # A pandas Series.to_string() is always readable
                        print(row.to_string())
                    print(f"---[ End of {total_rows} rows ]---")
                else:
                    # --- HORIZONTAL MODE ---
                    # The original method, for tables you know are small
                    print(df.to_string(index=False))

        except duckdb.Error as e:
            print(f"DuckDB Error: {e}")
        except Exception as e: # pylint: disable=broad-except
            print(f"An error occurred: {e}")

    print("\nClosing connection. Goodbye!")
    con.close()

def main():
    """Main function to parse arguments and start the REPL."""
    parser = argparse.ArgumentParser(
        description="""
        Interactively query parquet files from the Medallion ETL pipeline.
        
        Example:
        python query.py --gold
        """
    )
    # Create a mutually exclusive group, so only one layer can be chosen
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--bronze', 
        action='store_true',
        help="Query the Bronze layer parquet files."
    )
    group.add_argument(
        '--silver', 
        action='store_true',
        help="Query the Silver layer parquet files."
    )
    group.add_argument(
        '--gold', 
        action='store_true',
        help="Query the Gold layer parquet files."
    )

    args = parser.parse_args()

    # Determine which layer was selected
    if args.bronze:
        start_query_repl("bronze", PATH_CONFIG["bronze"])
    elif args.silver:
        start_query_repl("silver", PATH_CONFIG["silver"])
    elif args.gold:
        start_query_repl("gold", PATH_CONFIG["gold"])

if __name__ == "__main__":
    main()
# End of file query.py
