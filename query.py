"""
Interactive SQL query REPL for Medallion ETL parquet files.

This script is the main entry point.
The core logic is in the 'query_tool' package.
"""
import argparse

# Import the logic from our new package
from query_tool.config import PATH_CONFIG, apply_pandas_options
from query_tool.repl import start_query_repl

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

    # Apply the pandas settings
    apply_pandas_options()

    # Determine which layer was selected
    if args.bronze:
        start_query_repl("bronze", PATH_CONFIG["bronze"])
    elif args.silver:
        start_query_repl("silver", PATH_CONFIG["silver"])
    elif args.gold:
        start_query_repl("gold", PATH_CONFIG["gold"])

if __name__ == "__main__":
    main()
