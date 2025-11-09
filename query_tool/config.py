"""Configuration constants and pandas settings."""
import pandas as pd

# Define the default paths based on your project structure
PATH_CONFIG = {
    "bronze": "data/bronze/parquet",
    "silver": "data/silver",
    "gold": "data/gold"
}

def apply_pandas_options():
    """Sets global pandas display options."""
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_colwidth', None)
