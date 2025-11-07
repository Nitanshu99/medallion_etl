"""This module provides transformation functions for product DataFrames in the Silver layer."""
import pandas as pd
from silver.transform.common import rename_money_cols

def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw product data.
    - Renames 'price' to 'price_cents'
    """
    print("Transforming products...")
    # We use the common helper for renaming
    df = rename_money_cols(df)
    return df
