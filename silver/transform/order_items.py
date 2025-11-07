"""This module provides transformation functions for order items DataFrames in the Silver layer."""
import pandas as pd

def transform_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw items data.
    - Renames table to 'order_items'
    - Renames 'id' to 'order_item_id'
    """
    print("Transforming order_items (from raw_items)...")
    df = df.rename(columns={'id': 'order_item_id'})
    return df
