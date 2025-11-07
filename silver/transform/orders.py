"""This module provides transformation functions for orders DataFrames in the Silver layer."""
import pandas as pd
from silver.transform.common import rename_money_cols

def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw orders data.
    - Renames 'id' to 'order_id'
    - Renames 'customer' to 'customer_id'
    - Renames monetary columns
    """
    print("Transforming orders...")
    df = df.rename(columns={'id': 'order_id', 'customer': 'customer_id'})

    # Use the common helper
    df = rename_money_cols(df)

    return df
