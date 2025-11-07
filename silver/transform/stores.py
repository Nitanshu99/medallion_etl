"""This module provides transformation functions for store DataFrames in the Silver layer."""
import pandas as pd

def transform_stores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw store data.
    - Renames 'id' to 'store_id'
    - Casts 'opened_at' from timestamp to date
    """
    print("Transforming stores...")
    df = df.rename(columns={'id': 'store_id'})

    # Convert to datetime first (robust) then keep only the date
    df['opened_at'] = pd.to_datetime(df['opened_at']).dt.date

    return df
