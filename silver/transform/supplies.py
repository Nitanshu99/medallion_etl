"""This module provides transformation functions for supply DataFrames in the Silver layer."""
import pandas as pd

def transform_supplies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw supply data.
    - Renames 'id' to 'supply_id'
    """
    print("Transforming supplies...")
    df = df.rename(columns={'id': 'supply_id'})
    return df
