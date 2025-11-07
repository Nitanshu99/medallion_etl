"""This module provides transformation functions for customer DataFrames in the Silver layer."""
import pandas as pd

def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw customer data.
    - Renames 'id' to 'customer_id'
    """
    print("Transforming customers...")
    df = df.rename(columns={'id': 'customer_id'})
    return df
