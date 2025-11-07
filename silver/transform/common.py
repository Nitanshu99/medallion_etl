"""This module provides common transformation functions for Silver layer DataFrames."""
import pandas as pd

def rename_money_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames common monetary columns to *_cents for clarity.
    """
    rename_map = {
        "subtotal": "subtotal_cents",
        "tax_paid": "tax_paid_cents",
        "order_total": "order_total_cents",
        "price": "price_cents"
    }

    # Rename only columns that exist in the DataFrame
    cols_to_rename = {k: v for k, v in rename_map.items() if k in df.columns}
    if cols_to_rename:
        df = df.rename(columns=cols_to_rename)

    return df
