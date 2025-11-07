"""This module provides transformation functions for calculating 
Average Order Value (AOV) by store and month."""
import pandas as pd

def calculate_aov_by_store_month(orders_df, stores_df):
    """
    Calculates the Average Order Value (AOV) by store and by month.
    
    AOV is defined as the mean of 'order_total_cents'.
    """
    print("Transforming: Calculating AOV by store and month...")

    # Ensure orders_df is a copy to avoid SettingWithCopyWarning
    df = orders_df.copy()

    # Convert 'ordered_at' to datetime objects for time-based extraction
    df['ordered_at'] = pd.to_datetime(df['ordered_at'])

    # Extract year and month
    df['year'] = df['ordered_at'].dt.year
    df['month'] = df['ordered_at'].dt.month

    # Group by store, year, month and calculate the mean of 'order_total_cents'
    aov = df.groupby(['store_id', 'year', 'month'])['order_total_cents'].mean().reset_index()

    # Rename column for clarity
    aov = aov.rename(columns={'order_total_cents': 'average_order_value_cents'})

    # Round the AOV to the nearest integer (cent)
    aov['average_order_value_cents'] = aov['average_order_value_cents'].round(0).astype(int)

    # Join with stores_df to add the store_name for user-friendliness
    final_aov = aov.merge(
        stores_df[['store_id', 'name']], 
        on='store_id', 
        how='left'
    )

    # Rename 'name' to 'store_name' and re-order columns
    final_aov = final_aov.rename(columns={'name': 'store_name'})
    final_aov = final_aov[[
        'store_id', 
        'store_name', 
        'year', 
        'month', 
        'average_order_value_cents'
    ]]

    print("AOV transformation complete.")
    return final_aov
