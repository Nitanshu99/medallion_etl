"""This module provides transformation functions for support ticket DataFrame in the Silver layer"""
import pandas as pd

def transform_support_tickets(tickets_df: pd.DataFrame, orders_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms support tickets data.
    - Uses 'orders_df' to bridge 'order_id' to the UUID 'customer_id'
    - Flattens the 'sentiment' struct
    - Drops the old 'customer_external_id'
    """
    print("Transforming support_tickets...")

    # We only need a lookup from the raw orders table
    # This maps order_id (UUID) to customer_id (UUID)
    order_customer_lookup = orders_df[['id', 'customer']].rename(
        columns={'id': 'order_id', 'customer': 'customer_id'}
    )

    # Merge to get the UUID customer_id
    # We use 'order_id' from tickets_df and 'order_id' from our lookup
    df = tickets_df.merge(order_customer_lookup, on='order_id', how='left')

    # Flatten the 'sentiment' column (a dict/struct)
    # This is a safe way to extract from dicts that might be null
    df['sentiment_score'] = df['sentiment'].apply(
        lambda x: x.get('score') if isinstance(x, dict) else None
    )
    df['sentiment_model'] = df['sentiment'].apply(
        lambda x: x.get('model') if isinstance(x, dict) else None
    )

    # Drop columns that are now irrelevant or replaced
    df = df.drop(columns=['sentiment', 'customer_external_id'])

    return df
