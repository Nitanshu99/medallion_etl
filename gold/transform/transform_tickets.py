"""This module provides transformation functions for calculating ticket summaries per order."""

def calculate_orders_ticket_summary(orders_df, tickets_df, customers_df, stores_df):
    """
    Creates an enriched order summary table including a count of tickets
    for every order.
    
    Ensures all orders are present, with '0' for ticket_count if none exist.
    """
    print("Transforming: Calculating ticket summary per order...")

    # 1. Aggregate support tickets
    # Filter out tickets that are not linked to an order_id
    valid_tickets = tickets_df[tickets_df['order_id'].notna()].copy()

    # Group by order_id and count the number of tickets
    ticket_counts = valid_tickets.groupby('order_id').size().reset_index(name='ticket_count')

    # 2. Start with orders as the base (left) table
    base_df = orders_df.copy()

    # 3. Left join the ticket_counts to the base orders table
    base_df = base_df.merge(ticket_counts, on='order_id', how='left')

    # 4. Fill NaN values in 'ticket_count' with 0 and convert to integer
    base_df['ticket_count'] = base_df['ticket_count'].fillna(0).astype(int)

    # 5. Join with customers_df to get customer_name
    base_df = base_df.merge(
        customers_df[['customer_id', 'name']],
        on='customer_id',
        how='left'
    ).rename(columns={'name': 'customer_name'})

    # 6. Join with stores_df to get store_name
    base_df = base_df.merge(
        stores_df[['store_id', 'name']],
        on='store_id',
        how='left'
    ).rename(columns={'name': 'store_name'})

    # 7. Select and reorder the final columns for the gold table
    final_cols = [
        'order_id', 
        'ordered_at', 
        'store_id', 
        'store_name', 
        'customer_id', 
        'customer_name', 
        'ticket_count'
    ]

    # Filter base_df to only include the final desired columns
    final_summary = base_df[final_cols]

    print("Ticket summary transformation complete.")
    return final_summary
