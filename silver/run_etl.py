"""Orchestrates the Bronze-to-Silver ETL process."""
import os
import pandas as pd

# Import all our transformation functions
from silver.transform.customers import transform_customers
from silver.transform.stores import transform_stores
from silver.transform.products import transform_products
from silver.transform.supplies import transform_supplies
from silver.transform.order_items import transform_order_items
from silver.transform.orders import transform_orders
from silver.transform.support_tickets import transform_support_tickets

# Import our saver function
from silver.load.saver import save_to_silver

BRONZE_PATH = 'data/bronze/parquet'

def main():
    """
    Main ETL orchestration function.
    Reads all bronze data, transforms it, and saves it to silver.
    """
    print("--- Starting Bronze-to-Silver ETL ---")

    # 1. Load all Bronze data into memory
    print("Loading Bronze data...")
    try:
        raw_customers_df = pd.read_parquet(os.path.join(BRONZE_PATH, 'raw_customers.parquet'))
        raw_stores_df = pd.read_parquet(os.path.join(BRONZE_PATH, 'raw_stores.parquet'))
        raw_products_df = pd.read_parquet(os.path.join(BRONZE_PATH, 'raw_products.parquet'))
        raw_supplies_df = pd.read_parquet(os.path.join(BRONZE_PATH, 'raw_supplies.parquet'))
        raw_items_df = pd.read_parquet(os.path.join(BRONZE_PATH, 'raw_items.parquet'))
        raw_orders_df = pd.read_parquet(os.path.join(BRONZE_PATH, 'raw_orders.parquet'))
        support_tickets_df = pd.read_parquet(os.path.join(BRONZE_PATH, 'support_tickets.parquet'))
    except FileNotFoundError as e:
        print(f"Error: Missing bronze file - {e}")
        print("Please ensure all raw parquet files are in data/bronze/parquet/")
        return

    # 2. Run transformations
    print("Running transformations...")

    # Simple, independent transformations
    customers_silver = transform_customers(raw_customers_df)
    stores_silver = transform_stores(raw_stores_df)
    products_silver = transform_products(raw_products_df)
    supplies_silver = transform_supplies(raw_supplies_df)
    order_items_silver = transform_order_items(raw_items_df)
    orders_silver = transform_orders(raw_orders_df)

    # Complex transformation (with dependency)
    # We pass the *raw* orders_df as a lookup table
    support_tickets_silver = transform_support_tickets(support_tickets_df, raw_orders_df)

    # 3. Load data into Silver layer
    print("Saving to Silver layer...")

    save_to_silver(customers_silver, 'customers')
    save_to_silver(stores_silver, 'stores')
    save_to_silver(products_silver, 'products')
    save_to_silver(supplies_silver, 'supplies')
    save_to_silver(order_items_silver, 'order_items')
    save_to_silver(orders_silver, 'orders')
    save_to_silver(support_tickets_silver, 'support_tickets')

    print("--- Bronze-to-Silver ETL Complete ---")

if __name__ == "__main__":
    main()
