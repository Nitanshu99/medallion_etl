import pandas as pd
from dagster import asset, AssetKey, AssetIn
from silver.transform.customers import transform_customers
from silver.transform.stores import transform_stores
from silver.transform.products import transform_products
from silver.transform.supplies import transform_supplies
from silver.transform.order_items import transform_order_items
from silver.transform.orders import transform_orders
from silver.transform.support_tickets import transform_support_tickets

# --- FIX: Using 'ins={...}' to explicitly map inputs ---
@asset(
    key=AssetKey(["silver", "customers"]), 
    ins={"bronze_df": AssetIn(key=AssetKey(["bronze", "raw_customers"]))},
    group_name="silver", 
    io_manager_key="silver_io_manager"
)
def silver_customers(bronze_df: pd.DataFrame) -> pd.DataFrame:
    return transform_customers(bronze_df)

@asset(
    key=AssetKey(["silver", "stores"]), 
    ins={"bronze_df": AssetIn(key=AssetKey(["bronze", "raw_stores"]))},
    group_name="silver", 
    io_manager_key="silver_io_manager"
)
def silver_stores(bronze_df: pd.DataFrame) -> pd.DataFrame:
    return transform_stores(bronze_df)

@asset(
    key=AssetKey(["silver", "products"]), 
    ins={"bronze_df": AssetIn(key=AssetKey(["bronze", "raw_products"]))},
    group_name="silver", 
    io_manager_key="silver_io_manager"
)
def silver_products(bronze_df: pd.DataFrame) -> pd.DataFrame:
    return transform_products(bronze_df)

@asset(
    key=AssetKey(["silver", "supplies"]), 
    ins={"bronze_df": AssetIn(key=AssetKey(["bronze", "raw_supplies"]))},
    group_name="silver", 
    io_manager_key="silver_io_manager"
)
def silver_supplies(bronze_df: pd.DataFrame) -> pd.DataFrame:
    return transform_supplies(bronze_df)

@asset(
    key=AssetKey(["silver", "order_items"]), 
    ins={"bronze_df": AssetIn(key=AssetKey(["bronze", "raw_items"]))},
    group_name="silver", 
    io_manager_key="silver_io_manager"
)
def silver_order_items(bronze_df: pd.DataFrame) -> pd.DataFrame:
    return transform_order_items(bronze_df)

@asset(
    key=AssetKey(["silver", "orders"]), 
    ins={"bronze_df": AssetIn(key=AssetKey(["bronze", "raw_orders"]))},
    group_name="silver", 
    io_manager_key="silver_io_manager"
)
def silver_orders(bronze_df: pd.DataFrame) -> pd.DataFrame:
    return transform_orders(bronze_df)

@asset(
    key=AssetKey(["silver", "support_tickets"]), 
    ins={
        "tickets_df": AssetIn(key=AssetKey(["bronze", "support_tickets"])),
        "orders_df": AssetIn(key=AssetKey(["bronze", "raw_orders"]))
    },
    group_name="silver", 
    io_manager_key="silver_io_manager"
)
def silver_support_tickets(tickets_df: pd.DataFrame, orders_df: pd.DataFrame) -> pd.DataFrame:
    return transform_support_tickets(tickets_df, orders_df)

silver_assets = [silver_customers, silver_stores, silver_products, silver_supplies, silver_order_items, silver_orders, silver_support_tickets]
