import pandas as pd
from dagster import asset, AssetKey, AssetIn
from gold.transform.transform_aov import calculate_aov_by_store_month
from gold.transform.transform_tickets import calculate_orders_ticket_summary

@asset(
    key=AssetKey(["gold", "aov_by_store_month"]),
    ins={
        "in_orders": AssetIn(key=AssetKey(["silver", "orders"])),
        "in_stores": AssetIn(key=AssetKey(["silver", "stores"]))
    },
    group_name="gold", 
    io_manager_key="gold_io_manager"
)
def gold_aov_by_store_month(in_orders: pd.DataFrame, in_stores: pd.DataFrame) -> pd.DataFrame:
    return calculate_aov_by_store_month(in_orders, in_stores)

@asset(
    key=AssetKey(["gold", "orders_ticket_summary"]),
    ins={
        "in_orders": AssetIn(key=AssetKey(["silver", "orders"])),
        "in_tickets": AssetIn(key=AssetKey(["silver", "support_tickets"])),
        "in_customers": AssetIn(key=AssetKey(["silver", "customers"])),
        "in_stores": AssetIn(key=AssetKey(["silver", "stores"]))
    },
    group_name="gold", 
    io_manager_key="gold_io_manager"
)
def gold_orders_ticket_summary(
    in_orders: pd.DataFrame,
    in_tickets: pd.DataFrame,
    in_customers: pd.DataFrame,
    in_stores: pd.DataFrame
) -> pd.DataFrame:
    return calculate_orders_ticket_summary(
        in_orders,
        in_tickets,
        in_customers,
        in_stores
    )

gold_assets = [gold_aov_by_store_month, gold_orders_ticket_summary]
