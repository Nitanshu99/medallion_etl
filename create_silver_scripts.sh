#!/bin/bash
# This script generates the Python files for the Silver ETL layer.
# It creates the directory structure and populates the modules
# with one function per file, as requested.

set -e

echo "--- Creating Silver Layer Directories ---"
mkdir -p silver/transform
mkdir -p silver/load
mkdir -p data/silver # Ensure output directory exists

# Create __init__.py files to make directories Python modules
touch silver/__init__.py
touch silver/transform/__init__.py
touch silver/load/__init__.py

echo "--- Creating silver/load/saver.py ---"
cat <<'EOF' > silver/load/saver.py
import pandas as pd
import os

# Define the output path
SILVER_PATH = 'data/silver'

def save_to_silver(df: pd.DataFrame, table_name: str):
    """
    Saves a DataFrame to the Silver layer in Parquet format.
    """
    # Ensure the silver directory exists
    os.makedirs(SILVER_PATH, exist_ok=True)
    
    output_file = os.path.join(SILVER_PATH, f"{table_name}.parquet")
    
    print(f"Saving {table_name} to {output_file}...")
    df.to_parquet(output_file, index=False)
    print(f"Successfully saved {table_name}.")

EOF

echo "--- Creating silver/transform/common.py ---"
cat <<'EOF' > silver/transform/common.py
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
EOF

echo "--- Creating silver/transform/customers.py ---"
cat <<'EOF' > silver/transform/customers.py
import pandas as pd

def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw customer data.
    - Renames 'id' to 'customer_id'
    """
    print("Transforming customers...")
    df = df.rename(columns={'id': 'customer_id'})
    return df
EOF

echo "--- Creating silver/transform/stores.py ---"
cat <<'EOF' > silver/transform/stores.py
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
EOF

echo "--- Creating silver/transform/products.py ---"
cat <<'EOF' > silver/transform/products.py
import pandas as pd
from silver.transform.common import rename_money_cols

def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw product data.
    - Renames 'price' to 'price_cents'
    """
    print("Transforming products...")
    # We use the common helper for renaming
    df = rename_money_cols(df)
    return df
EOF

echo "--- Creating silver/transform/supplies.py ---"
cat <<'EOF' > silver/transform/supplies.py
import pandas as pd

def transform_supplies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw supply data.
    - Renames 'id' to 'supply_id'
    """
    print("Transforming supplies...")
    df = df.rename(columns={'id': 'supply_id'})
    return df
EOF

echo "--- Creating silver/transform/order_items.py ---"
cat <<'EOF' > silver/transform/order_items.py
import pandas as pd

def transform_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw items data.
    - Renames table to 'order_items'
    - Renames 'id' to 'order_item_id'
    """
    print("Transforming order_items (from raw_items)...")
    df = df.rename(columns={'id': 'order_item_id'})
    return df
EOF

echo "--- Creating silver/transform/orders.py ---"
cat <<'EOF' > silver/transform/orders.py
import pandas as pd
from silver.transform.common import rename_money_cols

def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw orders data.
    - Renames 'id' to 'order_id'
    - Renames 'customer' to 'customer_id'
    - Renames monetary columns
    """
    print("Transforming orders...")
    df = df.rename(columns={'id': 'order_id', 'customer': 'customer_id'})
    
    # Use the common helper
    df = rename_money_cols(df)
    
    return df
EOF

echo "--- Creating silver/transform/support_tickets.py ---"
cat <<'EOF' > silver/transform/support_tickets.py
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
    df['sentiment_score'] = df['sentiment'].apply(lambda x: x.get('score') if isinstance(x, dict) else None)
    df['sentiment_model'] = df['sentiment'].apply(lambda x: x.get('model') if isinstance(x, dict) else None)
    
    # Drop columns that are now irrelevant or replaced
    df = df.drop(columns=['sentiment', 'customer_external_id'])
    
    return df
EOF

echo "--- Creating silver/run_etl.py (Main Orchestrator) ---"
cat <<'EOF' > silver/run_etl.py
import pandas as pd
import os
import glob

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
EOF

echo "--- All scripts generated successfully! ---"
echo "To run the ETL, you will first need to install pandas and pyarrow:"
echo "  pip install pandas pyarrow"
echo "Then, you can run the main ETL script:"
echo "  python silver/run_etl.py"