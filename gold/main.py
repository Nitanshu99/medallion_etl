"""Main ETL pipeline for the Gold layer."""
import sys

# We assume this script is run from the root of the project (e.g., `python gold/main.py`)
# The root directory is automatically added to sys.path by Python.
from gold.extract.extract import read_silver_data
from gold.transform.transform_aov import calculate_aov_by_store_month
from gold.transform.transform_tickets import calculate_orders_ticket_summary
from gold.load.load import save_to_gold

def main():
    """Executes the main ETL pipeline for the Gold layer."""
    print("--- Starting Gold Layer ETL Pipeline ---")

    # 1. EXTRACT
    # Assumes silver data is in 'data/silver' relative to project root
    silver_data = read_silver_data(silver_path="data/silver")

    if not silver_data:
        print("ETL Pipeline FAILED: No data extracted from silver layer.", file=sys.stderr)
        return

    # Verify all necessary tables were loaded
    required_tables = ['orders', 'stores', 'support_tickets', 'customers']
    if not all(table in silver_data for table in required_tables):
        print("ETL Pipeline FAILED: Missing required data.", file=sys.stderr)
        print(f"Required: {required_tables}", file=sys.stderr)
        print(f"Found: {list(silver_data.keys())}", file=sys.stderr)
        return

    # 2. TRANSFORM

    # Objective 1: Calculate AOV
    aov_table = calculate_aov_by_store_month(
        silver_data['orders'], 
        silver_data['stores']
    )

    # Objective 2: Calculate Ticket Summary
    ticket_summary_table = calculate_orders_ticket_summary(
        silver_data['orders'], 
        silver_data['support_tickets'], 
        silver_data['customers'],
        silver_data['stores']
    )

    # 3. LOAD
    # Assumes gold data will be loaded to 'data/gold'
    if aov_table is not None:
        save_to_gold(aov_table, "aov_by_store_month.parquet")
    else:
        print("Skipping AOV load: transform function returned None.")

    if ticket_summary_table is not None:
        save_to_gold(ticket_summary_table, "orders_ticket_summary.parquet")
    else:
        print("Skipping Ticket Summary load: transform function returned None.")

    print("--- Gold Layer ETL Pipeline Finished ---")

if __name__ == "__main__":
    main()
