""" --- BRONZE ASSETS (Fixed) ---"""
import os
from pathlib import Path
import pandas as pd
from dagster import asset, AssetKey
from bronze.extract.csv_extractor import fetch_files as fetch_csv_files
from bronze.extract.jsonl_extractor import download_azure_jsonl
from .resources import PathConfig, AzureConfig

@asset(group_name="bronze_extract", compute_kind="http")
def raw_csv_files(context, paths: PathConfig) -> None:
    """Runs the 'csv_extractor' to download files to 'data/bronze/raw/local'."""
    # pylint: disable-next=C0415
    import bronze.extract.csv_extractor

    # FIX: Convert the 'str' from config into a 'Path' object
    output_dir_as_path = Path(paths.raw_local_path)
    bronze.extract.csv_extractor.OUTPUT_DIR = output_dir_as_path

    context.log.info(f"Fetching CSVs to {output_dir_as_path}...")
    fetch_csv_files(bronze.extract.csv_extractor.API_URL)

@asset(group_name="bronze_extract", compute_kind="azure")
def raw_jsonl_files(context, paths: PathConfig, azure: AzureConfig) -> None:
    """Runs the 'jsonl_extractor' to download files to 'data/bronze/raw/azure'."""
    context.log.info(f"Fetching JSONL files to {paths.raw_azure_path}...")
    download_azure_jsonl(url=azure.sas_url, local_directory=paths.raw_azure_path)

# --- BRONZE LOAD LAYER ---
@asset(key=AssetKey(["bronze", "raw_customers"]), group_name="bronze",
       deps=[raw_csv_files], io_manager_key="bronze_io_manager")
def bronze_raw_customers(paths: PathConfig) -> pd.DataFrame:
    """Loads raw_customers CSV into a DataFrame."""
    file_path = os.path.join(paths.raw_local_path, "raw_customers.csv")
    return pd.read_csv(file_path)

@asset(key=AssetKey(["bronze", "raw_stores"]), group_name="bronze",
       deps=[raw_csv_files], io_manager_key="bronze_io_manager")
def bronze_raw_stores(paths: PathConfig) -> pd.DataFrame:
    """Loads raw_stores CSV into a DataFrame."""
    file_path = os.path.join(paths.raw_local_path, "raw_stores.csv")
    return pd.read_csv(file_path)

@asset(key=AssetKey(["bronze", "raw_products"]), group_name="bronze",
       deps=[raw_csv_files], io_manager_key="bronze_io_manager")
def bronze_raw_products(paths: PathConfig) -> pd.DataFrame:
    """Loads raw_products CSV into a DataFrame."""
    file_path = os.path.join(paths.raw_local_path, "raw_products.csv")
    return pd.read_csv(file_path)

@asset(key=AssetKey(["bronze", "raw_supplies"]), group_name="bronze",
       deps=[raw_csv_files], io_manager_key="bronze_io_manager")
def bronze_raw_supplies(paths: PathConfig) -> pd.DataFrame:
    """Loads raw_supplies CSV into a DataFrame."""
    file_path = os.path.join(paths.raw_local_path, "raw_supplies.csv")
    return pd.read_csv(file_path)

@asset(key=AssetKey(["bronze", "raw_items"]), group_name="bronze",
       deps=[raw_csv_files], io_manager_key="bronze_io_manager")
def bronze_raw_items(paths: PathConfig) -> pd.DataFrame:
    """Loads raw_items CSV into a DataFrame."""
    file_path = os.path.join(paths.raw_local_path, "raw_items.csv")
    return pd.read_csv(file_path)

@asset(key=AssetKey(["bronze", "raw_orders"]), group_name="bronze",
       deps=[raw_csv_files], io_manager_key="bronze_io_manager")
def bronze_raw_orders(paths: PathConfig) -> pd.DataFrame:
    """Loads raw_orders CSV into a DataFrame."""
    file_path = os.path.join(paths.raw_local_path, "raw_orders.csv")
    return pd.read_csv(file_path)

@asset(key=AssetKey(["bronze", "support_tickets"]), group_name="bronze",
        deps=[raw_jsonl_files], io_manager_key="bronze_io_manager")
def bronze_support_tickets(paths: PathConfig) -> pd.DataFrame:
    """Loads support_tickets JSONL into a DataFrame."""
    file_path = os.path.join(paths.raw_azure_path, "support_tickets.jsonl")
    return pd.read_json(file_path, lines=True)

bronze_assets = [raw_csv_files, raw_jsonl_files, bronze_raw_customers,
                bronze_raw_stores, bronze_raw_products, bronze_raw_supplies,
                bronze_raw_items, bronze_raw_orders, bronze_support_tickets]
