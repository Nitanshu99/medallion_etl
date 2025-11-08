"""
This is the main entry point for Dagster.
It brings together all the assets, resources, and schedules.
"""
from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection
)

# --- Import asset lists from our new files ---
from medallion_dagster.bronze import bronze_assets
from medallion_dagster.silver import silver_assets
from medallion_dagster.gold import gold_assets

# Import our resources
from medallion_dagster.resources import (
    PathConfig, 
    AzureConfig, 
    parquet_io_manager
)

# Combine all assets
all_assets = [*bronze_assets, *silver_assets, *gold_assets]

# --- 1. Define Jobs ---
all_assets_job = define_asset_job(
    name="full_medallion_pipeline_job",
    selection=AssetSelection.all()
)

# --- 2. Define Schedules ---
hourly_schedule = ScheduleDefinition(
    job=all_assets_job,
    cron_schedule="0 * * * *", # "At minute 0 of every hour"
    description="Refreshes the full Medallion pipeline every hour."
)

# --- 3. Define Resources ---
resources_def = {
    # I/O Managers for each layer, configured with the correct path
    "bronze_io_manager": parquet_io_manager.configured(
        {"base_path": PathConfig().bronze_parquet_path}
    ),
    "silver_io_manager": parquet_io_manager.configured(
        {"base_path": PathConfig().silver_path}
    ),
    "gold_io_manager": parquet_io_manager.configured(
        {"base_path": PathConfig().gold_path}
    ),
    
    # Resources our assets need
    "paths": PathConfig(), # Provides all paths
    "azure": AzureConfig(), # Provides the SAS URL from .env
}


# --- 4. Create Main Definitions ---
defs = Definitions(
    assets=all_assets,
    resources=resources_def,
    jobs=[all_assets_job],
    schedules=[hourly_schedule],
)
