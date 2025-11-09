""" --- PARQUET I/O MANAGER (Fixed) ---"""
import pandas as pd
from dagster import ConfigurableResource, UPathIOManager, io_manager, EnvVar
from upath import UPath

# --- I/O MANAGER (Handles Parquet) ---
class ParquetIOManager(UPathIOManager):
    """
    Handles the I/O for pandas DataFrames as Parquet files.
    This version includes all fixes.
    """
    extension: str = ".parquet"


    def _get_path_without_extension(self, context) -> UPath:
        """
        Build the output path from the asset key.
        Uses the *last part* of the key as the filename.
        """
        # self._base_path is correct (from parent class)
        return self._base_path / context.asset_key.path[-1]

    def dump_to_path(self, context, obj, path: UPath):
        """Saves the DataFrame to the parquet file path."""
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"Expected pd.DataFrame, got {type(obj)}")

        # Use UPath's mkdir method
        path.parent.mkdir(parents=True, exist_ok=True)

        context.log.info(f"Saving parquet to {path}")
        # pd.to_parquet can handle a UPath object, but we'll
        # explicitly cast to str for safety.
        obj.to_parquet(str(path), index=False, engine='pyarrow')

    def load_from_path(self, context, path: UPath) -> pd.DataFrame:
        """Loads a DataFrame from a parquet file path."""
        context.log.info(f"Loading parquet from {path}")

        # --- FIX 1: pd.read_parquet needs a string, not a UPath ---
        return pd.read_parquet(str(path))

@io_manager(
    config_schema={"base_path": str},
    description="An I/O manager that stores/loads DataFrames as parquet files."
)
def parquet_io_manager(init_context):
    """Factory function for ParquetIOManager with configuration."""
    base_path_str = init_context.resource_config["base_path"]

    # --- FIX 2: The parent class UPathIOManager takes 'base_path' ---
    return ParquetIOManager(base_path=UPath(base_path_str))

# --- PATH CONFIG (Correct) ---
class PathConfig(ConfigurableResource):
    """Provides all the necessary paths for the Medallion architecture."""
    raw_local_path: str = "data/bronze/raw/local"
    raw_azure_path: str = "data/bronze/raw/azure"
    bronze_parquet_path: str = "data/bronze/parquet"
    silver_path: str = "data/silver"
    gold_path: str = "data/gold"

# --- AZURE RESOURCE (Correct) ---
class AzureConfig(ConfigurableResource):
    """Provides the Azure SAS URL from environment variable."""
    sas_url: str = EnvVar("AZURE_SAS_URL")
