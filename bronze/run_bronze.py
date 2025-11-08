"""Runs the entire Bronze ETL pipeline by executing each step in order."""
import subprocess
import sys
import os
from dotenv import load_dotenv

# --- Setup Paths ---

# Get the path to this script (run_bronze.py)
this_script_path = os.path.realpath(__file__)

# Get the path to the 'bronze' folder (where this script lives)
bronze_dir = os.path.dirname(this_script_path)

# Get the path to the project root (one level up)
project_root = os.path.dirname(bronze_dir)

# Add the project root to sys.path
# This allows 'load_csv.py' to 'from bronze.transform...'
sys.path.insert(0, project_root)

# Load the .env file from the project root
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path)

# Get the Python executable you are currently using
PYTHON_EXE = sys.executable

def run_step(module_path: str):
    """
    Runs a Python module from the project root using the '-m' flag.
    e.g., "bronze.extract.csv_extractor"
    """
    print(f"\n--- Running: {module_path} ---")
    try:
        # Run the script as a module (-m)
        # This correctly sets up the Python path for package imports.
        subprocess.run(
            [PYTHON_EXE, "-m", module_path], # Use the -m flag
            check=True,
            cwd=project_root # Run from project root
        )
    except subprocess.CalledProcessError as e:
        print(f"!!! SCRIPT FAILED: {module_path} !!!")
        sys.exit(f"Error: {e}") # Stop the pipeline

# --- Run Pipeline ---

print("=== STARTING BRONZE PIPELINE ===")

# 1. Extract
run_step("bronze.extract.csv_extractor")
run_step("bronze.extract.jsonl_extractor")

# 2. Transform (for preview)
run_step("bronze.transform.csv_transformer")
run_step("bronze.transform.jsonl_transformer")

# 3. Load (this also runs the transforms)
run_step("bronze.load.csv_loader")
run_step("bronze.load.jsonl_loader")

print("\n=== BRONZE PIPELINE COMPLETE ===")
# End of script
