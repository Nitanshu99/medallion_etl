"""Extract and save JSONL files from Azure Blob Storage locally."""

import os
from pathlib import Path
from azure.storage.blob import ContainerClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the project root (medallion_etl)
project_root = Path(__file__).resolve().parents[2]
os.chdir(project_root)

def download_azure_jsonl(url: str, local_directory: str = "./data"):
    """
    Download all JSONL files from Azure Blob Storage container locally.

    Args:
        url (str): The SAS URL of the Azure Blob Storage container.
        local_directory (str): Local directory to save files (default: ./data).
    """

    # Create a ContainerClient using the SAS URL
    container_client = ContainerClient.from_container_url(url)

    # List all blobs in the container
    blobs = container_client.list_blobs()

    for blob in blobs:
        # Download blob
        blob_client = container_client.get_blob_client(blob.name)
        blob_data = blob_client.download_blob().readall()
        # Save locally
        local_path = os.path.join(local_directory, blob.name)
        with open(local_path, 'wb') as output_file:
            output_file.write(blob_data)
        print(f"Saved: {local_path}")


if __name__ == "__main__":
    # Read SAS_URL from .env file
    sas_url = os.getenv("AZURE_SAS_URL")
    if sas_url is None:
        raise ValueError("AZURE_SAS_URL environment variable not set.")

    download_azure_jsonl(
        sas_url,
        local_directory = str(project_root / "data" / "bronze" / "raw" / "azure")
    )
    print("Download completed.")
    # Top 5 records of all downloaded files
    for file_name in os.listdir(str(project_root / "data" / "bronze" / "raw" / "azure")):
        if file_name.endswith('.jsonl'):
            with open(
                str(project_root / "data" / "bronze" / "raw" / "azure" / file_name),
                'r',
                encoding='utf-8'
                ) as f:
                print(f"\nTop 5 records from {file_name}:")
                for _ in range(5):
                    print(f.readline().strip())
                print("...")


# End-of-file (EOF)
