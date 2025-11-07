"""Extracts CSV files from a GitHub repository and saves them locally."""

from pathlib import Path
import requests

API_URL = "https://api.github.com/repos/dbt-labs/jaffle-shop-data/contents/jaffle-data"
OUTPUT_DIR = Path(__file__).parent.parent.parent / "data" / "bronze" / "raw" / "local"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def fetch_files(url):
    """Recursively fetch all CSV files from GitHub."""
    response = requests.get(url, timeout=30)  # 30 seconds timeout
    response.raise_for_status()

    for item in response.json():
        if item["type"] == "dir":
            fetch_files(item["url"])
        elif item["name"].endswith(".csv"):
            content = requests.get(item["download_url"], timeout=30).text
            output_path = OUTPUT_DIR / item["name"]
            output_path.write_text(content)
            print(f"✓ {item['name']}")

if __name__ == "__main__":
    fetch_files(API_URL)
# End-of-file (EOF)
