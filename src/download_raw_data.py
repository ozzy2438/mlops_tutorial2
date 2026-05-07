"""Download expected raw retail CSV files from Azure Blob Storage.

This utility is intentionally configured through environment variables so
secrets never need to be written into source code.
"""

from pathlib import Path
import os

from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient


RAW_DATA_DIR = Path("data/raw")

EXPECTED_RAW_FILES = [
    "raw_customers.csv",
    "raw_orders.csv",
    "raw_order_items.csv",
    "raw_products.csv",
    "raw_stores.csv",
    "raw_supplies.csv",
]


def get_required_environment_variable(name: str) -> str:
    """Read a required environment variable and show a helpful error if missing."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}\n"
            "Set it in your shell or in a local .env file that is not committed to Git."
        )
    return value


def download_blob(container_client, blob_name: str, destination_path: Path) -> None:
    """Download one blob to a local file path."""
    print(f"Downloading {blob_name} -> {destination_path}")

    blob_client = container_client.get_blob_client(blob_name)
    with destination_path.open("wb") as local_file:
        download_stream = blob_client.download_blob()
        local_file.write(download_stream.readall())


def main() -> None:
    """Create the raw data folder and download the expected Azure CSV files."""
    print("Preparing to download raw retail data from Azure Blob Storage.")

    # Load local environment variables from .env if the file exists.
    # The .env file should stay local and must not be committed to GitHub.
    load_dotenv()

    connection_string = get_required_environment_variable(
        "AZURE_STORAGE_CONNECTION_STRING"
    )
    container_name = get_required_environment_variable("AZURE_CONTAINER_NAME")

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Local raw data folder is ready: {RAW_DATA_DIR}")

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    for file_name in EXPECTED_RAW_FILES:
        destination_path = RAW_DATA_DIR / file_name
        download_blob(container_client, file_name, destination_path)

    print("Raw data download finished.")
    print("Reminder: data/raw files are ignored by Git and should not be committed.")


if __name__ == "__main__":
    main()
