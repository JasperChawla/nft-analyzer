import requests
import json
import os
from dotenv import load_dotenv
import yaml
import pathlib

# Maximum number of batches to fetch
MAX_BATCHES = 300  # Will stop after collecting 300 batches

def save_batch(folder_path, data, batch_count):
    file_path = os.path.join(folder_path, f"batch_{batch_count}.json")
    print(f"→ Saving to: {file_path}")  # Add this line
    with open(file_path, 'w') as file:
        json.dump(data, file)


def update_cursor(file_path, next_cursor, batch_count):
    """Update the log file with the latest cursor and batch count."""
    with open(file_path, 'w') as cursor_file:
        json.dump({"next": next_cursor, "batch_count": batch_count}, cursor_file)

def fetch_collections(base_url, headers, data_folder):
    """Fetch NFT collections using the OpenSea API and save in batches."""

    # Define folders
    data_collection_folder = os.path.join(data_folder, "raw_collections")
    data_log_folder = os.path.join(data_folder, "log")

    # Ensure folders exist
    os.makedirs(data_collection_folder, exist_ok=True)
    os.makedirs(data_log_folder, exist_ok=True)

    # Path to log file
    collection_log_path = os.path.join(data_log_folder, "raw_collection_log.json")

    # Read existing log file or initialize
    try:
        with open(collection_log_path, 'r') as cursor_file:
            last_cursor_info = json.load(cursor_file)
            next_cursor = last_cursor_info.get('next', '')
            batch_count = last_cursor_info.get('batch_count', 0)
    except FileNotFoundError:
        next_cursor = ''
        batch_count = 0

    while True:
        if batch_count >= MAX_BATCHES:
            print(f"Reached the maximum number of batches ({MAX_BATCHES}).")
            break

        url = f"{base_url}&next={next_cursor}" if next_cursor else base_url
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            next_cursor = data.get('next', None)
            if data.get('collections'):
                save_batch(data_collection_folder, data, batch_count)
                print(f"Saved batch {batch_count}")
                batch_count += 1
                update_cursor(collection_log_path, next_cursor if next_cursor else '', batch_count)

            if not next_cursor:
                print("Reached the end or no next cursor.")
                break
        else:
            print(f"Failed to fetch data: {response.status_code}")
            break

def main():
    load_dotenv()
    API_KEY = "ee8c4b1c8daa4f7aacc01ea458e801ab"

    # Load configuration file
    config_path = pathlib.Path(__file__).parent / "config.yml"
    with open(config_path, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            return

    base_url = "https://api.opensea.io/api/v2/collections?chain=ethereum&limit=100"
    headers = {
        "accept": "application/json",
        "x-api-key": API_KEY
    }

    ##Jasper

    data_folder = config['PATH']['DATA_RAW']
    os.makedirs(data_folder, exist_ok=True)

    fetch_collections(base_url, headers, data_folder)

if __name__ == "__main__":
    main()
