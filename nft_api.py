import requests
import json
import os
from dotenv import load_dotenv
import yaml

def save_batch(file_path, data, batch_count):
    file_path = os.path.join(file_path, f"batch_{batch_count}.json")
    with open(file_path, 'w') as file:
        json.dump(data, file)

def update_cursor(file_path, next_cursor, batch_count):
    with open(file_path, 'w') as cursor_file:
        json.dump({"next": next_cursor, "batch_count": batch_count}, cursor_file)

def fetch_collections(base_url, headers, data_folder):
    """Fetch collections using the OpenSea API and handle pagination."""

    # get folder path
    data_collection_folder = os.path.join(data_folder, "raw_collections")
    data_log = os.path.join(data_folder, "log")

    # make folder
    os.makedirs(data_collection_folder, exist_ok=True)
    os.makedirs(data_log, exist_ok=True)

    #get file path
    collection_log_path = os.path.join(data_log, "raw_collection_log.json")

    try:
        with open(collection_log_path, 'r') as cursor_file:
            last_cursor_info = json.load(cursor_file)
            next_cursor = last_cursor_info.get('next', '')
            batch_count = last_cursor_info.get('batch_count', 0)
    except FileNotFoundError:
        next_cursor = ''
        batch_count = 0

    while True:
        url = f"{base_url}&next={next_cursor}" if next_cursor else base_url
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            next_cursor = data.get('next', None)
            if data['collections']:
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
    API_KEY = os.getenv('API_KEY')
    
    # get config
    with open("config.yml", 'r') as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    base_url = "https://api.opensea.io/api/v2/collections?chain=ethereum&limit=100"
    headers = {
        "accept": "application/json",
        "x-api-key": API_KEY
    }

    data_folder = config['PATH']['DATA_RAW']
    os.makedirs(data_folder, exist_ok=True)
    fetch_collections(base_url, headers, data_folder)

if __name__ == "__main__":
    main()
