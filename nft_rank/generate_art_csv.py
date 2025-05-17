import requests
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))
API_KEY = os.getenv("OPENSEA_API_KEY")

if not API_KEY:
    raise ValueError("OPENSEA_API_KEY not found. Make sure it's set in your .env file.")

# OpenSea collections endpoint with art category filter
BASE_URL = "https://api.opensea.io/api/v2/collections?chain=ethereum&limit=100&category=art"
headers = {
    "accept": "application/json",
    "x-api-key": API_KEY
}

# Initialize collection list
collection_list = []
next_cursor = None
rank = 1

# Fetch top 100 art collections (can expand to more pages if needed)
for _ in range(1):  # increase to 2 or more to fetch 200+ collections
    url = BASE_URL if not next_cursor else f"{BASE_URL}&next={next_cursor}"
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"Error fetching data: {response.status_code}")
        print(f"Response: {response.text}")
        break
    
    data = response.json()
    collections = data.get("collections", [])
    
    for col in collections:
        name = col.get("name", "Unknown").strip()
        if name.lower() == "bull shit" or name == "":
            continue  # Skip garbage or empty names
        collection_list.append({
            "Rank": rank,
            "Collection": name
        })
        rank += 1
    
    next_cursor = data.get("next")
    if not next_cursor:
        break

# Save CSV in the same folder as this script
output_dir = os.path.dirname(__file__)
output_path = os.path.join(output_dir, "art.csv")

df = pd.DataFrame(collection_list)
# Include the DataFrame index in the CSV
df.to_csv(output_path, index=True)
print(f"Saved {len(df)} cleaned Art collections to {output_path}")