nft-data-engineering-project
This repo was specifically designed to get the contract addresses of each collection of each category. The system collects data from OpenSea's API (approximately 30,000 NFT collections across 300 JSON batch files) and processes it to match collection names with their contract addresses.
There will be a complete csv file (located in data/processed_collections folder) that has all the information of each nft collection and their corresponding contract addresses.
Repository Structure
nft-data-engineering-project/
├── nft_rank/                   # Collection ranking data
│   ├── art.csv                 # Art category collections
│   ├── pfp.csv                 # PFP category collections
│   ├── generate_art_csv.py     # Script to generate art.csv
│   └── generate_pfp_csv.py     # Script to generate pfp.csv
├── data/
│   └── processed_collections/  # Processed collection data
│       └── consolidated_collections.csv  # All collections with contracts
├── fetch_collections.py        # Gets collections from OpenSea (creates 300 JSON files)
├── process_collections.py      # Processes raw JSON into consolidated CSV
├── get_contract_address.py     # Matches collections with contracts
└── pfp_processed.csv           # Final output with contract addresses
How to Use This Repo
Here are the steps that you have to do:

Have the ranking table of each category ready in the csv format and put it in the nft_rank folder (for example we already had art and pfp collection)

You can use generate_pfp_csv.py or generate_art_csv.py to create these files
These scripts can be modified to generate rankings for any category by changing the URL parameter:
python# For PFP collections:
BASE_URL = "https://api.opensea.io/api/v2/collections?chain=ethereum&limit=100"

# For Art collections:
BASE_URL = "https://api.opensea.io/api/v2/collections?chain=ethereum&limit=100&category=art"

# For any other category (example - sports):
BASE_URL = "https://api.opensea.io/api/v2/collections?chain=ethereum&limit=100&category=sports"



If you need updated collection data:

Run fetch_collections.py to get the latest collections from OpenSea (creates 300 JSON files)
Run process_collections.py to convert the JSON files into a consolidated CSV


Run the get_contract_address.py script to get the contract addresses for each category.
3.1. Read the original csv file of each category (here, for example, pfp), please also change the input path
pythondf_art = pd.read_csv("/Users/vuh/Documents/nft-data-engineering-project/nft_rank/pfp.csv", index_col=0)
3.2 Change the name of the output_csv_path to its corresponding name, please also change the output path, ideally you should put that in an output folder
pythonoutput_csv_path = 'pfp_processed.csv'
3.3 This script will output a complete csv file having an additional contract address column corresponding with each collection within that category

Matching Process
The matching process uses several techniques to connect collections with contract addresses:

Direct name matching
Collection name matching
Normalized text matching (removing spaces, special characters)
Fuzzy matching for similar names

This helps ensure maximum match rate between collections and their contract addresses.
Important Notes
Please note that, for each category, each collection might have more than 1 contract address. Therefore, when the output csv file has less rows than the input csv file, there might be a case that the name of the collection is wrongly typed, please check again carefully by searching the name of that collection in this website https://opensea.io/. If there is nothing wrong with the name, you are good to go!
API Keys
This project requires an OpenSea API key. Create a .env file in the root directory with your API key:
OPENSEA_API_KEY=your_api_key_here


















Level complete! Player Jasper has logged off.
It's your turn to grab the controller and level up this NFT project!
Good luck and don't forget to stay caffeinated!