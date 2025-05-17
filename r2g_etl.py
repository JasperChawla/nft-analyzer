import pandas as pd
import json
import os
import yaml

# Load configuration
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, "config.yml")
with open(config_path, 'r') as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

data_folder = config['PATH']['DATA_RAW']
data_collection_folder = os.path.join(data_folder, 'raw_collections')

rows = []  # Initialize an empty list to collect rows

# Loop through each file in the directory
for file_name in os.listdir(data_collection_folder):
    if file_name.endswith('.json'):
        file_path = os.path.join(data_collection_folder, file_name)
        with open(file_path, 'r') as file:
            data = json.load(file)
            for collection in data['collections']:
                for contract in collection.get('contracts', []):
                    rows.append({
                        'contract_address': contract.get('address'),
                        'collection_name': collection.get('collection'),
                        'name': collection.get('name'),
                        'owner_address': collection.get('owner')
                    })

# Convert the list of rows into a DataFrame
df = pd.DataFrame(rows, columns=['contract_address', 'collection_name', 'name', 'owner_address'])

# Save the DataFrame to a CSV file
processed_collection_folder =os.path.join(data_folder, "processed_collections")
os.makedirs(processed_collection_folder, exist_ok=True)


output_csv_path = os.path.join(processed_collection_folder, 'consolidated_collections.csv')
df.to_csv(output_csv_path, index=False)
print(f'Data saved to {output_csv_path}')
