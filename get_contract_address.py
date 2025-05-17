import pandas as pd
import yaml
import os
import re
import numpy as np
from fuzzywuzzy import process

# Load config.yml
config_path = os.path.join(os.path.dirname(__file__), "config.yml")
with open(config_path, 'r') as stream:
    config = yaml.safe_load(stream)

# Function to normalize names
def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = name.lower().replace(" ", "").replace("-", "")
    return re.sub(r'[^a-z0-9]', '', name)

# Fuzzy match helper
def fuzzy_match(target, candidates, threshold=90):
    match, score = process.extractOne(target, candidates)
    return match if score >= threshold else None

# Load consolidated collections
data_folder = config['PATH']['DATA_RAW']
consolidated_path = os.path.join(data_folder, "processed_collections", "consolidated_collections.csv")
if not os.path.exists(consolidated_path):
    consolidated_path = os.path.join(data_folder, "trading_data", "processed_collections", "consolidated_collections.csv")
    if not os.path.exists(consolidated_path):
        print("Could not find consolidated_collections.csv.")
        exit()

df_consolidated = pd.read_csv(consolidated_path)
print(f"Loaded {len(df_consolidated)} consolidated collections.")
df_consolidated.drop_duplicates(subset='contract_address', inplace=True)

# Load PFP list
pfp_path = os.path.join(os.path.dirname(__file__), "nft_rank", "pfp.csv")
if not os.path.exists(pfp_path):
    print("pfp.csv not found in nft_rank/")
    exit()

df_art = pd.read_csv(pfp_path)
print(f"Loaded {len(df_art)} PFP collections.")

# Add normalized names - use suffixes to avoid column name conflicts
df_consolidated['normalized_name'] = df_consolidated['name'].apply(normalize_name)
df_consolidated['normalized_collection'] = df_consolidated['collection_name'].apply(normalize_name)
df_art['art_normalized_collection'] = df_art['Collection'].apply(normalize_name)

# Direct match on 'name'
df_joined = pd.merge(df_art, df_consolidated, left_on='Collection', right_on='name', how='left')
direct_matches = len(df_joined) - df_joined['contract_address'].isna().sum()

# Initialize counters
collection_matches = 0
norm_name_matches = 0
norm_collection_matches = 0
fuzzy_matches = 0

# Process each unmatched row one by one
for i, row in df_joined[df_joined['contract_address'].isna()].iterrows():
    # Try collection_name match
    match = df_consolidated[df_consolidated['collection_name'] == row['Collection']]
    if not match.empty:
        df_joined.loc[i, ['contract_address', 'collection_name', 'name', 'owner_address']] = match.iloc[0][
            ['contract_address', 'collection_name', 'name', 'owner_address']
        ]
        collection_matches += 1
        continue

    # Try normalized_name match
    match = df_consolidated[df_consolidated['normalized_name'] == row['art_normalized_collection']]
    if not match.empty:
        df_joined.loc[i, ['contract_address', 'collection_name', 'name', 'owner_address']] = match.iloc[0][
            ['contract_address', 'collection_name', 'name', 'owner_address']
        ]
        norm_name_matches += 1
        continue

    # Try normalized_collection match
    match = df_consolidated[df_consolidated['normalized_collection'] == row['art_normalized_collection']]
    if not match.empty:
        df_joined.loc[i, ['contract_address', 'collection_name', 'name', 'owner_address']] = match.iloc[0][
            ['contract_address', 'collection_name', 'name', 'owner_address']
        ]
        norm_collection_matches += 1
        continue

    # Try fuzzy match
    best_match = fuzzy_match(row['art_normalized_collection'], df_consolidated['normalized_collection'].tolist())
    if best_match:
        match_row = df_consolidated[df_consolidated['normalized_collection'] == best_match].iloc[0]
        df_joined.loc[i, ['contract_address', 'collection_name', 'name', 'owner_address']] = match_row[
            ['contract_address', 'collection_name', 'name', 'owner_address']
        ]
        fuzzy_matches += 1
        

# Summary
print(f"Direct matches: {direct_matches}")
print(f"Collection name matches: {collection_matches}")
print(f"Normalized name matches: {norm_name_matches}")
print(f"Normalized collection matches: {norm_collection_matches}")
print(f"Fuzzy matches: {fuzzy_matches}")
print(f"Total matched: {direct_matches + collection_matches + norm_name_matches + norm_collection_matches + fuzzy_matches} / {len(df_joined)}")

# Preview unmatched
unmatched = df_joined[df_joined['contract_address'].isna()]
print("\nUnmatched collections:")
print(unmatched[['Collection']].head(10).to_string())

# Save result
output_path = os.path.join(os.path.dirname(__file__), "pfp_processed.csv")
df_joined.to_csv(output_path, index=False)
print(f"\nSaved results to {output_path}")