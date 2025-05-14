import pandas as pd
import yaml
import os
import re

# Load configuration
with open("config.yml", 'r') as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

# Function to normalize collection names
def normalize_name(name):
    if not isinstance(name, str):
        return ""
    # Convert to lowercase and remove special characters
    return re.sub(r'[^a-z0-9]', '', name.lower())

# Get paths
data_folder = config['PATH']['DATA_RAW']
consolidated_path = os.path.join(data_folder, "processed_collections", 'consolidated_collections.csv')
print(f"Loading consolidated data from: {consolidated_path}")

# Check if file exists
if not os.path.exists(consolidated_path):
    alternative_path = os.path.join(data_folder, "trading_data", "processed_collections", 'consolidated_collections.csv')
    print(f"File not found. Trying alternative path: {alternative_path}")
    if os.path.exists(alternative_path):
        consolidated_path = alternative_path
    else:
        print("Alternative path also not found!")

# Load data
df_consolidated = pd.read_csv(consolidated_path)
print(f"Loaded {len(df_consolidated)} rows from consolidated collections")

# Remove duplicates
before_count = len(df_consolidated)
df_consolidated.drop_duplicates(subset='contract_address', keep='first', inplace=True)
after_count = len(df_consolidated)
print(f"Removed {before_count - after_count} duplicate contract addresses")

# Load PFP data
pfp_path = "nft_rank/pfp.csv"
df_art = pd.read_csv(pfp_path, index_col=0)
print(f"Loaded {len(df_art)} rows from PFP data")

# Create normalized columns for better matching
df_consolidated['normalized_name'] = df_consolidated['name'].apply(normalize_name)
df_consolidated['normalized_collection'] = df_consolidated['collection_name'].apply(normalize_name)
df_art['normalized_collection'] = df_art['Collection'].apply(normalize_name)

# Try different approaches to match
# 1. Direct match on name
df_joined = pd.merge(df_art, df_consolidated, left_on='Collection', right_on='name', how='left')

# 2. For rows that didn't match, try collection_name
mask = df_joined['contract_address'].isna()
direct_matches = len(df_joined) - mask.sum()
print(f"Direct name matches: {direct_matches}")

df_collection_match = pd.merge(
    df_art[mask], 
    df_consolidated, 
    left_on='Collection', 
    right_on='collection_name', 
    how='left'
)

# Update the unmatched rows with results from collection_name match
df_joined.loc[mask, ['contract_address', 'collection_name', 'name', 'owner_address']] = df_collection_match[
    ['contract_address', 'collection_name', 'name', 'owner_address']
]

# 3. For remaining unmatched rows, try normalized matching
mask = df_joined['contract_address'].isna()
collection_matches = direct_matches + (len(df_joined) - mask.sum() - direct_matches)
print(f"Collection name matches: {collection_matches - direct_matches}")

# Try normalized name
df_norm_match = pd.merge(
    df_art[mask], 
    df_consolidated, 
    left_on='normalized_collection', 
    right_on='normalized_name', 
    how='left'
)

# Update unmatched rows with normalized name matches
df_joined.loc[mask, ['contract_address', 'collection_name', 'name', 'owner_address']] = df_norm_match[
    ['contract_address', 'collection_name', 'name', 'owner_address']
]

# 4. For still unmatched rows, try normalized collection
mask = df_joined['contract_address'].isna()
norm_name_matches = len(df_joined) - mask.sum() - collection_matches
print(f"Normalized name matches: {norm_name_matches}")

df_norm_collection_match = pd.merge(
    df_art[mask], 
    df_consolidated, 
    left_on='normalized_collection', 
    right_on='normalized_collection', 
    how='left'
)

# Update unmatched rows with normalized collection matches
df_joined.loc[mask, ['contract_address', 'collection_name', 'name', 'owner_address']] = df_norm_collection_match[
    ['contract_address', 'collection_name', 'name', 'owner_address']
]

# See how many were matched in total
mask = df_joined['contract_address'].isna()
norm_collection_matches = len(df_joined) - mask.sum() - norm_name_matches - collection_matches
print(f"Normalized collection matches: {norm_collection_matches}")
print(f"Total matches found: {len(df_joined) - mask.sum()} out of {len(df_joined)}")

# Print unmatched collections
print("\nCollections that couldn't be matched:")
unmatched = df_joined[mask][['Collection']]
print(unmatched.head(10).to_string())  # Show first 10 unmatched

# Print matched collections
print("\nMatched collections:")
matched = df_joined[~mask][['Collection', 'contract_address']]
print(matched.head(10).to_string())  # Show first 10 matched

# Save the results
output_csv_path = 'pfp_processed.csv'
df_joined.to_csv(output_csv_path, index=False)
print(f'\nData saved to {output_csv_path}')

# Optional: Try to manually find famous collections
print("\nSearching for well-known collections in your data...")
famous_collections = [
    "bored ape", "cryptopunk", "mutant ape", "azuki", "clone x", "bayc", "mayc"
]

for term in famous_collections:
    for column in ['name', 'collection_name']:
        matches = df_consolidated[df_consolidated[column].str.lower().str.contains(term, na=False)]
        if not matches.empty:
            print(f"Found potential matches for '{term}' in {column}:")
            print(matches[['contract_address', 'name', 'collection_name']].head(3).to_string())
            print()