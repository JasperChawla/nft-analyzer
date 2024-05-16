import pandas as pd
import yaml
import os

# Load configuration
with open("config.yml", 'r') as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

data_folder = config['PATH']['DATA_RAW']
consolidated_path = os.path.join(data_folder, "processed_collections", 'consolidated_collections.csv')
df_consolidated = pd.read_csv(consolidated_path)


print(len(df_consolidated))
df_consolidated.drop_duplicates(subset=df_consolidated.columns[0], keep='first', inplace=True)
print(len(df_consolidated))

df_art = pd.read_csv("/Users/vuh/Documents/nft-data-engineering-project/nft_rank/pfp.csv", index_col=0)
print(df_art)
# Join on 'collection_name' to get contract addresses for these names
df_joined = pd.merge(df_art, df_consolidated, left_on='Collection', right_on='name', how='left')
# df_joined.dropna(inplace=True)
print(df_joined[['contract_address', 'Collection']])

na_rows = df_joined[df_joined['contract_address'].isna()]

output_csv_path = 'sample_data.csv'
df_joined.to_csv(output_csv_path, index=False)
print(f'Data saved to {output_csv_path}')
# print(na_rows[['contract_address', 'Collection']])
