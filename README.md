# nft-data-engineering-project
This repo was specifically designed to get the contract addresses of each collection of each category. There will be a complete csv file (located in data/processed_collections folder) that has all the information of each nft collection and their corresponding contract addresses. Here will be the steps that you have to do

1. Have the ranking table of each category ready in the csv format and put it in the nft_rank folder (for example we already had art and pfp collection)
2. Run the get_contract_address.py script to get the contract addresses for each category.

    2.1. Read the original csv file of each category (here, for example, pfp), please also change the input path
    
    df_art = pd.read_csv("/Users/vuh/Documents/nft-data-engineering-project/nft_rank/pfp.csv", index_col=0)

    2.2 Change the name of the output_csv_path to its corresponding name,  please also change the output path, ideally you should put that in an output folder
    
    output_csv_path = 'pfp_processed.csv'

    2.3 This script will output a complete csv file having an additional contract address column corresponding with each collection within that category

Please note that, for each category, each collection might have more than 1 contract address. Therefore, when the output csv file has less rows than the input csv file, there might be a case that the name of the collection is wrongly typed, please check again carefully by searching the name of that collection in this website https://opensea.io/. If there is nothing wrong with the name, you are good to go!
