# etl/extract.py
import pandas as pd
import os

def extract_data(raw_data_path):
    """
    Reads all CSV files from the raw data directory.
    Returns a dictionary of DataFrames.
    """
    files = {
        'customers': 'olist_customers_dataset.csv',
        'orders': 'olist_orders_dataset.csv',
        'items': 'olist_order_items_dataset.csv',
        'products': 'olist_products_dataset.csv',
        'payments': 'olist_order_payments_dataset.csv',
        'sellers': 'olist_sellers_dataset.csv',
        'reviews': 'olist_order_reviews_dataset.csv'
    }
    
    dataframes = {}
    
    print("--- Starting Extraction ---")
    for key, filename in files.items():
        path = os.path.join(raw_data_path, filename)
        if os.path.exists(path):
            dataframes[key] = pd.read_csv(path)
            print(f"Loaded {key}: {len(dataframes[key])} rows")
        else:
            print(f"Warning: {filename} not found!")
            
    return dataframes

if __name__ == "__main__":
    # Test the extraction
    raw_path = 'RetailFlow/data/raw'
    dfs = extract_data(raw_path)
    
    # Preview one to confirm
    if 'orders' in dfs:
        print("\n--- Preview: Orders Dataset ---")
        print(dfs['orders'].head())
