# etl/extract.py
import os
import json
import pandas as pd
from etl.config import RAW_DATA_PATH, FILES, CHUNK_SIZE, logger, WATERMARK_PATH

def load_watermark(watermark_path=WATERMARK_PATH):
    """
    Loads the watermark timestamp from watermark.json.
    Returns a pandas Timestamp or None.
    """
    if os.path.exists(watermark_path):
        try:
            with open(watermark_path, "r") as f:
                data = json.load(f)
                ts = data.get("last_processed_timestamp")
                if ts:
                    logger.info(f"Loaded watermark: {ts}")
                    return pd.to_datetime(ts)
        except Exception as e:
            logger.warning(f"Failed to read watermark.json: {e}. Starting full load.")
    else:
        logger.info("No watermark.json found. Starting full load.")
    return None

def extract_chunks(table_name, watermark=None, active_order_ids=None):
    """
    Generator that reads a CSV file in chunks and yields DataFrames.
    Filters orders by watermark and filters items/payments by active_order_ids.
    """
    filename = FILES.get(table_name)
    if not filename:
        logger.error(f"Unknown table name: {table_name}")
        raise ValueError(f"Unknown table name: {table_name}")

    file_path = os.path.join(RAW_DATA_PATH, filename)
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")

    logger.info(f"Extracting '{table_name}' from {filename} in chunks (size: {CHUNK_SIZE})...")
    
    # Track statistics
    total_rows_read = 0
    total_rows_yielded = 0

    # Read in chunks
    chunks = pd.read_csv(file_path, chunksize=CHUNK_SIZE)
    
    for chunk_idx, chunk in enumerate(chunks):
        chunk_len = len(chunk)
        total_rows_read += chunk_len
        
        # 1. Apply watermark filter if it's the orders table
        if table_name == "orders" and watermark is not None:
            # Temporarily parse date to perform comparison
            purchase_col = "order_purchase_timestamp"
            if purchase_col in chunk.columns:
                chunk_dates = pd.to_datetime(chunk[purchase_col], errors='coerce')
                chunk = chunk[chunk_dates > watermark]
                
        # 2. Apply active_order_ids filter if it is orders-linked table (items, payments)
        elif table_name in ["items", "payments"] and active_order_ids is not None:
            if "order_id" in chunk.columns:
                chunk = chunk[chunk["order_id"].isin(active_order_ids)]
                
        yielded_len = len(chunk)
        total_rows_yielded += yielded_len
        
        if yielded_len > 0:
            yield chunk
            
    logger.info(f"Completed extraction of '{table_name}'. Total rows read: {total_rows_read}, yielded after filtering: {total_rows_yielded}")

if __name__ == "__main__":
    # Test execution
    wm = load_watermark()
    print("Testing extraction of a few chunks...")
    
    # Test product chunk loading
    prod_chunks = extract_chunks("products")
    first_chunk = next(prod_chunks, None)
    if first_chunk is not None:
        print(f"Loaded products chunk with {len(first_chunk)} rows.")
        print(first_chunk.head(2))
