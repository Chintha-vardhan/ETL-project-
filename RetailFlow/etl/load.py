# etl/load.py
import os
import json
import sqlite3
import pandas as pd
from etl.config import DB_PATH, WATERMARK_PATH, logger

PRIMARY_KEYS = {
    "dim_customer": ["customer_id"],
    "dim_product": ["product_id"],
    "dim_seller": ["seller_id"],
    "dim_date": ["date_key"],
    "fact_orders": ["fact_key"]
}

def init_db(db_path=DB_PATH, schema_path=None):
    """
    Executes the DDL statements in schema.sql to create the target tables.
    """
    if schema_path is None:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        schema_path = os.path.join(base_dir, "sql", "schema.sql")

    if not os.path.exists(schema_path):
        logger.error(f"Schema file not found at {schema_path}!")
        return

    logger.info(f"Initializing database '{db_path}' with schema '{schema_path}'...")
    
    conn = sqlite3.connect(db_path)
    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            sql_script = f.read()
        conn.executescript(sql_script)
        conn.commit()
        logger.info("Database schema initialized/verified.")
    except Exception as e:
        logger.error(f"Failed to execute schema script: {e}")
        raise e
    finally:
        conn.close()

def load_chunk_upsert(df, table_name, db_path=DB_PATH):
    """
    Loads a DataFrame chunk into SQLite using a staging table and 'INSERT OR REPLACE'
    to perform a reliable, duplicate-free upsert.
    """
    if df.empty:
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    stg_table = f"stg_{table_name}"
    
    try:
        # 1. Write the DataFrame to a temporary staging table
        # We replace the staging table each time to ensure it matches the current chunk shape
        df.to_sql(stg_table, conn, if_exists="replace", index=False)
        
        # 2. Get columns dynamically to construct the SQL query
        columns = df.columns.tolist()
        columns_str = ", ".join([f'"{col}"' for col in columns])
        
        # 3. Perform the upsert (INSERT OR REPLACE) from staging into target table
        upsert_query = f"""
            INSERT OR REPLACE INTO "{table_name}" ({columns_str})
            SELECT {columns_str} FROM "{stg_table}"
        """
        cursor.execute(upsert_query)
        
        # 4. Drop the staging table
        cursor.execute(f'DROP TABLE IF EXISTS "{stg_table}"')
        
        conn.commit()
        logger.info(f"Upserted {len(df)} rows into '{table_name}'.")
        
    except Exception as e:
        logger.error(f"Failed to upsert chunk into table '{table_name}': {e}")
        conn.rollback()
        raise e
    finally:
        conn.close()

def save_watermark(max_timestamp, watermark_path=WATERMARK_PATH):
    """
    Saves the watermark timestamp to watermark.json.
    """
    if not max_timestamp:
        logger.info("No new watermark timestamp to save.")
        return

    # Convert pandas Timestamp or other datetime objects to ISO format string
    if hasattr(max_timestamp, "strftime"):
        ts_str = max_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    else:
        ts_str = str(max_timestamp)

    try:
        with open(watermark_path, "w") as f:
            json.dump({"last_processed_timestamp": ts_str}, f, indent=4)
        logger.info(f"Watermark updated in watermark.json: {ts_str}")
    except Exception as e:
        logger.error(f"Failed to write watermark.json: {e}")

if __name__ == "__main__":
    # Test DB init
    init_db()
    
    # Test load_chunk_upsert
    test_customers = pd.DataFrame([
        {"customer_id": "c1", "customer_unique_id": "cu1", "customer_zip_code_prefix": 1234, "customer_city": "Sao Paulo", "customer_state": "SP"}
    ])
    load_chunk_upsert(test_customers, "dim_customer")
