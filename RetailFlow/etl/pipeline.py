# etl/pipeline.py
import os
import sqlite3
import pandas as pd
from datetime import datetime
from etl.config import DB_PATH, WATERMARK_PATH, REPORT_PATH, logger
from etl.extract import load_watermark, extract_chunks
from etl.validate import validate_chunk
from etl.clean import clean_chunk
from etl.transform import transform_customer, transform_product, transform_seller, generate_dim_date
from etl.load import init_db, load_chunk_upsert, save_watermark

def run_pipeline():
    logger.info("=========================================")
    logger.info("Starting RetailFlow ETL Pipeline Run")
    logger.info("=========================================")

    # 1. Initialize Database Schema
    init_db()

    # 2. Setup run metadata
    run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stats = {}
    
    # Clean up any residual staging tables
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS stg_orders")
    cursor.execute("DROP TABLE IF EXISTS stg_items")
    conn.commit()
    conn.close()

    # 3. Load Watermark
    watermark = load_watermark(WATERMARK_PATH)

    # 4. Ingest Dimension tables
    dimensions = {
        "customers": ("dim_customer", transform_customer),
        "products": ("dim_product", transform_product),
        "sellers": ("dim_seller", transform_seller)
    }

    for src_table, (tgt_table, transform_fn) in dimensions.items():
        stats[src_table] = {"total": 0, "valid": 0, "invalid": 0}
        try:
            chunks = extract_chunks(src_table)
            for chunk in chunks:
                # Validate
                valid_chunk, tot, val, inv = validate_chunk(chunk, src_table)
                stats[src_table]["total"] += tot
                stats[src_table]["valid"] += val
                stats[src_table]["invalid"] += inv
                
                if not valid_chunk.empty:
                    # Clean
                    cleaned = clean_chunk(valid_chunk, src_table)
                    # Transform
                    transformed = transform_fn(cleaned)
                    # Load
                    load_chunk_upsert(transformed, tgt_table)
                    
            logger.info(f"Finished processing dimension: {tgt_table}")
        except Exception as e:
            logger.error(f"Error processing dimension '{src_table}': {e}")
            raise e

    # 5. Process Transactions to Staging (Orders & Items)
    stats["orders"] = {"total": 0, "valid": 0, "invalid": 0}
    max_timestamp = None
    
    conn = sqlite3.connect(DB_PATH)

    # Process Orders
    try:
        order_chunks = extract_chunks("orders", watermark=watermark)
        for chunk in order_chunks:
            valid_chunk, tot, val, inv = validate_chunk(chunk, "orders")
            stats["orders"]["total"] += tot
            stats["orders"]["valid"] += val
            stats["orders"]["invalid"] += inv
            
            if not valid_chunk.empty:
                cleaned = clean_chunk(valid_chunk, "orders")
                
                # Check for the latest timestamp in the valid chunk
                latest_in_chunk = cleaned["order_purchase_timestamp"].max()
                if max_timestamp is None or (latest_in_chunk is not None and latest_in_chunk > max_timestamp):
                    max_timestamp = latest_in_chunk
                
                # Format datetimes as strings for SQLite staging
                staging_df = cleaned.copy()
                for col in staging_df.select_dtypes(include=["datetime"]).columns:
                    staging_df[col] = staging_df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
                    
                # Write directly to SQLite staging table stg_orders
                staging_df.to_sql("stg_orders", conn, if_exists="append", index=False)
                
        logger.info("Finished staging new orders.")
    except Exception as e:
        logger.error(f"Error staging orders: {e}")
        conn.close()
        raise e

    # Check if we staged any new orders
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='stg_orders'")
    stg_orders_exists = cursor.fetchone()[0] > 0
    
    has_new_orders = False
    if stg_orders_exists:
        cursor.execute("SELECT count(*) FROM stg_orders")
        has_new_orders = cursor.fetchone()[0] > 0

    stats["items"] = {"total": 0, "valid": 0, "invalid": 0}
    if has_new_orders:
        try:
            # Get list of new order IDs to filter order items
            cursor.execute("SELECT order_id FROM stg_orders")
            active_order_ids = {row[0] for row in cursor.fetchall()}
            logger.info(f"Found {len(active_order_ids)} new orders. Filtering order items...")
            
            item_chunks = extract_chunks("items", active_order_ids=active_order_ids)
            for chunk in item_chunks:
                valid_chunk, tot, val, inv = validate_chunk(chunk, "items")
                stats["items"]["total"] += tot
                stats["items"]["valid"] += val
                stats["items"]["invalid"] += inv
                
                if not valid_chunk.empty:
                    cleaned = clean_chunk(valid_chunk, "items")
                    cleaned.to_sql("stg_items", conn, if_exists="append", index=False)
                    
            logger.info("Finished staging matched order items.")
        except Exception as e:
            logger.error(f"Error staging items: {e}")
            conn.close()
            raise e
    else:
        logger.info("No new orders to process. Skipping order items extraction.")

    # 6. Transform and Load Fact Orders & Dim Date
    if has_new_orders:
        try:
            # Get unique order purchase dates from stg_orders to populate dim_date
            cursor.execute("SELECT DISTINCT date(order_purchase_timestamp) FROM stg_orders WHERE order_purchase_timestamp IS NOT NULL")
            unique_dates = [row[0] for row in cursor.fetchall()]
            if unique_dates:
                logger.info(f"Generating date entries for {len(unique_dates)} unique dates...")
                date_dim_df = generate_dim_date(unique_dates)
                load_chunk_upsert(date_dim_df, "dim_date")

            # Check if stg_items exists and has rows
            cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='stg_items'")
            stg_items_exists = cursor.fetchone()[0] > 0
            
            has_items = False
            if stg_items_exists:
                cursor.execute("SELECT count(*) FROM stg_items")
                has_items = cursor.fetchone()[0] > 0
                
            if has_items:
                logger.info("Building fact_orders using SQL transformation...")
                
                # Execute transactional insert into fact_orders
                fact_insert_sql = """
                    INSERT OR REPLACE INTO fact_orders (
                        fact_key, order_id, order_item_id, customer_id, product_id, seller_id,
                        date_key, order_status, order_purchase_timestamp, order_delivered_customer_date,
                        price, freight_value, delivery_time_days
                    )
                    SELECT 
                        (i.order_id || '_' || i.order_item_id) as fact_key,
                        i.order_id,
                        i.order_item_id,
                        o.customer_id,
                        i.product_id,
                        i.seller_id,
                        CAST(strftime('%Y%m%d', o.order_purchase_timestamp) AS INTEGER) as date_key,
                        o.order_status,
                        o.order_purchase_timestamp,
                        o.order_delivered_customer_date,
                        i.price,
                        i.freight_value,
                        ROUND((julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp)), 2) as delivery_time_days
                    FROM stg_items i
                    INNER JOIN stg_orders o ON i.order_id = o.order_id
                """
                cursor.execute(fact_insert_sql)
                conn.commit()
                logger.info("Successfully populated/updated fact_orders.")
            else:
                logger.warning("No matched items found for the new orders. fact_orders was not updated.")
                
        except Exception as e:
            logger.error(f"Error during final star schema transformation: {e}")
            conn.rollback()
            raise e
        finally:
            # Cleanup staging tables
            cursor.execute("DROP TABLE IF EXISTS stg_orders")
            cursor.execute("DROP TABLE IF EXISTS stg_items")
            conn.commit()
    
    conn.close()

    # 7. Update Watermark
    if has_new_orders and max_timestamp:
        save_watermark(max_timestamp, WATERMARK_PATH)
    else:
        logger.info("Watermark remains unchanged (no new transactions processed).")

    # 8. Generate Summary Report
    logger.info("Generating pipeline summary report...")
    report_rows = []
    for table_name, counts in stats.items():
        report_rows.append({
            "run_timestamp": run_time,
            "source_table": table_name,
            "total_records": counts["total"],
            "valid_records": counts["valid"],
            "invalid_records": counts["invalid"]
        })
    
    report_df = pd.DataFrame(report_rows)
    file_exists = os.path.exists(REPORT_PATH)
    try:
        report_df.to_csv(REPORT_PATH, mode="a", index=False, header=not file_exists, encoding="utf-8")
        logger.info(f"Pipeline summary appended to {REPORT_PATH}")
    except Exception as e:
        logger.error(f"Failed to write pipeline summary report: {e}")

    logger.info("=========================================")
    logger.info("RetailFlow ETL Pipeline Run Completed Successfully")
    logger.info("=========================================")

if __name__ == "__main__":
    run_pipeline()
