# etl/config.py
import os
import logging
import sys

# Project Root Directory Setup
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ETL Paths
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DATA_PATH = os.path.join(DATA_DIR, "raw")
ERROR_DATA_PATH = os.path.join(DATA_DIR, "errors")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE_PATH = os.path.join(LOGS_DIR, "etl.log")
DB_PATH = os.path.join(BASE_DIR, "retailflow.db")
WATERMARK_PATH = os.path.join(BASE_DIR, "watermark.json")
REPORT_PATH = os.path.join(DATA_DIR, "pipeline_summary.csv")

# Chunk Size for Pandas Processing
CHUNK_SIZE = 10000

# Create required directories
os.makedirs(ERROR_DATA_PATH, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# Logger Configuration
def setup_logging():
    """
    Configures logging to output both to a log file and the console.
    """
    logger = logging.getLogger("RetailFlow")
    
    # Avoid duplicate handlers if already configured
    if logger.hasHandlers():
        return logger
        
    logger.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # File Handler
    file_handler = logging.FileHandler(LOG_FILE_PATH, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    
    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    
    return logger

# Initialize standard logger for import
logger = setup_logging()

# Dataset File Mapping
FILES = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "items": "olist_order_items_dataset.csv",
    "products": "olist_products_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "sellers": "olist_sellers_dataset.csv"
}

# Validation Constants
VALIDATION_SCHEMAS = {
    "customers": {
        "required_columns": ["customer_id", "customer_unique_id", "customer_state"],
        "state_len": 2
    },
    "orders": {
        "required_columns": ["order_id", "customer_id", "order_status", "order_purchase_timestamp"]
    },
    "items": {
        "required_columns": ["order_id", "order_item_id", "product_id", "seller_id", "price"],
        "min_price": 0.0
    },
    "products": {
        "required_columns": ["product_id"]
    },
    "payments": {
        "required_columns": ["order_id", "payment_value"],
        "min_payment": 0.0
    },
    "sellers": {
        "required_columns": ["seller_id", "seller_state"],
        "state_len": 2
    }
}
