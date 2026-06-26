# etl/clean.py
import pandas as pd
from etl.config import logger

def clean_chunk(df, table_name):
    """
    Standardizes column casing, trims strings, imputes missing values,
    and parses datetime fields.
    """
    if df.empty:
        return df

    df = df.copy()

    # 1. General String Standardizations: trim whitespace and adjust casing
    string_cols = df.select_dtypes(include=["object"]).columns
    for col in string_cols:
        # Avoid breaking lists, dicts, or nan fields
        try:
            df[col] = df[col].astype(str).str.strip()
        except Exception:
            pass

    # 2. Dataset-specific Cleaning & Standardization
    if table_name == "customers":
        if "customer_state" in df.columns:
            df["customer_state"] = df["customer_state"].str.upper()
        if "customer_city" in df.columns:
            df["customer_city"] = df["customer_city"].str.title()

    elif table_name == "sellers":
        if "seller_state" in df.columns:
            df["seller_state"] = df["seller_state"].str.upper()
        if "seller_city" in df.columns:
            df["seller_city"] = df["seller_city"].str.title()

    elif table_name == "products":
        if "product_category_name" in df.columns:
            df["product_category_name"] = df["product_category_name"].fillna("unknown")
            df["product_category_name"] = df["product_category_name"].str.replace("_", " ").str.title()

    elif table_name == "orders":
        date_columns = [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ]
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        if "order_status" in df.columns:
            df["order_status"] = df["order_status"].str.lower()

    elif table_name == "items":
        # Round prices and freight values to 2 decimal places
        for col in ["price", "freight_value"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    elif table_name == "payments":
        if "payment_value" in df.columns:
            df["payment_value"] = pd.to_numeric(df["payment_value"], errors='coerce').round(2)
        if "payment_type" in df.columns:
            df["payment_type"] = df["payment_type"].str.lower()

    return df

if __name__ == "__main__":
    # Test cleaning
    test_products = pd.DataFrame([
        {"product_id": "p1", "product_category_name": "perfumaria "},
        {"product_id": "p2", "product_category_name": None},
    ])
    cleaned_products = clean_chunk(test_products, "products")
    print("\n--- Cleaned Products ---")
    print(cleaned_products)

    test_orders = pd.DataFrame([
        {"order_id": "o1", "order_status": "DELIVERED", "order_purchase_timestamp": "2018-01-01 10:00:00"}
    ])
    cleaned_orders = clean_chunk(test_orders, "orders")
    print("\n--- Cleaned Orders ---")
    print(cleaned_orders)
    print(cleaned_orders.dtypes)
