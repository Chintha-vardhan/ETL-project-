# etl/transform.py
import pandas as pd
from etl.config import logger

def transform_customer(df):
    """
    Transforms customer chunk into dim_customer.
    """
    cols = ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]
    # Keep only columns that exist
    existing_cols = [c for c in cols if c in df.columns]
    return df[existing_cols].copy()

def transform_product(df):
    """
    Transforms product chunk into dim_product.
    """
    cols = [
        "product_id", "product_category_name", "product_name_length", 
        "product_description_length", "product_photos_qty", "product_weight_g", 
        "product_length_cm", "product_height_cm", "product_width_cm"
    ]
    existing_cols = [c for c in cols if c in df.columns]
    return df[existing_cols].copy()

def transform_seller(df):
    """
    Transforms seller chunk into dim_seller.
    """
    cols = ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]
    existing_cols = [c for c in cols if c in df.columns]
    return df[existing_cols].copy()

def generate_dim_date(date_series):
    """
    Generates dim_date rows dynamically from a Series of datetimes.
    """
    # Drop nulls, get unique dates, and normalize to midnight
    unique_dates = pd.to_datetime(pd.Series(date_series)).dropna().dt.normalize().unique()
    if len(unique_dates) == 0:
        return pd.DataFrame(columns=[
            "date_key", "date", "year", "month", "day", 
            "quarter", "day_of_week", "day_name", "is_weekend"
        ])

    df_date = pd.DataFrame({"date_obj": unique_dates})
    df_date["date_key"] = df_date["date_obj"].dt.strftime("%Y%m%d").astype(int)
    df_date["date"] = df_date["date_obj"].dt.strftime("%Y-%m-%d")
    df_date["year"] = df_date["date_obj"].dt.year
    df_date["month"] = df_date["date_obj"].dt.month
    df_date["day"] = df_date["date_obj"].dt.day
    df_date["quarter"] = df_date["date_obj"].dt.quarter
    # dayofweek is 0-6, make it 1-7 (1 = Monday, 7 = Sunday)
    df_date["day_of_week"] = df_date["date_obj"].dt.dayofweek + 1
    df_date["day_name"] = df_date["date_obj"].dt.day_name()
    df_date["is_weekend"] = df_date["day_of_week"].isin([6, 7]).astype(int)

    return df_date.drop(columns=["date_obj"])

def transform_fact_orders(orders_chunk, items_chunk):
    """
    Merges orders and items chunks to generate the fact_orders DataFrame.
    """
    if orders_chunk.empty or items_chunk.empty:
        return pd.DataFrame()

    # Inner join on order_id
    fact_df = items_chunk.merge(orders_chunk, on="order_id", how="inner")
    if fact_df.empty:
        return fact_df

    # Generate composite PK
    fact_df["fact_key"] = fact_df["order_id"] + "_" + fact_df["order_item_id"].astype(str)

    # Generate Date Key YYYYMMDD (fallback to 19700101 if date is NaT)
    purchase_dates = pd.to_datetime(fact_df["order_purchase_timestamp"])
    fact_df["date_key"] = purchase_dates.dt.strftime("%Y%m%d").fillna("19700101").astype(int)

    # Compute delivery time in days (rounded to 2 decimals)
    delivery_dates = pd.to_datetime(fact_df["order_delivered_customer_date"])
    delivery_time = (delivery_dates - purchase_dates).dt.total_seconds() / 86400.0
    fact_df["delivery_time_days"] = delivery_time.round(2)

    # Schema output
    cols = [
        "fact_key", "order_id", "order_item_id", "customer_id", "product_id", 
        "seller_id", "date_key", "order_status", "order_purchase_timestamp", 
        "order_delivered_customer_date", "price", "freight_value", "delivery_time_days"
    ]
    
    # Keep only existing columns
    existing_cols = [c for c in cols if c in fact_df.columns]
    
    # Format datetimes back to strings for SQLite loading (to avoid dtype object conflicts)
    for col in ["order_purchase_timestamp", "order_delivered_customer_date"]:
        if col in fact_df.columns:
            fact_df[col] = fact_df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    return fact_df[existing_cols].copy()

if __name__ == "__main__":
    # Test Transformations
    test_orders = pd.DataFrame([
        {
            "order_id": "o1", "customer_id": "c1", "order_status": "delivered",
            "order_purchase_timestamp": pd.to_datetime("2026-06-24 10:00:00"),
            "order_delivered_customer_date": pd.to_datetime("2026-06-26 12:00:00")
        }
    ])
    test_items = pd.DataFrame([
        {"order_id": "o1", "order_item_id": 1, "product_id": "p1", "seller_id": "s1", "price": 100.0, "freight_value": 15.0}
    ])
    
    fact_df = transform_fact_orders(test_orders, test_items)
    print("\n--- Fact Orders ---")
    print(fact_df)
    
    date_df = generate_dim_date(test_orders["order_purchase_timestamp"])
    print("\n--- Dim Date ---")
    print(date_df)
