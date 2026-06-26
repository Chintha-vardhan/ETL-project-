-- sql/schema.sql
-- Star Schema Database Definition for RetailFlow

PRAGMA foreign_keys = ON;

-- 1. Dimension: Customers
-- Contains unique customer profiles and geographical location.
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50) NOT NULL,
    customer_zip_code_prefix INTEGER,
    customer_city VARCHAR(100),
    customer_state CHAR(2)
);

-- 2. Dimension: Products
-- Contains details of products sold in the marketplace.
CREATE TABLE IF NOT EXISTS dim_product (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER
);

-- 3. Dimension: Sellers
-- Contains registry of sellers on the platform.
CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix INTEGER,
    seller_city VARCHAR(100),
    seller_state CHAR(2)
);

-- 4. Dimension: Date
-- Standard date dimension table for temporal analysis.
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    date TEXT UNIQUE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(15) NOT NULL,
    is_weekend INTEGER NOT NULL -- 0 or 1
);

-- 5. Fact Table: Orders
-- Transactional fact table modeled at the order-item granularity.
CREATE TABLE IF NOT EXISTS fact_orders (
    fact_key VARCHAR(100) PRIMARY KEY, -- Composite key of order_id + '_' + order_item_id
    order_id VARCHAR(50) NOT NULL,
    order_item_id INTEGER NOT NULL,
    customer_id VARCHAR(50) REFERENCES dim_customer(customer_id),
    product_id VARCHAR(50) REFERENCES dim_product(product_id),
    seller_id VARCHAR(50) REFERENCES dim_seller(seller_id),
    date_key INTEGER REFERENCES dim_date(date_key),
    order_status VARCHAR(20),
    order_purchase_timestamp TEXT,
    order_delivered_customer_date TEXT,
    price DECIMAL(10, 2) NOT NULL,
    freight_value DECIMAL(10, 2) NOT NULL,
    delivery_time_days DECIMAL(10, 2)
);

-- Performance Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_product ON fact_orders (product_id);
CREATE INDEX IF NOT EXISTS idx_fact_seller ON fact_orders (seller_id);
CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_orders (date_key);
CREATE INDEX IF NOT EXISTS idx_date_calendar ON dim_date (date);
