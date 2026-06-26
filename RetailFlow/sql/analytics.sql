-- sql/analytics.sql
-- KPI Analytics Queries on the RetailFlow Star Schema Database

-- 1. Revenue by Month
-- Sums the total revenue generated from items in delivered orders, grouped by year and month.
SELECT 
    d.year,
    d.month,
    ROUND(SUM(f.price), 2) as monthly_revenue
FROM fact_orders f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.order_status = 'delivered'
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- 2. Top Sellers
-- Finds the top 10 sellers by total revenue generated from items sold in delivered orders.
SELECT 
    s.seller_id,
    s.seller_city,
    s.seller_state,
    ROUND(SUM(f.price), 2) as total_revenue
FROM fact_orders f
JOIN dim_seller s ON f.seller_id = s.seller_id
WHERE f.order_status = 'delivered'
GROUP BY s.seller_id
ORDER BY total_revenue DESC
LIMIT 10;

-- 3. Average Delivery Time
-- Calculates the average delivery time in days from purchase to delivery for customer states.
SELECT 
    c.customer_state,
    ROUND(AVG(f.delivery_time_days), 2) as avg_delivery_time_days,
    COUNT(DISTINCT f.order_id) as delivered_orders_count
FROM fact_orders f
JOIN dim_customer c ON f.customer_id = c.customer_id
WHERE f.order_status = 'delivered' AND f.delivery_time_days IS NOT NULL
GROUP BY c.customer_state
ORDER BY avg_delivery_time_days ASC;

-- 4. Orders by State
-- Computes the total number of unique orders placed by customers in each state.
SELECT 
    c.customer_state,
    COUNT(DISTINCT f.order_id) as total_orders
FROM fact_orders f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY total_orders DESC;
