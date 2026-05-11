-- Sprint 2 Snowflake Model - INT_PRODUCTS
-- Purpose: Build product sales, popularity, and lifecycle metrics.
-- Source: RETAIL_MLOPS.STAGING.STG_PRODUCTS, STG_ORDER_ITEMS, and STG_ORDERS
-- Grain: One row per product.
-- Known limitations: Product activity is benchmarked against dataset_last_order_at for historical-aware status.

CREATE OR REPLACE TABLE RETAIL_MLOPS.INT_LAYER.INT_PRODUCTS AS
WITH dataset_context AS (SELECT MAX(ordered_at) AS dataset_last_order_at FROM RETAIL_MLOPS.STAGING.STG_ORDERS),
product_sales AS (
    SELECT oi.product_id, COUNT(DISTINCT oi.order_id) AS times_ordered, COUNT(oi.order_item_id) AS total_quantity_sold,
        COUNT(DISTINCT o.customer_id) AS unique_customers, MIN(o.ordered_at) AS first_sold_at, MAX(o.ordered_at) AS last_sold_at
    FROM RETAIL_MLOPS.STAGING.STG_ORDER_ITEMS oi JOIN RETAIL_MLOPS.STAGING.STG_ORDERS o ON oi.order_id = o.order_id GROUP BY 1
)
SELECT p.product_id, p.product_name, p.product_type, p.product_price, p.product_description,
    COALESCE(ps.times_ordered, 0) AS times_ordered, COALESCE(ps.total_quantity_sold, 0) AS total_quantity_sold,
    COALESCE(ps.unique_customers, 0) AS unique_customers, ps.first_sold_at, ps.last_sold_at,
    COALESCE(ps.total_quantity_sold, 0) * p.product_price AS estimated_total_revenue,
    CASE WHEN p.product_price >= 2000 THEN 'Premium' WHEN p.product_price >= 800 THEN 'Standard' ELSE 'Budget' END AS price_tier,
    CASE WHEN COALESCE(ps.times_ordered, 0) >= 50 THEN 'Best Seller'
        WHEN COALESCE(ps.times_ordered, 0) >= 20 THEN 'Popular'
        WHEN COALESCE(ps.times_ordered, 0) >= 5 THEN 'Moderate' ELSE 'Slow Mover' END AS popularity_segment,
    CASE WHEN ps.last_sold_at IS NULL THEN 'Never Sold'
        WHEN DATEDIFF('day', ps.last_sold_at, dc.dataset_last_order_at) <= 30 THEN 'Active'
        WHEN DATEDIFF('day', ps.last_sold_at, dc.dataset_last_order_at) <= 90 THEN 'Slowing' ELSE 'Dormant' END AS product_last_sold_status,
    CURRENT_TIMESTAMP() AS loaded_at
FROM RETAIL_MLOPS.STAGING.STG_PRODUCTS p
LEFT JOIN product_sales ps ON p.product_id = ps.product_id
CROSS JOIN dataset_context dc;
