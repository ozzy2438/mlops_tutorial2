-- Sprint 2 Snowflake Model - INT_ORDERS
-- Purpose: Build enriched order-level metrics and date attributes.
-- Source: RETAIL_MLOPS.STAGING.STG_ORDERS and RETAIL_MLOPS.STAGING.STG_ORDER_ITEMS
-- Grain: One row per order.
-- Known limitations: Average item value depends on available order item counts.

CREATE OR REPLACE TABLE RETAIL_MLOPS.INT_LAYER.INT_ORDERS AS
WITH item_counts AS (SELECT order_id, COUNT(order_item_id) AS total_items, COUNT(DISTINCT product_id) AS unique_products FROM RETAIL_MLOPS.STAGING.STG_ORDER_ITEMS GROUP BY 1)
SELECT o.order_id, o.customer_id, o.store_id, o.ordered_at,
    DATE(o.ordered_at) AS order_date, DAYNAME(o.ordered_at) AS order_day_name,
    WEEK(o.ordered_at) AS order_week, MONTH(o.ordered_at) AS order_month,
    TO_CHAR(o.ordered_at, 'YYYY-MM') AS order_year_month,
    QUARTER(o.ordered_at) AS order_quarter, YEAR(o.ordered_at) AS order_year,
    o.subtotal_amount, o.tax_amount, o.order_total_amount,
    o.tax_amount / NULLIF(o.subtotal_amount, 0) AS effective_tax_rate,
    COALESCE(ic.total_items, 0) AS total_items, COALESCE(ic.unique_products, 0) AS unique_products,
    o.order_total_amount / NULLIF(ic.total_items, 0) AS avg_item_value,
    CASE WHEN o.order_total_amount >= 10000 THEN 'Large' WHEN o.order_total_amount >= 3000 THEN 'Medium' ELSE 'Small' END AS order_size_segment,
    CASE WHEN DAYNAME(o.ordered_at) IN ('Sat','Sun') THEN TRUE ELSE FALSE END AS is_weekend_order,
    CURRENT_TIMESTAMP() AS loaded_at
FROM RETAIL_MLOPS.STAGING.STG_ORDERS o LEFT JOIN item_counts ic ON o.order_id = ic.order_id;
