-- Sprint 2 Snowflake Model - INT_CUSTOMERS
-- Purpose: Build customer-level order, spend, recency, and value segment metrics.
-- Source: RETAIL_MLOPS.STAGING.STG_CUSTOMERS and RETAIL_MLOPS.STAGING.STG_ORDERS
-- Grain: One row per customer.
-- Known limitations: Recency is calculated against dataset_last_order_at instead of current date for historical-aware metrics.

CREATE OR REPLACE TABLE RETAIL_MLOPS.INT_LAYER.INT_CUSTOMERS AS
WITH dataset_context AS (SELECT MAX(ordered_at) AS dataset_last_order_at FROM RETAIL_MLOPS.STAGING.STG_ORDERS),
order_metrics AS (
    SELECT o.customer_id, COUNT(DISTINCT o.order_id) AS total_orders, SUM(o.order_total_amount) AS total_spend,
        AVG(o.order_total_amount) AS avg_order_value, MIN(o.ordered_at) AS first_order_at, MAX(o.ordered_at) AS last_order_at,
        DATEDIFF('day', MIN(o.ordered_at), MAX(o.ordered_at)) AS customer_tenure_days
    FROM RETAIL_MLOPS.STAGING.STG_ORDERS o GROUP BY 1
),
order_item_count AS (
    SELECT o.customer_id, COUNT(DISTINCT oi.product_id) AS unique_products_purchased, COUNT(oi.order_item_id) AS total_items_purchased
    FROM RETAIL_MLOPS.STAGING.STG_ORDERS o JOIN RETAIL_MLOPS.STAGING.STG_ORDER_ITEMS oi ON o.order_id = oi.order_id GROUP BY 1
)
SELECT c.customer_id, c.customer_name, c.first_name, c.last_name,
    COALESCE(om.total_orders, 0) AS total_orders, COALESCE(om.total_spend, 0) AS total_spend,
    COALESCE(om.avg_order_value, 0) AS avg_order_value, om.first_order_at, om.last_order_at,
    COALESCE(om.customer_tenure_days, 0) AS customer_tenure_days,
    CASE WHEN om.last_order_at IS NULL THEN NULL ELSE DATEDIFF('day', om.last_order_at, dc.dataset_last_order_at) END AS days_since_last_order,
    COALESCE(oic.unique_products_purchased, 0) AS unique_products_purchased,
    COALESCE(oic.total_items_purchased, 0) AS total_items_purchased,
    CASE WHEN om.total_orders IS NULL THEN 'Never Ordered'
        WHEN DATEDIFF('day', om.last_order_at, dc.dataset_last_order_at) <= 30 THEN 'Active'
        WHEN DATEDIFF('day', om.last_order_at, dc.dataset_last_order_at) <= 90 THEN 'At Risk' ELSE 'Churned' END AS customer_status,
    CASE WHEN COALESCE(om.total_spend, 0) >= 50000 THEN 'High Value'
        WHEN COALESCE(om.total_spend, 0) >= 10000 THEN 'Medium Value' ELSE 'Low Value' END AS value_segment,
    CURRENT_TIMESTAMP() AS loaded_at
FROM RETAIL_MLOPS.STAGING.STG_CUSTOMERS c
LEFT JOIN order_metrics om ON c.customer_id = om.customer_id
LEFT JOIN order_item_count oic ON c.customer_id = oic.customer_id
CROSS JOIN dataset_context dc;
