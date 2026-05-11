-- Sprint 2 Snowflake Model - INT_STORES
-- Purpose: Build store performance, revenue, activity, and age metrics.
-- Source: RETAIL_MLOPS.STAGING.STG_STORES, STG_ORDERS, and STG_ORDER_ITEMS
-- Grain: One row per store.
-- Known limitations: Store activity is benchmarked against dataset_last_order_at for historical-aware status.

CREATE OR REPLACE TABLE RETAIL_MLOPS.INT_LAYER.INT_STORES AS
WITH dataset_context AS (SELECT MAX(ordered_at) AS dataset_last_order_at FROM RETAIL_MLOPS.STAGING.STG_ORDERS),
store_orders AS (
    SELECT store_id, COUNT(DISTINCT order_id) AS total_orders, COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(order_total_amount) AS total_revenue, SUM(subtotal_amount) AS total_subtotal, SUM(tax_amount) AS total_tax_collected,
        AVG(order_total_amount) AS avg_order_value, MIN(ordered_at) AS first_order_at, MAX(ordered_at) AS last_order_at
    FROM RETAIL_MLOPS.STAGING.STG_ORDERS GROUP BY 1
),
store_items AS (
    SELECT o.store_id, COUNT(oi.order_item_id) AS total_items_sold, COUNT(DISTINCT oi.product_id) AS unique_products_sold
    FROM RETAIL_MLOPS.STAGING.STG_ORDERS o JOIN RETAIL_MLOPS.STAGING.STG_ORDER_ITEMS oi ON o.order_id = oi.order_id GROUP BY 1
)
SELECT s.store_id, s.store_name, s.opened_at, s.tax_rate,
    GREATEST(DATEDIFF('day', s.opened_at, dc.dataset_last_order_at), 0) AS store_age_days,
    GREATEST(DATEDIFF('month', s.opened_at, dc.dataset_last_order_at), 0) AS store_age_months,
    COALESCE(so.total_orders, 0) AS total_orders, COALESCE(so.unique_customers, 0) AS unique_customers,
    COALESCE(so.avg_order_value, 0) AS avg_order_value, so.first_order_at, so.last_order_at,
    COALESCE(so.total_revenue, 0) AS total_revenue, COALESCE(so.total_subtotal, 0) AS total_subtotal,
    COALESCE(so.total_tax_collected, 0) AS total_tax_collected,
    COALESCE(si.total_items_sold, 0) AS total_items_sold, COALESCE(si.unique_products_sold, 0) AS unique_products_sold,
    CASE WHEN COALESCE(so.total_orders, 0) > 0 THEN ROUND(COALESCE(si.total_items_sold, 0)::FLOAT / so.total_orders, 2) ELSE 0 END AS avg_items_per_order,
    CASE WHEN COALESCE(so.total_revenue, 0) >= 1000000 THEN 'High Performer'
        WHEN COALESCE(so.total_revenue, 0) >= 300000 THEN 'Average Performer' ELSE 'Low Performer' END AS performance_segment,
    CASE WHEN so.last_order_at IS NULL THEN 'No Orders'
        WHEN DATEDIFF('day', so.last_order_at, dc.dataset_last_order_at) <= 30 THEN 'Active'
        WHEN DATEDIFF('day', so.last_order_at, dc.dataset_last_order_at) <= 90 THEN 'Slowing' ELSE 'Inactive' END AS store_activity_status,
    CURRENT_TIMESTAMP() AS loaded_at
FROM RETAIL_MLOPS.STAGING.STG_STORES s
LEFT JOIN store_orders so ON s.store_id = so.store_id
LEFT JOIN store_items si ON s.store_id = si.store_id
CROSS JOIN dataset_context dc;
