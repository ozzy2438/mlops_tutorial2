-- Sprint 2 Snowflake Model - DIM_STORES
-- Purpose: Publish store dimension for analytics.
-- Source: RETAIL_MLOPS.INT_LAYER.INT_STORES
-- Grain: One row per store.
-- Known limitations: Store performance is based on available order history.

CREATE OR REPLACE TABLE RETAIL_MLOPS.MARTS_LAYER.DIM_STORES AS
SELECT store_id, store_name, opened_at, tax_rate, store_age_days, store_age_months,
    total_orders, unique_customers, avg_order_value, first_order_at, last_order_at,
    total_revenue, total_subtotal, total_tax_collected, total_items_sold, unique_products_sold,
    avg_items_per_order, performance_segment, store_activity_status, loaded_at
FROM RETAIL_MLOPS.INT_LAYER.INT_STORES;
