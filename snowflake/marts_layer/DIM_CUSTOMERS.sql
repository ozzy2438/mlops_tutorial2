-- Sprint 2 Snowflake Model - DIM_CUSTOMERS
-- Purpose: Publish customer dimension for analytics.
-- Source: RETAIL_MLOPS.INT_LAYER.INT_CUSTOMERS
-- Grain: One row per customer.
-- Known limitations: Dimension reflects metrics materialized in the intermediate layer.

CREATE OR REPLACE TABLE DIM_CUSTOMERS AS
SELECT
    customer_id,
    customer_name,
    first_name,
    last_name,
    total_orders,
    total_spend,
    avg_order_value,
    first_order_at,
    last_order_at,
    days_since_last_order,
    unique_products_purchased,
    total_items_purchased,
    customer_status,
    value_segment,
    loaded_at
FROM RETAIL_MLOPS.INT_LAYER.INT_CUSTOMERS;
