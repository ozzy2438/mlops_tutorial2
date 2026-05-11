-- Sprint 2 Snowflake Model - FCT_ORDERS
-- Purpose: Publish order fact table for analytics.
-- Source: RETAIL_MLOPS.INT_LAYER.INT_ORDERS
-- Grain: One row per order.
-- Known limitations: Measures are based on source order totals and available item counts.

CREATE OR REPLACE TABLE RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS AS
SELECT order_id, customer_id, store_id, ordered_at, order_date, order_year_month, order_week,
    order_day_name, order_month, order_year, order_quarter, subtotal_amount, tax_amount,
    order_total_amount, effective_tax_rate, total_items, unique_products, avg_item_value,
    order_size_segment, is_weekend_order, loaded_at
FROM RETAIL_MLOPS.INT_LAYER.INT_ORDERS;
