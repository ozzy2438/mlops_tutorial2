-- Sprint 2 Snowflake Model - RETAIL_SALES_WIDE
-- Purpose: Create a convenience wide reporting model for retail sales analysis.
-- Source: FCT_ORDER_ITEMS joined to DIM_PRODUCTS, DIM_CUSTOMERS, and DIM_STORES
-- Grain: One row per order line item.
-- Known limitations: Wide model is for convenience reporting and does not replace curated facts/dimensions.

CREATE OR REPLACE TABLE RETAIL_MLOPS.MARTS_LAYER.RETAIL_SALES_WIDE AS
SELECT fi.order_item_id, fi.order_id, fi.product_id, fi.customer_id, fi.store_id, fi.ordered_at, fi.order_date,
    p.product_name, p.product_type, p.product_price, p.price_tier, p.popularity_segment, p.product_last_sold_status,
    c.customer_name, c.first_name, c.last_name, c.customer_status, c.value_segment,
    c.total_orders AS customer_total_orders, c.total_spend AS customer_total_spend,
    s.store_name, s.performance_segment AS store_performance_segment, s.store_activity_status,
    s.tax_rate AS store_tax_rate, fi.loaded_at
FROM RETAIL_MLOPS.MARTS_LAYER.FCT_ORDER_ITEMS fi
LEFT JOIN RETAIL_MLOPS.MARTS_LAYER.DIM_PRODUCTS p ON fi.product_id = p.product_id
LEFT JOIN RETAIL_MLOPS.MARTS_LAYER.DIM_CUSTOMERS c ON fi.customer_id = c.customer_id
LEFT JOIN RETAIL_MLOPS.MARTS_LAYER.DIM_STORES s ON fi.store_id = s.store_id;
