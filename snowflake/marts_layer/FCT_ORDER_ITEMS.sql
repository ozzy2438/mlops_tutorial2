-- Sprint 2 Snowflake Model - FCT_ORDER_ITEMS
-- Purpose: Publish order item fact table for analytics.
-- Source: RETAIL_MLOPS.INT_LAYER.INT_ORDER_ITEMS
-- Grain: One row per order line item.
-- Known limitations: Fact intentionally excludes unsafe product-supply profitability joins.

CREATE OR REPLACE TABLE RETAIL_MLOPS.MARTS_LAYER.FCT_ORDER_ITEMS AS
SELECT order_item_id, order_id, product_id, customer_id, store_id, ordered_at, order_date,
    product_name, product_type, product_price, price_tier, loaded_at
FROM RETAIL_MLOPS.INT_LAYER.INT_ORDER_ITEMS;
