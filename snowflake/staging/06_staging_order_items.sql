-- Sprint 2 Snowflake Model - STG_ORDER_ITEMS
-- Purpose: Clean and standardize raw order item data.
-- Source: RETAIL_MLOPS.RAW.RAW_ORDER_ITEMS
-- Grain: One row per order line item.
-- Known limitations: Product key is sourced from SKU and renamed to product_id.

CREATE OR REPLACE TABLE STG_ORDER_ITEMS AS
SELECT
    TRIM(id) AS order_item_id,
    TRIM(order_id) AS order_id,
    TRIM(sku) AS product_id
FROM RETAIL_MLOPS.RAW.RAW_ORDER_ITEMS
WHERE id IS NOT NULL
  AND order_id IS NOT NULL
  AND sku IS NOT NULL;
