-- Sprint 2 Snowflake Model - INT_ORDER_ITEMS
-- Purpose: Build order item records enriched with order and product attributes.
-- Source: RETAIL_MLOPS.STAGING.STG_ORDER_ITEMS, STG_ORDERS, and STG_PRODUCTS
-- Grain: One row per order line item.
-- Known limitations: Supply/product profitability joins are excluded until SKU mapping is validated.

CREATE OR REPLACE TABLE RETAIL_MLOPS.INT_LAYER.INT_ORDER_ITEMS AS
SELECT oi.order_item_id, oi.order_id, oi.product_id, o.customer_id, o.store_id, o.ordered_at,
    DATE(o.ordered_at) AS order_date, p.product_name, p.product_type, p.product_price,
    CASE WHEN p.product_price >= 2000 THEN 'Premium' WHEN p.product_price >= 800 THEN 'Standard' ELSE 'Budget' END AS price_tier,
    CURRENT_TIMESTAMP() AS loaded_at
FROM RETAIL_MLOPS.STAGING.STG_ORDER_ITEMS oi
LEFT JOIN RETAIL_MLOPS.STAGING.STG_ORDERS o ON oi.order_id = o.order_id
LEFT JOIN RETAIL_MLOPS.STAGING.STG_PRODUCTS p ON oi.product_id = p.product_id;
