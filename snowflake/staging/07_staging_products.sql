-- Sprint 2 Snowflake Model - STG_PRODUCTS
-- Purpose: Clean and standardize raw product data.
-- Source: RETAIL_MLOPS.RAW.RAW_PRODUCTS
-- Grain: One row per product.
-- Known limitations: Product description is available only after the later RAW_PRODUCTS table refresh.

CREATE OR REPLACE TABLE STG_PRODUCTS AS

SELECT
    TRIM(sku)                        AS product_id,
    TRIM(name)                       AS product_name,
    LOWER(TRIM(type))                AS product_type,
    price::NUMBER(12,2)              AS product_price,
    TRIM(description)                AS product_description

FROM RETAIL_MLOPS.RAW.RAW_PRODUCTS

WHERE sku IS NOT NULL;
