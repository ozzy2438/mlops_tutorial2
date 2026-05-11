-- Sprint 2 Snowflake Model - STG_ORDERS
-- Purpose: Clean and standardize raw order data.
-- Source: RETAIL_MLOPS.RAW.RAW_ORDERS
-- Grain: One row per order.
-- Known limitations: Uses loaded raw order amounts and timestamps as provided by source CSV.

CREATE OR REPLACE TABLE STG_ORDERS AS
SELECT
    TRIM(id) AS order_id,
    TRIM(customer) AS customer_id,
    TRIM(store_id) AS store_id,
    ordered_at::TIMESTAMP AS ordered_at,
    subtotal::NUMBER(12,2) AS subtotal_amount,
    tax_paid::NUMBER(12,2) AS tax_amount,
    order_total::NUMBER(12,2) AS order_total_amount
FROM RETAIL_MLOPS.RAW.RAW_ORDERS
WHERE id IS NOT NULL;
