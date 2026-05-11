-- Sprint 2 Snowflake Model - STG_STORES
-- Purpose: Clean and standardize raw store data.
-- Source: RETAIL_MLOPS.RAW.RAW_STORES
-- Grain: One row per store.
-- Known limitations: Store tax rates are taken from the raw source without independent validation.

CREATE OR REPLACE TABLE STG_STORES AS
SELECT
    TRIM(id) AS store_id,
    TRIM(name) AS store_name,
    opened_at::TIMESTAMP AS opened_at,
    tax_rate::NUMBER(6,4) AS tax_rate
FROM RETAIL_MLOPS.RAW.RAW_STORES
WHERE id IS NOT NULL;
