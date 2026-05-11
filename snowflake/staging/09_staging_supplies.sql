-- Sprint 2 Snowflake Model - STG_SUPPLIES
-- Purpose: Clean and standardize raw supply data.
-- Source: RETAIL_MLOPS.RAW.RAW_SUPPLIES
-- Grain: One row per supply record.
-- Known limitations: SKU mapping is intentionally not used in downstream profitability until validated.

CREATE OR REPLACE TABLE STG_SUPPLIES AS
SELECT
    TRIM(id) AS supply_id,
    TRIM(name) AS supply_name,
    cost::NUMBER(12,2) AS supply_cost,
    TRY_TO_BOOLEAN(perishable) AS is_perishable
FROM RETAIL_MLOPS.RAW.RAW_SUPPLIES
WHERE id IS NOT NULL;
