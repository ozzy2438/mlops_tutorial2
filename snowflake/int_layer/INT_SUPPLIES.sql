-- Sprint 2 Snowflake Model - INT_SUPPLIES
-- Purpose: Build deduplicated supply records with cost segments.
-- Source: RETAIL_MLOPS.STAGING.STG_SUPPLIES
-- Grain: One row per distinct supply record.
-- Known limitations: Product-supply profitability logic is excluded until SKU mapping is validated.

CREATE OR REPLACE TABLE RETAIL_MLOPS.INT_LAYER.INT_SUPPLIES AS
WITH supply_catalog AS (SELECT DISTINCT supply_id, supply_name, supply_cost, is_perishable FROM RETAIL_MLOPS.STAGING.STG_SUPPLIES)
SELECT supply_id, supply_name, supply_cost, is_perishable,
    CASE WHEN supply_cost >= 5 THEN 'High Cost' WHEN supply_cost >= 2 THEN 'Medium Cost' ELSE 'Low Cost' END AS cost_segment,
    CURRENT_TIMESTAMP() AS loaded_at
FROM supply_catalog;
