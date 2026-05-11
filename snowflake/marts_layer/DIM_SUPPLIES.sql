-- Sprint 2 Snowflake Model - DIM_SUPPLIES
-- Purpose: Publish supplies dimension for analytics.
-- Source: RETAIL_MLOPS.INT_LAYER.INT_SUPPLIES
-- Grain: One row per supply record.
-- Known limitations: Not joined to products for profitability until SKU mapping is validated.

CREATE OR REPLACE TABLE RETAIL_MLOPS.MARTS_LAYER.DIM_SUPPLIES AS
SELECT supply_id, supply_name, supply_cost, is_perishable, cost_segment, loaded_at
FROM RETAIL_MLOPS.INT_LAYER.INT_SUPPLIES;
