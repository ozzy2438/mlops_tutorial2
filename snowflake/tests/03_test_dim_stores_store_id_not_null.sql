-- Sprint 4 Data Quality Test
-- Purpose: Verify DIM_STORES.store_id is never null.
-- Source: RETAIL_MLOPS.MARTS_LAYER.DIM_STORES
-- Grain: One aggregated test result row.
-- Known limitations: Assumes MARTS_LAYER.DIM_STORES is already deployed.

SELECT
    'DIM_STORES store_id not null check' AS test_name,
    COUNT(*) AS failure_count,
    'Null store_id values found in MARTS_LAYER.DIM_STORES.' AS failure_message
FROM RETAIL_MLOPS.MARTS_LAYER.DIM_STORES
WHERE store_id IS NULL;
