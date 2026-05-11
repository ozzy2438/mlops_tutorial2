-- Sprint 4 Data Quality Test
-- Purpose: Verify FCT_ORDERS.store_id resolves to DIM_STORES.store_id.
-- Source: RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS and DIM_STORES
-- Grain: One aggregated test result row.
-- Known limitations: Null store_id values are counted here as FK failures if they bypass upstream checks.

SELECT
    'FCT_ORDERS FK to DIM_STORES check' AS test_name,
    COUNT(*) AS failure_count,
    'Unmatched store_id values found between FCT_ORDERS and DIM_STORES.' AS failure_message
FROM RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS f
LEFT JOIN RETAIL_MLOPS.MARTS_LAYER.DIM_STORES d
    ON f.store_id = d.store_id
WHERE d.store_id IS NULL;
