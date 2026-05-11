-- Sprint 4 Data Quality Test
-- Purpose: Verify DIM_CUSTOMERS.customer_id is never null.
-- Source: RETAIL_MLOPS.MARTS_LAYER.DIM_CUSTOMERS
-- Grain: One aggregated test result row.
-- Known limitations: Assumes MARTS_LAYER.DIM_CUSTOMERS is already deployed.

SELECT
    'DIM_CUSTOMERS customer_id not null check' AS test_name,
    COUNT(*) AS failure_count,
    'Null customer_id values found in MARTS_LAYER.DIM_CUSTOMERS.' AS failure_message
FROM RETAIL_MLOPS.MARTS_LAYER.DIM_CUSTOMERS
WHERE customer_id IS NULL;
