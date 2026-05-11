-- Sprint 4 Data Quality Test
-- Purpose: Verify DIM_CUSTOMERS.customer_id is unique.
-- Source: RETAIL_MLOPS.MARTS_LAYER.DIM_CUSTOMERS
-- Grain: One aggregated test result row.
-- Known limitations: Counts duplicate customer_id groups rather than listing all offending rows.

SELECT
    'DIM_CUSTOMERS unique customer_id check' AS test_name,
    COUNT(*) AS failure_count,
    'Duplicate customer_id values found in MARTS_LAYER.DIM_CUSTOMERS.' AS failure_message
FROM (
    SELECT customer_id
    FROM RETAIL_MLOPS.MARTS_LAYER.DIM_CUSTOMERS
    GROUP BY customer_id
    HAVING COUNT(*) > 1
) duplicate_groups;
