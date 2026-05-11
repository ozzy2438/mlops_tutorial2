-- Sprint 4 Data Quality Test
-- Purpose: Verify FCT_ORDERS.customer_id resolves to DIM_CUSTOMERS.customer_id.
-- Source: RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS and DIM_CUSTOMERS
-- Grain: One aggregated test result row.
-- Known limitations: Null customer_id values are counted here as FK failures if they bypass not-null checks.

SELECT
    'FCT_ORDERS FK to DIM_CUSTOMERS check' AS test_name,
    COUNT(*) AS failure_count,
    'Unmatched customer_id values found between FCT_ORDERS and DIM_CUSTOMERS.' AS failure_message
FROM RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS f
LEFT JOIN RETAIL_MLOPS.MARTS_LAYER.DIM_CUSTOMERS d
    ON f.customer_id = d.customer_id
WHERE d.customer_id IS NULL;
