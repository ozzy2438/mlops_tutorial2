-- Sprint 4 Data Quality Test
-- Purpose: Verify FCT_ORDERS does not contain orders with zero or negative item counts.
-- Source: RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS
-- Grain: One aggregated test result row.
-- Known limitations: Uses total_items as the quantity proxy because no explicit order quantity column exists at order grain.

SELECT
    'FCT_ORDERS zero quantity check' AS test_name,
    COUNT(*) AS failure_count,
    'Zero or negative total_items values found in MARTS_LAYER.FCT_ORDERS.' AS failure_message
FROM RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS
WHERE total_items IS NULL
   OR total_items <= 0;
