-- Sprint 4 Data Quality Test
-- Purpose: Verify FCT_ORDERS.order_id is unique at fact grain.
-- Source: RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS
-- Grain: One aggregated test result row.
-- Known limitations: Counts duplicate order_id groups rather than listing all offending rows.

SELECT
    'FCT_ORDERS unique order_id check' AS test_name,
    COUNT(*) AS failure_count,
    'Duplicate order_id values found in MARTS_LAYER.FCT_ORDERS.' AS failure_message
FROM (
    SELECT order_id
    FROM RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS
    GROUP BY order_id
    HAVING COUNT(*) > 1
) duplicate_groups;
