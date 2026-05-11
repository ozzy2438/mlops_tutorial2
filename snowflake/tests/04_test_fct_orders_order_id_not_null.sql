-- Sprint 4 Data Quality Test
-- Purpose: Verify FCT_ORDERS.order_id is never null.
-- Source: RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS
-- Grain: One aggregated test result row.
-- Known limitations: Assumes MARTS_LAYER.FCT_ORDERS is already deployed.

SELECT
    'FCT_ORDERS order_id not null check' AS test_name,
    COUNT(*) AS failure_count,
    'Null order_id values found in MARTS_LAYER.FCT_ORDERS.' AS failure_message
FROM RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS
WHERE order_id IS NULL;
