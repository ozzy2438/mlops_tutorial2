-- Sprint 4 Data Quality Test
-- Purpose: Verify FCT_ORDERS does not contain future order dates.
-- Source: RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS
-- Grain: One aggregated test result row.
-- Known limitations: Compares order_date against CURRENT_DATE() at validation runtime.

SELECT
    'FCT_ORDERS future date check' AS test_name,
    COUNT(*) AS failure_count,
    'Future order_date values found in MARTS_LAYER.FCT_ORDERS.' AS failure_message
FROM RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS
WHERE order_date > CURRENT_DATE();
