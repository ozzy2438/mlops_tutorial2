-- Sprint 4 Data Quality Test
-- Purpose: Verify FCT_ORDER_ITEMS.product_id is never null.
-- Source: RETAIL_MLOPS.MARTS_LAYER.FCT_ORDER_ITEMS
-- Grain: One aggregated test result row.
-- Known limitations: Assumes MARTS_LAYER.FCT_ORDER_ITEMS is already deployed.

SELECT
    'FCT_ORDER_ITEMS product_id not null check' AS test_name,
    COUNT(*) AS failure_count,
    'Null product_id values found in MARTS_LAYER.FCT_ORDER_ITEMS.' AS failure_message
FROM RETAIL_MLOPS.MARTS_LAYER.FCT_ORDER_ITEMS
WHERE product_id IS NULL;
