-- Sprint 4 Data Quality Test
-- Purpose: Verify FCT_ORDER_ITEMS does not contain duplicate fact-grain rows.
-- Source: RETAIL_MLOPS.MARTS_LAYER.FCT_ORDER_ITEMS
-- Grain: One aggregated test result row.
-- Known limitations: Uses order_item_id as the fact grain identifier for duplicate detection.

SELECT
    'FCT_ORDER_ITEMS duplicate fact grain check' AS test_name,
    COUNT(*) AS failure_count,
    'Duplicate order_item_id values found in MARTS_LAYER.FCT_ORDER_ITEMS.' AS failure_message
FROM (
    SELECT order_item_id
    FROM RETAIL_MLOPS.MARTS_LAYER.FCT_ORDER_ITEMS
    GROUP BY order_item_id
    HAVING COUNT(*) > 1
) duplicate_grain_rows;
