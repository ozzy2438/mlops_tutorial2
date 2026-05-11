-- Sprint 4 Data Quality Test
-- Purpose: Verify FCT_ORDER_ITEMS.product_id resolves to DIM_PRODUCTS.product_id.
-- Source: RETAIL_MLOPS.MARTS_LAYER.FCT_ORDER_ITEMS and DIM_PRODUCTS
-- Grain: One aggregated test result row.
-- Known limitations: Null product_id values are counted here as FK failures if they bypass upstream checks.

SELECT
    'FCT_ORDER_ITEMS FK to DIM_PRODUCTS check' AS test_name,
    COUNT(*) AS failure_count,
    'Unmatched product_id values found between FCT_ORDER_ITEMS and DIM_PRODUCTS.' AS failure_message
FROM RETAIL_MLOPS.MARTS_LAYER.FCT_ORDER_ITEMS f
LEFT JOIN RETAIL_MLOPS.MARTS_LAYER.DIM_PRODUCTS d
    ON f.product_id = d.product_id
WHERE d.product_id IS NULL;
