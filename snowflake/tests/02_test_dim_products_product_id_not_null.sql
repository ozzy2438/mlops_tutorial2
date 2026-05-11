-- Sprint 4 Data Quality Test
-- Purpose: Verify DIM_PRODUCTS.product_id is never null.
-- Source: RETAIL_MLOPS.MARTS_LAYER.DIM_PRODUCTS
-- Grain: One aggregated test result row.
-- Known limitations: Assumes MARTS_LAYER.DIM_PRODUCTS is already deployed.

SELECT
    'DIM_PRODUCTS product_id not null check' AS test_name,
    COUNT(*) AS failure_count,
    'Null product_id values found in MARTS_LAYER.DIM_PRODUCTS.' AS failure_message
FROM RETAIL_MLOPS.MARTS_LAYER.DIM_PRODUCTS
WHERE product_id IS NULL;
