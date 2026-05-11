-- Sprint 4 Data Quality Test
-- Purpose: Verify DIM_PRODUCTS.product_id is unique.
-- Source: RETAIL_MLOPS.MARTS_LAYER.DIM_PRODUCTS
-- Grain: One aggregated test result row.
-- Known limitations: Counts duplicate product_id groups rather than listing all offending rows.

SELECT
    'DIM_PRODUCTS unique product_id check' AS test_name,
    COUNT(*) AS failure_count,
    'Duplicate product_id values found in MARTS_LAYER.DIM_PRODUCTS.' AS failure_message
FROM (
    SELECT product_id
    FROM RETAIL_MLOPS.MARTS_LAYER.DIM_PRODUCTS
    GROUP BY product_id
    HAVING COUNT(*) > 1
) duplicate_groups;
