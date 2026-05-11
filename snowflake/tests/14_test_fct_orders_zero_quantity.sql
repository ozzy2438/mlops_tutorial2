-- Sprint 4 Data Quality Test
-- Purpose: Separate true item-count inconsistencies from documented zero-value
-- source order headers that arrive without item rows.
-- Source: RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS
-- Grain: One aggregated test result row.
-- Known limitations: Uses total_items as the quantity proxy because no explicit
-- order quantity column exists at order grain. Source order headers with
-- total_items = 0 and zero financial amounts are treated as historical
-- exceptions and reported as warnings, not hard failures.

WITH profiled_orders AS (
    SELECT
        SUM(
            CASE
                WHEN total_items IS NULL THEN 1
                WHEN total_items < 0 THEN 1
                WHEN total_items = 0
                     AND (
                         COALESCE(subtotal_amount, 0) <> 0
                         OR COALESCE(tax_amount, 0) <> 0
                         OR COALESCE(order_total_amount, 0) <> 0
                     ) THEN 1
                ELSE 0
            END
        ) AS hard_fail_count,
        SUM(
            CASE
                WHEN total_items = 0
                     AND COALESCE(subtotal_amount, 0) = 0
                     AND COALESCE(tax_amount, 0) = 0
                     AND COALESCE(order_total_amount, 0) = 0 THEN 1
                ELSE 0
            END
        ) AS warning_count
    FROM RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS
)
SELECT
    'FCT_ORDERS zero quantity hard-fail check' AS test_name,
    hard_fail_count AS failure_count,
    'Null or negative item counts, or zero-item orders with non-zero financial amounts found in MARTS_LAYER.FCT_ORDERS.' AS failure_message,
    'FAIL' AS severity
FROM profiled_orders

UNION ALL

SELECT
    'FCT_ORDERS zero quantity historical exception' AS test_name,
    warning_count AS failure_count,
    'Zero-item orders with zero subtotal, zero tax, and zero order total remain in MARTS_LAYER.FCT_ORDERS as documented source header exceptions.' AS failure_message,
    'WARN' AS severity
FROM profiled_orders;
