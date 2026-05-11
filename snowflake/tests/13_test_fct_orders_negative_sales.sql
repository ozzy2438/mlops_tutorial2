-- Sprint 4 Data Quality Test
-- Purpose: Verify FCT_ORDERS does not contain negative sales or tax values.
-- Source: RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS
-- Grain: One aggregated test result row.
-- Known limitations: Flags any negative subtotal, tax, or order total regardless of adjustment semantics.

SELECT
    'FCT_ORDERS negative sales amount check' AS test_name,
    COUNT(*) AS failure_count,
    'Negative subtotal, tax, or order total values found in MARTS_LAYER.FCT_ORDERS.' AS failure_message
FROM RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS
WHERE subtotal_amount < 0
   OR tax_amount < 0
   OR order_total_amount < 0;
