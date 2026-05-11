-- Sprint 5 Snowflake ML Layer
-- Purpose: Publish lightweight validation checks for anomaly output tables.
-- Source: RETAIL_MLOPS.ML_LAYER anomaly tables.
-- Grain: One row per validation check.
-- Known limitations: Designed as a reviewer-facing check view, not a hard-fail CI test.

CREATE OR REPLACE VIEW RETAIL_MLOPS.ML_LAYER.VW_ANOMALY_OUTPUT_VALIDATION AS
SELECT
    'daily_revenue_scored_rows' AS check_name,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS check_status,
    COUNT(*) AS observed_value,
    'Daily revenue scoring output should contain at least one row.' AS details
FROM RETAIL_MLOPS.ML_LAYER.DAILY_REVENUE_ANOMALY_RAW

UNION ALL

SELECT
    'daily_order_count_scored_rows' AS check_name,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS check_status,
    COUNT(*) AS observed_value,
    'Daily order-count scoring output should contain at least one row.' AS details
FROM RETAIL_MLOPS.ML_LAYER.DAILY_ORDER_COUNT_ANOMALY_RAW

UNION ALL

SELECT
    'store_daily_revenue_scored_rows' AS check_name,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS check_status,
    COUNT(*) AS observed_value,
    'Store-level daily revenue scoring output should contain at least one row.' AS details
FROM RETAIL_MLOPS.ML_LAYER.STORE_DAILY_REVENUE_ANOMALY_RAW

UNION ALL

SELECT
    'retail_sales_anomaly_rows' AS check_name,
    CASE WHEN COUNT(*) >= 0 THEN 'PASS' ELSE 'FAIL' END AS check_status,
    COUNT(*) AS observed_value,
    'Consolidated anomaly table should be materialized even when anomaly count is zero.' AS details
FROM RETAIL_MLOPS.ML_LAYER.RETAIL_SALES_ANOMALIES

UNION ALL

SELECT
    'retail_sales_anomaly_null_dates' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_status,
    COUNT(*) AS observed_value,
    'Consolidated anomaly rows should not have null metric_date values.' AS details
FROM RETAIL_MLOPS.ML_LAYER.RETAIL_SALES_ANOMALIES
WHERE metric_date IS NULL;

