-- Sprint 5 Data Quality Test
-- Purpose: Verify the Snowflake ML anomaly layer produces scored output tables.
-- Source: RETAIL_MLOPS.ML_LAYER anomaly raw tables
-- Grain: One aggregated test result row.
-- Known limitations: Checks for scored output rows, not a minimum anomaly count.

SELECT
    'ML retail sales anomaly output check' AS test_name,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM RETAIL_MLOPS.ML_LAYER.DAILY_REVENUE_ANOMALY_RAW
        )
         AND EXISTS (
            SELECT 1
            FROM RETAIL_MLOPS.ML_LAYER.DAILY_ORDER_COUNT_ANOMALY_RAW
        )
         AND EXISTS (
            SELECT 1
            FROM RETAIL_MLOPS.ML_LAYER.STORE_DAILY_REVENUE_ANOMALY_RAW
        )
        THEN 0
        ELSE 1
    END AS failure_count,
    'Snowflake ML anomaly detection did not produce one or more scored output tables.' AS failure_message;

