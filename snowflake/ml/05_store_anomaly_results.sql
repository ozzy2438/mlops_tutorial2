-- Sprint 5 Snowflake ML Layer
-- Purpose: Store a compact anomaly result set for downstream review.
-- Source: RETAIL_MLOPS.ML_LAYER anomaly scoring tables.
-- Grain: One row per detected anomaly.
-- Known limitations: Stores only rows flagged as anomalies to keep stakeholder-facing outputs compact.

CREATE OR REPLACE TABLE RETAIL_MLOPS.ML_LAYER.RETAIL_SALES_ANOMALIES AS
SELECT
    metric_name,
    entity_id,
    metric_date,
    metric_ts,
    actual_value,
    forecast,
    lower_bound,
    upper_bound,
    percentile,
    distance,
    detected_at
FROM RETAIL_MLOPS.ML_LAYER.DAILY_REVENUE_ANOMALY_RAW
WHERE is_anomaly

UNION ALL

SELECT
    metric_name,
    entity_id,
    metric_date,
    metric_ts,
    actual_value,
    forecast,
    lower_bound,
    upper_bound,
    percentile,
    distance,
    detected_at
FROM RETAIL_MLOPS.ML_LAYER.DAILY_ORDER_COUNT_ANOMALY_RAW
WHERE is_anomaly

UNION ALL

SELECT
    metric_name,
    entity_id,
    metric_date,
    metric_ts,
    actual_value,
    forecast,
    lower_bound,
    upper_bound,
    percentile,
    distance,
    detected_at
FROM RETAIL_MLOPS.ML_LAYER.STORE_DAILY_REVENUE_ANOMALY_RAW
WHERE is_anomaly;

