-- Sprint 5 Snowflake ML Layer
-- Purpose: Score recent daily sales data with the trained anomaly detection models.
-- Source: RETAIL_MLOPS.ML_LAYER scoring tables and Snowflake ML models.
-- Grain: One scored row per metric timestamp, plus one scored row per store and day for store-level revenue.
-- Known limitations: Scores only the most recent 60 days to keep runtime small and chronologically consistent with training.

CREATE OR REPLACE TABLE RETAIL_MLOPS.ML_LAYER.DAILY_REVENUE_ANOMALY_RAW AS
SELECT
    'daily_revenue' AS metric_name,
    NULL::VARCHAR AS entity_id,
    ts::DATE AS metric_date,
    ts AS metric_ts,
    y AS actual_value,
    forecast,
    lower_bound,
    upper_bound,
    is_anomaly,
    percentile,
    distance,
    CURRENT_TIMESTAMP() AS detected_at
FROM TABLE(
    RETAIL_MLOPS.ML_LAYER.DAILY_REVENUE_ANOMALY_MODEL!DETECT_ANOMALIES(
        INPUT_DATA => TABLE(RETAIL_MLOPS.ML_LAYER.DAILY_SALES_SCORING),
        TIMESTAMP_COLNAME => 'metric_ts',
        TARGET_COLNAME => 'daily_revenue',
        CONFIG_OBJECT => {
            'prediction_interval': 0.99,
            'on_error': 'skip'
        }
    )
);

CREATE OR REPLACE TABLE RETAIL_MLOPS.ML_LAYER.DAILY_ORDER_COUNT_ANOMALY_RAW AS
SELECT
    'daily_order_count' AS metric_name,
    NULL::VARCHAR AS entity_id,
    ts::DATE AS metric_date,
    ts AS metric_ts,
    y AS actual_value,
    forecast,
    lower_bound,
    upper_bound,
    is_anomaly,
    percentile,
    distance,
    CURRENT_TIMESTAMP() AS detected_at
FROM TABLE(
    RETAIL_MLOPS.ML_LAYER.DAILY_ORDER_COUNT_ANOMALY_MODEL!DETECT_ANOMALIES(
        INPUT_DATA => TABLE(RETAIL_MLOPS.ML_LAYER.DAILY_SALES_SCORING),
        TIMESTAMP_COLNAME => 'metric_ts',
        TARGET_COLNAME => 'daily_order_count',
        CONFIG_OBJECT => {
            'prediction_interval': 0.99,
            'on_error': 'skip'
        }
    )
);

CREATE OR REPLACE TABLE RETAIL_MLOPS.ML_LAYER.STORE_DAILY_REVENUE_ANOMALY_RAW AS
SELECT
    'store_daily_revenue' AS metric_name,
    TO_VARCHAR(series) AS entity_id,
    ts::DATE AS metric_date,
    ts AS metric_ts,
    y AS actual_value,
    forecast,
    lower_bound,
    upper_bound,
    is_anomaly,
    percentile,
    distance,
    CURRENT_TIMESTAMP() AS detected_at
FROM TABLE(
    RETAIL_MLOPS.ML_LAYER.STORE_DAILY_REVENUE_ANOMALY_MODEL!DETECT_ANOMALIES(
        INPUT_DATA => TABLE(RETAIL_MLOPS.ML_LAYER.STORE_DAILY_REVENUE_SCORING),
        SERIES_COLNAME => 'store_series',
        TIMESTAMP_COLNAME => 'metric_ts',
        TARGET_COLNAME => 'daily_revenue',
        CONFIG_OBJECT => {
            'prediction_interval': 0.99,
            'on_error': 'skip'
        }
    )
);

