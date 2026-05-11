-- Sprint 5 Snowflake ML Layer
-- Purpose: Train Snowflake ML anomaly detection models for daily revenue, daily order count, and store-level revenue.
-- Source: RETAIL_MLOPS.ML_LAYER training tables.
-- Grain: Model objects stored in the ML_LAYER schema.
-- Known limitations: Models are retrained on each deployment and use unsupervised labels with an empty label column.

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION RETAIL_MLOPS.ML_LAYER.DAILY_REVENUE_ANOMALY_MODEL(
    INPUT_DATA => TABLE(RETAIL_MLOPS.ML_LAYER.DAILY_SALES_TRAINING),
    TIMESTAMP_COLNAME => 'metric_ts',
    TARGET_COLNAME => 'daily_revenue',
    LABEL_COLNAME => '',
    CONFIG_OBJECT => {
        'frequency': '1 day',
        'on_error': 'skip',
        'evaluate': FALSE
    }
)
COMMENT = 'Detects anomalous total daily retail revenue from the marts layer.';

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION RETAIL_MLOPS.ML_LAYER.DAILY_ORDER_COUNT_ANOMALY_MODEL(
    INPUT_DATA => TABLE(RETAIL_MLOPS.ML_LAYER.DAILY_SALES_TRAINING),
    TIMESTAMP_COLNAME => 'metric_ts',
    TARGET_COLNAME => 'daily_order_count',
    LABEL_COLNAME => '',
    CONFIG_OBJECT => {
        'frequency': '1 day',
        'on_error': 'skip',
        'evaluate': FALSE
    }
)
COMMENT = 'Detects anomalous daily retail order counts from the marts layer.';

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION RETAIL_MLOPS.ML_LAYER.STORE_DAILY_REVENUE_ANOMALY_MODEL(
    INPUT_DATA => TABLE(RETAIL_MLOPS.ML_LAYER.STORE_DAILY_REVENUE_TRAINING),
    SERIES_COLNAME => 'store_series',
    TIMESTAMP_COLNAME => 'metric_ts',
    TARGET_COLNAME => 'daily_revenue',
    LABEL_COLNAME => '',
    CONFIG_OBJECT => {
        'frequency': '1 day',
        'on_error': 'skip',
        'evaluate': FALSE
    }
)
COMMENT = 'Detects anomalous store-level daily retail revenue from the marts layer.';

