-- Sprint 5 Snowflake ML Layer
-- Purpose: Build low-cost aggregated training and scoring tables for anomaly detection.
-- Source: RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS
-- Grain: Daily metric rows and store-by-day revenue rows.
-- Known limitations: Uses the last 60 days as the scoring window so anomaly detection respects chronological scoring requirements.

CREATE OR REPLACE TABLE RETAIL_MLOPS.ML_LAYER.DAILY_SALES_TRAINING AS
WITH daily_sales AS (
    SELECT
        order_date,
        TO_TIMESTAMP_NTZ(order_date) AS metric_ts,
        SUM(order_total_amount) AS daily_revenue,
        COUNT(*) AS daily_order_count
    FROM RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS
    GROUP BY 1, 2
),
dataset_boundary AS (
    SELECT DATEADD(DAY, -60, MAX(order_date)) AS scoring_start_date
    FROM daily_sales
)
SELECT
    metric_ts,
    order_date,
    daily_revenue,
    daily_order_count
FROM daily_sales
WHERE order_date < (SELECT scoring_start_date FROM dataset_boundary);

CREATE OR REPLACE TABLE RETAIL_MLOPS.ML_LAYER.DAILY_SALES_SCORING AS
WITH daily_sales AS (
    SELECT
        order_date,
        TO_TIMESTAMP_NTZ(order_date) AS metric_ts,
        SUM(order_total_amount) AS daily_revenue,
        COUNT(*) AS daily_order_count
    FROM RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS
    GROUP BY 1, 2
),
dataset_boundary AS (
    SELECT DATEADD(DAY, -60, MAX(order_date)) AS scoring_start_date
    FROM daily_sales
)
SELECT
    metric_ts,
    order_date,
    daily_revenue,
    daily_order_count
FROM daily_sales
WHERE order_date >= (SELECT scoring_start_date FROM dataset_boundary);

CREATE OR REPLACE TABLE RETAIL_MLOPS.ML_LAYER.STORE_DAILY_REVENUE_TRAINING AS
WITH store_daily_sales AS (
    SELECT
        order_date,
        TO_TIMESTAMP_NTZ(order_date) AS metric_ts,
        TO_VARIANT(store_id) AS store_series,
        store_id,
        SUM(order_total_amount) AS daily_revenue
    FROM RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS
    GROUP BY 1, 2, 3, 4
),
dataset_boundary AS (
    SELECT DATEADD(DAY, -60, MAX(order_date)) AS scoring_start_date
    FROM store_daily_sales
)
SELECT
    metric_ts,
    order_date,
    store_series,
    store_id,
    daily_revenue
FROM store_daily_sales
WHERE order_date < (SELECT scoring_start_date FROM dataset_boundary);

CREATE OR REPLACE TABLE RETAIL_MLOPS.ML_LAYER.STORE_DAILY_REVENUE_SCORING AS
WITH store_daily_sales AS (
    SELECT
        order_date,
        TO_TIMESTAMP_NTZ(order_date) AS metric_ts,
        TO_VARIANT(store_id) AS store_series,
        store_id,
        SUM(order_total_amount) AS daily_revenue
    FROM RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS
    GROUP BY 1, 2, 3, 4
),
dataset_boundary AS (
    SELECT DATEADD(DAY, -60, MAX(order_date)) AS scoring_start_date
    FROM store_daily_sales
)
SELECT
    metric_ts,
    order_date,
    store_series,
    store_id,
    daily_revenue
FROM store_daily_sales
WHERE order_date >= (SELECT scoring_start_date FROM dataset_boundary);

