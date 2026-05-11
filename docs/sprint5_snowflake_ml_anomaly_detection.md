# Sprint 5 Snowflake ML Anomaly Detection

## Goal

Sprint 5 adds a lightweight Snowflake ML layer that detects anomalous retail
sales behavior directly from the marts layer.

The first use cases are:

- daily total revenue anomalies
- daily order-volume anomalies
- optional store-level daily revenue anomalies

## Source Data

The ML layer reads from:

- `RETAIL_MLOPS.MARTS_LAYER.FCT_ORDERS`

No Azure ingestion or external stage access is required for this sprint.

## Folder Structure

Snowflake ML SQL now lives under:

- `snowflake/ml/`

The Sprint 5 documentation for this layer lives at:

- `docs/sprint5_snowflake_ml_anomaly_detection.md`

## Objects Added

The implementation creates:

- `RETAIL_MLOPS.ML_LAYER` schema
- daily aggregated training and scoring tables
- one anomaly model for daily revenue
- one anomaly model for daily order count
- one multi-series anomaly model for store-level daily revenue
- raw scoring output tables
- a consolidated anomaly result table
- a validation view for ML output

## Design Choices

### Low-Cost Training

To keep cost low, the SQL builds small aggregated daily tables instead of
training on order-level data.

### Chronological Split

The scoring window uses the most recent 60 days, while older data is used for
training. This follows Snowflake anomaly detection guidance that scoring data
should chronologically follow training data.

### Small Warehouse

The design assumes the existing `DEV_WH` warehouse remains sufficient because:

- daily tables are small
- store-level data is still heavily aggregated
- model retraining stays limited to a few series

## SQL Flow

1. `01_create_ml_layer.sql`
   - creates the `ML_LAYER` schema
2. `02_create_daily_sales_training_tables.sql`
   - builds daily and store-level training/scoring tables
3. `03_create_anomaly_detection_models.sql`
   - trains Snowflake ML anomaly detection models
4. `04_run_anomaly_detection.sql`
   - scores recent daily data for anomalies
5. `05_store_anomaly_results.sql`
   - stores only detected anomalies in a compact consolidated table
6. `06_validate_anomaly_output.sql`
   - creates a validation view for reviewer checks

## Result Tables

The main reviewer-facing table is:

- `RETAIL_MLOPS.ML_LAYER.RETAIL_SALES_ANOMALIES`

It stores anomaly rows with:

- metric name
- optional entity identifier
- anomaly date and timestamp
- actual value
- forecast
- prediction bounds
- percentile
- anomaly distance
- detection timestamp

## Validation

A lightweight validation test was added so CI/CD can confirm that the anomaly
layer produced scored output tables:

- `snowflake/tests/17_test_ml_retail_sales_anomalies_output.sql`

This check validates that the ML layer executed and materialized scoring
results. It does not enforce a minimum anomaly count, because zero anomalies can
still be a valid outcome.

## Security

- no secrets are hardcoded
- no Azure SAS token is used
- no password or connection string is committed
- the ML layer depends only on already-deployed marts data

## Reviewer Notes

Reviewers should focus on:

- whether the training/scoring split is appropriate for current data volume
- whether `prediction_interval = 0.99` is too strict or too loose
- whether the consolidated anomaly table should stay anomaly-only or retain all
  scored rows
- whether future work should add feature enrichment such as holidays, promotions,
  or store attributes
