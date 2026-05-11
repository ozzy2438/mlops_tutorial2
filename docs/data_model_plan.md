# Sprint 2 — Snowflake and dbt-style Data Model Plan

## Goal
Prepare the project structure for modelling retail sales data using a RAW → STAGING → MARTS approach.

## Data Flow
Azure Blob Storage stores the large raw CSV files.

Snowflake will be used as the cloud data warehouse.

dbt-style SQL models will transform the data into clean analytics-ready tables.

## Layers

### RAW
Original files loaded from Azure Blob Storage:
- raw_customers.csv
- raw_orders.csv
- raw_order_items.csv
- raw_products.csv
- raw_stores.csv
- raw_supplies.csv

### STAGING
Clean and rename columns from the raw files.

Example:
- stg_customers
- stg_orders
- stg_order_items
- stg_products
- stg_stores
- stg_supplies

### MARTS
Business-ready tables for reporting and analytics.

Example:
- fct_orders
- dim_customers
- dim_products
- dim_stores

## Team Split
ML Engineer focuses on future prediction and anomaly detection.

Analytics Engineer focuses on clean modelling, metrics, and business reporting.
