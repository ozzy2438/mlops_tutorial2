# Sprint 2 Snowflake Data Model Summary

## Purpose

Sprint 2 adds the Snowflake medallion-style SQL assets for the MLOps Retail Data Platform. The model is organized as RAW -> STAGING -> INT -> MARTS so raw Azure data can be loaded into Snowflake, standardized, enriched, and exposed for analytics.

## Layers

- `snowflake/setup`: database/schema setup, Azure external stage setup, and COPY INTO commands for RAW tables.
- `snowflake/staging`: cleaned source-aligned models for customers, orders, order items, products, stores, and supplies.
- `snowflake/int_layer`: enriched intermediate models with customer, order, product, store, and supply metrics.
- `snowflake/marts_layer`: star schema dimensions/facts plus `RETAIL_SALES_WIDE` for convenience reporting.

## Security

No Snowflake password, Azure SAS token, GitHub token, connection string, storage key, or `.env` file is committed. Any credential-bearing SQL is masked with placeholders such as:

```sql
AZURE_SAS_TOKEN = '<AZURE_SAS_TOKEN_PLACEHOLDER>'
```

## Known Limitations

- Product-supply profitability joins are removed/deferred until SKU mapping is validated.
- Current SQL is extracted from Snowflake objects and recent query history; future dbt conversion should add tests and materialization config.
- RAW CSV files remain in Azure Blob Storage and outside GitHub.
