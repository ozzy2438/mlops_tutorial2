# Intermediate Layer Backlog

## Current Notes

- Snowsight SQL has been extracted from Snowflake query history where available.
- Product-supply profitability joins remain intentionally excluded until SKU mapping is validated.
- Historical-aware activity metrics use `dataset_last_order_at` instead of wall-clock current date where implemented.
- Raw CSV files and credentials remain outside GitHub.

## Follow-up Work

- Validate SKU mapping between products and supplies before adding profitability models.
- Decide whether these SQL models should remain table materializations or move toward dbt-managed views/tables.
- Add model tests for primary keys, accepted values, and relationship integrity.
