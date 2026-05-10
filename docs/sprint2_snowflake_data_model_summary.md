# Sprint 2 Snowflake Data Model Summary

## Purpose

Sprint 2 prepares the Snowflake SQL structure for the MLOps Retail Data Platform.
The model follows a RAW -> STAGING -> INTERMEDIATE -> MARTS flow so the team can
load raw files safely, clean them in predictable layers, and build analytics-ready
tables for reporting and future machine learning work.

## Folder Structure

- `snowflake/setup`: Snowflake setup, Azure stage, and raw table load scripts.
- `snowflake/staging`: SQL files for cleaned staging models.
- `snowflake/int_layer`: SQL files for intermediate business logic models.
- `snowflake/marts_layer`: SQL files for dimensional and fact models.

## Security Rules

No real secrets should be committed to GitHub. This includes Azure SAS tokens,
Azure connection strings, storage account keys, passwords, and `.env` files.

When SQL needs a token or credential, use a placeholder such as:

```sql
AZURE_SAS_TOKEN = '<AZURE_SAS_TOKEN_PLACEHOLDER>'
```

Raw CSV files remain outside GitHub and should continue to live in Azure Blob
Storage.

## Next Step

The SQL files currently contain safe headers and placeholders. The actual
Snowflake SQL contents should be added one file at a time after review, with all
secrets replaced by placeholders before committing.
