-- Sprint 2 Snowflake Setup - Azure External Stage
-- Purpose: Create the CSV file format and Azure Blob Storage external stage used to load raw retail CSV files.
-- Source: Snowflake file format metadata and SHOW STAGES output for RETAIL_MLOPS.RAW.AZURE_RAW_STAGE.
-- Grain: One external stage pointing at the mlops-data Azure container.
-- Known limitations: The real Azure SAS token is intentionally replaced with a placeholder and must be supplied securely at runtime.

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
	SKIP_HEADER = 1
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
;

CREATE OR REPLACE STAGE RETAIL_MLOPS.RAW.AZURE_RAW_STAGE
    URL = 'azure://ozzymldata2410sa.blob.core.windows.net/mlops-data'
    CREDENTIALS = (AZURE_SAS_TOKEN = '<AZURE_SAS_TOKEN_PLACEHOLDER>')
    FILE_FORMAT = RETAIL_MLOPS.RAW.CSV_FORMAT;
