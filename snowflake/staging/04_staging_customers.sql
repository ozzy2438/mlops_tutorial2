-- Sprint 2 Snowflake Model - STG_CUSTOMERS
-- Purpose: Clean and standardize raw customer data.
-- Source: RETAIL_MLOPS.RAW.RAW_CUSTOMERS
-- Grain: One row per customer.
-- Known limitations: Derived from Snowsight query history; source raw customer table only includes id/name fields.

CREATE OR REPLACE TABLE STG_CUSTOMERS AS
SELECT
    TRIM(id) AS customer_id,
    TRIM(name) AS customer_name,
    TRIM(SPLIT_PART(name, ' ', 1)) AS first_name,
    TRIM(SPLIT_PART(name, ' ', 2)) AS last_name
FROM RETAIL_MLOPS.RAW.RAW_CUSTOMERS
WHERE id IS NOT NULL;
