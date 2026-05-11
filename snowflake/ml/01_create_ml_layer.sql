-- Sprint 5 Snowflake ML Layer
-- Purpose: Create a dedicated schema for Snowflake ML assets and anomaly output tables.
-- Source: RETAIL_MLOPS analytics database.
-- Grain: Schema-level setup.
-- Known limitations: Assumes the executing role can create schemas and Snowflake ML objects.

CREATE SCHEMA IF NOT EXISTS RETAIL_MLOPS.ML_LAYER;

