-- Sprint 2 Snowflake Model - DIM_PRODUCTS
-- Purpose: Publish product dimension for analytics.
-- Source: RETAIL_MLOPS.INT_LAYER.INT_PRODUCTS
-- Grain: One row per product.
-- Known limitations: No supply profitability metrics are included until SKU mapping is validated.

CREATE OR REPLACE TABLE RETAIL_MLOPS.MARTS_LAYER.DIM_PRODUCTS AS
SELECT product_id, product_name, product_type, product_price, product_description,
    times_ordered, total_quantity_sold, unique_customers, first_sold_at, last_sold_at,
    estimated_total_revenue, price_tier, popularity_segment, product_last_sold_status, loaded_at
FROM RETAIL_MLOPS.INT_LAYER.INT_PRODUCTS;
