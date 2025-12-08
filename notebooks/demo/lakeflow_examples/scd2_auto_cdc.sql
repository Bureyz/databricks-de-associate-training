-- Creating target table SCD2
CREATE OR REFRESH STREAMING TABLE silver_customers (
  customer_id        STRING,
  first_name         STRING,
  last_name          STRING,
  city               STRING,
  -- SCD2 columns added automatically:
  __START_AT         TIMESTAMP,
  __END_AT           TIMESTAMP
);

-- Flow with AUTO CDC for SCD2
CREATE FLOW silver_customers_scd2_flow
AS AUTO CDC INTO silver_customers
FROM STREAM bronze_customers
KEYS (customer_id)         -- Business key
SEQUENCE BY ingestion_ts   -- Column determining the order
STORED AS SCD TYPE 2;      -- SCD Type
