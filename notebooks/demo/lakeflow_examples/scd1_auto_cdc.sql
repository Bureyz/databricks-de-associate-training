CREATE FLOW silver_products_scd1_flow
AS AUTO CDC INTO silver_products
FROM bronze_products
KEYS (product_id)
SEQUENCE BY ingestion_ts
STORED AS SCD TYPE 1;  -- Overwrite without history
