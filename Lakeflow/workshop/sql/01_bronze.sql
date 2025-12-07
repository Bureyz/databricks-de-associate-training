-- ============================================================
-- Bronze Layer: Ingestion using Auto Loader (cloud_files)
-- Lakeflow SDP Syntax
-- ============================================================
-- Configuration: source_path must be set in Pipeline Settings

-- Bronze Customers
CREATE OR REFRESH STREAMING TABLE bronze_customers
COMMENT 'Raw customers data from CSV'
AS SELECT * FROM cloud_files('${source_path}/Customers', 'csv', map("header", "true", "inferSchema", "true"));

-- Bronze Products
CREATE OR REFRESH STREAMING TABLE bronze_products
COMMENT 'Raw products data from CSV'
AS SELECT * FROM cloud_files('${source_path}/Product', 'csv', map("header", "true", "inferSchema", "true"));

-- Bronze Orders
CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT 'Raw orders data from CSV'
AS SELECT * FROM cloud_files('${source_path}/SalesOrderHeader', 'csv', map("header", "true", "inferSchema", "true"));
