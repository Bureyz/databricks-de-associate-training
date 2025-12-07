-- ============================================================
-- Silver Layer: Cleaning and SCD (Slowly Changing Dimensions)
-- Lakeflow SDP Syntax with AUTO CDC
-- ============================================================

-- 1. Customers: SCD Type 2 (History Tracking)
CREATE OR REFRESH STREAMING TABLE silver_customers
COMMENT 'Cleaned customers with SCD Type 2';

AUTO CDC INTO silver_customers
FROM bronze_customers
KEYS (CustomerID)
SEQUENCE BY ModifiedDate
STORED AS SCD TYPE 2;

-- 2. Products: SCD Type 1 (Overwrite)
CREATE OR REFRESH STREAMING TABLE silver_products
COMMENT 'Cleaned products with SCD Type 1';

AUTO CDC INTO silver_products
FROM bronze_products
KEYS (ProductID)
SEQUENCE BY ModifiedDate
STORED AS SCD TYPE 1;

-- 3. Orders: Streaming Table with Data Quality Expectations
CREATE OR REFRESH STREAMING TABLE silver_orders
(
  CONSTRAINT valid_amount EXPECT (TotalDue > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer EXPECT (CustomerID IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Cleaned orders data with quality constraints'
AS SELECT 
  SalesOrderID,
  CustomerID,
  TotalDue,
  OrderDate,
  Status,
  current_timestamp() as processed_at
FROM STREAM(bronze_orders);
