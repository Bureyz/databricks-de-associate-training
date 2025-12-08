-- Dimension - current snapshot from SCD2
CREATE OR REFRESH MATERIALIZED VIEW dim_customer
AS
SELECT
  customer_id,
  first_name,
  last_name,
  email,
  city,
  customer_segment
FROM silver_customers
WHERE __END_AT IS NULL;

-- Date Dimension
CREATE OR REFRESH MATERIALIZED VIEW dim_date
AS
SELECT DISTINCT
  CAST(date_format(order_date, 'yyyyMMdd') AS INT) AS date_key,
  order_date AS date,
  year(order_date) AS year,
  quarter(order_date) AS quarter,
  month(order_date) AS month
FROM silver_orders;

-- Fact - streaming from Silver
CREATE OR REFRESH STREAMING TABLE fact_sales
AS
SELECT
  order_id,
  customer_id,
  product_id,
  order_date_key,
  quantity,
  gross_amount,
  net_amount
FROM STREAM(silver_orders);
