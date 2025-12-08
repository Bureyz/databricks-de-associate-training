CREATE OR REFRESH STREAMING TABLE silver_orders
(
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_quantity EXPECT (quantity > 0)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_price EXPECT (unit_price >= 0)
    ON VIOLATION FAIL UPDATE
)
AS
SELECT
  order_id,
  customer_id,
  product_id,
  CAST(order_datetime AS TIMESTAMP) AS order_ts,
  quantity,
  unit_price,
  (quantity * unit_price) AS gross_amount
FROM STREAM(bronze_orders);
