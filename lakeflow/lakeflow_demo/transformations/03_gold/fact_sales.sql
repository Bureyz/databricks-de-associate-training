------------------------------------------------------------------
-- GOLD – FACT FCT_SALES (STREAMING TABLE)
------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE fact_sales
AS
SELECT
  -- klucze do wymiarów (naturalne)
  o.order_id,
  o.store_id,
  COALESCE(o.customer_id, 'UNKNOWN')  AS customer_id,
  COALESCE(o.product_id, 'UNKNOWN')   AS product_id,
  o.payment_method_code,
  CAST(date_format(o.order_date, 'yyyyMMdd') AS INT) AS order_date_key,
  o.order_ts,

  -- miary
  o.quantity,
  o.unit_price,
  o.discount_percent,
  o.gross_amount,
  o.discount_amount,
  o.net_amount,

  -- flagi
  o.is_return,
  o.is_future_dated,
  o.is_unknown_customer,
  o.is_unknown_product,

  -- lineage
  o.source_system
FROM STREAM(silver_orders) o