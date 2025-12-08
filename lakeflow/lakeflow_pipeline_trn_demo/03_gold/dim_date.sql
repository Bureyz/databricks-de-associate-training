
------------------------------------------------------------------
-- GOLD – DIM DATE (MV)
------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW dim_date
AS
SELECT DISTINCT
  CAST(date_format(order_date, 'yyyyMMdd') AS INT) AS date_key,
  order_date                                AS date,
  year(order_date)                          AS year,
  quarter(order_date)                       AS quarter,
  month(order_date)                         AS month,
  day(order_date)                           AS day,
  date_format(order_date, 'E')              AS day_of_week,
  CASE 
    WHEN date_format(order_date, 'E') IN ('Sat', 'Sun') 
      THEN 1 ELSE 0 
  END                                       AS is_weekend
FROM silver_orders;
