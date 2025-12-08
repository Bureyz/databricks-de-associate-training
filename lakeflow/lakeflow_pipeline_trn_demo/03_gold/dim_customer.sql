------------------------------------------------------------------
-- GOLD – DIM CUSTOMER (snapshot z SCD2) jako MATERIALIZED VIEW
------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW dim_customer
AS
SELECT
  customer_id,
  first_name,
  last_name,
  email,
  phone,
  city,
  state,
  country,
  registration_date,
  customer_segment
FROM silver_customers
WHERE __END_AT IS NULL;