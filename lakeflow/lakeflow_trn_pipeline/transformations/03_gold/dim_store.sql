------------------------------------------------------------------
-- GOLD – DIM STORE (MV)
------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW dim_store
AS
SELECT DISTINCT
  store_id,
  store_id AS store_code
FROM silver_orders;