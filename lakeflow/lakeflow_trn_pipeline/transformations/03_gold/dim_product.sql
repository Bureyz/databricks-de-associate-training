------------------------------------------------------------------
-- GOLD – DIM PRODUCT (MV)
------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW dim_product
AS
SELECT
  product_id,
  product_name,
  subcategory_code,
  brand,
  unit_cost,
  list_price,
  weight_kg,
  status,
  is_active,
  is_unknown
FROM silver_products;