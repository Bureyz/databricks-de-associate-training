-- ============================================================
-- Gold Layer: Business Aggregates
-- Lakeflow SDP Syntax with Materialized Views
-- ============================================================

-- Customer Sales Summary
CREATE OR REFRESH MATERIALIZED VIEW gold_customer_sales
COMMENT 'Aggregated sales by customer'
AS SELECT 
  c.CustomerID,
  c.FirstName,
  c.LastName,
  c.CompanyName,
  count(o.SalesOrderID) as total_orders,
  sum(o.TotalDue) as total_spent,
  max(o.OrderDate) as last_order_date
FROM silver_orders o
JOIN silver_customers c ON o.CustomerID = c.CustomerID
WHERE c.__END_AT IS NULL -- Only current customer details (SCD Type 2)
GROUP BY c.CustomerID, c.FirstName, c.LastName, c.CompanyName;
