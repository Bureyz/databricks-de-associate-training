-- 1. We define empty target table
CREATE OR REFRESH STREAMING TABLE bronze_orders;

-- 2. We define FLOW(s) which populate it
CREATE FLOW flow_name
AS INSERT INTO target_table BY NAME
SELECT ... FROM source;
