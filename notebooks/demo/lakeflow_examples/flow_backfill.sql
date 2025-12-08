-- Target table
CREATE OR REFRESH STREAMING TABLE bronze_orders;

-- FLOW 1: One-time backfill
CREATE FLOW bronze_orders_backfill
AS 
INSERT INTO ONCE bronze_orders BY NAME
SELECT
  order_id,
  customer_id,
  product_id,
  order_datetime,
  'batch' AS source_system,
  _metadata.file_path AS source_file_path,
  current_timestamp() AS load_ts
FROM read_files(
  '${order_path}/orders_batch.json',
  format => 'json'
);

-- FLOW 2: Continuous streaming
CREATE FLOW bronze_orders_stream
AS 
INSERT INTO bronze_orders BY NAME
SELECT
  order_id,
  customer_id,
  'stream' AS source_system,
  _metadata.file_path AS source_file_path,
  current_timestamp() AS load_ts
FROM STREAM read_files(
  '${order_path}/stream/orders_stream_*.json',
  format => 'json'
);
