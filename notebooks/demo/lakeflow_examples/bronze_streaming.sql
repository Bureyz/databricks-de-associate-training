CREATE OR REFRESH STREAMING TABLE bronze_customers
AS
SELECT
  customer_id,
  first_name,
  last_name,
  email,
  city,
  _metadata.file_path                AS source_file_path,
  _metadata.file_modification_time   AS ingestion_ts,
  current_timestamp()                AS load_ts
FROM STREAM read_files(
  '${customer_path}',
  format           => 'csv',
  header           => true,
  inferColumnTypes => true
);
