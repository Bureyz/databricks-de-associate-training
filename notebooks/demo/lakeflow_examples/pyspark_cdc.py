from pyspark import pipelines as dp

# Define the target table
dp.create_streaming_table(
    name="silver_customers",
    schema="""
        customer_id STRING,
        first_name STRING,
        city STRING,
        __START_AT TIMESTAMP,
        __END_AT TIMESTAMP
    """
)

# Define the CDC flow
dp.create_auto_cdc_flow(
    target="silver_customers",
    source="bronze_customers",
    keys=["customer_id"],
    sequence_by="ingestion_ts",
    stored_as_scd_type=2  # or 1
)
