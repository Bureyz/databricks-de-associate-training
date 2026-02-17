from pyspark import pipelines as dp

# Create target streaming table for SCD Type 1
dp.create_streaming_table(
    name="silver_customers_scd1",
    comment="SCD Type 1: Current state of customers - updates overwrite previous values"
)

# Define Auto CDC flow for SCD Type 1
dp.create_auto_cdc_flow(
    target="silver_customers_scd1",
    source="bronze_customers",
    keys=["c_custkey"],
    sequence_by="c_custkey",  # Using customer key as sequence since TPC-H doesn't have timestamps
    stored_as_scd_type=1,  # SCD Type 1: Only current state
    ignore_null_updates=True
)

"""
SCD Type 1 Demo:
- Tracks ONLY the current state of each customer
- Updates overwrite previous values (no history)
- Efficient for scenarios where historical changes don't matter
- Uses c_custkey as both primary key and sequence column
"""
