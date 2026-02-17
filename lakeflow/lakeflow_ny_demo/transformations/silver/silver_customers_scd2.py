from pyspark import pipelines as dp

# Create target streaming table for SCD Type 2
dp.create_streaming_table(
    name="silver_customers_scd2",
    comment="SCD Type 2: Full history of customer changes with validity periods (__START_AT, __END_AT)"
)

# Define Auto CDC flow for SCD Type 2
dp.create_auto_cdc_flow(
    target="silver_customers_scd2",
    source="bronze_customers",
    keys=["c_custkey"],
    sequence_by="c_custkey",  # Using customer key as sequence since TPC-H doesn't have timestamps
    stored_as_scd_type=2,  # SCD Type 2: Full history tracking
    ignore_null_updates=True
)

"""
SCD Type 2 Demo:
- Tracks FULL HISTORY of customer changes
- Each change creates a new row with validity period
- Adds __START_AT and __END_AT columns automatically
- Current records have __END_AT = NULL
- Perfect for auditing and historical analysis
- Uses c_custkey as both primary key and sequence column
"""
