from pyspark import pipelines as dp
from pyspark.sql.functions import *

# STREAMING TABLE
@dp.table(
    name="bronze_customers",
    comment="Raw customers from CSV"
)
def bronze_customers():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load(spark.conf.get("customer_path"))
            .select(
                "*",
                "_metadata.file_path".alias("source_file_path"),
                current_timestamp().alias("load_ts")
            )
    )

# MATERIALIZED VIEW
@dp.table(name="dim_customer")
def dim_customer():
    return (
        spark.read.table("silver_customers")
            .filter(col("__END_AT").isNull())
            .select("customer_id", "first_name", "last_name")
    )
