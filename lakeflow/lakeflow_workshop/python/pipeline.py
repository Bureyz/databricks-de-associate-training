# ============================================================
# Lakeflow SDP Pipeline - Workshop Solution
# ============================================================
# This pipeline demonstrates the new Lakeflow Streaming Data Platform API
# using the pyspark.pipelines module (dp)

from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Configuration
source_path = spark.conf.get("source_path")

# ============================================================
# BRONZE LAYER - Streaming Tables with Auto Loader
# ============================================================

# Bronze Customers
dp.create_streaming_table(
    name="bronze_customers",
    comment="Raw customers data from CSV"
)

@dp.append_flow(target="bronze_customers")
def bronze_customers_flow():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{source_path}/Customers")
    )

# Bronze Products
dp.create_streaming_table(
    name="bronze_products",
    comment="Raw products data from CSV"
)

@dp.append_flow(target="bronze_products")
def bronze_products_flow():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{source_path}/Product")
    )

# Bronze Orders
dp.create_streaming_table(
    name="bronze_orders",
    comment="Raw orders data from CSV"
)

@dp.append_flow(target="bronze_orders")
def bronze_orders_flow():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{source_path}/SalesOrderHeader")
    )

# ============================================================
# SILVER LAYER - SCD with AUTO CDC
# ============================================================

# Silver Customers: SCD Type 2 (History Tracking)
dp.create_streaming_table(
    name="silver_customers",
    comment="Cleaned customers with SCD Type 2"
)

dp.create_auto_cdc_flow(
    target="silver_customers",
    source="bronze_customers",
    keys=["CustomerID"],
    sequence_by=col("ModifiedDate"),
    stored_as_scd_type="2"
)

# Silver Products: SCD Type 1 (Overwrite)
dp.create_streaming_table(
    name="silver_products",
    comment="Cleaned products with SCD Type 1"
)

dp.create_auto_cdc_flow(
    target="silver_products",
    source="bronze_products",
    keys=["ProductID"],
    sequence_by=col("ModifiedDate"),
    stored_as_scd_type="1"
)

# Silver Orders: Materialized View with Data Quality
@dp.materialized_view(
    name="silver_orders",
    comment="Cleaned orders data",
    expect_all_or_drop={"valid_amount": "TotalDue > 0"},
    expect_all_or_fail={"valid_customer": "CustomerID IS NOT NULL"}
)
def silver_orders():
    return (
        spark.readStream.table("bronze_orders")
        .select(
            "SalesOrderID",
            "CustomerID",
            "TotalDue",
            "OrderDate",
            "Status",
            current_timestamp().alias("processed_at")
        )
    )

# ============================================================
# GOLD LAYER - Business Aggregates
# ============================================================

@dp.materialized_view(
    name="gold_customer_sales",
    comment="Aggregated sales by customer"
)
def gold_customer_sales():
    orders = spark.read.table("silver_orders")
    customers = spark.read.table("silver_customers")
    
    # Filter for current customer records (SCD Type 2)
    # In Lakeflow SDP, __END_AT IS NULL indicates current record
    current_customers = customers.filter(col("__END_AT").isNull())
    
    return (
        orders.join(current_customers, ["CustomerID"], "inner")
        .groupBy(
            current_customers["CustomerID"],
            current_customers["FirstName"],
            current_customers["LastName"],
            current_customers["CompanyName"]
        )
        .agg(
            count("SalesOrderID").alias("total_orders"),
            sum("TotalDue").alias("total_spent"),
            max("OrderDate").alias("last_order_date")
        )
    )
