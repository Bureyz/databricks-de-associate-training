# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Customer Orders Summary
# MAGIC Aggregated customer order statistics — join customers with orders, compute metrics.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema_silver", "silver", "Silver Schema")
dbutils.widgets.text("schema_gold", "gold", "Gold Schema")

catalog = dbutils.widgets.get("catalog")
schema_silver = dbutils.widgets.get("schema_silver")
schema_gold = dbutils.widgets.get("schema_gold")

customers_table = f"{catalog}.{schema_silver}.silver_customers"
orders_table = f"{catalog}.{schema_silver}.silver_orders_cleaned"
target_table = f"{catalog}.{schema_gold}.gold_customer_orders_summary"

# COMMAND ----------

from pyspark.sql.functions import count, sum as spark_sum, avg, max as spark_max, col

customers = spark.table(customers_table)
orders = spark.table(orders_table)

gold_df = (
    orders
    .join(customers, "customer_id", "left")
    .groupBy("customer_id", "first_name", "last_name", "customer_segment")
    .agg(
        count("order_id").alias("total_orders"),
        spark_sum("net_amount").alias("total_revenue"),
        avg("net_amount").alias("avg_order_value"),
        spark_max("order_datetime").alias("last_order_date")
    )
)

gold_df.write.format("delta").mode("overwrite").saveAsTable(target_table)

row_count = spark.table(target_table).count()
print(f"Created {row_count} customer summaries in {target_table}")

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "table": target_table, "rows": row_count}))
