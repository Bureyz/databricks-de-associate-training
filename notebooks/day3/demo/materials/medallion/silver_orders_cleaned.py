# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Orders Cleaned
# MAGIC Read bronze orders, apply data quality checks, compute derived columns.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema_bronze", "bronze", "Bronze Schema")
dbutils.widgets.text("schema_silver", "silver", "Silver Schema")

catalog = dbutils.widgets.get("catalog")
schema_bronze = dbutils.widgets.get("schema_bronze")
schema_silver = dbutils.widgets.get("schema_silver")

source_table = f"{catalog}.{schema_bronze}.bronze_orders"
target_table = f"{catalog}.{schema_silver}.silver_orders_cleaned"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, when, round as spark_round

df = spark.table(source_table)

df_cleaned = (
    df
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("quantity") > 0)
    .withColumn("total_amount",
        spark_round(col("quantity") * col("unit_price"), 2))
    .withColumn("discount_amount",
        spark_round(col("quantity") * col("unit_price") * col("discount_percent") / 100, 2))
    .withColumn("net_amount",
        spark_round(col("total_amount") - col("discount_amount"), 2))
    .withColumn("_processed_at", current_timestamp())
)

df_cleaned.write.format("delta").mode("overwrite").saveAsTable(target_table)

row_count = spark.table(target_table).count()
print(f"Cleaned {row_count} rows into {target_table}")

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "table": target_table, "rows": row_count}))
