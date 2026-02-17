# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Daily Orders Aggregation
# MAGIC Daily order metrics — order count, revenue, average value per day.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema_silver", "silver", "Silver Schema")
dbutils.widgets.text("schema_gold", "gold", "Gold Schema")

catalog = dbutils.widgets.get("catalog")
schema_silver = dbutils.widgets.get("schema_silver")
schema_gold = dbutils.widgets.get("schema_gold")

source_table = f"{catalog}.{schema_silver}.silver_orders_cleaned"
target_table = f"{catalog}.{schema_gold}.gold_daily_orders"

# COMMAND ----------

from pyspark.sql.functions import col, count, sum as spark_sum, avg, to_date, round as spark_round

df = spark.table(source_table)

gold_df = (
    df
    .withColumn("order_date", to_date(col("order_datetime")))
    .groupBy("order_date")
    .agg(
        count("order_id").alias("order_count"),
        spark_sum("net_amount").alias("total_revenue"),
        spark_round(avg("net_amount"), 2).alias("avg_order_value")
    )
    .orderBy("order_date")
)

gold_df.write.format("delta").mode("overwrite").saveAsTable(target_table)

row_count = spark.table(target_table).count()
print(f"Created {row_count} daily aggregations in {target_table}")

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "table": target_table, "rows": row_count}))
