# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Orders
# MAGIC Batch ingestion of order data from JSON files into a Delta table.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema", "bronze", "Schema")
dbutils.widgets.text("source_path", "", "Source Path")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_path = dbutils.widgets.get("source_path")

target_table = f"{catalog}.{schema}.bronze_orders"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

df = (
    spark.read
    .format("json")
    .option("inferSchema", "true")
    .load(source_path)
    .withColumn("_source_file", input_file_name())
    .withColumn("_load_ts", current_timestamp())
)

df.write.format("delta").mode("append").saveAsTable(target_table)

row_count = spark.table(target_table).count()
print(f"Loaded {row_count} rows into {target_table}")

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "table": target_table, "rows": row_count}))
