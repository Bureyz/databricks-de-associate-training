# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Customers
# MAGIC Read bronze customers, deduplicate, standardize fields.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema_bronze", "bronze", "Bronze Schema")
dbutils.widgets.text("schema_silver", "silver", "Silver Schema")

catalog = dbutils.widgets.get("catalog")
schema_bronze = dbutils.widgets.get("schema_bronze")
schema_silver = dbutils.widgets.get("schema_silver")

source_table = f"{catalog}.{schema_bronze}.bronze_customers"
target_table = f"{catalog}.{schema_silver}.silver_customers"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, trim, lower, row_number
from pyspark.sql.window import Window

df = spark.table(source_table)

# Deduplicate: keep latest record per customer_id
w = Window.partitionBy("customer_id").orderBy(col("_load_ts").desc())

df_clean = (
    df
    .filter(col("customer_id").isNotNull())
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("_processed_at", current_timestamp())
)

df_clean.write.format("delta").mode("overwrite").saveAsTable(target_table)

row_count = spark.table(target_table).count()
print(f"Cleaned {row_count} customers into {target_table}")

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "table": target_table, "rows": row_count}))
