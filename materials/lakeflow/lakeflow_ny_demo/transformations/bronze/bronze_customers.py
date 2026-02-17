from pyspark import pipelines as dp

@dp.table(
    name="bronze_customers",
    comment="Raw customer data ingested from TPC-H sample dataset - simulates real-time customer updates"
)
def bronze_customers():
    """
    Bronze layer: Ingest customer data from samples.tpch.customer
    This simulates a streaming source of customer changes for CDC processing
    """
    return (
        spark.readStream.table("samples.tpch.customer")
    )
