@dp.table(name="silver_orders")
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_quantity", "quantity > 0")
@dp.expect_or_fail("valid_price", "unit_price >= 0")
def silver_orders():
    return (
        spark.readStream.table("bronze_orders")
            .select(
                "order_id",
                "customer_id",
                col("order_datetime").cast("timestamp").alias("order_ts"),
                (col("quantity") * col("unit_price")).alias("gross_amount")
            )
    )
