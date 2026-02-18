# LAB 07B: Lakeflow SQL Declarations — Practice

| Detail | Value |
|--------|-------|
| Duration | ~20 min |
| Day | Day 3 |
| Module | M07: Medallion & Lakeflow Pipelines |
| Difficulty | Advanced |
| Notebook | LAB_07_code.ipynb — Section 2 |

---

## Objectives

After completing this practice section you will be able to:
- Write `CREATE OR REFRESH STREAMING TABLE` declarations for Bronze and Silver layers
- Write `CREATE OR REFRESH MATERIALIZED VIEW` declarations for Gold layer
- Define data quality expectations with `CONSTRAINT ... EXPECT ... ON VIOLATION`
- Explain the difference between STREAMING TABLE and MATERIALIZED VIEW
- Query pipeline results and inspect the event log for data quality metrics

## Prerequisites

- Completed **LAB 07 Section 1** (Workshop — Building the Pipeline)
- Understanding of the Medallion Architecture (Bronze / Silver / Gold)

---

## Task 1: Write Bronze Declaration

Complete the SQL declaration to create a Bronze streaming table from JSON files.

**Fill in the blanks** in the Python cell:

```python
bronze_sql = f"""
CREATE OR REFRESH ________ TABLE bronze_orders
AS SELECT * 
FROM STREAM ________('{{DATASET_PATH}}/orders/stream', format => 'json');
"""
```

**What to fill:**
- First blank: the keyword that marks this table as a streaming table
- Second blank: the function used to read files from a path

**Hints:**
- Bronze layer uses `STREAMING TABLE` for incremental ingestion
- Use `read_files()` to read from a file path with a specified format

**Validation:** The cell below checks your answer — you should see `"Task 1 OK"`.

---

## Task 2: Write Silver Declaration with Expectations

Complete the Silver layer SQL with data quality constraints.

**Fill in the blanks:**

```python
silver_sql = """
CREATE OR REFRESH STREAMING TABLE silver_orders (
    CONSTRAINT valid_id EXPECT (order_id IS NOT NULL) ON VIOLATION ________,
    CONSTRAINT positive_amount EXPECT (total_price > 0) ON VIOLATION ________
)
AS SELECT 
    order_id, customer_id, product_id,
    CAST(quantity AS INT) AS quantity,
    CAST(total_price AS DOUBLE) AS total_price,
    CAST(order_date AS DATE) AS order_date,
    payment_method, store_id,
    current_timestamp() AS processed_at
FROM STREAM(bronze_orders);
"""
```

**What to fill:**
- Both blanks: the violation action that **removes** invalid rows

**Key syntax points:**
- `ON VIOLATION DROP ROW` — drops rows that fail the constraint
- `ON VIOLATION FAIL UPDATE` — fails the entire pipeline update
- No action specified — records metric only, rows pass through
- Source reference uses `STREAM(bronze_orders)` — direct table name, no prefix

**Validation:** The cell below checks for `CONSTRAINT` and `DROP ROW`.

---

## Task 3: Write Gold Declaration

Create a Materialized View for a daily revenue summary.

**Fill in the blanks:**

```python
gold_sql = """
CREATE OR REFRESH ________ ________ gold_daily_revenue
AS SELECT 
    order_date,
    SUM(total_price) AS total_revenue,
    COUNT(*) AS total_orders,
    AVG(total_price) AS avg_order_value
FROM silver_orders
GROUP BY order_date
ORDER BY order_date;
"""
```

**What to fill:**
- Two blanks: the two keywords that define a Gold-layer table type

**Hints:**
- Gold layer uses `MATERIALIZED VIEW` — it fully recomputes on each run
- References `silver_orders` directly by name (no prefix)

**Validation:** The cell below checks for `MATERIALIZED VIEW`.

---

## Task 4: Compare STREAMING TABLE vs MATERIALIZED VIEW

Fill in the comparison table in the notebook markdown cell:

| Feature | STREAMING TABLE | MATERIALIZED VIEW |
|---------|----------------|-------------------|
| Processing mode | ________ | ________ |
| Best for | ________ | ________ |
| Read from source | `STREAM(table_name)` | `table_name` |
| Supports expectations | Yes | Yes |

**Answers to verify yourself:**

| Feature | STREAMING TABLE | MATERIALIZED VIEW |
|---------|----------------|-------------------|
| Processing mode | Incremental (append-only) | Full recompute |
| Best for | Raw ingestion, event streams | Aggregations, joins, dimensions |
| Read from source | `STREAM(table_name)` | `table_name` |
| Supports expectations | Yes | Yes |

> **Exam Tip:** `STREAMING TABLE` processes only new data (incremental). `MATERIALIZED VIEW` recomputes the entire result set on each pipeline run.

---

## Task 5: Verify Pipeline Results

After creating and running the pipeline via the UI (Section 1), uncomment and run the verification queries in the notebook:

```sql
-- Check Bronze row count
SELECT COUNT(*) AS cnt FROM <catalog>.bronze.bronze_orders

-- Check Silver row count
SELECT COUNT(*) AS cnt FROM <catalog>.silver.silver_orders

-- Check Gold daily revenue
SELECT * FROM <catalog>.gold.gold_daily_revenue ORDER BY order_date
```

**Expected:** All three queries return data. Gold table should show aggregated revenue per date.

---

## Task 6: Check Pipeline Event Log

Query the pipeline event log to see data quality metrics:

```sql
SELECT timestamp, details:flow_progress:data_quality:expectations
FROM event_log(TABLE(<catalog>.silver.silver_orders))
WHERE details:flow_progress:data_quality IS NOT NULL
```

**What to check:**
- How many rows passed each expectation?
- How many rows were dropped by `ON VIOLATION DROP ROW`?
- The event log provides full audit trail of pipeline data quality

---

## Summary

In this practice section you:
- Wrote a Bronze `STREAMING TABLE` declaration using `read_files()`
- Wrote a Silver `STREAMING TABLE` declaration with `CONSTRAINT ... EXPECT ... ON VIOLATION DROP ROW`
- Wrote a Gold `MATERIALIZED VIEW` declaration for aggregations
- Compared STREAMING TABLE vs MATERIALIZED VIEW capabilities
- Verified pipeline results and checked data quality via event log

> **Exam Tip:** In Spark Declarative Pipelines, tables within the same pipeline reference each other directly by name — no prefix needed. Use `STREAM(table_name)` for streaming reads and just `table_name` for batch reads.

> **Important:** The `LIVE.` prefix is **no longer used** in modern Databricks syntax. Always reference tables directly by name within a pipeline.

## What's Next

Proceed to **LAB 08 — Lakeflow Jobs & Orchestration** to learn about multi-task jobs, triggers (File Arrival, Table Update), task dependencies, and repair runs.
