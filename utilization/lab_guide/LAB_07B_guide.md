# LAB 07B: Orchestration — Multi-task Jobs with Triggers

**Duration:** ~45 min  
**Day:** 3  
**After module:** M08: Orchestration & Lakeflow Jobs  
**Difficulty:** Advanced

---

## Scenario

> *"RetailHub needs two automated pipelines: one that processes orders when new files arrive,
> and another that builds customer reports whenever the orders pipeline updates its silver table.
> Set up cross-job orchestration using File Arrival and Table Update triggers."*

---

## Objectives

After completing this lab you will be able to:
- Create multi-task Lakeflow Jobs with task dependencies (DAG)
- Configure **File Arrival** triggers on Unity Catalog Volumes
- Configure **Table Update** triggers for cross-job orchestration
- Use a validation task with event logging
- Pass parameters between tasks using widgets and `{{run.id}}`

---

## Part 1: Preparation (~5 min)

### Task 1: Prepare Workspace

Import medallion notebooks from `materials/medallion/` and `materials/orchestration/` into your Databricks workspace.

**Notebooks needed:**
- `bronze_orders.py` — batch JSON read to Delta
- `silver_orders_cleaned.py` — quality filters + computed columns
- `gold_daily_orders.py` — daily order aggregation
- `bronze_customers.py` — batch CSV read to Delta
- `silver_customers.py` — dedup + normalization
- `gold_customer_orders_summary.py` — join + aggregate metrics
- `task_validate_pipeline.py` — validates all tables + logs to event_log

---

## Part 2: Job A — Orders Pipeline (~15 min)

### Task 2: Create Job A in UI

**Job name:** `LAB_Orders_Pipeline`

| # | Task name | Notebook | Depends on | Key Parameters |
|---|-----------|----------|------------|----------------|
| 1 | bronze_orders | medallion/bronze_orders | — | catalog, schema=bronze |
| 2 | silver_orders | medallion/silver_orders_cleaned | bronze_orders | catalog, schema_bronze, schema_silver |
| 3 | gold_daily | medallion/gold_daily_orders | silver_orders | catalog, schema_silver, schema_gold |

**Trigger:** File Arrival
- URL: `/Volumes/<catalog>/default/landing_zone/trigger`
- Min time between triggers: 60s
- Wait after last change: 15s

### Task 3: Trigger Job A

Create a signal file in the monitored Volume path to trigger the job.

**Expected:** Job starts within ~60s, all 3 tasks complete successfully.

---

## Part 3: Job B — Customer Pipeline (~15 min)

### Task 4: Create Job B in UI

**Job name:** `LAB_Customer_Pipeline`

| # | Task name | Notebook | Depends on | Key Parameters |
|---|-----------|----------|------------|----------------|
| 1 | bronze_customers | medallion/bronze_customers | — | catalog, schema=bronze |
| 2 | silver_customers | medallion/silver_customers | bronze_customers | catalog, schema_bronze, schema_silver |
| 3 | gold_summary | medallion/gold_customer_orders_summary | silver_customers | catalog, schema_silver, schema_gold |
| 4 | validate | orchestration/task_validate_pipeline | gold_summary | catalog, all schemas, job_run_id={{run.id}} |

**Trigger:** Table updated
- Table: `<catalog>.silver.silver_orders_cleaned`
- Condition: Any new rows
- Min time between triggers: 60s

**Cross-job flow:**
```
File arrives → Job A: bronze→silver→gold_daily
                         ↓
              silver_orders_cleaned updated
                         ↓
              Job B triggers: bronze_cust→silver_cust→gold_summary→validate
```

---

## Part 4: Verify & Explore (~10 min)

### Task 5: Verify Results

After both jobs complete, check that all 6 tables exist and have rows.

### Task 6: Check Event Log

Query `pipeline_event_log` to find the validation record.

### Task 7: Cross-Job Pattern

Answer questions about trigger chain behavior, Repair Runs, and alerting.

---

## Summary

| Trigger Type | Fires when | Best for |
|-------------|-----------|----------|
| **File Arrival** | New file in Volume/path | Event-driven ingestion |
| **Table Update** | Delta table DML | Cross-pipeline orchestration |
| **Scheduled** | CRON time hit | Regular ETL |
| **Continuous** | Always | Near-real-time |

### Exam Tips
- File Arrival monitors a cloud storage path or UC Volume for new files
- Table Update fires on `inserted_count > 0` (or `updated_count`, `deleted_count`)
- Repair Runs re-execute only failed + downstream tasks
- `dbutils.notebook.exit()` returns JSON readable via `taskValues`
- `max_concurrent_runs: 1` prevents duplicate runs
- Use Serverless or Job clusters (not All-Purpose) for production

---

> **Next:** LAB 08 - Unity Catalog Governance
