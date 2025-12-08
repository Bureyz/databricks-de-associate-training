1.Introduction to the Databricks platform: overview of Lakehouse architecture, workspace, clusters, notebooks, DBFS.

2. Ingest and basic transformations:

  - Creating DataFrames in SQL and PySpark; select, filter, join, groupBy operations.

  - Loading data from CSV, JSON, Parquet formats and reading/writing Delta tables.

  - Schema management, ACID, time travel, MERGE, UPDATE, DELETE in Delta Lake.

3. Streaming and incremental data loading:

  - Introduction to Auto Loader and cloudFiles for automatic detection of new files and exactly-once processing.

  - Comparison of "trigger once" and "continuous" modes; schema evolution handling and multiple file formats.

  - Structured Streaming: readStream / writeStream, checkpointing, fault tolerance.

4.Medallion architecture and Delta Live Tables:

  - Concept of Bronze, Silver, Gold layers and their application in data preparation.

  - Working with Delta Live Tables – declarative table/view P<definitions, pipeline creation in GUI, scheduling, data quality expectations, monitoring, and lineage.

  - Combining DLT with Auto Loader and Structured Streaming in one flow.

5.Orchestration and governance:

  - Using Databricks Workflows for jobs and multitask jobs.

  - Basics of access management and Unity Catalog.

  - Best practices for code versioning (Repos) and performance monitoring (Spark UI).