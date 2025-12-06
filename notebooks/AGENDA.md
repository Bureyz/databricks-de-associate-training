Dzień 1 – Data Engineering for Machine Learning (pigułka)
•	Wprowadzenie do platformy Databricks: przegląd architektury Lakehouse, workspace, klastry, notebooki, DBFS. 
•	Ingest i podstawowe transformacje: 
•	Tworzenie DataFrame’ów w SQL i PySpark; operacje select, filter, join, groupBy. 
•	Ładowanie danych z różnych formatów (CSV, JSON, Parquet) oraz zapis/odczyt tabel Delta. 
•	Zarządzanie schematem, ACID, time travel, MERGE, UPDATE i DELETE w Delta Lake. 
•	Strumieniowe i incrementalne ładowanie danych: 
•	Wprowadzenie do Auto Loader i cloudFiles do automatycznego wykrywania nowych plików i przetwarzania ich dokładnie raz. 
•	Porównanie trybów „trigger once” i „continuous”; obsługa schema evolution i wielu formatów plików. 
•	Structured Streaming: readStream / writeStream, checkpointing i fault tolerance. 
•	Architektura Medallion i Lakeflow: 
•	Omówienie koncepcji warstw Bronze, Silver, Gold i ich zastosowania w przygotowaniu danych. 
•	Praca z Lakeflow – deklaratywne definiowanie tabel i widoków, tworzenie pipeline’u w GUI, ustalanie harmonogramów, oczekiwań jakości danych, monitoring i lineage. 
•	Łączenie DLT z Auto Loader i Structured Streaming w jednym przepływie. 
•	Orkiestracja zadań i governance: 
•	Użycie Databricks Workflows do tworzenia zadań i multi task jobów. 
•	Podstawy zarządzania dostępem i katalogiem danych w Unity Catalog. 
- row lever security and data masking 
•	Dobre praktyki wersjonowania kodu (Repos) i monitorowania wydajności (Spark UI). 
