# Plan Szkolenia: Databricks Data Engineer Associate (3 dni)

> **Data opracowania:** 15 lutego 2026  
> **Certyfikacja:** Databricks Certified Data Engineer Associate (Exam Guide: Nov 30, 2025)  
> **Format:** 3 dni × 7h (9:00–16:00), w tym przerwa obiadowa + krótkie przerwy  
> **Netto:** ~5h 30min treści dziennie (~16h 30min łącznie)

---

## 1. Analiza wymagań egzaminacyjnych (stan na 01.02.2026)

### Aktualna struktura egzaminu

| Sekcja | Waga | Opis |
|--------|------|------|
| **Databricks Data Intelligence Platform** | 10% | Workspace, Compute, Repos/Git Folders, DBFS → Volumes |
| **Development & Ingestion** | 30% | ETL, Spark SQL/PySpark, odczyt formatów, Auto Loader, COPY INTO |
| **Data Processing & Transformations** | 31% | Transformacje, Delta Lake, funkcje okienkowe, UDF, widoki |
| **Productionizing Data Pipelines** | 18% | Lakeflow Jobs, Lakeflow Spark Declarative Pipelines, parametryzacja |
| **Data Governance & Quality** | 11% | Unity Catalog, GRANT/REVOKE, Delta Sharing, lineage, expectations |

### Kluczowe zmiany w Databricks (2025–2026) vs. stare wymagania

| Zmiana | Data | Wpływ na szkolenie |
|--------|------|-------------------|
| **DLT → Lakeflow Spark Declarative Pipelines (SDP)** | Czerwiec 2025 GA, rebrand Listopad 2025 | Zmiana nazewnictwa w materiałach. `CREATE LIVE TABLE` → `CREATE STREAMING TABLE` / `CREATE MATERIALIZED VIEW` z nową składnią |
| **Databricks Jobs → Lakeflow Jobs** | Czerwiec 2025 | Zmiana nazewnictwa. Unified Jobs & Pipelines view |
| **APPLY CHANGES → AUTO CDC** | Czerwiec 2025 | Nowe API do SCD Type 1/2 |
| **DBFS deprecation, DBFS root/mounts disabled** | Listopad 2025 GA | Zamiast DBFS stosujemy **Unity Catalog Volumes** |
| **Liquid Clustering GA** | Luty 2025 (Preview), Czerwiec 2025 (GA), Automatic LC (Sierpień 2025 GA) | Zastępuje ZORDER + partycjonowanie jako domyślna strategia. Egzamin nadal pokrywa ZORDER ale LC to best practice |
| **Predictive Optimization** | Maj 2025 GA | Automatyczne OPTIMIZE/VACUUM/ANALYZE. Zastępuje manualne podejście |
| **Serverless Compute domyślny** | Październik 2025 | Serverless jest domyślnym compute. Classic clusters nadal dostępne |
| **Git Folders (ex-Repos)** | Maj 2024 GA | Nazwa zmieniona z "Repos" na "Git Folders" |
| **Lakeflow Connect** | 2025 GA connectors | Managed ingestion connectors (Salesforce, SQL Server, etc.) |
| **ABAC (Attribute-Based Access Control)** | Listopad 2025 Public Preview | Nowy model uprawnień obok RBAC |
| **Data Classification** | Październik 2025 Public Preview | Automatyczna klasyfikacja PII |
| **Deletion Vectors** | Domyślne od 2023 | Wpływa na MERGE/DELETE performance |
| **Lakeflow Pipelines Editor** | Wrzesień 2025 Public Preview | Wizualny edytor pipeline'ów |
| **ForEachBatch for SDP** | Grudzień 2025 Public Preview | Nowe możliwości streamingu |
| **INSERT REPLACE** | Sierpień 2025 GA | Nowa składnia DML |
| **Real-time mode Structured Streaming** | Lipiec 2025 Public Preview | Nowy tryb streamingu |
| **Built-in Excel support** | Grudzień 2025 Beta | Natywny odczyt Excel |

---

## 2. Audyt repozytorium - pokrycie tematów

### Istniejące materiały (Demo Notebooks)

| # | Notebook | Linii | Tematy pokryte | Status |
|---|----------|-------|----------------|--------|
| 01 | `01_platform_intro` | 806 | Lakehouse arch, UC hierarchy, Compute, DBFS/Volumes, Workspace | [OK] Dobry, wymaga aktualizacji nazewnictwa (Git Folders, Serverless domyślny) |
| 02 | `02_ingestion_transformations` | 1185 | CSV/JSON/Parquet read, inferSchema, Bronze philosophy, PySpark transforms, SQL, groupBy, agg, widoki temp, czyszczenie danych | [OK] Bardzo dobry, kompletny |
| 03 | `03_delta_lake_optimization` | 1468 | Delta Lake ACID, schema enforcement/evolution, time travel, MERGE/UPDATE/DELETE, CLONE, OPTIMIZE, ZORDER, VACUUM, Liquid Clustering | [OK] Bardzo dobry, zawiera Liquid Clustering |
| 04 | `04_streaming_incremental` | 1098 | Structured Streaming, Auto Loader, COPY INTO, checkpoints, trigger modes, schema evolution, batch vs stream | [OK] Dobry |
| 05 | `05_medallion_lakeflow` | 680 | Medallion Architecture, Bronze→Silver→Gold, Lakeflow SDP (SQL), expectations, DAG | [OK] Dobry, wymaga rozszerzenia o Python SDP |
| 06 | `06_orchestration` | 498 | Lakeflow Jobs, multi-task, widgets, parameters, system tables, triggers | [OK] Dobry |
| 07 | `07_governance` | 1083 | Unity Catalog, GRANT/REVOKE, Row Filters, Column Masks, Lineage, Delta Sharing, audit logs | [OK] Bardzo dobry |

### Istniejące materiały (Warsztaty)

| # | Workshop | Linii | Tematy | Status |
|---|----------|-------|--------|--------|
| W1 | Ingestion & Transformations | 325 | CSV read, select, withColumn, Delta save | [OK] OK |
| W2 | Delta Optimization | 405 | Small files problem, OPTIMIZE, ZORDER, VACUUM | [OK] OK |
| W3 | Lakeflow Pipeline | 411 | Full Medallion pipeline w SQL/Python, SCD Type 2, expectations | [OK] OK |
| W4 | Governance & Security | 343 | Permissions, Row Filter, Data Mask | [OK] OK |

### Lakeflow Pipeline Definitions

| Typ | Pliki | Status |
|-----|-------|--------|
| Training pipeline (SQL) | Bronze (3), Silver (3), Gold (6) | [OK] Kompletny model gwiazdy |
| Workshop pipeline (SQL/Python) | Bronze, Silver, Gold | [OK] OK |

### Datasety

| Dataset | Format | Rozmiar | Użycie |
|---------|--------|---------|--------|
| `customers.csv` | CSV | ~10K rows | Demo + Workshop |
| `customers_extended.csv` | CSV | rozszerzony | Demo |
| `customers_new.csv` | CSV | nowi klienci | MERGE demo |
| `orders_batch.json` | JSON | ~50K rows | Demo + Workshop |
| `orders_stream_*.json` | JSON | 6 plików | Streaming demo |
| `products.csv` | CSV | ~500 rows | Demo + Workshop |
| `workshop/*.csv` | CSV | 5 tabel | Workshop (Star Schema) |

### Pokrycie tematów egzaminacyjnych

| Temat egzaminacyjny | Pokryty? | Gdzie? |
|--------------------|----------|--------|
| Workspace & Navigation | [OK] | 01_platform_intro |
| Compute (clusters, SQL WH, serverless) | [OK] | 01_platform_intro |
| Git Folders (ex-Repos) | [CZESCIOWO] | 01_platform_intro (stara nazwa "Repos") |
| DBFS vs Volumes | [OK] | 01_platform_intro |
| Read CSV/JSON/Parquet | [OK] | 02_ingestion |
| Schema definition & inferSchema | [OK] | 02_ingestion |
| select, filter, withColumn, groupBy | [OK] | 02_ingestion |
| dropDuplicates, dropna, fillna | [OK] | 02_ingestion |
| Temp Views & SQL | [OK] | 02_ingestion |
| Delta Lake ACID | [OK] | 03_delta_lake |
| Time Travel | [OK] | 03_delta_lake |
| MERGE INTO | [OK] | 03_delta_lake |
| Schema Enforcement & Evolution | [OK] | 03_delta_lake |
| OPTIMIZE, ZORDER, VACUUM | [OK] | 03_delta_lake |
| Liquid Clustering | [OK] | 03_delta_lake |
| Managed vs External tables | [OK] | 03_delta_lake |
| Auto Loader | [OK] | 04_streaming |
| COPY INTO | [OK] | 04_streaming |
| Structured Streaming | [OK] | 04_streaming |
| Trigger modes | [OK] | 04_streaming |
| Medallion Architecture | [OK] | 05_medallion |
| Lakeflow SDP (DLT) | [OK] | 05_medallion |
| Expectations (Data Quality) | [OK] | 05_medallion |
| Lakeflow Jobs | [OK] | 06_orchestration |
| Multi-task Jobs | [OK] | 06_orchestration |
| Parameters & Widgets | [OK] | 06_orchestration |
| Unity Catalog | [OK] | 07_governance |
| GRANT/REVOKE | [OK] | 07_governance |
| Row Filters & Column Masks | [OK] | 07_governance |
| Delta Sharing | [OK] | 07_governance |
| Lineage | [OK] | 07_governance |
| **Funkcje okienkowe (LAG, LEAD, RANK)** | [OK] | **06a_advanced_pyspark** (PySpark) + **06b_advanced_sql** (SQL) |
| **CTE (Common Table Expressions)** | [OK] | **06b_advanced_sql** |
| **Subqueries (scalar, correlated, EXISTS)** | [OK] | **06b_advanced_sql** |
| **Complex types (struct, array, explode)** | [OK] | **06a_advanced_pyspark** (explode, posexplode, array of structs) |
| **JSON processing (from_json, to_json)** | [OK] | **06a_advanced_pyspark** |
| **Date functions (date_trunc, datediff)** | [OK] | **06a_advanced_pyspark** |
| **CTAS, CREATE VIEW, DDL** | [OK] | **06b_advanced_sql** + CLONE w 03 |
| **CASE WHEN, COALESCE, NVL** | [OK] | **06b_advanced_sql** |
| **UDF (User Defined Functions)** | [BRAK] | **BRAK - niewielka luka, rzadko na egzaminie** |
| **Higher-order functions (TRANSFORM, FILTER)** | [BRAK] | **BRAK - niewielka luka** |
| **Predictive Optimization** | [BRAK] | **BRAK - WYMAGA DODANIA** |
| **ELT vs ETL methodology** | [BRAK] | **BRAK - WYMAGA DODANIA** (ważny temat konceptualny, ~5 min) |
| **Views: Temp vs Global Temp vs Permanent** | [CZESCIOWO] | Częściowo w 02/06b, brak jawnego porównania |
| **Streaming Tables vs Materialized Views** | [CZESCIOWO] | 05_medallion, brak jawnej tabeli porównawczej |
| **Output modes (append/complete/update)** | [CZESCIOWO] | 04_streaming, sprawdzić czy jawnie pokryte |
| **DESCRIBE HISTORY / DETAIL** | [CZESCIOWO] | 03_delta_lake, upewnić się że jawnie |
| **Notebook magic commands (%sql, %python, %run)** | [CZESCIOWO] | Używane, ale nie wyjaśnione jawnie |
| **dbutils (fs, widgets, secrets, notebook.run)** | [CZESCIOWO] | Częściowo w 06_orchestration |
| **Task types & repair runs** | [CZESCIOWO] | 06_orchestration, sprawdzić |
| **Information Schema** | [BRAK] | **BRAK - dodać do governance** |

---

## 3. Plan szkolenia (3 dni, 9:00–16:00)

### Format

- [T] = Teoria/Prezentacja
- [D] = Demo (trener prowadzi, uczestnicy sledza)
- [L] = Lab/Workshop (uczestnicy samodzielnie, 2 pliki: guide.md + code.ipynb)
- [P] = Przerwa (5-15 min)

> **Przerwa obiadowa (~45 min)** nie jest zaznaczona w harmonogramie -- trener decyduje o momencie przerwy w zaleznosci od tempa grupy (zwykle ok. 12:00-13:00). Czasy ponizej sa orientacyjne.

### Moduły i Laboratoria (9 modułów + 8 labów)

> **Numeracja labów = numer modułu** (LAB_02 odpowiada M02, itd.). LAB_01 = setup platformy. Każdy moduł z labem ma LAB o tym samym numerze.

| Moduł | Temat | Lab | Lab temat |
|-------|-------|-----|-----------|
| M01 | Platform & Workspace | LAB_01 | GUI: Cluster, Volume, External Connection |
| M02 | ELT Data Ingestion | LAB_02 | Ingestion, transforms, zapis do Delta |
| M03 | Delta Lake Fundamentals | LAB_03 | Delta DML, Time Travel, MERGE |
| M04 | Delta Lake Optimization | LAB_04 | OPTIMIZE, VACUUM, Liquid Clustering |
| M05 | Incremental Data Processing | LAB_05 | Auto Loader, Streaming, COPY INTO |
| M06 | Advanced Transforms — PySpark & SQL | LAB_06 | Window Functions, CTE, Explode, JSON |
| M07 | Medallion & Lakeflow Pipelines | LAB_07 | Pipeline Medallion z expectations |
| M08 | Lakeflow Jobs & Orchestration | LAB_08 | Triggers, Dependencies, Job Configuration |
| M09 | Governance & Security | LAB_09 | Permissions, Row Filters, Masking |

> **Quizy:** `day{1,2,3}/materials/day{1,2,3}_quiz.pdf` -- 20 pytan exam-style na koniec kazdego dnia

---

### Fabuła laboratoriów — Projekt "RetailHub"

Laboratoria opowiadają wspólną historię: **budowa platformy danych dla fikcyjnego e-commerce RetailHub**. Każdy lab to kolejny etap projektu, ale technicznie jest **samodzielny** (posiada własną komórkę setup, która tworzy/ładuje potrzebne dane, jeśli nie istnieją).

#### Historia

> RetailHub to rosnący sklep internetowy. Zespół danych dostał zadanie zbudowania nowoczesnej platformy analitycznej w Databricks. Przez 3 dni szkolenia uczestnicy przechodzą przez kolejne etapy — od postawienia infrastruktury, przez import i transformację danych, aż po governance i udostępnianie danych partnerom.

| Lab | Etap projektu | Scenariusz fabularny |
|-----|--------------|---------------------|
| **LAB_01** | Infrastruktura | *"Pierwszy dzień w projekcie — stawiamy workspace. Tworzymy cluster, konfigurujemy Volume na surowe pliki, podpinamy external connection do źródła danych."* |
| **LAB_02** | Import danych | *"Dostaliśmy pierwszy eksport z systemu sklepowego — pliki CSV z klientami i produktami. Ładujemy je do Lakehouse, czyścimy i zapisujemy jako tabele Delta."* |
| **LAB_03** | Aktualizacja danych | *"Przyszła aktualizacja danych klientów — część to nowi klienci, część to zmiany adresów. Używamy MERGE. Ktoś przez pomyłkę usunął kolumnę — ratujemy się Time Travel."* |
| **LAB_04** | Optymalizacja | *"Tabela zamówień urosła do milionów wierszy. Czasy zapytań się wydłużyły. Optymalizujemy: OPTIMIZE, VACUUM, wdrażamy Liquid Clustering."* |
| **LAB_05** | Dane real-time | *"RetailHub uruchomił nowy kanał sprzedaży — zamówienia spływają jako pliki JSON co 5 minut. Konfigurujemy Auto Loader do ciągłego przetwarzania."* |
| **LAB_06** | Analityka | *"Dział biznesowy chce raportów: Top 10 produktów, ranking klientów wg wydatków, analiza trendów czasowych. Budujemy zapytania analityczne w PySpark i SQL."* |
| **LAB_07** | Pipeline produkcyjny | *"Czas sformalizować przepływ danych. Budujemy pipeline Medallion w Lakeflow: Bronze (surowe) → Silver (czyste) → Gold (agregaty), z expectations i SCD Type 2."* |
| **LAB_08** | Orkiestracja | *"Pipeline działa — czas go zautomatyzować. Konfigurujemy multi-task job z triggerami (cron, file arrival), definiujemy zależności między taskami i testujemy repair run po błędzie."* |
| **LAB_09** | Governance | *"Przed go-live: zabezpieczamy dane. Dział HR nie powinien widzieć kwot zamówień. Dane PII klientów maskujemy. Udostępniamy agregaty partnerowi via Delta Sharing."* |

#### Zasady techniczne

- **Niezaleznosc:** Kazdy lab posiada komorke `# Setup` na poczatku, ktora sprawdza i tworzy wymagane tabele/dane (`IF NOT EXISTS`). Uczestnik moze uruchomic dowolny lab bez wykonywania poprzednich.
- **Wspolna przestrzen:** Wszystkie laby operuja w katalogu `retailhub_{username}` i schematach `bronze`/`silver`/`gold`, tworzac spojna baze projektu.
- **Walidacja:** Kazde zadanie konczy sie komorka `assert` weryfikujaca wynik. Uczestnik natychmiast widzi czy rozwiazanie jest poprawne.
- **Dwa pliki:** `LAB_XX_guide.pdf` (teoria, instrukcje, scenariusz, screenshoty) + `LAB_XX_code.ipynb` (kod z `# TODO` do uzupelnienia + asserty).

---

### Przewodnik prowadzenia labow (Lab Conduct Guide)

#### Przed szkoleniem (trener)

1. Upewnic sie, ze kazdy uczestnik ma konto w Databricks workspace.
2. Wgrac datasety do wspolnej lokalizacji (Volume lub DBFS).
3. Zweryfikowac, ze `00_setup.ipynb` dziala poprawnie (tworzy katalog, schematy, laduje dane).
4. Przygotowac gotowe rozwiazania labow (notebooki bez `# TODO`) do pokazania w razie potrzeby.

#### Prowadzenie laba (krok po kroku)

1. **Wprowadzenie (2-3 min):** Otworz `LAB_XX_guide.pdf` i omow cel laba, scenariusz fabularny i szacowany czas.
2. **Setup (1 min):** Poprosi uczestnikow o otwarcie `LAB_XX_code.ipynb` i uruchomienie komorki Setup.
3. **Praca samodzielna (XX min):** Uczestnicy pracuja samodzielnie, uzupelniajac `# TODO` w kodzie. Trener chodzi po sali / monitoruje czat.
4. **Wskazowki:** Jesli uczestnik utknol dluzej niz 3 minuty -- daj hint (nie pelne rozwiazanie). Jesli wielu utknielo -- pokaz rozwiazanie danego taska na projektorze.
5. **Walidacja:** Kazdy task ma komorke `assert` -- uczestnik uruchamia ja sam. Jesli assert przechodzi, task jest zaliczony.
6. **Podsumowanie (2-3 min):** Po zakonczeniu laba omow kluczowe takeaways i Exam Tips z guide'a.

#### Typowe problemy i rozwiazania

| Problem | Rozwiazanie |
|---------|------------|
| Setup nie dziala | Sprawdz cluster status, sprawdz czy `00_setup` jest w sciezce `../setup/` |
| Permission denied | Uczestnik nie ma uprawnien -- trener nadaje `USE CATALOG` + `USE SCHEMA` |
| Assert nie przechodzi | Sprawdz czy TODO zostalo poprawnie uzupelnione; poinstruuj uczestnika o dokladnym wymaganym kodzie |
| Uczestnik skonczyl wczesniej | Zaproponuj BONUS task z guide'a lub cheatsheet do przejrzenia |
| Uczestnik nie nadaza | Pokaz rozwiazanie na projektorze, pozwol skopiowac i kontynuowac |

#### Format placeholderow w guide'ach

- `<screen = opis>` -- miejsce na screenshot z UI Databricks. Trener moze dodac wlasne screenshoty lub nakazac uczestnikom wykonanie ich samodzielnie.
- `# TODO` / `________` -- placeholder w kodzie do uzupelnienia przez uczestnika.

---

### Struktura folderów

Każdy dzień (`day1/`, `day2/`, `day3/`) zawiera dwa podfoldery:

```
training_2026/
├── setup/
│   ├── 00_pre_config.ipynb   # Trener: tworzenie catalogów
│   └── 00_setup.ipynb        # Uczestnik: walidacja środowiska
├── day1/
│   ├── demo/          # M01, M02, M03
│   ├── lab/           # LAB_01-03 (code.ipynb + guide.pdf)
│   └── materials/     # day1_quiz.pdf + day1_cheatsheet.pdf
├── day2/
│   ├── demo/          # M04, M05, M06
│   ├── lab/           # LAB_04-06 (code.ipynb + guide.pdf)
│   └── materials/     # day2_quiz.pdf + day2_cheatsheet.pdf
└── day3/
    ├── demo/          # M07, M08, M09
    ├── lab/           # LAB_07-09 (code.ipynb + guide.pdf)
    └── materials/     # day3_quiz.pdf + day3_cheatsheet.pdf
```

- **demo/** — notebooki demonstracyjne prowadzone przez trenera (M0X_*.ipynb)
- **lab/** — notebooki kodu + PDF guide do samodzielnej pracy (LAB_XX_code.ipynb + LAB_XX_guide.pdf)
- **materials/** — quiz exam-style + ściąga (cheatsheet) w PDF

> **Źródła** (.md, .ipynb) w `utilization/` (w .gitignore) -- do generowania PDF.

### Struktura notebooków demo (moduły M01–M09)

Każdy notebook demo powinien mieć spójną strukturę:

```
┌─────────────────────────────────────────────────────┐
│  1. NAGŁÓWEK                                        │
│     - Tytuł modułu (# M0X: Nazwa)                  │
│     - Cel modułu (1–2 zdania)                       │
│     - Lista tematów / Learning Objectives           │
│     - Szacowany czas                                │
├─────────────────────────────────────────────────────┤
│  2. SETUP                                           │
│     - %run ../../setup/00_setup                     │
│     - Konfiguracja zmiennych (catalog, schema)      │
│     - Import/tworzenie danych do demo               │
├─────────────────────────────────────────────────────┤
│  3. TRESC GLOWNA (powtarzalny wzorzec)              │
│     Dla kazdego tematu:                             │
│     a) Markdown: teoria, koncepcja, diagram          │
│     b) Code cell: live demo z komentarzami           │
│     c) Code cell: wariant / rozszerzenie             │
│     d) Markdown: podsumowanie + exam tip             │
├─────────────────────────────────────────────────────┤
│  4. PORÓWNANIA (jeśli dotyczy)                      │
│     - Tabele porównawcze w markdown                 │
│     - Np. Views: Temp vs Global vs Permanent        │
│     - Np. COPY INTO vs Auto Loader                  │
├─────────────────────────────────────────────────────┤
│  5. PODSUMOWANIE                                    │
│     - Key Takeaways (5-7 bulletow)                   │
│     - Exam Tips (co warto zapamietac)                │
│     - Co dalej (link do nastepnego modulu/labu)      │
├─────────────────────────────────────────────────────┤
│  6. CLEANUP (opcjonalnie)                           │
│     - Usunięcie tabel tymczasowych                  │
│     - spark.catalog.clearCache()                    │
└─────────────────────────────────────────────────────┘
```

#### Konwencje w komórkach kodu

| Element | Wzorzec |
|---------|--------|
| Nagłówek sekcji | `# COMMAND ----------` + markdown z `## Temat` |
| Komentarze | Polski (treść szkoleniowa) |
| Zmienne | snake_case, prefiksy: `df_`, `path_`, `table_` |
| SQL w notebooku | `%sql` magic lub `spark.sql()` z komentarzem kiedy co |
| Print wyników | `display(df)` zamiast `df.show()` (Databricks native) |
| Exam tip | Komórka markdown: `> **Exam Tip:** ...` |
| Ważne uwagi | Komórka markdown: `> **Uwaga:** ...` |

---

### Dzień 1: Platform, ELT Ingestion & Delta Lake Fundamentals (9:00–16:00)

**Cel dnia:** Zrozumienie platformy, ELT import danych, transformacje, Delta Lake — fundamenty

| # | Blok | Typ | ~Czas | Materiał |
|---|------|-----|-------|----------|
| 1 | Wprowadzenie, agenda, logowanie do workspace | [T] | 25 min | Slajdy |
| 2 | **M01: Platform & Workspace** — Lakehouse arch, Compute (Serverless domyslny), Git Folders, UC Volumes, magic commands, dbutils, SQL Editor | [T][D] | 55 min | `demo/M01_platform_workspace` |
| | Przerwa | | 10 min | |
| 3 | **LAB 01: Platform Setup** — Tworzenie clustra, Unity Catalog Volume, External Connection | [L] | 35 min | `lab/LAB_01_guide.pdf` + `lab/LAB_01_code.ipynb` |
| 4 | **M02: ELT Data Ingestion** (cz.1) — ELT vs ETL, CSV/JSON/Parquet read, inferSchema vs explicit, `read_files()`, Bronze layer | [D] | 55 min | `demo/M02_elt_ingestion` (cz.1) |
| | Przerwa | | 10 min | |
| 5 | **M02: Transforms & Views** (cz.2) — select, withColumn, filter, groupBy, agg, Views (Temp/Global Temp/Permanent), SQL, czyszczenie danych | [D] | 50 min | `demo/M02_elt_ingestion` (cz.2) |
| 6 | **LAB 02: Ingestion & Transforms** — odczyt CSV, transformacje, zapis Delta | [L] | 40 min | `lab/LAB_02_guide.pdf` + `lab/LAB_02_code.ipynb` |
| | Przerwa | | 10 min | |
| 7 | **M03: Delta Lake Fundamentals** — ACID, Delta Log, Schema Enforcement & Evolution, Time Travel, DESCRIBE HISTORY/DETAIL, RESTORE | [D] | 55 min | `demo/M03_delta_fundamentals` |
| 8 | **Quiz Dnia 1** (5-10 pytan exam-style) + Q&A + preview dnia 2 | [T] | 15 min | `materials/day1_quiz.pdf` |

> Dzien 1: ~330 min tresci + 30 min przerw + ~45 min obiad = 6h 45min

---

### Dzień 2: Delta DML & Optimization, Streaming & Advanced Transforms (9:00–16:00)

**Cel dnia:** Delta DML, optymalizacja, incremental processing, zaawansowane transformacje PySpark & SQL

| # | Blok | Typ | ~Czas | Materiał |
|---|------|-----|-------|----------|
| 1 | Recap dnia 1, pytania | [T] | 10 min | |
| 2 | **M03: Delta Fundamentals** (cz.2) — MERGE INTO, UPDATE, DELETE, CLONE, managed vs external tables | [D] | 45 min | `demo/M03_delta_fundamentals` (sekcje DML) |
| 3 | **LAB 03: Delta DML & Time Travel** — MERGE, UPDATE, Time Travel, RESTORE | [L] | 35 min | `lab/LAB_03_guide.pdf` + `lab/LAB_03_code.ipynb` |
| | Przerwa | | 10 min | |
| 4 | **M04: Delta Lake Optimization** — OPTIMIZE, ZORDER, VACUUM, Liquid Clustering, Automatic LC, Deletion Vectors, Predictive Optimization, INSERT REPLACE | [D] | 45 min | `demo/M04_delta_optimization` |
| 5 | **LAB 04: Delta Optimization** — OPTIMIZE, VACUUM, Liquid Clustering | [L] | 30 min | `lab/LAB_04_guide.pdf` + `lab/LAB_04_code.ipynb` |
| | Przerwa | | 10 min | |
| 6 | **M05: Incremental Data Processing** — COPY INTO (batch), Auto Loader (stream), Structured Streaming, readStream/writeStream, triggers, output modes (append/complete/update), checkpoints | [D] | 55 min | `demo/M05_incremental_processing` |
| 7 | **LAB 05: Streaming & Auto Loader** — Auto Loader, streaming do Delta, trigger.availableNow | [L] | 35 min | `lab/LAB_05_guide.pdf` + `lab/LAB_05_code.ipynb` |
| | Przerwa | | 10 min | |
| 8 | **M06: Advanced Transforms — PySpark & SQL** — Window Functions, CTE, Subqueries, Explode, JSON, Date, CASE WHEN, UDF, Higher-order functions, CTAS | [D] | 55 min | `demo/M06_advanced_transforms` |
| 9 | **Quiz Dnia 2** (5-10 pytan exam-style) + Q&A | [T] | 15 min | `materials/day2_quiz.pdf` |

> Dzien 2: ~325 min tresci + 30 min przerw + ~45 min obiad = 6h 40min

---

### Dzień 3: Lakeflow, Orkiestracja, Governance & Exam Prep (9:00–16:00)

**Cel dnia:** Lab transformacyjny, Medallion & Lakeflow pipelines, orkiestracja, governance, przygotowanie do egzaminu

| # | Blok | Typ | ~Czas | Materiał |
|---|------|-----|-------|----------|
| 1 | Recap dnia 2, pytania | [T] | 10 min | |
| 2 | **LAB 06: Advanced Transforms** -- Window functions PySpark & SQL, CTE, Explode, CTAS | [L] | 40 min | `lab/LAB_06_guide.pdf` + `lab/LAB_06_code.ipynb` |
| | Przerwa | | 10 min | |
| 3 | **M07: Medallion & Lakeflow Pipelines** -- Bronze->Silver->Gold arch (~20 min) -> CREATE STREAMING TABLE, MATERIALIZED VIEW, ST vs MV, Expectations, AUTO CDC, DAG (~50 min) | [D] | 70 min | `demo/M07_medallion_lakeflow` |
| 4 | **LAB 07: Lakeflow Pipeline** -- Budowa pipeline Medallion z expectations i SCD Type 2 | [L] | 45 min | `lab/LAB_07_guide.pdf` + `lab/LAB_07_code.ipynb` |
| | Przerwa | | 10 min | |
| 5 | **M08: Lakeflow Jobs & Orchestration** -- Multi-task jobs, scheduling, triggers, parametry, task types, repair runs, system tables + demo tworzenia job w UI | [D] | 45 min | `demo/M08_orchestration` |
| 6 | **LAB 08: Orchestration** -- Triggers, Dependencies, Job Configuration, Repair Runs | [L] | 30 min | `lab/LAB_08_guide.pdf` + `lab/LAB_08_code.ipynb` |
| | Przerwa | | 10 min | |
| 7 | **M09: Governance & Security** -- Unity Catalog, GRANT/REVOKE, Row Filters, Column Masks, Lineage, Delta Sharing, Information Schema, Data Classification, ABAC | [D] | 50 min | `demo/M09_governance` |
| 8 | **LAB 09: Governance** -- Permissions, Row Filters, Column Masking, Information Schema | [L] | 30 min | `lab/LAB_09_guide.pdf` + `lab/LAB_09_code.ipynb` |
| 9 | **Quiz Dnia 3** + **Exam Prep** -- exam tips, zasoby do nauki, Q&A koncowe | [T] | 25 min | `materials/day3_quiz.pdf` + Slajdy |
> Dzien 3: ~335 min tresci + 30 min przerw + ~45 min obiad = 6h 50min

---

### Podsumowanie czasowe

| Metryka | Dzień 1 | Dzień 2 | Dzień 3 | Suma |
|---------|---------|---------|---------|------|
| Tresci (demo+teoria) | 240 min | 225 min | 205 min | 670 min |
| Labs | 75 min | 100 min | 120 min | 295 min |
| Quizy | 15 min | 15 min | 25 min | 55 min |
| Przerwy krotkie | 30 min | 30 min | 30 min | 90 min |
| Obiad (implicit) | ~45 min | ~45 min | ~45 min | ~135 min |
| **Razem** | **~6h45** | **~6h40** | **~6h40** | **~980 min** |

---

### Tematy do dodania/zaktualizowania

**Dzień 1:**
- [x] Serverless Compute jako domyślny, Git Folders, UC Volumes
- [x] `read_files()` jako unified function, Excel Beta
- [x] **ELT vs ETL** teoria w M02
- [x] **Views:** Temp vs Global Temp vs Permanent porównanie w M02
- [x] **DESCRIBE HISTORY / DETAIL** jawne demo w M03
- [x] **Notebook magic commands & dbutils** sekcja w M01
- [x] **LAB_01**: GUI - Cluster, Volume, External Connection

**Dzień 2:**
- [x] **M04: Delta Optimization** — OPTIMIZE, ZORDER, VACUUM, Liquid Clustering, Predictive Optimization, Deletion Vectors
- [x] **Output modes** (append/complete/update) jawna sekcja w M05
- [x] **M06 merged PySpark & SQL** side-by-side, UDF, Higher-order functions
- [x] **Mini-quiz** exam-style na koniec dnia

**Dzień 3:**
- [x] **Medallion + Lakeflow SDP złączone** w M07 (ST vs MV, AUTO CDC)
- [x] **Task types & repair runs** sekcja w M08
- [x] **Information Schema** demo w M09
- [x] **Data Classification, ABAC** wzmianki w M09
- [x] **Exam prep tips** + quiz końcowy

---

## 4. Datasety: nasze vs. wbudowane w Databricks

### Rekomendacja: **Korzystamy z naszych datasetów** (z uzupełnieniami)

| Aspekt | Nasze datasety | Datasety Databricks |
|--------|---------------|-------------------|
| **Kontrola** | [OK] Pełna kontrola nad schematem, wielkością, jakością | [BRAK] Zależność od wersji platformy |
| **Spójność narracji** | [OK] Jeden story (e-commerce) przez 3 dni | [BRAK] Różne konteksty |
| **Reprodukowalność** | [OK] Identyczne wyniki dla każdego uczestnika | [CZESCIOWO] Mogą się zmienić |
| **Streaming** | [OK] Mamy pliki stream (6 plików JSON) | [BRAK] Trzeba generować |
| **Offline access** | [OK] W repozytorium | [BRAK] Wymaga internetu |
| **Realność** | [CZESCIOWO] Syntetyczne ale realistyczne | [OK] Mogą być bardziej realistyczne |
| **Aktualizacja** | [OK] Łatwa | [BRAK] Zależna od Databricks |

**Decyzja:** Pozostajemy przy naszych datasetach. Są dobrze zaprojektowane, spójne z narracją szkolenia i pokrywają potrzebne scenariusze.

**Brakujące datasety do dodania:**
- [x] ~~Dane do demonstracji funkcji okienkowych~~ → **ZREALIZOWANE**: 06a/06b używają inline sample data (orders, products), nie potrzebują zewnętrznych plików
- [x] ~~Dane z complex types~~ → **ZREALIZOWANE**: 06a zawiera inline dane z ArrayType, array of structs, nested JSON
- [x] ~~Opcjonalnie: dodać plik JSON z nested events do dataset/~~ -- inline w M06 (nie potrzebny osobny plik)

---

## 5. Plan działania - TODO

### Priorytet: WYSOKI (blokujące)

| # | Zadanie | Priorytet | Estymacja |
|---|---------|-----------|-----------|
| 1 | ~~Stworzyć notebook: zaawansowane transformacje~~ → **ZREALIZOWANE** (06a_advanced_pyspark + 06b_advanced_sql) | [OK] DONE | — |
| 2 | ~~Stworzyc nowy M04_delta_optimization~~ | [OK] DONE | -- |
| 3 | ~~Stworzyc nowy M06_advanced_transforms -- PySpark & SQL side-by-side~~ | [OK] DONE | -- |
| 4 | ~~Stworzyc M07_medallion_lakeflow -- zlaczenie Medallion + Lakeflow SDP + ST vs MV~~ | [OK] DONE | -- |
| 5 | ~~Zaktualizowac nazewnictwo (DLT->Lakeflow SDP, Jobs->Lakeflow Jobs)~~ | [OK] DONE | -- |
| 6 | ~~Zaktualizowac README.md pod nowa strukture 9 modulow~~ | [OK] DONE | -- |

### Priorytet: ŚREDNI (ulepszenia)

| # | Zadanie | Priorytet | Estymacja |
|---|---------|-----------|-----------|
| 7 | ~~Dodac AUTO CDC (ex-APPLY CHANGES) do M07~~ | [OK] DONE | -- |
| 8 | ~~Rozszerzyc M02 o read_files() unified function + ELT vs ETL~~ | [OK] DONE | -- |
| 9 | ~~Dodac INSERT REPLACE demo do M04~~ | [OK] DONE | -- |
| 10 | ~~Stworzyc cheatsheet/podsumowanie na kazdy dzien~~ | [OK] DONE | -- |
| 11 | ~~Stworzyc quizy exam-style na kazdy dzien (5-10 pytan)~~ | [OK] DONE | -- |
| 12 | Przygotowac slajdy wprowadzajace (agenda, architektura) | SREDNI | 2h | (poza repo -- PowerPoint/Canva) |

### Priorytet: NISKI (nice-to-have)

| # | Zadanie | Priorytet | Estymacja |
|---|---------|-----------|-----------|
| 13 | ~~Dodac Dataset z nested JSON (complex types)~~ -- inline w M06 | [OK] DONE | -- |
| 14 | Dodac diagram architektury do `assets/images/training_2026/` | NISKI | 1h | (poza repo -- Mermaid/Draw.io) |
| 15 | ~~Wzmianka o Lakeflow Connect (managed connectors) w M01~~ | [OK] DONE | -- |
| 16 | ~~Dodac sekcje o Data Classification i ABAC do M09~~ | [OK] DONE | -- |

### Estymacja łączna

| Kategoria | Czas |
|-----------|------|
| Krytyczne (#2-#6) | ~10-12h |
| Średnie (#7-#12) | ~9-12h |
| Nice-to-have (#13-#16) | ~3.5h |
| **RAZEM** | **~25-28h pracy** |

---

## 6. Podsumowanie

### Co mamy [DONE]
- **9 demo notebooków (M01-M09)** pokrywających ~95% tematów egzaminacyjnych
- **9 labów (LAB_01-LAB_09)** -- guide.md + code.ipynb z `# TODO` i assertami
- **3 quizy** (20 pytań exam-style per dzień) + wersje PDF
- **3 cheatsheets** (quick reference per dzień)
- **Setup notebook** (`00_setup.ipynb`) z izolacją per-user (catalog + schemas)
- **Kompletne definicje Lakeflow pipelines** (Bronze/Silver/Gold)
- **Spójny dataset** (RetailHub e-commerce) z CSV, JSON, streaming
- **Struktura folderów** `training_2026/{setup,day1,day2,day3}/{demo,lab}`
- **README.md** zaktualizowane pod 3-dniowe szkolenie
- **ABAC** (Tags + Column Masks + Row Filters) w M09
- **External Connection** GUI task w LAB_01

### Czego brakuje [TODO] -> Plan rozwiazania
Wszystkie blokery zamknięte. Opcjonalne elementy:
- Slajdy wprowadzające (agenda, architektura) -- PowerPoint/Canva, poza repo
- Diagram architektury -- Mermaid/Draw.io, poza repo

> **Wniosek:** Materiały szkoleniowe kompletne. Wszystkie 9 modułów, 9 labów, quizy i cheatsheets gotowe.  
> **Struktura:** 9 modułów (M01-M09) + 9 labów (LAB_01-LAB_09) + 3 quizy + 3 cheatsheets
