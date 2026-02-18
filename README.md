# Databricks Data Engineer Associate — Training (3 Days)

## About This Repository

Training materials for the **Databricks Data Engineer Associate** certification preparation course (3 days × 7h). The training uses a hands-on project narrative: **RetailHub** — building a complete analytics platform for an e-commerce company.

**Certification exam:** 45 questions, 90 minutes, ~70% passing score.

## Training Structure

| Day | Modules | Focus |
|-----|---------|-------|
| **Day 1** | M00–M03 | Intro, Platform, ELT & Ingestion, Delta Lake Fundamentals |
| **Day 2** | M04–M06 | Delta Optimization, Incremental Processing, Advanced Transforms |
| **Day 3** | M07–M10 | Medallion & Lakeflow, Orchestration, Governance, Exam Prep |

## Modules (Demo Notebooks)

| # | Module | Path |
|---|--------|------|
| M00 | Training Introduction | `notebooks/day1/demo/M00_intro.ipynb` |
| M01 | Platform & Workspace | `notebooks/day1/demo/M01_platform_workspace.ipynb` |
| M02 | ELT & Ingestion | `notebooks/day1/demo/M02_elt_ingestion.ipynb` |
| M03 | Delta Lake Fundamentals | `notebooks/day1/demo/M03_delta_fundamentals.ipynb` |
| M04 | Delta Optimization | `notebooks/day2/demo/M04_delta_optimization.ipynb` |
| M05 | Incremental Processing | `notebooks/day2/demo/M05_incremental_processing.ipynb` |
| M06 | Advanced Transforms | `notebooks/day2/demo/M06_advanced_transforms.ipynb` |
| M07 | Medallion & Lakeflow | `notebooks/day3/demo/M07_medallion_lakeflow.ipynb` |
| M08 | Orchestration & Jobs | `notebooks/day3/demo/M08_orchestration.ipynb` |
| M09 | Governance & Security | `notebooks/day3/demo/M09_governance.ipynb` |
| M10 | Exam Prep | `notebooks/day3/demo/M10_exam_prep.ipynb` |

## Labs

Each module has a corresponding hands-on lab (`LAB_XX_code.ipynb` + `LAB_XX_guide.ipynb` / `.pdf`):

| # | Lab | Path |
|---|-----|------|
| LAB_01 | Platform & Workspace Setup | `notebooks/day1/lab/` |
| LAB_02 | Ingestion & Transformations | `notebooks/day1/lab/` |
| LAB_03 | Delta Lake Operations | `notebooks/day1/lab/` |
| LAB_04 | Delta Optimization | `notebooks/day2/lab/` |
| LAB_05 | Streaming & Auto Loader | `notebooks/day2/lab/` |
| LAB_06 | Advanced Transforms | `notebooks/day2/lab/` |
| LAB_07 | Medallion & Lakeflow | `notebooks/day3/lab/` |
| LAB_08 | Orchestration & Jobs | `notebooks/day3/lab/` |
| LAB_09 | Governance & Unity Catalog | `notebooks/day3/lab/` |

## Additional Materials

- **Quizzes:** 3 × 20 exam-style questions per day (PDF in `docs/quiz/`)
- **Cheatsheets:** Quick reference per day (PDF in `docs/cheatsheet/`)
- **Lab Guides:** Step-by-step instructions (notebook + PDF in `notebooks/dayX/lab/guide/`)
- **Training Plan:** `utilization/TRAINING_PLAN_2026.md`

## Repository Structure

```text
.
├── dataset/                          # Sample datasets (CSV, JSON)
│   ├── customers/                    # customers.csv, customers_new.csv, customers_extended.csv
│   ├── orders/                       # orders_batch.json + stream/ (streaming JSON files)
│   └── products/                     # products.csv
├── docs/                             # Generated PDFs for participants
│   ├── cheatsheet/                   # day1-3_cheatsheet.pdf
│   └── quiz/                         # day1-3_quiz.pdf
├── materials/                        # Supporting pipeline notebooks
│   ├── medallion/                    # Medallion pipeline (Bronze/Silver/Gold)
│   ├── orchestration/                # Job task notebooks
│   └── lakeflow/                     # Lakeflow (DLT) pipeline definitions
├── notebooks/                        # Main training content
│   ├── setup/
│   │   ├── 00_pre_config.ipynb       # Trainer: create catalogs for participants
│   │   └── 00_setup.ipynb            # Participant: validate environment
│   ├── day1/
│   │   ├── demo/                     # M00, M01, M02, M03
│   │   └── lab/                      # LAB_01-03 code + guide (ipynb + pdf)
│   ├── day2/
│   │   ├── demo/                     # M04, M05, M06
│   │   └── lab/                      # LAB_04-06 code + guide (ipynb + pdf)
│   └── day3/
│       ├── demo/                     # M07, M08, M09, M10
│       └── lab/                      # LAB_07-09 code + guide (ipynb + pdf)
├── utilization/                      # Source files for PDF generation (not deployed)
│   ├── quiz/                         # Quiz source notebooks (.ipynb)
│   ├── cheatsheet/                   # Cheatsheet source markdown (.md)
│   ├── lab_guide/                    # Lab guide source markdown (.md)
│   └── generate_pdfs.py             # PDF generation script
└── README.md
```

## Prerequisites

- Basic knowledge of SQL and Python
- Access to Databricks workspace (URL + credentials provided by trainer)
- Basic understanding of cloud storage and database concepts

## How to Use

1. Clone the repository to your Databricks Workspace via **Repos**
2. Run `notebooks/setup/00_setup.ipynb` to create your catalog and schemas
3. Follow the modules in order (M00 through M10)
4. Complete the lab after each module using the guide and code notebook
5. Take the daily quiz to test your knowledge

## Project: RetailHub

All modules and labs use a shared narrative — **RetailHub**, an e-commerce analytics platform:

- **Catalog:** `retailhub_{username}`
- **Schemas:** `bronze` (raw), `silver` (cleaned), `gold` (aggregated)
- **Data sources:** Customers (CSV), Products (CSV), Orders (JSON + streaming)
- **Pipeline:** Raw files → Bronze → Silver → Gold → Reports

---
*Altkom Akademia Training Materials*
