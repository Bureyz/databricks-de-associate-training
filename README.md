# Databricks Data Engineer Associate -- Training (3 Days)

## About This Repository

Training materials for the **Databricks Data Engineer Associate** certification preparation course (3 days x 7h). The training uses a hands-on project narrative: **RetailHub** -- building a complete analytics platform for an e-commerce company.

**Certification exam:** 45 questions, 90 minutes, ~70% passing score.

## Training Structure

| Day | Modules | Focus |
|-----|---------|-------|
| **Day 1** | M01, M02, M03 | Platform, Ingestion & Transformations, Delta Lake |
| **Day 2** | M04, M05, M06 | Streaming, Medallion & Lakeflow, Orchestration |
| **Day 3** | M07, M08, M09 | Advanced Delta, Exam Prep & Labs, Governance |

## Modules (Demo Notebooks)

| # | Module | Path |
|---|--------|------|
| M01 | Platform & Workspace | `training_2026/day1/demo/M01_platform_workspace.ipynb` |
| M02 | ELT & Ingestion | `training_2026/day1/demo/M02_elt_ingestion.ipynb` |
| M03 | Delta Lake Fundamentals | `training_2026/day1/demo/M03_delta_fundamentals.ipynb` |
| M04 | Delta Optimization | `training_2026/day2/demo/M04_delta_optimization.ipynb` |
| M05 | Incremental Processing | `training_2026/day2/demo/M05_incremental_processing.ipynb` |
| M06 | Advanced Transforms | `training_2026/day2/demo/M06_advanced_transforms.ipynb` |
| M07 | Medallion & Lakeflow | `training_2026/day3/demo/M07_medallion_lakeflow.ipynb` |
| M08 | Orchestration & Jobs | `training_2026/day3/demo/M08_orchestration.ipynb` |
| M09 | Governance & Security | `training_2026/day3/demo/M09_governance.ipynb` |

## Labs

Each module has a corresponding hands-on lab (`LAB_XX_code.ipynb` + `LAB_XX_guide.pdf`):

| # | Lab | Path |
|---|-----|------|
| LAB_01 | Platform & Workspace Setup | `training_2026/day1/lab/` |
| LAB_02 | Ingestion & Transformations | `training_2026/day1/lab/` |
| LAB_03 | Delta Lake Operations | `training_2026/day1/lab/` |
| LAB_04 | Streaming & Auto Loader | `training_2026/day2/lab/` |
| LAB_05 | Lakeflow Pipeline | `training_2026/day2/lab/` |
| LAB_06 | Workflow Orchestration | `training_2026/day2/lab/` |
| LAB_07 | Advanced Delta Optimization | `training_2026/day3/lab/` |
| LAB_08 | Exam Practice Lab | `training_2026/day3/lab/` |
| LAB_09 | Governance & Unity Catalog | `training_2026/day3/lab/` |

## Additional Materials

- **Quizzes:** 3 x 20 exam-style questions per day (PDF in `materials/quiz/`)
- **Cheatsheets:** Quick reference per day (PDF in `materials/cheatsheet/`)
- **Lab Guides:** Step-by-step instructions (PDF in `training_2026/dayX/lab/`)
- **Training Plan:** `docs/TRAINING_PLAN_2026.md`

## Repository Structure

```text
.
├── dataset/                          # Sample datasets (CSV, JSON)
│   ├── customers/
│   ├── orders/
│   ├── products/
│   └── workshop/
├── docs/                             # Training plan & documentation
│   └── TRAINING_PLAN_2026.md
├── materials/                         # Shared training materials
│   ├── medallion/                    # Medallion pipeline notebooks (Bronze/Silver/Gold)
│   ├── orchestration/                # Orchestration task notebooks
│   └── lakeflow/                     # Lakeflow (DLT) pipeline definitions
├── training_2026/                    # Main training content
│   ├── setup/
│   │   ├── 00_pre_config.ipynb       # Trainer: create catalogs for participants
│   │   └── 00_setup.ipynb            # Participant: validate environment
│   ├── day1/
│   │   ├── demo/                     # M01, M02, M03
│   │   ├── lab/                      # LAB_01-03 code + guide PDF
│   │   └── materials/                # day1_quiz.pdf + day1_cheatsheet.pdf
│   ├── day2/
│   │   ├── demo/                     # M04, M05, M06
│   │   ├── lab/                      # LAB_04-06 code + guide PDF
│   │   └── materials/                # day2_quiz.pdf + day2_cheatsheet.pdf
│   └── day3/
│       ├── demo/                     # M07, M08, M09
│       ├── lab/                      # LAB_07-09 code + guide PDF
│       └── materials/                # day3_quiz.pdf + day3_cheatsheet.pdf
├── utilization/                      # Source files (.md, .ipynb) for PDF generation
│   ├── quiz/                         # Quiz source notebooks
│   ├── cheatsheet/                   # Cheatsheet source markdown
│   └── lab_guide/                    # Lab guide source markdown
└── README.md
```

## Prerequisites

- Basic knowledge of SQL and Python
- Access to Databricks workspace (URL + credentials provided by trainer)
- Basic understanding of cloud storage and database concepts

## How to Use

1. Clone the repository to your Databricks Workspace via **Repos**
2. Run `training_2026/setup/00_setup.ipynb` to create your catalog and schemas
3. Follow the modules in order (M01 through M09)
4. Complete the lab after each module using the guide and code notebook
5. Take the daily quiz to test your knowledge

## Project: RetailHub

All modules and labs use a shared narrative -- **RetailHub**, an e-commerce analytics platform:

- **Catalog:** `retailhub_{username}`
- **Schemas:** `bronze` (raw), `silver` (cleaned), `gold` (aggregated)
- **Data sources:** Customers (CSV), Products (CSV), Orders (JSON + streaming)
- **Pipeline:** Raw files --> Bronze --> Silver --> Gold --> Reports

---
*Altkom Akademia Training Materials*
