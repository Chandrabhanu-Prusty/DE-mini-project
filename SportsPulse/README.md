# SportsPulse

An end-to-end Data Engineering pipeline for sports analytics (IPL match and ball-by-ball data) built using a Medallion Architecture (Bronze -> Silver -> Gold).

## Project Structure

```
SportsPulse/
│
├── data/
│   ├── bronze/          # Raw ingested data (IPL JSON files)
│   │   └── ipl/
│   ├── silver/          # Cleansed, normalized and typed tables
│   ├── gold/            # Curated business-level aggregates & metrics
│   └── quarantine/      # Records failing validation checks
│
├── src/
│   ├── ingestion/       # Data acquisition and raw file registration
│   │   ├── acquire_data.py
│   │   └── register_files.py
│   │
│   ├── validation/      # Schema enforcement and data quality checks
│   │   ├── validate_schema.py
│   │   └── quality_check.py
│   │
│   ├── processing/      # Transformations across Medallion layers
│   │   ├── parse_matches.py
│   │   ├── normalize_entities.py
│   │   ├── build_silver.py
│   │   └── build_gold.py
│   │
│   ├── warehouse/       # Loading processed data to analytics store
│   │   └── load_warehouse.py
│   │
│   └── reporting/       # Metrics refresh and quality reporting
│       ├── refresh_dashboard_tables.py
│       └── publish_quality_report.py
│
├── dashboard/           # Visualization application
│   └── app.py
│
├── dags/                # Airflow / Orchestration DAG definitions
│   └── sportspulse_pipeline.py
│
├── models/              # ML/analytics models
├── notebooks/           # Exploratory notebooks
├── tests/               # Unit and integration tests
├── logs/                # Execution logs
├── reports/             # Generated HTML/PDF data quality reports
├── sql/                 # Ad-hoc & analytical queries
│
├── .env                 # Environment configuration
├── .gitignore
├── requirements.txt
└── README.md
```

## Setup & Getting Started

1. **Create and activate a virtual environment**:
   ```powershell
   uv venv --python 3.11 .venv
   .\.venv\Scripts\Activate.ps1
   ```

2. **Install dependencies**:
   ```powershell
   pip install -r requirements.txt
   ```
