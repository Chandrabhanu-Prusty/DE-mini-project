# ⚡ SportsPulse: End-to-End IPL Sports Analytics Data Engineering Pipeline

> **VESIT — Semester 5 Data Engineering Mini Project**  
> An end-to-end Lakehouse and Data Engineering pipeline built on a **Medallion Architecture (Bronze ➔ Silver ➔ Gold ➔ Warehouse ➔ Dashboard)** for processing, cleansing, and serving granular IPL match and ball-by-ball telemetry (1,240+ historical matches, 2,500+ innings, 295,000+ deliveries).

---

## 📌 Project Overview

**SportsPulse** is an automated data engineering platform designed to ingest raw, nested JSON match files from historical Indian Premier League (IPL) matches, validate schemas, cleanse and normalize delivery-level records into relational entities, compute business-level aggregates (player efficiency, batting averages, economy rates, team win percentages), load them into a PostgreSQL analytical warehouse, and present actionable insights through an interactive **Streamlit** and **Plotly** dashboard.

---

## 🚦 Project Status & Module Implementation

| Layer / Module | Tasks & Deliverables | Implemented Scripts | Status |
| :--- | :--- | :--- | :---: |
| **01. Setup & Environment** | Virtual environment, dependencies, `.env.example`, Git policies | `.venv`, `requirements.txt`, `.env.example` | ✅ Complete |
| **02. Bronze (Ingestion)** | Raw JSON discovery, SHA-256 checksum audit, registration manifest | `src/ingestion/register_files.py` | ✅ Complete |
| **03. Validation & Quality** | Schema enforcement, structural checks, quarantine detection | `src/validation/validate_schema.py`, `src/validation/quality_check.py` | ✅ Complete |
| **04. Silver (Processing)** | Nested JSON de-flattening, dimensional entity normalization | `src/processing/parse_matches.py`, `src/processing/normalize_entities.py` | ✅ Complete |
| **05. Gold (Aggregations)** | Curated analytical marts: batting, bowling, team performance | `src/processing/build_gold.py` | ✅ Complete |
| **06. Warehouse (PostgreSQL)** | Schema creation, dimension upserting, fact reload | `sql/schema.sql`, `src/warehouse/load_warehouse.py` | ✅ Complete |
| **07. Analytics Dashboard** | 5 analytics pages + Pipeline Control Center + System Architecture | `dashboard/app.py`, `dashboard/pipeline_control.py`, `dashboard/architecture.py` | ✅ Complete |
| **08. Orchestration & Scale** | Modular Apache Airflow DAG (8 tasks) & Docker Compose setup | `dags/sportspulse_pipeline.py`, `docker-compose.yaml` | ✅ Complete |
| **09. Automated Testing** | 21 automated unit tests across runner, control, & architecture | `tests/test_runner.py`, `tests/test_dashboard_control.py`, `tests/test_architecture.py` | ✅ Complete |
| **10. Supplementary Data Export** | CSV dataset export with README specification | `data/csv/` (`matches.csv`, `innings.csv`, `deliveries.csv`) | ✅ Complete |

> 📖 **Demonstration & Evaluation Guide**: See [`SportsPulse/DEMO_GUIDE.md`](SportsPulse/DEMO_GUIDE.md) for step-by-step presentation scripts, benchmark row counts, Airflow commands, and dashboard tour instructions.

---

## 🏗️ Data Pipeline Architecture (Medallion Pattern)

The project implements a modern **Medallion Lakehouse Architecture** to guarantee data quality, idempotence, and high analytical query performance:

<p align="center">
  <img src="assets/sportspulse_architecture.png" alt="SportsPulse Architecture Diagram" width="100%">
</p>

### 🔄 End-to-End Pipeline Flow

```mermaid
flowchart TD
    subgraph S0["1. Data Source"]
        RawJSON["Cricsheet IPL Dataset<br/>1,243 match JSON files<br/>(ball-by-ball, innings, metadata)"]
        CSVExp["CSV Dataset<br/>Converted Export<br/>(matches, innings, deliveries)"]
    end

    subgraph S1["2. Bronze Layer (Raw)"]
        Reg["register_files.py<br/>Register source files<br/>Generate SHA-256 checksums"]
        Manifest["file_manifest.csv<br/>Immutable file manifest"]
        RawJSON --> Reg --> Manifest
    end

    subgraph S2["3. Validation & Quality"]
        Val["validate_schema.py<br/>Schema validation"]
        QC["quality_check.py<br/>Data quality checks<br/>Duplicate detection<br/>Consistency checks"]
        Reports["reports/<br/>schema_validation_report.csv<br/>data_quality_report.csv"]
        Reg --> Val --> QC --> Reports
    end

    subgraph S3["4. Silver Layer (Processed)"]
        Parse["parse_matches.py<br/>Parse JSON/CSV to Parquet"]
        Norm["normalize_entities.py<br/>Normalize entities & dimensions"]
        SilverParquet["Silver Parquet Tables<br/>• matches.parquet (1,243 rows)<br/>• innings.parquet (2,514 rows)<br/>• deliveries.parquet (295,732 rows)<br/>• dim_teams.parquet (19 teams)<br/>• dim_players.parquet (807 players)"]
        QC --> Parse --> Norm --> SilverParquet
    end

    subgraph S4["5. Gold Layer (Analytics)"]
        Gold["build_gold.py<br/>Aggregation & Analytics Engine"]
        GoldParquet["Gold Curated Parquet<br/>• player_batting.parquet (738 rows)<br/>• player_bowling.parquet (577 rows)<br/>• team_performance.parquet (19 rows)<br/>• match_summary.parquet (1,243 rows)<br/>• innings_summary.parquet (2,480 rows)"]
        SilverParquet --> Gold --> GoldParquet
    end

    subgraph S5["6. PostgreSQL Warehouse"]
        Loader["load_warehouse.py<br/>Idempotent Upserts & Fact Reload"]
        PG[("PostgreSQL Warehouse<br/>Star Schema<br/>• dim_teams (19)<br/>• dim_players (807)<br/>• fact_matches (1,243)<br/>• fact_player_batting (738)<br/>• fact_player_bowling (577)")]
        GoldParquet --> Loader --> PG
    end

    subgraph Orchestration["Orchestration (Apache Airflow)"]
        Airflow["Airflow 2.8.1 (Docker Compose)<br/>8 Modular Tasks:<br/>1. Register Source Files<br/>2. Validate Schemas<br/>3. Data Quality Checks<br/>4. Parse Matches to Silver<br/>5. Normalize Entities<br/>6. Build Gold Datasets<br/>7. Load PostgreSQL<br/>8. Audit Summary"]
        Airflow -.->|Orchestrates Pipeline| S1
        Airflow -.->|Task Retries & Schedules| S5
    end

    subgraph Serving["SportsPulse Streamlit Dashboard"]
        Streamlit["Streamlit Interactive UI<br/>• Overview (Tournament summary & trends)<br/>• Batting Analytics (Performance & strike rate)<br/>• Bowling Analytics (Wicket takers & economy)<br/>• Team Analytics (Team stats & win %)<br/>• Match Explorer (Search & filter matches)<br/>• Pipeline Control (Trigger & live logs)<br/>• System Architecture (Visual component flow)"]
        PG --> Streamlit
        GoldParquet -.->|Granular Telemetry| Streamlit
    end

    subgraph Access["Outputs & Access"]
        OutCSV["CSV Dataset<br/>data/csv/"]
        OutPG["PostgreSQL Database<br/>localhost:5432/sportspulse"]
        OutPq["Parquet Files<br/>data/silver & data/gold"]
        OutDash["Streamlit Dashboard<br/>http://localhost:8501"]
        OutAF["Airflow UI<br/>http://localhost:8080"]
    end

    Streamlit --- OutDash
    Airflow --- OutAF
    PG --- OutPG
    GoldParquet --- OutPq
    CSVExp --- OutCSV
```

### 🏛️ Pipeline Architectural Stages

1. **Data Source**: 1,243 historical IPL ball-by-ball JSON match files from Cricsheet, plus converted CSV export files (`matches.csv`, `innings.csv`, `deliveries.csv`).
2. **Bronze Layer (Raw)**: Raw ingestion registry with cryptographic SHA-256 checksum audit (`file_manifest.csv`) ensuring zero data tampering.
3. **Validation & Quality**: Automated schema verification against JSON structural requirements, null validation, duplicate detection, and match score sanity checks.
4. **Silver Layer (Processed)**: Nested JSON de-flattening and relational modeling into compressed Apache Parquet datasets (`matches`, `innings`, `deliveries`) and cleansed dimensional tables (`dim_teams`, `dim_players`).
5. **Gold Layer (Analytics)**: Business analytics aggregation engine computing batting averages, strike rates, bowling economy, match summaries, and team win percentages into partitioned Parquet marts.
6. **PostgreSQL Warehouse**: Star schema relational warehouse with idempotent upsert transactions and primary/foreign key referential integrity.
7. **Orchestration (Apache Airflow)**: 8-stage modular DAG running inside Docker Compose with retry logic, execution timeouts, and dependency graphs.
8. **Streamlit Analytics Dashboard**: 7 interactive views (Overview, Batting Analytics, Bowling Analytics, Team Analytics, Match Explorer, Pipeline Control Center, and System Architecture visualizer).
9. **Outputs & Access**: Multi-modal serving endpoints spanning REST/SQL, web dashboards, raw CSV datasets, and Parquet data files.

---

## 🗄️ PostgreSQL Warehouse Schema

The relational data warehouse is modeled as a star schema in PostgreSQL:

- **`dim_teams`**: `team_id` (PK, Serial), `team_name` (Unique)
- **`dim_players`**: `player_id` (PK), `player_name`
- **`fact_matches`**: `match_id` (PK), `match_date`, `team1_id` (FK), `team2_id` (FK), `venue`, `winner_id` (FK)
- **`fact_player_batting`**: `player_name` (PK), `runs`, `innings`, `dismissals`, `average`, `strike_rate`, `fours`, `sixes`, `balls_faced`
- **`fact_player_bowling`**: `player_name` (PK), `legal_balls`, `runs_conceded`, `wickets`, `deliveries_recorded`, `overs`, `economy`, `bowling_average`, `bowling_strike_rate`

The initial table definitions are available in [`SportsPulse/sql/schema.sql`](SportsPulse/sql/schema.sql).

---

## 🚀 Getting Started

### 1. Prerequisites
- **Python**: Version 3.10 or 3.11
- **PostgreSQL**: Version 14 or higher running locally (or via Docker/remote)
- **Git**

### 2. Clone the Repository
```bash
git clone https://github.com/Chandrabhanu-Prusty/DE-mini-project.git
cd DE-mini-project/SportsPulse
```

### 3. Set Up Python Virtual Environment
On Windows (PowerShell):
```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
```
On macOS / Linux:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 4. Install Dependencies
```powershell
pip install --upgrade pip
pip install -r requirements.txt
```

### 5. Configure Environment Variables
Create your local `.env` from the provided `.env.example`:
```powershell
Copy-Item .env.example .env
```
Edit `.env` to supply your local PostgreSQL database credentials:
```ini
ENVIRONMENT=development
LOG_LEVEL=INFO

DATA_BRONZE_DIR=data/bronze/ipl
DATA_SILVER_DIR=data/silver
DATA_GOLD_DIR=data/gold
DATA_QUARANTINE_DIR=data/quarantine
LOG_DIR=logs
REPORT_DIR=reports

# Update USERNAME and PASSWORD for your PostgreSQL instance:
DATABASE_URL=postgresql+psycopg2://postgres:YOUR_PASSWORD@localhost:5432/sportspulse
```

### 6. Set Up PostgreSQL Database
Open PostgreSQL (`psql` or pgAdmin) and create the database:
```sql
CREATE DATABASE sportspulse;
```
Apply the warehouse DDL schema:
```powershell
psql -U postgres -d sportspulse -f sql/schema.sql
```

### 7. Acquire Source Data
SportsPulse uses ball-by-ball match telemetry from [Cricsheet](https://cricsheet.org/matches/).
1. Download the IPL match JSON archive: [ipl_json.zip](https://cricsheet.org/downloads/ipl_json.zip)
2. Extract all match JSON files into `SportsPulse/data/bronze/ipl/` (e.g. `SportsPulse/data/bronze/ipl/1082591.json`).

---

## ⚙️ Running the Data Pipeline

### Option A: Complete End-to-End Pipeline Runner (Recommended)

Run all Medallion stages (Bronze registration ➔ validation ➔ quality audit ➔ Silver parsing ➔ entity normalization ➔ Gold marts ➔ PostgreSQL warehouse ➔ final execution report) with a single command:

```powershell
# From the SportsPulse directory (or repository root):
python run_pipeline.py
```

#### Execution Options & Flags:
```powershell
# Run Bronze -> Silver -> Gold without requiring a local PostgreSQL database:
python run_pipeline.py --skip-warehouse

# Enforce strict zero-warning policy (exits with non-zero if quality warnings exist):
python run_pipeline.py --strict

# Perform a dry-run to validate all runner stages without processing datasets:
python run_pipeline.py --dry-run
```

Each run produces:
- A timestamped execution audit report: `reports/pipeline_run_<TIMESTAMP>.json`
- A latest run pointer: `reports/latest_pipeline_run.json`
- An append-only historical run ledger: `reports/pipeline_runs_history.csv`

---

### Option B: Modular Stage-by-Stage Execution

You can also run individual modular components independently for development or targeted debugging:

```powershell
# Step 1: Register Bronze raw match files & compute SHA-256 manifest
python -m src.ingestion.register_files

# Step 2: Validate JSON schema structures
python -m src.validation.validate_schema

# Step 3: Run comprehensive data quality and integrity checks
python -m src.validation.quality_check

# Step 4: Parse raw JSONs into relational Silver Parquet tables
python -m src.processing.parse_matches

# Step 5: Normalize team and player entities
python -m src.processing.normalize_entities

# Step 6: Compute Gold analytical marts
python -m src.processing.build_gold

# Step 7: Load dimensions and facts into PostgreSQL warehouse
python -m src.warehouse.load_warehouse
```

### Option C: Containerized Apache Airflow Orchestration (Docker Compose)

Orchestrate the entire pipeline as a directed acyclic graph (DAG) inside an isolated Docker container with automated task scheduling, dependencies, and retries:

```powershell
# 1. Start the Airflow standalone container in the background
docker compose up -d

# 2. Trigger the SportsPulse Medallion pipeline DAG
docker compose exec airflow airflow dags trigger sportspulse_pipeline

# 3. Monitor DAG execution status
docker compose exec airflow airflow dags list-runs -d sportspulse_pipeline

# 4. Access the Airflow UI at http://localhost:8080
```

Inside the Airflow UI, the pipeline is represented as 8 distinct tasks:
`bronze_register_files` ➔ `schema_validate` ➔ `data_quality_checks` ➔ `silver_parse_matches` ➔ `silver_normalize_entities` ➔ `gold_build_analytics` ➔ `warehouse_load` ➔ `pipeline_audit_summary`.

---

### Verification
Upon completion, `load_warehouse.py` prints summary counts:
- `dim_teams`: 19 rows
- `dim_players`: 807 rows
- `fact_matches`: 1,243 rows
- `fact_player_batting`: 738 rows
- `fact_player_bowling`: 577 rows

---

## 📊 Launching the Streamlit Dashboard

Launch the interactive analytics dashboard:
```powershell
streamlit run dashboard/app.py
```
Open your browser at `http://localhost:8501`.

### Dashboard Features
- **Overview & Match KPIs**: Total matches, runs scored, boundaries, and win trends.
- **Batting Analysis**: Top run-scorers, batting strike rates, boundary percentages, and averages.
- **Bowling Analysis**: Top wicket-takers, economy rates, bowling averages, and strike rates.
- **Team Dynamics**: Win percentage comparisons, head-to-head records, and toss impact.
- **Match Explorer**: Interactive match search, team filtering, and ball-by-ball innings trajectories.
- **Pipeline Control Center**: Trigger pipeline runs, view live execution logs, track 7 Medallion stages, inspect Parquet and warehouse metrics, view execution history, and monitor Apache Airflow container health.
- **System Architecture**: High-resolution interactive architecture diagram visualizer, detailed breakdown of all 9 pipeline components, data layer specs, and output endpoints.

---

## 🧪 Automated Testing Suite (21 Unit Tests)

The repository includes a comprehensive unit testing suite verifying pipeline reliability, failure isolation, subprocess execution, credential redaction, and Airflow DAG syntax:

```powershell
# Run the complete test suite:
python -m unittest discover tests -v
```

### Verified Test Suites:
- **`tests/test_runner.py` (8 Tests)**: Stage ordering, dry-run mode, `--skip-warehouse` option, fail-fast cascading, credential masking, metrics extraction, report filename derivation, and Airflow DAG syntax.
- **`tests/test_dashboard_control.py` (9 Tests)**: File lock acquisition/concurrency prevention, report parser, corrupt JSON resilience, historical log parser, live log streaming, dry-run execution, Airflow healthcheck, and secret redaction.
- **`tests/test_architecture.py` (4 Tests)**: 9 architectural component specifications, CSV export artifact specification, and image asset path resolver (`FOUND` vs `MISSING`).

**Result**: `Ran 21 tests in ~3.0s — OK (All tests passing)`.

---

## 📁 Repository Directory Structure

```text
SportsPulse/
├── assets/                  # Architecture diagrams & media assets
│   ├── sportspulse_architecture.png
│   └── sportspulse_architecture.jpg
├── dashboard/               # Streamlit web application
│   ├── app.py               # Interactive multi-page analytics dashboard
│   ├── architecture.py      # System Architecture visualizer & component breakdown
│   └── pipeline_control.py  # Pipeline Control Center & operations engine
├── data/                    # Medallion data directories
│   ├── bronze/ipl/          # Raw Cricsheet IPL match JSON files (1,243 files)
│   ├── silver/              # Normalized dimensional & fact Parquet tables
│   ├── gold/                # Curated analytical aggregates & marts
│   ├── csv/                 # Converted CSV export dataset (matches, innings, deliveries)
│   └── quarantine/          # Isolated malformed or corrupt records
├── reports/                 # Pipeline audit and validation reports
│   ├── file_manifest.csv
│   ├── schema_validation_report.csv
│   ├── data_quality_report.csv
│   ├── latest_pipeline_run.json
│   └── pipeline_runs_history.csv
├── sql/                     # PostgreSQL warehouse DDLs
│   └── schema.sql           # Table definitions, primary keys & foreign keys
├── src/                     # Pipeline source code
│   ├── ingestion/           # File registration & checksum calculation
│   │   └── register_files.py
│   ├── validation/          # Schema enforcement & quality checks
│   │   ├── validate_schema.py
│   │   └── quality_check.py
│   ├── processing/          # Silver parsing, normalization & Gold marts
│   │   ├── parse_matches.py
│   │   ├── normalize_entities.py
│   │   └── build_gold.py
│   └── warehouse/           # PostgreSQL warehouse loader (idempotent upserts)
│       └── load_warehouse.py
├── dags/                    # Production Apache Airflow DAGs
│   └── sportspulse_pipeline.py
├── docker-compose.yaml      # Dockerized Apache Airflow orchestration service
├── run_pipeline.py          # Unified CLI pipeline runner with audit reporting
├── DEMO_GUIDE.md            # Comprehensive viva & demonstration walkthrough
├── models/                  # Statistical and ML modeling placeholders
├── notebooks/               # Exploratory Jupyter analysis
├── tests/                   # Automated pipeline test suites (21 unit tests)
│   ├── test_runner.py
│   ├── test_dashboard_control.py
│   └── test_architecture.py
├── .env.example             # Template environment variables (safe placeholders)
├── .dockerignore            # Clean Docker build context filters
├── .gitignore               # Strict ignore rules for data, secrets, caches
├── requirements.txt         # Pinned Python package dependencies
└── README.md                # Project documentation
```

---

## 🔮 Architectural Assessment & Future Roadmap

- **PySpark Distributed Computing Feasibility**: For the 295,732 delivery records in the IPL dataset, single-node Pandas/PyArrow vectorization completes in under 10 seconds. In enterprise production spanning all global cricket formats (50M+ balls), the pipeline architecture is designed to execute PySpark transformations via Airflow `DataprocSubmitPySparkJobOperator` or `SparkSubmitOperator` on managed clusters (Google Cloud Dataproc / AWS EMR) rather than local JVM emulation.
- **Machine Learning Integration**: Building predictive match models (in-game win probability calculator and bowler-batter matchup clustering).
- **Automated Ingestion**: Implementing an automatic streaming downloader and unpacker for live seasonal updates from Cricsheet.

---

## 👨‍💻 Author & Academic Attribution

- **Chandrabhanu Prusty**  
- **Institution**: Vivekanand Education Society's Institute of Technology (VESIT)  
- **Course**: B.E. Computer Engineering (Data Engineering Mini Project, Semester 5)  
- **Repository**: [https://github.com/Chandrabhanu-Prusty/DE-mini-project](https://github.com/Chandrabhanu-Prusty/DE-mini-project)
