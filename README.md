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
| **07. Analytics Dashboard** | Interactive match explorer, player stats, team analysis | `dashboard/app.py` | ✅ Complete |
| **08. Orchestration & Scale** | Scheduled pipeline runs & distributed processing | `dags/sportspulse_pipeline.py` | ⏳ Planned Roadmap |

---

## 🏗️ Data Pipeline Architecture (Medallion Pattern)

The project implements a modern **Medallion Lakehouse Architecture** to guarantee data quality, idempotence, and high analytical query performance:

```
[ Raw Sources ]
 1,240+ IPL Match JSONs from Cricsheet (Ball-by-Ball)
       │
       ▼
┌────────────────────────────────────────────────────────┐
│  🥉 BRONZE LAYER (Raw Ingestion)                      │
│  - Raw file registration with metadata & SHA-256 sums │
│  - Immutable append-only ingestion                    │
│  - Path: data/bronze/ipl/*.json                       │
│  - Report: reports/file_manifest.csv                  │
└────────────────────────────────────────────────────────┘
       │
       ├───► [ Schema Validation & Integrity Checks ]
       │            │
       │            ├───► Failed ──► 🚨 QUARANTINE (`data/quarantine/`)
       ▼            ▼
┌────────────────────────────────────────────────────────┐
│  🥈 SILVER LAYER (Cleaned & Normalized Parquet)        │
│  - De-nests matches, innings, and deliveries           │
│  - Standardizes player, team, and venue naming         │
│  - Builds relational entities & dimensions            │
│  - Tables: matches, innings, deliveries,               │
│            dim_teams, dim_players                      │
│  - Path: data/silver/*.parquet                         │
└────────────────────────────────────────────────────────┘
       │
       ▼
┌────────────────────────────────────────────────────────┐
│  🥇 GOLD LAYER (Curated Business Marts)               │
│  - Batting performance (Runs, Avg, Strike Rate, 4s/6s) │
│  - Bowling metrics (Wickets, Overs, Economy, Avg, SR)  │
│  - Match & Innings summaries, Team win percentages     │
│  - Tables: player_batting, player_bowling,             │
│            team_performance, match_summary,            │
│            innings_summary                             │
│  - Path: data/gold/*.parquet                           │
└────────────────────────────────────────────────────────┘
       │
       ├───► 🗄️ Analytical Warehouse (PostgreSQL)
       │      - dim_teams, dim_players
       │      - fact_matches, fact_player_batting, fact_player_bowling
       │
       └───► 📊 Streamlit Interactive Analytics Dashboard
              - Reads facts & dimensions directly from PostgreSQL
              - Reads granular innings data from Gold Parquet
```

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

Execute the pipeline stages sequentially from the `SportsPulse` directory:

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
- **Innings Telemetry**: Match-level innings trajectories and worm charts.

---

## 📁 Repository Directory Structure

```text
SportsPulse/
├── dashboard/               # Streamlit web application
│   └── app.py               # Interactive multi-tab analytics dashboard
├── data/                    # Medallion data directories (gitignored datasets)
│   ├── bronze/ipl/          # Raw Cricsheet IPL match JSON files
│   ├── silver/              # Normalized dimensional & fact Parquet tables
│   ├── gold/                # Curated analytical aggregates & marts
│   └── quarantine/          # Isolated malformed or corrupt records
├── reports/                 # Pipeline audit and validation reports
│   ├── file_manifest.csv
│   ├── schema_validation_report.csv
│   └── data_quality_report.csv
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
├── dags/                    # Orchestration DAG placeholders
│   └── sportspulse_pipeline.py
├── models/                  # Statistical and ML modeling placeholders
├── notebooks/               # Exploratory Jupyter analysis
├── tests/                   # Automated pipeline test suites
├── .env.example             # Template environment variables (safe placeholders)
├── .gitignore               # Strict ignore rules for data, secrets, caches
├── requirements.txt         # Pinned Python package dependencies
└── README.md                # Project documentation
```

---

## 🔮 Limitations & Future Roadmap

- **Airflow Orchestration**: Integrating scheduled workflow DAGs in `dags/sportspulse_pipeline.py` with automated alert webhooks.
- **PySpark Migration**: Extending the parsing and aggregation stages to distributed Spark for handling all international formats (ODIs, T20Is, Tests).
- **Machine Learning**: Building predictive match models (in-game win probability calculator and bowler-batter matchup clustering).
- **Automated Ingestion**: Implementing an automatic downloader and unpacker for live seasonal updates from Cricsheet.

---

## 👨‍💻 Author & Academic Attribution

- **Chandrabhanu Prusty**  
- **Institution**: Vivekanand Education Society's Institute of Technology (VESIT)  
- **Course**: B.E. Computer Engineering (Data Engineering Mini Project, Semester 5)  
- **Repository**: [https://github.com/Chandrabhanu-Prusty/DE-mini-project](https://github.com/Chandrabhanu-Prusty/DE-mini-project)
