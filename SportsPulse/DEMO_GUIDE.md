# ⚡ SportsPulse — Evaluation & Demonstration Guide

> **VESIT — Semester 5 Data Engineering Mini Project**  
> **Student**: Chandrabhanu Prusty  
> **Project**: SportsPulse — End-to-End IPL Sports Analytics Data Engineering Pipeline  
> **Repository**: [https://github.com/Chandrabhanu-Prusty/DE-mini-project](https://github.com/Chandrabhanu-Prusty/DE-mini-project)

---

## 🎯 Executive Summary for Evaluators

SportsPulse is an enterprise-grade, production-structured Data Engineering pipeline built on the **Medallion Lakehouse Pattern** (Bronze ➔ Silver ➔ Gold ➔ Warehouse ➔ Dashboard). It processes **1,243 historical IPL matches** spanning 2008 to 2024, converting **295,732 ball-by-ball raw telemetry records** into cleaned dimensional entities, curated analytical marts, a PostgreSQL data warehouse, and a high-performance Streamlit analytics dashboard.

---

## 📊 Verified Dataset & Warehouse Metrics

When the pipeline runs or when inspecting the warehouse, verify these exact benchmark numbers:

| Layer | Dataset / Table | Format / Target | Row Count | Description |
| :--- | :--- | :--- | :---: | :--- |
| **Bronze** | Raw Match JSONs | JSON files | **1,243** | Cricsheet ball-by-ball tournament files |
| **Bronze** | `file_manifest.csv` | CSV Audit | **1,243** | Immutable audit log with SHA-256 checksums |
| **Validation** | `schema_validation_report.csv` | CSV Audit | **1,243** | 100% schema compliance, 0 malformed files |
| **Quality** | `data_quality_report.csv` | CSV Audit | **0 issues** | Zero critical errors or broken constraints |
| **Silver** | `matches.parquet` | Columnar Parquet | **1,243** | De-nested match-level metadata |
| **Silver** | `innings.parquet` | Columnar Parquet | **2,514** | De-nested team innings records |
| **Silver** | `deliveries.parquet` | Columnar Parquet | **295,732** | Granular ball-by-ball delivery telemetry |
| **Silver** | `dim_teams.parquet` | Columnar Parquet | **19** | Standardized franchises (e.g. DC, PBKS normalized) |
| **Silver** | `dim_players.parquet` | Columnar Parquet | **807** | Unique standardized player registry |
| **Gold** | `player_batting.parquet` | Analytical Mart | **738** | Aggregate batting: runs, average, strike rate, boundaries |
| **Gold** | `player_bowling.parquet` | Analytical Mart | **577** | Aggregate bowling: wickets, overs, economy, strike rate |
| **Gold** | `team_performance.parquet` | Analytical Mart | **19** | Matches played, wins, losses, win percentage |
| **Gold** | `match_summary.parquet` | Analytical Mart | **1,243** | Curated match outcomes with normalized team names |
| **Gold** | `innings_summary.parquet` | Analytical Mart | **2,480** | Innings totals, extras, run-rates |
| **Warehouse**| `dim_teams` | PostgreSQL Table | **19** | Primary key `team_id`, unique constraint on name |
| **Warehouse**| `dim_players` | PostgreSQL Table | **807** | Primary key `player_id`, indexed player names |
| **Warehouse**| `fact_matches` | PostgreSQL Table | **1,243** | Foreign keys to `dim_teams` for team1, team2, winner |
| **Warehouse**| `fact_player_batting` | PostgreSQL Table | **738** | Dimensional batting performance table |
| **Warehouse**| `fact_player_bowling` | PostgreSQL Table | **577** | Dimensional bowling performance table |

---

## ⚡ 1. Running the Automated Pipeline

### Option A: Complete End-to-End Pipeline Runner (Recommended)

Run all 7 Medallion stages in a single command from PowerShell or Bash:

```powershell
# From the SportsPulse directory:
python run_pipeline.py
```

#### What the runner does:
1. Sequentially executes: Bronze Ingestion ➔ Schema Validation ➔ Data Quality Audit ➔ Silver Parsing ➔ Silver Normalization ➔ Gold Marts ➔ PostgreSQL Warehouse Upsert.
2. Implements a fail-fast architecture: if any stage fails, execution halts immediately and skips downstream stages.
3. Produces three audit artifacts in `reports/`:
   - Timestamped run report: `reports/pipeline_run_<TIMESTAMP>.json`
   - Latest run symlink/pointer: `reports/latest_pipeline_run.json`
   - Append-only execution history: `reports/pipeline_runs_history.csv`

#### Runner Flags for Demonstration:
```powershell
# Dry run: Validates the orchestrator without recomputing 295k deliveries (instant verification)
python run_pipeline.py --dry-run

# Run transformations without requiring local PostgreSQL:
python run_pipeline.py --skip-warehouse

# Strict mode: Enforces zero-tolerance on data warnings
python run_pipeline.py --strict
```

---

## 🧪 2. Running Automated Unit Tests

SportsPulse includes an automated test suite verifying stage definitions, credential sanitization (preventing secret leakage), metrics extraction, dry-run safety, skip-warehouse execution, and Airflow DAG syntax:

```powershell
# Run the test suite:
python -m unittest tests/test_runner.py -v
```

Expected output:
```text
test_airflow_dag_structure_and_syntax ... ok
test_credential_sanitization ... ok
test_dry_run_mode ... ok
test_fail_fast_on_stage_error ... ok
test_metrics_extraction_from_reports ... ok
test_skip_warehouse_flag ... ok
test_stage_definitions_order_and_modules ... ok
test_timestamped_filename_matches_run_id ... ok

----------------------------------------------------------------------
Ran 8 tests in ~1.0s

OK
```

---

## 📊 3. Launching & Exploring the Streamlit Dashboard

Launch the interactive UI backed by the PostgreSQL analytical warehouse and Gold Parquet layers:

```powershell
streamlit run dashboard/app.py
```
Open **`http://localhost:8501`** in your browser.

### Suggested Demonstration Tour (Tab-by-Tab)

1. **Overview Tab (Tournament Telemetry)**:
   - Highlight high-level tournament KPI tiles: **1,243 Matches**, **295,732 Balls Bowled**, total runs, and boundaries.
   - Inspect the interactive tournament trajectory chart tracking runs scored by season.
   - Showcase toss impact analysis: match win percentage when winning the toss vs losing the toss.

2. **Batting Tab (Player Analytics)**:
   - Select the minimum runs filter (e.g. 1,000+ runs).
   - Point out top tournament run-scorers: **Virat Kohli** (8,000+ runs), **Shikhar Dhawan**, **Rohit Sharma**, **David Warner**.
   - Review the Strike Rate vs Average scatter plot with quartile reference lines.

3. **Bowling Tab (Wicket-Takers & Economy)**:
   - Filter by minimum overs (e.g. 100+ overs).
   - Point out top wicket-takers: **Yuzvendra Chahal**, **Dwayne Bravo**, **Piyush Chawla**, **Bhuvneshwar Kumar**.
   - Highlight the Economy Rate vs Bowling Average chart identifying high-impact bowlers.

4. **Team Dynamics Tab (Franchise Head-to-Head)**:
   - Compare franchise win percentages: **Chennai Super Kings** and **Mumbai Indians** leading historical win rates.
   - Select two rival teams (e.g. `Mumbai Indians` vs `Chennai Super Kings`) to view head-to-head win distributions.

5. **Match Explorer Tab (Ball-by-Ball Worm Charts)**:
   - Select any historical IPL match from the dropdown.
   - Observe the cumulative worm chart comparing Innings 1 vs Innings 2 progression over 20 overs.
   - Point out fall-of-wicket markers directly on the trajectory curves.

6. **Pipeline Control Page (Operations & Execution Center)**:
   - Live pipeline status: Idle, Running, Success, or Failed with duration and timestamp.
   - Trigger the real pipeline directly from the UI with optional flags (`--skip-warehouse`, `--strict`, `--dry-run`).
   - Monitor real-time captured terminal output in the expandable log console.
   - Track stage-wise Medallion progress across all 7 layers with durations and error diagnostics.
   - Inspect Medallion Parquet row counts, PostgreSQL table metrics, and schema compliance.
   - Review historical run ledger from `reports/pipeline_runs_history.csv` with JSON report inspection.
   - Check live Dockerized Apache Airflow service connectivity (`http://localhost:8080`).

---

## 🐳 4. Apache Airflow Orchestration (Docker Compose)

Airflow orchestration is configured in `docker-compose.yaml` and `dags/sportspulse_pipeline.py`.

### Architecture & Network Configuration:
- Container runs Apache Airflow in standalone mode on port **8080**.
- The project directory is mounted into `/opt/airflow/sportspulse`.
- On Docker for Windows, the container communicates with the host machine's PostgreSQL instance via `host.docker.internal:5432` using Docker's `extra_hosts` bridge.
- Pipeline dependencies (`pandas`, `pyarrow`, `SQLAlchemy`, `psycopg2-binary`) are automatically installed via `_PIP_ADDITIONAL_REQUIREMENTS`.

### Commands to Run:

```powershell
# 1. Validate Docker Compose configuration
docker compose config

# 2. Start the Airflow container in the background
docker compose up -d

# 3. View Airflow container logs
docker compose logs -f airflow

# 4. Access the Airflow Web UI
# Open http://localhost:8080 in your browser
# (Standalone credentials are generated in the container logs or default admin)

# 5. Trigger the SportsPulse DAG via CLI
docker compose exec airflow airflow dags trigger sportspulse_pipeline

# 6. Monitor DAG execution status
docker compose exec airflow airflow dags list-runs -d sportspulse_pipeline

# 7. Stop the Airflow container when finished
docker compose down
```

### DAG Task Dependency Graph in Airflow:
```text
bronze_register_files
       │
       ▼
 schema_validate
       │
       ▼
data_quality_checks
       │
       ▼
silver_parse_matches
       │
       ▼
silver_normalize_entities
       │
       ▼
gold_build_analytics
       │
       ▼
  warehouse_load
       │
       ▼
pipeline_audit_summary
```

---

## 🔬 5. PySpark Distributed Computing Feasibility Assessment

As part of the senior data engineering evaluation, a feasibility study was conducted on adding a PySpark transformation module to process the 295,732 ball delivery records:

1. **Current Pipeline Profile**:
   - The current Pandas/PyArrow vectorization processes all 1,243 JSON matches and 295,732 deliveries into columnar Parquet in **under 10 seconds** on a single workstation node.
   - Parquet compression reduces the raw 150MB JSON payload down to 10MB columnar files with Snappy compression, enabling sub-second analytical querying.

2. **Distributed Processing Tradeoffs on Local Workstations**:
   - PySpark requires downloading a 450MB distribution package and JVM runtime.
   - On native Windows environments, local Spark execution requires Hadoop native binaries (`winutils.exe` and `hadoop.dll`) configured under `%HADOOP_HOME%\bin` to handle file locking and POSIX permissions.
   - Running Spark locally for 295,000 records introduces JVM serialization overhead that exceeds the execution time of Pandas/PyArrow without distributed multi-node benefits.

3. **Enterprise Production Recommendation (Next Phase)**:
   - For scaling beyond IPL to all international cricket ball-by-ball datasets (ODIs, T20Is, Test matches totaling 50M+ balls), PySpark is ideally executed as an Apache Airflow `DataprocSubmitPySparkJobOperator` or `SparkSubmitOperator` running on a managed Google Cloud Dataproc or AWS EMR cluster rather than single-node Windows emulation.

---

## 📸 6. Submission Evidence & Screenshots to Capture

For your submission documentation and project defense, capture these 6 key screenshots:

1. **Pipeline Execution Success Banner**: Run `python run_pipeline.py` and capture the final ASCII execution table showing all 7 stages marked `SUCCESS`.
2. **Unit Test Results**: Run `python -m unittest tests/test_runner.py -v` showing all 8 tests passing with `OK`.
3. **Streamlit Overview Page**: Capture the tournament telemetry tiles and season runs chart.
4. **Streamlit Batting / Bowling Scatter Plots**: Capture the Strike Rate vs Average or Wickets vs Economy visual.
5. **Streamlit Match Explorer Worm Chart**: Select a close match (e.g. an IPL final) and capture the dual-innings worm chart.
6. **PostgreSQL pgAdmin or psql Table Counts**: Query `SELECT count(*) FROM fact_matches;` and `SELECT count(*) FROM fact_player_batting;` to show live warehouse records.

---

## ✅ 7. Final Verification Checklist

- [x] Bronze layer discovered and registered 1,243 raw JSON match files.
- [x] Schema validation completed with 0 errors across all 1,243 matches.
- [x] Data quality checks verified with 0 integrity issues.
- [x] Silver Parquet datasets generated: `matches.parquet`, `innings.parquet`, `deliveries.parquet`.
- [x] Dimensional entities normalized: `dim_teams` (19) and `dim_players` (807).
- [x] Gold analytical marts generated: `player_batting`, `player_bowling`, `team_performance`, `match_summary`.
- [x] PostgreSQL warehouse schema defined in `sql/schema.sql` and loaded with 5 populated tables.
- [x] Interactive Streamlit dashboard fully operational with 5 analytics pages.
- [x] Apache Airflow DAG implemented with 8 sequential Medallion tasks in `dags/sportspulse_pipeline.py`.
- [x] Docker Compose configured with container networking for host PostgreSQL via `host.docker.internal`.
- [x] All 8 unit tests passing with 100% green status.
- [x] Zero credential leaks in logs, reports, or repository tracking.
