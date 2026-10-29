"""
SportsPulse: End-to-End IPL Sports Analytics Data Engineering Pipeline DAG
==========================================================================
Orchestrates the SportsPulse Medallion Architecture across Bronze, Silver,
and Gold layers, followed by PostgreSQL warehouse loading.

Architecture Workflow:
----------------------
[Bronze Registration] -> [Schema Validation] -> [Data Quality Checks]
        |
        v
[Silver: Parse Matches] -> [Silver: Normalize Entities]
        |
        v
[Gold: Build Analytics Marts]
        |
        v
[PostgreSQL Warehouse Load] -> [Audit Summary]

Execution Environment:
----------------------
Configured to execute within an Apache Airflow container or runner where
the project root is mounted at `/opt/airflow/sportspulse` (or configured via
SPORTSPULSE_HOME environment variable).
"""

from datetime import datetime, timedelta
import os
from pathlib import Path

# Airflow imports with graceful fallbacks for local compilation / linting
try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.operators.empty import EmptyOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    DAG = None
    BashOperator = None
    EmptyOperator = None


# Project root within container or execution host
SPORTSPULSE_HOME = os.getenv("SPORTSPULSE_HOME", "/opt/airflow/sportspulse")

DEFAULT_ARGS = {
    "owner": "sportspulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=20),
}

DAG_DOC_MD = """
# SportsPulse Data Engineering Pipeline

Automated medallion ETL workflow for 1,243 IPL match datasets (2008-2024).

### Pipeline Stages:
1. **Bronze Ingestion**: Scan raw Cricsheet JSON files and generate SHA-256 integrity manifest.
2. **Schema Validation**: Verify structural integrity and schema conformity across all match files.
3. **Data Quality Audit**: Check logical validity (dates, team names, match ties, deliverable balls).
4. **Silver Parsing**: Flatten nested JSON structures into columnar Parquet (matches, innings, deliveries).
5. **Silver Normalization**: Disambiguate player aliases and map franchise name variations into dimension tables.
6. **Gold Marts**: Compute analytical aggregates (batting strike rates, bowling economies, team records).
7. **Warehouse Loading**: Idempotent upsert into PostgreSQL dimensional warehouse star schema.
8. **Audit Verification**: Verify artifact creation and summary report availability.
"""


def create_sportspulse_dag():
    """Instantiate and configure the SportsPulse DAG."""
    if not AIRFLOW_AVAILABLE:
        return None

    with DAG(
        dag_id="sportspulse_pipeline",
        description="SportsPulse Medallion Pipeline: Bronze -> Silver -> Gold -> PostgreSQL",
        doc_md=DAG_DOC_MD,
        default_args=DEFAULT_ARGS,
        start_date=datetime(2026, 1, 1),
        schedule=None,  # Manual trigger for evaluation and on-demand runs
        catchup=False,
        max_active_runs=1,
        tags=["data-engineering", "ipl", "medallion", "warehouse", "analytics"],
    ) as dag:

        # -------------------------------------------------------------
        # Stage 1: Bronze Ingestion & File Registration
        # -------------------------------------------------------------
        t1_bronze = BashOperator(
            task_id="bronze_register_files",
            bash_command=(
                f"cd {SPORTSPULSE_HOME} && "
                "python -m src.ingestion.register_files"
            ),
            doc_md="Discovers raw JSON files, calculates SHA-256 checksums, and updates file_manifest.csv.",
        )

        # -------------------------------------------------------------
        # Stage 2: Schema Validation
        # -------------------------------------------------------------
        t2_validate = BashOperator(
            task_id="schema_validate",
            bash_command=(
                f"cd {SPORTSPULSE_HOME} && "
                "python -m src.validation.validate_schema"
            ),
            doc_md="Validates JSON structure and required keys against expected IPL match specs.",
        )

        # -------------------------------------------------------------
        # Stage 3: Data Quality & Integrity Checks
        # -------------------------------------------------------------
        t3_quality = BashOperator(
            task_id="data_quality_checks",
            bash_command=(
                f"cd {SPORTSPULSE_HOME} && "
                "python -m src.validation.quality_check"
            ),
            doc_md="Runs logical integrity audits: innings count, legal deliveries, valid outcomes.",
        )

        # -------------------------------------------------------------
        # Stage 4: Silver Parsing (Matches, Innings, Deliveries)
        # -------------------------------------------------------------
        t4_silver_parse = BashOperator(
            task_id="silver_parse_matches",
            bash_command=(
                f"cd {SPORTSPULSE_HOME} && "
                "python -m src.processing.parse_matches"
            ),
            doc_md="De-nests match JSONs into relational Parquet datasets in data/silver.",
        )

        # -------------------------------------------------------------
        # Stage 5: Silver Normalization (Teams & Players)
        # -------------------------------------------------------------
        t5_silver_normalize = BashOperator(
            task_id="silver_normalize_entities",
            bash_command=(
                f"cd {SPORTSPULSE_HOME} && "
                "python -m src.processing.normalize_entities"
            ),
            doc_md="Disambiguates player IDs and normalizes historical franchise rebrands.",
        )

        # -------------------------------------------------------------
        # Stage 6: Gold Analytics Marts
        # -------------------------------------------------------------
        t6_gold_build = BashOperator(
            task_id="gold_build_analytics",
            bash_command=(
                f"cd {SPORTSPULSE_HOME} && "
                "python -m src.processing.build_gold"
            ),
            doc_md="Calculates analytical performance aggregates for batting, bowling, and teams.",
        )

        # -------------------------------------------------------------
        # Stage 7: PostgreSQL Warehouse Loader
        # -------------------------------------------------------------
        t7_warehouse = BashOperator(
            task_id="warehouse_load",
            bash_command=(
                f"cd {SPORTSPULSE_HOME} && "
                "python -m src.warehouse.load_warehouse"
            ),
            doc_md="Executes upserts into PostgreSQL dimensional star schema (dim_teams, dim_players, fact_matches, facts).",
        )

        # -------------------------------------------------------------
        # Stage 8: Pipeline Audit Verification
        # -------------------------------------------------------------
        t8_audit_summary = BashOperator(
            task_id="pipeline_audit_summary",
            bash_command=(
                f"cd {SPORTSPULSE_HOME} && "
                "python -c \""
                "from pathlib import Path; "
                "r = Path('reports/latest_pipeline_run.json'); "
                "print('[AIRFLOW AUDIT] Pipeline execution complete.'); "
                "print(f'Latest report exists: {r.exists()}'); "
                "\""
            ),
            doc_md="Verifies final artifact creation and prints pipeline status summary.",
        )

        # -------------------------------------------------------------
        # Orchestration Workflow Dependencies (Linear Medallion Pipeline)
        # -------------------------------------------------------------
        (
            t1_bronze
            >> t2_validate
            >> t3_quality
            >> t4_silver_parse
            >> t5_silver_normalize
            >> t6_gold_build
            >> t7_warehouse
            >> t8_audit_summary
        )

    return dag


# Expose DAG object at global module level for Airflow discovery
if AIRFLOW_AVAILABLE:
    dag = create_sportspulse_dag()
