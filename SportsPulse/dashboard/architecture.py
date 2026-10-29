"""
SportsPulse: System Architecture Module
=======================================
Renders the System Architecture page inside the Streamlit dashboard, displaying
the end-to-end Medallion Lakehouse diagram and comprehensive component explanations.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st


# =============================================================================
# ARCHITECTURE COMPONENT SPECIFICATIONS
# =============================================================================

ARCHITECTURE_COMPONENTS: List[Dict[str, Any]] = [
    {
        "id": "source_dataset",
        "name": "1. Source Cricsheet IPL JSON Dataset",
        "icon": "🏏",
        "category": "Source Ingestion",
        "badge": "Input Telemetry",
        "description": (
            "1,243 historical match JSON files spanning IPL seasons 2008 to 2024. "
            "Provides granular ball-by-ball telemetry, match metadata, toss decisions, "
            "player rosters, dismissals, extras, and boundary tracking."
        ),
        "details": [
            "Source Provider: Cricsheet (open sports data repository).",
            "Data Format: Hierarchical JSON with nested innings and deliveries arrays.",
            "Scale: 1,243 match files containing 2,514 innings and 295,732 ball deliveries.",
        ],
    },
    {
        "id": "bronze_layer",
        "name": "2. Bronze File Registration & SHA-256 Manifest",
        "icon": "🥉",
        "category": "Bronze Layer",
        "badge": "Raw Ingestion",
        "description": (
            "Immutable append-only ingestion layer. Scans raw JSON match archives, "
            "calculates SHA-256 cryptographic checksums for data integrity, records file sizes, "
            "and generates an audit manifest."
        ),
        "details": [
            "Script: `src/ingestion/register_files.py`",
            "Output Manifest: `reports/file_manifest.csv`",
            "Guarantees: Idempotent file discovery, tamper detection, and duplicate prevention.",
        ],
    },
    {
        "id": "validation_quality",
        "name": "3. Schema Validation & Data-Quality Checks",
        "icon": "🛡️",
        "category": "Validation & Audit",
        "badge": "Quality Gate",
        "description": (
            "Dual-stage quality gate enforcing structural schema specifications and business-logic "
            "integrity rules. Identifies malformed payloads and isolates invalid records into quarantine."
        ),
        "details": [
            "Schema Enforcement: `src/validation/validate_schema.py` validates required keys, types, and schema versions.",
            "Logical Integrity: `src/validation/quality_check.py` asserts legal overs, valid outcomes, and consistent player rosters.",
            "Reports: `reports/schema_validation_report.csv` (1,243 valid) and `reports/data_quality_report.csv` (0 errors).",
            "Quarantine: Malformed or unparseable files are isolated in `data/quarantine/`.",
        ],
    },
    {
        "id": "silver_layer",
        "name": "4. Silver Parsing & Entity Normalization",
        "icon": "🥈",
        "category": "Silver Layer",
        "badge": "Cleaned Lakehouse",
        "description": (
            "De-nests and relationalizes hierarchical JSON match files into high-performance columnar "
            "Parquet tables, followed by entity disambiguation for players and standardized franchise names."
        ),
        "details": [
            "Match Parsing (`parse_matches.py`): Produces `matches.parquet` (1,243 rows), `innings.parquet` (2,514 rows), and `deliveries.parquet` (295,732 rows).",
            "Entity Normalization (`normalize_entities.py`): Builds `dim_teams.parquet` (19 normalized franchises) and `dim_players.parquet` (807 registered athletes).",
            "Entity Disambiguation: Maps historical franchise rebrands (e.g., Delhi Daredevils ➔ Delhi Capitals, Kings XI Punjab ➔ Punjab Kings) and normalizes player name variants.",
        ],
    },
    {
        "id": "gold_layer",
        "name": "5. Gold Analytical Datasets (Marts)",
        "icon": "🥇",
        "category": "Gold Layer",
        "badge": "Curated Marts",
        "description": (
            "Curated business-level analytical marts computed from Silver delivery telemetry. "
            "Pre-aggregates player efficiency, batting averages, bowling economy rates, and team head-to-head records."
        ),
        "details": [
            "Script: `src/processing/build_gold.py`",
            "Player Batting (`player_batting.parquet`): 738 players with runs, innings, strike rate, average, 4s, 6s, and balls faced.",
            "Player Bowling (`player_bowling.parquet`): 577 bowlers with wickets, overs, economy, bowling average, and bowling strike rate.",
            "Team Performance (`team_performance.parquet`): 19 teams with matches played, matches won, and win percentages.",
            "Match & Innings Summaries: `match_summary.parquet` (1,243 rows) and `innings_summary.parquet` (2,480 rows).",
        ],
    },
    {
        "id": "postgresql_warehouse",
        "name": "6. PostgreSQL Analytical Warehouse",
        "icon": "🗄️",
        "category": "Warehouse Layer",
        "badge": "Relational Star Schema",
        "description": (
            "Relational dimensional warehouse modeled in a star schema. Supports idempotent upserts, "
            "referential integrity with primary/foreign keys, and sub-second SQL queries for serving analytics."
        ),
        "details": [
            "DDL Schema: `sql/schema.sql`",
            "Warehouse Loader: `src/warehouse/load_warehouse.py` (idempotent dimensional upserts and fact table reloads).",
            "Dimension Tables: `dim_teams` (19 rows) and `dim_players` (807 rows).",
            "Fact Tables: `fact_matches` (1,243 rows), `fact_player_batting` (738 rows), and `fact_player_bowling` (577 rows).",
        ],
    },
    {
        "id": "csv_export",
        "name": "7. CSV Dataset Export (Artifact)",
        "icon": "📄",
        "category": "Export & Portability",
        "badge": "Export Artifact",
        "description": (
            "Additional dataset export artifact providing flattened, portable CSV representations "
            "of the IPL dataset for ad-hoc exploration, spreadsheet inspection, and external machine learning pipelines."
        ),
        "details": [
            "Role: Supplementary dataset artifact (not a mandatory internal pipeline processing stage).",
            "Purpose: Enables cross-platform interoperability, external notebook analysis, and standalone dataset distribution.",
            "Relationship: Extracted from verified Silver/Gold Lakehouse layers without modifying core Parquet pipelines.",
        ],
    },
    {
        "id": "streamlit_dashboard",
        "name": "8. Streamlit Analytics Dashboard & Pipeline Control",
        "icon": "📊",
        "category": "Serving & UI",
        "badge": "Interactive Application",
        "description": (
            "Modern, multi-tab interactive web interface backed by PostgreSQL and Parquet layers. "
            "Combines rich player/team analytics with an operations Pipeline Control Center."
        ),
        "details": [
            "Analytics Exploration: Overview KPIs, Batting Analytics, Bowling Analytics, Team Dynamics, and Match Explorer worm charts.",
            "Pipeline Control Center (`dashboard/pipeline_control.py`): Trigger one-click runs, view real-time logs, track stage progress, and inspect audit reports.",
            "System Architecture: Interactive documentation and architectural diagrams for evaluation and viva defense.",
        ],
    },
    {
        "id": "airflow_orchestration",
        "name": "9. Apache Airflow Orchestration & Docker Environment",
        "icon": "🐳",
        "category": "Orchestration & DevOps",
        "badge": "Containerized Workflow",
        "description": (
            "Enterprise workflow orchestrator executing inside a Docker container. Encapsulates "
            "the 7 Medallion stages as directed acyclic graph (DAG) tasks with automated retries and logging."
        ),
        "details": [
            "DAG Specification: `dags/sportspulse_pipeline.py` (8 modular tasks chained via bitshift dependencies).",
            "Docker Compose: `docker-compose.yaml` (Airflow 2.8.1 standalone on port 8080).",
            "Container Networking: Uses `host.docker.internal:host-gateway` to connect containerized tasks to the Windows host PostgreSQL database.",
        ],
    },
]


# =============================================================================
# IMAGE PATH RESOLUTION & GRACEFUL HANDLING
# =============================================================================

def resolve_architecture_image_path(project_root: Path) -> Tuple[Optional[Path], str]:
    """
    Resolve the architecture diagram image path relative to project root.
    Checks primary and fallback locations safely.
    """
    candidate_paths = [
        project_root / "assets" / "sportspulse_architecture.png",
        project_root / "assets" / "architecture.png",
        project_root / "assets" / "system_architecture.png",
        project_root / "assets" / "architecture_diagram.png",
    ]

    for path in candidate_paths:
        if path.exists() and path.is_file():
            return path, "FOUND"

    return candidate_paths[0], "MISSING"


# =============================================================================
# STREAMLIT UI RENDERER
# =============================================================================

def render_system_architecture_page(project_root: Path):
    """Render the System Architecture page inside Streamlit."""

    st.header("🏗️ System Architecture")
    st.caption(
        "SportsPulse End-to-End Data Engineering Pipeline: "
        "Medallion Lakehouse Architecture, Dimensional Warehouse & Orchestration"
    )

    # -------------------------------------------------------------------------
    # 1. ARCHITECTURE DIAGRAM SECTION
    # -------------------------------------------------------------------------
    st.subheader("📐 High-Level Architecture Diagram")

    image_path, status = resolve_architecture_image_path(project_root)

    if status == "FOUND" and image_path and image_path.exists():
        try:
            st.image(
                str(image_path),
                caption="Figure: SportsPulse End-to-End Data Engineering Pipeline Architecture.",
                use_container_width=True,
            )
        except Exception as exc:
            st.warning(f"Could not render image file: {exc}")
    else:
        # Graceful missing-image notification with clear placement instructions
        relative_target = "assets/sportspulse_architecture.png"
        st.info(
            f"ℹ️ **Architecture Diagram File**: The visual diagram is configured to load from `{relative_target}`. "
            f"Please place your architecture diagram image at this location to display it here.\n\n"
            f"*Expected path: `{image_path}`*"
        )

    st.markdown("---")

    # -------------------------------------------------------------------------
    # 2. PIPELINE FLOW OVERVIEW (SUMMARY METRICS)
    # -------------------------------------------------------------------------
    st.subheader("⚡ End-to-End Medallion Pipeline Flow")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Bronze Files", "1,243 JSON", help="Cricsheet ball-by-ball matches")
    c2.metric("Silver Deliveries", "295,732 Balls", help="Parsed relational Parquet records")
    c3.metric("Gold Aggregates", "738 Bat / 577 Bowl", help="Curated player analytical marts")
    c4.metric("Warehouse Tables", "5 Relational", help="PostgreSQL dimensional star schema")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # 3. DETAILED COMPONENT BREAKDOWN
    # -------------------------------------------------------------------------
    st.subheader("🔍 Core Architecture Components")

    for comp in ARCHITECTURE_COMPONENTS:
        with st.expander(f"{comp['icon']} {comp['name']}  ·  [{comp['badge']}]", expanded=False):
            st.markdown(f"**Layer / Role**: `{comp['category']}`")
            st.write(comp["description"])

            st.markdown("**Key Characteristics & Deliverables:**")
            for item in comp["details"]:
                st.markdown(f"- {item}")

            # Explicit note for CSV Export to avoid implying it is a pipeline stage
            if comp["id"] == "csv_export":
                st.info(
                    "📌 **Dataset Artifact Note**: The CSV export is provided as a standalone "
                    "dataset artifact for cross-platform convenience and data portability. "
                    "It is not a mandatory intermediate processing stage within the core "
                    "Bronze ➔ Silver ➔ Gold ➔ Warehouse pipeline."
                )

    st.markdown("---")

    # -------------------------------------------------------------------------
    # 4. OUTPUTS & ACCESS SUBSYSTEMS
    # -------------------------------------------------------------------------
    st.subheader("📤 Outputs & Subsystem Access")

    out1, out2, out3 = st.columns(3)

    with out1:
        st.markdown("#### 📄 Datasets & Files")
        st.markdown("- **Parquet Layers**: `data/silver/` & `data/gold/`")
        st.markdown("- **CSV Dataset Export**: Standalone tabular source data")
        st.markdown("- **Audit Logs**: `reports/latest_pipeline_run.json`")

    with out2:
        st.markdown("#### 🗄️ Database & Warehouse")
        st.markdown("- **PostgreSQL Warehouse**: 5 verified dimensional & fact tables")
        st.markdown("- **Connection**: `host.docker.internal:5432` / local port `5432`")
        st.markdown("- **Database Name**: `sportspulse`")

    with out3:
        st.markdown("#### 🌐 Web Applications & UIs")
        st.markdown("- **Streamlit Dashboard**: `http://localhost:8501`")
        st.markdown("- **Apache Airflow UI**: `http://localhost:8080`")
        st.markdown("- **Docker Services**: Airflow standalone + PostgreSQL")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # 5. ARCHITECTURAL HIGHLIGHTS & DESIGN PRINCIPLES
    # -------------------------------------------------------------------------
    st.subheader("💎 Key Engineering Design Principles")

    h1, h2, h3 = st.columns(3)

    with h1:
        st.markdown("#### 🔄 Idempotence & Isolation")
        st.write(
            "Every stage can be re-run safely without corrupting existing records. "
            "Bronze manifest hashing ensures deduplication, and PostgreSQL uses ON CONFLICT DO UPDATE upserts."
        )

    with h2:
        st.markdown("#### ⚡ High-Throughput Columnar Parquet")
        st.write(
            "Snappy-compressed columnar Parquet tables in Silver and Gold layers achieve "
            ">10x compression over raw JSON and enable sub-second aggregations on 295k+ deliveries."
        )

    with h3:
        st.markdown("#### 🛡️ Fail-Fast Quality Gates")
        st.write(
            "Dual-stage validation halts execution before corrupt data reaches downstream marts. "
            "Schema enforcement logs structural anomalies, while data-quality checks audit logical consistency."
        )

    # Diagram Footer Verification Badges
    st.markdown("---")
    st.caption(
        "🏆 **SportsPulse Architectural Guarantees**: "
        "Medallion Architecture (Bronze ➔ Silver ➔ Gold)  |  "
        "Data Validation (Schema + Quality Checks)  |  "
        "PostgreSQL Warehouse (5 Verified Tables)  |  "
        "Orchestration (Airflow 8 Tasks)  |  "
        "Docker Compose Containerized  |  "
        "Automated Test Suite (21 Unit Tests Passed)"
    )
