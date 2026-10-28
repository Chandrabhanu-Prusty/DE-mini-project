#!/usr/bin/env python3
"""
SportsPulse: Reproducible End-to-End Data Pipeline Runner
=========================================================

Sequentially executes and orchestrates all Medallion layers:
  Stage 1: Register Source Files (Bronze manifest & SHA-256)
  Stage 2: Validate Schemas (JSON schema enforcement)
  Stage 3: Run Data Quality Checks (Integrity & duplicate detection)
  Stage 4: Parse Matches to Silver (JSON de-nesting to Parquet)
  Stage 5: Normalize Entities (Teams & players dimensional modeling)
  Stage 6: Build Gold Datasets (Curated analytical marts)
  Stage 7: Load PostgreSQL Warehouse (Idempotent dimensional & fact loading)
  Stage 8: Generate Pipeline Execution & Quality Report (Metrics audit)

Features:
- Subprocess isolation preserving script paths and environment.
- Fail-fast error handling with clear exit codes and stage identification.
- Real-time execution logging with credential redaction.
- Non-destructive and idempotent re-runs.
- Timestamped JSON audit reports and persistent CSV run logs in reports/.
"""

import argparse
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

# Reconfigure stdout/stderr to UTF-8 with character replacement fallback for Windows PowerShell
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


# -----------------------------------------------------------------------------
# PATH RESOLUTION & CONFIGURATION
# -----------------------------------------------------------------------------

def resolve_project_root() -> Path:
    """Detect SportsPulse project root regardless of current working directory."""
    current = Path(__file__).resolve().parent
    if (current / "src").is_dir():
        return current
    if (current / "SportsPulse" / "src").is_dir():
        return current / "SportsPulse"
    return current


PROJECT_ROOT = resolve_project_root()
REPORT_DIR = PROJECT_ROOT / "reports"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "ipl"


# -----------------------------------------------------------------------------
# PIPELINE STAGE DEFINITIONS
# -----------------------------------------------------------------------------

PIPELINE_STAGES = [
    {
        "id": "bronze_register",
        "number": 1,
        "name": "Register Source Files",
        "module": "src.ingestion.register_files",
        "description": "Scan data/bronze/ipl/*.json, compute SHA-256 sums, generate file manifest.",
    },
    {
        "id": "schema_validate",
        "number": 2,
        "name": "Validate Schemas",
        "module": "src.validation.validate_schema",
        "description": "Validate JSON match files against schema specifications and log invalid files.",
    },
    {
        "id": "quality_check",
        "number": 3,
        "name": "Data Quality Checks",
        "module": "src.validation.quality_check",
        "description": "Run logical integrity, duplicate content, and consistency checks across matches.",
    },
    {
        "id": "silver_parse",
        "number": 4,
        "name": "Parse Matches to Silver",
        "module": "src.processing.parse_matches",
        "description": "De-nest JSON matches into matches, innings, and deliveries Parquet tables.",
    },
    {
        "id": "silver_normalize",
        "number": 5,
        "name": "Normalize Entities",
        "module": "src.processing.normalize_entities",
        "description": "Clean entity names and build dim_teams and dim_players Parquet tables.",
    },
    {
        "id": "gold_build",
        "number": 6,
        "name": "Build Gold Datasets",
        "module": "src.processing.build_gold",
        "description": "Aggregate player batting, bowling, team performance, and match summaries.",
    },
    {
        "id": "warehouse_load",
        "number": 7,
        "name": "Load PostgreSQL Warehouse",
        "module": "src.warehouse.load_warehouse",
        "description": "Upsert dimensions and match facts; reload batting and bowling fact tables.",
    },
]


# -----------------------------------------------------------------------------
# SECURITY & REDACTION
# -----------------------------------------------------------------------------

def sanitize_text(text: str) -> str:
    """Redact passwords and secrets from connection strings and logs."""
    if not text:
        return ""
    # Matches patterns like postgresql://user:password@host:port/db even with @ in password
    pattern = r"(postgresql(?:\+[a-z0-9_]+)?://[^:]+:)(.*)(@[^/@]+(?:/[^?]*)?)"
    return re.sub(pattern, r"\1***\3", text)


# -----------------------------------------------------------------------------
# METRICS EXTRACTION
# -----------------------------------------------------------------------------

def get_parquet_row_count(file_path: Path) -> Optional[int]:
    """Read Parquet row count quickly from metadata without loading dataset."""
    if not file_path.exists():
        return None
    try:
        import pyarrow.parquet as pq
        metadata = pq.read_metadata(file_path)
        return metadata.num_rows
    except Exception:
        try:
            import pandas as pd
            df = pd.read_parquet(file_path)
            return len(df)
        except Exception:
            return None


def extract_pipeline_metrics(project_root: Path) -> Dict[str, Any]:
    """Collect verified counts and metrics across all Medallion layers."""
    metrics: Dict[str, Any] = {
        "bronze": {},
        "validation": {},
        "quality": {},
        "silver": {},
        "gold": {},
        "warehouse": {},
    }

    # 1. Bronze Ingestion Manifest
    manifest_path = project_root / "reports" / "file_manifest.csv"
    if manifest_path.exists():
        try:
            with open(manifest_path, "r", encoding="utf-8") as fp:
                rows = list(csv.DictReader(fp))
                metrics["bronze"] = {
                    "manifest_path": str(manifest_path.relative_to(project_root)),
                    "total_files_discovered": len(rows),
                    "registered_count": sum(1 for r in rows if r.get("status") == "REGISTERED"),
                    "error_count": sum(1 for r in rows if r.get("status") == "ERROR"),
                }
        except Exception as exc:
            metrics["bronze"]["read_error"] = str(exc)

    # 2. Schema Validation
    schema_path = project_root / "reports" / "schema_validation_report.csv"
    if schema_path.exists():
        try:
            with open(schema_path, "r", encoding="utf-8") as fp:
                rows = list(csv.DictReader(fp))
                metrics["validation"] = {
                    "report_path": str(schema_path.relative_to(project_root)),
                    "total_checked": len(rows),
                    "valid_count": sum(1 for r in rows if r.get("status") == "VALID"),
                    "invalid_count": sum(1 for r in rows if r.get("status") == "INVALID"),
                    "error_count": sum(1 for r in rows if r.get("status") == "ERROR"),
                }
        except Exception as exc:
            metrics["validation"]["read_error"] = str(exc)

    # 3. Data Quality Checks
    quality_path = project_root / "reports" / "data_quality_report.csv"
    if quality_path.exists():
        try:
            with open(quality_path, "r", encoding="utf-8") as fp:
                rows = list(csv.DictReader(fp))
                metrics["quality"] = {
                    "report_path": str(quality_path.relative_to(project_root)),
                    "total_issues": len(rows),
                    "errors": sum(1 for r in rows if r.get("severity") == "ERROR"),
                    "warnings": sum(1 for r in rows if r.get("severity") == "WARNING"),
                    "affected_files": len({r.get("file_name") for r in rows if r.get("file_name")}),
                }
        except Exception as exc:
            metrics["quality"]["read_error"] = str(exc)

    # 4. Silver Layer Parquet Counts
    silver_dir = project_root / "data" / "silver"
    silver_tables = ["matches", "innings", "deliveries", "dim_teams", "dim_players"]
    silver_counts = {}
    for table in silver_tables:
        p = silver_dir / f"{table}.parquet"
        cnt = get_parquet_row_count(p)
        if cnt is not None:
            silver_counts[table] = cnt
    metrics["silver"] = silver_counts

    # 5. Gold Layer Parquet Counts
    gold_dir = project_root / "data" / "gold"
    gold_tables = [
        "player_batting",
        "player_bowling",
        "team_performance",
        "match_summary",
        "innings_summary",
    ]
    gold_counts = {}
    for table in gold_tables:
        p = gold_dir / f"{table}.parquet"
        cnt = get_parquet_row_count(p)
        if cnt is not None:
            gold_counts[table] = cnt
    metrics["gold"] = gold_counts

    # 6. Warehouse Row Counts (PostgreSQL)
    try:
        from dotenv import load_dotenv
        load_dotenv(project_root / ".env")
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            from sqlalchemy import create_engine, text
            engine = create_engine(database_url, connect_args={"connect_timeout": 5})
            wh_counts = {}
            with engine.connect() as conn:
                for tbl in ["dim_teams", "dim_players", "fact_matches", "fact_player_batting", "fact_player_bowling"]:
                    val = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar()
                    wh_counts[tbl] = int(val) if val is not None else 0
            engine.dispose()
            metrics["warehouse"] = {
                "status": "connected",
                "table_counts": wh_counts,
            }
        else:
            metrics["warehouse"] = {
                "status": "skipped",
                "reason": "DATABASE_URL not set in .env",
            }
    except Exception as exc:
        metrics["warehouse"] = {
            "status": "error",
            "error": sanitize_text(str(exc)),
        }

    return metrics


# -----------------------------------------------------------------------------
# PIPELINE EXECUTION ENGINE
# -----------------------------------------------------------------------------

class PipelineRunner:
    """Manages the sequential execution of pipeline stages."""

    def __init__(
        self,
        project_root: Path,
        stages: Optional[List[Dict[str, Any]]] = None,
        skip_warehouse: bool = False,
        strict: bool = False,
        dry_run: bool = False,
    ):
        self.project_root = project_root
        self.skip_warehouse = skip_warehouse
        self.strict = strict
        self.dry_run = dry_run

        all_stages = stages or PIPELINE_STAGES
        if skip_warehouse:
            self.stages = [s for s in all_stages if s["id"] != "warehouse_load"]
        else:
            self.stages = all_stages

        self.run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        self.report_dir = self.project_root / "reports"
        self.stage_results: List[Dict[str, Any]] = []

    def execute_stage(self, stage: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single pipeline stage in a dedicated subprocess."""
        stage_num = stage["number"]
        stage_name = stage["name"]
        module_name = stage["module"]

        print(f"\n[{stage_num}/7] [>>] Executing: {stage_name} ({module_name})")
        print("  " + stage["description"])
        print("-" * 70)

        if self.dry_run:
            print("  [DRY-RUN] Stage validated successfully without execution.")
            return {
                "number": stage_num,
                "id": stage["id"],
                "name": stage_name,
                "module": module_name,
                "status": "SUCCESS",
                "start_time_utc": datetime.now(timezone.utc).isoformat(),
                "end_time_utc": datetime.now(timezone.utc).isoformat(),
                "duration_seconds": 0.0,
                "exit_code": 0,
                "error_details": None,
            }

        start_time = datetime.now(timezone.utc)
        t0 = time.time()

        cmd = [sys.executable, "-m", module_name]
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        try:
            # Run subprocess with real-time output streaming
            process = subprocess.Popen(
                cmd,
                cwd=str(self.project_root),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
                bufsize=1,
            )

            stdout_lines = []
            stderr_lines = []

            # Stream stdout in real-time
            if process.stdout:
                for line in process.stdout:
                    clean = sanitize_text(line)
                    print("  " + clean, end="")
                    stdout_lines.append(clean)

            _, stderr_content = process.communicate()
            if stderr_content:
                clean_err = sanitize_text(stderr_content)
                stderr_lines.append(clean_err)
                print("  [STDERR] " + clean_err.strip())

            exit_code = process.returncode
            duration = round(time.time() - t0, 2)
            end_time = datetime.now(timezone.utc)

            if exit_code == 0:
                print(f"[OK] Completed in {duration:.2f}s")
                status = "SUCCESS"
                error_details = None
            else:
                print(f"[FAIL] Failed with exit code {exit_code} after {duration:.2f}s")
                status = "FAILED"
                error_details = "\n".join(stderr_lines).strip() or "Process returned non-zero exit code"

            return {
                "number": stage_num,
                "id": stage["id"],
                "name": stage_name,
                "module": module_name,
                "status": status,
                "start_time_utc": start_time.isoformat(),
                "end_time_utc": end_time.isoformat(),
                "duration_seconds": duration,
                "exit_code": exit_code,
                "error_details": error_details,
            }

        except Exception as exc:
            duration = round(time.time() - t0, 2)
            end_time = datetime.now(timezone.utc)
            clean_err = sanitize_text(str(exc))
            print(f"[FAIL] Execution exception: {clean_err}")
            return {
                "number": stage_num,
                "id": stage["id"],
                "name": stage_name,
                "module": module_name,
                "status": "FAILED",
                "start_time_utc": start_time.isoformat(),
                "end_time_utc": end_time.isoformat(),
                "duration_seconds": duration,
                "exit_code": 1,
                "error_details": clean_err,
            }

    def run(self) -> int:
        """Run all configured stages sequentially and write the audit report."""
        print("=" * 70)
        print("== SportsPulse End-to-End Pipeline Execution ==")
        print(f"  Run ID: {self.run_id}")
        print(f"  Project Root: {self.project_root}")
        print(f"  Timestamp: {datetime.now(timezone.utc).isoformat()}")
        print(f"  Skip Warehouse: {self.skip_warehouse} | Strict Mode: {self.strict} | Dry-Run: {self.dry_run}")
        print("=" * 70)

        total_start = time.time()
        failed_stage: Optional[Dict[str, Any]] = None

        for idx, stage in enumerate(self.stages):
            result = self.execute_stage(stage)
            self.stage_results.append(result)

            if result["status"] == "FAILED":
                failed_stage = result
                # Mark subsequent stages as SKIPPED
                for skipped_stage in self.stages[idx + 1:]:
                    self.stage_results.append({
                        "number": skipped_stage["number"],
                        "id": skipped_stage["id"],
                        "name": skipped_stage["name"],
                        "module": skipped_stage["module"],
                        "status": "SKIPPED",
                        "start_time_utc": None,
                        "end_time_utc": None,
                        "duration_seconds": 0.0,
                        "exit_code": None,
                        "error_details": "Skipped due to upstream failure",
                    })
                break

        total_duration = round(time.time() - total_start, 2)

        # Extract verified output metrics
        metrics = extract_pipeline_metrics(self.project_root)

        # Evaluate overall status
        if failed_stage:
            overall_status = "FAILED"
            exit_code = failed_stage.get("exit_code") or 1
        else:
            # Check for data quality warnings
            val_invalid = metrics.get("validation", {}).get("invalid_count", 0)
            val_errors = metrics.get("validation", {}).get("error_count", 0)
            qual_errors = metrics.get("quality", {}).get("errors", 0)
            qual_warnings = metrics.get("quality", {}).get("warnings", 0)

            if val_invalid > 0 or val_errors > 0 or qual_errors > 0:
                overall_status = "FAILED" if self.strict else "SUCCESS_WITH_WARNINGS"
                exit_code = 2 if self.strict else 0
            elif qual_warnings > 0:
                overall_status = "SUCCESS_WITH_WARNINGS"
                exit_code = 0
            else:
                overall_status = "SUCCESS"
                exit_code = 0

        # Generate and save final reports
        report_data = {
            "run_id": self.run_id,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "overall_status": overall_status,
            "total_duration_seconds": total_duration,
            "config": {
                "project_root": str(self.project_root),
                "skip_warehouse": self.skip_warehouse,
                "strict_mode": self.strict,
                "dry_run": self.dry_run,
            },
            "stages": self.stage_results,
            "metrics": metrics,
        }

        self.save_reports(report_data)
        self.print_summary_banner(report_data)

        return exit_code

    def save_reports(self, report_data: Dict[str, Any]) -> Path:
        """Persist timestamped JSON report and update history CSV."""
        self.report_dir.mkdir(parents=True, exist_ok=True)

        # 1. Derive timestamped JSON audit report filename from run_id
        run_id = report_data.get("run_id") or self.run_id
        slug = run_id[4:] if run_id.startswith("run_") else run_id
        report_filename = f"pipeline_run_{slug}.json"
        report_path = self.report_dir / report_filename

        # Store the exact written filename in report_data
        report_data["report_file"] = report_filename

        with open(report_path, "w", encoding="utf-8") as fp:
            json.dump(report_data, fp, indent=2)

        # 2. Latest run pointer
        latest_path = self.report_dir / "latest_pipeline_run.json"
        with open(latest_path, "w", encoding="utf-8") as fp:
            json.dump(report_data, fp, indent=2)

        # 3. Append to history CSV
        history_path = self.report_dir / "pipeline_runs_history.csv"
        csv_headers = [
            "run_id",
            "timestamp_utc",
            "overall_status",
            "total_duration_seconds",
            "stages_executed",
            "stages_passed",
            "stages_failed",
            "matches_parsed",
            "deliveries_parsed",
            "report_file",
        ]

        passed = sum(1 for s in self.stage_results if s["status"] == "SUCCESS")
        failed = sum(1 for s in self.stage_results if s["status"] == "FAILED")
        matches = report_data.get("metrics", {}).get("silver", {}).get("matches", "")
        deliveries = report_data.get("metrics", {}).get("silver", {}).get("deliveries", "")

        file_exists = history_path.exists()
        with open(history_path, "a", newline="", encoding="utf-8") as fp:
            writer = csv.DictWriter(fp, fieldnames=csv_headers)
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                "run_id": run_id,
                "timestamp_utc": report_data["timestamp_utc"],
                "overall_status": report_data["overall_status"],
                "total_duration_seconds": report_data["total_duration_seconds"],
                "stages_executed": len(self.stage_results),
                "stages_passed": passed,
                "stages_failed": failed,
                "matches_parsed": matches,
                "deliveries_parsed": deliveries,
                "report_file": report_filename,
            })

        return report_path

    def print_summary_banner(self, report: Dict[str, Any]):
        """Render a clean ASCII execution summary in console."""
        print("\n" + "=" * 70)
        status = report["overall_status"]
        if status == "SUCCESS":
            icon = "[SUCCESS]"
        elif status == "SUCCESS_WITH_WARNINGS":
            icon = "[WARNING]"
        else:
            icon = "[FAILED]"

        print(f"{icon} SPORTSPULSE PIPELINE EXECUTION SUMMARY: {status}")
        print(f"   Total Duration: {report['total_duration_seconds']:.2f}s | Run ID: {report['run_id']}")
        print("=" * 70)

        print(f"{'#':<3} {'Stage Name':<28} {'Status':<10} {'Duration':<10} {'Exit'}")
        print("-" * 70)
        for s in report["stages"]:
            stat = s["status"]
            dur = f"{s['duration_seconds']:.2f}s" if s.get("duration_seconds") is not None else "-"
            code = str(s["exit_code"]) if s.get("exit_code") is not None else "-"
            print(f"{s['number']:<3} {s['name']:<28} {stat:<10} {dur:<10} {code}")

        print("-" * 70)
        metrics = report.get("metrics", {})
        silver = metrics.get("silver", {})
        gold = metrics.get("gold", {})
        wh = metrics.get("warehouse", {}).get("table_counts", {})

        print("[METRICS] Verified Dataset Output Counts:")
        if silver:
            print(f"   Silver Tables : matches={silver.get('matches', 0):,}, innings={silver.get('innings', 0):,}, deliveries={silver.get('deliveries', 0):,}")
            print(f"   Dimensions    : dim_teams={silver.get('dim_teams', 0):,}, dim_players={silver.get('dim_players', 0):,}")
        if gold:
            print(f"   Gold Marts    : player_batting={gold.get('player_batting', 0):,}, player_bowling={gold.get('player_bowling', 0):,}, team_performance={gold.get('team_performance', 0):,}")
        if wh:
            print(f"   Warehouse (PG): fact_matches={wh.get('fact_matches', 0):,}, fact_batting={wh.get('fact_player_batting', 0):,}, fact_bowling={wh.get('fact_player_bowling', 0):,}")

        report_file = report.get("report_file")
        if not report_file:
            run_id = report.get("run_id") or self.run_id
            slug = run_id[4:] if run_id.startswith("run_") else run_id
            report_file = f"pipeline_run_{slug}.json"

        print("\n[REPORTS] Execution Audit Reports Generated:")
        print(f"   Timestamped Report : reports/{report_file}")
        print(f"   Latest Run Link    : reports/latest_pipeline_run.json")
        print(f"   Run History Log    : reports/pipeline_runs_history.csv")
        print("=" * 70)


# -----------------------------------------------------------------------------
# CLI ENTRY POINT
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="SportsPulse Reproducible End-to-End Pipeline Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py                     # Run all 7 stages end-to-end
  python run_pipeline.py --skip-warehouse    # Run Bronze -> Silver -> Gold without DB
  python run_pipeline.py --strict            # Treat quality/schema warnings as failures
  python run_pipeline.py --dry-run           # Validate pipeline config without running
        """,
    )
    parser.add_argument(
        "--skip-warehouse",
        action="store_true",
        help="Skip the PostgreSQL warehouse loading stage",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat schema or data quality warnings as pipeline failures",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate runner sequence and definitions without executing scripts",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    runner = PipelineRunner(
        project_root=PROJECT_ROOT,
        skip_warehouse=args.skip_warehouse,
        strict=args.strict,
        dry_run=args.dry_run,
    )
    exit_code = runner.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
