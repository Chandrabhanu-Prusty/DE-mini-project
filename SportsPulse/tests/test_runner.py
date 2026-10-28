"""
SportsPulse: Unit Tests for Pipeline Runner
===========================================

Tests pipeline runner sequencing, failure handling, secret redaction,
metrics extraction, and audit report generation.
"""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from run_pipeline import (
    PIPELINE_STAGES,
    PipelineRunner,
    PROJECT_ROOT,
    extract_pipeline_metrics,
    sanitize_text,
)


class TestPipelineRunner(unittest.TestCase):
    """Test suite for SportsPulse pipeline runner and orchestration."""

    def test_stage_definitions_order_and_modules(self):
        """Verify that all 7 stages exist in exact Medallion order and point to real files."""
        expected_stages = [
            ("bronze_register", "src.ingestion.register_files"),
            ("schema_validate", "src.validation.validate_schema"),
            ("quality_check", "src.validation.quality_check"),
            ("silver_parse", "src.processing.parse_matches"),
            ("silver_normalize", "src.processing.normalize_entities"),
            ("gold_build", "src.processing.build_gold"),
            ("warehouse_load", "src.warehouse.load_warehouse"),
        ]

        self.assertEqual(len(PIPELINE_STAGES), 7)
        for idx, (expected_id, expected_module) in enumerate(expected_stages, start=1):
            stage = PIPELINE_STAGES[idx - 1]
            self.assertEqual(stage["number"], idx)
            self.assertEqual(stage["id"], expected_id)
            self.assertEqual(stage["module"], expected_module)

            # Check module file exists on disk
            rel_path = stage["module"].replace(".", "/") + ".py"
            full_path = PROJECT_ROOT / rel_path
            self.assertTrue(
                full_path.exists(),
                f"Module file not found for stage {expected_id}: {full_path}",
            )

    def test_credential_sanitization(self):
        """Verify that connection strings and error logs never leak passwords."""
        raw_url = "postgresql+psycopg2://postgres:SuperSecretP@ss123@localhost:5432/sportspulse"
        sanitized = sanitize_text(raw_url)
        self.assertNotIn("SuperSecretP@ss123", sanitized)
        self.assertIn("postgresql+psycopg2://postgres:***@localhost:5432/sportspulse", sanitized)

        # Standard text should be unchanged
        plain = "Processing match 1082591.json without errors."
        self.assertEqual(sanitize_text(plain), plain)
        self.assertEqual(sanitize_text(""), "")

    def test_metrics_extraction_integrity(self):
        """Verify metrics extractor reads valid data structures from reports and Parquet."""
        metrics = extract_pipeline_metrics(PROJECT_ROOT)

        self.assertIn("bronze", metrics)
        self.assertIn("validation", metrics)
        self.assertIn("quality", metrics)
        self.assertIn("silver", metrics)
        self.assertIn("gold", metrics)
        self.assertIn("warehouse", metrics)

        # Silver tables should be present with non-negative row counts
        silver = metrics["silver"]
        for table in ["matches", "innings", "deliveries", "dim_teams", "dim_players"]:
            self.assertIn(table, silver)
            self.assertGreaterEqual(silver[table], 0)

        # Gold marts should be present with non-negative row counts
        gold = metrics["gold"]
        for mart in ["player_batting", "player_bowling", "team_performance", "match_summary", "innings_summary"]:
            self.assertIn(mart, gold)
            self.assertGreaterEqual(gold[mart], 0)

    def test_dry_run_execution(self):
        """Verify dry-run mode validates all stages without executing subprocesses."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_root = Path(tmp_dir)
            runner = PipelineRunner(project_root=tmp_root, dry_run=True)
            exit_code = runner.run()

            self.assertEqual(exit_code, 0)
            self.assertEqual(len(runner.stage_results), 7)
            for res in runner.stage_results:
                self.assertEqual(res["status"], "SUCCESS")
                self.assertEqual(res["duration_seconds"], 0.0)

            # Check that reports were generated
            report_dir = tmp_root / "reports"
            latest_report = report_dir / "latest_pipeline_run.json"
            self.assertTrue(latest_report.exists())

            with open(latest_report, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                self.assertEqual(data["overall_status"], "SUCCESS")
                self.assertTrue(data["config"]["dry_run"])

    def test_timestamped_report_filename_matches_run_id(self):
        """Verify that the timestamped report filename is derived from run_id and matches printed output."""
        import csv
        import io
        from contextlib import redirect_stdout

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_root = Path(tmp_dir)
            runner = PipelineRunner(project_root=tmp_root, dry_run=True)

            buf = io.StringIO()
            with redirect_stdout(buf):
                exit_code = runner.run()

            self.assertEqual(exit_code, 0)
            slug = runner.run_id[4:] if runner.run_id.startswith("run_") else runner.run_id
            expected_filename = f"pipeline_run_{slug}.json"
            expected_report_path = tmp_root / "reports" / expected_filename

            # 1. Verify the timestamped file exists on disk
            self.assertTrue(
                expected_report_path.exists(),
                f"Expected report file {expected_report_path} was not created.",
            )

            # 2. Verify file content matches run_id
            with open(expected_report_path, "r", encoding="utf-8") as fp:
                report_content = json.load(fp)
                self.assertEqual(report_content["run_id"], runner.run_id)
                self.assertEqual(report_content["report_file"], expected_filename)

            # 3. Verify history CSV records the exact same report_file
            history_csv = tmp_root / "reports" / "pipeline_runs_history.csv"
            self.assertTrue(history_csv.exists())
            with open(history_csv, "r", encoding="utf-8") as fp:
                rows = list(csv.DictReader(fp))
                self.assertGreater(len(rows), 0)
                self.assertEqual(rows[-1]["run_id"], runner.run_id)
                self.assertEqual(rows[-1]["report_file"], expected_filename)

            # 4. Verify console summary banner printed the exact written path
            printed_output = buf.getvalue()
            expected_printed_line = f"reports/{expected_filename}"
            self.assertIn(
                expected_printed_line,
                printed_output,
                f"Console output did not contain the exact written path '{expected_printed_line}'",
            )

    def test_skip_warehouse_option(self):
        """Verify that --skip-warehouse configures exactly 6 stages."""
        runner = PipelineRunner(project_root=PROJECT_ROOT, skip_warehouse=True, dry_run=True)
        self.assertEqual(len(runner.stages), 6)
        stage_ids = [s["id"] for s in runner.stages]
        self.assertNotIn("warehouse_load", stage_ids)
        self.assertIn("gold_build", stage_ids)

    def test_fail_fast_error_handling(self):
        """Verify that if an upstream stage fails, execution stops and marks downstream SKIPPED."""
        runner = PipelineRunner(project_root=PROJECT_ROOT, dry_run=False)

        # Mock execute_stage: stage 1 succeeds, stage 2 fails
        def mock_execute(stage):
            if stage["number"] == 1:
                return {
                    "number": 1,
                    "id": stage["id"],
                    "name": stage["name"],
                    "module": stage["module"],
                    "status": "SUCCESS",
                    "duration_seconds": 1.2,
                    "exit_code": 0,
                    "error_details": None,
                }
            elif stage["number"] == 2:
                return {
                    "number": 2,
                    "id": stage["id"],
                    "name": stage["name"],
                    "module": stage["module"],
                    "status": "FAILED",
                    "duration_seconds": 0.5,
                    "exit_code": 1,
                    "error_details": "Simulated validation failure",
                }
            else:
                self.fail(f"Stage {stage['number']} should not have been executed after failure!")

        with patch.object(runner, "execute_stage", side_effect=mock_execute):
            with patch.object(runner, "save_reports"):
                with patch.object(runner, "print_summary_banner"):
                    exit_code = runner.run()

        self.assertEqual(exit_code, 1)
        self.assertEqual(len(runner.stage_results), 7)
        self.assertEqual(runner.stage_results[0]["status"], "SUCCESS")
        self.assertEqual(runner.stage_results[1]["status"], "FAILED")

        # Stages 3..7 must be SKIPPED
        for idx in range(2, 7):
            self.assertEqual(
                runner.stage_results[idx]["status"],
                "SKIPPED",
                f"Stage {idx + 1} was not marked as SKIPPED",
            )


if __name__ == "__main__":
    unittest.main()
