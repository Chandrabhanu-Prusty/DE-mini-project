"""
SportsPulse: Unit Tests for Dashboard Pipeline Control Center
=============================================================
Tests report loading, lock management, subprocess execution in dry-run mode,
Airflow health checking, and data quality extraction.
"""

import json
from pathlib import Path
import tempfile
import time
import unittest

from dashboard.pipeline_control import (
    acquire_pipeline_lock,
    check_airflow_status,
    execute_pipeline_run,
    is_pipeline_running,
    load_data_quality_summary,
    load_latest_run_report,
    load_run_history_df,
    release_pipeline_lock,
    sanitize_text,
    STAGE_METADATA,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class TestDashboardPipelineControl(unittest.TestCase):
    """Test suite for Streamlit dashboard pipeline control functions."""

    def test_stage_metadata_definitions(self):
        """Verify that all 7 Medallion stages are defined with correct IDs and numbers."""
        self.assertEqual(len(STAGE_METADATA), 7)
        expected_ids = [
            "bronze_register",
            "schema_validate",
            "quality_check",
            "silver_parse",
            "silver_normalize",
            "gold_build",
            "warehouse_load",
        ]
        for idx, expected_id in enumerate(expected_ids, start=1):
            stage = STAGE_METADATA[idx - 1]
            self.assertEqual(stage["number"], idx)
            self.assertEqual(stage["id"], expected_id)
            self.assertTrue(stage["name"])
            self.assertTrue(stage["description"])

    def test_load_latest_run_report_real_data(self):
        """Verify that actual latest_pipeline_run.json parses properly."""
        report = load_latest_run_report(PROJECT_ROOT)
        self.assertIsNotNone(report, "latest_pipeline_run.json should exist and be valid dict")
        self.assertIn("run_id", report)
        self.assertIn("overall_status", report)
        self.assertIn("stages", report)
        self.assertIn("metrics", report)
        self.assertEqual(len(report["stages"]), 7)

    def test_load_latest_run_report_missing_and_corrupt(self):
        """Verify that missing files and malformed JSON are handled gracefully without exceptions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_root = Path(tmpdir)
            # Missing file
            self.assertIsNone(load_latest_run_report(tmp_root))

            # Corrupted JSON
            rep_dir = tmp_root / "reports"
            rep_dir.mkdir(parents=True, exist_ok=True)
            corrupt_file = rep_dir / "latest_pipeline_run.json"
            corrupt_file.write_text("{invalid json", encoding="utf-8")
            self.assertIsNone(load_latest_run_report(tmp_root))

    def test_load_run_history_df_real_data(self):
        """Verify that pipeline_runs_history.csv loads as a valid DataFrame."""
        df = load_run_history_df(PROJECT_ROOT)
        self.assertFalse(df.empty, "Run history should contain recorded pipeline executions")
        self.assertIn("run_id", df.columns)
        self.assertIn("overall_status", df.columns)
        self.assertIn("total_duration_seconds", df.columns)

    def test_load_data_quality_summary(self):
        """Verify extraction of data quality metrics from CSV files."""
        summary = load_data_quality_summary(PROJECT_ROOT)
        self.assertGreater(summary["validation_total"], 0, "Validation total should be > 0")
        self.assertEqual(summary["validation_invalid"], 0, "All matches should be valid")
        self.assertEqual(summary["quality_errors"], 0, "Zero quality errors expected")

    def test_check_airflow_status_format(self):
        """Verify that Airflow healthcheck returns a structured dictionary without raising errors."""
        res = check_airflow_status()
        self.assertIsInstance(res, dict)
        self.assertIn("reachable", res)
        self.assertIn("url", res)
        self.assertIn("message", res)

    def test_pipeline_lock_acquire_and_release(self):
        """Verify process lock prevents concurrent execution and cleans up reliably."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_root = Path(tmpdir)
            self.assertFalse(is_pipeline_running(tmp_root))

            # Acquire lock
            acquired = acquire_pipeline_lock(tmp_root, "Test Run 1")
            self.assertTrue(acquired)
            self.assertTrue(is_pipeline_running(tmp_root))

            # Second acquire must fail
            second = acquire_pipeline_lock(tmp_root, "Test Run 2")
            self.assertFalse(second)

            # Release lock
            release_pipeline_lock(tmp_root)
            self.assertFalse(is_pipeline_running(tmp_root))

    def test_execute_pipeline_run_dry_run(self):
        """Verify subprocess execution in dry-run mode returns success with captured logs."""
        captured_lines = []

        def callback(line: str):
            captured_lines.append(line)

        result = execute_pipeline_run(
            project_root=PROJECT_ROOT,
            skip_warehouse=False,
            strict=False,
            dry_run=True,
            log_stream_callback=callback,
        )

        self.assertEqual(result["exit_code"], 0)
        self.assertTrue(result["success"])
        self.assertIn("DRY-RUN", result["stdout"])
        self.assertGreater(len(captured_lines), 0)

    def test_credential_sanitization(self):
        """Verify passwords and secrets are redacted from console output and error logs."""
        raw = "postgresql+psycopg2://postgres:SecretPassword123@localhost:5432/sportspulse"
        clean = sanitize_text(raw)
        self.assertNotIn("SecretPassword123", clean)
        self.assertIn("***", clean)


if __name__ == "__main__":
    unittest.main()
