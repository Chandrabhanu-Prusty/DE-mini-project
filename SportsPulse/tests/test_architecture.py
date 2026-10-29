"""
SportsPulse: Unit Tests for System Architecture Module
======================================================
Tests architecture image path resolution, graceful missing-image handling,
and component specification integrity.
"""

from pathlib import Path
import tempfile
import unittest

from dashboard.architecture import (
    ARCHITECTURE_COMPONENTS,
    resolve_architecture_image_path,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class TestSystemArchitecture(unittest.TestCase):
    """Test suite for System Architecture page and components."""

    def test_architecture_components_specification(self):
        """Verify that all 9 required architecture components are defined."""
        expected_ids = [
            "source_dataset",
            "bronze_layer",
            "validation_quality",
            "silver_layer",
            "gold_layer",
            "postgresql_warehouse",
            "csv_export",
            "streamlit_dashboard",
            "airflow_orchestration",
        ]
        comp_map = {c["id"]: c for c in ARCHITECTURE_COMPONENTS}
        self.assertEqual(len(ARCHITECTURE_COMPONENTS), 9)

        for cid in expected_ids:
            self.assertIn(cid, comp_map, f"Missing component ID: {cid}")
            comp = comp_map[cid]
            self.assertTrue(comp.get("name"))
            self.assertTrue(comp.get("description"))
            self.assertTrue(len(comp.get("details", [])) > 0)

    def test_csv_export_artifact_description(self):
        """Verify CSV export is specified as an export artifact and not a mandatory pipeline stage."""
        comp_map = {c["id"]: c for c in ARCHITECTURE_COMPONENTS}
        csv_comp = comp_map.get("csv_export")
        self.assertIsNotNone(csv_comp)
        self.assertIn("Export Artifact", csv_comp.get("badge", ""))
        # Verify text explicitly highlights supplementary artifact nature
        self.assertIn("artifact", csv_comp.get("description", "").lower())

    def test_resolve_architecture_image_path_missing(self):
        """Verify graceful MISSING status returned when image does not exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_root = Path(tmpdir)
            resolved_path, status = resolve_architecture_image_path(tmp_root)
            self.assertEqual(status, "MISSING")
            self.assertEqual(resolved_path, tmp_root / "assets" / "sportspulse_architecture.png")

    def test_resolve_architecture_image_path_found(self):
        """Verify FOUND status returned when actual image file is present."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_root = Path(tmpdir)
            assets_dir = tmp_root / "assets"
            assets_dir.mkdir(parents=True, exist_ok=True)
            mock_img = assets_dir / "sportspulse_architecture.png"
            mock_img.write_bytes(b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR")

            resolved_path, status = resolve_architecture_image_path(tmp_root)
            self.assertEqual(status, "FOUND")
            self.assertEqual(resolved_path, mock_img)


if __name__ == "__main__":
    unittest.main()
