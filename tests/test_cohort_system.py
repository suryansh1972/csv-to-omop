"""
test_cohort_system.py - Unit tests for cohort management system.

Tests the cohort builder, cohort extractor, and bundle exporter
using mock connections (no live DB required).
"""
import csv
import io
import json
import os
import sys
import tempfile
import types
import unittest
import zipfile
from datetime import date
from unittest.mock import MagicMock, patch, call

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "omop_etl"))

# Provide a fake psycopg2 so imports don't fail without the real package
fake_psycopg2 = types.ModuleType("psycopg2")
fake_psycopg2.extras = types.ModuleType("psycopg2.extras")
sys.modules.setdefault("psycopg2", fake_psycopg2)
sys.modules.setdefault("psycopg2.extras", fake_psycopg2.extras)

from omop_etl.core.cohort_builder import CohortBuilder, CohortMetadata
from omop_etl.loaders.bundle_exporter import BundleExporter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _MockCursor:
    """Minimal cursor mock that records executed SQL and returns canned data."""

    def __init__(self, results=None, rowcount=0):
        self._results = results or []
        self.rowcount = rowcount
        self.executed_sql = []
        self.executed_params = []

    def execute(self, sql, params=None):
        self.executed_sql.append(sql)
        self.executed_params.append(params)

    def fetchone(self):
        return self._results[0] if self._results else None

    def fetchall(self):
        return self._results

    def close(self):
        pass


class _MockConn:
    """Minimal connection mock."""

    def __init__(self, cursor_factory=None):
        self._cursor_factory = cursor_factory

    def cursor(self):
        if self._cursor_factory:
            return self._cursor_factory()
        return _MockCursor()

    def commit(self):
        pass

    def rollback(self):
        pass


# ---------------------------------------------------------------------------
# CohortBuilder Tests
# ---------------------------------------------------------------------------

class CohortBuilderTests(unittest.TestCase):

    def test_ensure_table_executes_ddl(self):
        """Verify ensure_table runs CREATE TABLE IF NOT EXISTS."""
        cur = _MockCursor()
        conn = MagicMock()
        conn.cursor.return_value = cur

        builder = CohortBuilder(conn, schema="test_schema")
        builder.ensure_table()

        self.assertEqual(len(cur.executed_sql), 1)
        self.assertIn("CREATE TABLE IF NOT EXISTS", cur.executed_sql[0])
        self.assertIn("test_schema.cohort", cur.executed_sql[0])
        conn.commit.assert_called_once()

    def test_build_cohort_inserts_and_returns_metadata(self):
        """Verify build_cohort executes INSERT and returns summary metadata."""
        call_count = [0]
        cursors = []

        def make_cursor():
            call_count[0] += 1
            if call_count[0] <= 2:
                # First two cursors: DDL ensure + INSERT
                c = _MockCursor(rowcount=5)
                cursors.append(c)
                return c
            else:
                # Third cursor: metadata query
                c = _MockCursor(
                    results=[(5, date(2024, 1, 1), date(2024, 12, 31))],
                    rowcount=1,
                )
                cursors.append(c)
                return c

        conn = _MockConn(cursor_factory=make_cursor)

        builder = CohortBuilder(conn, schema="public")
        meta = builder.build_cohort(
            cohort_definition_id=42,
            cohort_name="Test Cohort",
            cohort_description="A test cohort",
        )

        self.assertIsInstance(meta, CohortMetadata)
        self.assertEqual(meta.cohort_definition_id, 42)
        self.assertEqual(meta.cohort_name, "Test Cohort")
        self.assertEqual(meta.person_count, 5)
        self.assertEqual(meta.earliest_start, date(2024, 1, 1))
        self.assertEqual(meta.latest_end, date(2024, 12, 31))

    def test_build_cohort_with_where_clause(self):
        """Verify the WHERE clause is injected into the INSERT SQL."""
        call_count = [0]

        def make_cursor():
            call_count[0] += 1
            if call_count[0] <= 2:
                c = _MockCursor(rowcount=3)
                return c
            else:
                return _MockCursor(results=[(3, date(2024, 6, 1), date(2024, 6, 30))])

        conn = _MockConn(cursor_factory=make_cursor)

        builder = CohortBuilder(conn, schema="public")
        builder.build_cohort(
            cohort_definition_id=1,
            cohort_name="Filtered",
            where_clause="p.year_of_birth > 1950",
        )

        # Check that at least one cursor got SQL containing the WHERE clause
        all_sql = []
        # We need to inspect what was executed — gather from all cursors
        # Since we use a factory, we check via call_count
        # The INSERT is in the second cursor call
        # We can check by re-running with a capturing mock
        conn2 = MagicMock()
        cur2 = _MockCursor(rowcount=3)
        conn2.cursor.return_value = cur2

        builder2 = CohortBuilder(conn2, schema="public")
        builder2.ensure_table = MagicMock()  # skip DDL
        # Mock the metadata gathering
        builder2._gather_metadata = MagicMock(
            return_value=CohortMetadata(1, "Filtered", "", 3)
        )
        builder2.build_cohort(
            cohort_definition_id=1,
            cohort_name="Filtered",
            where_clause="p.year_of_birth > 1950",
        )

        insert_sql = cur2.executed_sql[0]
        self.assertIn("WHERE p.year_of_birth > 1950", insert_sql)

    def test_list_cohorts_returns_empty_when_no_cohorts(self):
        """Verify list_cohorts returns empty list when table is empty."""
        cur = _MockCursor(results=[])
        conn = MagicMock()
        conn.cursor.return_value = cur

        builder = CohortBuilder(conn, schema="public")
        result = builder.list_cohorts()

        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# BundleExporter Tests
# ---------------------------------------------------------------------------

class BundleExporterTests(unittest.TestCase):

    def test_export_creates_valid_zip(self):
        """Verify the zip contains clinical/, vocabulary/, and manifest.json."""
        data = {
            "person": [
                {"person_id": 1, "year_of_birth": 1980, "gender_concept_id": 8507},
                {"person_id": 2, "year_of_birth": 1975, "gender_concept_id": 8532},
            ],
            "measurement": [
                {"measurement_id": 10, "person_id": 1,
                 "measurement_concept_id": 3000, "value_as_number": 120.5},
            ],
            "concept": [
                {"concept_id": 8507, "concept_name": "MALE"},
                {"concept_id": 3000, "concept_name": "Systolic BP"},
            ],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            exporter = BundleExporter()
            zip_path = exporter.export(
                data=data,
                output_dir=tmpdir,
                cohort_definition_id=99,
                cohort_name="Test Bundle",
            )

            self.assertTrue(os.path.exists(zip_path))
            self.assertTrue(zip_path.endswith(".zip"))

            with zipfile.ZipFile(zip_path, "r") as zf:
                names = zf.namelist()

                # Check structure
                self.assertIn("manifest.json", names)
                self.assertIn("clinical/person.csv", names)
                self.assertIn("clinical/measurement.csv", names)
                self.assertIn("vocabulary/concept.csv", names)

                # Check manifest content
                manifest = json.loads(zf.read("manifest.json"))
                self.assertEqual(manifest["cohort_definition_id"], 99)
                self.assertEqual(manifest["cohort_name"], "Test Bundle")
                self.assertEqual(manifest["person_count"], 2)
                self.assertEqual(manifest["table_row_counts"]["person"], 2)
                self.assertEqual(manifest["table_row_counts"]["measurement"], 1)
                self.assertEqual(manifest["table_row_counts"]["concept"], 2)
                self.assertEqual(manifest["total_tables"], 3)
                self.assertEqual(manifest["total_rows"], 5)

    def test_manifest_row_counts_match_csv_rows(self):
        """Verify manifest row counts match actual CSV line counts."""
        data = {
            "person": [{"person_id": i} for i in range(1, 11)],  # 10 persons
            "observation": [{"observation_id": i, "person_id": 1} for i in range(1, 6)],  # 5 obs
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            exporter = BundleExporter()
            zip_path = exporter.export(
                data=data,
                output_dir=tmpdir,
                cohort_definition_id=1,
            )

            with zipfile.ZipFile(zip_path, "r") as zf:
                manifest = json.loads(zf.read("manifest.json"))

                # Check person CSV
                person_csv = zf.read("clinical/person.csv").decode("utf-8")
                person_lines = [l for l in person_csv.strip().split("\n") if l]
                # lines = header + data rows
                self.assertEqual(len(person_lines) - 1, manifest["table_row_counts"]["person"])

                # Check observation CSV
                obs_csv = zf.read("clinical/observation.csv").decode("utf-8")
                obs_lines = [l for l in obs_csv.strip().split("\n") if l]
                self.assertEqual(len(obs_lines) - 1, manifest["table_row_counts"]["observation"])

    def test_export_handles_date_serialization(self):
        """Verify date/datetime objects are serialised to ISO strings in CSVs."""
        from datetime import datetime

        data = {
            "visit_occurrence": [
                {
                    "visit_occurrence_id": 1,
                    "person_id": 1,
                    "visit_start_date": date(2024, 3, 15),
                    "visit_start_datetime": datetime(2024, 3, 15, 10, 30),
                },
            ],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            exporter = BundleExporter()
            zip_path = exporter.export(
                data=data,
                output_dir=tmpdir,
                cohort_definition_id=1,
            )

            with zipfile.ZipFile(zip_path, "r") as zf:
                csv_content = zf.read("clinical/visit_occurrence.csv").decode("utf-8")
                self.assertIn("2024-03-15", csv_content)

    def test_export_skips_empty_tables(self):
        """Verify tables with empty row lists are not included in the zip."""
        data = {
            "person": [{"person_id": 1}],
            "drug_exposure": [],  # empty — should be skipped
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            exporter = BundleExporter()
            zip_path = exporter.export(
                data=data,
                output_dir=tmpdir,
                cohort_definition_id=1,
            )

            with zipfile.ZipFile(zip_path, "r") as zf:
                names = zf.namelist()
                self.assertIn("clinical/person.csv", names)
                self.assertNotIn("clinical/drug_exposure.csv", names)


if __name__ == "__main__":
    unittest.main()
