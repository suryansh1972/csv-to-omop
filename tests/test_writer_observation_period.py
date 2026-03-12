import os
import sys
import types
import unittest

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "omop_etl"))

fake_psycopg2 = types.ModuleType("psycopg2")
fake_psycopg2.extras = types.ModuleType("psycopg2.extras")
sys.modules.setdefault("psycopg2", fake_psycopg2)
sys.modules.setdefault("psycopg2.extras", fake_psycopg2.extras)

from omop_etl.loaders.omop_writer import OMOPWriter


class _DummyConn:
    def cursor(self):
        raise AssertionError("cursor should not be used in dry-run mode")

    def rollback(self):
        return None


class ObservationPeriodWriterTests(unittest.TestCase):
    def test_dry_run_accepts_large_person_id_without_overflow_formula(self):
        writer = OMOPWriter(_DummyConn(), dry_run=True)

        writer.write_observation_period(
            person_id=1_987_654_321,
            start_date="2024-01-01",
            end_date="2024-01-31",
            period_type_concept_id=0,
            observation_period_id=42,
        )

        stats = writer.stats()
        self.assertEqual(stats["observation_period"]["written"], 1)
        self.assertEqual(stats["observation_period"]["errors"], 0)


if __name__ == "__main__":
    unittest.main()
