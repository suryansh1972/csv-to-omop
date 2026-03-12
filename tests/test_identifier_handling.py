import csv
import os
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "omop_etl"))

from omop_etl.core.profiler import profile_csv
from omop_etl.mappers.person_mapper import PersonMapper


class _DummyResolver:
    def resolve_gender(self, value):
        return 0

    def resolve_race(self, value):
        return 0


class _DummyIdGenerator:
    def __init__(self):
        self._next = 1000

    def next_id(self, table_name):
        self._next += 1
        return self._next


class IdentifierHandlingTests(unittest.TestCase):
    def _write_csv(self, headers, rows):
        fd, path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
        self.addCleanup(lambda: os.path.exists(path) and os.remove(path))
        return path

    def test_profile_prefers_semantic_person_identifier(self):
        path = self._write_csv(
            ["subject_id", "meta:instanceID", "gender", "visit_date"],
            [
                {
                    "subject_id": "S001",
                    "meta:instanceID": "uuid:11111111-1111-1111-1111-111111111111",
                    "gender": "female",
                    "visit_date": "2024-01-01",
                },
                {
                    "subject_id": "S002",
                    "meta:instanceID": "uuid:22222222-2222-2222-2222-222222222222",
                    "gender": "male",
                    "visit_date": "2024-01-02",
                },
            ],
        )

        profile = profile_csv(path)

        self.assertEqual(profile.person_id_col, "subject_id")
        self.assertIn("subject_id", profile.person_id_candidates)

    def test_profile_rejects_uuid_override_without_opt_in(self):
        path = self._write_csv(
            ["meta:instanceID", "visit_date"],
            [
                {
                    "meta:instanceID": "uuid:11111111-1111-1111-1111-111111111111",
                    "visit_date": "2024-01-01",
                },
                {
                    "meta:instanceID": "uuid:22222222-2222-2222-2222-222222222222",
                    "visit_date": "2024-01-02",
                },
            ],
        )

        with self.assertRaisesRegex(RuntimeError, "allow-uuid-person-id"):
            profile_csv(path, person_id_column="meta:instanceID")

    def test_profile_accepts_uuid_override_with_opt_in(self):
        path = self._write_csv(
            ["meta:instanceID", "visit_date"],
            [
                {
                    "meta:instanceID": "uuid:11111111-1111-1111-1111-111111111111",
                    "visit_date": "2024-01-01",
                },
                {
                    "meta:instanceID": "uuid:22222222-2222-2222-2222-222222222222",
                    "visit_date": "2024-01-02",
                },
            ],
        )

        profile = profile_csv(
            path,
            person_id_column="meta:instanceID",
            allow_uuid_person_id=True,
        )

        self.assertEqual(profile.person_id_col, "meta:instanceID")

    def test_person_ids_are_namespaced_by_source_prefix(self):
        path = self._write_csv(
            ["subject_id", "visit_date"],
            [{"subject_id": "S001", "visit_date": "2024-01-01"}],
        )
        profile = profile_csv(path)
        row = {"subject_id": "S001", "visit_date": "2024-01-01"}

        mapper_a = PersonMapper(profile, _DummyResolver(), _DummyIdGenerator(), source_value_prefix="SRC_A:")
        mapper_b = PersonMapper(profile, _DummyResolver(), _DummyIdGenerator(), source_value_prefix="SRC_B:")

        self.assertNotEqual(
            mapper_a.get_or_create_person_id(row),
            mapper_b.get_or_create_person_id(row),
        )

    def test_person_mapper_accepts_synthetic_row_identifier(self):
        path = self._write_csv(
            ["visit_date", "age"],
            [{"visit_date": "2024-01-01", "age": "64"}],
        )
        profile = profile_csv(path)
        profile.person_id_col = "__synthetic_person_id__"
        row = {
            "visit_date": "2024-01-01",
            "age": "64",
            "__synthetic_person_id__": "synthetic-person:1",
        }

        mapper = PersonMapper(profile, _DummyResolver(), _DummyIdGenerator(), source_value_prefix="SRC:")
        person = mapper.map_row(row)

        self.assertIsNotNone(person)
        self.assertEqual(person["person_source_value"], "SRC:synthetic-person:1")


if __name__ == "__main__":
    unittest.main()
