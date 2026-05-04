"""
bundle_exporter.py - Writes cohort-extracted OMOP data into a .zip bundle.

Creates a structured zip file containing:
  - clinical/<table>.csv     for each clinical OMOP table
  - vocabulary/<table>.csv   for concept + concept_relationship
  - manifest.json            with cohort metadata, row counts, timestamps

Nothing hardcoded — table list and columns come from the extracted data dict.
"""
import csv
import io
import json
import logging
import os
import zipfile
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

def _json_serial(obj: Any) -> str:
    """JSON serialiser for date/datetime objects."""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


class BundleExporter:
    """
    Writes extracted OMOP data into a zip file on the local filesystem.

    Usage::

        exporter = BundleExporter()
        zip_path = exporter.export(
            data={"person": [...], "measurement": [...], "concept": [...]},
            output_dir="/path/to/output",
            cohort_definition_id=1,
            cohort_name="My Cohort",
        )
    """

    def __init__(self, conn=None, schema: str = "public"):
        self.conn = conn
        self.schema = schema
        self._person_scoped_tables: Optional[Set[str]] = None

    def export(
        self,
        data: Dict[str, List[Dict[str, Any]]],
        output_dir: str,
        cohort_definition_id: int,
        cohort_name: str = "",
        cohort_description: str = "",
        cdm_version: str = "5.4",
        source_schema: str = "public",
    ) -> str:
        """
        Write all extracted data into a zip bundle.

        Args:
            data: Dict of ``{table_name: [row_dicts]}`` from CohortExtractor.
            output_dir: Directory where the zip file will be created.
            cohort_definition_id: Cohort ID for naming and manifest.
            cohort_name: Human-readable cohort label.
            cohort_description: Free-text description.
            cdm_version: OMOP CDM version string.
            source_schema: Source DB schema name.

        Returns:
            Absolute path to the created zip file.
        """
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_filename = f"cohort_{cohort_definition_id}_{timestamp}.zip"
        zip_path = os.path.join(output_dir, zip_filename)

        table_row_counts: Dict[str, int] = {}

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for table_name, rows in data.items():
                if not rows:
                    continue

                # Determine subdirectory
                subdir = "clinical" if self._is_person_scoped_table(table_name, rows) else "vocabulary"
                csv_path_in_zip = f"{subdir}/{table_name}.csv"

                # Write CSV to zip
                csv_content = self._rows_to_csv(rows)
                zf.writestr(csv_path_in_zip, csv_content)
                table_row_counts[table_name] = len(rows)

                logger.info(
                    f"  Bundled {table_name}: {len(rows)} rows → {csv_path_in_zip}"
                )

            # Count persons
            person_count = len(data.get("person", []))

            # Count vocabulary concepts
            vocab_concept_count = len(data.get("concept", []))

            # Build and write manifest
            manifest = {
                "cohort_definition_id": cohort_definition_id,
                "cohort_name": cohort_name,
                "cohort_description": cohort_description,
                "cdm_version": cdm_version,
                "export_timestamp": datetime.now().isoformat(),
                "person_count": person_count,
                "table_row_counts": table_row_counts,
                "vocabulary_concept_count": vocab_concept_count,
                "source_schema": source_schema,
                "total_tables": len(table_row_counts),
                "total_rows": sum(table_row_counts.values()),
            }
            manifest_json = json.dumps(manifest, indent=2, default=_json_serial)
            zf.writestr("manifest.json", manifest_json)

        logger.info(
            f"Bundle created: {zip_path} "
            f"({len(table_row_counts)} tables, "
            f"{sum(table_row_counts.values()):,} total rows)"
        )
        return os.path.abspath(zip_path)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_person_scoped_tables(self) -> Set[str]:
        if not self.conn:
            return set()
        try:
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT table_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND column_name = 'person_id'
                """,
                (self.schema,),
            )
            tables = {row[0] for row in cur.fetchall()}
            cur.close()
            return tables
        except Exception as exc:
            try:
                self.conn.rollback()
            except Exception:
                pass
            logger.warning(f"Could not load person-scoped tables: {exc}")
            return set()

    def _is_person_scoped_table(self, table_name: str, rows: List[Dict[str, Any]]) -> bool:
        if self.conn and self._person_scoped_tables is None:
            self._person_scoped_tables = self._load_person_scoped_tables()
        if self._person_scoped_tables:
            return table_name in self._person_scoped_tables
        if not rows:
            return False
        sample = rows[0]
        return isinstance(sample, dict) and "person_id" in sample

    @staticmethod
    def _rows_to_csv(rows: List[Dict[str, Any]]) -> str:
        """Convert a list of row dicts to a CSV string."""
        if not rows:
            return ""

        # Use the keys from the first row as headers
        headers = list(rows[0].keys())
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=headers,
            quoting=csv.QUOTE_MINIMAL,
            extrasaction="ignore",
        )
        writer.writeheader()
        for row in rows:
            # Convert date/datetime objects to ISO strings for CSV
            serialised = {}
            for k, v in row.items():
                if isinstance(v, (date, datetime)):
                    serialised[k] = v.isoformat()
                elif v is None:
                    serialised[k] = ""
                else:
                    serialised[k] = v
            writer.writerow(serialised)

        return output.getvalue()
