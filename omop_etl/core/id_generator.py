"""
id_generator.py - Thread-safe sequential OMOP ID generation.
Queries max existing ID from DB to avoid collisions on incremental loads.
"""
import logging
import threading
from typing import Dict

logger = logging.getLogger(__name__)


class OMOPIdGenerator:
    """
    Generates sequential integer IDs for OMOP CDM tables.
    Starts from max(existing_id) + 1 to support incremental loads.

    The table→id-column mapping is defined here as a class constant because
    it reflects the OMOP CDM 5.4 schema specification — it is not
    dataset-specific configuration.
    """

    _TABLE_ID_COLS: Dict[str, str] = {
        "person":               "person_id",
        "observation_period":   "observation_period_id",
        "visit_occurrence":     "visit_occurrence_id",
        "condition_occurrence": "condition_occurrence_id",
        "drug_exposure":        "drug_exposure_id",
        "measurement":          "measurement_id",
        "observation":          "observation_id",
        "procedure_occurrence": "procedure_occurrence_id",
        "device_exposure":      "device_exposure_id",
        "note":                 "note_id",
        "death":                "death_id",
        "cohort":               "cohort_definition_id",
    }

    def __init__(self, conn, schema: str = "public"):
        self._lock      = threading.Lock()
        self._counters: Dict[str, int] = {}
        self._conn      = conn
        self._schema    = schema
        self._initialize_from_db()

    def _initialize_from_db(self) -> None:
        for table, id_col in self._TABLE_ID_COLS.items():
            try:
                cur = self._conn.cursor()
                cur.execute(
                    f"SELECT COALESCE(MAX({id_col}), 0) FROM {self._schema}.{table}"
                )
                row = cur.fetchone()
                self._counters[table] = (row[0] if row else 0) + 1
                cur.close()
            except Exception as exc:
                self._conn.rollback()
                logger.debug(f"Could not read max ID for {table}: {exc}")
                self._counters[table] = 1
        logger.info("ID generators initialized from database")

    def next_id(self, table: str) -> int:
        """Return the next available ID for a table. Thread-safe."""
        with self._lock:
            current = self._counters.get(table, 1)
            self._counters[table] = current + 1
            return current