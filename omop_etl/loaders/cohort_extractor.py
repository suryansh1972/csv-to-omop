"""
cohort_extractor.py - Extracts cohort-scoped data from all OMOP CDM tables.

Given a cohort_definition_id, queries every clinical table for rows belonging
to the cohort's person_ids, then collects a filtered vocabulary subset
(concept + concept_relationship) covering only the concept_ids actually used.

Nothing is hardcoded:
  - Clinical table list comes from the DB (information_schema)
  - Concept-ID columns discovered dynamically per table
  - Vocabulary tables filtered by collected concept_ids
"""
import logging
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

class CohortExtractor:
    """
    Extracts all OMOP data belonging to a cohort, plus a filtered
    vocabulary subset.

    Usage::

        extractor = CohortExtractor(conn, schema="public")
        data = extractor.extract(cohort_definition_id=1)
        # data == {"person": [rows...], "measurement": [rows...], ...,
        #          "concept": [rows...], "concept_relationship": [rows...]}
    """

    def __init__(self, conn, schema: str = "public"):
        self.conn = conn
        self.schema = schema
        self._table_columns: Dict[str, List[str]] = {}
        self._load_schema_metadata()

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def _load_schema_metadata(self) -> None:
        """Cache column lists per table from information_schema."""
        try:
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                ORDER BY table_name, ordinal_position
                """,
                (self.schema,),
            )
            for table_name, column_name in cur.fetchall():
                self._table_columns.setdefault(table_name, []).append(column_name)
            cur.close()
        except Exception as exc:
            self.conn.rollback()
            logger.warning(f"Could not load schema metadata: {exc}")

    def _get_columns(self, table: str) -> List[str]:
        return self._table_columns.get(table, [])

    def _get_concept_id_columns(self, table: str) -> List[str]:
        """Return all columns ending in '_concept_id' for a table."""
        return [c for c in self._get_columns(table) if c.endswith("_concept_id")]

    def _table_exists(self, table: str) -> bool:
        return table in self._table_columns

    def _get_person_scoped_tables(self) -> List[str]:
        """Return all tables in the schema that carry a person_id column."""
        return sorted(
            [table for table, cols in self._table_columns.items() if "person_id" in cols]
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(
        self,
        cohort_definition_id: int,
        tables: Optional[List[str]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract cohort-scoped data from all relevant OMOP tables.

        Args:
            cohort_definition_id: Which cohort to extract.
            tables: Override list of clinical tables (default: all person-scoped).

        Returns:
            Dict mapping table names to lists of row dicts.
            Includes ``concept`` and ``concept_relationship`` vocabulary subsets.
        """
        # Step 1: Load cohort person_ids
        person_ids = self._load_cohort_person_ids(cohort_definition_id)
        if not person_ids:
            logger.warning(
                f"Cohort {cohort_definition_id} has no persons — nothing to extract"
            )
            return {}

        logger.info(
            f"Extracting cohort {cohort_definition_id}: "
            f"{len(person_ids)} person(s)"
        )

        # Step 2: Extract clinical tables
        clinical_tables = tables or self._get_person_scoped_tables()
        result: Dict[str, List[Dict[str, Any]]] = {}
        all_concept_ids: Set[int] = set()

        for table in clinical_tables:
            if not self._table_exists(table):
                logger.debug(f"Table '{table}' not found in schema — skipping")
                continue

            rows = self._extract_table(table, person_ids)
            if rows:
                result[table] = rows
                # Collect concept_ids from this table
                concept_cols = self._get_concept_id_columns(table)
                for row in rows:
                    for col in concept_cols:
                        cid = row.get(col)
                        if cid and isinstance(cid, int) and cid > 0:
                            all_concept_ids.add(cid)

            logger.info(f"  {table}: {len(rows)} rows extracted")

        # Step 3: Extract vocabulary subset
        if all_concept_ids:
            concept_rows = self._extract_concepts(all_concept_ids)
            if concept_rows:
                result["concept"] = concept_rows
                logger.info(f"  concept: {len(concept_rows)} rows extracted")

            rel_rows = self._extract_concept_relationships(all_concept_ids)
            if rel_rows:
                result["concept_relationship"] = rel_rows
                logger.info(
                    f"  concept_relationship: {len(rel_rows)} rows extracted"
                )

        return result

    # ------------------------------------------------------------------
    # Internal extraction helpers
    # ------------------------------------------------------------------

    def _load_cohort_person_ids(self, cohort_definition_id: int) -> List[int]:
        """Get all subject_ids for a cohort."""
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT DISTINCT subject_id
                FROM {self.schema}.cohort
                WHERE cohort_definition_id = %s
                """,
                (cohort_definition_id,),
            )
            ids = [row[0] for row in cur.fetchall()]
            cur.close()
            return ids
        except Exception as exc:
            self.conn.rollback()
            logger.error(f"Could not load cohort person_ids: {exc}")
            cur.close()
            return []

    def _extract_table(
        self,
        table: str,
        person_ids: List[int],
    ) -> List[Dict[str, Any]]:
        """Extract all rows from a table where person_id is in the cohort."""
        columns = self._get_columns(table)
        if not columns:
            return []

        # Not all tables have a 'person_id' column (e.g. concept, vocabulary)
        if "person_id" not in columns:
            return []

        col_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(person_ids))

        cur = self.conn.cursor()
        try:
            cur.execute(
                f"SELECT {col_str} FROM {self.schema}.{table} "
                f"WHERE person_id IN ({placeholders})",
                tuple(person_ids),
            )
            rows = []
            for db_row in cur.fetchall():
                rows.append(dict(zip(columns, db_row)))
            cur.close()
            return rows
        except Exception as exc:
            self.conn.rollback()
            logger.error(f"Error extracting {table}: {exc}")
            cur.close()
            return []

    def _extract_concepts(
        self, concept_ids: Set[int]
    ) -> List[Dict[str, Any]]:
        """Extract concept rows matching the given concept_ids."""
        if not concept_ids or not self._table_exists("concept"):
            return []

        columns = self._get_columns("concept")
        col_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(concept_ids))

        cur = self.conn.cursor()
        try:
            cur.execute(
                f"SELECT {col_str} FROM {self.schema}.concept "
                f"WHERE concept_id IN ({placeholders})",
                tuple(concept_ids),
            )
            rows = [dict(zip(columns, r)) for r in cur.fetchall()]
            cur.close()
            return rows
        except Exception as exc:
            self.conn.rollback()
            logger.error(f"Error extracting concepts: {exc}")
            cur.close()
            return []

    def _extract_concept_relationships(
        self, concept_ids: Set[int]
    ) -> List[Dict[str, Any]]:
        """Extract concept_relationship rows where either side is in concept_ids."""
        if not concept_ids or not self._table_exists("concept_relationship"):
            return []

        columns = self._get_columns("concept_relationship")
        col_str = ", ".join(columns)
        id_list = tuple(concept_ids)
        placeholders = ", ".join(["%s"] * len(id_list))

        cur = self.conn.cursor()
        try:
            cur.execute(
                f"SELECT {col_str} FROM {self.schema}.concept_relationship "
                f"WHERE concept_id_1 IN ({placeholders}) "
                f"   OR concept_id_2 IN ({placeholders})",
                id_list + id_list,
            )
            rows = [dict(zip(columns, r)) for r in cur.fetchall()]
            cur.close()
            return rows
        except Exception as exc:
            self.conn.rollback()
            logger.error(f"Error extracting concept_relationships: {exc}")
            cur.close()
            return []
