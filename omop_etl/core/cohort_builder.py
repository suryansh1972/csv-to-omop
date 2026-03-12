"""
cohort_builder.py - Builds OMOP COHORT table from already-ingested person data.

Creates cohort entries by querying the person table (with optional WHERE filter)
and joining observation_period for date ranges.  Nothing is hardcoded — all
table names, column names, and filter criteria are configurable.
"""
import logging
from dataclasses import dataclass
from datetime import date
from typing import Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class CohortMetadata:
    """Returned after cohort creation to summarise what was built."""
    cohort_definition_id: int
    cohort_name: str
    cohort_description: str
    person_count: int
    earliest_start: Optional[date] = None
    latest_end: Optional[date] = None


class CohortBuilder:
    """
    Populates the OMOP ``cohort`` table from data already loaded into the
    ``person`` and ``observation_period`` tables.

    Design decisions
    ----------------
    * ``subject_id`` in cohort = ``person_id`` from the person table
      (OMOP CDM convention).
    * ``cohort_start_date`` / ``cohort_end_date`` derived from
      ``observation_period`` per person; falls back to ``CURRENT_DATE``
      when no observation period exists.
    * The optional ``where_clause`` lets the caller scope which persons
      enter the cohort (e.g. ``year_of_birth > 1950``).
    """

    _COHORT_DDL = """
    CREATE TABLE IF NOT EXISTS {schema}.cohort (
        cohort_definition_id  INTEGER NOT NULL,
        subject_id            INTEGER NOT NULL,
        cohort_start_date     DATE    NOT NULL,
        cohort_end_date       DATE    NOT NULL
    )
    """

    def __init__(self, conn, schema: str = "public"):
        self.conn = conn
        self.schema = schema

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ensure_table(self) -> None:
        """Create the cohort table if it does not already exist."""
        cur = self.conn.cursor()
        cur.execute(self._COHORT_DDL.format(schema=self.schema))
        self.conn.commit()
        cur.close()
        logger.info("Ensured cohort table exists")

    def build_cohort(
        self,
        cohort_definition_id: int,
        cohort_name: str = "",
        cohort_description: str = "",
        where_clause: Optional[str] = None,
    ) -> CohortMetadata:
        """
        Insert rows into ``{schema}.cohort`` for every qualifying person.

        Args:
            cohort_definition_id: Unique ID for this cohort.
            cohort_name: Human-readable label (stored in returned metadata).
            cohort_description: Free-text description.
            where_clause: Optional SQL predicate applied to the person table
                          (e.g. ``"year_of_birth > 1950"``).  Applied as
                          ``WHERE <predicate>`` on the person table alias ``p``.

        Returns:
            CohortMetadata with summary statistics.
        """
        self.ensure_table()

        # Build the insertion query
        where_sql = f"WHERE {where_clause}" if where_clause else ""
        insert_sql = f"""
            INSERT INTO {self.schema}.cohort
                (cohort_definition_id, subject_id,
                 cohort_start_date, cohort_end_date)
            SELECT
                %s,
                p.person_id,
                COALESCE(
                    (SELECT MIN(op.observation_period_start_date)
                     FROM {self.schema}.observation_period op
                     WHERE op.person_id = p.person_id),
                    CURRENT_DATE
                ),
                COALESCE(
                    (SELECT MAX(op.observation_period_end_date)
                     FROM {self.schema}.observation_period op
                     WHERE op.person_id = p.person_id),
                    CURRENT_DATE
                )
            FROM {self.schema}.person p
            {where_sql}
        """

        cur = self.conn.cursor()
        try:
            cur.execute(insert_sql, (cohort_definition_id,))
            inserted = cur.rowcount
            self.conn.commit()
            logger.info(
                f"Cohort {cohort_definition_id} ('{cohort_name}'): "
                f"inserted {inserted} subject(s)"
            )
        except Exception as exc:
            self.conn.rollback()
            logger.error(f"Cohort build failed: {exc}")
            raise

        # Gather summary metadata
        meta = self._gather_metadata(
            cohort_definition_id, cohort_name, cohort_description
        )
        cur.close()
        return meta

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _gather_metadata(
        self,
        cohort_definition_id: int,
        cohort_name: str,
        cohort_description: str,
    ) -> CohortMetadata:
        cur = self.conn.cursor()
        cur.execute(
            f"""
            SELECT COUNT(*),
                   MIN(cohort_start_date),
                   MAX(cohort_end_date)
            FROM {self.schema}.cohort
            WHERE cohort_definition_id = %s
            """,
            (cohort_definition_id,),
        )
        row = cur.fetchone()
        cur.close()

        return CohortMetadata(
            cohort_definition_id=cohort_definition_id,
            cohort_name=cohort_name,
            cohort_description=cohort_description,
            person_count=row[0] if row else 0,
            earliest_start=row[1] if row else None,
            latest_end=row[2] if row else None,
        )

    def list_cohorts(self) -> list:
        """Return summary of all existing cohorts."""
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT cohort_definition_id,
                       COUNT(*) AS person_count,
                       MIN(cohort_start_date),
                       MAX(cohort_end_date)
                FROM {self.schema}.cohort
                GROUP BY cohort_definition_id
                ORDER BY cohort_definition_id
                """
            )
            rows = cur.fetchall()
            cur.close()
            return [
                {
                    "cohort_definition_id": r[0],
                    "person_count": r[1],
                    "earliest_start": r[2],
                    "latest_end": r[3],
                }
                for r in rows
            ]
        except Exception:
            self.conn.rollback()
            cur.close()
            return []
