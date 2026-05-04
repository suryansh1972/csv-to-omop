"""
omop_writer.py - Batch-writes OMOP CDM records to PostgreSQL.

No concept IDs are defaulted here — all type concepts are injected by
pipeline.py, which resolves them dynamically from the OMOP vocabulary.
"""
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


class OMOPWriter:
    """
    Writes OMOP CDM records to PostgreSQL in batches.

    Features:
    - Schema-aware column ordering (reads information_schema at init)
    - varchar truncation based on live schema limits
    - ON CONFLICT DO NOTHING for idempotent event inserts
    - ON CONFLICT DO UPDATE for person upserts
    - Row-by-row fallback to salvage partial batches
    - Dry-run mode
    """

    def __init__(
        self,
        conn,
        schema:     str  = "public",
        batch_size: int  = 1_000,
        dry_run:    bool = False,
    ):
        self.conn       = conn
        self.schema     = schema
        self.batch_size = batch_size
        self.dry_run    = dry_run

        self._buffers:       Dict[str, List]                      = defaultdict(list)
        self._counts:        Dict[str, int]                       = defaultdict(int)
        self._errors:        Dict[str, int]                       = defaultdict(int)
        self._table_columns: Dict[str, List[str]]                 = defaultdict(list)
        self._column_limits: Dict[str, Dict[str, Optional[int]]]  = defaultdict(dict)
        self._primary_keys:  Dict[str, List[str]]                 = defaultdict(list)
        self._load_schema_metadata()

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def _load_schema_metadata(self) -> None:
        try:
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT table_name, column_name, ordinal_position, character_maximum_length
                FROM information_schema.columns
                WHERE table_schema = %s
                ORDER BY table_name, ordinal_position
                """,
                (self.schema,),
            )
            for table_name, column_name, _, max_len in cur.fetchall():
                self._table_columns[table_name].append(column_name)
                self._column_limits[table_name][column_name] = max_len

            cur.execute(
                """
                SELECT kcu.table_name, kcu.column_name, kcu.ordinal_position
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema    = kcu.table_schema
                 AND tc.table_name      = kcu.table_name
                WHERE tc.table_schema   = %s
                  AND tc.constraint_type = 'PRIMARY KEY'
                ORDER BY kcu.table_name, kcu.ordinal_position
                """,
                (self.schema,),
            )
            for table_name, column_name, _ in cur.fetchall():
                self._primary_keys[table_name].append(column_name)
            cur.close()
        except Exception as exc:
            self.conn.rollback()
            logger.warning(f"Could not load schema metadata: {exc}")

    def _get_table_columns(self, table: str) -> List[str]:
        cols = self._table_columns.get(table, [])
        if not cols:
            logger.warning(f"No live schema columns found for table '{table}'")
        return cols

    def _get_primary_key(self, table: str) -> List[str]:
        return self._primary_keys.get(table, [])

    def _sanitize_record(self, table: str, record: Dict[str, Any]) -> Dict[str, Any]:
        limits   = self._column_limits.get(table, {})
        result   = {}
        for col, val in record.items():
            max_len = limits.get(col)
            if isinstance(val, str) and max_len and len(val) > max_len:
                result[col] = val[:max_len]
            else:
                result[col] = val
        return result

    # ------------------------------------------------------------------
    # Buffered write
    # ------------------------------------------------------------------

    def write(self, table: str, record: Optional[Dict[str, Any]]) -> None:
        if not record:
            return
        self._buffers[table].append(record)
        if len(self._buffers[table]) >= self.batch_size:
            self._flush(table)

    def _flush(self, table: str) -> None:
        records = self._buffers[table]
        if not records:
            return

        if self.dry_run:
            self._counts[table] += len(records)
            self._buffers[table] = []
            return

        cols = self._get_table_columns(table)
        if not cols:
            self._buffers[table] = []
            return

        rows = [
            [self._sanitize_record(table, r).get(c) for c in cols]
            for r in records
        ]
        col_str      = ", ".join(cols)
        placeholders = ", ".join(["%s"] * len(cols))
        sql = (
            f"INSERT INTO {self.schema}.{table} ({col_str}) "
            f"VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        )
        try:
            cur = self.conn.cursor()
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=self.batch_size)
            self.conn.commit()
            self._counts[table] += len(records)
            cur.close()
        except Exception as exc:
            self.conn.rollback()
            self._errors[table] += len(records)
            logger.error(f"Batch write error for {table}: {exc}")
            self._flush_one_by_one(table, sql, rows)

        self._buffers[table] = []

    def _flush_one_by_one(self, table: str, sql: str, rows: list) -> None:
        cur      = self.conn.cursor()
        salvaged = 0
        for row in rows:
            try:
                cur.execute(sql, row)
                self.conn.commit()
                salvaged          += 1
                self._counts[table] += 1
                self._errors[table] -= 1
            except Exception:
                self.conn.rollback()
        if salvaged:
            logger.info(f"  Salvaged {salvaged}/{len(rows)} rows for {table}")
        cur.close()

    def flush_all(self) -> None:
        for table in list(self._buffers.keys()):
            self._flush(table)

    # ------------------------------------------------------------------
    # Immediate single-record write
    # ------------------------------------------------------------------

    def write_immediate(self, table: str, record: Dict[str, Any]) -> bool:
        if not record:
            return False
        if self.dry_run:
            self._counts[table] += 1
            return True

        cols         = self._get_table_columns(table)
        if not cols:
            return False
        sanitized    = self._sanitize_record(table, record)
        col_str      = ", ".join(cols)
        placeholders = ", ".join(["%s"] * len(cols))
        sql = (
            f"INSERT INTO {self.schema}.{table} ({col_str}) "
            f"VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        )
        row = [sanitized.get(c) for c in cols]
        try:
            cur = self.conn.cursor()
            cur.execute(sql, row)
            self.conn.commit()
            self._counts[table] += 1
            cur.close()
            return True
        except Exception as exc:
            self.conn.rollback()
            self._errors[table] += 1
            logger.error(f"Write error for {table}: {exc}")
            return False

    # ------------------------------------------------------------------
    # Person upsert
    # ------------------------------------------------------------------

    def upsert_person(self, record: Dict[str, Any]) -> bool:
        if not record:
            return False
        if self.dry_run:
            self._counts["person"] += 1
            return True

        cols    = self._get_table_columns("person")
        pk_cols = self._get_primary_key("person")
        if not cols or not pk_cols:
            logger.error("Cannot upsert person: schema metadata missing")
            return False

        update_cols     = [c for c in cols if c not in pk_cols]
        col_str         = ", ".join(cols)
        placeholders    = ", ".join(["%s"] * len(cols))
        conflict_target = ", ".join(pk_cols)
        update_str      = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

        sql = (
            f"INSERT INTO {self.schema}.person ({col_str}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_target}) DO UPDATE SET {update_str}"
        )
        sanitized = self._sanitize_record("person", record)
        row       = [sanitized.get(c) for c in cols]

        try:
            cur = self.conn.cursor()
            cur.execute(sql, row)
            self.conn.commit()
            self._counts["person"] += 1
            cur.close()
            return True
        except Exception as exc:
            self.conn.rollback()
            self._errors["person"] += 1
            logger.error(f"Person upsert error: {exc}")
            return False

    # ------------------------------------------------------------------
    # Observation period
    # Note: period_type_concept_id has NO default — caller must supply it.
    # ------------------------------------------------------------------

    def write_observation_period(
        self,
        person_id:                 int,
        start_date,
        end_date,
        period_type_concept_id:    int,
        observation_period_id:     Optional[int] = None,
    ) -> None:
        self.write_immediate(
            "observation_period",
            {
                "observation_period_id":         observation_period_id,
                "person_id":                     person_id,
                "observation_period_start_date": start_date,
                "observation_period_end_date":   end_date,
                "period_type_concept_id":        period_type_concept_id,
            },
        )

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def stats(self) -> Dict[str, Dict]:
        return {
            table: {"written": self._counts[table], "errors": self._errors[table]}
            for table in set(list(self._counts) + list(self._errors))
        }

    def print_summary(self) -> None:
        print("\n" + "=" * 50)
        print("OMOP ETL WRITE SUMMARY")
        print("=" * 50)
        total_records = total_errors = 0
        for table, s in sorted(self.stats().items()):
            total_records += s["written"]
            total_errors  += s["errors"]
            status         = "✓" if s["errors"] == 0 else "⚠"
            print(f"  {status} {table:<35} {s['written']:>8,} records  {s['errors']:>5} errors")
        print("-" * 50)
        print(f"  TOTAL: {total_records:,} records, {total_errors} errors")
        if self.dry_run:
            print("  [DRY RUN — nothing was actually written]")
        print("=" * 50 + "\n")