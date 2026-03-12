"""
vocab_loader.py - Bulk-loads Athena vocabulary CSVs into OMOP CDM.

All table names, filenames, and load order are driven by VocabConfig.
Nothing is hardcoded in this file.
"""
import csv
import logging
import os
from pathlib import Path
from typing import List, Optional

from config.settings import VocabConfig

logger = logging.getLogger(__name__)


def _find_vocab_file(vocab_dir: str, candidates: List[str]) -> Optional[str]:
    """Find a vocab CSV in a directory by trying candidate names case-insensitively."""
    d        = Path(vocab_dir)
    all_files = {f.name.lower(): f for f in d.iterdir() if f.is_file()}
    for c in candidates:
        match = all_files.get(c.lower())
        if match:
            return str(match)
    return None


def _count_csv_rows(path: str) -> int:
    with open(path, "r", encoding="utf-8-sig") as f:
        return sum(1 for _ in f) - 1


def load_vocabulary(
    vocab_dir:  str,
    conn,
    schema:     str           = "public",
    tables:     Optional[List[str]] = None,
    cfg:        Optional[VocabConfig] = None,
) -> None:
    """
    Load Athena vocabulary CSVs into OMOP CDM tables.

    Args:
        vocab_dir: Path to Athena vocab directory.
        conn:      psycopg2 connection.
        schema:    OMOP schema name.
        tables:    Specific tables to load (default: all, in FK-safe order).
        cfg:       VocabConfig (defaults to VocabConfig()).
    """
    if cfg is None:
        cfg = VocabConfig()

    if not os.path.isdir(vocab_dir):
        raise FileNotFoundError(f"Vocabulary directory not found: {vocab_dir}")

    # Respect FK-safe load order, then append any caller-requested extras
    ordered = [t for t in cfg.load_order if t in (tables or cfg.load_order)]
    if tables:
        extras = [t for t in tables if t not in ordered]
        ordered.extend(extras)

    for table in ordered:
        candidates = cfg.table_to_filenames.get(table, [f"{table.upper()}.csv"])
        csv_path   = _find_vocab_file(vocab_dir, candidates)

        if not csv_path:
            logger.warning(f"No CSV found for table '{table}' in {vocab_dir}")
            continue

        n_rows = _count_csv_rows(csv_path)
        logger.info(f"Loading {table}: {csv_path} ({n_rows:,} rows)")

        try:
            cur = conn.cursor()
            cur.execute(f"TRUNCATE TABLE {schema}.{table} CASCADE")
            with open(csv_path, "r", encoding="utf-8-sig") as f:
                f.readline()  # skip header
                cur.copy_expert(
                    f"COPY {schema}.{table} FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', QUOTE '\"', NULL '')",
                    f,
                )
            conn.commit()
            logger.info(f"  ✓ {table} loaded")
            cur.close()
        except Exception as exc:
            conn.rollback()
            logger.error(f"  ✗ Failed to load {table}: {exc}")
            try:
                _load_csv_fallback(conn, schema, table, csv_path, cfg)
            except Exception as exc2:
                logger.error(f"  ✗ Fallback also failed for {table}: {exc2}")


def _load_csv_fallback(
    conn, schema: str, table: str, csv_path: str, cfg: VocabConfig
) -> None:
    """Detect delimiter and insert row-by-row when COPY fails."""
    cur = conn.cursor()
    with open(csv_path, "r", encoding="utf-8-sig", errors="replace") as f:
        sample  = f.read(4096)
        f.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters="\t,|")
        reader  = csv.DictReader(f, dialect=dialect)
        headers = reader.fieldnames

        if not headers:
            raise ValueError(f"No headers in {csv_path}")

        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        db_cols = [r[0] for r in cur.fetchall()]
        col_map = {h: dc for h in headers for dc in db_cols if h.lower() == dc.lower()}

        if not col_map:
            raise ValueError(f"No matching columns between CSV and DB for {table}")

        cols_str     = ", ".join(col_map.values())
        placeholders = ", ".join(["%s"] * len(col_map))
        sql          = (
            f"INSERT INTO {schema}.{table} ({cols_str}) "
            f"VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        )
        batch: list = []
        for row in reader:
            batch.append([row.get(h) or None for h in col_map])
            if len(batch) >= cfg.fallback_batch_size:
                cur.executemany(sql, batch)
                batch = []
        if batch:
            cur.executemany(sql, batch)
        conn.commit()
        logger.info(f"  ✓ {table} loaded via fallback CSV reader")
    cur.close()