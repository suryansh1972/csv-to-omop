"""
cohort_filter_api.py - Flask API for concept-ID based cohort filtering.

Endpoints:
  GET  /                          → Serve the HTML UI
  GET  /api/concept-lookup        → Look up a concept by ID
  GET  /api/concept-search        → Search concepts by name
  POST /api/filter-cohort         → Filter patients by concept IDs (OR), export DuckDB
  GET  /api/download/<filename>   → Download the generated DuckDB file
  GET  /api/ping                  → Health check
"""
import logging
import os
import tempfile
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request, send_file, abort

# Support running as a script (python omop_etl/cohort_filter_api.py)
# or as a module (python -m omop_etl.cohort_filter_api).
try:
    from .duckdb_exporter import DuckDBExporter, PERSON_SCOPED_TABLES
except Exception:
    from duckdb_exporter import DuckDBExporter, PERSON_SCOPED_TABLES

# ---------------------------------------------------------------------------
# Logging — set up FIRST so config-load messages appear
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DB config — use exactly the same source as pipeline.py (config.settings)
# Falls back to env vars only if that import fails.
# ---------------------------------------------------------------------------
DB_SCHEMA = "public"

try:
    try:
        from .config.settings import ETLConfig as _ETLConfig
    except Exception:
        from config.settings import ETLConfig as _ETLConfig
    _cfg = _ETLConfig()
    _dsn_string = _cfg.db.dsn()          # e.g. "host=... dbname=... user=... password=..."
    DB_SCHEMA   = getattr(_cfg.db, "schema", "public")
    logger.info(f"DB config loaded from config.settings  schema={DB_SCHEMA}")
    _USE_CONFIG_DSN = True
except Exception as _cfg_err:
    logger.warning(f"config.settings unavailable ({_cfg_err}) — falling back to env vars")
    _dsn_string = None
    _USE_CONFIG_DSN = False
    DB_HOST     = os.environ.get("OMOP_DB_HOST",     "localhost")
    DB_PORT     = int(os.environ.get("OMOP_DB_PORT", "5432"))
    DB_NAME     = os.environ.get("OMOP_DB_NAME",     "omop")
    DB_USER     = os.environ.get("OMOP_DB_USER",     "postgres")
    DB_PASSWORD = os.environ.get("OMOP_DB_PASSWORD", "")
    DB_SCHEMA   = os.environ.get("OMOP_DB_SCHEMA",   "public")


def _get_conn():
    """Open a psycopg2 connection using the same DSN as pipeline.py."""
    if _USE_CONFIG_DSN:
        # Re-read config each time so any credential rotation is picked up
        try:
            from .config.settings import ETLConfig
        except Exception:
            from config.settings import ETLConfig
        return psycopg2.connect(ETLConfig().db.dsn())
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )


# ---------------------------------------------------------------------------
# Output dir — where DuckDB files are written
# ---------------------------------------------------------------------------
OUTPUT_DIR = os.environ.get("OMOP_OUTPUT_DIR", tempfile.gettempdir())

# ---------------------------------------------------------------------------
# HTML path — resolve relative to THIS file so it works from any cwd
# ---------------------------------------------------------------------------
_HERE    = os.path.dirname(os.path.abspath(__file__))
UI_HTML  = os.path.join(_HERE, "cohort_filter.html")

# ---------------------------------------------------------------------------
# Domain → OMOP table (source of truth: config.settings.DomainConfig if present)
# Table → concept-id column (derived with small exception map)
# ---------------------------------------------------------------------------
try:
    try:
        from .config.settings import ETLConfig as _ETLConfig2
    except Exception:
        from config.settings import ETLConfig as _ETLConfig2
    DOMAIN_TO_TABLE: Dict[str, str] = _ETLConfig2().domains.domain_to_table
except Exception as _dom_err:
    logger.warning(f"DomainConfig unavailable ({_dom_err}) — using default mapping")
    DOMAIN_TO_TABLE = {
        "Condition":   "condition_occurrence",
        "Measurement": "measurement",
        "Observation": "observation",
        "Drug":        "drug_exposure",
        "Visit":       "visit_occurrence",
        "Device":      "device_exposure",
        "Procedure":   "procedure_occurrence",
        "Specimen":    "specimen",
        "Death":       "death",
        "Note":        "note",
    }

_TABLE_CONCEPT_EXCEPTIONS: Dict[str, str] = {
    "note":  "note_type_concept_id",
    "death": "cause_concept_id",
}

def _concept_col_for_table(table: str) -> Optional[str]:
    if table in _TABLE_CONCEPT_EXCEPTIONS:
        return _TABLE_CONCEPT_EXCEPTIONS[table]
    base = table.split("_")[0]
    return f"{base}_concept_id" if base else None

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__)

# Add CORS headers to every response so the UI works whether it is opened
# directly as a file:// OR served from a different port during development.
@app.after_request
def _add_cors(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response

@app.route("/api/concept-lookup", methods=["OPTIONS"])
@app.route("/api/concept-search", methods=["OPTIONS"])
@app.route("/api/filter-cohort",  methods=["OPTIONS"])
def _options():
    return "", 204

# In-memory registry: filename → absolute path on disk
_generated_files: Dict[str, str] = {}
_files_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Tiny helpers
# ---------------------------------------------------------------------------

def _fetch_one(conn, sql: str, params: tuple) -> Optional[tuple]:
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return cur.fetchone()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def _fetch_all(conn, sql: str, params: tuple) -> List[tuple]:
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def _fetch_table_as_dicts(
    conn, table: str, schema: str, person_ids: List[int]
) -> List[Dict]:
    """Fetch all rows from a person-scoped table for the given person_ids."""
    if not person_ids:
        return []

    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
            (schema, table),
        )
        columns = [r[0] for r in cur.fetchall()]
    except Exception as exc:
        conn.rollback()
        logger.warning(f"Cannot read columns for {table}: {exc}")
        cur.close()
        return []

    if not columns or "person_id" not in columns:
        cur.close()
        return []

    placeholders = ",".join(["%s"] * len(person_ids))
    col_str      = ", ".join(columns)
    try:
        cur.execute(
            f"SELECT {col_str} FROM {schema}.{table} WHERE person_id IN ({placeholders})",
            tuple(person_ids),
        )
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        cur.close()
        return rows
    except Exception as exc:
        conn.rollback()
        logger.warning(f"Error fetching {table}: {exc}")
        cur.close()
        return []


def _fetch_concepts(conn, schema: str, concept_ids: Set[int]) -> List[Dict]:
    if not concept_ids:
        return []
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name='concept' ORDER BY ordinal_position",
            (schema,),
        )
        columns = [r[0] for r in cur.fetchall()]
        if not columns:
            return []
        placeholders = ",".join(["%s"] * len(concept_ids))
        cur.execute(
            f"SELECT {', '.join(columns)} FROM {schema}.concept "
            f"WHERE concept_id IN ({placeholders})",
            tuple(concept_ids),
        )
        rows = [dict(zip(columns, r)) for r in cur.fetchall()]
        cur.close()
        return rows
    except Exception as exc:
        conn.rollback()
        logger.warning(f"Error fetching concepts: {exc}")
        cur.close()
        return []


def _fetch_concept_relationships(
    conn, schema: str, concept_ids: Set[int]
) -> List[Dict]:
    if not concept_ids:
        return []
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name='concept_relationship' ORDER BY ordinal_position",
            (schema,),
        )
        columns = [r[0] for r in cur.fetchall()]
        if not columns:
            return []
        id_tuple     = tuple(concept_ids)
        placeholders = ",".join(["%s"] * len(id_tuple))
        cur.execute(
            f"SELECT {', '.join(columns)} FROM {schema}.concept_relationship "
            f"WHERE concept_id_1 IN ({placeholders}) OR concept_id_2 IN ({placeholders})",
            id_tuple + id_tuple,
        )
        rows = [dict(zip(columns, r)) for r in cur.fetchall()]
        cur.close()
        return rows
    except Exception as exc:
        conn.rollback()
        logger.warning(f"Error fetching concept_relationships: {exc}")
        cur.close()
        return []


# ---------------------------------------------------------------------------
# Core business logic
# ---------------------------------------------------------------------------

def _resolve_concept(conn, schema: str, concept_id: int) -> Optional[Dict]:
    try:
        row = _fetch_one(
            conn,
            f"SELECT concept_id, concept_name, domain_id, vocabulary_id, "
            f"concept_class_id, standard_concept "
            f"FROM {schema}.concept WHERE concept_id = %s",
            (concept_id,),
        )
        if not row:
            return None
        return {
            "concept_id":    row[0],
            "concept_name":  row[1],
            "domain_id":     row[2],
            "vocabulary_id": row[3],
            "concept_class": row[4],
            "standard":      row[5],
        }
    except Exception as exc:
        logger.warning(f"Could not resolve concept {concept_id}: {exc}")
        return None


def _person_ids_for_concept(
    conn, schema: str, concept_id: int, domain_id: str
) -> Set[int]:
    target_table = DOMAIN_TO_TABLE.get(domain_id, "observation")
    concept_col  = _concept_col_for_table(target_table)
    if not concept_col:
        return set()
    try:
        rows = _fetch_all(
            conn,
            f"SELECT DISTINCT person_id FROM {schema}.{target_table} "
            f"WHERE {concept_col} = %s",
            (concept_id,),
        )
        return {r[0] for r in rows if r[0]}
    except Exception as exc:
        logger.warning(f"Error finding persons for concept {concept_id}: {exc}")
        return set()


def _run_filter(concept_ids: List[int], match_mode: str = "any") -> Tuple[Dict[str, List], Set[int]]:
    conn   = _get_conn()
    schema = DB_SCHEMA
    try:
        per_concept_persons: List[Set[int]] = []
        for cid in concept_ids:
            concept = _resolve_concept(conn, schema, cid)
            if not concept:
                logger.warning(f"Concept {cid} not found")
                continue
            pids = _person_ids_for_concept(conn, schema, cid, concept["domain_id"])
            logger.info(f"  Concept {cid} ({concept['concept_name']}): {len(pids)} persons")
            per_concept_persons.append(pids)

        if not per_concept_persons:
            return {}, set()

        if match_mode == "all":
            all_person_ids = set.intersection(*per_concept_persons) if per_concept_persons else set()
        else:
            all_person_ids = set.union(*per_concept_persons) if per_concept_persons else set()

        if not all_person_ids:
            return {}, set()

        logger.info(f"Total unique persons ({match_mode}): {len(all_person_ids)}")
        pid_list = list(all_person_ids)

        data: Dict[str, List[Dict]] = {}
        all_concept_ids_used: Set[int] = set()

        for table in PERSON_SCOPED_TABLES:
            rows = _fetch_table_as_dicts(conn, table, schema, pid_list)
            if rows:
                data[table] = rows
                for row in rows:
                    for col, val in row.items():
                        if col.endswith("_concept_id") and isinstance(val, int) and val > 0:
                            all_concept_ids_used.add(val)
                logger.info(f"  {table}: {len(rows)} rows")

        if all_concept_ids_used:
            concept_rows = _fetch_concepts(conn, schema, all_concept_ids_used)
            if concept_rows:
                data["concept"] = concept_rows
            rel_rows = _fetch_concept_relationships(conn, schema, all_concept_ids_used)
            if rel_rows:
                data["concept_relationship"] = rel_rows

        return data, all_person_ids
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    if os.path.exists(UI_HTML):
        with open(UI_HTML, "r", encoding="utf-8") as f:
            return f.read(), 200, {"Content-Type": "text/html; charset=utf-8"}
    return (
        "<h1>cohort_filter.html not found</h1>"
        f"<p>Expected: <code>{UI_HTML}</code></p>",
        404,
    )


@app.route("/api/ping")
def ping():
    """Health check — also tests the DB connection."""
    try:
        conn = _get_conn()
        cur  = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.concept LIMIT 1")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return jsonify({"status": "ok", "concept_rows": count})
    except Exception as exc:
        logger.error(f"Ping DB error: {exc}")
        return jsonify({"status": "error", "detail": str(exc)}), 500


@app.route("/api/concept-lookup")
def concept_lookup():
    concept_id_str = request.args.get("concept_id", "").strip()
    if not concept_id_str:
        return jsonify({"error": "concept_id parameter required"}), 400
    try:
        concept_id = int(concept_id_str)
    except ValueError:
        return jsonify({"error": "concept_id must be an integer"}), 400
    try:
        conn    = _get_conn()
        concept = _resolve_concept(conn, DB_SCHEMA, concept_id)
        conn.close()
    except Exception as exc:
        logger.error(f"DB error in concept-lookup: {exc}")
        return jsonify({"error": f"Database error: {exc}"}), 500
    if not concept:
        return jsonify({"error": f"Concept {concept_id} not found"}), 404
    return jsonify(concept)

@app.route("/api/concept-search")
def concept_search():
    query = (request.args.get("q") or request.args.get("concept_name") or "").strip()
    if not query:
        return jsonify({"error": "q parameter required"}), 400
    try:
        limit = int(request.args.get("limit", 20))
    except ValueError:
        limit = 20
    limit = max(1, min(limit, 50))

    try:
        conn = _get_conn()
        cur  = conn.cursor()
        cur.execute(
            f"""
            SELECT concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept
            FROM {DB_SCHEMA}.concept
            WHERE invalid_reason IS NULL
              AND concept_name ILIKE %s
            ORDER BY
              CASE WHEN lower(concept_name) = lower(%s) THEN 0 ELSE 1 END,
              CASE WHEN standard_concept = 'S' THEN 0 ELSE 1 END,
              length(concept_name) ASC,
              concept_id ASC
            LIMIT %s
            """,
            (f"%{query}%", query, limit),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        results = [
            {
                "concept_id": r[0],
                "concept_name": r[1],
                "domain_id": r[2],
                "vocabulary_id": r[3],
                "concept_class": r[4],
                "standard": r[5],
            }
            for r in rows
        ]
        return jsonify({"query": query, "count": len(results), "results": results})
    except Exception as exc:
        logger.error(f"DB error in concept-search: {exc}")
        return jsonify({"error": f"Database error: {exc}"}), 500


@app.route("/api/filter-cohort", methods=["POST"])
def filter_cohort():
    body        = request.get_json(force=True, silent=True) or {}
    concept_ids = body.get("concept_ids", [])
    if not concept_ids:
        return jsonify({"error": "concept_ids list required"}), 400
    try:
        concept_ids = [int(c) for c in concept_ids]
    except (ValueError, TypeError):
        return jsonify({"error": "All concept_ids must be integers"}), 400

    logger.info(f"Filter request: {concept_ids}  mode=any")
    try:
        data, person_ids = _run_filter(concept_ids, match_mode="any")
    except Exception as exc:
        logger.error(f"Filter failed: {exc}", exc_info=True)
        return jsonify({"error": str(exc)}), 500

    if not data:
        return jsonify({"person_count": 0, "table_counts": {},
                        "message": "No patients matched."})

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename  = f"omop_filter_{timestamp}.duckdb"
    out_path  = os.path.join(OUTPUT_DIR, filename)
    try:
        DuckDBExporter().export(data, out_path)
    except Exception as exc:
        logger.error(f"DuckDB export failed: {exc}", exc_info=True)
        return jsonify({"error": f"DuckDB export failed: {exc}"}), 500

    with _files_lock:
        _generated_files[filename] = out_path

    table_counts = {t: len(r) for t, r in data.items()}
    return jsonify({
        "filename":     filename,
        "person_count": len(person_ids),
        "table_counts": table_counts,
        "download_url": f"/api/download/{filename}",
        "total_rows":   sum(table_counts.values()),
    })


@app.route("/api/download/<path:filename>")
def download_file(filename: str):
    with _files_lock:
        file_path = _generated_files.get(filename)
    if not file_path or not os.path.exists(file_path):
        abort(404, description="File not found or expired")
    return send_file(
        file_path,
        as_attachment=True,
        download_name=filename,
        mimetype="application/octet-stream",
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info("=" * 60)
    logger.info("OMOP Cohort Filter API")
    logger.info(f"  UI  →  http://localhost:5050")
    logger.info(f"  Ping → http://localhost:5050/api/ping  (DB health check)")
    logger.info(f"  Schema : {DB_SCHEMA}")
    logger.info(f"  Output : {OUTPUT_DIR}")
    logger.info(f"  HTML   : {UI_HTML}  (exists={os.path.exists(UI_HTML)})")
    logger.info("=" * 60)
    app.run(host="0.0.0.0", port=5050, debug=False, use_reloader=False)
