#!/usr/bin/env python3
"""
Script 2: load_snomed_vocabularies.py
Copies the Athena vocabulary download (SNOMED + others) into the Docker
container and bulk-loads all CSV files into the OMOP CDM vocabulary tables.
Reads all paths from config.json.
"""

import subprocess
import sys
import json
from pathlib import Path


# ── Load config ────────────────────────────────────────────────────────────────

def load_config(config_path: str = "config.json") -> dict:
    config_file = Path(config_path)
    if not config_file.exists():
        print(f"[ERROR] config.json not found at: {config_file.resolve()}")
        sys.exit(1)
    with open(config_file) as f:
        return json.load(f)


# ── Helpers ────────────────────────────────────────────────────────────────────

def run(cmd: list[str], description: str) -> subprocess.CompletedProcess:
    print(f"\n[RUN] {description}")
    print(f"      $ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.returncode != 0:
        print(f"[ERROR] {result.stderr.strip()}")
        sys.exit(1)
    return result


def psql_exec(container: str, user: str, db: str,
              sql: str, description: str, allow_fail: bool = False):
    """Execute a SQL string inside the container."""
    cmd = ["docker", "exec", container, "psql", "-U", user, "-d", db, "-c", sql]
    print(f"\n[SQL] {description}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.returncode != 0 and not allow_fail:
        print(f"[ERROR] {result.stderr.strip()}")
        sys.exit(1)
    return result


def psql_copy(container: str, user: str, db: str,
              table: str, csv_path: str) -> bool:
    """Run a \\COPY command for a vocabulary CSV file."""
    copy_sql = (
        f"\\COPY {table} FROM '{csv_path}' "
        f"WITH DELIMITER E'\\t' CSV HEADER QUOTE E'\\b';"
    )
    cmd = ["docker", "exec", container,
           "psql", "-U", user, "-d", db, "-c", copy_sql]
    print(f"  → Loading {table} from {csv_path} …", end=" ", flush=True)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        # Extract row count from "COPY N" message
        output = result.stdout.strip()
        count = output.replace("COPY", "").strip()
        print(f"✅  {count} rows")
        return True
    else:
        print(f"❌")
        print(f"    [ERROR] {result.stderr.strip()}")
        return False


# ── Table → CSV mapping (order respects FK dependencies) ─────────────────────

TABLE_CSV_MAP = [
    ("vocabulary",          "VOCABULARY.csv"),
    ("domain",              "DOMAIN.csv"),
    ("concept_class",       "CONCEPT_CLASS.csv"),
    ("relationship",        "RELATIONSHIP.csv"),
    ("concept",             "CONCEPT.csv"),
    ("concept_synonym",     "CONCEPT_SYNONYM.csv"),
    ("concept_relationship","CONCEPT_RELATIONSHIP.csv"),
    ("concept_ancestor",    "CONCEPT_ANCESTOR.csv"),
    ("drug_strength",       "DRUG_STRENGTH.csv"),
]


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    cfg = load_config()

    container      = cfg["postgres"]["container_name"]
    user           = cfg["postgres"]["user"]
    db             = cfg["postgres"]["database"]
    vocab_local    = cfg["paths"]["vocabulary_local"]
    vocab_container= cfg["paths"]["vocabulary_container"]
    vocab_files    = cfg["vocabulary_files"]

    print("=" * 60)
    print("  OMOP CDM 5.4 — SNOMED Vocabulary Load")
    print("=" * 60)

    # Step 1: Verify container is running
    print("\n[STEP 1] Checking Docker container…")
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", container],
        capture_output=True, text=True
    )
    if result.returncode != 0 or result.stdout.strip() != "true":
        print(f"[ERROR] Container '{container}' is not running.")
        sys.exit(1)
    print(f"  ✅ Container '{container}' is running.")

    # Step 2: Validate local vocabulary directory
    print(f"\n[STEP 2] Validating local vocabulary directory…")
    local_dir = Path(vocab_local)
    if not local_dir.exists():
        print(f"[ERROR] Vocabulary path not found: {local_dir}")
        print("        Update 'paths.vocabulary_local' in config.json.")
        sys.exit(1)
    missing = [f for f in vocab_files if not (local_dir / f).exists()]
    if missing:
        print(f"[WARNING] These vocabulary CSVs were not found locally: {missing}")
        print("          They will be skipped during load.")
    found = [f for f in vocab_files if (local_dir / f).exists()]
    print(f"  ✅ {len(found)}/{len(vocab_files)} vocabulary CSVs found.")

    # Step 3: Copy vocabulary folder into container
    print(f"\n[STEP 3] Copying vocabulary folder → container:{vocab_container} …")
    run(
        ["docker", "cp", str(local_dir), f"{container}:{vocab_container}"],
        "docker cp vocabulary folder"
    )
    print(f"  ✅ Vocabulary folder copied.")

    # Step 4: Disable FK constraints
    print(f"\n[STEP 4] Disabling FK constraints (session_replication_role = replica)…")
    psql_exec(container, user, db,
              "SET session_replication_role = replica;",
              "Disable FK constraints")
    print("  ✅ FK constraints disabled.")

    # Step 5: Load each vocabulary CSV
    print(f"\n[STEP 5] Loading vocabulary CSVs into OMOP tables…")
    failures = []
    for table, csv_file in TABLE_CSV_MAP:
        if csv_file not in found:
            print(f"  ⚠️  Skipping {table} — {csv_file} not found locally.")
            continue
        container_csv = f"{vocab_container}/{csv_file}"
        ok = psql_copy(container, user, db, table, container_csv)
        if not ok:
            failures.append((table, csv_file))

    # Step 6: Re-enable FK constraints
    print(f"\n[STEP 6] Re-enabling FK constraints…")
    psql_exec(container, user, db,
              "SET session_replication_role = DEFAULT;",
              "Re-enable FK constraints")
    print("  ✅ FK constraints re-enabled.")

    # Step 7: Verify row counts
    print(f"\n[STEP 7] Verifying vocabulary row counts…")
    for table, _ in TABLE_CSV_MAP:
        result = subprocess.run(
            ["docker", "exec", container,
             "psql", "-U", user, "-d", db,
             "-t", "-c", f"SELECT COUNT(*) FROM {table};"],
            capture_output=True, text=True
        )
        count = result.stdout.strip()
        print(f"  {table:<30} {count:>10} rows")

    # Summary
    print("\n" + "=" * 60)
    if failures:
        print(f"  ⚠️  Completed with {len(failures)} failure(s):")
        for table, csv_file in failures:
            print(f"      - {table} ({csv_file})")
    else:
        print("  ✅  All vocabulary tables loaded successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
