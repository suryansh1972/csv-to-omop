#!/usr/bin/env python3
"""
Script 3: load_gender_vocabularies.py
Safely merges gender vocabulary CSVs into the OMOP CDM tables using a
staging-table upsert pattern (INSERT … ON CONFLICT DO NOTHING) so that
existing rows are never overwritten.
Reads all paths from config.json.
"""

import subprocess
import sys
import json
import re
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


def psql(container: str, user: str, db: str,
         sql: str, description: str = "", allow_fail: bool = False):
    """Run SQL inside the container and return the CompletedProcess."""
    if description:
        print(f"  [SQL] {description}")
    cmd = ["docker", "exec", container,
           "psql", "-U", user, "-d", db, "-c", sql]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout.strip():
        print("        " + result.stdout.strip())
    if result.returncode != 0 and not allow_fail:
        print(f"        [ERROR] {result.stderr.strip()}")
        sys.exit(1)
    return result


def psql_copy_stage(container: str, user: str, db: str,
                    stage_table: str, csv_path: str) -> bool:
    """Load a CSV into a temporary staging table.

    session_replication_role = replica is set in the SAME psql session
    so FK constraints are bypassed.
    """
    copy_sql = (
        f"\\COPY {stage_table} FROM '{csv_path}' "
        f"WITH DELIMITER E'\\t' CSV HEADER QUOTE E'\\b';"
    )
    cmd = [
        "docker", "exec", container,
        "psql", "-U", user, "-d", db,
        "-c", "SET session_replication_role = replica;",
        "-c", copy_sql,
    ]
    print(f"  → Staging {stage_table} from {csv_path} …", end=" ", flush=True)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        lines = [l for l in result.stdout.strip().split('\n') if l.startswith('COPY')]
        count = lines[-1].replace('COPY', '').strip() if lines else '?'
        print(f"✅  {count} rows staged")
        return True
    else:
        print("❌")
        print(f"    [WARN] {result.stderr.strip()}")
        return False


# ── Table config (stage → real, ordered by FK dependency) ─────────────────────

# (real_table, stage_table, gender_csv_filename)
TABLE_CONFIG = [
    ("vocabulary",          "stage_vocabulary",          "gender_VOCABULARY.csv"),
    ("domain",              "stage_domain",              "gender_DOMAIN.csv"),
    ("concept_class",       "stage_concept_class",       "gender_CONCEPT_CLASS.csv"),
    ("relationship",        "stage_relationship",        "gender_RELATIONSHIP.csv"),
    ("concept",             "stage_concept",             "gender_CONCEPT.csv"),
    ("concept_relationship","stage_concept_relationship","gender_CONCEPT_RELATIONSHIP.csv"),
    ("concept_synonym",     "stage_concept_synonym",     "gender_CONCEPT_SYNONYM.csv"),
    ("concept_ancestor",    "stage_concept_ancestor",    "gender_CONCEPT_ANCESTOR.csv"),
]


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    cfg = load_config()

    container         = cfg["postgres"]["container_name"]
    user              = cfg["postgres"]["user"]
    db                = cfg["postgres"]["database"]
    gender_local      = cfg["paths"]["gender_vocab_local"]
    gender_container  = cfg["paths"]["gender_vocab_container"]
    gender_files      = cfg["gender_vocab_files"]

    print("=" * 60)
    print("  OMOP CDM 5.4 — Gender Vocabulary Load (Safe Upsert)")
    print("=" * 60)

    # Step 1: Verify container
    print("\n[STEP 1] Checking Docker container…")
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", container],
        capture_output=True, text=True
    )
    if result.returncode != 0 or result.stdout.strip() != "true":
        print(f"[ERROR] Container '{container}' is not running.")
        sys.exit(1)
    print(f"  ✅ Container '{container}' is running.")

    # Step 2: Validate local gender vocabulary directory
    print(f"\n[STEP 2] Validating local gender vocabulary directory…")
    local_dir = Path(gender_local)
    if not local_dir.exists():
        print(f"[ERROR] Gender vocab path not found: {local_dir}")
        print("        Update 'paths.gender_vocab_local' in config.json.")
        sys.exit(1)
    missing = [f for f in gender_files if not (local_dir / f).exists()]
    if missing:
        print(f"[WARNING] These gender CSV files are missing locally: {missing}")
    found = [f for f in gender_files if (local_dir / f).exists()]
    print(f"  ✅ {len(found)}/{len(gender_files)} gender CSV files found.")

    # Step 3: Copy gender vocabulary folder into container
    print(f"\n[STEP 3] Copying gender vocab folder → container:{gender_container} …")
    # Remove any previous copy so docker-cp creates the dir fresh
    # (otherwise docker-cp nests the folder INSIDE the existing dir)
    subprocess.run(
        ["docker", "exec", container, "rm", "-rf", gender_container],
        capture_output=True, text=True,
    )
    # Copy *contents* so we don't end up with /gender_vocabulary/gender/...
    run(
        ["docker", "cp", f"{str(local_dir)}/.", f"{container}:{gender_container}"],
        "docker cp gender_vocabulary contents"
    )

    # Detect whether files landed in the expected path or a nested folder
    gender_container_effective = gender_container
    sample_file = gender_files[0] if gender_files else None
    if sample_file:
        def container_has_file(path: str) -> bool:
            return subprocess.run(
                ["docker", "exec", container, "sh", "-lc", f"test -f '{path}'"],
                capture_output=True, text=True,
            ).returncode == 0

        if not container_has_file(f"{gender_container}/{sample_file}"):
            nested = f"{gender_container}/gender"
            if container_has_file(f"{nested}/{sample_file}"):
                gender_container_effective = nested
                print(f"  ⚠️  Detected nested folder. Using: {gender_container_effective}")
            else:
                print("  [ERROR] Gender CSVs not found in container after copy.")
                sys.exit(1)

    # Verify files landed correctly
    result = subprocess.run(
        ["docker", "exec", container, "ls", "-1", gender_container_effective],
        capture_output=True, text=True,
    )
    print("  Files in container:")
    for f in result.stdout.strip().split("\n"):
        if f.strip():
            print(f"    {f}")
    print("  ✅ Gender vocabulary folder copied.")

    # Step 4: Create staging tables (UNLOGGED so they persist across sessions)
    print(f"\n[STEP 4] Creating staging tables…")
    for _, stage_table, _ in TABLE_CONFIG:
        real_table = stage_table.replace("stage_", "")
        psql(container, user, db,
             f"DROP TABLE IF EXISTS {stage_table};",
             f"DROP TABLE IF EXISTS {stage_table}")
        psql(container, user, db,
             f"CREATE UNLOGGED TABLE {stage_table} (LIKE {real_table});",
             f"CREATE UNLOGGED TABLE {stage_table}")
    print("  ✅ All staging tables created.")

    # Step 5: Load gender CSVs into staging tables
    print(f"\n[STEP 5] Loading gender CSVs into staging tables…")
    print(f"         (FK constraints bypassed per-session)")
    staged = set()
    for real_table, stage_table, csv_file in TABLE_CONFIG:
        if csv_file not in found:
            print(f"  ⚠️  Skipping {stage_table} — {csv_file} not found.")
            continue
        container_csv = f"{gender_container_effective}/{csv_file}"
        ok = psql_copy_stage(container, user, db, stage_table, container_csv)
        if ok:
            staged.add(real_table)

    # Step 6: Merge staged rows into real tables (INSERT … ON CONFLICT DO NOTHING)
    print(f"\n[STEP 6] Merging staged data into OMOP tables (no overwrites)…")
    for real_table, stage_table, _ in TABLE_CONFIG:
        if real_table not in staged:
            print(f"  ⚠️  Skipping merge for {real_table} — nothing staged.")
            continue
        merge_sql = (
            f"SET session_replication_role = replica; "
            f"INSERT INTO {real_table} "
            f"SELECT * FROM {stage_table} "
            f"ON CONFLICT DO NOTHING;"
        )
        result = psql(container, user, db, merge_sql,
                      f"Merge {stage_table} → {real_table}",
                      allow_fail=True)
        output = result.stdout.strip()
        inserted = output.replace("INSERT 0", "").strip() if output else "?"
        print(f"      {real_table:<30} {inserted:>6} new rows inserted")

    # Step 7: Verify gender concepts exist
    print(f"\n[STEP 7] Verifying gender concepts in OMOP…")
    verify_sql = (
        "SELECT concept_id, concept_name, vocabulary_id, domain_id "
        "FROM concept WHERE domain_id = 'Gender' LIMIT 20;"
    )
    result = subprocess.run(
        ["docker", "exec", container,
         "psql", "-U", user, "-d", db, "-c", verify_sql],
        capture_output=True, text=True
    )
    print(result.stdout)
    count_sql = "SELECT COUNT(*) FROM concept WHERE domain_id = 'Gender';"
    count_result = subprocess.run(
        ["docker", "exec", container,
         "psql", "-U", user, "-d", db, "-t", "-A", "-c", count_sql],
        capture_output=True, text=True
    )
    if count_result.returncode != 0:
        print("  ⚠️  Could not determine row count from verification query.")
    else:
        count_match = re.search(r"\\d+", count_result.stdout.strip())
        if not count_match:
            print("  ⚠️  Could not determine row count from verification query.")
        elif int(count_match.group(0)) == 0:
            print("  ⚠️  No gender concepts found. Check your CSV files.")
        else:
            print("  ✅ Gender concepts verified.")

    # Step 8: Drop staging tables
    print(f"\n[STEP 8] Dropping staging tables…")
    for _, stage_table, _ in TABLE_CONFIG:
        psql(container, user, db,
             f"DROP TABLE IF EXISTS {stage_table};",
             f"DROP TABLE IF EXISTS {stage_table}")
    print("  ✅ Staging tables dropped.")

    print("\n" + "=" * 60)
    print("  ✅  Gender vocabulary load complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
