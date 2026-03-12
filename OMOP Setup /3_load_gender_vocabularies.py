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
    """Load a CSV into a temporary staging table."""
    copy_sql = (
        f"\\COPY {stage_table} FROM '{csv_path}' "
        f"WITH DELIMITER E'\\t' CSV HEADER QUOTE E'\\b';"
    )
    cmd = ["docker", "exec", container,
           "psql", "-U", user, "-d", db, "-c", copy_sql]
    print(f"  → Staging {stage_table} from {csv_path} …", end=" ", flush=True)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        count = result.stdout.strip().replace("COPY", "").strip()
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
    run(
        ["docker", "cp", str(local_dir), f"{container}:{gender_container}"],
        "docker cp gender_vocabulary folder"
    )
    print("  ✅ Gender vocabulary folder copied.")

    # Step 4: Disable FK constraints
    print(f"\n[STEP 4] Disabling FK constraints…")
    psql(container, user, db,
         "SET session_replication_role = replica;",
         "Disable FK constraints")
    print("  ✅ FK constraints disabled.")

    # Step 5: Create temp staging tables
    print(f"\n[STEP 5] Creating temporary staging tables…")
    for _, stage_table, _ in TABLE_CONFIG:
        real_table = stage_table.replace("stage_", "")
        psql(container, user, db,
             f"CREATE TEMP TABLE {stage_table} (LIKE {real_table});",
             f"CREATE TEMP TABLE {stage_table}")
    print("  ✅ All staging tables created.")

    # Step 6: Load gender CSVs into staging tables
    print(f"\n[STEP 6] Loading gender CSVs into staging tables…")
    staged = set()
    for real_table, stage_table, csv_file in TABLE_CONFIG:
        if csv_file not in found:
            print(f"  ⚠️  Skipping {stage_table} — {csv_file} not found.")
            continue
        container_csv = f"{gender_container}/{csv_file}"
        ok = psql_copy_stage(container, user, db, stage_table, container_csv)
        if ok:
            staged.add(real_table)

    # Step 7: Merge staged rows into real tables (INSERT … ON CONFLICT DO NOTHING)
    print(f"\n[STEP 7] Merging staged data into OMOP tables (no overwrites)…")
    for real_table, stage_table, _ in TABLE_CONFIG:
        if real_table not in staged:
            print(f"  ⚠️  Skipping merge for {real_table} — nothing staged.")
            continue
        merge_sql = (
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

    # Step 8: Re-enable FK constraints
    print(f"\n[STEP 8] Re-enabling FK constraints…")
    psql(container, user, db,
         "SET session_replication_role = DEFAULT;",
         "Re-enable FK constraints")
    print("  ✅ FK constraints re-enabled.")

    # Step 9: Verify gender concepts exist
    print(f"\n[STEP 9] Verifying gender concepts in OMOP…")
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
    if "0 rows" in result.stdout or result.stdout.strip() == "":
        print("  ⚠️  No gender concepts found. Check your CSV files.")
    else:
        print("  ✅ Gender concepts verified.")

    print("\n" + "=" * 60)
    print("  ✅  Gender vocabulary load complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
