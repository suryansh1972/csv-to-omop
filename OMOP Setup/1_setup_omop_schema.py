#!/usr/bin/env python3
"""
Script 1: setup_omop_schema.py
Sets up the OMOP CDM 5.4 schema by copying DDL files into the Docker container
and executing them against the PostgreSQL database.
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


def docker_exec_sql(container: str, user: str, db: str, sql: str, description: str):
    """Run a SQL string inside the container via psql -c."""
    cmd = [
        "docker", "exec", container,
        "psql", "-U", user, "-d", db, "-c", sql
    ]
    run(cmd, description)


def docker_exec_sql_file(container: str, user: str, db: str,
                         container_path: str, description: str):
    """Run a .sql file inside the container via psql -f."""
    cmd = [
        "docker", "exec", container,
        "psql", "-U", user, "-d", db, "-f", container_path
    ]
    run(cmd, description)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    cfg = load_config()

    container    = cfg["postgres"]["container_name"]
    user         = cfg["postgres"]["user"]
    db           = cfg["postgres"]["database"]
    ddl_local    = cfg["paths"]["ddl_local"]
    ddl_container= cfg["paths"]["ddl_container"]
    ddl_files    = cfg["ddl_files"]

    print("=" * 60)
    print("  OMOP CDM 5.4 — Schema Setup")
    print("=" * 60)

    # Step 1: Verify the Docker container is running
    print("\n[STEP 1] Checking Docker container is running…")
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", container],
        capture_output=True, text=True
    )
    if result.returncode != 0 or result.stdout.strip() != "true":
        print(f"[ERROR] Container '{container}' is not running.")
        print("        Start it with:  docker start " + container)
        sys.exit(1)
    print(f"  ✅ Container '{container}' is running.")

    # Step 2: Validate local DDL directory exists
    print("\n[STEP 2] Validating local DDL directory…")
    local_dir = Path(ddl_local)
    if not local_dir.exists():
        print(f"[ERROR] Local DDL path not found: {local_dir}")
        print("        Update 'paths.ddl_local' in config.json.")
        sys.exit(1)
    missing = [f for f in ddl_files if not (local_dir / f).exists()]
    if missing:
        print(f"[ERROR] Missing DDL files: {missing}")
        sys.exit(1)
    print(f"  ✅ All {len(ddl_files)} DDL files found in {local_dir}")

    # Step 3: Copy DDL folder into container
    print(f"\n[STEP 3] Copying DDL files → container:{ddl_container} …")
    run(
        ["docker", "cp", str(local_dir), f"{container}:{ddl_container}"],
        f"docker cp {local_dir} → {container}:{ddl_container}"
    )
    print(f"  ✅ DDL folder copied.")

    # Step 4: Confirm files landed inside the container
    print(f"\n[STEP 4] Verifying files inside container…")
    result = subprocess.run(
        ["docker", "exec", container, "ls", ddl_container],
        capture_output=True, text=True
    )
    print("  Files found:\n  " + "\n  ".join(result.stdout.strip().split("\n")))

    # Step 5: Execute DDL files in order
    print(f"\n[STEP 5] Running DDL SQL files against '{db}'…")
    for sql_file in ddl_files:
        container_file = f"{ddl_container}/{sql_file}"
        docker_exec_sql_file(container, user, db, container_file,
                             f"Executing {sql_file}")
        print(f"  ✅ {sql_file} done.")

    # Step 6: Verify tables exist
    print(f"\n[STEP 6] Verifying core OMOP tables exist…")
    verify_sql = (
        "SELECT tablename FROM pg_tables "
        "WHERE schemaname = 'public' "
        "ORDER BY tablename;"
    )
    docker_exec_sql(container, user, db, verify_sql, "List OMOP tables")

    print("\n" + "=" * 60)
    print("  ✅  OMOP CDM schema setup complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
