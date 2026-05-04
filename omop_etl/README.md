# OMOP CDM 5.4 — Complete Setup & CSV-to-OMOP ETL Runbook

> **Scope:** End-to-end guide from a blank machine to fully loaded OMOP tables.  
> **Environment tested on:** Ubuntu (Debian-based), Docker Desktop, Python 3.10+, PostgreSQL 15 (Docker).

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Project Structure](#project-structure)
4. [Phase 1 — Environment Setup](#phase-1--environment-setup)
5. [Phase 2 — PostgreSQL in Docker](#phase-2--postgresql-in-docker)
6. [Phase 3 — OMOP Schema Setup](#phase-3--omop-schema-setup)
7. [Phase 4 — Load Vocabularies](#phase-4--load-vocabularies)
8. [Phase 5 — Verify the Database](#phase-5--verify-the-database)
9. [Phase 6 — Run the ETL](#phase-6--run-the-etl)
10. [Phase 7 — Verify the Loaded Data](#phase-7--verify-the-loaded-data)
11. [Using Your Own CSV](#using-your-own-csv)
12. [Troubleshooting Reference](#troubleshooting-reference)
13. [Minimal Command Sequence (Quick Run)](#minimal-command-sequence-quick-run)

---

## Overview

This project converts clinical CSV data into the OMOP Common Data Model (CDM) 5.4 stored in PostgreSQL. There are two logical components:

| Component | Purpose |
|---|---|
| `OMOP Setup /` | Creates the OMOP schema in PostgreSQL and loads SNOMED + Gender vocabularies |
| `omop_etl/` | Profiles your CSV, maps fields via SNOMED mapping files, and writes rows into OMOP tables |

> **Note on folder name:** The setup folder is literally named `OMOP Setup ` with a **trailing space**. Always wrap it in quotes in every command.

---

## Prerequisites

### Required software

| Tool | Minimum version | Check |
|---|---|---|
| Python | 3.10+ | `python3 --version` |
| Docker | Any recent | `docker --version` |
| PostgreSQL client (psql) | Via Docker (no local install needed) | `docker exec omop-postgres psql --version` |

### Required files already in the repo

- SNOMED vocabulary CSVs → `data/vocabularies/snomed/`
- Gender vocabulary CSVs → `data/vocabularies/gender/`
- SNOMED mapping files:
  - `data/sanscog/sanscog/nurse_v10_snomed_mapping.csv`
  - `data/sanscog/sanscog/clinical_v10_snomed_mapping.csv`
- Sample clinical CSV → `omop_etl/SANSCOG_ClinicalAssessment_IUDXTestData (2).csv`

### Required external download

- **OHDSI CommonDataModel DDL files** — clone from GitHub if not already present:

```bash
git clone https://github.com/OHDSI/CommonDataModel.git \
  /home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/CommonDataModel
```

The DDL folder that `config.json` points to must contain these four files:

```
OMOPCDM_postgresql_5.4_ddl.sql
OMOPCDM_postgresql_5.4_primary_keys.sql
OMOPCDM_postgresql_5.4_constraints.sql
OMOPCDM_postgresql_5.4_indices.sql
```

---

## Project Structure

```
csv-to-omop/
├── OMOP Setup /                  ← trailing space in folder name; always quote
│   ├── config.json
│   ├── 1_setup_omop_schema.py
│   ├── 2_load_snomed_vocabularies.py
│   └── 3_load_gender_vocabularies.py
├── omop_etl/
│   ├── cli/
│   │   └── main.py
│   ├── requirements.txt
│   └── SANSCOG_ClinicalAssessment_IUDXTestData (2).csv
├── data/
│   ├── vocabularies/
│   │   ├── snomed/
│   │   └── gender/
│   └── sanscog/sanscog/
│       ├── nurse_v10_snomed_mapping.csv
│       └── clinical_v10_snomed_mapping.csv
└── CommonDataModel/
    └── inst/ddl/5.4/postgresql/
```

---

## Phase 1 — Environment Setup

### 1.1 — Install Python dependencies

```bash
cd "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop"

# Install system-level psycopg2 and pip tools
sudo apt update
sudo apt install -y python3-pip python3-venv python3-psycopg2 libpq-dev

# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Upgrade pip and install ETL dependencies
python3 -m pip install --upgrade pip
pip install -r omop_etl/requirements.txt
```

**Expected output:** `Successfully installed click psycopg2-binary` (and any other listed packages).

**If `pip install` fails:**

```bash
# Try installing psycopg2-binary with the system flag
pip install psycopg2-binary --break-system-packages

# Or install without a venv using apt
sudo apt install -y python3-psycopg2
```

### 1.2 — Check `config.json`

```bash
cd "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/OMOP Setup "
nano config.json
```

Verify these fields match your environment:

```json
{
  "postgres": {
    "container_name": "omop-postgres",
    "user": "postgres",
    "password": "omop",
    "database": "omop"
  },
  "paths": {
    "ddl_local": "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/CommonDataModel/inst/ddl/5.4/postgresql",
    "vocabulary_local": "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/data/vocabularies/snomed",
    "gender_vocab_local": "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/data/vocabularies/gender"
  }
}
```

| Field | When to change |
|---|---|
| `container_name` | If your Docker container has a different name |
| `user` / `password` / `database` | If your DB credentials differ |
| `ddl_local` | If you cloned CommonDataModel to a different path |
| `vocabulary_local` | If you moved the SNOMED vocabulary folder |
| `gender_vocab_local` | If you moved the gender vocabulary folder |

---

## Phase 2 — PostgreSQL in Docker

### 2.1 — Start the container (if it already exists)

```bash
docker start omop-postgres
```

**Check it is running:**

```bash
docker ps | grep omop-postgres
```

**Watch the logs until it says "ready to accept connections":**

```bash
docker logs -f omop-postgres
```

Press `Ctrl+C` once you see:

```
database system is ready to accept connections
```

---

### 2.2 — First-time container creation (only if it does not exist yet)

```bash
docker run -d \
  --name omop-postgres \
  -e POSTGRES_DB=omop \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=omop \
  -p 5432:5432 \
  -v omop-postgres-data:/var/lib/postgresql/data \
  postgres:15
```

**If `docker run` fails with "port already in use":**

```bash
# Find what is using port 5432
ss -ltnp '( sport = :5432 )'

# Or with lsof
sudo lsof -i :5432

# If it's another Postgres process, stop it:
sudo systemctl stop postgresql

# Then retry docker run
```

---

### 2.3 — If the container is broken and you need to recreate it

> **Warning:** This destroys all data in the container. Only do this for a full reset.

```bash
docker rm -f omop-postgres

docker run -d \
  --name omop-postgres \
  -e POSTGRES_DB=omop \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=omop \
  -p 5432:5432 \
  -v omop-postgres-data:/var/lib/postgresql/data \
  postgres:15
```

---

### 2.4 — Wipe and reset an existing database (optional)

If you want to start from a clean schema without recreating the container:

```bash
docker exec omop-postgres psql -U postgres -d omop \
  -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
```

---

## Phase 3 — OMOP Schema Setup

This installs the OMOP CDM 5.4 table definitions into the PostgreSQL database.

```bash
cd "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/OMOP Setup "
python3 1_setup_omop_schema.py
```

**What this script does:**

- Verifies the Docker container is running
- Copies the four DDL SQL files into the container
- Runs them in order (DDL → primary keys → constraints → indices)
- Confirms that OMOP tables now exist

**Expected output ends with something like:**

```
✓ OMOP schema created successfully.
Tables found: person, visit_occurrence, observation, measurement, ...
```

---

### If schema setup fails

**Error: `config.json not found`**

You are not in the correct folder. The trailing space matters:

```bash
cd "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/OMOP Setup "
# Note: there is a space before the closing quote
```

**Error: `DDL files not found` or `No such file or directory`**

Check that `paths.ddl_local` in `config.json` points to the correct location:

```bash
ls -la "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/CommonDataModel/inst/ddl/5.4/postgresql"
```

If that path does not exist, re-clone CommonDataModel (see Prerequisites).

**Error: `could not connect to server` or `Connection refused`**

The container is not running. Fix:

```bash
docker start omop-postgres
docker logs -f omop-postgres   # wait for "ready to accept connections"
```

**Error: `relation already exists`**

The schema already has tables. Either:
- Skip this step (schema is already set up), or
- Wipe and redo (see Phase 2.4 above) if you want a clean install.

---

## Phase 4 — Load Vocabularies

Vocabularies must be loaded before the ETL. Run both scripts in order.

### 4.1 — Load SNOMED Vocabularies

```bash
cd "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/OMOP Setup "
python3 2_load_snomed_vocabularies.py
```

**What this does:**

- Copies SNOMED CSVs into the Docker container
- Truncates the OMOP vocabulary tables
- Bulk loads the SNOMED files
- Prints row counts per table

**Expected output:**

```
concept: 123456 rows
concept_relationship: 234567 rows
vocabulary: 89 rows
...
```

**If vocabulary loading fails:**

```bash
# Verify vocabulary files exist
ls -la "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/data/vocabularies/snomed/"

# Check that the OMOP tables exist (schema must be set up first)
docker exec omop-postgres psql -U postgres -d omop -c "\dt public.*"
```

If the tables are missing, re-run Phase 3 before attempting vocabulary load.

---

### 4.2 — Load Gender Vocabularies

```bash
cd "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/OMOP Setup "
python3 3_load_gender_vocabularies.py
```

**What this does:**

- Stages gender vocabulary CSVs in temporary tables
- Merges them into OMOP vocabulary tables without overwriting SNOMED rows

**If this fails:**

```bash
ls -la "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/data/vocabularies/gender/"
```

Update `paths.gender_vocab_local` in `config.json` if the path is wrong.

---

## Phase 5 — Verify the Database

Run these checks after completing Phases 3 and 4.

### List all OMOP tables

```bash
docker exec omop-postgres psql -U postgres -d omop \
  -c "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename;"
```

You should see tables like: `concept`, `concept_relationship`, `condition_occurrence`, `measurement`, `observation`, `person`, `visit_occurrence`, etc.

### Check vocabulary row counts

```bash
docker exec omop-postgres psql -U postgres -d omop \
  -c "SELECT COUNT(*) FROM public.concept;"

docker exec omop-postgres psql -U postgres -d omop \
  -c "SELECT COUNT(*) FROM public.concept_relationship;"

docker exec omop-postgres psql -U postgres -d omop \
  -c "SELECT COUNT(*) FROM public.vocabulary;"
```

These should all return non-zero values.

### Use the ETL verifier

```bash
cd "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop"
source .venv/bin/activate

python3 omop_etl/cli/main.py verify \
  --host localhost \
  --port 5432 \
  --dbname omop \
  --user postgres \
  --password omop \
  --schema public
```

---

## Phase 6 — Run the ETL

### 6.1 — Profile your CSV (recommended first step)

This gives you a preview of how the CSV will map before touching the database.

```bash
cd "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop"
source .venv/bin/activate

python3 omop_etl/cli/main.py profile \
  --csv "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/omop_etl/SANSCOG_ClinicalAssessment_IUDXTestData (2).csv" \
  --mapping "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/data/sanscog/sanscog/nurse_v10_snomed_mapping.csv" \
  --mapping "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/data/sanscog/sanscog/clinical_v10_snomed_mapping.csv"
```

**Output includes:**

- Detected person ID column
- Detected age / gender / date columns
- Column domain distribution (observation / measurement / condition)
- Coverage percentage of your CSV by the mapping files

---

### 6.2 — Inspect field routing

Confirms exactly which columns route to which OMOP table.

```bash
python3 omop_etl/cli/main.py inspect \
  --csv "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/omop_etl/SANSCOG_ClinicalAssessment_IUDXTestData (2).csv" \
  --mapping "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/data/sanscog/sanscog/nurse_v10_snomed_mapping.csv" \
  --mapping "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/data/sanscog/sanscog/clinical_v10_snomed_mapping.csv"
```

---

### 6.3 — Dry run (validates without writing to DB)

Always run this before the actual ETL to catch mapping or person ID issues.

```bash
python3 omop_etl/cli/main.py run \
  --csv "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/omop_etl/SANSCOG_ClinicalAssessment_IUDXTestData (2).csv" \
  --mapping "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/data/sanscog/sanscog/nurse_v10_snomed_mapping.csv" \
  --mapping "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/data/sanscog/sanscog/clinical_v10_snomed_mapping.csv" \
  --source-name "SANSCOG" \
  --allow-synthetic-person-id \
  --host localhost \
  --port 5432 \
  --dbname omop \
  --user postgres \
  --password omop \
  --dry-run
```

Review the output. If it looks correct, proceed to the actual run.

---

### 6.4 — Run the actual ETL

```bash
python3 omop_etl/cli/main.py run \
  --csv "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/omop_etl/SANSCOG_ClinicalAssessment_IUDXTestData (2).csv" \
  --mapping "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/data/sanscog/sanscog/nurse_v10_snomed_mapping.csv" \
  --mapping "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/data/sanscog/sanscog/clinical_v10_snomed_mapping.csv" \
  --source-name "SANSCOG" \
  --allow-synthetic-person-id \
  --host localhost \
  --port 5432 \
  --dbname omop \
  --user postgres \
  --password omop
```

When prompted:

```
Proceed with ETL? [y/N]:
```

Type `y` and press Enter.

---

### ETL flags reference

| Flag | Purpose | When to use |
|---|---|---|
| `--dry-run` | Validate without writing to DB | Always run first |
| `--allow-synthetic-person-id` | Creates one OMOP person per CSV row | When CSV has no stable patient identifier |
| `--person-id-column <col>` | Use a specific column as the person ID | When auto-detection picks the wrong column |
| `--allow-uuid-person-id` | Accept UUID-format person IDs | When patient IDs are UUIDs |
| `--source-name <name>` | Tag rows with a source name | Always recommended |
| `--schema <schema>` | Target PostgreSQL schema | Default is `public` |

---

## Phase 7 — Verify the Loaded Data

After the ETL completes, confirm rows were written:

```bash
docker exec omop-postgres psql -U postgres -d omop \
  -c "SELECT COUNT(*) FROM public.person;"

docker exec omop-postgres psql -U postgres -d omop \
  -c "SELECT COUNT(*) FROM public.visit_occurrence;"

docker exec omop-postgres psql -U postgres -d omop \
  -c "SELECT COUNT(*) FROM public.observation;"

docker exec omop-postgres psql -U postgres -d omop \
  -c "SELECT COUNT(*) FROM public.measurement;"

docker exec omop-postgres psql -U postgres -d omop \
  -c "SELECT COUNT(*) FROM public.condition_occurrence;"

docker exec omop-postgres psql -U postgres -d omop \
  -c "SELECT COUNT(*) FROM public.observation_period;"
```

Or use the verifier CLI:

```bash
python3 omop_etl/cli/main.py verify \
  --host localhost --port 5432 --dbname omop --user postgres --password omop --schema public
```

---

## Using Your Own CSV

Replace paths with your own file. If person identity is not detected automatically, use one of the flags below:

```bash
python3 omop_etl/cli/main.py run \
  --csv "/absolute/path/to/your_file.csv" \
  --mapping "/absolute/path/to/mapping1.csv" \
  --mapping "/absolute/path/to/mapping2.csv" \
  --source-name "my_dataset" \
  --host localhost \
  --port 5432 \
  --dbname omop \
  --user postgres \
  --password omop \
  --person-id-column "participant_id"
```

**Choose the right person ID strategy:**

| Your data | Flag to use |
|---|---|
| CSV has a stable numeric patient column | `--person-id-column <col>` |
| CSV has UUID patient IDs | `--allow-uuid-person-id` |
| CSV has no patient identifier at all | `--allow-synthetic-person-id` |

---

## Troubleshooting Reference

### Docker & connectivity

| Error | Cause | Fix |
|---|---|---|
| `Cannot connect to the Docker daemon` | Docker Desktop not running | Open Docker Desktop and wait for it to start |
| `port is already allocated` | Another process on port 5432 | `sudo systemctl stop postgresql` then retry |
| `container name already in use` | Old container exists | `docker rm -f omop-postgres` then re-run `docker run` |
| `Connection refused` on ETL | Container stopped | `docker start omop-postgres` |

---

### Schema & setup

| Error | Cause | Fix |
|---|---|---|
| `config.json not found` | Wrong working directory | `cd "/home/gourish/.../OMOP Setup "` — include trailing space |
| `DDL files not found` | Wrong `ddl_local` path | Update `config.json`; verify with `ls` |
| `relation already exists` | Schema already set up | Skip step or wipe DB first (Phase 2.4) |
| `relation "<table>" does not exist` during ETL | Schema not set up | Run `1_setup_omop_schema.py` then vocabulary scripts before ETL |

---

### Vocabulary loading

| Error | Cause | Fix |
|---|---|---|
| `vocabulary files not found` | Wrong vocabulary path | Update `paths.vocabulary_local` in `config.json` |
| `gender vocab files not found` | Wrong gender path | Update `paths.gender_vocab_local` in `config.json` |
| Row count is 0 after load | Load was silently skipped | Check logs; re-run the vocabulary script with Python output visible |

---

### ETL

| Error | Cause | Fix |
|---|---|---|
| `Invalid value for '--csv'` | File path wrong or file missing | Verify path with `ls`; check for spaces in path and always quote |
| `Invalid value for '--mapping'` | Mapping file not found | Mappings are in `data/sanscog/sanscog/`, not `data/vocabularies/` |
| `No stable person identifier found` | CSV has no usable ID column | Add `--allow-synthetic-person-id` or `--person-id-column <col>` |
| `ModuleNotFoundError: No module named 'psycopg2'` | Dependency not installed | `sudo apt install python3-psycopg2` |
| `ModuleNotFoundError: No module named 'click'` | venv not activated | `source .venv/bin/activate` |
| Multiline command fails unexpectedly | Trailing space after `\` | Ensure `\` is the very last character on each continuation line |

---

### Viewing logs

```bash
# Tail the ETL log file
tail -f omop_etl.log

# View Docker container logs
docker logs -f omop-postgres
```

---

## Minimal Command Sequence (Quick Run)

Use this only if `config.json` is already correctly set up and the schema has never been loaded before.

```bash
# Activate Python environment
cd "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop"
source .venv/bin/activate

# Start the database
docker start omop-postgres

# Set up OMOP schema and vocabularies
cd "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/OMOP Setup "
python3 1_setup_omop_schema.py
python3 2_load_snomed_vocabularies.py
python3 3_load_gender_vocabularies.py

# Run the ETL
cd "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop"

python3 omop_etl/cli/main.py run \
  --csv "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/omop_etl/SANSCOG_ClinicalAssessment_IUDXTestData (2).csv" \
  --mapping "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/data/sanscog/sanscog/nurse_v10_snomed_mapping.csv" \
  --mapping "/home/gourish/Datakaveri-projects/csv-omph/csv-to-omop/data/sanscog/sanscog/clinical_v10_snomed_mapping.csv" \
  --source-name "SANSCOG" \
  --allow-synthetic-person-id \
  --host localhost \
  --port 5432 \
  --dbname omop \
  --user postgres \
  --password omop

# Verify
python3 omop_etl/cli/main.py verify \
  --host localhost --port 5432 --dbname omop --user postgres --password omop --schema public
```