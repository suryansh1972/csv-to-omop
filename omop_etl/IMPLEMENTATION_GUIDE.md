# OMOP ETL: Step-by-Step Implementation Guide
## Dynamic CSV → OMOP CDM 5.4 (PostgreSQL)

---

## ARCHITECTURE OVERVIEW

```
Any CSV  ──►  Profiler ──►  SNOMED Parser ──►  Domain Classifier
                                                      │
                         ┌────────────────────────────┴──────────────────────┐
                         ▼           ▼            ▼          ▼          ▼
                      Person     Visit       Observation  Measurement  Condition
                      Mapper     Mapper        Mapper       Mapper      Mapper
                         │           │            │          │          │
                         └───────────┴────────────┴──────────┴──────────┘
                                                  │
                                           OMOP Writer
                                          (batch upsert)
                                                  │
                                          PostgreSQL OMOP DB
```

### FHIR-Borrowed Patterns (no FHIR required)
| Pattern | What it does | CSV→OMOP mapping |
|---|---|---|
| CodeableConcept | Column name → SNOMED code → OMOP concept | field_name → observation_concept_id |
| ValueAsNumber | Numeric field value | value → measurement.value_as_number |
| ValueAsConcept | Coded answer (yes/no/scale) | answer → observation.value_as_concept_id |
| ValueAsString | Free text value | text → observation.value_as_string |
| Maps to relationship | SNOMED non-standard → standard concept | CONCEPT_RELATIONSHIP lookup |

---

## PREREQUISITES

### 1. PostgreSQL in Docker (already done)
```bash
docker run \
  --name omop-postgres \
  -p 5432:5432 \
  -e POSTGRES_PASSWORD=omop \
  -e POSTGRES_DB=omop \
  -d postgres:15

docker ps  # verify running
```

### 2. OMOP CDM Schema (already done)
```bash
docker cp /Users/suryanshshivaprasad/Documents/OMOP/CommonDataModel/inst/ddl/5.4/postgresql \
  omop-postgres:/omop_ddl

docker exec -it omop-postgres psql -U postgres -d omop -c "\i /omop_ddl/OMOPCDM_postgresql_5.4_ddl.sql"
docker exec -it omop-postgres psql -U postgres -d omop -c "\i /omop_ddl/OMOPCDM_postgresql_5.4_primary_keys.sql"
```

---

## STEP-BY-STEP USAGE

### STEP 1: Install the Tool

```bash
# Navigate to the omop_etl directory
cd /path/to/omop_etl

# Install dependencies
pip install psycopg2-binary click

# Verify
python cli/main.py --help
```

### STEP 2: Verify Database Connection
```bash
python cli/main.py verify \
  --host localhost \
  --dbname omop \
  --password omop
```
Expected output:
```
○ person                                      0 rows
○ concept                                     0 rows
...
```

### STEP 3: Load Athena Vocabulary

This loads all SNOMED CT (and other) vocabularies from your Athena download:

```bash
python cli/main.py load-vocab \
  --vocab-dir "/Users/suryanshshivaprasad/Documents/OMOP/CsV to OMOP/vocabulary_download_v5_{c622e6c4-8d47-4fe9-967f-36bff11d22eb}1772460435126" \
  --host localhost \
  --dbname omop \
  --password omop
```

This loads (in order):
- CONCEPT.csv → concept table
- CONCEPT_RELATIONSHIP.csv → concept_relationship table
- CONCEPT_ANCESTOR.csv → concept_ancestor table
- CONCEPT_SYNONYM.csv → concept_synonym table
- VOCABULARY.csv → vocabulary table
- DOMAIN.csv → domain table
- RELATIONSHIP.csv → relationship table
- DRUG_STRENGTH.csv → drug_strength table

Verify after:
```bash
python cli/main.py verify --host localhost --dbname omop --password omop
# Should show concept table with millions of rows
```

### STEP 4: Profile Your Dataset (Optional but Recommended)

See what the tool detects before running ETL:

```bash
python cli/main.py profile \
  --csv SANSCOG_ClinicalAssessment_IUDXTestData__2_.csv \
  --mapping nurse_v10_snomed_mapping.csv \
  --mapping clinical_v10_snomed_mapping.csv \
  --output profile_report.txt
```

Output shows:
- Detected person_id, gender, age, birth_date columns
- Domain distribution (observation/measurement/condition/drug)
- SNOMED mapping coverage %
- Unmapped columns list

### STEP 5: Inspect Domain Routing (Optional)

See exactly which OMOP table each column routes to (requires live DB):

```bash
python cli/main.py inspect \
  --csv SANSCOG_ClinicalAssessment_IUDXTestData__2_.csv \
  --mapping nurse_v10_snomed_mapping.csv \
  --mapping clinical_v10_snomed_mapping.csv \
  --host localhost --dbname omop --password omop
```

### STEP 6: Dry Run (Validate Without Writing)

```bash
python cli/main.py run \
  --csv SANSCOG_ClinicalAssessment_IUDXTestData__2_.csv \
  --mapping nurse_v10_snomed_mapping.csv \
  --mapping clinical_v10_snomed_mapping.csv \
  --source-name SANSCOG \
  --host localhost --dbname omop --password omop \
  --dry-run
```

### STEP 7: Full ETL Run

```bash
python cli/main.py run \
  --csv SANSCOG_ClinicalAssessment_IUDXTestData__2_.csv \
  --mapping nurse_v10_snomed_mapping.csv \
  --mapping clinical_v10_snomed_mapping.csv \
  --source-name SANSCOG \
  --host localhost \
  --dbname omop \
  --password omop \
  --batch-size 1000
```

### Step 8: Verify Results

```bash
python cli/main.py verify --host localhost --dbname omop --password omop
```

Expected output:
```
✓ person                                     13 rows
✓ observation_period                         13 rows
✓ visit_occurrence                           13 rows
✓ observation                             8,000+ rows
✓ measurement                             2,000+ rows
✓ condition_occurrence                      100+ rows
✓ concept                              4,500,000+ rows
```

---

## USING ENVIRONMENT VARIABLES (Recommended)

Instead of passing credentials on every command:

```bash
export OMOP_HOST=localhost
export OMOP_PORT=5432
export OMOP_DB=omop
export OMOP_USER=postgres
export OMOP_PASSWORD=omop
export OMOP_SCHEMA=public

# Now commands are shorter:
python cli/main.py run \
  --csv data.csv \
  --mapping snomed_map.csv \
  --source-name MY_STUDY
```

---

## INTERACTIVE WIZARD (For First-Time Setup)

```bash
python cli/main.py wizard
```

Guides you through all steps interactively.

---

## USING WITH ANY NEW DATASET

The tool is fully dynamic. For any new CSV:

1. **Create a SNOMED mapping CSV** with columns:
   - `name` — matches the CSV column name exactly
   - `snomed` — SNOMED codes in format: `123456789 (Description)` (pipe-separated for multiple)
   - `description` — human-readable label (optional)

2. **Run profile** to verify detection
3. **Run ETL**

### SNOMED Mapping CSV Format (any of these work):
```
name,description,snomed
my_column,My Label,123456789 (Some concept) | 987654321 (Another concept)
```

The tool auto-detects:
- Which column has the field names (`name`, `field`, `variable`, `column`)
- Which column has the SNOMED codes (`snomed`, `code`, `concept`, `mapping`)

---

## DOMAIN INFERENCE

The profiler now infers support fields from observed values instead of
keyword lists in column names:
- person identifiers from completeness, uniqueness, and identifier-like structure
- dates from parseable date values
- age from plausible human-age numeric ranges
- gender from values that resolve to OMOP Gender concepts when a DB is available

The classifier routes fields primarily from resolved OMOP concept domains and
data type rather than a custom keyword dictionary.

---

## LOGS

All ETL activity is logged to:
- Console (stdout)
- `omop_etl.log` (in working directory)

For verbose output:
```bash
python cli/main.py --log-level DEBUG run --csv ...
```

---

## KNOWN LIMITATIONS & NEXT STEPS

1. **observation_period**: Currently set to min/max visit date per person.
   For longitudinal studies, you may want to extend this window.

2. **visit_type**: Defaults to Outpatient Visit (9202). Override by adding
   a visit type column to your CSV.

3. **Drug exposure**: Only creates records for affirmative values (yes/currently_using).
   Extend `DrugMapper.map_field()` to handle quantity/dose columns.

4. **Large files**: For CSVs >100k rows, increase `--batch-size 5000` for performance.

5. **Multiple visits per person**: If your CSV has repeated rows per person (visits),
   the tool creates one visit_occurrence per row automatically.
