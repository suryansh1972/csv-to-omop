# OMOP ETL - Dynamic CSV to OMOP CDM Tool

## Architecture Overview

```
omop_etl/
├── cli/
│   └── main.py              # Click-based CLI entrypoint
├── core/
│   ├── profiler.py          # Auto-profiles any CSV: detects columns, types, domains
│   ├── concept_resolver.py  # Resolves SNOMED codes → OMOP concept_ids via DB
│   ├── domain_classifier.py # Classifies each field to OMOP domain (Condition/Obs/Measurement/Drug)
│   └── id_generator.py      # Thread-safe sequential OMOP ID generation
├── mappers/
│   ├── base_mapper.py       # Abstract mapper interface
│   ├── person_mapper.py     # CSV row → OMOP person table
│   ├── observation_mapper.py# Categorical/text → observation_occurrence
│   ├── measurement_mapper.py# Numeric → measurement table
│   ├── condition_mapper.py  # Condition fields → condition_occurrence
│   └── visit_mapper.py      # Visit/encounter → visit_occurrence
├── loaders/
│   ├── vocab_loader.py      # Bulk-loads Athena vocab CSVs into OMOP schema
│   └── omop_writer.py       # Batch-writes OMOP tables to PostgreSQL
├── config/
│   └── settings.py          # DB connection, paths (all from env/CLI args)
└── cli/
    └── main.py              # Interactive CLI wizard
```

## Core Design Principles

1. **Zero hardcoding** - All field-to-domain mappings are computed dynamically at runtime
2. **SNOMED-first** - Uses mapping CSVs (any format) to extract SNOMED codes, then resolves to OMOP concept_ids via live DB query
3. **FHIR-inspired domain routing** - Borrows CodeableConcept + ValueAsConcept patterns without FHIR dependency
4. **Any CSV** - Profiles columns by name pattern + data type + cardinality to auto-classify domains

## FHIR→OMOP Logic Borrowed (No FHIR Required)

| FHIR Pattern | CSV Equivalent | OMOP Target |
|---|---|---|
| CodeableConcept | Column with SNOMED mapped value (categorical) | observation/condition |
| ValueAsConcept | Column with coded answer (yes/no/scale) | observation.value_as_concept_id |
| ValueAsNumber | Numeric column | measurement.value_as_number |
| ValueAsString | Free-text column | observation.value_as_string |
| Subject reference | subject_id / participant_id column | person.person_id |
| Effective date | date column | event_date fields |
