"""
settings.py - Single source of truth for all configuration.

Every value that was previously hardcoded in other modules now lives here as a
typed, documented, overridable field.  Nothing outside this file defines magic
strings, threshold numbers, fallback years, or OMOP concept-name queries.
"""
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

@dataclass
class DBConfig:
    host:     str = field(default_factory=lambda: os.getenv("OMOP_HOST",     "localhost"))
    port:     int = field(default_factory=lambda: int(os.getenv("OMOP_PORT", "5432")))
    dbname:   str = field(default_factory=lambda: os.getenv("OMOP_DB",       "omop"))
    user:     str = field(default_factory=lambda: os.getenv("OMOP_USER",     "postgres"))
    password: str = field(default_factory=lambda: os.getenv("OMOP_PASSWORD", "omop"))
    schema:   str = field(default_factory=lambda: os.getenv("OMOP_SCHEMA",   "public"))

    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password={self.password}"
        )

    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.dbname}"
        )


# ---------------------------------------------------------------------------
# Profiler behaviour
# ---------------------------------------------------------------------------

@dataclass
class ProfilerConfig:
    """All thresholds and lookup tables for CSV profiling."""

    null_like_values: Set[str] = field(default_factory=lambda: {
        "", "NA", "N/A", "na", "n/a", "null", "NULL", "none", "None", "NONE",
        ".", "-", "unknown", "Unknown", "UNKNOWN",
    })

    boolean_values: Set[str] = field(default_factory=lambda: {
        "yes", "no", "true", "false", "1", "0", "y", "n",
        "YES", "NO", "TRUE", "FALSE", "Y", "N",
    })

    date_formats: Tuple[str, ...] = field(default_factory=lambda: (
        "%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y",
        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f",
    ))

    identifier_name_strong_hints: Tuple[str, ...] = field(default_factory=lambda: (
        "subject_id", "subjectid", "participant_id", "participantid",
        "patient_id", "patientid", "person_id", "personid",
        "member_id", "memberid", "study_id", "studyid",
        "record_id", "recordid", "mrn", "uhid", "empi",
    ))

    identifier_name_weak_hints: Tuple[str, ...] = field(default_factory=lambda: (
        "subject", "participant", "patient", "person",
        "member", "record", "study", "barcode",
    ))

    uuid_identifier_hints: Tuple[str, ...] = field(default_factory=lambda: (
        "instanceid", "instance_id", "uuid", "submissionid", "submission_id",
    ))

    # Person-ID scoring thresholds
    person_id_min_score:       float = 4.0
    person_id_fill_rate:       float = 0.80
    person_id_uniqueness_rate: float = 0.90
    person_id_max_avg_length:  float = 48.0
    person_id_id_like_rate:    float = 0.80

    # Age column thresholds
    age_min_score:        float = 1.5
    age_min_value:        float = 0.0
    age_max_value:        float = 130.0
    age_median_min:       float = 18.0
    age_median_max:       float = 100.0
    age_integer_like_min: float = 0.80

    birth_date_min_score:      float = 1.0
    date_parse_rate_threshold: float = 0.80

    # dtype inference thresholds
    boolean_rate_threshold: float = 0.95
    date_rate_threshold:    float = 0.90
    numeric_rate_threshold: float = 0.90

    sample_size: int = 100

    # Visit source resolution hints
    site_column_tokens: Tuple[str, ...] = field(default_factory=lambda: (
        "site", "centre", "center", "location", "facility", "clinic",
    ))

    # Demographic field tokens (excluded from person-ID candidacy)
    demographic_exclusion_tokens: Tuple[str, ...] = field(default_factory=lambda: (
        "race", "ethnicity", "caste", "religion", "community",
    ))


# ---------------------------------------------------------------------------
# Concept resolution
# ---------------------------------------------------------------------------

@dataclass
class ConceptConfig:
    """Parameters for OMOP concept lookups."""

    snomed_vocabulary_id:     str = "SNOMED"
    maps_to_relationship_id:  str = "Maps to"
    gender_domain:            str = "Gender"
    race_domain:              str = "Race"
    unknown_concept_id:       int = 0

    value_concept_domains: List[str] = field(default_factory=lambda: [
        "Meas Value", "Observation",
    ])

    # Sentinel used when birth year cannot be inferred.
    # Explicit here — not buried in mapper code.
    unknown_birth_year_sentinel: int = 1900

    birth_age_tolerance_years: int = 1
    max_plausible_age:         int = 120

    # (concept_name, domain_id) pairs for resolving OMOP type concepts.
    # Keys are used by pipeline.py to inject IDs into mappers.
    # Add/change entries here — no code changes required anywhere else.
    type_concept_lookups: Dict[str, Tuple[str, str]] = field(default_factory=lambda: {
        "visit_type":      ("EHR encounter",                  "Visit Type"),
        "obs_type":        ("EHR",                            "Type Concept"),
        "meas_type":       ("Lab result",                     "Type Concept"),
        "cond_type":       ("EHR encounter diagnosis",        "Type Concept"),
        "drug_type":       ("Prescription written",           "Type Concept"),
        "obs_period_type": ("Period while enrolled in study", "Type Concept"),
        "visit_concept":   ("Outpatient Visit",               "Visit"),
        "cond_status":     ("Primary",                        "Type Concept"),
    })


# ---------------------------------------------------------------------------
# Domain routing
# ---------------------------------------------------------------------------

@dataclass
class DomainConfig:
    """Maps OMOP domain_id values to CDM tables and value strategies."""

    domain_to_table: Dict[str, str] = field(default_factory=lambda: {
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
    })

    # profiler dtype → value storage strategy
    dtype_to_value_strategy: Dict[str, str] = field(default_factory=lambda: {
        "numeric":     "value_as_number",
        "boolean":     "value_as_concept_id",
        "categorical": "value_as_concept_id",
        "text":        "value_as_string",
        "date":        "value_as_string",
        "empty":       "skip",
    })

    # Domains compatible with numeric value_as_number storage.
    # Numeric fields whose SNOMED mapping resolves to a domain outside this set
    # have their concept_id cleared to avoid cross-domain misrouting.
    numeric_compatible_domains: Set[str] = field(default_factory=lambda: {
        "Measurement", "Observation", "Meas Value",
    })

    default_table:    str = "observation"
    default_domain:   str = "Observation"
    skip_strategy:    str = "skip"


# ---------------------------------------------------------------------------
# Event mapper behaviour
# ---------------------------------------------------------------------------

@dataclass
class MapperConfig:
    """Controls how clinical event mappers interpret source values."""

    # Values that indicate presence/confirmation of a condition or drug.
    # Runtime comparison is case-insensitive.
    affirmative_values: Set[str] = field(default_factory=lambda: {
        "yes", "y", "1", "true", "present", "positive",
        "confirmed", "active", "diagnosed", "had", "have",
        "currently_using", "using", "on_medication",
    })

    affirmative_prefixes: Tuple[str, ...] = field(default_factory=lambda: (
        "currently_", "using_", "on_",
    ))

    source_value_max_length: int = 50
    missing_visit_sentinel:  Optional[int] = None


# ---------------------------------------------------------------------------
# Vocabulary loader
# ---------------------------------------------------------------------------

@dataclass
class VocabConfig:
    """Maps CDM vocabulary table names to expected Athena CSV filenames."""

    table_to_filenames: Dict[str, List[str]] = field(default_factory=lambda: {
        "concept":              ["CONCEPT.csv",              "concept.csv"],
        "concept_relationship": ["CONCEPT_RELATIONSHIP.csv", "concept_relationship.csv"],
        "concept_ancestor":     ["CONCEPT_ANCESTOR.csv",     "concept_ancestor.csv"],
        "concept_synonym":      ["CONCEPT_SYNONYM.csv",      "concept_synonym.csv"],
        "concept_class":        ["CONCEPT_CLASS.csv",        "concept_class.csv"],
        "domain":               ["DOMAIN.csv",               "domain.csv"],
        "relationship":         ["RELATIONSHIP.csv",         "relationship.csv"],
        "vocabulary":           ["VOCABULARY.csv",           "vocabulary.csv"],
        "drug_strength":        ["DRUG_STRENGTH.csv",        "drug_strength.csv"],
    })

    # FK-safe load order; tables earlier in the list must be loaded first
    load_order: List[str] = field(default_factory=lambda: [
        "vocabulary", "domain", "concept_class", "relationship",
        "concept", "concept_relationship", "concept_synonym",
        "concept_ancestor", "drug_strength",
    ])

    fallback_batch_size: int = 5_000


# ---------------------------------------------------------------------------
# Cohort management
# ---------------------------------------------------------------------------

@dataclass
class CohortConfig:
    """Parameters for cohort creation and export."""

    # Fallback date when observation_period is missing for a person
    default_cohort_start_date: str = "1970-01-01"

    # Export format: "zip" or "directory"
    export_format: str = "zip"

    # CDM version stamped in the manifest
    cdm_version: str = "5.4"


# ---------------------------------------------------------------------------
# Top-level ETL config
# ---------------------------------------------------------------------------

@dataclass
class ETLConfig:
    db:       DBConfig       = field(default_factory=DBConfig)
    profiler: ProfilerConfig = field(default_factory=ProfilerConfig)
    concepts: ConceptConfig  = field(default_factory=ConceptConfig)
    domains:  DomainConfig   = field(default_factory=DomainConfig)
    mappers:  MapperConfig   = field(default_factory=MapperConfig)
    vocab:    VocabConfig    = field(default_factory=VocabConfig)
    cohort:   CohortConfig   = field(default_factory=CohortConfig)

    csv_path:                  Optional[str] = None
    mapping_paths:             List[str]     = field(default_factory=list)
    vocab_dir:                 Optional[str] = None
    batch_size:                int           = 1_000
    dry_run:                   bool          = False
    log_level:                 str           = "INFO"
    source_name:               str           = "UNKNOWN_SOURCE"
    person_id_column:          Optional[str] = None
    allow_uuid_person_id:      bool          = False
    allow_synthetic_person_id: bool          = False
    cdm_version:               str           = "5.4"