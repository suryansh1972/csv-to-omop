"""
pipeline.py - Main ETL orchestration.

Ties together: profiler → concept resolver → domain classifier → mappers → writer.

TWO-PASS approach to respect FK constraints:
  Pass 1: Write all person + visit_occurrence rows first (committed to DB)
  Pass 2: Write all clinical events (measurement, observation, condition, drug)

This guarantees person_id and visit_occurrence_id exist before events reference them.
"""
import csv
import logging
import psycopg2
from datetime import date
from typing import Dict, List, Optional
from pathlib import Path

from config.settings import ETLConfig
from core.profiler import profile_csv, DatasetProfile
from core.concept_resolver import ConceptResolver, parse_mapping_file
from core.domain_classifier import DomainClassifier, VALUE_STRATEGY_SKIP
from core.id_generator import OMOPIdGenerator
from mappers.person_mapper import PersonMapper
from mappers.event_mappers import (
    VisitMapper, ObservationMapper, MeasurementMapper,
    ConditionMapper, DrugMapper
)
from loaders.omop_writer import OMOPWriter

logger = logging.getLogger(__name__)
SYNTHETIC_PERSON_ID_COL = "__synthetic_person_id__"


def _synthetic_person_source_value(source_name: str, row_index: int) -> str:
    return f"synthetic-person:{row_index + 1}"


def _format_person_id_error(profile: DatasetProfile) -> str:
    identifier_named_cols = [
        col_name for col_name, cp in profile.columns.items()
        if cp.non_empty_count == 0 and any(
            token in col_name.lower()
            for token in ("subject", "participant", "patient", "person", "mrn", "uhid", "empi")
        )
    ]
    suggestions = []
    if profile.person_id_candidates:
        suggestions.append(f"top candidates={profile.person_id_candidates}")
    if identifier_named_cols:
        suggestions.append(
            "empty identifier-like columns="
            + str(identifier_named_cols[:5])
        )
    hint = ""
    if "meta:instanceID" in profile.columns:
        hint = (
            " If each submission UUID should represent a distinct person, rerun with "
            "--person-id-column meta:instanceID --allow-uuid-person-id."
        )
    synthetic_hint = (
        " To create one synthetic OMOP person per CSV row instead, rerun with "
        "--allow-synthetic-person-id."
    )
    details = f" ({'; '.join(suggestions)})" if suggestions else ""
    return (
        "No stable person identifier column could be inferred from the source CSV. "
        "The ETL will not fabricate patient identity from row hashes or submission UUIDs"
        f"{details}.{hint}{synthetic_hint}"
    )


def _parse_positive_int(value: str) -> Optional[int]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = float(raw)
    except ValueError:
        return None
    if parsed < 1 or not parsed.is_integer():
        return None
    return int(parsed)


def _infer_visit_ordinal_column(
    rows: List[Dict[str, str]],
    routes,
) -> Optional[str]:
    if not rows:
        return None

    min_non_empty = max(3, len(rows) // 2)
    best_col = None
    best_score = None

    for col_name in rows[0].keys():
        parsed_values = []
        for row in rows:
            parsed = _parse_positive_int(row.get(col_name, ""))
            if parsed is None:
                continue
            parsed_values.append(parsed)

        if len(parsed_values) < min_non_empty:
            continue

        distinct_values = sorted(set(parsed_values))
        if len(distinct_values) < 2:
            continue

        max_value = distinct_values[-1]
        if max_value > max(10, len(rows)):
            continue
        if distinct_values != list(range(1, max_value + 1)):
            continue

        score = (
            len(parsed_values) / len(rows),
            -max_value,
            -len(distinct_values),
        )
        if best_score is None or score > best_score:
            best_col = col_name
            best_score = score

    return best_col


def _resolve_visit_source_value(
    row: Dict[str, str],
    profile: DatasetProfile,
    default_source: str,
) -> str:
    site_like_cols = [
        col_name for col_name in profile.columns
        if any(token in col_name.lower() for token in ("site", "centre", "center", "location", "facility"))
    ]
    for col_name in site_like_cols:
        value = row.get(col_name, "")
        if value and str(value).strip():
            return str(value).strip()
    return default_source


class OMOPPipeline:
    """
    Full CSV → OMOP CDM ETL pipeline.

    Steps:
    1. Profile source CSV
    2. Parse SNOMED mapping files
    3. Classify columns to OMOP domains
    4. Resolve type concept IDs dynamically from DB
    5. Pass 1 — write person + visit_occurrence (flush + commit)
    6. Pass 2 — write all clinical events referencing committed persons/visits
    7. Write observation periods
    """

    def __init__(self, config: ETLConfig):
        self.config = config
        self.conn: Optional[psycopg2.extensions.connection] = None
        self.writer: Optional[OMOPWriter] = None
        self.resolver: Optional[ConceptResolver] = None
        self.id_gen: Optional[OMOPIdGenerator] = None

    def connect(self) -> bool:
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(self.config.db.dsn())
            logger.info(f"Connected to {self.config.db.host}:{self.config.db.port}/{self.config.db.dbname}")
            return True
        except Exception as e:
            logger.error(f"DB connection failed: {e}")
            return False

    def _resolve_type_concepts(self) -> Dict[str, int]:
        """
        Dynamically resolve all OMOP type concept IDs from the DB.
        Queries by concept_name — no hardcoding of concept IDs.
        Falls back to 0 if not found (FK constraints allow 0 in CDM).
        """
        lookups = {
            "visit_type":       ("Visit Type", "EHR encounter"),
            "obs_type":         ("Type Concept", "EHR"),
            "meas_type":        ("Type Concept", "Lab result"),
            "cond_type":        ("Type Concept", "EHR encounter diagnosis"),
            "drug_type":        ("Type Concept", "Prescription written"),
            "obs_period_type":  ("Type Concept", "Period while enrolled in study"),
            "visit_concept":    ("Visit", "Outpatient Visit"),
            "cond_status":      ("Type Concept", "Primary"),
        }

        resolved = {}
        for key, (domain_id, concept_name) in lookups.items():
            try:
                resolved[key] = self.resolver.lookup_standard_concept_id(
                    concept_name,
                    domains=[domain_id],
                    allow_partial=True,
                )
                logger.debug(f"  Resolved {key} → concept_id={resolved[key]} ('{concept_name}')")
            except Exception as e:
                self.conn.rollback()
                logger.warning(f"  Could not resolve type concept '{key}': {e}")
                resolved[key] = 0

        logger.info(f"Type concepts resolved from DB: {resolved}")
        return resolved

    def run(self, progress_callback=None):
        """
        Execute the full ETL pipeline.

        Args:
            progress_callback: Optional callable(current, total, message)
        """
        cfg = self.config

        if not self.connect():
            raise ConnectionError("Cannot connect to OMOP database")

        self.writer = OMOPWriter(
            self.conn, schema=cfg.db.schema,
            batch_size=cfg.batch_size, dry_run=cfg.dry_run
        )
        self.resolver = ConceptResolver(self.conn)
        self.id_gen = OMOPIdGenerator(self.conn, schema=cfg.db.schema)

        # --- Step 1: Profile CSV ---
        logger.info("Step 1/5: Profiling source CSV...")
        if progress_callback:
            progress_callback(0, 5, "Profiling source CSV...")
        profile = profile_csv(
            cfg.csv_path,
            mapping_paths=cfg.mapping_paths,
            resolver=self.resolver,
            person_id_column=cfg.person_id_column,
            allow_uuid_person_id=cfg.allow_uuid_person_id,
        )
        if not profile.person_id_col:
            if cfg.allow_synthetic_person_id:
                profile.person_id_col = SYNTHETIC_PERSON_ID_COL
                logger.warning(
                    "No stable source person identifier detected. "
                    "Falling back to synthetic person IDs (one person per CSV row)."
                )
            else:
                raise RuntimeError(_format_person_id_error(profile))

        # --- Step 2: Load SNOMED mappings ---
        logger.info("Step 2/5: Loading SNOMED mappings...")
        if progress_callback:
            progress_callback(1, 5, "Loading SNOMED concept mappings...")
        field_snomed_map = {}
        for mp in cfg.mapping_paths:
            field_snomed_map.update(parse_mapping_file(mp))
        logger.info(f"Loaded {len(field_snomed_map)} field→SNOMED mappings")

        # --- Step 3: Classify columns ---
        logger.info("Step 3/5: Classifying columns to OMOP domains...")
        if progress_callback:
            progress_callback(2, 5, "Classifying columns to OMOP domains...")
        classifier = DomainClassifier(self.resolver, field_snomed_map)
        routes = classifier.classify_all(profile)
        candidate_mapped_cols = [c for c in profile.columns if c in field_snomed_map]
        resolved_mapped_cols = sum(1 for route in routes.values() if route.is_mapped)
        if candidate_mapped_cols and resolved_mapped_cols == 0:
            raise RuntimeError(
                "Mapping files matched source columns, but none of their SNOMED codes "
                "resolved against the OMOP vocabulary in the target database. "
                "Load the OMOP vocabulary into this database before running ETL."
            )

        # --- Step 4: Resolve type concept IDs from DB ---
        logger.info("Step 4/5: Resolving type concept IDs from DB...")
        if progress_callback:
            progress_callback(3, 5, "Resolving type concepts from DB...")
        type_concepts = self._resolve_type_concepts()

        # Inject resolved IDs into mappers (no hardcoding)
        visit_mapper = VisitMapper(
            self.id_gen, profile.date_cols,
            visit_type_concept_id=type_concepts.get("visit_type", 0),
            visit_concept_id=type_concepts.get("visit_concept", 0),
        )
        obs_mapper = ObservationMapper(
            self.id_gen, self.resolver, profile.date_cols,
            obs_type_concept_id=type_concepts.get("obs_type", 0),
        )
        meas_mapper = MeasurementMapper(
            self.id_gen, self.resolver, profile.date_cols,
            meas_type_concept_id=type_concepts.get("meas_type", 0),
        )
        cond_mapper = ConditionMapper(
            self.id_gen, self.resolver, profile.date_cols,
            cond_type_concept_id=type_concepts.get("cond_type", 0),
            cond_status_concept_id=type_concepts.get("cond_status", 0),
        )
        drug_mapper = DrugMapper(
            self.id_gen, self.resolver, profile.date_cols,
            drug_type_concept_id=type_concepts.get("drug_type", 0),
        )
        person_mapper = PersonMapper(
            profile, self.resolver, self.id_gen,
            source_value_prefix=cfg.source_name + ":",
        )

        # --- Step 5: Two-pass ETL ---
        logger.info("Step 5/5: Two-pass ETL (persons+visits → then events)...")
        if progress_callback:
            progress_callback(4, 5, "Writing persons and visits...")

        # Read all rows into memory (safe for typical clinical datasets)
        all_rows = []
        with open(cfg.csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if profile.person_id_col == SYNTHETIC_PERSON_ID_COL:
                    row[SYNTHETIC_PERSON_ID_COL] = _synthetic_person_source_value(
                        cfg.source_name,
                        i,
                    )
                all_rows.append(row)

        logger.info(f"Loaded {len(all_rows)} source rows into memory")
        visit_ordinal_col = _infer_visit_ordinal_column(all_rows, routes)
        if visit_ordinal_col:
            logger.info(f"Detected source visit ordinal column: {visit_ordinal_col}")

        # ── PASS 1: persons and visits ─────────────────────────────────────
        logger.info("Pass 1: Writing persons and visit_occurrence...")
        person_ids: Dict[int, int] = {}   # row_index → person_id
        visit_ids: Dict[int, int] = {}    # row_index → visit_occurrence_id
        person_dates: Dict[int, list] = {}

        for i, row in enumerate(all_rows):
            person_rec = person_mapper.map_row(row)
            if not person_rec:
                logger.warning(f"Row {i+1}: could not build person record — skipping")
                continue

            person_id = person_rec["person_id"]
            if self.writer.upsert_person(person_rec):
                person_ids[i] = person_id
            else:
                logger.warning(f"Row {i+1}: person write failed — skipping dependent records")

        logger.info(f"  Committed {len(person_ids)} person records")

        for i, row in enumerate(all_rows):
            if i not in person_ids:
                continue
            person_id = person_ids[i]

            visit_source = _resolve_visit_source_value(row, profile, cfg.source_name)
            visit_ordinal = row.get(visit_ordinal_col, "").strip() if visit_ordinal_col else ""

            visit_rec = visit_mapper.map_row(
                row,
                person_id,
                visit_source=visit_source,
                visit_ordinal=visit_ordinal,
            )
            if visit_rec:
                if self.writer.write_immediate("visit_occurrence", visit_rec):
                    visit_ids[i] = visit_rec["visit_occurrence_id"]
                    vd = visit_rec["visit_start_date"]
                    if vd:
                        person_dates.setdefault(person_id, []).append(vd)
                else:
                    logger.warning(f"Row {i+1}: visit write failed — events will load without visit linkage")

        logger.info(f"  Committed {len(visit_ids)} visit_occurrence records")

        # ── PASS 2: clinical events ────────────────────────────────────────
        logger.info("Pass 2: Writing clinical events...")
        if progress_callback:
            progress_callback(4, 5, "Writing clinical events...")

        for i, row in enumerate(all_rows):
            if i not in person_ids:
                continue
            person_id = person_ids[i]
            visit_id = visit_ids.get(i)

            for col_name, route in routes.items():
                if route.value_strategy == VALUE_STRATEGY_SKIP:
                    continue
                if route.target_table == "observation":
                    rec = obs_mapper.map_field(row, person_id, visit_id, route)
                    self.writer.write("observation", rec)
                elif route.target_table == "measurement":
                    rec = meas_mapper.map_field(row, person_id, visit_id, route)
                    self.writer.write("measurement", rec)
                elif route.target_table == "condition_occurrence":
                    rec = cond_mapper.map_field(row, person_id, visit_id, route)
                    self.writer.write("condition_occurrence", rec)
                elif route.target_table == "drug_exposure":
                    rec = drug_mapper.map_field(row, person_id, visit_id, route)
                    self.writer.write("drug_exposure", rec)

        self.writer.flush_all()

        # ── Observation periods ────────────────────────────────────────────
        obs_period_type = type_concepts.get("obs_period_type", 0)
        for pid, dates in person_dates.items():
            self.writer.write_observation_period(
                pid,
                min(dates),
                max(dates),
                obs_period_type,
                observation_period_id=self.id_gen.next_id("observation_period"),
            )

        logger.info(f"ETL complete. Processed {len(all_rows)} source rows.")
        if progress_callback:
            progress_callback(5, 5, f"Complete! Processed {len(all_rows)} rows.")

        return self.writer.stats()