"""
event_mappers.py - Maps CSV fields to OMOP clinical event tables.

All type concept IDs are injected via constructor from pipeline.py.
All affirmative-value sets and thresholds come from MapperConfig.
No magic strings or hardcoded sets anywhere in this file.
"""
import logging
import re
from datetime import datetime, date
from typing import Any, Dict, List, Optional

from config.settings import MapperConfig
from core.domain_classifier import FieldRoute
from core.concept_resolver import ConceptResolver
from core.id_generator import OMOPIdGenerator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _parse_date_str(value: str) -> Optional[date]:
    m = re.match(r"(\d{4}-\d{2}-\d{2})", str(value).strip()) if value else None
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            pass
    return None


def _safe_float(value: str) -> Optional[float]:
    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
        return None


def _best_date(row: Dict[str, str], date_cols: List[str]) -> date:
    for dc in date_cols:
        d = _parse_date_str(row.get(dc, ""))
        if d:
            return d
    return date.today()


def _trunc(value: str, max_len: int) -> str:
    return str(value)[:max_len] if value else ""


def _is_null_like(value: str) -> bool:
    return value is None or str(value).strip().upper() in (
        "", "NA", "N/A", "NULL", "NONE", ".",
    )


def _is_affirmative(value: str, cfg: MapperConfig) -> bool:
    """Return True if the value indicates presence/confirmation."""
    lower = value.lower()
    if lower in {v.lower() for v in cfg.affirmative_values}:
        return True
    return any(lower.startswith(p) for p in cfg.affirmative_prefixes)


# ---------------------------------------------------------------------------
# VisitMapper
# ---------------------------------------------------------------------------

class VisitMapper:
    def __init__(
        self,
        id_gen: OMOPIdGenerator,
        date_cols: List[str],
        visit_type_concept_id: int = 0,
        visit_concept_id: int = 0,
        cfg: Optional[MapperConfig] = None,
    ):
        self.id_gen                 = id_gen
        self.date_cols              = date_cols
        self.visit_type_concept_id  = visit_type_concept_id
        self.visit_concept_id       = visit_concept_id
        self.cfg                    = cfg or MapperConfig()

    def map_row(
        self,
        row: Dict[str, str],
        person_id: int,
        visit_source: str = "",
        visit_ordinal: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        visit_date = _best_date(row, self.date_cols)
        source_parts = [s for s in (visit_source.strip(), f"visit={visit_ordinal.strip()}" if visit_ordinal and visit_ordinal.strip() else "") if s]
        return {
            "visit_occurrence_id":           self.id_gen.next_id("visit_occurrence"),
            "person_id":                     person_id,
            "visit_concept_id":              self.visit_concept_id,
            "visit_start_date":              visit_date,
            "visit_start_datetime":          datetime.combine(visit_date, datetime.min.time()),
            "visit_end_date":                visit_date,
            "visit_end_datetime":            datetime.combine(visit_date, datetime.min.time()),
            "visit_type_concept_id":         self.visit_type_concept_id,
            "provider_id":                   None,
            "care_site_id":                  None,
            "visit_source_value":            _trunc(" | ".join(source_parts), self.cfg.source_value_max_length),
            "visit_source_concept_id":       0,
            "admitted_from_concept_id":      0,
            "admitted_from_source_value":    "",
            "discharged_to_concept_id":      0,
            "discharged_to_source_value":    "",
            "preceding_visit_occurrence_id": None,
        }


# ---------------------------------------------------------------------------
# ObservationMapper
# ---------------------------------------------------------------------------

class ObservationMapper:
    # Maps value_strategy string → which field to populate (pure data, no if/elif)
    _VALUE_FIELD_MAP = {
        "value_as_number":     "value_as_number",
        "value_as_concept_id": "value_as_concept_id",
        "value_as_string":     "value_as_string",
    }

    def __init__(
        self,
        id_gen: OMOPIdGenerator,
        resolver: ConceptResolver,
        date_cols: List[str],
        obs_type_concept_id: int = 0,
        cfg: Optional[MapperConfig] = None,
    ):
        self.id_gen              = id_gen
        self.resolver            = resolver
        self.date_cols           = date_cols
        self.obs_type_concept_id = obs_type_concept_id
        self.cfg                 = cfg or MapperConfig()

    def map_field(
        self,
        row: Dict[str, str],
        person_id: int,
        visit_id: Optional[int],
        route: FieldRoute,
    ) -> Optional[Dict[str, Any]]:
        raw = row.get(route.col_name, "")
        if _is_null_like(raw):
            return None
        raw = str(raw).strip()

        value_as_number     = None
        value_as_concept_id = 0
        value_as_string     = None

        strategy = route.value_strategy
        if strategy == "value_as_number":
            value_as_number = _safe_float(raw)
            if value_as_number is None:
                return None
        elif strategy == "value_as_concept_id":
            value_as_concept_id = self.resolver.resolve_value_as_concept(raw)
        else:
            value_as_string = raw

        obs_date = _best_date(row, self.date_cols)
        return {
            "observation_id":                self.id_gen.next_id("observation"),
            "person_id":                     person_id,
            "observation_concept_id":        route.concept_id,
            "observation_date":              obs_date,
            "observation_datetime":          datetime.combine(obs_date, datetime.min.time()),
            "observation_type_concept_id":   self.obs_type_concept_id,
            "value_as_number":               value_as_number,
            "value_as_string":               value_as_string,
            "value_as_concept_id":           value_as_concept_id,
            "qualifier_concept_id":          0,
            "unit_concept_id":               0,
            "provider_id":                   None,
            "visit_occurrence_id":           visit_id,
            "visit_detail_id":               None,
            "observation_source_value":      _trunc(route.col_name, self.cfg.source_value_max_length),
            "observation_source_concept_id": route.source_concept_id,
            "unit_source_value":             "",
            "qualifier_source_value":        "",
            "value_source_value":            _trunc(raw, self.cfg.source_value_max_length),
            "observation_event_id":          None,
            "obs_event_field_concept_id":    0,
        }


# ---------------------------------------------------------------------------
# MeasurementMapper
# ---------------------------------------------------------------------------

class MeasurementMapper:
    def __init__(
        self,
        id_gen: OMOPIdGenerator,
        resolver: ConceptResolver,
        date_cols: List[str],
        meas_type_concept_id: int = 0,
        cfg: Optional[MapperConfig] = None,
    ):
        self.id_gen               = id_gen
        self.resolver             = resolver
        self.date_cols            = date_cols
        self.meas_type_concept_id = meas_type_concept_id
        self.cfg                  = cfg or MapperConfig()

    def map_field(
        self,
        row: Dict[str, str],
        person_id: int,
        visit_id: Optional[int],
        route: FieldRoute,
    ) -> Optional[Dict[str, Any]]:
        raw = row.get(route.col_name, "")
        if _is_null_like(raw):
            return None
        value_as_number = _safe_float(raw)
        if value_as_number is None:
            return None
        meas_date = _best_date(row, self.date_cols)
        return {
            "measurement_id":                self.id_gen.next_id("measurement"),
            "person_id":                     person_id,
            "measurement_concept_id":        route.concept_id,
            "measurement_date":              meas_date,
            "measurement_datetime":          datetime.combine(meas_date, datetime.min.time()),
            "measurement_time":              None,
            "measurement_type_concept_id":   self.meas_type_concept_id,
            "operator_concept_id":           0,
            "value_as_number":               value_as_number,
            "value_as_concept_id":           0,
            "unit_concept_id":               0,
            "range_low":                     None,
            "range_high":                    None,
            "provider_id":                   None,
            "visit_occurrence_id":           visit_id,
            "visit_detail_id":               None,
            "measurement_source_value":      _trunc(route.col_name, self.cfg.source_value_max_length),
            "measurement_source_concept_id": route.source_concept_id,
            "unit_source_value":             "",
            "unit_source_concept_id":        0,
            "value_source_value":            _trunc(str(raw), self.cfg.source_value_max_length),
            "measurement_event_id":          None,
            "meas_event_field_concept_id":   0,
        }


# ---------------------------------------------------------------------------
# ConditionMapper
# ---------------------------------------------------------------------------

class ConditionMapper:
    def __init__(
        self,
        id_gen: OMOPIdGenerator,
        resolver: ConceptResolver,
        date_cols: List[str],
        cond_type_concept_id: int = 0,
        cond_status_concept_id: int = 0,
        cfg: Optional[MapperConfig] = None,
    ):
        self.id_gen                 = id_gen
        self.resolver               = resolver
        self.date_cols              = date_cols
        self.cond_type_concept_id   = cond_type_concept_id
        self.cond_status_concept_id = cond_status_concept_id
        self.cfg                    = cfg or MapperConfig()

    def map_field(
        self,
        row: Dict[str, str],
        person_id: int,
        visit_id: Optional[int],
        route: FieldRoute,
    ) -> Optional[Dict[str, Any]]:
        raw = row.get(route.col_name, "")
        if _is_null_like(raw):
            return None
        raw = str(raw).strip()

        if route.concept_id == 0:
            return None

        cond_date = _best_date(row, self.date_cols)
        # Both affirmative ("yes") and non-affirmative ("no") values produce
        # a record.  condition_status_concept_id signals presence vs absence;
        # condition_status_source_value preserves the original raw value so
        # "yes"/"no" is always visible in the table.
        is_present = (
            route.value_strategy == "value_as_number"
            or _is_affirmative(raw, self.cfg)
        )
        status_concept_id = self.cond_status_concept_id if is_present else 0
        return {
            "condition_occurrence_id":       self.id_gen.next_id("condition_occurrence"),
            "person_id":                     person_id,
            "condition_concept_id":          route.concept_id,
            "condition_start_date":          cond_date,
            "condition_start_datetime":      datetime.combine(cond_date, datetime.min.time()),
            "condition_end_date":            None,
            "condition_end_datetime":        None,
            "condition_type_concept_id":     self.cond_type_concept_id,
            "condition_status_concept_id":   status_concept_id,
            "stop_reason":                   None,
            "provider_id":                   None,
            "visit_occurrence_id":           visit_id,
            "visit_detail_id":               None,
            "condition_source_value":        _trunc(route.col_name, self.cfg.source_value_max_length),
            "condition_source_concept_id":   route.source_concept_id,
            "condition_status_source_value": _trunc(raw, self.cfg.source_value_max_length),
        }


# ---------------------------------------------------------------------------
# DrugMapper
# ---------------------------------------------------------------------------

class DrugMapper:
    def __init__(
        self,
        id_gen: OMOPIdGenerator,
        resolver: ConceptResolver,
        date_cols: List[str],
        drug_type_concept_id: int = 0,
        cfg: Optional[MapperConfig] = None,
    ):
        self.id_gen              = id_gen
        self.resolver            = resolver
        self.date_cols           = date_cols
        self.drug_type_concept_id = drug_type_concept_id
        self.cfg                 = cfg or MapperConfig()

    def map_field(
        self,
        row: Dict[str, str],
        person_id: int,
        visit_id: Optional[int],
        route: FieldRoute,
    ) -> Optional[Dict[str, Any]]:
        raw = row.get(route.col_name, "")
        if _is_null_like(raw):
            return None
        raw = str(raw).strip()
        if route.concept_id <= 0:
            return None
        if not _is_affirmative(raw, self.cfg):
            return None

        drug_date     = _best_date(row, self.date_cols)
        drug_datetime = datetime.combine(drug_date, datetime.min.time())
        return {
            "drug_exposure_id":              self.id_gen.next_id("drug_exposure"),
            "person_id":                     person_id,
            "drug_concept_id":               route.concept_id,
            "drug_exposure_start_date":      drug_date,
            "drug_exposure_start_datetime":  drug_datetime,
            "drug_exposure_end_date":        drug_date,
            "drug_exposure_end_datetime":    drug_datetime,
            "verbatim_end_date":             None,
            "drug_type_concept_id":          self.drug_type_concept_id,
            "stop_reason":                   None,
            "refills":                       None,
            "quantity":                      None,
            "days_supply":                   None,
            "sig":                           None,
            "route_concept_id":              0,
            "lot_number":                    None,
            "provider_id":                   None,
            "visit_occurrence_id":           visit_id,
            "visit_detail_id":               None,
            "drug_source_value":             _trunc(route.col_name, self.cfg.source_value_max_length),
            "drug_source_concept_id":        route.source_concept_id,
            "route_source_value":            "",
            "dose_unit_source_value":        "",
        }