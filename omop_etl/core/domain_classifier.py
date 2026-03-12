"""
domain_classifier.py - Routes each CSV column to the correct OMOP table.

Key change: passes col.dtype as field_dtype_hint to get_best_concept_for_field
so the concept prioritization framework can reward measurement-compatible
concepts for numeric fields.
"""
from dataclasses import dataclass
from typing import Dict, List, Optional
import logging

from core.profiler import ColumnProfile, DatasetProfile

logger = logging.getLogger(__name__)

# OMOP domain → CDM table (CDM spec — appropriate here, not in resolver)
DOMAIN_TO_TABLE = {
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
}

VALUE_STRATEGY_NUMERIC  = "value_as_number"
VALUE_STRATEGY_CONCEPT  = "value_as_concept_id"
VALUE_STRATEGY_STRING   = "value_as_string"
VALUE_STRATEGY_SKIP     = "skip"

# Domains that are numerically compatible
_NUMERIC_COMPATIBLE_DOMAINS = {"Measurement", "Observation", "Meas Value"}


@dataclass
class FieldRoute:
    col_name:          str
    target_table:      str
    concept_id:        int
    value_strategy:    str
    domain:            str
    is_mapped:         bool
    source_concept_id: int = 0


class DomainClassifier:
    """
    Classifies each CSV column to an OMOP target table and value strategy.

    Passes col.dtype as a hint to ConceptResolver so the multi-code
    prioritization can prefer measurement-domain concepts for numeric fields.
    """

    def __init__(self, concept_resolver, field_snomed_map: Dict[str, List[str]]):
        self.resolver         = concept_resolver
        self.field_snomed_map = field_snomed_map

    def _should_skip(self, col: ColumnProfile) -> bool:
        return (
            col.dtype == "empty"
            or col.n_missing == col.n_total
            or col.domain_hint == "meta"
            or col.is_person_id
            or col.is_gender
            or col.is_age
            or col.is_birth_date
        )

    def classify_column(self, col: ColumnProfile) -> FieldRoute:
        if self._should_skip(col):
            return FieldRoute(col.name, "", 0, VALUE_STRATEGY_SKIP, "", False, 0)

        snomed_codes = self.field_snomed_map.get(col.name, [])
        concept_id   = 0
        source_concept_id = 0
        domain       = ""
        is_mapped    = False

        if snomed_codes:
            # Pass dtype hint so the resolver can weight candidates correctly
            concept_id = self.resolver.get_best_concept_for_field(
                col.name, snomed_codes, field_dtype_hint=col.dtype
            )
            if concept_id > 0:
                domain    = self.resolver.get_domain_for_concept(concept_id)
                is_mapped = True
            # Retrieve the original source SNOMED concept_id
            source_concept_id = self.resolver.get_source_concept_for_field(
                col.name, snomed_codes, field_dtype_hint=col.dtype
            )

        is_numeric = col.dtype == "numeric"

        if is_numeric:
            target_table = "measurement"
            domain       = "Measurement"
            # If the best concept resolved to an incompatible domain, discard it
            if concept_id > 0:
                concept_domain = self.resolver.get_domain_for_concept(concept_id)
                if concept_domain not in _NUMERIC_COMPATIBLE_DOMAINS:
                    concept_id = 0
                    is_mapped  = False
        elif domain and domain in DOMAIN_TO_TABLE:
            target_table = DOMAIN_TO_TABLE[domain]
        else:
            target_table = "observation"
            domain       = "Observation"

        # Value strategy
        if is_numeric:
            value_strategy = VALUE_STRATEGY_NUMERIC
        elif col.dtype in ("boolean", "categorical"):
            value_strategy = VALUE_STRATEGY_CONCEPT
        elif col.dtype == "text":
            value_strategy = VALUE_STRATEGY_STRING
        else:
            value_strategy = VALUE_STRATEGY_CONCEPT

        return FieldRoute(col.name, target_table, concept_id, value_strategy, domain, is_mapped, source_concept_id)

    def classify_all(self, dataset_profile: DatasetProfile) -> Dict[str, FieldRoute]:
        routes: Dict[str, FieldRoute] = {}
        stats: Dict[str, int]         = {}

        for col_name, col_profile in dataset_profile.columns.items():
            route       = self.classify_column(col_profile)
            routes[col_name] = route
            key = route.target_table if route.value_strategy != VALUE_STRATEGY_SKIP else "skip"
            stats[key]  = stats.get(key, 0) + 1

        mapped = sum(1 for r in routes.values() if r.is_mapped)
        logger.info(
            f"Classification: {mapped} SNOMED-mapped, "
            + ", ".join(f"{k}={v}" for k, v in sorted(stats.items()))
        )
        return routes
