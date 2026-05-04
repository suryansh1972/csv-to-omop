"""
domain_classifier.py - Routes each CSV column to the correct OMOP table.

Domain routing is vocabulary-driven: the OMOP concept's domain_id
is the authority for which table a column maps to.  col.dtype is passed
as a scoring hint to ConceptResolver so that numeric fields prefer
measurement-compatible concepts during multi-code prioritisation.

If no concept resolves, the column is skipped — there is no basis for
routing it anywhere and silent misfiling is worse than omission. Numeric
fields that resolve to non-numeric-compatible domains are also skipped
to prevent cross-domain misrouting (see DomainConfig).
"""
from dataclasses import dataclass
from typing import Dict, List, Optional
import logging

from config.settings import DomainConfig
from core.profiler import ColumnProfile, DatasetProfile

logger = logging.getLogger(__name__)

VALUE_STRATEGY_NUMERIC  = "value_as_number"
VALUE_STRATEGY_CONCEPT  = "value_as_concept_id"
VALUE_STRATEGY_STRING   = "value_as_string"
VALUE_STRATEGY_SKIP     = "skip"


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

    Routing is vocabulary-driven: the OMOP concept's domain_id
    determines the target table.  Columns with no resolved concept are
    skipped — dtype is never used as a routing fallback. Numeric fields
    mapped to non-numeric-compatible domains are skipped to avoid
    cross-domain misrouting.

    col.dtype is passed as a scoring hint to ConceptResolver so that
    numeric fields receive a modest boost toward measurement-compatible
    concepts during multi-code prioritisation.  It influences which concept
    wins the scoring race; it has no role after that.
    """

    def __init__(
        self,
        concept_resolver,
        field_snomed_map: Dict[str, List[str]],
        domain_cfg: Optional[DomainConfig] = None,
    ):
        self.resolver         = concept_resolver
        self.field_snomed_map = field_snomed_map
        self.domain_cfg       = domain_cfg or DomainConfig()

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

        snomed_codes      = self.field_snomed_map.get(col.name, [])
        concept_id        = 0
        source_concept_id = 0
        domain            = ""
        is_mapped         = False

        if snomed_codes:
            # dtype is passed only as a scoring hint so the resolver can
            # prefer measurement-compatible concepts for numeric fields.
            # It does NOT drive the routing decision below.
            concept_id = self.resolver.get_best_concept_for_field(
                col.name, snomed_codes, field_dtype_hint=col.dtype
            )
            if concept_id > 0:
                domain    = self.resolver.get_domain_for_concept(concept_id)
                is_mapped = True
            source_concept_id = self.resolver.get_source_concept_for_field(
                col.name, snomed_codes, field_dtype_hint=col.dtype
            )

        # ── Table routing — vocabulary only ──────────────────────────────
        # The concept's domain_id from the OMOP vocabulary is the sole
        # authority.  If no concept resolved, the column is skipped —
        # there is no basis for routing it anywhere.
        if not (domain and domain in self.domain_cfg.domain_to_table):
            logger.debug(f"Column '{col.name}': no concept resolved — skipping")
            return FieldRoute(col.name, "", 0, VALUE_STRATEGY_SKIP, "", False, 0)

        # Guard: numeric fields should only map into numeric-compatible domains.
        # If a numeric field resolves to a non-numeric domain, skip to avoid
        # cross-domain misrouting (see DomainConfig.numeric_compatible_domains).
        if col.dtype == "numeric" and domain not in self.domain_cfg.numeric_compatible_domains:
            logger.debug(
                f"Column '{col.name}': numeric dtype but domain '{domain}' "
                f"is not numeric-compatible — skipping"
            )
            return FieldRoute(col.name, "", 0, VALUE_STRATEGY_SKIP, "", False, 0)

        target_table = self.domain_cfg.domain_to_table[domain]

        # ── Value strategy — driven by col.dtype only ────────────────────
        # This is independent of the routing decision above.  A drug_exposure
        # row can still carry value_as_number (e.g. a dose quantity field).
        value_strategy = self.domain_cfg.dtype_to_value_strategy.get(
            col.dtype, VALUE_STRATEGY_CONCEPT
        )

        if value_strategy == VALUE_STRATEGY_SKIP:
            return FieldRoute(col.name, "", 0, VALUE_STRATEGY_SKIP, "", False, 0)

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
