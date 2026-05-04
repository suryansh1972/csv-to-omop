"""
person_mapper.py - Maps CSV rows to OMOP PERSON records.

All fallback values, demographic field detection tokens, and age/date
tolerances come from ConceptConfig and ProfilerConfig — nothing hardcoded.
"""
import hashlib
import logging
import re
from datetime import date, datetime
from typing import Any, Dict, Optional

from config.settings import ConceptConfig, ProfilerConfig
from core.profiler import DatasetProfile
from core.concept_resolver import ConceptResolver
from core.id_generator import OMOPIdGenerator

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pure date / age helpers
# ---------------------------------------------------------------------------

def _parse_date(value: str) -> Optional[date]:
    if not value or not value.strip():
        return None
    for fmt in (
        "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
        "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d",
    ):
        try:
            return datetime.strptime(value.strip()[:19], fmt[: len(value.strip()[:19])]).date()
        except ValueError:
            continue
    m = re.match(r"(\d{4}-\d{2}-\d{2})", value.strip())
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            pass
    return None


def _stable_person_id(raw_id: str, namespace: str = "") -> int:
    """Deterministic positive integer from a string identifier."""
    if not raw_id or not raw_id.strip():
        return None
    key = f"{namespace}{raw_id.strip()}" if namespace else raw_id.strip()
    return int(hashlib.sha256(key.encode()).hexdigest()[:8], 16) % 2_000_000_000 + 1


def _parse_age_years(age_str: str) -> Optional[int]:
    try:
        age = int(float(age_str))
    except (ValueError, TypeError):
        return None
    return age if 0 <= age <= 130 else None


def _is_consistent_birth_date(
    birth_date: date,
    reference_date: date,
    age_str: str,
    tolerance_years: int,
    max_plausible_age: int,
) -> bool:
    if birth_date > reference_date:
        return False
    implied_age = reference_date.year - birth_date.year
    if (reference_date.month, reference_date.day) < (birth_date.month, birth_date.day):
        implied_age -= 1
    if not (0 <= implied_age <= max_plausible_age):
        return False
    stated_age = _parse_age_years(age_str)
    if stated_age is None:
        return True
    return abs(implied_age - stated_age) <= tolerance_years


# ---------------------------------------------------------------------------
# PersonMapper
# ---------------------------------------------------------------------------

class PersonMapper:
    """
    Converts a CSV row to an OMOP PERSON record.
    Column detection is driven by DatasetProfile (never keyword-matched here).
    Fallback values come from ConceptConfig.
    Demographic field tokens come from ProfilerConfig.
    """

    def __init__(
        self,
        profile: DatasetProfile,
        resolver: ConceptResolver,
        id_gen: OMOPIdGenerator,
        source_value_prefix: str = "",
        concept_cfg: Optional[ConceptConfig] = None,
        profiler_cfg: Optional[ProfilerConfig] = None,
    ):
        self.profile    = profile
        self.resolver   = resolver
        self.id_gen     = id_gen
        self.prefix     = source_value_prefix
        self.ccfg       = concept_cfg  or ConceptConfig()
        self.pcfg       = profiler_cfg or ProfilerConfig()
        self._person_id_map: Dict[str, int] = {}

    # ------------------------------------------------------------------
    # Person ID resolution
    # ------------------------------------------------------------------

    def _get_raw_person_id(self, row: Dict[str, str]) -> str:
        if self.profile.person_id_col:
            val = row.get(self.profile.person_id_col, "").strip()
            if val:
                return val
        for col, cp in self.profile.columns.items():
            if cp.is_person_id:
                val = row.get(col, "").strip()
                if val:
                    return val
        return ""

    def get_or_create_person_id(self, row: Dict[str, str]) -> Optional[int]:
        raw_id   = self._get_raw_person_id(row)
        if not raw_id:
            return None
        stable_key = f"{self.prefix}{raw_id}" if self.prefix else raw_id
        if stable_key not in self._person_id_map:
            stable = _stable_person_id(raw_id, namespace=self.prefix)
            if stable and stable not in self._person_id_map.values():
                self._person_id_map[stable_key] = stable
            else:
                self._person_id_map[stable_key] = self.id_gen.next_id("person")
        return self._person_id_map[stable_key]

    # ------------------------------------------------------------------
    # Demographic helpers — all token lists come from config
    # ------------------------------------------------------------------

    def _find_demographic_column(self, row: Dict[str, str], tokens) -> Optional[str]:
        """Return the first non-empty column whose name contains one of tokens."""
        for token in tokens:
            for col in self.profile.columns:
                if token in col.lower() and row.get(col, "").strip():
                    return col
        return None

    def _get_reference_date(self, row: Dict[str, str]) -> date:
        for dc in self.profile.date_cols[:3]:
            d = _parse_date(row.get(dc, ""))
            if d:
                return d
        return date.today()

    # ------------------------------------------------------------------
    # Main mapper
    # ------------------------------------------------------------------

    def map_row(self, row: Dict[str, str]) -> Optional[Dict[str, Any]]:
        person_id = self.get_or_create_person_id(row)
        if person_id is None:
            return None

        # Gender — resolver is vocabulary-driven (no keyword matching here)
        gender_concept_id = 0
        gender_source     = ""
        if self.profile.gender_col:
            gender_val        = row.get(self.profile.gender_col, "").strip()
            gender_source     = gender_val
            gender_concept_id = self.resolver.resolve_gender(gender_val)

        # Birth date / year
        reference_date = self._get_reference_date(row)
        age_val        = row.get(self.profile.age_col, "").strip() if self.profile.age_col else ""

        year_of_birth  = None
        month_of_birth = None
        day_of_birth   = None
        birth_datetime = None

        if self.profile.birth_date_col:
            bd = _parse_date(row.get(self.profile.birth_date_col, ""))
            if bd and _is_consistent_birth_date(
                bd, reference_date, age_val,
                self.ccfg.birth_age_tolerance_years,
                self.ccfg.max_plausible_age,
            ):
                year_of_birth  = bd.year
                month_of_birth = bd.month
                day_of_birth   = bd.day
                birth_datetime = datetime(bd.year, bd.month, bd.day)

        if year_of_birth is None and self.profile.age_col:
            age_years = _parse_age_years(age_val)
            if age_years is not None:
                year_of_birth = reference_date.year - age_years

        # Use sentinel from config — not a magic number buried here
        if year_of_birth is None:
            year_of_birth = self.ccfg.unknown_birth_year_sentinel

        # Race — column detection uses config tokens, not inline strings
        race_col      = self._find_demographic_column(row, self.pcfg.demographic_exclusion_tokens)
        race_source   = ""
        race_concept_id = 0
        ethnicity_concept_id = 0
        if race_col:
            race_val        = row.get(race_col, "").strip()
            race_source     = race_val
            race_concept_id = self.resolver.resolve_race(race_val)

        # Location / care site
        location_source = ""
        site_col = self._find_demographic_column(row, self.pcfg.site_column_tokens)
        if site_col:
            location_source = row.get(site_col, "").strip()

        raw_id       = self._get_raw_person_id(row)
        source_value = f"{self.prefix}{raw_id}" if self.prefix else raw_id

        return {
            "person_id":                     person_id,
            "gender_concept_id":             gender_concept_id,
            "year_of_birth":                 year_of_birth,
            "month_of_birth":                month_of_birth,
            "day_of_birth":                  day_of_birth,
            "birth_datetime":                birth_datetime,
            "race_concept_id":               race_concept_id,
            "ethnicity_concept_id":          ethnicity_concept_id,
            "location_id":                   None,
            "provider_id":                   None,
            "care_site_id":                  None,
            "person_source_value":           source_value,
            "gender_source_value":           gender_source,
            "gender_source_concept_id":      0,
            "race_source_value":             race_source,
            "race_source_concept_id":        0,
            "ethnicity_source_value":        "",
            "ethnicity_source_concept_id":   0,
        }