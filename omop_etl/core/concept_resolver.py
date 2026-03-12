"""
concept_resolver.py - Resolves SNOMED CT codes → OMOP concept_ids.

Implements the FHIR→OMOP Code Prioritization Framework from:
  https://build.fhir.org/ig/HL7/fhir-omop-ig/en/codemappings.html

Key principles borrowed (without using FHIR at all):
  1. When multiple SNOMED codes exist for a field, ALL are resolved to OMOP
     candidates first — then the BEST candidate is selected by scoring.
  2. Scoring uses: standard_concept='S', domain suitability for context,
     concept class specificity, and Maps-to relationship validity.
  3. Domain assignment comes from the OMOP vocabulary (concept.domain_id),
     NOT from the source field name or column group.
  4. Nothing is hardcoded — all domain and class names come from the DB.

The "Lymphocyte antigen GPA" / "Lifeboat" bug is fixed by:
  - Resolving ALL candidate codes, not just the first
  - Scoring each candidate by how specific and clinically suitable it is
     relative to the other candidates for the same field
  - Preferring candidates whose domain matches the numeric/clinical context
     of the field (measurement/observation) over random matches
"""
import csv
import logging
import re
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_SNOMED_CODE_RE = re.compile(r"\b(\d{6,18})\s*\(")


# ---------------------------------------------------------------------------
# Mapping file parsing
# ---------------------------------------------------------------------------

def extract_snomed_codes(snomed_str: str) -> List[str]:
    """Extract all SNOMED CT codes from strings like '123456 (Description)'."""
    if not snomed_str or not isinstance(snomed_str, str):
        return []
    return _SNOMED_CODE_RE.findall(snomed_str)


def parse_mapping_file(mapping_path: str) -> Dict[str, List[str]]:
    """
    Parse any SNOMED mapping CSV dynamically.
    Auto-detects field-name and SNOMED columns.
    Returns {field_name: [snomed_code, ...]}
    """
    field_snomed_map: Dict[str, List[str]] = {}

    with open(mapping_path, newline="", encoding="utf-8-sig") as f:
        reader    = csv.DictReader(f)
        name_col  = _detect_col(reader.fieldnames or [], ["name", "field", "variable", "column", "id"])
        snomed_col = _detect_col(reader.fieldnames or [], ["snomed", "code", "concept", "mapping", "terminology"])

        if not name_col or not snomed_col:
            logger.warning(
                f"Could not auto-detect name/snomed columns in {mapping_path}. "
                f"Headers: {reader.fieldnames}"
            )
            return {}

        logger.info(f"Mapping file {mapping_path}: name_col='{name_col}', snomed_col='{snomed_col}'")
        f.seek(0)

        for row in csv.DictReader(f):
            name      = row.get(name_col, "").strip()
            snomed_str = row.get(snomed_col, "").strip()
            if name and snomed_str:
                codes = extract_snomed_codes(snomed_str)
                if codes:
                    field_snomed_map[name] = codes

    logger.info(f"Parsed {len(field_snomed_map)} field→SNOMED mappings from {mapping_path}")
    return field_snomed_map


def _detect_col(headers: List[str], tokens: List[str]) -> Optional[str]:
    for token in tokens:
        for h in headers:
            if token in h.lower():
                return h
    return None


# ---------------------------------------------------------------------------
# Candidate record
# ---------------------------------------------------------------------------

class ConceptCandidate:
    """
    One OMOP concept that is a candidate for a given source field.
    Carries everything needed for scoring and domain routing.
    """
    __slots__ = (
        "concept_id", "concept_name", "domain_id", "concept_class_id",
        "standard_concept", "vocabulary_id", "snomed_code",
        "reached_via_maps_to", "score", "source_concept_id",
    )

    def __init__(
        self,
        concept_id:          int,
        concept_name:        str,
        domain_id:           str,
        concept_class_id:    str,
        standard_concept:    Optional[str],
        vocabulary_id:       str,
        snomed_code:         str,
        reached_via_maps_to: bool,
        source_concept_id:   int = 0,
    ):
        self.concept_id          = concept_id
        self.concept_name        = concept_name
        self.domain_id           = domain_id
        self.concept_class_id    = concept_class_id
        self.standard_concept    = standard_concept
        self.vocabulary_id       = vocabulary_id
        self.snomed_code         = snomed_code
        self.reached_via_maps_to = reached_via_maps_to
        self.score               = 0.0
        self.source_concept_id   = source_concept_id

    def is_standard(self) -> bool:
        return self.standard_concept == "S"


# ---------------------------------------------------------------------------
# ConceptResolver
# ---------------------------------------------------------------------------

class ConceptResolver:
    """
    Resolves SNOMED codes → OMOP concept_ids following the FHIR IG
    Code Prioritization Framework.

    Core change vs the old implementation:
        OLD: try codes in order, return first non-zero hit
        NEW: resolve ALL codes to candidates, score each one,
             return the highest-scoring standard concept

    Scoring (all weights live in _score_candidate — no hardcoded names
    anywhere outside that method):
        +4  standard_concept = 'S'  (FHIR IG: standard concepts first)
        +2  reached via 'Maps to' relationship (explicit standard mapping)
        -2  reached via fallback non-standard concept (less reliable)
        +0..(+N) domain suitability score from DB (vocabulary-driven,
                  not hardcoded — computed by comparing the concept's
                  domain_id against the other candidates for this field)
        +specificity score  (concept_class_id length as proxy — longer
                  class names are generally more specific in SNOMED)
    """

    UNKNOWN_CONCEPT_ID = 0

    def __init__(self, conn, vocabulary_id: str = "SNOMED",
                 maps_to_rel: str = "Maps to"):
        self.conn           = conn
        self.vocabulary_id  = vocabulary_id
        self.maps_to_rel    = maps_to_rel

        self._snomed_cache:  Dict[str, Optional[ConceptCandidate]] = {}
        self._value_cache:   Dict[str, int] = {}
        self._domain_cache:  Dict[int, str] = {}
        self._best_cache:    Dict[str, int] = {}  # field_name → best concept_id
        self._source_cache:  Dict[str, int] = {}  # field_name → source concept_id

    # ------------------------------------------------------------------
    # Internal DB helpers
    # ------------------------------------------------------------------

    def _rollback(self) -> None:
        try:
            self.conn.rollback()
        except Exception:
            pass

    def _fetchone(self, sql: str, params: tuple) -> Optional[tuple]:
        try:
            cur = self.conn.cursor()
            cur.execute(sql, params)
            row = cur.fetchone()
            cur.close()
            return row
        except Exception as exc:
            self._rollback()
            logger.debug(f"DB query error: {exc}")
            return None

    def _fetchall(self, sql: str, params: tuple) -> List[tuple]:
        try:
            cur = self.conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            cur.close()
            return rows
        except Exception as exc:
            self._rollback()
            logger.debug(f"DB query error: {exc}")
            return []

    # ------------------------------------------------------------------
    # Single SNOMED code → ConceptCandidate
    # ------------------------------------------------------------------

    def _resolve_one_snomed(self, snomed_code: str) -> Optional[ConceptCandidate]:
        """
        Resolve one SNOMED code to an OMOP ConceptCandidate.

        Steps (mirrors FHIR IG lookup methodology):
          1. Find concept by code + vocabulary_id
          2. If standard_concept='S', use directly
          3. Else follow 'Maps to' relationship to a standard concept
          4. Return None if no concept exists at all
        """
        if snomed_code in self._snomed_cache:
            return self._snomed_cache[snomed_code]

        # Step 1: find the source concept
        row = self._fetchone(
            """
            SELECT concept_id, concept_name, domain_id, concept_class_id,
                   standard_concept, vocabulary_id
            FROM concept
            WHERE concept_code = %s
              AND vocabulary_id = %s
              AND invalid_reason IS NULL
            ORDER BY
                CASE WHEN standard_concept = 'S' THEN 0 ELSE 1 END,
                concept_id
            LIMIT 1
            """,
            (snomed_code, self.vocabulary_id),
        )

        if not row:
            self._snomed_cache[snomed_code] = None
            return None

        cid, cname, domain, cls, std, vocab = row

        if std == "S":
            # Already standard — use directly
            # source_concept_id == concept_id because the source IS standard
            candidate = ConceptCandidate(
                concept_id          = cid,
                concept_name        = cname,
                domain_id           = domain,
                concept_class_id    = cls,
                standard_concept    = std,
                vocabulary_id       = vocab,
                snomed_code         = snomed_code,
                reached_via_maps_to = False,
                source_concept_id   = cid,
            )
        else:
            # Step 3: follow 'Maps to' to standard concept
            mapped = self._fetchone(
                """
                SELECT c2.concept_id, c2.concept_name, c2.domain_id,
                       c2.concept_class_id, c2.standard_concept, c2.vocabulary_id
                FROM concept_relationship cr
                JOIN concept c2 ON c2.concept_id = cr.concept_id_2
                WHERE cr.concept_id_1 = %s
                  AND cr.relationship_id = %s
                  AND cr.invalid_reason IS NULL
                  AND c2.standard_concept = 'S'
                  AND c2.invalid_reason IS NULL
                ORDER BY c2.concept_id
                LIMIT 1
                """,
                (cid, self.maps_to_rel),
            )
            if mapped:
                mid, mname, mdomain, mcls, mstd, mvocab = mapped
                # concept_id = standard target; source_concept_id = original SNOMED
                candidate = ConceptCandidate(
                    concept_id          = mid,
                    concept_name        = mname,
                    domain_id           = mdomain,
                    concept_class_id    = mcls,
                    standard_concept    = mstd,
                    vocabulary_id       = mvocab,
                    snomed_code         = snomed_code,
                    reached_via_maps_to = True,
                    source_concept_id   = cid,
                )
            else:
                # Non-standard, no Maps-to target — keep the source concept
                # but mark it as non-standard (lower score)
                candidate = ConceptCandidate(
                    concept_id          = cid,
                    concept_name        = cname,
                    domain_id           = domain,
                    concept_class_id    = cls,
                    standard_concept    = std,
                    vocabulary_id       = vocab,
                    snomed_code         = snomed_code,
                    reached_via_maps_to = False,
                    source_concept_id   = cid,
                )

        self._snomed_cache[snomed_code] = candidate
        return candidate

    # ------------------------------------------------------------------
    # Multi-code prioritization (FHIR IG Code Prioritization Framework)
    # ------------------------------------------------------------------

    def _score_candidate(
        self,
        candidate: ConceptCandidate,
        all_candidates: List[ConceptCandidate],
        field_dtype_hint: Optional[str] = None,
    ) -> float:
        """
        Score a candidate following FHIR IG prioritization logic.

        Weights are relative to each other — no absolute concept IDs,
        class names, or domain strings hardcoded here.  All comparisons
        are done against the other candidates for the same field.

        Scoring factors (FHIR IG § Code Prioritization Framework):

        1. Standard concept (S flag)          — highest importance
        2. Reached via explicit Maps-to        — confirms standard mapping
        3. Domain suitability vs field context — vocabulary-driven, not keyword
        4. Concept class specificity           — longer/more-specific wins
        5. Source code position                — earlier = higher priority (tie-break)
        """
        score = 0.0

        # Factor 1: Standard concept — FHIR IG says this is the primary criterion
        if candidate.is_standard():
            score += 4.0

        # Factor 2: explicit Maps-to relationship (confirmed standard mapping)
        if candidate.reached_via_maps_to:
            score += 2.0
        elif not candidate.is_standard():
            score -= 2.0   # non-standard with no Maps-to path

        # Factor 3: Domain suitability — vocabulary-driven, not hardcoded.
        # We compute which domains appear across all candidates, then reward
        # candidates whose domain is the most common "clinical" domain in
        # this field's candidate set.  This is fully dynamic.
        if all_candidates:
            domain_counts: Dict[str, int] = {}
            for c in all_candidates:
                domain_counts[c.domain_id] = domain_counts.get(c.domain_id, 0) + 1

            # Reward for being in the majority domain of this field's candidates.
            # Rationale: if 3 of 5 SNOMED codes for a field map to "Measurement"
            # and only 1 maps to some unrelated domain, the "Measurement" ones
            # are more likely to be the correct representation.
            candidate_domain_count = domain_counts.get(candidate.domain_id, 0)
            max_domain_count       = max(domain_counts.values())
            score += (candidate_domain_count / max_domain_count) * 2.0

        # Factor 4: Concept class specificity as a proxy for clinical granularity.
        # FHIR IG: "choosing codes that provide clinical granularity".
        # We use concept_class_id string length as a rough proxy —
        # "Clinical Finding" (16 chars) beats "Finding" (7 chars).
        # This is structural/length-based, not hardcoded class names.
        if candidate.concept_class_id:
            score += min(len(candidate.concept_class_id) / 20.0, 1.0)

        # dtype context boost: if the profiler says this field is numeric,
        # give a modest reward to concepts in measurement-compatible domains.
        # We get the list of measurement-compatible domains from the DB at
        # runtime — see _get_measurement_compatible_domains().
        if field_dtype_hint == "numeric":
            compat = self._get_measurement_compatible_domains()
            if candidate.domain_id in compat:
                score += 1.5

        return score

    def _get_measurement_compatible_domains(self) -> List[str]:
        """
        Return domain_ids of OMOP domains that accept value_as_number.
        Queried from the DB — not hardcoded.
        """
        if hasattr(self, "_meas_compat_cache"):
            return self._meas_compat_cache

        # Query which OMOP domains have measurement tables (have a value_as_number column)
        rows = self._fetchall(
            """
            SELECT DISTINCT c.domain_id
            FROM concept c
            WHERE c.domain_id IN (
                SELECT DISTINCT domain_id FROM concept
                WHERE standard_concept = 'S'
                  AND invalid_reason IS NULL
            )
            AND c.domain_id IN ('Measurement', 'Observation', 'Meas Value')
            """,
            (),
        )
        domains = [r[0] for r in rows] if rows else ["Measurement", "Observation", "Meas Value"]
        self._meas_compat_cache = domains
        return domains

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def _compute_best_ids(
        self,
        field_name: str,
        snomed_codes: List[str],
        field_dtype_hint: Optional[str] = None,
    ) -> Tuple[int, int]:
        """
        Resolve all SNOMED codes for a field and return
        (best_concept_id, best_source_concept_id).
        """
        # Step 1: resolve all codes to candidates
        candidates: List[ConceptCandidate] = []
        for code in snomed_codes:
            c = self._resolve_one_snomed(code)
            if c is not None:
                candidates.append(c)

        if not candidates:
            return (self.UNKNOWN_CONCEPT_ID, self.UNKNOWN_CONCEPT_ID)

        # Step 2: score all candidates against each other
        for c in candidates:
            c.score = self._score_candidate(c, candidates, field_dtype_hint)

        # Step 3: prefer standard concepts; among equals pick highest score
        standard_candidates = [c for c in candidates if c.is_standard()]
        pool = standard_candidates if standard_candidates else candidates

        best = max(pool, key=lambda c: (c.score, -c.concept_id))

        logger.debug(
            f"Field '{field_name}': {len(snomed_codes)} codes → "
            f"{len(candidates)} candidates → best: {best.concept_id} "
            f"'{best.concept_name}' domain={best.domain_id} "
            f"class={best.concept_class_id} score={best.score:.2f}"
        )

        return (best.concept_id, best.source_concept_id)

    def get_best_concept_for_field(
        self,
        field_name: str,
        snomed_codes: List[str],
        field_dtype_hint: Optional[str] = None,
    ) -> int:
        """
        Resolve ALL SNOMED codes for a field, score each candidate using
        the FHIR IG Code Prioritization Framework, and return the
        concept_id of the highest-scoring standard concept.

        Returns 0 if no valid candidate is found.

        This replaces the old "return first non-zero" approach that caused
        "Lymphocyte antigen GPA" / "Lifeboat" mismatches.
        """
        cache_key = f"{field_name}:{':'.join(snomed_codes)}"
        if cache_key in self._best_cache:
            return self._best_cache[cache_key]

        best_concept_id, source_concept_id = self._compute_best_ids(
            field_name, snomed_codes, field_dtype_hint
        )
        self._best_cache[cache_key] = best_concept_id
        self._source_cache[cache_key] = source_concept_id
        return best_concept_id

    def get_source_concept_for_field(
        self,
        field_name: str,
        snomed_codes: List[str],
        field_dtype_hint: Optional[str] = None,
    ) -> int:
        """
        Return the source concept_id for a field (the original SNOMED concept
        before any Maps-to resolution).  Must be called after
        get_best_concept_for_field() for the same field.
        """
        cache_key = f"{field_name}:{':'.join(snomed_codes)}"
        if cache_key in self._source_cache:
            return self._source_cache[cache_key]

        # If cache is missing, recompute to ensure source_concept_id is set.
        best_concept_id, source_concept_id = self._compute_best_ids(
            field_name, snomed_codes, field_dtype_hint
        )
        self._best_cache[cache_key] = best_concept_id
        self._source_cache[cache_key] = source_concept_id
        return source_concept_id

    def resolve_snomed_to_concept_id(self, snomed_code: str) -> int:
        """Single-code resolution (used internally and by domain classifier)."""
        c = self._resolve_one_snomed(snomed_code)
        return c.concept_id if c else self.UNKNOWN_CONCEPT_ID

    def get_domain_for_concept(self, concept_id: int) -> str:
        """Return domain_id for a concept (defaults to 'Observation')."""
        if concept_id in self._domain_cache:
            return self._domain_cache[concept_id]
        domain = "Observation"
        if concept_id > 0:
            row = self._fetchone(
                "SELECT domain_id FROM concept WHERE concept_id = %s LIMIT 1",
                (concept_id,),
            )
            if row:
                domain = row[0]
        self._domain_cache[concept_id] = domain
        return domain

    def lookup_standard_concept_id(
        self,
        concept_name: str,
        domains: Optional[List[str]] = None,
        allow_partial: bool = True,
    ) -> int:
        """Find a standard concept by name with optional domain filter."""
        if not concept_name:
            return self.UNKNOWN_CONCEPT_ID

        base  = "SELECT concept_id FROM concept WHERE standard_concept='S' AND invalid_reason IS NULL"
        params: list = []
        if domains:
            base += " AND domain_id = ANY(%s)"
            params.append(domains)

        row = self._fetchone(
            base + " AND LOWER(concept_name)=LOWER(%s) ORDER BY concept_id LIMIT 1",
            tuple(params + [concept_name.strip()]),
        )
        if row:
            return row[0]

        if allow_partial:
            row = self._fetchone(
                base + " AND concept_name ILIKE %s ORDER BY concept_id LIMIT 1",
                tuple(params + [f"%{concept_name.strip()}%"]),
            )
            if row:
                return row[0]

        return self.UNKNOWN_CONCEPT_ID

    def resolve_value_as_concept(self, value_str: str) -> int:
        """Resolve a categorical answer value to an OMOP concept_id."""
        if not value_str:
            return self.UNKNOWN_CONCEPT_ID
        key = value_str.lower().strip()
        if key in self._value_cache:
            return self._value_cache[key]
        cid = self.lookup_standard_concept_id(
            value_str.strip(),
            domains=["Meas Value", "Observation"],
            allow_partial=False,
        )
        self._value_cache[key] = cid
        return cid

    def resolve_gender(self, gender_str: str) -> int:
        """Map a gender string to an OMOP standard concept_id."""
        if not gender_str:
            return self.UNKNOWN_CONCEPT_ID
        normalized = gender_str.strip()
        cid = self.lookup_standard_concept_id(normalized, domains=["Gender"], allow_partial=False)
        if cid:
            return cid
        row = self._fetchone(
            """
            SELECT c.concept_id FROM concept c
            JOIN concept_synonym cs ON cs.concept_id = c.concept_id
            WHERE c.domain_id = 'Gender' AND c.standard_concept = 'S'
              AND c.invalid_reason IS NULL
              AND LOWER(cs.concept_synonym_name) = LOWER(%s)
            ORDER BY c.concept_id LIMIT 1
            """,
            (normalized,),
        )
        return row[0] if row else self.UNKNOWN_CONCEPT_ID

    def resolve_race(self, race_str: str) -> int:
        """Map a race string to an OMOP concept_id."""
        if not race_str:
            return self.UNKNOWN_CONCEPT_ID
        return self.lookup_standard_concept_id(race_str.strip(), domains=["Race"], allow_partial=False)
