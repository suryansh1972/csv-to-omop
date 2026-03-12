"""
profiler.py - Dynamically profiles any CSV dataset.

Detects:
  - Person identifier columns from value structure and uniqueness
  - Date columns from parseable date content
  - Numeric vs categorical vs free-text columns
  - Support columns such as age / birth date / gender from data itself

Avoids dataset-specific keyword lists. Inference is based on observed values,
column cardinality, and optional OMOP concept resolution.
"""
import csv
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

NULL_LIKE = {"", "NA", "N/A", "na", "n/a", "null", "NULL"}
BOOLEAN_VALUES = {"yes", "no", "true", "false", "1", "0", "y", "n"}
DATE_FORMATS = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
)
ID_SAFE_RE = re.compile(r"^[A-Za-z0-9._:/-]+$")
UUID_LIKE_RE = re.compile(
    r"^(uuid:)?[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
TIME_LIKE_RE = re.compile(r"^\d{1,2}([:.]\d{1,2})?\s*(am|pm|noon)?\.?$", re.IGNORECASE)
IDENTIFIER_NAME_HINTS = (
    "subject_id",
    "subjectid",
    "participant_id",
    "participantid",
    "patient_id",
    "patientid",
    "person_id",
    "personid",
    "member_id",
    "memberid",
    "study_id",
    "studyid",
    "record_id",
    "recordid",
    "mrn",
    "uhid",
    "empi",
)
IDENTIFIER_NAME_WEAK_HINTS = (
    "subject",
    "participant",
    "patient",
    "person",
    "member",
    "record",
    "study",
    "barcode",
)
UUID_IDENTIFIER_HINTS = (
    "instanceid",
    "instance_id",
    "uuid",
    "submissionid",
    "submission_id",
)


@dataclass
class ColumnProfile:
    name: str
    clean_name: str
    group: str
    dtype: str
    domain_hint: str
    n_unique: int = 0
    n_missing: int = 0
    n_total: int = 0
    sample_values: List = field(default_factory=list)
    is_person_id: bool = False
    is_date: bool = False
    is_gender: bool = False
    is_age: bool = False
    is_birth_date: bool = False
    numeric_min: Optional[float] = None
    numeric_max: Optional[float] = None
    non_empty_count: int = 0
    unique_ratio: float = 0.0
    date_parse_rate: float = 0.0
    numeric_parse_rate: float = 0.0
    boolean_parse_rate: float = 0.0
    id_like_rate: float = 0.0
    avg_length: float = 0.0
    median_numeric: Optional[float] = None
    median_year: Optional[int] = None
    integer_like_rate: float = 0.0
    uuid_like_rate: float = 0.0
    time_like_rate: float = 0.0


@dataclass
class DatasetProfile:
    columns: Dict[str, ColumnProfile] = field(default_factory=dict)
    person_id_col: Optional[str] = None
    person_id_candidates: List[str] = field(default_factory=list)
    date_cols: List[str] = field(default_factory=list)
    gender_col: Optional[str] = None
    age_col: Optional[str] = None
    birth_date_col: Optional[str] = None
    n_rows: int = 0
    source_name: str = ""
    groups: Dict[str, List[str]] = field(default_factory=dict)


def _clean_col_name(name: str) -> tuple[str, str]:
    """Split grouped names such as 'group:field' or 'group.field'."""
    if ":" in name:
        group, field_name = name.split(":", 1)
        return group.strip(), field_name.strip()
    if "." in name:
        group, field_name = name.split(".", 1)
        return group.strip(), field_name.strip()
    return "", name.strip()


def _is_nullish(value: str) -> bool:
    return value is None or str(value).strip() in NULL_LIKE


def _normalize_values(values: List[str]) -> List[str]:
    return [str(v).strip() for v in values if not _is_nullish(v)]


def _parse_date_value(value: str) -> Optional[date]:
    if _is_nullish(value):
        return None
    text = str(value).strip()

    if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
        try:
            return datetime.strptime(text, "%Y-%m-%d").date()
        except ValueError:
            return None

    if re.match(r"^\d{4}-\d{2}-\d{2}[T ]", text):
        date_part = text.split("T", 1)[0].split(" ", 1)[0]
        try:
            return datetime.strptime(date_part, "%Y-%m-%d").date()
        except ValueError:
            return None

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _safe_float(value: str) -> Optional[float]:
    if _is_nullish(value):
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _infer_dtype(non_empty: List[str], date_rate: float, numeric_rate: float, boolean_rate: float) -> str:
    if not non_empty:
        return "empty"
    if boolean_rate >= 0.95:
        return "boolean"
    if date_rate >= 0.9:
        return "date"
    if numeric_rate >= 0.9:
        return "numeric"
    if len(set(non_empty)) <= max(10, len(non_empty) * 0.1):
        return "categorical"
    return "text"


def _default_domain_hint(dtype: str) -> str:
    if dtype == "date":
        return "meta"
    if dtype == "numeric":
        return "measurement"
    return "observation"


def _score_person_id(cp: ColumnProfile) -> float:
    if cp.dtype in ("empty", "date"):
        return -1.0
    if cp.non_empty_count == 0:
        return -1.0
    if (cp.non_empty_count / max(cp.n_total, 1)) < 0.8:
        return -1.0
    if cp.unique_ratio < 0.8:           # relaxed from 0.9 → handles small datasets (14 rows)
        return -1.0
    if cp.id_like_rate < 0.5:           # relaxed from 0.8 → barcodes/hyphens still qualify
        return -1.0
    if cp.avg_length > 80:              # relaxed from 48 → barcode strings can be longer
        return -1.0
    if cp.time_like_rate > 0.5:
        return -1.0
    score = 0.0
    score += cp.unique_ratio * 4.0
    score += (1.0 - (cp.n_missing / max(cp.n_total, 1))) * 2.0
    score += cp.id_like_rate * 2.0
    if cp.avg_length > 64:
        score -= 2.0
    if cp.dtype == "text":
        score -= 1.0
    score -= cp.uuid_like_rate * 5.0
    if cp.boolean_parse_rate > 0.9 or cp.date_parse_rate > 0.5:
        score -= 3.0
    if cp.numeric_max is not None and cp.numeric_max <= 150:
        score -= 2.0
    return score


def _normalize_identifier_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _identifier_name_bonus(cp: ColumnProfile) -> float:
    tokens = {
        _normalize_identifier_name(cp.name),
        _normalize_identifier_name(cp.clean_name),
    }
    bonus = 0.0
    if any(hint in token for token in tokens for hint in IDENTIFIER_NAME_HINTS):
        bonus += 2.5
    elif any(hint in token for token in tokens for hint in IDENTIFIER_NAME_WEAK_HINTS):
        bonus += 1.0
    if any(token.endswith("_id") or token.endswith("id") for token in tokens):
        bonus += 0.5
    return bonus


def _looks_like_uuid_identifier(cp: ColumnProfile) -> bool:
    if cp.uuid_like_rate >= 0.8:
        return True
    tokens = {
        _normalize_identifier_name(cp.name),
        _normalize_identifier_name(cp.clean_name),
    }
    return any(hint in token for token in tokens for hint in UUID_IDENTIFIER_HINTS)


def _rank_person_id_candidates(
    columns: Dict[str, ColumnProfile],
    allow_uuid_person_id: bool = False,
) -> List[Tuple[str, float]]:
    ranked = []
    for col_name, cp in columns.items():
        score = _score_person_id(cp)
        if score < 0:
            continue
        score += _identifier_name_bonus(cp)
        if _looks_like_uuid_identifier(cp) and not allow_uuid_person_id:
            score -= 4.0
        ranked.append((col_name, score))
    ranked.sort(key=lambda item: item[1], reverse=True)
    return ranked


def _mark_person_id_column(profile: DatasetProfile, col_name: str) -> None:
    for cp in profile.columns.values():
        cp.is_person_id = False
    profile.person_id_col = col_name
    profile.columns[col_name].is_person_id = True


def resolve_person_id_column(
    profile: DatasetProfile,
    preferred_column: Optional[str] = None,
    allow_uuid_person_id: bool = False,
) -> Optional[str]:
    """
    Resolve and validate the source person identifier column.

    Args:
        profile: Dataset profile with column statistics populated.
        preferred_column: Explicit identifier column override from the caller.
        allow_uuid_person_id: Allow UUID-style record identifiers such as
            ODK instance IDs to become OMOP person keys.
    """
    profile.person_id_candidates = [
        col_name for col_name, _ in _rank_person_id_candidates(
            profile.columns,
            allow_uuid_person_id=allow_uuid_person_id,
        )[:5]
    ]

    if preferred_column:
        if preferred_column not in profile.columns:
            raise RuntimeError(
                f"Configured person identifier column '{preferred_column}' was not found in the CSV."
            )
        cp = profile.columns[preferred_column]
        if cp.non_empty_count == 0:
            raise RuntimeError(
                f"Configured person identifier column '{preferred_column}' is empty for all rows."
            )
        if cp.unique_ratio < 0.9:
            raise RuntimeError(
                f"Configured person identifier column '{preferred_column}' is not unique enough "
                f"for person identity (unique_ratio={cp.unique_ratio:.2f})."
            )
        if _looks_like_uuid_identifier(cp) and not allow_uuid_person_id:
            raise RuntimeError(
                f"Configured person identifier column '{preferred_column}' looks like a submission "
                f"or record UUID. Re-run with --allow-uuid-person-id only if each UUID should "
                f"represent a distinct person in OMOP."
            )
        _mark_person_id_column(profile, preferred_column)
        return preferred_column

    ranked = _rank_person_id_candidates(profile.columns, allow_uuid_person_id=allow_uuid_person_id)
    if ranked and ranked[0][1] >= 3.0:  # lowered: small datasets (14 rows) hit ~3.5
        _mark_person_id_column(profile, ranked[0][0])
        return ranked[0][0]

    profile.person_id_col = None
    return None


def _score_age(cp: ColumnProfile) -> float:
    if cp.dtype != "numeric" or cp.numeric_min is None or cp.numeric_max is None:
        return -1.0
    if cp.numeric_min < 0 or cp.numeric_max > 130:
        return -1.0
    if cp.numeric_max < 25:
        return -1.0
    if cp.median_numeric is None or cp.median_numeric < 18 or cp.median_numeric > 100:
        return -1.0
    if cp.integer_like_rate < 0.8:
        return -1.0
    score = 0.0
    score += min(cp.unique_ratio, 1.0) * 2.0
    score += (1.0 - (cp.n_missing / max(cp.n_total, 1)))
    if cp.numeric_max > 95:
        score -= 0.5
    return score


def _score_birth_date(cp: ColumnProfile, latest_median_year: Optional[int]) -> float:
    if not cp.is_date or cp.median_year is None:
        return -1.0
    score = 0.0
    if latest_median_year is not None:
        score += max(0, latest_median_year - cp.median_year) / 10.0
    score += (1.0 - (cp.n_missing / max(cp.n_total, 1)))
    return score


def _infer_gender_column(columns: Dict[str, ColumnProfile], resolver) -> Optional[str]:
    if resolver is None:
        return None

    best_col = None
    best_score = 0.0
    for col_name, cp in columns.items():
        if cp.dtype not in ("categorical", "boolean", "text"):
            continue
        if cp.n_unique == 0 or cp.n_unique > 6:
            continue
        resolved = 0
        distinct_values = cp.sample_values[:6]
        for value in distinct_values:
            if resolver.resolve_gender(value):
                resolved += 1
        if resolved == 0:
            continue
        score = resolved / max(len(distinct_values), 1)
        score += (1.0 - (cp.n_missing / max(cp.n_total, 1))) * 0.5
        if score > best_score:
            best_score = score
            best_col = col_name
    return best_col


def profile_csv(csv_path: str, mapping_paths: List[str] = None,
                sample_size: int = 100, resolver=None,
                person_id_column: Optional[str] = None,
                allow_uuid_person_id: bool = False) -> DatasetProfile:
    """
    Dynamically profile a CSV file using observed values.

    Args:
        csv_path: Path to the source CSV
        mapping_paths: Optional SNOMED mapping CSV paths (coverage logging only)
        sample_size: Number of rows to sample for profiling
        resolver: Optional ConceptResolver for semantic gender inference
    """
    logger.info(f"Profiling dataset: {csv_path}")
    profile = DatasetProfile(source_name=csv_path)

    mapped_fields: Set[str] = set()
    if mapping_paths:
        for mp in mapping_paths:
            try:
                with open(mp, newline="", encoding="utf-8-sig") as mf:
                    mr = csv.DictReader(mf)
                    for row in mr:
                        if "name" in row:
                            mapped_fields.add(row["name"])
                logger.info(f"Loaded {len(mapped_fields)} mapped field names from {mp}")
            except Exception as e:
                logger.warning(f"Could not load mapping file {mp}: {e}")

    rows = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        for row in reader:
            rows.append(row)
            profile.n_rows += 1

    if not headers:
        raise ValueError(f"No headers found in {csv_path}")

    logger.info(f"Dataset: {len(headers)} columns, {profile.n_rows} rows")

    for col in headers:
        if col is None:
            continue

        group, clean = _clean_col_name(col)
        values = [str(r.get(col, "")) for r in rows[:sample_size]]
        non_empty = _normalize_values(values)
        parsed_dates = [d for d in (_parse_date_value(v) for v in non_empty) if d is not None]
        parsed_numbers = [n for n in (_safe_float(v) for v in non_empty) if n is not None]
        bool_matches = [v for v in non_empty if v.lower() in BOOLEAN_VALUES]
        id_like = [v for v in non_empty if ID_SAFE_RE.match(v)]
        uuid_like = [v for v in non_empty if UUID_LIKE_RE.match(v)]
        time_like = [v for v in non_empty if TIME_LIKE_RE.match(v)]

        date_rate = len(parsed_dates) / max(len(non_empty), 1)
        numeric_rate = len(parsed_numbers) / max(len(non_empty), 1)
        boolean_rate = len(bool_matches) / max(len(non_empty), 1)
        avg_length = sum(len(v) for v in non_empty) / max(len(non_empty), 1)
        unique_values = list(dict.fromkeys(non_empty))
        dtype = _infer_dtype(non_empty, date_rate, numeric_rate, boolean_rate)

        numeric_min = min(parsed_numbers) if parsed_numbers else None
        numeric_max = max(parsed_numbers) if parsed_numbers else None
        numeric_median = sorted(parsed_numbers)[len(parsed_numbers) // 2] if parsed_numbers else None
        median_year = sorted(d.year for d in parsed_dates)[len(parsed_dates) // 2] if parsed_dates else None
        integer_like_rate = (
            sum(1 for n in parsed_numbers if float(n).is_integer()) / max(len(parsed_numbers), 1)
            if parsed_numbers else 0.0
        )

        cp = ColumnProfile(
            name=col,
            clean_name=clean,
            group=group,
            dtype=dtype,
            domain_hint=_default_domain_hint(dtype),
            n_unique=len(set(non_empty)),
            n_missing=len(values) - len(non_empty),
            n_total=len(values),
            sample_values=unique_values[:10],
            numeric_min=numeric_min,
            numeric_max=numeric_max,
            non_empty_count=len(non_empty),
            unique_ratio=(len(set(non_empty)) / max(len(non_empty), 1)),
            date_parse_rate=date_rate,
            numeric_parse_rate=numeric_rate,
            boolean_parse_rate=boolean_rate,
            id_like_rate=(len(id_like) / max(len(non_empty), 1)),
            avg_length=avg_length,
            median_numeric=numeric_median,
            median_year=median_year,
            integer_like_rate=integer_like_rate,
            uuid_like_rate=(len(uuid_like) / max(len(non_empty), 1)),
            time_like_rate=(len(time_like) / max(len(non_empty), 1)),
        )

        profile.columns[col] = cp
        if group:
            profile.groups.setdefault(group, []).append(col)

    resolve_person_id_column(
        profile,
        preferred_column=person_id_column,
        allow_uuid_person_id=allow_uuid_person_id,
    )

    profile.date_cols = [
        c for c, cp in profile.columns.items()
        if cp.date_parse_rate >= 0.8 and cp.dtype == "date"
    ]
    for c in profile.date_cols:
        profile.columns[c].is_date = True

    latest_median_year = None
    date_years = [profile.columns[c].median_year for c in profile.date_cols if profile.columns[c].median_year]
    if date_years:
        latest_median_year = max(date_years)

    age_scores = {c: _score_age(cp) for c, cp in profile.columns.items()}
    if age_scores:
        best_age = max(age_scores, key=age_scores.get)
        if age_scores[best_age] >= 1.5:
            profile.age_col = best_age
            profile.columns[best_age].is_age = True

    birth_scores = {
        c: _score_birth_date(cp, latest_median_year)
        for c, cp in profile.columns.items()
    }
    if birth_scores:
        best_birth = max(birth_scores, key=birth_scores.get)
        if birth_scores[best_birth] >= 1.0:
            profile.birth_date_col = best_birth
            profile.columns[best_birth].is_birth_date = True

    profile.gender_col = _infer_gender_column(profile.columns, resolver)
    if profile.gender_col:
        profile.columns[profile.gender_col].is_gender = True

    logger.info(
        f"Profile complete: person_id={profile.person_id_col}, "
        f"gender={profile.gender_col}, age={profile.age_col}, "
        f"date_cols={len(profile.date_cols)}, groups={len(profile.groups)}"
    )
    return profile