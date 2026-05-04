"""
profiler.py - Dynamically profiles any CSV dataset.

Detects:
  - Person identifier columns from value structure and uniqueness
  - Date columns from parseable date content
  - Numeric vs categorical vs free-text columns
  - Support columns such as age / birth date / gender from data itself

Inference is based on observed values, column cardinality, optional
schema hints (clinical_v10.json / nurse_v10.json), and optional OMOP
concept resolution.
"""
import csv
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from config.settings import ProfilerConfig

logger = logging.getLogger(__name__)

DEFAULT_SCHEMA_HINT_FILENAMES = ("clinical_v10.json", "nurse_v10.json")

ID_SAFE_RE = re.compile(r"^[A-Za-z0-9._:/-]+$")
UUID_LIKE_RE = re.compile(
    r"^(uuid:)?[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
TIME_LIKE_RE = re.compile(r"^\d{1,2}([:.]\d{1,2})?\s*(am|pm|noon)?\.?$", re.IGNORECASE)


@dataclass
class ColumnProfile:
    name: str
    clean_name: str
    group: str
    dtype: str
    domain_hint: str
    schema_type: Optional[str] = None
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


@dataclass
class SchemaHints:
    column_types: Dict[str, str] = field(default_factory=dict)
    identifier_strong_hints: Set[str] = field(default_factory=set)
    identifier_weak_hints: Set[str] = field(default_factory=set)
    uuid_identifier_hints: Set[str] = field(default_factory=set)


@dataclass
class ProfilerRuntime:
    cfg: ProfilerConfig
    null_like_values: Set[str]
    boolean_values: Set[str]
    date_formats: Tuple[str, ...]
    identifier_strong_hints: Tuple[str, ...]
    identifier_weak_hints: Tuple[str, ...]
    uuid_identifier_hints: Tuple[str, ...]
    column_type_overrides: Dict[str, str]

    def person_id_thresholds(self, sample_size: int) -> Tuple[float, float, float, float]:
        if sample_size <= self.cfg.small_dataset_row_threshold:
            return (
                self.cfg.person_id_fill_rate,
                self.cfg.person_id_uniqueness_rate_small,
                self.cfg.person_id_id_like_rate_small,
                self.cfg.person_id_max_avg_length_small,
            )
        return (
            self.cfg.person_id_fill_rate,
            self.cfg.person_id_uniqueness_rate,
            self.cfg.person_id_id_like_rate,
            self.cfg.person_id_max_avg_length,
        )

    def person_id_min_score(self, sample_size: int) -> float:
        if sample_size <= self.cfg.small_dataset_row_threshold:
            return self.cfg.person_id_min_score_small
        return self.cfg.person_id_min_score


def _normalize_column_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _discover_schema_hint_paths(csv_path: str) -> List[str]:
    candidates: List[str] = []
    for base in [Path.cwd(), Path(csv_path).resolve().parent]:
        for fname in DEFAULT_SCHEMA_HINT_FILENAMES:
            path = base / fname
            if path.exists():
                candidates.append(str(path))
    # Preserve order, de-duplicate
    seen: Set[str] = set()
    ordered: List[str] = []
    for path in candidates:
        if path not in seen:
            seen.add(path)
            ordered.append(path)
    return ordered


def _map_question_type(raw_type: str) -> Optional[str]:
    if not raw_type:
        return None
    t = str(raw_type).strip().lower()
    if t in ("datetime", "date", "timestamp"):
        return "date"
    if t in ("boolean", "bool"):
        return "boolean"
    if t in ("integer", "int", "float", "number", "decimal"):
        return "numeric"
    if t in ("string", "str", "text", "hyperlink"):
        return "text"
    return None


def _extract_identifier_hints(name: str) -> Tuple[Set[str], Set[str], Set[str]]:
    strong: Set[str] = set()
    weak: Set[str] = set()
    uuid: Set[str] = set()
    norm = _normalize_column_key(name)
    if not norm:
        return strong, weak, uuid
    tokens = [t for t in norm.split("_") if t]
    for idx, token in enumerate(tokens):
        if token == "id" and idx > 0:
            base = tokens[idx - 1]
            if len(base) >= 3:
                strong.add(f"{base}_id")
                weak.add(base)
        if token.endswith("id") and len(token) >= 5:
            base = token[:-2]
            if len(base) >= 3:
                strong.add(f"{base}_id")
                strong.add(token)
                weak.add(base)
        if token in ("uuid", "instanceid"):
            uuid.add(token)
    if "uuid" in norm:
        uuid.add("uuid")
    if "instanceid" in norm:
        uuid.add("instanceid")
    return strong, weak, uuid


def _load_schema_hints(paths: Optional[List[str]]) -> SchemaHints:
    hints = SchemaHints()
    if not paths:
        return hints

    clean_type_map: Dict[str, str] = {}
    clean_type_conflicts: Set[str] = set()

    for path in paths:
        try:
            with open(path, "r", encoding="utf-8-sig") as handle:
                data = json.load(handle)
        except Exception as exc:
            logger.warning(f"Could not load schema hints from {path}: {exc}")
            continue

        questions = data.get("questions", []) if isinstance(data, dict) else []
        for q in questions:
            name = q.get("name") if isinstance(q, dict) else None
            if not name:
                continue
            qtype = _map_question_type(q.get("type"))
            if qtype:
                full_key = _normalize_column_key(name)
                if full_key and full_key not in hints.column_types:
                    hints.column_types[full_key] = qtype
                _, clean = _clean_col_name(name)
                clean_key = _normalize_column_key(clean)
                if clean_key:
                    existing = clean_type_map.get(clean_key)
                    if existing and existing != qtype:
                        clean_type_conflicts.add(clean_key)
                    else:
                        clean_type_map[clean_key] = qtype

            strong, weak, uuid = _extract_identifier_hints(name)
            hints.identifier_strong_hints.update(strong)
            hints.identifier_weak_hints.update(weak)
            hints.uuid_identifier_hints.update(uuid)

    for clean_key, qtype in clean_type_map.items():
        if clean_key not in clean_type_conflicts and clean_key not in hints.column_types:
            hints.column_types[clean_key] = qtype

    return hints


def _merge_hint_lists(base: Tuple[str, ...], extra: Set[str]) -> Tuple[str, ...]:
    seen: Set[str] = set()
    merged: List[str] = []
    for item in list(base) + sorted(extra):
        if item and item not in seen:
            seen.add(item)
            merged.append(item)
    return tuple(merged)


def _build_runtime(
    profiler_cfg: Optional[ProfilerConfig],
    schema_hint_paths: Optional[List[str]],
) -> ProfilerRuntime:
    cfg = profiler_cfg or ProfilerConfig()
    hints = _load_schema_hints(schema_hint_paths)

    null_like = {str(v).strip().lower() for v in cfg.null_like_values if str(v).strip()}
    boolean_vals = {str(v).strip().lower() for v in cfg.boolean_values if str(v).strip()}

    return ProfilerRuntime(
        cfg=cfg,
        null_like_values=null_like,
        boolean_values=boolean_vals,
        date_formats=tuple(cfg.date_formats),
        identifier_strong_hints=_merge_hint_lists(cfg.identifier_name_strong_hints, hints.identifier_strong_hints),
        identifier_weak_hints=_merge_hint_lists(cfg.identifier_name_weak_hints, hints.identifier_weak_hints),
        uuid_identifier_hints=_merge_hint_lists(cfg.uuid_identifier_hints, hints.uuid_identifier_hints),
        column_type_overrides=hints.column_types,
    )


def _clean_col_name(name: str) -> tuple[str, str]:
    """Split grouped names such as 'group:field' or 'group.field'."""
    if ":" in name:
        group, field_name = name.split(":", 1)
        return group.strip(), field_name.strip()
    if "." in name:
        group, field_name = name.split(".", 1)
        return group.strip(), field_name.strip()
    return "", name.strip()


def _is_nullish(value: str, runtime: ProfilerRuntime) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    if not text:
        return True
    return text.lower() in runtime.null_like_values


def _normalize_values(values: List[str], runtime: ProfilerRuntime) -> List[str]:
    return [str(v).strip() for v in values if not _is_nullish(v, runtime)]


def _parse_date_value(value: str, runtime: ProfilerRuntime) -> Optional[date]:
    if _is_nullish(value, runtime):
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

    for fmt in runtime.date_formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _safe_float(value: str, runtime: ProfilerRuntime) -> Optional[float]:
    if _is_nullish(value, runtime):
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _infer_dtype(
    non_empty: List[str],
    date_rate: float,
    numeric_rate: float,
    boolean_rate: float,
    runtime: ProfilerRuntime,
) -> str:
    if not non_empty:
        return "empty"
    if boolean_rate >= runtime.cfg.boolean_rate_threshold:
        return "boolean"
    if date_rate >= runtime.cfg.date_rate_threshold:
        return "date"
    if numeric_rate >= runtime.cfg.numeric_rate_threshold:
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


def _boolean_match_rate(values: List[str], runtime: ProfilerRuntime) -> float:
    if not values:
        return 0.0
    normalized = [str(v).strip().lower() for v in values if str(v).strip()]
    if not normalized:
        return 0.0
    if all(re.fullmatch(r"[01]", v) for v in normalized):
        return 1.0
    matches = sum(1 for v in normalized if v in runtime.boolean_values)
    return matches / max(len(normalized), 1)


def _apply_schema_type_hint(
    dtype: str,
    schema_type: Optional[str],
    date_rate: float,
    numeric_rate: float,
    boolean_rate: float,
    runtime: ProfilerRuntime,
) -> str:
    if not schema_type:
        return dtype
    if schema_type == "date" and date_rate >= runtime.cfg.schema_hint_min_date_rate:
        return "date"
    if schema_type == "numeric" and numeric_rate >= runtime.cfg.schema_hint_min_numeric_rate:
        return "numeric"
    if schema_type == "boolean" and boolean_rate >= runtime.cfg.schema_hint_min_boolean_rate:
        return "boolean"
    return dtype


def _score_person_id(cp: ColumnProfile, runtime: ProfilerRuntime) -> float:
    if cp.dtype in ("empty", "date"):
        return -1.0
    if cp.non_empty_count == 0:
        return -1.0
    fill_rate, unique_rate, id_like_rate, max_avg_len = runtime.person_id_thresholds(cp.n_total)
    if (cp.non_empty_count / max(cp.n_total, 1)) < fill_rate:
        return -1.0
    if cp.unique_ratio < unique_rate:
        return -1.0
    if cp.id_like_rate < id_like_rate:
        return -1.0
    if cp.avg_length > max_avg_len:
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
    return _normalize_column_key(name)


def _identifier_name_bonus(cp: ColumnProfile, runtime: ProfilerRuntime) -> float:
    tokens = {
        _normalize_identifier_name(cp.name),
        _normalize_identifier_name(cp.clean_name),
    }
    bonus = 0.0
    if any(hint in token for token in tokens for hint in runtime.identifier_strong_hints):
        bonus += 2.5
    elif any(hint in token for token in tokens for hint in runtime.identifier_weak_hints):
        bonus += 1.0
    if any(token.endswith("_id") or token.endswith("id") for token in tokens):
        bonus += 0.5
    return bonus


def _looks_like_uuid_identifier(cp: ColumnProfile, runtime: ProfilerRuntime) -> bool:
    if cp.uuid_like_rate >= 0.8:
        return True
    tokens = {
        _normalize_identifier_name(cp.name),
        _normalize_identifier_name(cp.clean_name),
    }
    return any(hint in token for token in tokens for hint in runtime.uuid_identifier_hints)


def _rank_person_id_candidates(
    columns: Dict[str, ColumnProfile],
    runtime: ProfilerRuntime,
    allow_uuid_person_id: bool = False,
) -> List[Tuple[str, float]]:
    ranked = []
    for col_name, cp in columns.items():
        score = _score_person_id(cp, runtime)
        if score < 0:
            continue
        score += _identifier_name_bonus(cp, runtime)
        if _looks_like_uuid_identifier(cp, runtime) and not allow_uuid_person_id:
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
    runtime: ProfilerRuntime,
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
            runtime,
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
        _, min_unique, _, _ = runtime.person_id_thresholds(profile.n_rows)
        if cp.unique_ratio < min_unique:
            raise RuntimeError(
                f"Configured person identifier column '{preferred_column}' is not unique enough "
                f"for person identity (unique_ratio={cp.unique_ratio:.2f})."
            )
        if _looks_like_uuid_identifier(cp, runtime) and not allow_uuid_person_id:
            raise RuntimeError(
                f"Configured person identifier column '{preferred_column}' looks like a submission "
                f"or record UUID. Re-run with --allow-uuid-person-id only if each UUID should "
                f"represent a distinct person in OMOP."
            )
        _mark_person_id_column(profile, preferred_column)
        return preferred_column

    ranked = _rank_person_id_candidates(profile.columns, runtime, allow_uuid_person_id=allow_uuid_person_id)
    if ranked and ranked[0][1] >= runtime.person_id_min_score(profile.n_rows):
        _mark_person_id_column(profile, ranked[0][0])
        return ranked[0][0]

    profile.person_id_col = None
    return None


def _score_age(cp: ColumnProfile, runtime: ProfilerRuntime) -> float:
    if cp.dtype != "numeric" or cp.numeric_min is None or cp.numeric_max is None:
        return -1.0
    if cp.numeric_min < runtime.cfg.age_min_value or cp.numeric_max > runtime.cfg.age_max_value:
        return -1.0
    if cp.numeric_max < runtime.cfg.age_max_min_value:
        return -1.0
    if cp.median_numeric is None or cp.median_numeric < runtime.cfg.age_median_min or cp.median_numeric > runtime.cfg.age_median_max:
        return -1.0
    if cp.integer_like_rate < runtime.cfg.age_integer_like_min:
        return -1.0
    score = 0.0
    score += min(cp.unique_ratio, 1.0) * 2.0
    score += (1.0 - (cp.n_missing / max(cp.n_total, 1)))
    if cp.numeric_max > 95:
        score -= 0.5
    return score


def _score_birth_date(
    cp: ColumnProfile,
    latest_median_year: Optional[int],
    runtime: ProfilerRuntime,
) -> float:
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


def profile_csv(
    csv_path: str,
    mapping_paths: List[str] = None,
    sample_size: Optional[int] = None,
    resolver=None,
    person_id_column: Optional[str] = None,
    allow_uuid_person_id: bool = False,
    profiler_cfg: Optional[ProfilerConfig] = None,
    schema_hint_paths: Optional[List[str]] = None,
) -> DatasetProfile:
    """
    Dynamically profile a CSV file using observed values.

    Args:
        csv_path: Path to the source CSV
        mapping_paths: Optional SNOMED mapping CSV paths (coverage logging only)
        sample_size: Number of rows to sample for profiling
        resolver: Optional ConceptResolver for semantic gender inference
        profiler_cfg: Optional ProfilerConfig overrides
        schema_hint_paths: Optional schema hint JSON files (clinical_v10.json, nurse_v10.json)
    """
    logger.info(f"Profiling dataset: {csv_path}")
    if schema_hint_paths is None:
        schema_hint_paths = _discover_schema_hint_paths(csv_path)
    runtime = _build_runtime(profiler_cfg, schema_hint_paths)
    if sample_size is None:
        sample_size = runtime.cfg.sample_size

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
        non_empty = _normalize_values(values, runtime)
        parsed_dates = [d for d in (_parse_date_value(v, runtime) for v in non_empty) if d is not None]
        parsed_numbers = [n for n in (_safe_float(v, runtime) for v in non_empty) if n is not None]
        boolean_rate = _boolean_match_rate(non_empty, runtime)
        id_like = [v for v in non_empty if ID_SAFE_RE.match(v)]
        uuid_like = [v for v in non_empty if UUID_LIKE_RE.match(v)]
        time_like = [v for v in non_empty if TIME_LIKE_RE.match(v)]

        date_rate = len(parsed_dates) / max(len(non_empty), 1)
        numeric_rate = len(parsed_numbers) / max(len(non_empty), 1)
        avg_length = sum(len(v) for v in non_empty) / max(len(non_empty), 1)
        unique_values = list(dict.fromkeys(non_empty))
        dtype = _infer_dtype(non_empty, date_rate, numeric_rate, boolean_rate, runtime)
        schema_key = _normalize_column_key(col)
        schema_type = runtime.column_type_overrides.get(schema_key)
        if not schema_type:
            schema_type = runtime.column_type_overrides.get(_normalize_column_key(clean))
        dtype = _apply_schema_type_hint(dtype, schema_type, date_rate, numeric_rate, boolean_rate, runtime)

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
            schema_type=schema_type,
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
        runtime,
        preferred_column=person_id_column,
        allow_uuid_person_id=allow_uuid_person_id,
    )

    profile.date_cols = []
    for c, cp in profile.columns.items():
        if cp.dtype != "date":
            continue
        threshold = runtime.cfg.date_parse_rate_threshold
        if cp.schema_type == "date":
            threshold = min(threshold, runtime.cfg.schema_hint_min_date_rate)
        if cp.date_parse_rate >= threshold:
            profile.date_cols.append(c)
    for c in profile.date_cols:
        profile.columns[c].is_date = True

    latest_median_year = None
    date_years = [profile.columns[c].median_year for c in profile.date_cols if profile.columns[c].median_year]
    if date_years:
        latest_median_year = max(date_years)

    age_scores = {c: _score_age(cp, runtime) for c, cp in profile.columns.items()}
    if age_scores:
        best_age = max(age_scores, key=age_scores.get)
        if age_scores[best_age] >= runtime.cfg.age_min_score:
            profile.age_col = best_age
            profile.columns[best_age].is_age = True

    birth_scores = {
        c: _score_birth_date(cp, latest_median_year, runtime)
        for c, cp in profile.columns.items()
    }
    if birth_scores:
        best_birth = max(birth_scores, key=birth_scores.get)
        if birth_scores[best_birth] >= runtime.cfg.birth_date_min_score:
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
