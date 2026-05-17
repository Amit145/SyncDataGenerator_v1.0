from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from helper.canonical_raw_schema import CANONICAL_RAW_SCHEMAS


def _str_or_empty(value):
    if pd.isna(value):
        return ""
    return str(value)


def _non_empty(value) -> bool:
    return _str_or_empty(value).strip() != ""


def _first_non_empty(values):
    for value in values:
        if _non_empty(value):
            return _str_or_empty(value)
    return ""


def _hash_text(parts, prefix=""):
    raw = "|".join(_str_or_empty(part) for part in parts)
    return f"{prefix}{hashlib.md5(raw.encode('utf-8')).hexdigest()}"


def _load_sources(input_dir: Path, config: dict) -> dict[str, pd.DataFrame]:
    frames = {}
    for source in config.get("sources", []):
        alias = source["alias"]
        path = input_dir / source["path"]
        frames[alias] = pd.read_csv(path)
    return frames


def _row_passes_filters(row: pd.Series, filters: list[dict]) -> bool:
    for rule in filters:
        kind = rule["kind"]
        column = rule.get("column")
        value = row.get(column) if column else None

        if kind == "equals" and _str_or_empty(value) != _str_or_empty(rule.get("value")):
            return False
        if kind == "not_equals" and _str_or_empty(value) == _str_or_empty(rule.get("value")):
            return False
        if kind == "in" and _str_or_empty(value) not in {_str_or_empty(item) for item in rule.get("values", [])}:
            return False
        if kind == "not_empty" and not _non_empty(value):
            return False
        if kind == "any_non_empty":
            if not any(_non_empty(row.get(col)) for col in rule.get("columns", [])):
                return False
    return True


def _eval_rule(row: pd.Series, rule: dict):
    kind = rule["kind"]

    if kind == "source":
        return _str_or_empty(row.get(rule["column"]))

    if kind == "literal":
        return _str_or_empty(rule.get("value", ""))

    if kind == "coalesce":
        return _first_non_empty(row.get(column) for column in rule.get("columns", []))

    if kind == "concat":
        separator = rule.get("separator", "")
        return separator.join(_str_or_empty(row.get(column)) for column in rule.get("columns", []))

    if kind == "hash_id":
        parts = [row.get(column) for column in rule.get("columns", [])]
        return _hash_text(parts, prefix=rule.get("prefix", ""))

    if kind == "map":
        source_value = _str_or_empty(row.get(rule["column"]))
        mapping = {str(key): str(value) for key, value in rule.get("mapping", {}).items()}
        return mapping.get(source_value, _str_or_empty(rule.get("default", "")))

    if kind == "date_format":
        source_value = row.get(rule["column"])
        if not _non_empty(source_value):
            return ""
        parsed = pd.to_datetime(source_value, errors="coerce")
        if pd.isna(parsed):
            return ""
        return parsed.strftime(rule.get("format", "%Y-%m-%d"))

    raise ValueError(f"Unsupported mapping kind: {kind}")


def _canonical_row(row: pd.Series, table_config: dict, batch_id: str, extract_ts: str, source_system: str):
    target_file = table_config["target_file"]
    canonical = {
        "_batch_id": batch_id,
        "_extract_ts": extract_ts,
        "_source_system": source_system,
    }

    for target_column, rule in table_config.get("column_map", {}).items():
        canonical[target_column] = _eval_rule(row, rule)

    for field in CANONICAL_RAW_SCHEMAS[target_file]:
        canonical.setdefault(field, "")

    return canonical


def _dedupe_rows(rows: list[dict], key_fields: list[str]) -> list[dict]:
    if not key_fields:
        return rows
    seen = set()
    out = []
    for row in rows:
        key = tuple(row.get(field, "") for field in key_fields)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def build_canonical_raw_rows(input_dir: str, config_path: str, batch_id: str | None = None, extract_ts: str | None = None):
    input_dir_path = Path(input_dir)
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    batch_id = batch_id or datetime.now().strftime("%Y%m%d_%H%M%S")
    extract_ts = extract_ts or datetime.now().replace(microsecond=0).isoformat()
    source_system = config.get("source_system", "KAGGLE")
    dataset_name = config.get("dataset_name", "kaggle_dataset")

    sources = _load_sources(input_dir_path, config)
    out = {name: [] for name in CANONICAL_RAW_SCHEMAS}

    for table_config in config.get("tables", []):
        target_file = table_config["target_file"]
        if target_file not in CANONICAL_RAW_SCHEMAS:
            raise ValueError(f"Unsupported canonical raw target file: {target_file}")

        source_alias = table_config["source_alias"]
        if source_alias not in sources:
            raise ValueError(f"Missing source alias in config: {source_alias}")

        frame = sources[source_alias].fillna("")
        filters = table_config.get("filters", [])

        for _, row in frame.iterrows():
            if filters and not _row_passes_filters(row, filters):
                continue
            out[target_file].append(
                _canonical_row(row, table_config, batch_id, extract_ts, source_system)
            )

        out[target_file] = _dedupe_rows(out[target_file], table_config.get("dedupe_by", []))

    manifest = {
        "dataset_name": dataset_name,
        "source_system": source_system,
        "batch_id": batch_id,
        "extract_ts": extract_ts,
        "config_path": str(config_path),
        "tables_generated": sorted([name for name, rows in out.items() if rows]),
    }
    return out, manifest
