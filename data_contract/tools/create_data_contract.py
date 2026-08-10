"""Generate an ODCS v3.1.0 data contract from a folder of CSV files.

The generated contract is a draft. Review business descriptions, keys,
classifications, SLA, and rule applicability before promoting it.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from jsonschema import Draft201909Validator, FormatChecker


DEFAULT_SCHEMA = Path("data_contract/odcs-v3.1.0.schema_ODCS.json")
DEFAULT_RULES = Path("data_contract/new 2.txt")

PII_NAME_PARTS = (
    "email",
    "phone",
    "dob",
    "birth",
    "first_name",
    "last_name",
    "full_name",
    "given_nm",
    "family_nm",
    "display_nm",
    "person",
    "party",
    "occupation",
    "marital",
    "gender",
    "nationality",
    "address",
    "city",
    "state",
    "country",
    "zipcode",
    "latitude",
    "longitude",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-path", required=True, type=Path, help="Folder containing source CSV files.")
    parser.add_argument("--output", required=True, type=Path, help="Output ODCS YAML contract path.")
    parser.add_argument("--contract-id", required=True)
    parser.add_argument("--contract-name", required=True)
    parser.add_argument("--domain", required=True)
    parser.add_argument("--layer", required=True, help="Pipeline layer, for example raw, bronze, silver, gold.")
    parser.add_argument("--source-feed", required=True, help="Source feed identifier, for example prd_01.")
    parser.add_argument("--source-system", required=True)
    parser.add_argument("--version", default="1.0.0")
    parser.add_argument("--status", default="draft")
    parser.add_argument("--tenant", default="Allianz")
    parser.add_argument("--owner-email", default="data-modelling-engineering-coe@example-internal")
    parser.add_argument("--rules-file", default=DEFAULT_RULES, type=Path)
    parser.add_argument("--schema-path", default=DEFAULT_SCHEMA, type=Path)
    parser.add_argument("--config", type=Path, help="Optional YAML or CSV override config.")
    parser.add_argument(
        "--path-template",
        help="Runtime source path written to the contract server block. Defaults to '<input-path>/*.csv'.",
    )
    parser.add_argument("--batch-filter", default="batch_ref = '{batch_ref}'")
    parser.add_argument("--sample-rows", type=int, default=0, help="Rows to profile. 0 means all rows.")
    return parser.parse_args()


def stable_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "_", value).strip("_") or "value"


def title_from_stem(stem: str) -> str:
    return " ".join(part.capitalize() for part in stem.split("_"))


def parse_config_scalar(value: Any) -> Any:
    if value is None:
        return None
    if not isinstance(value, str):
        return value

    text = value.strip()
    if text == "":
        return None

    lowered = text.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"none", "null"}:
        return None

    if (text.startswith("[") and text.endswith("]")) or (text.startswith("{") and text.endswith("}")):
        return yaml.safe_load(text)

    if "|" in text:
        return [parse_config_scalar(part) for part in text.split("|") if part.strip()]

    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        return text


def parse_config_mapping(value: Any) -> Any:
    parsed = parse_config_scalar(value)
    if not isinstance(parsed, str):
        return parsed

    mapping: dict[str, Any] = {}
    for part in parsed.split(";"):
        if "=" not in part:
            return parsed
        key, raw_value = part.split("=", 1)
        key = key.strip()
        if not key:
            return parsed
        mapping[key] = parse_config_scalar(raw_value)
    return mapping


def set_if_present(target: dict[str, Any], key: str, value: Any) -> None:
    parsed = parse_config_scalar(value)
    if parsed is not None:
        target[key] = parsed


def row_value(row: dict[str, str], key: str) -> str | None:
    value = row.get(key)
    if value is None or value.strip() == "":
        return None
    return value


def table_config(config: dict[str, Any], table_name: str) -> dict[str, Any]:
    return config.setdefault("tables", {}).setdefault(table_name, {})


def csv_rule(row: dict[str, str]) -> dict[str, Any]:
    rule: dict[str, Any] = {}
    aliases = {
        "ruleId": "ruleId",
        "ruleName": "ruleName",
        "type": "type",
        "query": "query",
        "metric": "metric",
        "dimension": "dimension",
        "severity": "severity",
        "description": "description",
        "failureAction": "failureAction",
        "businessImpact": "businessImpact",
        "schedule": "schedule",
        "scheduler": "scheduler",
        "unit": "unit",
        "method": "method",
        "arguments": "arguments",
        "referencedColumns": "referencedColumns",
    }
    for column_name, rule_key in aliases.items():
        value = row_value(row, column_name)
        if value is None:
            continue
        if rule_key in {"arguments", "referencedColumns"}:
            rule[rule_key] = parse_config_scalar(value)
        else:
            rule[rule_key] = value

    for key in (
        "mustBe",
        "mustNotBe",
        "mustBeGreaterThan",
        "mustBeGreaterOrEqualTo",
        "mustBeLessThan",
        "mustBeLessOrEqualTo",
        "mustBeBetween",
        "mustNotBeBetween",
    ):
        value = row_value(row, key)
        if value is not None:
            rule[key] = parse_config_mapping(value)

    return rule


def load_csv_config(path: Path) -> dict[str, Any]:
    config: dict[str, Any] = {"tables": {}}
    contract_fields = {
        "purpose",
        "limitations",
        "usage",
        "frequency",
        "frequencyUnit",
        "latency",
        "latencyUnit",
        "retention",
        "retentionUnit",
        "teamName",
        "firstLevelApprovers",
        "secondLevelApprovers",
        "supportChannel",
        "supportUrl",
        "allianzClassification",
        "containsPersonalData",
        "ownerDateIn",
        "accessRole",
        "contractCreatedTs",
    }
    table_fields = {"primaryKey", "businessKey", "businessName", "grain", "allowNoPrimaryKey", "exclude"}
    column_fields = {
        "required",
        "logicalType",
        "physicalType",
        "classification",
        "description",
        "allowedValues",
        "minValue",
        "maxValue",
        "pii",
    }

    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            record_type = (row_value(row, "record_type") or row_value(row, "section") or "").lower()
            table_name = row_value(row, "table")
            column_name = row_value(row, "column")

            if record_type in {"contract", "metadata"}:
                key = row_value(row, "key")
                if key:
                    set_if_present(config, key, row_value(row, "value"))
                for field in contract_fields:
                    set_if_present(config, field, row_value(row, field))
                continue

            if record_type == "table" and table_name:
                cfg = table_config(config, table_name)
                for field in table_fields:
                    set_if_present(cfg, field, row_value(row, field))
                continue

            if record_type == "column" and table_name and column_name:
                cfg = table_config(config, table_name).setdefault("columns", {}).setdefault(column_name, {})
                for field in column_fields:
                    set_if_present(cfg, field, row_value(row, field))
                continue

            if record_type == "table_rule" and table_name:
                rule = csv_rule(row)
                if rule:
                    table_config(config, table_name).setdefault("tableRules", []).append(rule)
                continue

            if record_type == "column_rule" and table_name and column_name:
                rule = csv_rule(row)
                if rule:
                    column_cfg = table_config(config, table_name).setdefault("columns", {}).setdefault(column_name, {})
                    column_cfg.setdefault("columnRules", []).append(rule)

    if not config["tables"]:
        config.pop("tables")
    return config


def resolve_config_path(path: Path) -> Path:
    if path.suffix.lower() in {".yaml", ".yml"}:
        csv_path = path.with_suffix(".csv")
        if csv_path.exists():
            return csv_path
    return path


def load_config(path: Path | None) -> dict[str, Any]:
    if not path:
        return {}
    path = resolve_config_path(path)
    if path.suffix.lower() == ".csv":
        return load_csv_config(path)
    with path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def logical_type(series: pd.Series, column: str) -> str:
    col = column.lower()
    date_like = (
        col.endswith("_date")
        or "_date" in col
        or col.endswith("_ts")
        or col.endswith("_time")
        or col in {"pull_ts", "dob", "birth_date", "constitution_dt", "converted_date"}
    )
    if date_like:
        return "timestamp"
    if pd.api.types.is_integer_dtype(series):
        return "integer"
    if pd.api.types.is_float_dtype(series):
        return "number"
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    return "string"


def physical_type(logical: str, series: pd.Series) -> str:
    if logical == "integer":
        return "bigint"
    if logical == "number":
        return "decimal(18,2)"
    if logical == "boolean":
        return "boolean"
    if logical == "timestamp":
        return "timestamp"

    non_null = series.dropna().astype(str)
    max_len = int(non_null.map(len).max()) if not non_null.empty else 1
    if max_len <= 1:
        return "char(1)"
    if max_len <= 32:
        return "varchar(32)"
    if max_len <= 64:
        return "varchar(64)"
    if max_len <= 255:
        return "varchar(255)"
    return "varchar(1000)"


def is_pii(column: str) -> bool:
    col = column.lower()
    return any(part in col for part in PII_NAME_PARTS)


def read_csv(path: Path, sample_rows: int) -> pd.DataFrame:
    nrows = sample_rows if sample_rows > 0 else None
    return pd.read_csv(path, nrows=nrows, low_memory=False)


def normalize_key(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]


def infer_key(df: pd.DataFrame, file_name: str, table_cfg: dict[str, Any]) -> list[str]:
    override = table_cfg.get("primaryKey") or table_cfg.get("businessKey")
    if override:
        return normalize_key(override)

    preferred_patterns = ("_identifier", "_id", "_ref", "_number")
    unique = [
        col
        for col in df.columns
        if df[col].isna().sum() == 0 and df[col].nunique(dropna=True) == len(df)
    ]
    for pattern in preferred_patterns:
        for col in unique:
            if col.lower().endswith(pattern):
                return [col]
    return [unique[0]] if unique else []


def quality_rule(rule_id: str, key: str) -> dict[str, Any]:
    concat_expr = f"concat({key})"
    if rule_id == "rule_0001":
        return {
            "type": "sql",
            "query": (
                "select count(*) as null_count from {full_table_name} a "
                f"where {concat_expr} is null and {{target_watermak_exp}}"
            ),
            "mustBe": {"null_count": 0},
            "dimension": "completeness",
            "severity": "error",
            "description": f"rule_0001: BK Check - {key} business key must not be null.",
        }
    if rule_id == "rule_0003":
        return {
            "type": "sql",
            "query": (
                f"SELECT SUM(CASE WHEN {concat_expr} is null THEN 1 ELSE 0 END) AS pk_nulls, "
                f"COUNT(1) - COUNT(DISTINCT {concat_expr}) AS pk_duplicates "
                "FROM {full_table_name} a WHERE {target_watermak_exp}"
            ),
            "mustBe": {"pk_nulls": 0, "pk_duplicates": 0},
            "dimension": "uniqueness",
            "severity": "error",
            "description": f"rule_0003: Not Null Check - {key} primary key must be populated and unique.",
        }
    return {
        "type": "sql",
        "query": (
            f"SELECT SUM(CASE WHEN {concat_expr} is null THEN 1 ELSE 0 END) AS bk_nulls, "
            f"COUNT(1) - COUNT(DISTINCT {concat_expr}) AS bk_duplicates "
            "FROM {full_table_name} a WHERE {target_watermak_exp}"
        ),
        "mustBe": {"bk_nulls": 0, "bk_duplicates": 0},
        "dimension": "uniqueness",
        "severity": "error",
        "description": f"rule_0007: BK Not Null Check - {key} business key must be populated and unique.",
    }


QUALITY_DIRECT_FIELDS = {
    "id",
    "name",
    "type",
    "metric",
    "rule",
    "query",
    "mustBe",
    "mustNotBe",
    "mustBeGreaterThan",
    "mustBeGreaterOrEqualTo",
    "mustBeLessThan",
    "mustBeLessOrEqualTo",
    "mustBeBetween",
    "mustNotBeBetween",
    "dimension",
    "severity",
    "description",
    "arguments",
    "businessImpact",
    "schedule",
    "scheduler",
    "unit",
    "method",
}


def normalize_quality_rule(rule: dict[str, Any], default_column: str | None = None) -> dict[str, Any]:
    """Convert config rule syntax into an ODCS-compatible quality rule."""
    normalized: dict[str, Any] = {}
    custom_properties: list[dict[str, Any]] = []

    for key, value in rule.items():
        if key == "ruleId":
            normalized["id"] = stable_id(str(value))
        elif key == "ruleName":
            normalized["name"] = str(value)
        elif key in QUALITY_DIRECT_FIELDS:
            normalized[key] = value
        else:
            custom_properties.append({"property": key, "value": value})

    if default_column:
        existing_refs = next(
            (item for item in custom_properties if item["property"] == "referencedColumns"),
            None,
        )
        if existing_refs is None:
            custom_properties.append({"property": "referencedColumns", "value": [default_column]})

    if custom_properties:
        normalized["customProperties"] = custom_properties
    return normalized


def allowed_values_quality_rule(column: str, allowed_values: list[Any]) -> dict[str, Any]:
    return {
        "id": stable_id(f"{column}_allowed_values"),
        "name": f"{column} allowed values",
        "type": "library",
        "metric": "invalidValues",
        "mustBe": 0,
        "dimension": "conformity",
        "severity": "error",
        "description": f"{column} must contain only configured allowed values.",
        "arguments": {"validValues": allowed_values},
        "customProperties": [{"property": "referencedColumns", "value": [column]}],
    }


def min_value_quality_rule(column: str, value: int | float) -> dict[str, Any]:
    return {
        "id": stable_id(f"{column}_min_value"),
        "name": f"{column} minimum value",
        "type": "sql",
        "query": (
            f"SELECT COUNT(*) AS invalid_values FROM {{full_table_name}} "
            f"WHERE {column} < {value} AND {{target_watermak_exp}}"
        ),
        "mustBe": {"invalid_values": 0},
        "dimension": "accuracy",
        "severity": "error",
        "description": f"{column} must be greater than or equal to {value}.",
        "customProperties": [{"property": "referencedColumns", "value": [column]}],
    }


def max_value_quality_rule(column: str, value: int | float) -> dict[str, Any]:
    return {
        "id": stable_id(f"{column}_max_value"),
        "name": f"{column} maximum value",
        "type": "sql",
        "query": (
            f"SELECT COUNT(*) AS invalid_values FROM {{full_table_name}} "
            f"WHERE {column} > {value} AND {{target_watermak_exp}}"
        ),
        "mustBe": {"invalid_values": 0},
        "dimension": "accuracy",
        "severity": "error",
        "description": f"{column} must be less than or equal to {value}.",
        "customProperties": [{"property": "referencedColumns", "value": [column]}],
    }


def build_property(col: str, series: pd.Series, key_cols: list[str], table_cfg: dict[str, Any]) -> dict[str, Any]:
    column_cfg = (table_cfg.get("columns") or {}).get(col, {})
    logical = column_cfg.get("logicalType") or logical_type(series, col)
    required = bool(column_cfg.get("required", series.isna().sum() == 0))
    classification = column_cfg.get("classification") or ("confidential" if is_pii(col) else "internal")

    prop: dict[str, Any] = {
        "id": stable_id(col),
        "name": col,
        "logicalType": logical,
        "physicalType": column_cfg.get("physicalType") or physical_type(logical, series),
        "required": required,
        "classification": classification,
        "description": column_cfg.get("description") or f"Source field {col}.",
    }
    if col in key_cols:
        prop.update(
            {
                "unique": True,
                "primaryKey": True,
                "primaryKeyPosition": key_cols.index(col) + 1,
                "criticalDataElement": True,
                "description": column_cfg.get("description") or "Primary business key for this source object.",
            }
        )
    if column_cfg.get("pii", is_pii(col)):
        prop["customProperties"] = [{"property": "pii", "value": True}]
    quality: list[dict[str, Any]] = []
    if "allowedValues" in column_cfg:
        quality.append(allowed_values_quality_rule(col, column_cfg["allowedValues"]))
    if "minValue" in column_cfg:
        quality.append(min_value_quality_rule(col, column_cfg["minValue"]))
    if "maxValue" in column_cfg:
        quality.append(max_value_quality_rule(col, column_cfg["maxValue"]))
    for rule in column_cfg.get("columnRules", []) or []:
        quality.append(normalize_quality_rule(rule, default_column=col))
    if quality:
        prop["quality"] = quality
    return prop


def parse_rule_file(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def rule_catalog(rule_rows: list[dict[str, str]], entities: list[dict[str, Any]], layer: str) -> dict[str, Any]:
    entity_names = [entity["entity"] for entity in entities]
    active_ids = {"rule_0001", "rule_0003", "rule_0007"}
    active: list[dict[str, Any]] = []
    inactive: list[dict[str, Any]] = []

    defaults = {
        "rule_0001": {
            "severity": "error",
            "failureAction": "reject",
            "expectedOutcome": {"null_count": 0},
            "queryTemplate": "select count(*) as null_count from {full_table_name} a where ({table_bk}) is null and {target_watermak_exp}",
            "queryDesc": "Business key should not be null.",
        },
        "rule_0003": {
            "severity": "error",
            "failureAction": "reject",
            "expectedOutcome": {"pk_nulls": 0, "pk_duplicates": 0},
            "queryTemplate": "SELECT SUM(CASE WHEN concat({table_pk}) is null THEN 1 ELSE 0 END) AS pk_nulls, COUNT(1) - COUNT(DISTINCT {table_pk}) AS pk_duplicates FROM {full_table_name} a WHERE {target_watermak_exp}",
            "queryDesc": "Primary key should be populated and unique.",
        },
        "rule_0007": {
            "severity": "error",
            "failureAction": "reject",
            "expectedOutcome": {"bk_nulls": 0, "bk_duplicates": 0},
            "queryTemplate": "SELECT SUM(CASE WHEN concat({table_bk}) is null THEN 1 ELSE 0 END) AS bk_nulls, COUNT(1) - COUNT(DISTINCT {table_bk}) AS bk_duplicates FROM {full_table_name} a WHERE {target_watermak_exp}",
            "queryDesc": "Business key should be populated and unique.",
        },
    }

    for row in rule_rows:
        rule_id = row.get("rule_id", "")
        common = {
            "ruleId": rule_id,
            "ruleName": row.get("rule_name", ""),
            "sourceRuleType": row.get("rule_type", ""),
            "ruleCategory": row.get("rule_category", ""),
        }
        if rule_id in active_ids:
            active.append({**common, **defaults[rule_id], "layer": layer, "applicableTo": entity_names})
        elif rule_id == "rule_0002":
            inactive.append(
                {
                    **common,
                    "reasonNotActive": "Requires a source-to-target comparison; apply during reconciliation, not standalone raw source arrival.",
                    "expectedOutcome": {"missing_keys": 0},
                    "queryTemplate": row.get("query_text", "").strip(),
                }
            )
        else:
            inactive.append(
                {
                    **common,
                    "reasonNotActive": "Requires SCD2 effective dating columns that are not expected in raw source files.",
                    "queryTemplate": row.get("query_text", "").strip(),
                }
            )

    return {"sourceRuleFile": str(DEFAULT_RULES).replace("\\", "/"), "activeRules": active, "inactiveRules": inactive}


def build_contract(args: argparse.Namespace, config: dict[str, Any]) -> dict[str, Any]:
    table_configs = config.get("tables") or {}
    files = sorted(args.input_path.glob("*.csv"))
    if not files:
        raise SystemExit(f"No CSV files found under {args.input_path}")

    schema_objects: list[dict[str, Any]] = []
    entities: list[dict[str, Any]] = []
    expected_files: list[str] = []
    contains_personal_data = False

    for path in files:
        table_cfg = table_configs.get(path.name, {})
        if table_cfg.get("exclude", False):
            continue
        df = read_csv(path, args.sample_rows)
        key_cols = infer_key(df, path.name, table_cfg)
        table_name = table_cfg.get("name") or path.stem
        properties = [build_property(col, df[col], key_cols, table_cfg) for col in df.columns]
        contains_personal_data = contains_personal_data or any(
            prop.get("classification") == "confidential" for prop in properties
        )
        quality = [
            {
                "metric": "rowCount",
                "mustBeGreaterThan": 0,
                "dimension": "completeness",
                "type": "library",
                "severity": "error",
                "description": f"{path.name} must not be empty.",
            }
        ]
        if key_cols:
            key_expr = ", ".join(key_cols)
            concat_key_expr = ", ".join(key_cols)
            quality.extend([quality_rule("rule_0003", concat_key_expr), quality_rule("rule_0001", key_expr), quality_rule("rule_0007", concat_key_expr)])
            entities.append(
                {
                    "entity": table_name,
                    "sourceFile": path.name,
                    "primaryKeyColumns": key_cols,
                    "businessKeyColumns": normalize_key(table_cfg.get("businessKey")) or key_cols,
                    "fullTableNamePlaceholder": table_name,
                    "targetWatermakExp": args.batch_filter,
                }
            )
        for rule in table_cfg.get("tableRules", []) or []:
            quality.append(normalize_quality_rule(rule))

        schema_objects.append(
            {
                "id": stable_id(table_name),
                "name": table_name,
                "physicalName": path.name,
                "physicalType": "table",
                "businessName": table_cfg.get("businessName") or title_from_stem(table_name),
                "description": table_cfg.get("description") or f"Raw {args.source_feed} source records for {title_from_stem(table_name)}.",
                "tags": [args.domain, args.layer, args.source_feed, "source_feed"],
                "dataGranularityDescription": table_cfg.get("grain") or (f"One row per {', '.join(key_cols)} per batch." if key_cols else "One row per source record per batch."),
                "properties": properties,
                "quality": quality,
                **(
                    {
                        "customProperties": [
                            {
                                "property": "allowNoPrimaryKey",
                                "value": True,
                                "description": "This source object is a grouped/sparse extract without one universal physical primary key column.",
                            }
                        ]
                    }
                    if table_cfg.get("allowNoPrimaryKey")
                    else {}
                ),
            }
        )
        expected_files.append(path.name)

    path_template = args.path_template or f"{args.input_path.as_posix()}/*.csv"
    custom_properties = [
        {"property": "allianzClassification", "value": config.get("allianzClassification", "Internal")},
        {"property": "containsPersonalData", "value": bool(config.get("containsPersonalData", contains_personal_data))},
        {"property": "sourceSystem", "value": args.source_system},
        {"property": "sourceFeed", "value": args.source_feed},
        {"property": "pipelineLayer", "value": args.layer},
        {"property": "expectedFiles", "value": expected_files},
        {
            "property": "notebookValidationConfig",
            "value": {
                "contractReader": "yaml.safe_load",
                "schemaObjectPath": "schema",
                "qualityRulesPath": "schema[].quality",
                "ruleCatalogPath": "customProperties[property=dqRuleCatalog].value",
                "sourceFileField": "physicalName",
                "entityField": "name",
                "batchParameter": "batch_ref",
                "defaultTargetWatermakExp": args.batch_filter,
                "placeholderStyle": "python_format",
                "placeholders": {
                    "full_table_name": "Databricks temp view or fully qualified table name for the current source object",
                    "target_watermak_exp": "Batch filter expression used by existing ETL DQ SQL templates",
                    "table_pk": "Primary key column list for the current source object",
                    "table_bk": "Business key column list for the current source object",
                    "table_bk_join": "Join condition between aliases a and b for the business key",
                    "source_table": "Source table/view used for reconciliation checks",
                    "source_pk": "Source primary key used for reconciliation checks",
                },
                "entities": entities,
            },
        },
        {"property": "dqRuleCatalog", "value": rule_catalog(parse_rule_file(args.rules_file), entities, args.layer)},
        {
            "property": "breakingChangePolicy",
            "value": "Removing, renaming, or changing type/nullability of a required field is breaking.",
        },
    ]

    return {
        "kind": "DataContract",
        "apiVersion": "v3.1.0",
        "id": args.contract_id,
        "name": args.contract_name,
        "version": args.version,
        "status": args.status,
        "domain": args.domain,
        "tenant": args.tenant,
        "description": {
            "purpose": config.get("purpose") or f"Defines the {args.layer} source contract for {args.domain} {args.source_feed}.",
            "limitations": config.get("limitations") or "Generated draft contract; review business metadata before approval.",
            "usage": config.get("usage") or f"Validate {args.source_feed} files before downstream ingestion.",
        },
        "authoritativeDefinitions": [
            {"type": "canonical", "url": args.output.as_posix(), "description": "Repository copy of this data contract"}
        ],
        "servers": [{"server": f"{args.domain}-{args.source_feed}-{args.layer}", "type": "local", "path": path_template, "format": "csv"}],
        "schema": schema_objects,
        "slaProperties": [
            {"property": "frequency", "value": int(config.get("frequency", 1)), "unit": config.get("frequencyUnit", "d")},
            {"property": "latency", "value": int(config.get("latency", 24)), "unit": config.get("latencyUnit", "h")},
            {"property": "retention", "value": int(config.get("retention", 7)), "unit": config.get("retentionUnit", "y")},
        ],
        "team": {
            "name": config.get("teamName", "Data Modelling & Engineering CoE"),
            "description": "Producing team / owner of this contract",
            "members": [{"username": args.owner_email, "role": "Owner", "dateIn": config.get("ownerDateIn", "2026-08-09")}],
        },
        "roles": [
            {
                "role": config.get("accessRole", f"{args.domain}_{args.layer}_ingestion"),
                "access": "read",
                "firstLevelApprovers": config.get("firstLevelApprovers", "Data Owner"),
                "secondLevelApprovers": config.get("secondLevelApprovers", "Data Governance"),
            }
        ],
        "support": [{"channel": config.get("supportChannel", "coe-data-contracts"), "tool": "email", "url": config.get("supportUrl", "mailto:coe-data-contracts@example-internal")}],
        "tags": [args.domain, args.layer, args.source_feed, "source_contract"],
        "customProperties": custom_properties,
        "contractCreatedTs": config.get("contractCreatedTs", "2026-08-09T00:00:00+00:00"),
    }


def validate_contract(contract: dict[str, Any], schema_path: Path) -> None:
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    errors = sorted(
        Draft201909Validator(schema, format_checker=FormatChecker()).iter_errors(contract),
        key=lambda err: list(err.absolute_path),
    )
    if errors:
        for err in errors[:50]:
            loc = "/".join(str(part) for part in err.absolute_path) or "<root>"
            print(f"{loc}: {err.message}")
        raise SystemExit(f"Generated contract failed ODCS validation with {len(errors)} error(s).")


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    contract = build_contract(args, config)
    validate_contract(contract, args.schema_path)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    header = (
        f"# =============================================================================\n"
        f"# Data Contract - {args.contract_name}\n"
        f"# ODCS v3.1.0\n"
        f"# =============================================================================\n\n"
    )
    args.output.write_text(header + yaml.safe_dump(contract, sort_keys=False, allow_unicode=False, width=120), encoding="utf-8")
    print(f"Wrote {args.output}")
    print(f"Objects: {len(contract['schema'])}")
    print(f"Columns: {sum(len(obj['properties']) for obj in contract['schema'])}")


if __name__ == "__main__":
    main()
