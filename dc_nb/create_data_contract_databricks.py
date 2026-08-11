# Databricks notebook source
# MAGIC %md
# MAGIC # Create ODCS Data Contract
# MAGIC
# MAGIC Self-contained Databricks notebook source. It creates an ODCS v3.1.0 data contract from:
# MAGIC
# MAGIC - a template YAML
# MAGIC - a folder of input CSV files
# MAGIC - a YAML or CSV table config
# MAGIC - the ODCS JSON schema
# MAGIC
# MAGIC It does not import any project Python modules.

# COMMAND ----------

try:
    dbutils.widgets.text("template_path", "dc_nb/outputs/templates/raw_source_ODCS_template.yaml")
    dbutils.widgets.text("input_data_path", "F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260811151951/vault_ready_28")
    dbutils.widgets.text("table_config_path", "dc_nb/inputs/enhanced_prd01_contract_config.yaml")
    dbutils.widgets.text("rules_file_path", "")
    dbutils.widgets.text("contract_output_path", "dc_nb/outputs/contracts/enhanced_prd01_raw_ODCS.yaml")
    dbutils.widgets.text("odcs_schema_path", "dc_nb/inputs/odcs-v3.1.0.schema_ODCS.json")
    dbutils.widgets.text("contract_id", "enhanced-prd-01-raw-0001")
    dbutils.widgets.text("contract_name", "Enhanced PRD 01 Raw Source Feed")
    dbutils.widgets.text("domain", "enhanced")
    dbutils.widgets.text("layer", "raw")
    dbutils.widgets.text("source_feed", "prd_01")
    dbutils.widgets.text("source_system", "Enhanced source-1 vault_ready_28")
    dbutils.widgets.text("version", "1.0.0")
    dbutils.widgets.dropdown("status", "draft", ["draft", "active", "deprecated", "retired"])
    dbutils.widgets.text("tenant", "Allianz")
    dbutils.widgets.text("owner_email", "data-modelling-engineering-coe@example-internal")
    dbutils.widgets.text("batch_filter", "batch_ref = '{batch_ref}'")
    dbutils.widgets.text("sample_rows", "0")
except NameError:
    pass


def widget_value(name: str, default: str = "") -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value is not None else default
    except Exception:
        return default


TEMPLATE_PATH = widget_value("template_path", "dc_nb/outputs/templates/raw_source_ODCS_template.yaml")
INPUT_DATA_PATH = widget_value("input_data_path", "F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260811151951/vault_ready_28")
TABLE_CONFIG_PATH = widget_value("table_config_path", "dc_nb/inputs/enhanced_prd01_contract_config.yaml")
RULES_FILE_PATH = widget_value("rules_file_path", "")
CONTRACT_OUTPUT_PATH = widget_value("contract_output_path", "dc_nb/outputs/contracts/enhanced_prd01_raw_ODCS.yaml")
ODCS_SCHEMA_PATH = widget_value("odcs_schema_path", "dc_nb/inputs/odcs-v3.1.0.schema_ODCS.json")
CONTRACT_ID = widget_value("contract_id", "enhanced-prd-01-raw-0001")
CONTRACT_NAME = widget_value("contract_name", "Enhanced PRD 01 Raw Source Feed")
DOMAIN = widget_value("domain", "enhanced")
LAYER = widget_value("layer", "raw")
SOURCE_FEED = widget_value("source_feed", "prd_01")
SOURCE_SYSTEM = widget_value("source_system", "Enhanced source-1 vault_ready_28")
VERSION = widget_value("version", "1.0.0")
STATUS = widget_value("status", "draft")
TENANT = widget_value("tenant", "Allianz")
OWNER_EMAIL = widget_value("owner_email", "data-modelling-engineering-coe@example-internal")
BATCH_FILTER = widget_value("batch_filter", "batch_ref = '{batch_ref}'")
SAMPLE_ROWS = int(widget_value("sample_rows", "0") or "0")

# COMMAND ----------

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from jsonschema import Draft201909Validator, FormatChecker

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


def normalize_databricks_path(path: str) -> Path:
    if path.startswith("dbfs:/"):
        return Path("/dbfs") / path[len("dbfs:/") :]
    return Path(path)


def read_text(path: Path) -> str:
    with path.open("r", encoding="utf-8") as handle:
        return handle.read()


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        handle.write(text)


def stable_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "_", value).strip("_") or "value"


def title_from_stem(stem: str) -> str:
    return " ".join(part.capitalize() for part in stem.split("_"))


def parse_config_scalar(value: Any) -> Any:
    if value is None or not isinstance(value, str):
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
        rule[rule_key] = parse_config_scalar(value) if rule_key in {"arguments", "referencedColumns"} else value

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
            elif record_type == "table" and table_name:
                cfg = table_config(config, table_name)
                for field in table_fields:
                    set_if_present(cfg, field, row_value(row, field))
            elif record_type == "column" and table_name and column_name:
                cfg = table_config(config, table_name).setdefault("columns", {}).setdefault(column_name, {})
                for field in column_fields:
                    set_if_present(cfg, field, row_value(row, field))
            elif record_type == "table_rule" and table_name:
                rule = csv_rule(row)
                if rule:
                    table_config(config, table_name).setdefault("tableRules", []).append(rule)
            elif record_type == "column_rule" and table_name and column_name:
                rule = csv_rule(row)
                if rule:
                    column_cfg = table_config(config, table_name).setdefault("columns", {}).setdefault(column_name, {})
                    column_cfg.setdefault("columnRules", []).append(rule)

    if not config["tables"]:
        config.pop("tables")
    return config


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(path: Path | None) -> dict[str, Any]:
    if not path:
        return {}
    if path.suffix.lower() == ".csv":
        return load_csv_config(path)
    if path.suffix.lower() in {".yaml", ".yml"}:
        yaml_config = yaml.safe_load(read_text(path)) or {}
        csv_path = path.with_suffix(".csv")
        if csv_path.exists():
            return deep_merge(yaml_config, load_csv_config(csv_path))
        return yaml_config
    return yaml.safe_load(read_text(path)) or {}


def normalize_key(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]


def logical_type(series: pd.Series, column: str) -> str:
    col = column.lower()
    date_like = (
        col.endswith("_date")
        or "_date" in col
        or col.endswith("_dt")
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


def infer_key(df: pd.DataFrame, table_cfg: dict[str, Any]) -> list[str]:
    override = table_cfg.get("primaryKey") or table_cfg.get("businessKey")
    if override:
        return normalize_key(override)
    preferred_patterns = ("_identifier", "_id", "_ref", "_number")
    unique = [col for col in df.columns if df[col].isna().sum() == 0 and df[col].nunique(dropna=True) == len(df)]
    for pattern in preferred_patterns:
        for col in unique:
            if col.lower().endswith(pattern):
                return [col]
    return [unique[0]] if unique else []

# COMMAND ----------

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
    if default_column and not any(item["property"] == "referencedColumns" for item in custom_properties):
        custom_properties.append({"property": "referencedColumns", "value": [default_column]})
    if custom_properties:
        normalized["customProperties"] = custom_properties
    return normalized


def quality_rule(rule_id: str, key: str) -> dict[str, Any]:
    concat_expr = f"concat({key})"
    if rule_id == "rule_0001":
        return {
            "type": "sql",
            "query": f"select count(*) as null_count from {{full_table_name}} a where {concat_expr} is null and {{target_watermak_exp}}",
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
        "query": f"SELECT COUNT(*) AS invalid_values FROM {{full_table_name}} WHERE {column} < {value} AND {{target_watermak_exp}}",
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
        "query": f"SELECT COUNT(*) AS invalid_values FROM {{full_table_name}} WHERE {column} > {value} AND {{target_watermak_exp}}",
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


def parse_rule_file(path: Path | None) -> list[dict[str, str]]:
    if not path or not path.exists():
        return []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def rule_catalog(rule_rows: list[dict[str, str]], entities: list[dict[str, Any]], layer: str, source_rule_file: Path | None) -> dict[str, Any]:
    entity_names = [entity["entity"] for entity in entities]
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
    active_ids = {"rule_0001", "rule_0003", "rule_0007"}
    if not rule_rows:
        rule_rows = [{"rule_id": rule_id, "rule_name": defaults[rule_id]["queryDesc"], "rule_type": "error", "rule_category": defaults[rule_id]["queryDesc"]} for rule_id in sorted(active_ids)]
    active: list[dict[str, Any]] = []
    inactive: list[dict[str, Any]] = []
    for row in rule_rows:
        rule_id = row.get("rule_id", "")
        common = {"ruleId": rule_id, "ruleName": row.get("rule_name", ""), "sourceRuleType": row.get("rule_type", ""), "ruleCategory": row.get("rule_category", "")}
        if rule_id in active_ids:
            active.append({**common, **defaults[rule_id], "layer": layer, "applicableTo": entity_names})
        else:
            inactive.append({**common, "reasonNotActive": "Not applied by default for this generated contract.", "queryTemplate": row.get("query_text", "").strip()})
    source = str(source_rule_file).replace("\\", "/") if source_rule_file else "table_config_defaults"
    return {"sourceRuleFile": source, "activeRules": active, "inactiveRules": inactive}

# COMMAND ----------

def load_template(path: Path) -> dict[str, Any]:
    template = yaml.safe_load(read_text(path))
    if not isinstance(template, dict):
        raise ValueError(f"Template must parse to a mapping/object: {path}")
    if template.get("kind") != "DataContract":
        raise ValueError(f"Template kind must be DataContract. Got: {template.get('kind')}")
    if template.get("apiVersion") != "v3.1.0":
        raise ValueError(f"Template apiVersion must be v3.1.0. Got: {template.get('apiVersion')}")
    return template


def apply_template_defaults(contract: dict[str, Any], template: dict[str, Any]) -> dict[str, Any]:
    for key in ("servers", "slaProperties", "team", "support", "customProperties"):
        if key not in contract and key in template:
            contract[key] = template[key]
    contract["kind"] = template.get("kind", contract.get("kind", "DataContract"))
    contract["apiVersion"] = template.get("apiVersion", contract.get("apiVersion", "v3.1.0"))
    return contract


def validate_contract(contract: dict[str, Any], schema_path: Path) -> None:
    schema = json.loads(read_text(schema_path))
    errors = sorted(
        Draft201909Validator(schema, format_checker=FormatChecker()).iter_errors(contract),
        key=lambda err: list(err.absolute_path),
    )
    if errors:
        for err in errors[:50]:
            loc = "/".join(str(part) for part in err.absolute_path) or "<root>"
            print(f"{loc}: {err.message}")
        raise RuntimeError(f"Generated contract failed ODCS validation with {len(errors)} error(s).")


def build_contract(input_path: Path, output_path: Path, config: dict[str, Any], rules_file: Path | None) -> dict[str, Any]:
    table_configs = config.get("tables") or {}
    files = sorted(input_path.glob("*.csv"))
    if not files:
        raise ValueError(f"No CSV files found under input_data_path: {input_path}")

    schema_objects: list[dict[str, Any]] = []
    entities: list[dict[str, Any]] = []
    expected_files: list[str] = []
    contains_personal_data = False

    for path in files:
        table_cfg = table_configs.get(path.name) or table_configs.get(path.stem) or {}
        if table_cfg.get("exclude", False):
            continue
        df = pd.read_csv(path, nrows=SAMPLE_ROWS if SAMPLE_ROWS > 0 else None, low_memory=False)
        key_cols = infer_key(df, table_cfg)
        table_name = table_cfg.get("name") or path.stem
        properties = [build_property(col, df[col], key_cols, table_cfg) for col in df.columns]
        contains_personal_data = contains_personal_data or any(prop.get("classification") == "confidential" for prop in properties)
        quality: list[dict[str, Any]] = [
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
            entity_config = {
                "entity": table_name,
                "sourceFile": path.name,
                "primaryKeyColumns": key_cols,
                "businessKeyColumns": normalize_key(table_cfg.get("businessKey")) or key_cols,
                "fullTableNamePlaceholder": table_name,
                "targetWatermakExp": BATCH_FILTER,
            }
            if table_cfg.get("sourceTable"):
                entity_config["sourceTable"] = table_cfg["sourceTable"]
            if table_cfg.get("sourcePrimaryKey"):
                entity_config["sourcePrimaryKeyColumns"] = normalize_key(table_cfg["sourcePrimaryKey"])
            entities.append(entity_config)
        for rule in table_cfg.get("tableRules", []) or []:
            quality.append(normalize_quality_rule(rule))

        schema_objects.append(
            {
                "id": stable_id(table_name),
                "name": table_name,
                "physicalName": path.name,
                "physicalType": "table",
                "businessName": table_cfg.get("businessName") or title_from_stem(table_name),
                "description": table_cfg.get("description") or f"Raw {SOURCE_FEED} source records for {title_from_stem(table_name)}.",
                "tags": [DOMAIN, LAYER, SOURCE_FEED, "source_feed"],
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

    custom_properties = [
        {"property": "allianzClassification", "value": config.get("allianzClassification", "Internal")},
        {"property": "containsPersonalData", "value": bool(config.get("containsPersonalData", contains_personal_data))},
        {"property": "sourceSystem", "value": SOURCE_SYSTEM},
        {"property": "sourceFeed", "value": SOURCE_FEED},
        {"property": "pipelineLayer", "value": LAYER},
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
                "defaultTargetWatermakExp": BATCH_FILTER,
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
        {"property": "dqRuleCatalog", "value": rule_catalog(parse_rule_file(rules_file), entities, LAYER, rules_file)},
        {"property": "breakingChangePolicy", "value": "Removing, renaming, or changing type/nullability of a required field is breaking."},
    ]

    return {
        "kind": "DataContract",
        "apiVersion": "v3.1.0",
        "id": CONTRACT_ID,
        "name": CONTRACT_NAME,
        "version": VERSION,
        "status": STATUS,
        "domain": DOMAIN,
        "tenant": TENANT,
        "description": {
            "purpose": config.get("purpose") or f"Defines the {LAYER} source contract for {DOMAIN} {SOURCE_FEED}.",
            "limitations": config.get("limitations") or "Generated draft contract; review business metadata before approval.",
            "usage": config.get("usage") or f"Validate {SOURCE_FEED} files before downstream ingestion.",
        },
        "authoritativeDefinitions": [{"type": "canonical", "url": str(output_path).replace("\\", "/"), "description": "Repository copy of this data contract"}],
        "servers": [{"server": f"{DOMAIN}-{SOURCE_FEED}-{LAYER}", "type": "local", "path": f"{str(input_path).replace(chr(92), '/')}/*.csv", "format": "csv"}],
        "schema": schema_objects,
        "slaProperties": [
            {"property": "frequency", "value": int(config.get("frequency", 1)), "unit": config.get("frequencyUnit", "d")},
            {"property": "latency", "value": int(config.get("latency", 24)), "unit": config.get("latencyUnit", "h")},
            {"property": "retention", "value": int(config.get("retention", 7)), "unit": config.get("retentionUnit", "y")},
        ],
        "team": {
            "name": config.get("teamName", "Data Modelling & Engineering CoE"),
            "description": "Producing team / owner of this contract",
            "members": [{"username": OWNER_EMAIL, "role": "Owner", "dateIn": config.get("ownerDateIn", datetime.now(timezone.utc).date().isoformat())}],
        },
        "roles": [
            {
                "role": config.get("accessRole", f"{DOMAIN}_{LAYER}_ingestion"),
                "access": "read",
                "firstLevelApprovers": config.get("firstLevelApprovers", "Data Owner"),
                "secondLevelApprovers": config.get("secondLevelApprovers", "Data Governance"),
            }
        ],
        "support": [{"channel": config.get("supportChannel", "coe-data-contracts"), "tool": "email", "url": config.get("supportUrl", "mailto:coe-data-contracts@example-internal")}],
        "tags": [DOMAIN, LAYER, SOURCE_FEED, "source_contract"],
        "customProperties": custom_properties,
        "contractCreatedTs": config.get("contractCreatedTs", datetime.now(timezone.utc).isoformat()),
    }

# COMMAND ----------

template_path = normalize_databricks_path(TEMPLATE_PATH)
input_data_path = normalize_databricks_path(INPUT_DATA_PATH)
table_config_path = normalize_databricks_path(TABLE_CONFIG_PATH)
rules_file_path = normalize_databricks_path(RULES_FILE_PATH) if RULES_FILE_PATH else None
contract_output_path = normalize_databricks_path(CONTRACT_OUTPUT_PATH)
odcs_schema_path = normalize_databricks_path(ODCS_SCHEMA_PATH)

for label, path in {
    "template_path": template_path,
    "input_data_path": input_data_path,
    "table_config_path": table_config_path,
    "odcs_schema_path": odcs_schema_path,
}.items():
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")

csv_files = sorted(input_data_path.glob("*.csv"))
if not csv_files:
    raise ValueError(f"No CSV files found under input_data_path: {input_data_path}")

print(f"Template: {template_path}")
print(f"Input data: {input_data_path}")
print(f"CSV files: {len(csv_files)}")
print(f"Table config: {table_config_path}")
print(f"Rules file: {rules_file_path if rules_file_path else 'not supplied; using table config rules and default PK/BK catalog'}")
print(f"ODCS schema: {odcs_schema_path}")
print(f"Output contract: {contract_output_path}")

template = load_template(template_path)
print("Template validation before generation: pass")
print(f"Template apiVersion: {template['apiVersion']}")

config = load_config(table_config_path)
contract = build_contract(input_data_path, contract_output_path, config, rules_file_path)
contract = apply_template_defaults(contract, template)
validate_contract(contract, odcs_schema_path)

header = (
    "# =============================================================================\n"
    "# Generated ODCS v3.1.0 data contract. Review before production approval.\n"
    "# Generated from template + input data + table config + rule catalog.\n"
    "# =============================================================================\n"
)
write_text(contract_output_path, header + yaml.safe_dump(contract, sort_keys=False, allow_unicode=False, width=120))

object_count = len(contract.get("schema", []) or [])
column_count = sum(len(obj.get("properties", []) or []) for obj in contract.get("schema", []) or [])
rule_count = sum(len(obj.get("quality", []) or []) for obj in contract.get("schema", []) or [])
rule_count += sum(len(prop.get("quality", []) or []) for obj in contract.get("schema", []) or [] for prop in obj.get("properties", []) or [])

print(f"Contract written: {contract_output_path}")
print(f"Objects: {object_count}")
print(f"Columns: {column_count}")
print(f"Quality rules: {rule_count}")
print("ODCS schema validation: pass")

