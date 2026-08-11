# Databricks notebook source
# MAGIC %md
# MAGIC # Data Contract Validator
# MAGIC
# MAGIC Generic ODCS v3.1.0 contract runner.
# MAGIC
# MAGIC This notebook accepts a data contract YAML path, loads matching source data, runs contract checks, prints pass/fail results, and can optionally write a Delta execution log.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inputs

# COMMAND ----------

# DBTITLE 1,Initialize Databricks Widgets for Parameter Configurati ...
try:
    dbutils.widgets.text("contract_path", "")
    dbutils.widgets.text("input_base_path", "")
    dbutils.widgets.text("batch_ref", "")
    dbutils.widgets.dropdown("mode", "file", ["file", "table", "postgres"])
    dbutils.widgets.dropdown("run_mode", "report_only", ["report_only", "fail_fast"])
    dbutils.widgets.text("result_table", "")
    dbutils.widgets.text("table_prefix", "")
    dbutils.widgets.text("jdbc_url", "")
    dbutils.widgets.text("jdbc_user", "")
    dbutils.widgets.text("jdbc_password_secret_scope", "")
    dbutils.widgets.text("jdbc_password_secret_key", "")
except NameError:
    # Allows static parsing outside Databricks.
    pass

# COMMAND ----------

# DBTITLE 1,Import Libraries and Handle PyYAML Dependency Check
import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

try:
    import yaml
except ImportError as exc:
    raise ImportError(
        "PyYAML is required. In Databricks, run `%pip install PyYAML` and restart Python, "
        "or install PyYAML as a cluster library."
    ) from exc

# COMMAND ----------

# DBTITLE 1,Validate and Initialize Runtime Widgets with Defaults
def widget_value(name: str, default: str = "") -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value is not None else default
    except Exception:
        return default


CONTRACT_PATH = widget_value("contract_path")
INPUT_BASE_PATH = widget_value("input_base_path")
BATCH_REF = widget_value("batch_ref")
MODE = widget_value("mode", "file").lower()
RUN_MODE = widget_value("run_mode", "report_only").lower()
RESULT_TABLE = widget_value("result_table")
TABLE_PREFIX = widget_value("table_prefix")
JDBC_URL = widget_value("jdbc_url")
JDBC_USER = widget_value("jdbc_user")
JDBC_PASSWORD_SECRET_SCOPE = widget_value("jdbc_password_secret_scope")
JDBC_PASSWORD_SECRET_KEY = widget_value("jdbc_password_secret_key")
VALIDATION_RUN_ID = str(uuid.uuid4())
VALIDATION_TS = datetime.now(timezone.utc).isoformat()

if not CONTRACT_PATH:
    raise ValueError("contract_path widget is required.")
if MODE not in {"file", "table", "postgres"}:
    raise ValueError("mode must be one of: file, table, postgres.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

# DBTITLE 1,Define Utility Functions for Data Loading and Validatio ...
def load_yaml(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Contract did not parse to a mapping: {path}")
    return data


def custom_property_map(items: list[dict[str, Any]] | None) -> dict[str, Any]:
    return {item.get("property"): item.get("value") for item in items or []}


def sanitize_view_name(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_]", "_", value)
    if not re.match(r"^[A-Za-z_]", sanitized):
        sanitized = f"v_{sanitized}"
    return f"dc_{sanitized}"


def csv_path(base_path: str, physical_name: str) -> str:
    return f"{base_path.rstrip('/')}/{physical_name}"


def get_jdbc_password() -> str:
    if JDBC_PASSWORD_SECRET_SCOPE and JDBC_PASSWORD_SECRET_KEY:
        return dbutils.secrets.get(JDBC_PASSWORD_SECRET_SCOPE, JDBC_PASSWORD_SECRET_KEY)
    return ""


def table_name_for(entity: str, physical_name: str) -> str:
    base = TABLE_PREFIX or INPUT_BASE_PATH
    if not base:
        return entity
    table_leaf = physical_name.rsplit(".", 1)[0] if physical_name else entity
    return f"{base.rstrip('.')}.{table_leaf}"


def load_object_df(obj: dict[str, Any]) -> DataFrame:
    entity = obj["name"]
    physical_name = obj.get("physicalName") or entity

    if MODE == "file":
        if not INPUT_BASE_PATH:
            raise ValueError("input_base_path is required for file mode.")
        path = csv_path(INPUT_BASE_PATH, physical_name)
        return spark.read.option("header", True).option("inferSchema", True).csv(path)

    if MODE == "table":
        return spark.table(table_name_for(entity, physical_name))

    if MODE == "postgres":
        if not JDBC_URL:
            raise ValueError("jdbc_url is required for postgres mode.")
        dbtable = table_name_for(entity, physical_name)
        reader = (
            spark.read.format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", dbtable)
            .option("driver", "org.postgresql.Driver")
        )
        if JDBC_USER:
            reader = reader.option("user", JDBC_USER)
        password = get_jdbc_password()
        if password:
            reader = reader.option("password", password)
        return reader.load()

    raise ValueError(f"Unsupported mode: {MODE}")


def get_entity_config(contract: dict[str, Any], entity: str) -> dict[str, Any]:
    custom = custom_property_map(contract.get("customProperties"))
    notebook_config = custom.get("notebookValidationConfig") or {}
    for item in notebook_config.get("entities", []) or []:
        if item.get("entity") == entity:
            return item
    return {}


def render_filter(template: str | None) -> str:
    if not template:
        return "1 = 1"
    return template.format(batch_ref=BATCH_REF)


def key_columns(obj: dict[str, Any], entity_config: dict[str, Any], key_name: str) -> list[str]:
    configured = entity_config.get(key_name)
    if configured:
        return configured
    if key_name == "primaryKeyColumns":
        return [prop["name"] for prop in obj.get("properties", []) if prop.get("primaryKey")]
    if key_name == "businessKeyColumns":
        primary = [prop["name"] for prop in obj.get("properties", []) if prop.get("primaryKey")]
        return primary
    return []


def comma_columns(columns: list[str]) -> str:
    return ", ".join(columns)


def join_condition(columns: list[str]) -> str:
    return " AND ".join([f"a.{col} = b.{col}" for col in columns]) if columns else "1 = 1"


def render_query(
    query: str,
    view_name: str,
    pk_cols: list[str],
    bk_cols: list[str],
    target_filter: str,
    source_table: str = "",
    source_pk_cols: list[str] | None = None,
) -> str:
    return query.format(
        full_table_name=view_name,
        table_pk=comma_columns(pk_cols),
        table_bk=comma_columns(bk_cols),
        table_bk_join=join_condition(bk_cols),
        target_watermak_exp=target_filter,
        source_table=source_table,
        source_pk=comma_columns(source_pk_cols or []),
    )


def compare_expected(actual: dict[str, Any], expected: Any) -> tuple[str, str]:
    if isinstance(expected, dict):
        mismatches = []
        for key, expected_value in expected.items():
            actual_value = actual.get(key)
            if actual_value != expected_value:
                mismatches.append(f"{key}: expected {expected_value}, actual {actual_value}")
        return ("pass", "") if not mismatches else ("fail", "; ".join(mismatches))
    first_value = next(iter(actual.values())) if actual else None
    return ("pass", "") if first_value == expected else ("fail", f"expected {expected}, actual {first_value}")


RESULTS: list[dict[str, Any]] = []


def add_result(
    *,
    entity: str,
    physical_name: str,
    rule_id: str,
    rule_name: str,
    rule_type: str,
    severity: str,
    expected_value: Any,
    actual_value: Any,
    status: str,
    message: str = "",
    failure_action: str = "",
) -> None:
    RESULTS.append(
        {
            "validation_run_id": VALIDATION_RUN_ID,
            "contract_id": CONTRACT.get("id"),
            "contract_version": str(CONTRACT.get("version")),
            "contract_path": CONTRACT_PATH,
            "domain": CONTRACT.get("domain"),
            "layer": custom_property_map(CONTRACT.get("customProperties")).get("pipelineLayer"),
            "source_feed": custom_property_map(CONTRACT.get("customProperties")).get("sourceFeed"),
            "entity": entity,
            "physical_name": physical_name,
            "rule_id": rule_id,
            "rule_name": rule_name,
            "rule_type": rule_type,
            "severity": severity,
            "expected_value": json.dumps(expected_value, default=str),
            "actual_value": json.dumps(actual_value, default=str),
            "status": status,
            "failure_action": failure_action,
            "error_message": message,
            "batch_ref": BATCH_REF,
            "validation_ts": VALIDATION_TS,
        }
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule Runners

# COMMAND ----------

# DBTITLE 1,Perform Schema and Key Validations for DataFrames
def run_structure_checks(obj: dict[str, Any], df: DataFrame) -> None:
    entity = obj["name"]
    physical_name = obj.get("physicalName") or entity
    expected_cols = [prop["name"] for prop in obj.get("properties", [])]
    actual_cols = df.columns

    missing = [col for col in expected_cols if col not in actual_cols]
    extra = [col for col in actual_cols if col not in expected_cols]
    add_result(
        entity=entity,
        physical_name=physical_name,
        rule_id="contract_schema_columns",
        rule_name="Expected columns exist",
        rule_type="schema",
        severity="error",
        expected_value={"missing_columns": []},
        actual_value={"missing_columns": missing, "extra_columns": extra},
        status="pass" if not missing else "fail",
        message="" if not missing else f"Missing columns: {missing}",
    )


def run_required_checks(obj: dict[str, Any], df: DataFrame) -> None:
    entity = obj["name"]
    physical_name = obj.get("physicalName") or entity
    for prop in obj.get("properties", []) or []:
        col = prop["name"]
        if prop.get("required") is not True or col not in df.columns:
            continue
        null_count = df.filter(F.col(col).isNull()).count()
        add_result(
            entity=entity,
            physical_name=physical_name,
            rule_id=f"{col}_required",
            rule_name=f"{col} required",
            rule_type="required",
            severity="error",
            expected_value={"null_count": 0},
            actual_value={"null_count": null_count},
            status="pass" if null_count == 0 else "fail",
            message="" if null_count == 0 else f"{col} has {null_count} null row(s).",
        )


def run_key_checks(obj: dict[str, Any], df: DataFrame, entity_config: dict[str, Any]) -> None:
    entity = obj["name"]
    physical_name = obj.get("physicalName") or entity
    if custom_property_map(obj.get("customProperties")).get("allowNoPrimaryKey") is True:
        add_result(
            entity=entity,
            physical_name=physical_name,
            rule_id="primary_key_not_required",
            rule_name="Primary key not required",
            rule_type="key",
            severity="info",
            expected_value={"allow_no_primary_key": True},
            actual_value={"allow_no_primary_key": True},
            status="pass",
            message="Primary key check skipped because allowNoPrimaryKey is true.",
        )
        return
    pk_cols = key_columns(obj, entity_config, "primaryKeyColumns")
    if not pk_cols:
        add_result(
            entity=entity,
            physical_name=physical_name,
            rule_id="primary_key_defined",
            rule_name="Primary key defined",
            rule_type="key",
            severity="error",
            expected_value={"primary_key_columns": "non_empty"},
            actual_value={"primary_key_columns": []},
            status="fail",
            message="No primary key defined in contract.",
        )
        return

    missing_key_cols = [col for col in pk_cols if col not in df.columns]
    if missing_key_cols:
        add_result(
            entity=entity,
            physical_name=physical_name,
            rule_id="primary_key_columns_exist",
            rule_name="Primary key columns exist",
            rule_type="key",
            severity="error",
            expected_value={"missing_key_columns": []},
            actual_value={"missing_key_columns": missing_key_cols},
            status="fail",
            message=f"Missing key columns: {missing_key_cols}",
        )
        return

    null_rows = df.filter(" OR ".join([f"{col} IS NULL" for col in pk_cols])).count()
    duplicate_rows = df.groupBy(*pk_cols).count().filter(F.col("count") > 1).count()
    add_result(
        entity=entity,
        physical_name=physical_name,
        rule_id="primary_key_nulls",
        rule_name="Primary key not null",
        rule_type="key",
        severity="error",
        expected_value={"null_rows": 0},
        actual_value={"null_rows": null_rows},
        status="pass" if null_rows == 0 else "fail",
        message="" if null_rows == 0 else f"Primary key has {null_rows} null row(s).",
    )
    add_result(
        entity=entity,
        physical_name=physical_name,
        rule_id="primary_key_duplicates",
        rule_name="Primary key unique",
        rule_type="key",
        severity="error",
        expected_value={"duplicate_keys": 0},
        actual_value={"duplicate_keys": duplicate_rows},
        status="pass" if duplicate_rows == 0 else "fail",
        message="" if duplicate_rows == 0 else f"Primary key has {duplicate_rows} duplicate key group(s).",
    )


def run_library_quality(obj: dict[str, Any], df: DataFrame, rule: dict[str, Any], scope_column: str | None = None) -> None:
    entity = obj["name"]
    physical_name = obj.get("physicalName") or entity
    metric = rule.get("metric")
    rule_id = rule.get("id") or metric or "library_quality"
    rule_name = rule.get("name") or rule.get("description") or rule_id
    severity = rule.get("severity", "error")
    expected_operator = next(
        (op for op in ("mustBe", "mustBeGreaterThan", "mustBeGreaterOrEqualTo", "mustBeLessThan", "mustBeLessOrEqualTo") if op in rule),
        None,
    )

    actual_value: Any
    status = "pass"
    message = ""

    if metric == "rowCount":
        actual_value = df.count()
    elif metric in {"nullValues", "missingValues"}:
        if not scope_column:
            status = "skip"
            actual_value = None
            message = "nullValues/missingValues requires a column scope."
        elif scope_column not in df.columns:
            status = "fail"
            actual_value = None
            message = f"Column not found: {scope_column}"
        else:
            actual_value = df.filter(F.col(scope_column).isNull()).count()
    elif metric == "duplicateValues":
        cols = [scope_column] if scope_column else key_columns(obj, get_entity_config(CONTRACT, entity), "primaryKeyColumns")
        if not cols or any(col not in df.columns for col in cols):
            status = "fail"
            actual_value = None
            message = f"Duplicate check columns missing: {cols}"
        else:
            actual_value = df.groupBy(*cols).count().filter(F.col("count") > 1).count()
    elif metric == "invalidValues":
        valid_values = ((rule.get("arguments") or {}).get("validValues")) or []
        if not scope_column:
            custom = custom_property_map(rule.get("customProperties"))
            refs = custom.get("referencedColumns") or []
            scope_column = refs[0] if refs else None
        if not scope_column or scope_column not in df.columns:
            status = "fail"
            actual_value = None
            message = f"invalidValues column not found: {scope_column}"
        else:
            actual_value = df.filter(~F.col(scope_column).isin(valid_values)).count()
    else:
        status = "skip"
        actual_value = None
        message = f"Unsupported library metric: {metric}"

    expected_value = {expected_operator: rule.get(expected_operator)} if expected_operator else {}
    if status == "pass" and expected_operator:
        expected = rule[expected_operator]
        if expected_operator == "mustBe":
            status = "pass" if actual_value == expected else "fail"
        elif expected_operator == "mustBeGreaterThan":
            status = "pass" if actual_value > expected else "fail"
        elif expected_operator == "mustBeGreaterOrEqualTo":
            status = "pass" if actual_value >= expected else "fail"
        elif expected_operator == "mustBeLessThan":
            status = "pass" if actual_value < expected else "fail"
        elif expected_operator == "mustBeLessOrEqualTo":
            status = "pass" if actual_value <= expected else "fail"
        if status == "fail" and not message:
            message = f"Expected {expected_operator} {expected}, actual {actual_value}"

    add_result(
        entity=entity,
        physical_name=physical_name,
        rule_id=rule_id,
        rule_name=rule_name,
        rule_type="library",
        severity=severity,
        expected_value=expected_value,
        actual_value={"actual": actual_value},
        status=status,
        message=message,
        failure_action=custom_property_map(rule.get("customProperties")).get("failureAction", ""),
    )


def run_sql_quality(obj: dict[str, Any], rule: dict[str, Any], view_name: str, entity_config: dict[str, Any]) -> None:
    entity = obj["name"]
    physical_name = obj.get("physicalName") or entity
    pk_cols = key_columns(obj, entity_config, "primaryKeyColumns")
    bk_cols = key_columns(obj, entity_config, "businessKeyColumns")
    source_pk_cols = key_columns(obj, entity_config, "sourcePrimaryKeyColumns")
    target_filter = render_filter(entity_config.get("targetWatermakExp") or "1 = 1")
    query = render_query(
        rule["query"],
        view_name,
        pk_cols,
        bk_cols,
        target_filter,
        source_table=entity_config.get("sourceTable", ""),
        source_pk_cols=source_pk_cols,
    )
    rule_id = rule.get("id") or rule.get("description", "sql_quality")
    rule_name = rule.get("name") or rule.get("description") or rule_id

    try:
        row = spark.sql(query).first()
        actual = row.asDict() if row is not None else {}
        status, message = compare_expected(actual, rule.get("mustBe"))
    except Exception as exc:
        actual = {}
        status = "fail"
        message = f"SQL execution failed: {exc}"

    add_result(
        entity=entity,
        physical_name=physical_name,
        rule_id=rule_id,
        rule_name=rule_name,
        rule_type="sql",
        severity=rule.get("severity", "error"),
        expected_value=rule.get("mustBe"),
        actual_value=actual,
        status=status,
        message=message,
        failure_action=custom_property_map(rule.get("customProperties")).get("failureAction", ""),
    )


def run_quality_rules(obj: dict[str, Any], df: DataFrame, view_name: str, entity_config: dict[str, Any]) -> None:
    for rule in obj.get("quality", []) or []:
        if rule.get("type", "library") == "sql":
            run_sql_quality(obj, rule, view_name, entity_config)
        elif rule.get("type", "library") == "library":
            run_library_quality(obj, df, rule)

    for prop in obj.get("properties", []) or []:
        column = prop["name"]
        for rule in prop.get("quality", []) or []:
            if rule.get("type", "library") == "sql":
                run_sql_quality(obj, rule, view_name, entity_config)
            elif rule.get("type", "library") == "library":
                run_library_quality(obj, df, rule, scope_column=column)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Contract

# COMMAND ----------

# DBTITLE 1,Contract Validation and Schema Integrity Checks
CONTRACT = load_yaml(CONTRACT_PATH)

required_sections = ["id", "name", "version", "status", "domain", "schema"]
for section in required_sections:
    status = "pass" if CONTRACT.get(section) else "fail"
    add_result(
        entity="_contract",
        physical_name="",
        rule_id=f"contract_{section}_present",
        rule_name=f"Contract {section} present",
        rule_type="contract",
        severity="error",
        expected_value={"present": True},
        actual_value={"present": bool(CONTRACT.get(section))},
        status=status,
        message="" if status == "pass" else f"Missing contract section: {section}",
    )

for obj in CONTRACT.get("schema", []) or []:
    entity = obj["name"]
    physical_name = obj.get("physicalName") or entity
    entity_config = get_entity_config(CONTRACT, entity)
    print(f"Validating {entity} ({physical_name})")

    try:
        df = load_object_df(obj)
        view_name = sanitize_view_name(entity)
        df.createOrReplaceTempView(view_name)
        run_structure_checks(obj, df)
        run_required_checks(obj, df)
        run_key_checks(obj, df, entity_config)
        run_quality_rules(obj, df, view_name, entity_config)
    except Exception as exc:
        add_result(
            entity=entity,
            physical_name=physical_name,
            rule_id="object_load",
            rule_name="Object load",
            rule_type="load",
            severity="error",
            expected_value={"loaded": True},
            actual_value={"loaded": False},
            status="fail",
            message=str(exc),
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

# DBTITLE 1,Summarize Validation Results and Handle Failures
result_schema = StructType(
    [
        StructField("validation_run_id", StringType(), True),
        StructField("contract_id", StringType(), True),
        StructField("contract_version", StringType(), True),
        StructField("contract_path", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("layer", StringType(), True),
        StructField("source_feed", StringType(), True),
        StructField("entity", StringType(), True),
        StructField("physical_name", StringType(), True),
        StructField("rule_id", StringType(), True),
        StructField("rule_name", StringType(), True),
        StructField("rule_type", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("expected_value", StringType(), True),
        StructField("actual_value", StringType(), True),
        StructField("status", StringType(), True),
        StructField("failure_action", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("batch_ref", StringType(), True),
        StructField("validation_ts", StringType(), True),
    ]
)

results_df = spark.createDataFrame(RESULTS, result_schema)
summary_df = results_df.groupBy("status").count()

print(f"Validation run ID: {VALIDATION_RUN_ID}")
print(f"Contract: {CONTRACT.get('id')} version {CONTRACT.get('version')}")
print(f"Mode: {MODE}")
print(f"Result count: {len(RESULTS)}")

display(summary_df)
display(results_df.orderBy("status", "entity", "rule_id"))

failed_df = results_df.filter(F.col("status") == "fail")
failed_count = failed_df.count()

if RESULT_TABLE:
    (
        results_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(RESULT_TABLE)
    )
    print(f"Wrote validation results to {RESULT_TABLE}")

if failed_count > 0:
    print(f"FAILED rules: {failed_count}")
    display(failed_df.orderBy("entity", "rule_id"))
else:
    print("All executed contract checks passed.")

if failed_count > 0 and RUN_MODE == "fail_fast":
    raise RuntimeError(f"Data contract validation failed. Failed rules: {failed_count}")
