"""Verify an ODCS data contract against schema and physical CSV files."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from jsonschema import Draft201909Validator, FormatChecker


DEFAULT_SCHEMA = Path("data_contract/odcs-v3.1.0.schema_ODCS.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", required=True, type=Path, help="ODCS YAML contract path.")
    parser.add_argument("--input-path", required=True, type=Path, help="Folder containing physical CSV files.")
    parser.add_argument("--schema-path", default=DEFAULT_SCHEMA, type=Path)
    parser.add_argument("--check-keys", action="store_true", help="Check primary key nulls and duplicates.")
    parser.add_argument("--report", type=Path, help="Optional JSON report output path.")
    return parser.parse_args()


def add_issue(issues: list[dict[str, Any]], category: str, severity: str, message: str, **extra: Any) -> None:
    issues.append({"category": category, "severity": severity, "message": message, **extra})


def load_contract(path: Path, issues: list[dict[str, Any]]) -> dict[str, Any] | None:
    try:
        with path.open(encoding="utf-8") as handle:
            return yaml.safe_load(handle)
    except Exception as exc:
        add_issue(issues, "syntax", "critical", f"Could not parse YAML: {exc}")
        return None


def validate_odcs(contract: dict[str, Any], schema_path: Path, issues: list[dict[str, Any]]) -> None:
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        errors = sorted(
            Draft201909Validator(schema, format_checker=FormatChecker()).iter_errors(contract),
            key=lambda err: list(err.absolute_path),
        )
    except Exception as exc:
        add_issue(issues, "schema", "critical", f"Could not validate against ODCS schema: {exc}")
        return

    for err in errors:
        loc = "/".join(str(part) for part in err.absolute_path) or "<root>"
        add_issue(issues, "schema", "critical", err.message, field=loc)


def custom_property_map(contract: dict[str, Any]) -> dict[str, Any]:
    return {item.get("property"): item.get("value") for item in contract.get("customProperties", [])}


def item_custom_property_map(item: dict[str, Any]) -> dict[str, Any]:
    return {prop.get("property"): prop.get("value") for prop in item.get("customProperties", [])}


def validate_required_sections(contract: dict[str, Any], issues: list[dict[str, Any]]) -> None:
    for section in ("id", "name", "version", "status", "domain", "servers", "schema", "team", "support"):
        if not contract.get(section):
            add_issue(issues, "mandatory_section", "critical", f"Missing or empty required section: {section}", field=section)


def validate_physical_files(contract: dict[str, Any], input_path: Path, issues: list[dict[str, Any]]) -> None:
    seen_objects: set[str] = set()
    seen_ids: set[str] = set()
    for obj in contract.get("schema", []):
        entity = obj.get("name")
        physical = obj.get("physicalName")
        if entity in seen_objects:
            add_issue(issues, "duplicate", "critical", f"Duplicate schema object name: {entity}", entity=entity)
        seen_objects.add(entity)
        if obj.get("id") in seen_ids:
            add_issue(issues, "duplicate", "critical", f"Duplicate schema object id: {obj.get('id')}", entity=entity)
        seen_ids.add(obj.get("id"))

        if not physical:
            add_issue(issues, "schema", "critical", "Schema object has no physicalName.", entity=entity)
            continue
        path = input_path / physical
        if not path.exists():
            add_issue(issues, "file", "critical", f"Expected file is missing: {path}", entity=entity, file=physical)
            continue

        with path.open(newline="", encoding="utf-8") as handle:
            actual_columns = next(csv.reader(handle))
        contract_columns = [prop.get("name") for prop in obj.get("properties", [])]
        missing = [col for col in contract_columns if col not in actual_columns]
        extra = [col for col in actual_columns if col not in contract_columns]
        if missing:
            add_issue(issues, "schema_mismatch", "critical", f"Missing physical columns: {missing}", entity=entity, file=physical)
        if extra:
            add_issue(issues, "schema_mismatch", "warning", f"Extra physical columns not in contract: {extra}", entity=entity, file=physical)

        duplicate_columns = sorted({col for col in contract_columns if contract_columns.count(col) > 1})
        if duplicate_columns:
            add_issue(issues, "duplicate", "critical", f"Duplicate contract columns: {duplicate_columns}", entity=entity, file=physical)


def validate_keys(contract: dict[str, Any], input_path: Path, issues: list[dict[str, Any]]) -> None:
    for obj in contract.get("schema", []):
        physical = obj.get("physicalName")
        entity = obj.get("name")
        if item_custom_property_map(obj).get("allowNoPrimaryKey") is True:
            continue
        path = input_path / physical if physical else None
        if not path or not path.exists():
            continue
        key_cols = [prop.get("name") for prop in obj.get("properties", []) if prop.get("primaryKey")]
        if not key_cols:
            add_issue(issues, "key", "critical", "No primary key defined for schema object.", entity=entity, file=physical)
            continue
        try:
            df = pd.read_csv(path, usecols=key_cols)
        except Exception as exc:
            add_issue(issues, "key", "critical", f"Could not read key columns {key_cols}: {exc}", entity=entity, file=physical)
            continue
        nulls = int(df[key_cols].isna().any(axis=1).sum())
        duplicates = int(df.duplicated(subset=key_cols).sum())
        if nulls:
            add_issue(issues, "key", "critical", f"Primary key has {nulls} null row(s).", entity=entity, file=physical, columns=key_cols)
        if duplicates:
            add_issue(issues, "key", "critical", f"Primary key has {duplicates} duplicate row(s).", entity=entity, file=physical, columns=key_cols)


def validate_quality_rules(contract: dict[str, Any], issues: list[dict[str, Any]]) -> None:
    seen_rule_ids: set[str] = set()
    for obj in contract.get("schema", []):
        entity = obj.get("name")
        column_names = {prop.get("name") for prop in obj.get("properties", [])}
        rules: list[tuple[str, dict[str, Any]]] = [("table", rule) for rule in obj.get("quality", []) or []]
        for prop in obj.get("properties", []) or []:
            for rule in prop.get("quality", []) or []:
                rules.append((prop.get("name") or "column", rule))

        for scope, rule in rules:
            rule_id = rule.get("id")
            if rule_id:
                scoped_id = f"{entity}.{scope}.{rule_id}"
                if scoped_id in seen_rule_ids:
                    add_issue(issues, "duplicate", "critical", f"Duplicate quality rule id in scope: {scoped_id}", entity=entity)
                seen_rule_ids.add(scoped_id)

            custom = item_custom_property_map(rule)
            referenced_columns = custom.get("referencedColumns") or custom.get("columnNames") or []
            if isinstance(referenced_columns, str):
                referenced_columns = [referenced_columns]
            for column in referenced_columns:
                if column not in column_names:
                    add_issue(
                        issues,
                        "dq_rule_reference",
                        "critical",
                        f"Quality rule references missing column: {column}",
                        entity=entity,
                        rule_id=rule_id,
                    )


def validate_notebook_metadata(contract: dict[str, Any], issues: list[dict[str, Any]]) -> None:
    custom = custom_property_map(contract)
    schema_names = {obj.get("name") for obj in contract.get("schema", [])}
    config = custom.get("notebookValidationConfig")
    catalog = custom.get("dqRuleCatalog")
    if not isinstance(config, dict):
        add_issue(issues, "notebook_metadata", "critical", "Missing customProperties notebookValidationConfig.")
    else:
        entities = config.get("entities")
        if not isinstance(entities, list) or not entities:
            add_issue(issues, "notebook_metadata", "critical", "notebookValidationConfig.entities is missing or empty.")
        else:
            for entity in entities:
                if entity.get("entity") not in schema_names:
                    add_issue(issues, "notebook_metadata", "critical", f"Notebook entity not found in schema: {entity.get('entity')}")
                if not entity.get("sourceFile"):
                    add_issue(issues, "notebook_metadata", "critical", f"Notebook entity has no sourceFile: {entity.get('entity')}")

    if not isinstance(catalog, dict):
        add_issue(issues, "dq_rule_catalog", "critical", "Missing customProperties dqRuleCatalog.")
    else:
        active_rules = catalog.get("activeRules")
        if not isinstance(active_rules, list) or not active_rules:
            add_issue(issues, "dq_rule_catalog", "warning", "dqRuleCatalog.activeRules is missing or empty.")
        else:
            for rule in active_rules:
                for entity in rule.get("applicableTo", []):
                    if entity not in schema_names:
                        add_issue(issues, "dq_rule_catalog", "critical", f"Rule {rule.get('ruleId')} references unknown entity {entity}.")


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    issues: list[dict[str, Any]] = []
    contract = load_contract(args.contract, issues)
    if contract:
        validate_odcs(contract, args.schema_path, issues)
        validate_required_sections(contract, issues)
        validate_physical_files(contract, args.input_path, issues)
        validate_quality_rules(contract, issues)
        validate_notebook_metadata(contract, issues)
        if args.check_keys:
            validate_keys(contract, args.input_path, issues)

    critical_count = sum(1 for issue in issues if issue["severity"] == "critical")
    warning_count = sum(1 for issue in issues if issue["severity"] == "warning")
    report = {
        "contract": str(args.contract),
        "inputPath": str(args.input_path),
        "status": "fail" if critical_count else "pass",
        "criticalCount": critical_count,
        "warningCount": warning_count,
        "issues": issues,
    }
    if args.report:
        write_report(args.report, report)

    print(f"Contract: {args.contract}")
    print(f"Status: {report['status']}")
    print(f"Critical: {critical_count}")
    print(f"Warnings: {warning_count}")
    for issue in issues[:25]:
        print(f"- [{issue['severity']}] {issue['category']}: {issue['message']}")
    if len(issues) > 25:
        print(f"... {len(issues) - 25} more issue(s)")
    raise SystemExit(1 if critical_count else 0)


if __name__ == "__main__":
    main()
