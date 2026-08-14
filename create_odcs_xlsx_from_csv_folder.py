from __future__ import annotations

import argparse
import csv
import re
from copy import copy
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from openpyxl import load_workbook
from openpyxl.workbook.defined_name import DefinedName


DEFAULT_TEMPLATE = Path("dimdc/odcs-template-updated.xlsx")
DEFAULT_OUTPUT_DIR = Path("dimdc/outputs")
DEFAULT_RULES = Path("dimdc/rules.csv")
DEFAULT_CUSTOM_RULE_IDS = "rule_0001,rule_0007"

PROPERTY_COLUMNS = [
    "Property",
    "Business Name",
    "Logical Type",
    "Physical Type",
    "Example(s)",
    "Description",
    "Required",
    "Unique",
    "Classification",
    "Tags",
    "Authoritative Definition URL",
    "Authoritative Definition Type",
]

QUALITY_COLUMNS = [
    "Schema",
    "Property",
    "Quality Type",
    "Description",
    "Rule (Library)",
    "Query (SQL)",
    "Threshold Operator",
    "Threshold Value",
    "Quality Engine (Custom)",
    "Implementation (Custom)",
    "Severity",
    "Scheduler",
    "Schedule",
]


def _business_name(value: str) -> str:
    return value.replace("_", " ").replace("-", " ").title()


def _safe_sheet_title(table_name: str, used_titles: set[str]) -> str:
    raw = "Schema " + re.sub(r"[\\/*?:\[\]]", "_", table_name)
    if len(raw) <= 31 and raw not in used_titles:
        used_titles.add(raw)
        return raw
    for index in range(1, 1000):
        suffix = f"_{index}"
        candidate = raw[: 31 - len(suffix)] + suffix
        if candidate not in used_titles:
            used_titles.add(candidate)
            return candidate
    raise RuntimeError(f"Could not create a unique sheet title for {table_name}")


def _quoted_sheet_title(title: str) -> str:
    return "'" + title.replace("'", "''") + "'"


def _add_local_defined_name(ws: Any, name: str, cell_range: str) -> None:
    ws.defined_names.add(DefinedName(name=name, attr_text=f"{_quoted_sheet_title(ws.title)}!{cell_range}"))


def _add_schema_defined_names(ws: Any) -> None:
    ws.defined_names.clear()
    _add_local_defined_name(ws, "schema.name", "$B$5")
    _add_local_defined_name(ws, "schema.physicalType", "$B$6")
    _add_local_defined_name(ws, "schema.description", "$B$7")
    _add_local_defined_name(ws, "schema.businessName", "$B$8")
    _add_local_defined_name(ws, "schema.physicalName", "$B$9")
    _add_local_defined_name(ws, "schema.dataGranularityDescription", "$B$10")
    _add_local_defined_name(ws, "schema.tags", "$B$11")
    _add_local_defined_name(ws, "schema.properties", "$A$13:$AZ$981")


def _copy_row_style(ws: Any, source_row: int, target_row: int, max_col: int) -> None:
    ws.row_dimensions[target_row].height = ws.row_dimensions[source_row].height
    for col in range(1, max_col + 1):
        source = ws.cell(source_row, col)
        target = ws.cell(target_row, col)
        if source.has_style:
            target._style = copy(source._style)
        if source.number_format:
            target.number_format = source.number_format
        if source.alignment:
            target.alignment = copy(source.alignment)
        if source.protection:
            target.protection = copy(source.protection)
        if source.font:
            target.font = copy(source.font)
        if source.fill:
            target.fill = copy(source.fill)
        if source.border:
            target.border = copy(source.border)


def _find_row(ws: Any, label: str) -> int:
    wanted = label.strip().lower()
    for row in range(1, ws.max_row + 1):
        for col in range(1, ws.max_column + 1):
            value = ws.cell(row, col).value
            if isinstance(value, str) and value.strip().lower() == wanted:
                return row
    raise RuntimeError(f"Could not find row label {label!r} in {ws.title!r}")


def _set_label_value(ws: Any, label: str, value: Any) -> None:
    wanted = label.strip().lower()
    for row in range(1, ws.max_row + 1):
        for col in range(1, min(ws.max_column, 6) + 1):
            cell_value = ws.cell(row, col).value
            if isinstance(cell_value, str) and cell_value.strip().lower() == wanted:
                ws.cell(row, col + 1).value = value
                return


def _clear_rows(ws: Any, start_row: int, max_col: int) -> None:
    for row in range(start_row, ws.max_row + 1):
        for col in range(1, max_col + 1):
            ws.cell(row, col).value = None


def _read_csv_sample(path: Path, sample_rows: int) -> tuple[list[str], list[dict[str, str]], int]:
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        headers = [header.strip() for header in (reader.fieldnames or []) if header and header.strip()]
        rows: list[dict[str, str]] = []
        total_rows = 0
        for row in reader:
            total_rows += 1
            if len(rows) < sample_rows:
                rows.append({header: str(row.get(header, "") or "").strip() for header in headers})
    return headers, rows, total_rows


def _is_integer(values: list[str]) -> bool:
    if not values:
        return False
    for value in values:
        try:
            int(value)
        except ValueError:
            return False
    return True


def _is_number(values: list[str]) -> bool:
    if not values:
        return False
    for value in values:
        try:
            float(value)
        except ValueError:
            return False
    return True


def _is_boolean(values: list[str]) -> bool:
    valid = {"true", "false", "t", "f", "yes", "no", "y", "n", "0", "1"}
    return bool(values) and all(value.lower() in valid for value in values)


def _is_datetime(values: list[str]) -> bool:
    if not values:
        return False
    for value in values:
        text = value.replace("Z", "+00:00")
        try:
            datetime.fromisoformat(text)
        except ValueError:
            return False
    return True


def _is_date(values: list[str]) -> bool:
    if not values:
        return False
    for value in values:
        try:
            datetime.strptime(value[:10], "%Y-%m-%d")
        except ValueError:
            return False
    return True


def _infer_types(values: list[str]) -> tuple[str, str]:
    non_empty = [value for value in values if value != ""]
    if _is_boolean(non_empty):
        return "boolean", "boolean"
    if _is_integer(non_empty):
        return "integer", "bigint"
    if _is_number(non_empty):
        return "number", "decimal(18,2)"
    if _is_datetime(non_empty):
        return "timestamp", "timestamp"
    if _is_date(non_empty):
        return "date", "date"

    max_len = max((len(value) for value in non_empty), default=64)
    size = 32 if max_len <= 32 else 64 if max_len <= 64 else 128 if max_len <= 128 else 512
    return "string", f"varchar({size})"


def _classification(column: str) -> str:
    normalized = column.lower()
    sensitive_tokens = [
        "email",
        "phone",
        "mobile",
        "name",
        "address",
        "dob",
        "birth",
        "gender",
        "nationality",
        "postcode",
        "zipcode",
        "zip",
    ]
    if any(token in normalized for token in sensitive_tokens):
        return "confidential"
    if normalized.endswith("_sk") or normalized == "sk":
        return "restricted"
    return "internal"


def _infer_primary_key(table_name: str, columns: list[str], rows: list[dict[str, str]]) -> str | None:
    candidates = [
        f"{table_name}_id",
        f"{table_name}_identifier",
        f"{table_name}_sk",
        "id",
        "identifier",
    ]
    lower_to_actual = {column.lower(): column for column in columns}
    for candidate in candidates:
        if candidate in lower_to_actual:
            return lower_to_actual[candidate]

    for suffix in ("_identifier", "_id", "_sk"):
        for column in columns:
            if column.lower().endswith(suffix):
                return column

    for column in columns:
        values = [row.get(column, "") for row in rows if row.get(column, "")]
        if values and len(values) == len(set(values)):
            return column
    return None


def _column_required(column: str, pk: str | None, rows: list[dict[str, str]]) -> bool:
    if column == pk:
        return True
    if column in {"batch_ref", "pull_ts", "origin_sys"}:
        return True
    if column.lower().endswith(("_id", "_identifier", "_sk")):
        return True
    values = [row.get(column, "") for row in rows]
    return bool(values) and all(value != "" for value in values)


def _table_metadata(path: Path, sample_rows: int) -> dict[str, Any]:
    columns, rows, row_count = _read_csv_sample(path, sample_rows)
    table_name = path.stem
    pk = _infer_primary_key(table_name, columns, rows)
    properties = []
    for column in columns:
        values = [row.get(column, "") for row in rows]
        logical_type, physical_type = _infer_types(values)
        properties.append({
            "Property": column,
            "Business Name": _business_name(column),
            "Logical Type": logical_type,
            "Physical Type": physical_type,
            "Example(s)": next((value for value in values if value), ""),
            "Description": f"Source field {column}.",
            "Required": "TRUE" if _column_required(column, pk, rows) else "FALSE",
            "Unique": "TRUE" if column == pk else "FALSE",
            "Classification": _classification(column),
            "Tags": "primary_key" if column == pk else "",
            "Authoritative Definition URL": "",
            "Authoritative Definition Type": "",
        })
    return {
        "table_name": table_name,
        "physical_name": path.name,
        "path": path,
        "columns": columns,
        "rows": rows,
        "row_count": row_count,
        "primary_key": pk,
        "properties": properties,
    }


def _write_fundamentals(wb: Any, input_path: Path, contract_name: str, domain: str, data_product: str) -> None:
    if "Fundamentals" not in wb.sheetnames:
        return
    ws = wb["Fundamentals"]
    values = {
        "Kind": "DataContract",
        "API Version": "v3.1.0",
        "ID": re.sub(r"[^a-zA-Z0-9_-]+", "-", contract_name.lower()).strip("-"),
        "Name": contract_name,
        "Version": "1.0.0",
        "Status": "draft",
        "Domain": domain,
        "Data Product": data_product,
        "Tenant": "",
        "Purpose": f"Ad hoc ODCS Excel generated from CSV files in {input_path}.",
        "Limitations": "Generated from CSV headers and sampled values; review inferred types and key columns before approval.",
        "Usage": "Used to bootstrap an ODCS data contract workbook from a folder of CSV files.",
    }
    for label, value in values.items():
        _set_label_value(ws, label, value)


def _write_schema_sheet(ws: Any, table: dict[str, Any], tags: str) -> None:
    _add_schema_defined_names(ws)
    table_name = table["table_name"]
    _set_label_value(ws, "Name", table_name)
    _set_label_value(ws, "Type", "table")
    _set_label_value(ws, "Description", f"CSV source records for {table_name}.")
    _set_label_value(ws, "Business Name", _business_name(table_name))
    _set_label_value(ws, "Physical Name", table["physical_name"])
    grain = f"One row per {table['primary_key']}." if table["primary_key"] else "One row per CSV record."
    _set_label_value(ws, "Data Granularity", grain)
    _set_label_value(ws, "Tags", tags)

    header_row = _find_row(ws, "Property")
    data_start = header_row + 1
    _clear_rows(ws, data_start, max(len(PROPERTY_COLUMNS), ws.max_column))
    for row_index, prop in enumerate(table["properties"], start=data_start):
        _copy_row_style(ws, data_start, row_index, len(PROPERTY_COLUMNS))
        for col_index, key in enumerate(PROPERTY_COLUMNS, start=1):
            ws.cell(row_index, col_index).value = prop.get(key)


def _load_rule_catalog(rules_path: Path | None) -> dict[str, dict[str, str]]:
    if not rules_path or not rules_path.exists():
        return {}
    rules = pd.read_csv(rules_path).fillna("")
    return {
        str(row["rule_id"]).strip(): {key: str(value).strip() for key, value in row.items()}
        for row in rules.to_dict("records")
        if str(row.get("rule_id", "")).strip()
    }


def _custom_rule_quality_row(table: dict[str, Any], rule: dict[str, str]) -> dict[str, Any] | None:
    rule_id = rule.get("rule_id", "")
    rule_name = rule.get("rule_name", rule_id)
    if not table["primary_key"]:
        return None
    return {
        "Schema": table["table_name"],
        "Property": table["primary_key"],
        "Quality Type": "custom",
        "Description": f"{rule_id}: {rule_name} - {rule.get('query_desc', '')}",
        "Rule (Library)": rule_id,
        "Query (SQL)": "",
        "Threshold Operator": "",
        "Threshold Value": "",
        "Quality Engine (Custom)": "business_rule_catalog",
        "Implementation (Custom)": f"{rule_id}: {rule_name}",
        "Severity": rule.get("rule_type") or "Warning",
        "Scheduler": "",
        "Schedule": "",
    }


def _write_quality(
    wb: Any,
    tables: list[dict[str, Any]],
    rule_catalog: dict[str, dict[str, str]],
    custom_rule_ids: list[str],
) -> None:
    if "Quality" not in wb.sheetnames:
        return
    ws = wb["Quality"]
    header_row = _find_row(ws, "Schema")
    data_start = header_row + 1
    _clear_rows(ws, data_start, len(QUALITY_COLUMNS))

    row_number = data_start
    quality_rows: list[dict[str, Any]] = []
    for table in tables:
        quality_rows.append({
            "Schema": table["table_name"],
            "Property": "",
            "Quality Type": "custom",
            "Description": "Table must contain at least one row.",
            "Rule (Library)": "rowCount",
            "Query (SQL)": "",
            "Threshold Operator": "",
            "Threshold Value": "",
            "Quality Engine (Custom)": "odcs_metric",
            "Implementation (Custom)": "rowCount mustBeGreaterThan 0",
            "Severity": "Warning",
            "Scheduler": "",
            "Schedule": "",
        })
        if table["primary_key"]:
            quality_rows.append({
                "Schema": table["table_name"],
                "Property": table["primary_key"],
                "Quality Type": "custom",
                "Description": "Primary key must not contain null values.",
                "Rule (Library)": "nullValues",
                "Query (SQL)": "",
                "Threshold Operator": "",
                "Threshold Value": "",
                "Quality Engine (Custom)": "odcs_metric",
                "Implementation (Custom)": "nullValues mustBe 0",
                "Severity": "Warning",
                "Scheduler": "",
                "Schedule": "",
            })
            quality_rows.append({
                "Schema": table["table_name"],
                "Property": table["primary_key"],
                "Quality Type": "custom",
                "Description": "Primary key must not contain duplicate values.",
                "Rule (Library)": "duplicateValues",
                "Query (SQL)": "",
                "Threshold Operator": "",
                "Threshold Value": "",
                "Quality Engine (Custom)": "odcs_metric",
                "Implementation (Custom)": "duplicateValues mustBe 0",
                "Severity": "Warning",
                "Scheduler": "",
                "Schedule": "",
            })
            for rule_id in custom_rule_ids:
                rule = rule_catalog.get(rule_id)
                if not rule:
                    continue
                row = _custom_rule_quality_row(table, rule)
                if row:
                    quality_rows.append(row)

    for row in quality_rows:
        _copy_row_style(ws, data_start, row_number, len(QUALITY_COLUMNS))
        for col_index, key in enumerate(QUALITY_COLUMNS, start=1):
            ws.cell(row_number, col_index).value = row.get(key)
        row_number += 1


def create_odcs_xlsx_from_csv_folder(
    input_path: Path,
    output_path: Path,
    template_path: Path,
    sample_rows: int,
    contract_name: str,
    domain: str,
    data_product: str,
    tags: str,
    rules_path: Path | None,
    custom_rule_ids: list[str],
) -> Path:
    if not input_path.exists() or not input_path.is_dir():
        raise FileNotFoundError(f"Input path is not a directory: {input_path}")
    if not template_path.exists():
        raise FileNotFoundError(f"ODCS Excel template not found: {template_path}")

    csv_files = sorted(input_path.glob("*.csv"))
    if not csv_files:
        raise RuntimeError(f"No CSV files found in {input_path}")

    tables = [_table_metadata(path, sample_rows) for path in csv_files]
    rule_catalog = _load_rule_catalog(rules_path)
    wb = load_workbook(template_path)
    _write_fundamentals(wb, input_path, contract_name, domain, data_product)

    schema_templates = [name for name in wb.sheetnames if name.startswith("Schema")]
    if not schema_templates:
        raise RuntimeError("Template does not contain any Schema sheet.")
    template_ws = wb[schema_templates[0]]
    used_titles = {name for name in wb.sheetnames if not name.startswith("Schema")}

    created_sheets = []
    for table in tables:
        ws = wb.copy_worksheet(template_ws)
        ws.title = _safe_sheet_title(table["table_name"], used_titles)
        _write_schema_sheet(ws, table, tags)
        created_sheets.append(ws.title)

    for name in schema_templates:
        if name in wb.sheetnames:
            del wb[name]

    fundamentals_index = wb.sheetnames.index("Fundamentals") + 1 if "Fundamentals" in wb.sheetnames else 0
    for offset, name in enumerate(created_sheets):
        current_index = wb.sheetnames.index(name)
        wb.move_sheet(wb[name], offset=-(current_index - fundamentals_index - offset))

    _write_quality(wb, tables, rule_catalog, custom_rule_ids)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)
    return output_path


def _default_output(input_path: Path) -> Path:
    safe_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", input_path.name).strip("_") or "csv_folder"
    return DEFAULT_OUTPUT_DIR / f"{safe_name}_odcs_contract.xlsx"


def main() -> None:
    parser = argparse.ArgumentParser(description="Create an ODCS Excel workbook from all CSV files in a folder.")
    parser.add_argument("--input-path", required=True, help="Folder containing CSV files. Each CSV becomes one schema sheet.")
    parser.add_argument("--output", help="Output XLSX path. Defaults to dimdc/outputs/<folder>_odcs_contract.xlsx.")
    parser.add_argument("--template", default=str(DEFAULT_TEMPLATE), help="ODCS Excel template path.")
    parser.add_argument("--sample-rows", type=int, default=1000, help="Rows sampled per CSV for type/key inference.")
    parser.add_argument("--contract-name", default="Ad Hoc CSV Folder ODCS Data Contract")
    parser.add_argument("--domain", default="adhoc")
    parser.add_argument("--data-product", default="csv_folder")
    parser.add_argument("--tags", default="adhoc, csv, source_feed")
    parser.add_argument("--rules", default=str(DEFAULT_RULES), help="Rule catalog CSV for custom quality rule references.")
    parser.add_argument(
        "--custom-rule-ids",
        default=DEFAULT_CUSTOM_RULE_IDS,
        help="Comma-separated rule IDs from --rules to add for inferred primary keys. Use empty string to disable.",
    )
    args = parser.parse_args()

    input_path = Path(args.input_path)
    output_path = Path(args.output) if args.output else _default_output(input_path)
    custom_rule_ids = [rule_id.strip() for rule_id in args.custom_rule_ids.split(",") if rule_id.strip()]
    output = create_odcs_xlsx_from_csv_folder(
        input_path=input_path,
        output_path=output_path,
        template_path=Path(args.template),
        sample_rows=args.sample_rows,
        contract_name=args.contract_name,
        domain=args.domain,
        data_product=args.data_product,
        tags=args.tags,
        rules_path=Path(args.rules) if args.rules else None,
        custom_rule_ids=custom_rule_ids,
    )
    print(f"Generated: {output}")


if __name__ == "__main__":
    main()
