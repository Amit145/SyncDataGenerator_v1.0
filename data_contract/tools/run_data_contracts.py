"""Run configured data contract generation and verification for a main.py run."""

from __future__ import annotations

import json
import os
import subprocess
from argparse import Namespace
from pathlib import Path
from typing import Any

import yaml

from data_contract.tools import create_data_contract
from data_contract.tools import export_odcs_excel
from data_contract.tools import verify_data_contract


DEFAULT_RUN_CONFIG = Path("data_contract/contract_run_config.yaml")
DEFAULT_SCHEMA = Path("data_contract/odcs-v3.1.0.schema_ODCS.json")
DEFAULT_EXCEL_BASE = Path("data_contract/gen_excels")


def load_run_config(path: str | Path = DEFAULT_RUN_CONFIG) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        return {"enabled": False, "contracts": []}
    with config_path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def resolve_context_value(context: dict[str, Any], dotted_name: str) -> Any:
    value: Any = context
    for part in dotted_name.split("."):
        if isinstance(value, dict):
            value = value.get(part)
        else:
            value = getattr(value, part, None)
        if value is None:
            return None
    return value


def render_template(value: str | None, context: dict[str, Any]) -> str | None:
    if value is None:
        return None
    rendered = value
    for key, raw_value in context.items():
        if isinstance(raw_value, (str, int, float)):
            rendered = rendered.replace("{" + key + "}", str(raw_value))
    return rendered


def write_contract(path: Path, contract: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header = (
        "# =============================================================================\n"
        "# Generated ODCS v3.1.0 data contract. Review before production approval.\n"
        "# =============================================================================\n"
    )
    path.write_text(header + yaml.safe_dump(contract, sort_keys=False, allow_unicode=False), encoding="utf-8")


def verify_contract(contract_path: Path, input_path: Path, schema_path: Path, check_keys: bool) -> dict[str, Any]:
    issues: list[dict[str, Any]] = []
    contract = verify_data_contract.load_contract(contract_path, issues)
    if contract:
        verify_data_contract.validate_odcs(contract, schema_path, issues)
        verify_data_contract.validate_required_sections(contract, issues)
        verify_data_contract.validate_physical_files(contract, input_path, issues)
        verify_data_contract.validate_quality_rules(contract, issues)
        verify_data_contract.validate_notebook_metadata(contract, issues)
        if check_keys:
            verify_data_contract.validate_keys(contract, input_path, issues)

    critical_count = sum(1 for issue in issues if issue["severity"] == "critical")
    warning_count = sum(1 for issue in issues if issue["severity"] == "warning")
    return {
        "contract": str(contract_path),
        "inputPath": str(input_path),
        "status": "fail" if critical_count else "pass",
        "criticalCount": critical_count,
        "warningCount": warning_count,
        "issues": issues,
    }


def derive_excel_path(contract_path: Path, excel_base: Path) -> Path:
    parts = list(contract_path.parts)
    if "generated" in parts:
        generated_index = parts.index("generated")
        relative = Path(*parts[generated_index + 1 :])
    else:
        relative = Path(contract_path.name)
    return excel_base / relative.with_suffix(".xlsx")


def derive_imported_yaml_path(excel_path: Path) -> Path:
    stem = excel_path.stem
    if stem.endswith("_ODCS"):
        stem = stem[: -len("_ODCS")]
    return excel_path.with_name(f"{stem}_imported.yaml")


def run_datacontract_command(command: list[str]) -> dict[str, Any]:
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
            env=env,
        )
    except FileNotFoundError:
        return {
            "command": command,
            "status": "fail",
            "returnCode": None,
            "stdout": "",
            "stderr": "datacontract CLI not found. Install it with: pip install datacontract-cli",
        }

    return {
        "command": command,
        "status": "pass" if completed.returncode == 0 else "fail",
        "returnCode": completed.returncode,
        "stdout": (completed.stdout or "").strip(),
        "stderr": (completed.stderr or "").strip(),
    }


def excel_round_trip(
    entry: dict[str, Any],
    contract_path: Path,
    execution_context: dict[str, Any],
    settings: dict[str, Any],
) -> dict[str, Any]:
    excel_base = Path(settings.get("outputBase", DEFAULT_EXCEL_BASE))
    template_path = Path(settings.get("template", export_odcs_excel.DEFAULT_EXCEL_TEMPLATE))

    excel_value = render_template(entry.get("excelOutput"), execution_context)
    excel_path = Path(excel_value) if excel_value else derive_excel_path(contract_path, excel_base)

    imported_value = render_template(entry.get("importedYamlOutput"), execution_context)
    imported_yaml_path = Path(imported_value) if imported_value else derive_imported_yaml_path(excel_path)

    export_summary = export_odcs_excel.export_contract_to_excel(
        contract_path=contract_path,
        output_path=excel_path,
        template_path=template_path,
    )

    import_command = [
        "datacontract",
        "import",
        "excel",
        "--source",
        str(excel_path),
        "--output",
        str(imported_yaml_path),
    ]
    import_result = run_datacontract_command(import_command)

    lint_result: dict[str, Any] | None = None
    if import_result["status"] == "pass" and settings.get("lintImportedYaml", True):
        lint_result = run_datacontract_command(["datacontract", "lint", str(imported_yaml_path)])

    status = "pass"
    if import_result["status"] != "pass" or (lint_result and lint_result["status"] != "pass"):
        status = "fail"

    return {
        "status": status,
        "excel": str(excel_path),
        "importedYaml": str(imported_yaml_path),
        "export": export_summary,
        "import": import_result,
        "lint": lint_result,
    }


def generate_contract(entry: dict[str, Any], input_path: Path, output_path: Path, schema_path: Path) -> None:
    args = Namespace(
        input_path=input_path,
        output=output_path,
        contract_id=entry["contractId"],
        contract_name=entry["contractName"],
        domain=entry["domain"],
        layer=entry["layer"],
        source_feed=entry["sourceFeed"],
        source_system=entry["sourceSystem"],
        version=str(entry.get("version", "1.0.0")),
        status=str(entry.get("status", "draft")),
        tenant=str(entry.get("tenant", "Allianz")),
        owner_email=str(entry.get("ownerEmail", "data-modelling-engineering-coe@example-internal")),
        rules_file=Path(entry["rulesFile"]) if entry.get("rulesFile") else create_data_contract.DEFAULT_RULES,
        schema_path=schema_path,
        config=Path(entry["config"]) if entry.get("config") else None,
        path_template=entry.get("pathTemplate"),
        batch_filter=str(entry.get("batchFilter", "batch_ref = '{batch_ref}'")),
        sample_rows=int(entry.get("sampleRows", 0)),
    )
    config = create_data_contract.load_config(args.config)
    contract = create_data_contract.build_contract(args, config)
    create_data_contract.validate_contract(contract, schema_path)
    write_contract(output_path, contract)


def run_data_contracts(
    execution_context: dict[str, Any],
    run_config_path: str | Path = DEFAULT_RUN_CONFIG,
) -> list[dict[str, Any]]:
    run_config = load_run_config(run_config_path)
    if not run_config.get("enabled", False):
        print("DATA CONTRACTS: skipped (disabled)")
        return []

    schema_path = Path(run_config.get("schemaPath", DEFAULT_SCHEMA))
    check_keys = bool(run_config.get("checkKeys", True))
    fail_main = bool(run_config.get("failMainOnContractError", True))
    excel_settings = run_config.get("excelRoundTrip") or {}
    excel_enabled = bool(excel_settings.get("enabled", False))
    fail_main_on_excel_error = bool(excel_settings.get("failMainOnError", fail_main))
    summaries: list[dict[str, Any]] = []

    for entry in run_config.get("contracts", []) or []:
        name = entry.get("name", "unnamed_contract")
        if not entry.get("enabled", True):
            summaries.append({"name": name, "status": "skipped", "reason": "disabled"})
            print(f"DATA CONTRACT {name}: skipped (disabled)")
            continue

        input_ref = entry.get("inputFrom")
        input_value = resolve_context_value(execution_context, input_ref) if input_ref else entry.get("inputPath")
        if not input_value:
            summaries.append({"name": name, "status": "skipped", "reason": f"input not available: {input_ref}"})
            print(f"DATA CONTRACT {name}: skipped (input not available: {input_ref})")
            continue

        input_path = Path(str(input_value))
        if not input_path.exists():
            summaries.append({"name": name, "status": "skipped", "reason": f"input path not found: {input_path}"})
            print(f"DATA CONTRACT {name}: skipped (input path not found: {input_path})")
            continue

        output_path = Path(render_template(entry["output"], execution_context) or entry["output"])
        report_value = render_template(entry.get("report"), execution_context) or entry.get("report")
        if report_value:
            report_path = Path(report_value)
        else:
            safe_name = str(name).replace(" ", "_").lower()
            report_path = Path(run_config.get("reportsBase", "data_contract/reports")) / f"{safe_name}_validation.json"

        generate_contract(entry, input_path, output_path, schema_path)
        report = verify_contract(output_path, input_path, schema_path, check_keys)

        excel_report: dict[str, Any] | None = None
        if excel_enabled and report["status"] == "pass":
            excel_report = excel_round_trip(entry, output_path, execution_context, excel_settings)
        elif excel_enabled:
            excel_report = {"status": "skipped", "reason": "YAML contract verification failed"}

        report_path.parent.mkdir(parents=True, exist_ok=True)
        if excel_report:
            report["excelRoundTrip"] = excel_report
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

        overall_status = report["status"]
        if excel_report and excel_report.get("status") == "fail":
            overall_status = "fail"

        summary = {
            "name": name,
            "status": overall_status,
            "criticalCount": report["criticalCount"],
            "warningCount": report["warningCount"],
            "contract": str(output_path),
            "report": str(report_path),
            "inputPath": str(input_path),
        }
        if excel_report:
            summary["excel"] = excel_report.get("excel")
            summary["importedYaml"] = excel_report.get("importedYaml")
            summary["excelRoundTripStatus"] = excel_report.get("status")
        summaries.append(summary)
        print(f"DATA CONTRACT {name}:")
        print(f"  input: {input_path}")
        print(f"  contract: {output_path}")
        print(f"  report: {report_path}")
        print(
            f"  verification: {report['status']} "
            f"(critical={report['criticalCount']}, warnings={report['warningCount']})"
        )
        if excel_report:
            print(f"  excel: {excel_report.get('excel')}")
            print(f"  imported yaml: {excel_report.get('importedYaml')}")
            print(f"  excel import/lint: {excel_report.get('status')}")

    failed = [item for item in summaries if item.get("status") == "fail"]
    yaml_failed = [item for item in failed if item.get("criticalCount", 0) > 0]
    excel_failed = [item for item in failed if item.get("excelRoundTripStatus") == "fail"]
    if (yaml_failed and fail_main) or (excel_failed and fail_main_on_excel_error):
        details = ", ".join(
            f"{item['name']} critical={item['criticalCount']} excel={item.get('excelRoundTripStatus', 'n/a')}"
            for item in yaml_failed + excel_failed
        )
        raise RuntimeError(f"Data contract verification failed: {details}")

    return summaries
