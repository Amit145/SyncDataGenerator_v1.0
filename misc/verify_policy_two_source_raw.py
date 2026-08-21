from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path

import openpyxl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from generators.raw_policy_generator import (
    LATEST_POLICY_PRD1_SCHEMAS,
    LATEST_POLICY_PRD2_SCHEMAS,
)


WORKBOOK = Path("policy_prd_inputs/nPolicy_BV_Mapping_Rules.xlsx")


def _latest_run(root: Path) -> str:
    runs = sorted([path.name for path in root.iterdir() if path.is_dir()], reverse=True)
    if not runs:
        raise AssertionError(f"No policy raw runs found under {root}")
    return runs[0]


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def _header(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return next(csv.reader(handle))


def _parse_date(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt)
        except ValueError:
            continue
    raise AssertionError(f"Unsupported date value: {value}")


def _workbook_sap_tables(workbook: Path) -> set[str]:
    wb = openpyxl.load_workbook(workbook, read_only=True, data_only=True)
    ws = wb["Src1 & Src2 attributes"]
    tables: set[str] = set()
    current_table = ""
    for table, _attribute, source in ws.iter_rows(min_row=2, max_col=3, values_only=True):
        current_table = table or current_table
        if current_table and source and "SAP" in str(source).upper():
            tables.add(f"{str(current_table).strip().lower().replace(' ', '_')}.csv")
    return tables


def _assert_headers(folder: Path, schemas: dict[str, list[str]], label: str) -> None:
    found = {path.name for path in folder.glob("*.csv")}
    expected = set(schemas)
    missing = sorted(expected - found)
    extra = sorted(found - expected)
    if missing or extra:
        raise AssertionError(f"{label} file mismatch. missing={missing}, extra={extra}")
    for file_name, expected_header in schemas.items():
        actual = _header(folder / file_name)
        if actual != expected_header:
            raise AssertionError(
                f"{label}/{file_name} header mismatch.\n"
                f"expected={expected_header}\nactual={actual}"
            )


def _assert_origin(rows: list[dict[str, str]], expected_origin: str, label: str) -> None:
    bad = [row.get("origin_sys") for row in rows if row.get("origin_sys") != expected_origin]
    if bad:
        raise AssertionError(f"{label} has non-{expected_origin} origin_sys values: {bad[:5]}")


def _nonblank_set(rows: list[dict[str, str]], column: str) -> set[str]:
    return {row.get(column, "") for row in rows if row.get(column, "")}


def _assert_fk(
    rows: list[dict[str, str]],
    column: str,
    parent_values: set[str],
    label: str,
    required: bool = False,
) -> None:
    bad = []
    for row in rows:
        value = row.get(column, "")
        if not value and not required:
            continue
        if value not in parent_values:
            bad.append(value)
    if bad:
        raise AssertionError(f"{label}.{column} has unresolved references: {bad[:10]}")


def _assert_policy_dates(rows: list[dict[str, str]], label: str) -> None:
    for row in rows:
        policy_id = row.get("policy_identifier") or row.get("policy_number")
        start = _parse_date(row.get("policy_start_date", ""))
        end = _parse_date(row.get("policy_end_date", ""))
        renewal = _parse_date(row.get("renewal_date", ""))
        issue = _parse_date(row.get("policy_issue_date", ""))
        status = str(row.get("policy_status", "")).upper()
        cycle = int(float(row.get("policy_cycle") or 0))
        if start and end and start > end:
            raise AssertionError(f"{label} {policy_id}: policy_start_date after policy_end_date")
        if issue and start and issue > start:
            raise AssertionError(f"{label} {policy_id}: policy_issue_date after policy_start_date")
        if renewal and end:
            delta_days = (end - renewal).days
            if delta_days < 0 or delta_days > 10:
                raise AssertionError(f"{label} {policy_id}: renewal_date outside 0-10 day window")
        if status == "LAPSED" and cycle < 1:
            raise AssertionError(f"{label} {policy_id}: LAPSED policy has policy_cycle < 1")
        if status == "EXPIRED":
            raise AssertionError(f"{label} {policy_id}: EXPIRED status is not currently supported")


def _assert_date_order(rows: list[dict[str, str]], start_col: str, end_col: str, label: str) -> None:
    for row in rows:
        start = _parse_date(row.get(start_col, ""))
        end = _parse_date(row.get(end_col, ""))
        if start and end and start > end:
            raise AssertionError(f"{label}: {start_col} after {end_col} for row {row}")


def _assert_match_rules(prd1: dict[str, list[dict[str, str]]], prd2: dict[str, list[dict[str, str]]]) -> None:
    crm_policies = prd1["policy.csv"]
    sap_policies = prd2["policy.csv"]
    crm_policy_numbers = _nonblank_set(crm_policies, "policy_number")
    crm_policy_fallback = {
        (
            row.get("person_identifier", ""),
            row.get("product_identifier", ""),
            row.get("policy_start_date", ""),
            row.get("policy_end_date", ""),
        )
        for row in crm_policies
    }
    for row in sap_policies:
        if row.get("policy_number") in crm_policy_numbers:
            continue
        fallback = (
            row.get("person_identifier", ""),
            row.get("product_identifier", ""),
            row.get("policy_start_date", ""),
            row.get("policy_end_date", ""),
        )
        if fallback not in crm_policy_fallback:
            raise AssertionError(f"PRD2 policy does not satisfy workbook match rules: {row}")

    crm_coverages = prd1["coverage.csv"]
    sap_coverages = prd2["coverage.csv"]
    crm_coverage_ids = _nonblank_set(crm_coverages, "coverage_identifier")
    crm_coverage_fallback = {
        (row.get("coverage_code", ""), row.get("coverage_type", ""), row.get("coverage_name", ""))
        for row in crm_coverages
    }
    for row in sap_coverages:
        if row.get("coverage_identifier") in crm_coverage_ids:
            continue
        fallback = (row.get("coverage_code", ""), row.get("coverage_type", ""), row.get("coverage_name", ""))
        if fallback not in crm_coverage_fallback:
            raise AssertionError(f"PRD2 coverage does not satisfy workbook match rules: {row}")

    crm_policy_coverages = prd1["policy_coverage.csv"]
    sap_policy_coverages = prd2["policy_coverage.csv"]
    crm_policy_coverage_ids = _nonblank_set(crm_policy_coverages, "policy_coverage_identifier")
    crm_policy_coverage_fallback = {
        (row.get("policy_identifier", ""), row.get("coverage_identifier", ""))
        for row in crm_policy_coverages
    }
    for row in sap_policy_coverages:
        if row.get("policy_coverage_identifier") in crm_policy_coverage_ids:
            continue
        fallback = (row.get("policy_identifier", ""), row.get("coverage_identifier", ""))
        if fallback not in crm_policy_coverage_fallback:
            raise AssertionError(f"PRD2 policy coverage does not satisfy workbook match rules: {row}")


def verify(run_id: str | None, raw_root: Path, workbook: Path) -> None:
    run_id = run_id or _latest_run(raw_root)
    root = raw_root / run_id
    prd1_dir = root / "prd_01"
    prd2_dir = root / "prd_02"
    if not prd1_dir.exists() or not prd2_dir.exists():
        raise AssertionError(f"Policy raw run must contain prd_01 and prd_02: {root}")

    sap_tables = _workbook_sap_tables(workbook)
    expected_prd2 = set(LATEST_POLICY_PRD2_SCHEMAS)
    if sap_tables != expected_prd2:
        raise AssertionError(f"Workbook SAP table scope mismatch. workbook={sap_tables}, generator={expected_prd2}")

    _assert_headers(prd1_dir, LATEST_POLICY_PRD1_SCHEMAS, "prd_01")
    _assert_headers(prd2_dir, LATEST_POLICY_PRD2_SCHEMAS, "prd_02")

    prd1 = {name: _read_csv(prd1_dir / name) for name in LATEST_POLICY_PRD1_SCHEMAS}
    prd2 = {name: _read_csv(prd2_dir / name) for name in LATEST_POLICY_PRD2_SCHEMAS}

    for file_name, rows in prd1.items():
        _assert_origin(rows, "CRM", f"prd_01/{file_name}")
    for file_name, rows in prd2.items():
        _assert_origin(rows, "SAP", f"prd_02/{file_name}")

    prd1_person_ids = _nonblank_set(prd1["person.csv"], "person_identifier")
    prd1_address_ids = _nonblank_set(prd1["address.csv"], "address_identifier")
    prd1_policy_ids = _nonblank_set(prd1["policy.csv"], "policy_identifier")
    prd1_product_ids = _nonblank_set(prd1["product.csv"], "product_identifier")
    prd1_quote_ids = _nonblank_set(prd1["quote.csv"], "quote_identifier")
    prd1_coverage_ids = _nonblank_set(prd1["coverage.csv"], "coverage_identifier")
    prd1_insured_object_ids = _nonblank_set(prd1["insured_object.csv"], "insured_object_identifier")
    prd1_sales_channel_ids = _nonblank_set(prd1["sales_channel.csv"], "sales_channel_identifier")

    _assert_fk(prd1["person.csv"], "address_identifier", prd1_address_ids, "prd_01/person")
    _assert_fk(prd1["natural_person.csv"], "person_identifier", prd1_person_ids, "prd_01/natural_person", True)
    _assert_fk(prd1["legal_entity.csv"], "person_identifier", prd1_person_ids, "prd_01/legal_entity", True)
    _assert_fk(prd1["policy.csv"], "person_identifier", prd1_person_ids, "prd_01/policy", True)
    _assert_fk(prd1["policy.csv"], "product_identifier", prd1_product_ids, "prd_01/policy", True)
    _assert_fk(prd1["policy.csv"], "quote_identifier", prd1_quote_ids, "prd_01/policy")
    _assert_fk(prd1["policy.csv"], "sales_channel_identifier", prd1_sales_channel_ids, "prd_01/policy", True)
    _assert_fk(prd1["quote.csv"], "person_identifier", prd1_person_ids, "prd_01/quote", True)
    _assert_fk(prd1["quote.csv"], "product_identifier", prd1_product_ids, "prd_01/quote", True)
    _assert_fk(prd1["quote.csv"], "sales_channel_identifier", prd1_sales_channel_ids, "prd_01/quote")
    _assert_fk(prd1["policy_coverage.csv"], "policy_identifier", prd1_policy_ids, "prd_01/policy_coverage", True)
    _assert_fk(prd1["policy_coverage.csv"], "coverage_identifier", prd1_coverage_ids, "prd_01/policy_coverage", True)
    _assert_fk(prd1["insured_object.csv"], "policy_identifier", prd1_policy_ids, "prd_01/insured_object", True)
    _assert_fk(prd1["insured_object.csv"], "address_identifier", prd1_address_ids, "prd_01/insured_object", True)
    _assert_fk(prd1["home.csv"], "insured_object_identifier", prd1_insured_object_ids, "prd_01/home", True)
    _assert_fk(prd1["motor.csv"], "insured_object_identifier", prd1_insured_object_ids, "prd_01/motor", True)
    _assert_fk(prd1["risk_assessment.csv"], "insured_object_identifier", prd1_insured_object_ids, "prd_01/risk_assessment", True)
    _assert_fk(prd1["loss_event.csv"], "insured_object_identifier", prd1_insured_object_ids, "prd_01/loss_event")
    _assert_fk(prd1["loss_event.csv"], "address_identifier", prd1_address_ids, "prd_01/loss_event", True)
    _assert_fk(prd1["claim.csv"], "policy_identifier", prd1_policy_ids, "prd_01/claim", True)

    for label, rows in (("prd_01/policy", prd1["policy.csv"]), ("prd_02/policy", prd2["policy.csv"])):
        _assert_policy_dates(rows, label)
    for label, rows, start_col, end_col in (
        ("prd_01/coverage", prd1["coverage.csv"], "coverage_effective_date", "coverage_discontinue_date"),
        ("prd_02/coverage", prd2["coverage.csv"], "coverage_effective_date", "coverage_discontinue_date"),
        ("prd_01/policy_coverage", prd1["policy_coverage.csv"], "coverage_start_date", "coverage_end_date"),
        ("prd_02/policy_coverage", prd2["policy_coverage.csv"], "coverage_start_date", "coverage_end_date"),
        ("prd_01/insured_object", prd1["insured_object.csv"], "insured_object_start_date", "insured_object_end_date"),
        ("prd_01/loss_event", prd1["loss_event.csv"], "loss_event_start_date", "loss_event_end_date"),
    ):
        _assert_date_order(rows, start_col, end_col, label)

    policy_by_id = {row["policy_identifier"]: row for row in prd1["policy.csv"]}
    for row in prd1["insured_object.csv"]:
        policy = policy_by_id[row["policy_identifier"]]
        if row["insured_object_start_date"] != policy["policy_start_date"]:
            raise AssertionError(f"insured_object start date does not align to policy: {row}")
        if row["insured_object_end_date"] != policy["policy_end_date"]:
            raise AssertionError(f"insured_object end date does not align to policy: {row}")
        if row["insured_object_current_status"] != policy["policy_status"]:
            raise AssertionError(f"insured_object status does not align to policy: {row}")

    _assert_match_rules(prd1, prd2)
    print(f"Policy two-source raw valid: {root}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify policy PRD1/PRD2 raw source rules.")
    parser.add_argument("--run-id", help="Policy raw run id. Defaults to latest.")
    parser.add_argument("--raw-root", default="data/raw/policy", help="Policy raw root folder.")
    parser.add_argument("--workbook", default=str(WORKBOOK), help="Policy source/matching workbook.")
    args = parser.parse_args()
    verify(args.run_id, Path(args.raw_root), Path(args.workbook))


if __name__ == "__main__":
    main()
