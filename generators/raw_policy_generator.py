from __future__ import annotations

import csv
import shutil
from pathlib import Path

from generators.raw_crm_generator import write_raw_crm_batch
from helper.csv_writer import write_csv
from helper.raw_metadata import RAW_PULL_TS


POLICY_SOURCE_FILES = [
    "party_master.csv",
    "contact_point.csv",
    "address_book.csv",
    "customer_portfolio.csv",
    "account_book.csv",
    "product_catalog.csv",
    "quote_register.csv",
    "policy_register.csv",
    "property_asset.csv",
    "vehicle_asset.csv",
]


POLICY_COVERAGE_COLUMNS = [
    "batch_ref",
    "pull_ts",
    "origin_sys",
    "policy_coverage_ref",
    "policy_ref",
    "coverage_ref",
    "product_ref",
    "coverage_status_txt",
    "coverage_start_dt",
    "coverage_end_dt",
    "sum_insured_amt",
    "gross_annual_premium_amt",
    "deductible_amt",
    "coverage_level_txt",
    "automatic_indexation_ind",
]


COVERAGE_CATALOG_COLUMNS = [
    "batch_ref",
    "pull_ts",
    "origin_sys",
    "coverage_ref",
    "product_ref",
    "coverage_type_txt",
    "coverage_desc",
    "coverage_start_dt",
    "coverage_end_dt",
    "limit_amt",
    "deductible_amt",
    "peril_txt",
    "coverage_status_txt",
]


BILLING_REGISTER_COLUMNS = [
    "batch_ref",
    "pull_ts",
    "origin_sys",
    "billing_ref",
    "policy_ref",
    "customer_ref",
    "account_ref",
    "billing_account_no",
    "billing_status_txt",
    "billing_frequency_txt",
    "payment_method_txt",
    "annual_premium_amt",
    "billed_premium_amt",
    "paid_premium_amt",
    "next_bill_dt",
    "last_payment_dt",
]


POLICY_EXTRA_FILES = {
    "coverage_catalog.csv": COVERAGE_CATALOG_COLUMNS,
    "policy_coverage.csv": POLICY_COVERAGE_COLUMNS,
    "billing_register.csv": BILLING_REGISTER_COLUMNS,
}


SAP_SOURCE_SCHEMAS = {
    "sap_policy.csv": [
        "batch_ref", "pull_ts", "origin_sys", "sap_policy_id", "crm_policy_ref",
        "sap_party_id", "sap_customer_id", "sap_product_id", "sap_quote_id",
        "policy_number", "policy_status", "policy_start_date", "policy_end_date",
        "policy_issue_date", "renewal_date", "policy_cycle", "cover_option",
        "gross_written_premium", "gross_earned_premium", "current_renewal_premium",
        "next_renewal_premium", "sales_channel", "payment_method",
        "auto_renew_flag", "fraud_flag",
    ],
    "sap_coverage.csv": [
        "batch_ref", "pull_ts", "origin_sys", "sap_coverage_id",
        "crm_coverage_ref", "sap_product_id", "coverage_type",
        "coverage_description", "coverage_start_date", "coverage_end_date",
        "limit_amount", "deductible_amount", "peril_type", "coverage_status",
    ],
    "sap_policy_coverage.csv": [
        "batch_ref", "pull_ts", "origin_sys", "sap_policy_coverage_id",
        "crm_policy_coverage_ref", "sap_policy_id", "sap_coverage_id",
        "sap_product_id", "coverage_status", "coverage_start_date",
        "coverage_end_date", "sum_insured_amount", "gross_annual_premium",
        "deductible_amount", "coverage_level", "automatic_indexation_flag",
    ],
}


def _read_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _read_header(path: Path) -> list[str]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return next(csv.reader(handle), [])


def _write_rows(folder: Path, name: str, rows: list[dict], fieldnames: list[str]) -> None:
    write_csv(str(folder), name, [{field: row.get(field, "") for field in fieldnames} for row in rows], fieldnames=fieldnames)


def _copy_selected_policy_files(source_dir: Path, target_dir: Path) -> None:
    for file_name in POLICY_SOURCE_FILES:
        source_file = source_dir / file_name
        if source_file.exists():
            shutil.copy2(source_file, target_dir / file_name)


def _date_part(value: str) -> str:
    return str(value or "").strip().split("T")[0].split(" ")[0]


def _money(value: str, default: str = "0.00") -> str:
    text = str(value or "").strip().replace(",", "")
    if not text:
        return default
    try:
        return f"{float(text):.2f}"
    except ValueError:
        return default


def _stable_index(value: str, modulo: int) -> int:
    return sum(ord(char) for char in str(value or "")) % modulo if modulo else 0


def _sap_value(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text if text.startswith("SAP_") else f"SAP_{text}"


def _first_value(*values: str) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _metadata(batch_id: str, origin_sys: str) -> dict:
    return {"batch_ref": batch_id, "pull_ts": RAW_PULL_TS, "origin_sys": origin_sys}


def _build_policy_coverage_files(source_dir: Path, target_dir: Path, batch_id: str, origin_sys: str) -> None:
    policies = _read_rows(source_dir / "policy_register.csv")
    products = {row.get("product_ref", ""): row for row in _read_rows(source_dir / "product_catalog.csv")}

    coverage_rows: list[dict] = []
    policy_coverage_rows: list[dict] = []
    seen_coverages: set[str] = set()
    for index, policy in enumerate(policies, start=1):
        policy_ref = policy.get("policy_ref", "")
        product_ref = policy.get("product_ref", "")
        product = products.get(product_ref, {})
        product_line = product.get("product_line", "") or product.get("product_type_txt", "") or policy.get("product_code", "")
        coverage_type = "Motor" if str(product_line).upper().startswith("M") else "Property" if str(product_line).upper().startswith("H") else "Core"
        coverage_ref = f"COV_{product_ref or policy_ref}"
        if origin_sys == "SAP":
            coverage_ref = _sap_value(coverage_ref)
        premium = _money(policy.get("renewal_premium_curr") or policy.get("gross_amt"))
        try:
            premium_float = float(premium)
        except ValueError:
            premium_float = 0.0
        limit_amount = f"{max(premium_float * 120, 50000):.2f}"
        deductible = f"{100 + (_stable_index(policy_ref, 10) * 50):.2f}"
        status = policy.get("policy_status_txt", "") or "Active"

        if coverage_ref not in seen_coverages:
            seen_coverages.add(coverage_ref)
            coverage_rows.append({
                **_metadata(batch_id, origin_sys),
                "coverage_ref": coverage_ref,
                "product_ref": product_ref,
                "coverage_type_txt": coverage_type,
                "coverage_desc": f"{coverage_type} base cover for {product_line or 'policy'}",
                "coverage_start_dt": _date_part(policy.get("policy_start_dt")),
                "coverage_end_dt": _date_part(policy.get("policy_end_dt")),
                "limit_amt": limit_amount,
                "deductible_amt": deductible,
                "peril_txt": "Collision" if coverage_type == "Motor" else "Fire" if coverage_type == "Property" else "General",
                "coverage_status_txt": status,
            })

        policy_coverage_rows.append({
            **_metadata(batch_id, origin_sys),
            "policy_coverage_ref": _sap_value(f"PCOV_{policy_ref}") if origin_sys == "SAP" else f"PCOV_{policy_ref}",
            "policy_ref": policy_ref,
            "coverage_ref": coverage_ref,
            "product_ref": product_ref,
            "coverage_status_txt": status,
            "coverage_start_dt": _date_part(policy.get("policy_start_dt")),
            "coverage_end_dt": _date_part(policy.get("policy_end_dt")),
            "sum_insured_amt": limit_amount,
            "gross_annual_premium_amt": premium,
            "deductible_amt": deductible,
            "coverage_level_txt": policy.get("cover_option_txt", "") or ("Comprehensive" if index % 3 else "Standard"),
            "automatic_indexation_ind": "Y" if index % 4 else "N",
        })

    _write_rows(target_dir, "coverage_catalog.csv", coverage_rows, COVERAGE_CATALOG_COLUMNS)
    _write_rows(target_dir, "policy_coverage.csv", policy_coverage_rows, POLICY_COVERAGE_COLUMNS)


def _build_billing_file(source_dir: Path, target_dir: Path, batch_id: str, origin_sys: str) -> None:
    policies = _read_rows(source_dir / "policy_register.csv")
    accounts = _read_rows(source_dir / "account_book.csv")
    accounts_by_party = {row.get("party_ref", ""): row for row in accounts}
    rows: list[dict] = []
    for index, policy in enumerate(policies, start=1):
        party_ref = policy.get("party_ref", "")
        account = accounts_by_party.get(party_ref, {})
        current = _money(policy.get("renewal_premium_curr") or policy.get("gross_amt"))
        next_bill_dt = _date_part(policy.get("renewal_dt") or policy.get("policy_end_dt"))
        rows.append({
            **_metadata(batch_id, origin_sys),
            "billing_ref": _sap_value(f"BILL_{policy.get('policy_ref', '')}") if origin_sys == "SAP" else f"BILL_{policy.get('policy_ref', '')}",
            "policy_ref": policy.get("policy_ref", ""),
            "customer_ref": policy.get("customer_ref", ""),
            "account_ref": account.get("account_ref", ""),
            "billing_account_no": account.get("account_no", "") or f"BA-{index:08d}",
            "billing_status_txt": "Active" if str(policy.get("policy_status_txt", "")).upper() == "ACTIVE" else "Closed",
            "billing_frequency_txt": "Monthly" if index % 3 else "Annual",
            "payment_method_txt": policy.get("payment_method", "") or ("Direct Debit" if index % 3 else "Card"),
            "annual_premium_amt": current,
            "billed_premium_amt": current,
            "paid_premium_amt": "0.00" if str(policy.get("policy_status_txt", "")).upper() in {"LAPSED", "CANCELLED"} else current,
            "next_bill_dt": next_bill_dt,
            "last_payment_dt": _date_part(policy.get("policy_start_dt")),
        })
    _write_rows(target_dir, "billing_register.csv", rows, BILLING_REGISTER_COLUMNS)


def _sap_policy_no(value: str, index: int) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    # Keep most policy numbers common for BV matching, with a controlled subset
    # different so match/merge fallback rules are testable.
    return text if index % 5 else f"SAP-{text}"


def _write_sap_view(prd1_dir: Path, prd2_dir: Path, batch_id: str) -> None:
    sap_policy_rows = []
    for index, row in enumerate(_read_rows(prd1_dir / "policy_register.csv"), start=1):
        sap_policy_rows.append({
            **_metadata(batch_id, "SAP"),
            "sap_policy_id": _sap_value(row.get("policy_ref", "")),
            "crm_policy_ref": row.get("policy_ref", ""),
            "sap_party_id": _sap_value(row.get("party_ref", "")),
            "sap_customer_id": _sap_value(row.get("customer_ref", "")),
            "sap_product_id": _sap_value(row.get("product_ref", "")),
            "sap_quote_id": _sap_value(row.get("quote_ref", "")),
            "policy_number": _sap_policy_no(row.get("policy_no", ""), index),
            "policy_status": row.get("policy_status_txt", ""),
            "policy_start_date": _date_part(row.get("policy_start_dt", "")),
            "policy_end_date": _date_part(row.get("policy_end_dt", "")),
            "policy_issue_date": _date_part(row.get("policy_start_dt", "")),
            "renewal_date": _date_part(row.get("renewal_dt", "")),
            "policy_cycle": row.get("policy_cycle_no", ""),
            "cover_option": row.get("cover_option_txt", ""),
            "gross_written_premium": _money(row.get("gross_amt")),
            "gross_earned_premium": _money(row.get("net_amt")),
            "current_renewal_premium": _money(row.get("renewal_premium_curr")),
            "next_renewal_premium": _money(row.get("renewal_premium_next")),
            "sales_channel": row.get("sales_channel_txt", ""),
            "payment_method": "DD" if row.get("payment_method") == "MONTHLY_DD" else row.get("payment_method", ""),
            "auto_renew_flag": row.get("is_auto_renew_enabled", ""),
            "fraud_flag": row.get("fraud_ind", ""),
        })

    sap_coverage_rows = []
    for row in _read_rows(prd1_dir / "coverage_catalog.csv"):
        sap_coverage_rows.append({
            **_metadata(batch_id, "SAP"),
            "sap_coverage_id": _sap_value(row.get("coverage_ref", "")),
            "crm_coverage_ref": row.get("coverage_ref", ""),
            "sap_product_id": _sap_value(row.get("product_ref", "")),
            "coverage_type": row.get("coverage_type_txt", ""),
            "coverage_description": row.get("coverage_desc", ""),
            "coverage_start_date": _date_part(row.get("coverage_start_dt", "")),
            "coverage_end_date": _date_part(row.get("coverage_end_dt", "")),
            "limit_amount": _money(row.get("limit_amt")),
            "deductible_amount": _money(row.get("deductible_amt")),
            "peril_type": row.get("peril_txt", ""),
            "coverage_status": row.get("coverage_status_txt", ""),
        })

    sap_policy_coverage_rows = []
    for row in _read_rows(prd1_dir / "policy_coverage.csv"):
        sap_policy_coverage_rows.append({
            **_metadata(batch_id, "SAP"),
            "sap_policy_coverage_id": _sap_value(row.get("policy_coverage_ref", "")),
            "crm_policy_coverage_ref": row.get("policy_coverage_ref", ""),
            "sap_policy_id": _sap_value(row.get("policy_ref", "")),
            "sap_coverage_id": _sap_value(row.get("coverage_ref", "")),
            "sap_product_id": _sap_value(row.get("product_ref", "")),
            "coverage_status": row.get("coverage_status_txt", ""),
            "coverage_start_date": _date_part(row.get("coverage_start_dt", "")),
            "coverage_end_date": _date_part(row.get("coverage_end_dt", "")),
            "sum_insured_amount": _money(row.get("sum_insured_amt")),
            "gross_annual_premium": _money(row.get("gross_annual_premium_amt")),
            "deductible_amount": _money(row.get("deductible_amt")),
            "coverage_level": row.get("coverage_level_txt", ""),
            "automatic_indexation_flag": row.get("automatic_indexation_ind", ""),
        })

    outputs = {
        "sap_policy.csv": sap_policy_rows,
        "sap_coverage.csv": sap_coverage_rows,
        "sap_policy_coverage.csv": sap_policy_coverage_rows,
    }
    for file_name, rows in outputs.items():
        _write_rows(prd2_dir, file_name, rows, SAP_SOURCE_SCHEMAS[file_name])


def _validate_required_files(folder: Path, expected: set[str]) -> None:
    found = {path.name for path in folder.glob("*.csv")}
    missing = sorted(expected - found)
    if missing:
        raise RuntimeError(f"Policy raw output missing files in {folder}: {missing}")


def write_policy_two_source_raw(raw_root: str, batch_id: str, ctx: dict) -> dict[str, str]:
    """Write raw policy PRD1/PRD2 feeds.

    PRD1 is CRM-shaped. PRD2 is SAP-shaped with source-specific file names
    and columns, while retaining CRM reference columns where useful for later
    Business Vault match/merge testing.
    """
    root = Path(raw_root) / "policy" / batch_id
    prd1_dir = root / "prd_01"
    prd2_dir = root / "prd_02"
    work_root = root / "_crm_work"

    for path in (prd1_dir, prd2_dir, work_root):
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)

    crm_work_dir = Path(write_raw_crm_batch(str(work_root), batch_id, ctx, source_dir_name="crm", source_system="CRM"))
    _copy_selected_policy_files(crm_work_dir, prd1_dir)
    _build_policy_coverage_files(prd1_dir, prd1_dir, batch_id, "CRM")
    _build_billing_file(prd1_dir, prd1_dir, batch_id, "CRM")

    _write_sap_view(prd1_dir, prd2_dir, batch_id)

    shutil.rmtree(work_root)
    _validate_required_files(prd1_dir, set(POLICY_SOURCE_FILES) | set(POLICY_EXTRA_FILES))
    _validate_required_files(prd2_dir, set(SAP_SOURCE_SCHEMAS))

    with (root / "_source_manifest.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["source", "folder", "description"])
        writer.writeheader()
        writer.writerows([
            {"source": "prd_01", "folder": str(prd1_dir), "description": "Policy CRM raw source-1 files"},
            {"source": "prd_02", "folder": str(prd2_dir), "description": "Policy SAP raw source-2 files with SAP-specific table names and columns"},
        ])

    return {"prd_01": str(prd1_dir), "prd_02": str(prd2_dir), "root": str(root)}
