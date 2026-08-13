from __future__ import annotations

import csv
import shutil
from datetime import datetime, timedelta
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


CLAIM_REGISTER_COLUMNS = [
    "batch_ref",
    "pull_ts",
    "origin_sys",
    "claim_identifier",
    "claim_status",
    "claim_number",
    "claim_type",
    "claim_reason",
    "claim_reported_date",
    "claim_settlement_date",
    "claim_channel",
    "claim_handler",
    "claim_product",
    "claim_process_method",
    "claim_state",
    "claim_band",
    "claim_band_sort",
    "claims_rejection_reason",
    "claim_sensitivity",
    "coverage_verification_result",
    "applicable_deductible_flag",
    "indemnity_logic",
    "claim_specific_flag",
    "cross_border_claim_indicator",
    "bodily_injury_indicator",
    "hospitalization_flag",
    "total_loss_flag",
    "claims_history_lob",
    "claim_amt",
    "claims_paid",
    "is_claim_suspicious",
    "is_claim_fraud",
    "claim_open_date",
    "claim_approval_date",
    "claim_status_date",
    "claim_close_date",
    "claim_duration",
    "total_incurred",
    "no_claims_discount",
    "policy_identifier",
    "loss_event_identifier",
]


LOSS_EVENT_REGISTER_COLUMNS = [
    "batch_ref",
    "pull_ts",
    "origin_sys",
    "loss_event_name",
    "loss_event_description",
    "loss_date",
    "loss_time",
    "loss_event_start_date",
    "loss_event_end_date",
    "loss_event_status",
    "loss_type",
    "loss_category",
    "loss_cause",
    "property_liability_loss_cause",
    "effective_loss_date",
    "notification_date",
    "notification_channel",
    "casualty_indicator",
    "minimal_impact_flag",
    "number_of_injured_parties",
    "number_of_people_involved",
    "number_of_vehicles_involved",
    "drugs_alcohol_indicator",
    "contributory_negligence_flag",
    "loss_event_identifier",
    "days_to_fnol",
    "catastrophe_flag",
    "natcat_flag",
    "insured_object_identifier",
    "loss_event_number",
    "loss_event_source_system",
    "loss_event_created_date",
    "loss_event_update_date",
    "loss_event_severity",
    "loss_event_address",
    "catastrophe_code",
    "catastrophe_name",
    "geography_identifier",
]


EVENT_TYPE_CATALOG_COLUMNS = [
    "batch_ref",
    "pull_ts",
    "origin_sys",
    "event_type_identifier",
    "event_type_code",
    "event_type_name",
    "event_type_description",
]


PERSON_ROLE_COLUMNS = [
    "batch_ref",
    "pull_ts",
    "origin_sys",
    "person_role_identifier",
    "person_identifier",
    "role_identifier",
]


POLICY_EVENT_COLUMNS = [
    "batch_ref",
    "pull_ts",
    "origin_sys",
    "policy_identifier",
    "policy_event_identifier",
    "policy_event_number",
    "policy_event_date",
    "policy_event_effective_date",
    "policy_event_type",
    "policy_event_status",
    "premium_impact_amount",
    "event_type_identifier",
]


POLICY_PARTICIPANT_COLUMNS = [
    "batch_ref",
    "pull_ts",
    "origin_sys",
    "policy_participant_identifier",
    "participant_effective_date",
    "participant_end_date",
    "participant_status",
    "primary_indicator",
    "ownership_percentage",
    "benefit_percentage",
    "policy_identifier",
    "person_role_identifier",
]


RISK_ASSESSMENT_COLUMNS = [
    "batch_ref",
    "pull_ts",
    "origin_sys",
    "risk_assessment_identifier",
    "risk_effective_date",
    "risk_assessment_date",
    "risk_assessment_method",
    "risk_assessment_outcome",
    "insured_object_identifier",
    "risk_number",
    "risk_type",
    "risk_status",
    "risk_end_date",
    "risk_classification",
    "risk_rating",
    "risk_score",
    "insured_value",
    "sum_insured",
    "exposure_amount",
    "claim_amount",
    "total_incurred_amount",
    "risk_status_date",
    "policy_identifier",
]


ROLE_CATALOG_COLUMNS = [
    "batch_ref",
    "pull_ts",
    "origin_sys",
    "role_identifier",
    "role_code",
    "role_name",
    "role_description",
    "active_indicator",
]


SALES_CHANNEL_COLUMNS = [
    "batch_ref",
    "pull_ts",
    "origin_sys",
    "channel_name",
    "channel_type",
    "distribution_channel",
    "channel_status",
    "sales_channel_identifier",
]


POLICY_EXCEL_DERIVED_FILES = {
    "claim_register.csv": CLAIM_REGISTER_COLUMNS,
    "loss_event_register.csv": LOSS_EVENT_REGISTER_COLUMNS,
    "event_type_catalog.csv": EVENT_TYPE_CATALOG_COLUMNS,
    "person_role.csv": PERSON_ROLE_COLUMNS,
    "policy_event.csv": POLICY_EVENT_COLUMNS,
    "policy_participant.csv": POLICY_PARTICIPANT_COLUMNS,
    "risk_assessment.csv": RISK_ASSESSMENT_COLUMNS,
    "role_catalog.csv": ROLE_CATALOG_COLUMNS,
    "sales_channel.csv": SALES_CHANNEL_COLUMNS,
}


RAW_META_COLUMNS = ["batch_ref", "pull_ts", "origin_sys"]


LATEST_POLICY_PRD1_SCHEMAS = {
    "billing.csv": RAW_META_COLUMNS + [
        "billing_identifier", "billing_account_number", "billing_status",
        "billing_frequency", "payment_method", "annual_premium_amount",
        "billed_premium_amount", "net_premium_amount", "outstanding_amount",
        "last_payment_date", "next_payment_due_date", "last_invoice_date",
        "next_invoice_date", "payment_default_indicator", "policy_identifier",
        "gross_written_premium", "gross_earned_premium", "missed_payment_count",
        "policy_renewal_current_period_amount",
        "policy_renewal_next_period_amount", "commission_paid",
        "is_installment_default", "policy_gross_revenue_amount",
        "policy_net_revenue_amount",
    ],
    "claim.csv": RAW_META_COLUMNS + [
        "claim_identifier", "claim_status", "claim_number", "claim_type",
        "claim_reason", "claim_reported_date", "claim_settlement_date",
        "claim_process_method", "claim_state", "claim_sensitivity",
        "coverage_verification_result", "applicable_deductible_flag",
        "total_loss_flag", "claim_amt", "claims_paid", "claim_open_date",
        "claim_approval_date", "claim_close_date", "claim_duration",
        "total_incurred", "policy_identifier", "loss_event_identifier",
        "claim_channel", "cross_border_claim_indicator",
        "bodily_injury_indicator", "hospitalization_flag",
    ],
    "coverage.csv": RAW_META_COLUMNS + [
        "coverage_identifier", "coverage_code", "coverage_category",
        "coverage_effective_date", "coverage_discontinue_date",
        "coverage_type", "maximum_deductible", "coverage_name",
        "default_limit", "waiting_period",
    ],
    "event_type.csv": RAW_META_COLUMNS + [
        "event_type_identifier", "event_type_code", "event_type_name",
        "event_type_description",
    ],
    "geography.csv": RAW_META_COLUMNS + [
        "applicable_jurisdiction", "census_zone", "commune_code",
        "geocoding_level", "latitude", "longitude", "municipal_district",
        "natcat_hazard_zone_scheme", "sub_region", "surface_elevation",
        "city", "country", "state", "geography_identifier",
    ],
    "home.csv": RAW_META_COLUMNS + [
        "is_existing_home_customer", "home_type",
        "roof_construction_material_type", "wall_construction_material_type",
        "home_risk_address", "value_of_content", "value_of_goods_carried",
        "replacement_value", "actual_pre_event_entity_value", "home_state",
        "insured_object_identifier", "insured_object_type",
        "insured_object_subtype", "insured_object_description",
        "insured_value", "insured_object_start_date", "insured_object_end_date",
        "insured_object_current_status", "geography_identifier",
        "policy_identifier",
    ],
    "loss_event.csv": RAW_META_COLUMNS + [
        "loss_event_name", "loss_event_description", "loss_date", "loss_time",
        "loss_event_start_date", "loss_event_end_date", "loss_event_status",
        "loss_type", "loss_category", "loss_cause", "effective_loss_date",
        "casualty_indicator", "minimal_impact_flag",
        "number_of_injured_parties", "number_of_people_involved",
        "number_of_vehicles_involved", "drugs_alcohol_indicator",
        "contributory_negligence_flag", "loss_event_identifier",
        "catastrophe_flag", "natcat_flag", "record_version",
        "insured_object_identifier", "loss_event_number",
        "loss_event_source_system", "loss_event_created_date",
        "loss_event_update_date", "loss_event_severity", "loss_event_address",
        "catastrophe_code", "catastrophe_name", "geography_identifier",
    ],
    "motor.csv": RAW_META_COLUMNS + [
        "vehicle_risk_class_code", "vehicle_body_type", "vehicle_fuel_type",
        "vehicle_variant", "vehicle_age", "vehicle_class", "vehicle_model",
        "vehicle_owner_type", "vehicle_reg_state", "vehicle_type",
        "vehicle_manufacturing_year", "vehicle_risk_address",
        "insured_object_identifier", "insured_object_type",
        "insured_object_subtype", "insured_object_description",
        "insured_value", "insured_object_start_date", "insured_object_end_date",
        "insured_object_current_status", "geography_identifier",
        "policy_identifier",
    ],
    "person.csv": RAW_META_COLUMNS + [
        "person_identifier", "assessed_disability_degree",
        "preferred_language", "tenant_identifier", "source_identifier",
        "source_type", "is_opt_in_validated",
        "is_opt_in_legitimate_interest", "is_lead",
        "is_operational_paperless_consent", "record_version", "tenant_id",
        "person_type", "courtesy_title", "last_name", "full_name",
        "birth_date", "gender", "nationality", "marital_status", "occupation",
        "street_address", "postcode", "home_phone_number", "work_phone_number",
        "personal_email", "work_email", "job_title", "role", "first_name",
        "company_name", "date_of_constitution", "geography_identifier",
    ],
    "policy.csv": RAW_META_COLUMNS + [
        "policy_cover_option", "is_fraud", "policy_cycle", "policy_end_date",
        "policy_number", "policy_start_date", "policy_status",
        "renewal_date", "product_identifier", "record_version",
        "policy_issue_date", "policy_sum_insured", "policy_tenure",
        "policy_type", "is_policy_renewal", "policy_cancellation_date",
        "policy_cancellation_reason", "policy_retention_limit",
        "policy_base_premium", "sales_channel_identifier",
        "quote_identifier", "policy_status_date", "limit_for_indemnification",
        "policy_expiry_date", "is_auto_renew_enabled",
        "policy_renewal_satisfaction_score", "policy_renewal_feedback",
        "number_of_insured_persons", "loyalty_discount_usage",
        "policy_identifier", "discount", "is_renewal_escalation",
    ],
    "policy_coverage.csv": RAW_META_COLUMNS + [
        "coverage_start_date", "coverage_amount", "deductible_amount",
        "coverage_identifier", "coverage_status",
        "policy_coverage_identifier", "coverage_status_code",
        "coverage_limit_amount", "gross_premium", "coverage_end_date",
        "policy_identifier",
    ],
    "policy_event.csv": RAW_META_COLUMNS + [
        "policy_identifier", "policy_event_identifier", "policy_event_number",
        "event_date", "policy_event_effective_date", "event_status",
        "premium_impact_amount", "event_type_identifier", "event_reason",
        "event_outcome",
    ],
    "policy_participant.csv": RAW_META_COLUMNS + [
        "participant_effective_date", "participant_status",
        "participant_end_date", "benefit_percentage", "primary_indicator",
        "policy_identifier", "policy_participant_identifier",
        "person_identifier", "role_identifier",
    ],
    "product.csv": RAW_META_COLUMNS + [
        "product_identifier", "product_type", "record_version", "product_name",
        "line_of_business_code", "product_status", "product_variant",
        "product_launch_date", "product_description",
    ],
    "quote.csv": RAW_META_COLUMNS + [
        "quote_identifier", "product_identifier", "person_identifier",
        "quote_number", "quote_status", "quoted_gross_premium_amount",
        "quoted_net_premium_amount", "quote_premium", "quote_date",
        "rejection_reason", "underwriting_approval_type",
        "renewal_current_period_amount", "renewal_previous_period_amount",
        "sales_channel_identifier", "quote_version_number",
    ],
    "risk_assessment.csv": RAW_META_COLUMNS + [
        "risk_assessment_identifier", "risk_effective_date",
        "risk_assessment_date", "risk_assessment_method",
        "risk_assessment_outcome", "insured_object_identifier", "risk_number",
        "risk_type", "risk_status", "risk_end_date", "risk_classification",
        "risk_rating", "risk_score", "exposure_amount", "claim_amount",
        "total_incurred_amount", "risk_status_date",
    ],
    "role.csv": RAW_META_COLUMNS + [
        "role_name", "role_description", "role_identifier", "role_code",
    ],
    "sales_channel.csv": RAW_META_COLUMNS + [
        "channel_name", "channel_type", "distribution_channel",
        "channel_status", "sales_channel_identifier",
    ],
}


LATEST_POLICY_PRD2_SCHEMAS = {
    key: LATEST_POLICY_PRD1_SCHEMAS[key]
    for key in ("policy.csv", "coverage.csv", "policy_coverage.csv")
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


def _is_sk_column(column_name: str) -> bool:
    normalized = " ".join(str(column_name or "").strip().lower().split())
    return normalized == "sk" or normalized.endswith(" sk")


def _without_sk_columns(columns: list[str]) -> list[str]:
    return [column for column in columns if not _is_sk_column(column)]


def _write_rows(folder: Path, name: str, rows: list[dict], fieldnames: list[str]) -> None:
    clean_fieldnames = _without_sk_columns(fieldnames)
    write_csv(
        str(folder),
        name,
        [{field: row.get(field, "") for field in clean_fieldnames} for row in rows],
        fieldnames=clean_fieldnames,
    )


def _copy_selected_policy_files(source_dir: Path, target_dir: Path) -> None:
    for file_name in POLICY_SOURCE_FILES:
        source_file = source_dir / file_name
        if not source_file.exists():
            continue
        rows = _read_rows(source_file)
        header = _without_sk_columns(_read_header(source_file))
        _write_rows(target_dir, file_name, rows, header)


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


def _int_text(value: str, default: int = 0) -> str:
    try:
        return str(int(float(str(value or "").strip().replace(",", ""))))
    except ValueError:
        return str(default)


def _date_plus(value: str, days: int) -> str:
    text = _date_part(value)
    try:
        return (datetime.fromisoformat(text) + timedelta(days=days)).date().isoformat()
    except ValueError:
        return text


def _stable_index(value: str, modulo: int) -> int:
    return sum(ord(char) for char in str(value or "")) % modulo if modulo else 0


def _sap_value(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text if text.startswith("SAP_") else f"SAP_{text}"


def _remove_sap_prefix(value: str) -> str:
    text = str(value or "").strip()
    return text[4:] if text.startswith("SAP_") else text


def _first_value(*values: str) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _metadata(batch_id: str, origin_sys: str) -> dict:
    return {"batch_ref": batch_id, "pull_ts": RAW_PULL_TS, "origin_sys": origin_sys}


def _first_existing_row(rows: list[dict], key: str, value: str) -> dict:
    for row in rows:
        if row.get(key) == value:
            return row
    return {}


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


def _build_excel_policy_support_files(target_dir: Path, batch_id: str) -> None:
    policies = _read_rows(target_dir / "policy_register.csv")
    parties = _read_rows(target_dir / "party_master.csv")
    products = {row.get("product_ref", ""): row for row in _read_rows(target_dir / "product_catalog.csv")}
    properties = _read_rows(target_dir / "property_asset.csv")
    vehicles = _read_rows(target_dir / "vehicle_asset.csv")

    def asset_for(policy: dict) -> dict:
        policy_ref = policy.get("policy_ref", "")
        return (
            _first_existing_row(properties, "policy_ref", policy_ref)
            or _first_existing_row(vehicles, "policy_ref", policy_ref)
        )

    roles = [
        {
            **_metadata(batch_id, "CRM"),
            "role_identifier": "ROLE_POLICY_HOLDER",
            "role_code": "POLICY_HOLDER",
            "role_name": "Policy Holder",
            "role_description": "Primary policy holder role",
            "active_indicator": "Y",
        },
        {
            **_metadata(batch_id, "CRM"),
            "role_identifier": "ROLE_INSURED_PARTY",
            "role_code": "INSURED_PARTY",
            "role_name": "Insured Party",
            "role_description": "Covered party role",
            "active_indicator": "Y",
        },
        {
            **_metadata(batch_id, "CRM"),
            "role_identifier": "ROLE_BENEFICIARY",
            "role_code": "BENEFICIARY",
            "role_name": "Beneficiary",
            "role_description": "Policy beneficiary role",
            "active_indicator": "Y",
        },
    ]
    _write_rows(target_dir, "role_catalog.csv", roles, ROLE_CATALOG_COLUMNS)

    person_role_rows = []
    for party in parties:
        party_ref = party.get("party_ref", "")
        if not party_ref:
            continue
        person_role_rows.append({
            **_metadata(batch_id, "CRM"),
            "person_role_identifier": f"PROLE_{party_ref}",
            "person_identifier": party.get("person_identifier") or party_ref,
            "role_identifier": "ROLE_POLICY_HOLDER",
        })
    _write_rows(target_dir, "person_role.csv", person_role_rows, PERSON_ROLE_COLUMNS)

    channels = {}
    for index, policy in enumerate(policies, start=1):
        name = policy.get("sales_channel_txt", "") or "Direct"
        identifier = f"CHAN_{name.upper().replace(' ', '_')}"
        channels[identifier] = {
            **_metadata(batch_id, "CRM"),
            "channel_name": name,
            "channel_type": "Digital" if name.upper() in {"WEB", "ONLINE", "DIRECT"} else "Assisted",
            "distribution_channel": name,
            "channel_status": "Active",
            "sales_channel_identifier": identifier or f"CHAN_{index:03d}",
        }
    _write_rows(target_dir, "sales_channel.csv", list(channels.values()), SALES_CHANNEL_COLUMNS)

    event_types = [
        ("EVT_ISSUE", "ISSUE", "Policy Issue", "Policy created or issued"),
        ("EVT_RENEWAL", "RENEWAL", "Policy Renewal", "Policy renewed"),
        ("EVT_CANCEL", "CANCEL", "Policy Cancellation", "Policy cancelled"),
        ("EVT_ENDORSE", "ENDORSE", "Policy Endorsement", "Policy amended"),
    ]
    event_type_rows = [
        {
            **_metadata(batch_id, "CRM"),
            "event_type_identifier": identifier,
            "event_type_code": code,
            "event_type_name": name,
            "event_type_description": description,
        }
        for identifier, code, name, description in event_types
    ]
    _write_rows(target_dir, "event_type_catalog.csv", event_type_rows, EVENT_TYPE_CATALOG_COLUMNS)

    policy_event_rows = []
    policy_participant_rows = []
    risk_rows = []
    loss_rows = []
    claim_rows = []
    for index, policy in enumerate(policies, start=1):
        policy_ref = policy.get("policy_ref", "")
        party_ref = policy.get("party_ref", "")
        status = policy.get("policy_status_txt", "") or "Active"
        start_date = _date_part(policy.get("policy_start_dt"))
        end_date = _date_part(policy.get("policy_end_dt"))
        product = products.get(policy.get("product_ref", ""), {})
        product_line = product.get("product_line", "") or product.get("product_type_txt", "") or "Policy"
        asset = asset_for(policy)
        asset_ref = asset.get("property_ref") or asset.get("vehicle_ref") or policy_ref
        premium = _money(policy.get("renewal_premium_curr") or policy.get("gross_amt"))
        premium_float = float(premium)
        risk_score = 10 + _stable_index(policy_ref, 90)
        risk_rating = "LOW" if risk_score < 35 else "MEDIUM" if risk_score < 70 else "HIGH"
        event_type_identifier = "EVT_CANCEL" if status.upper() in {"CANCELLED", "LAPSED"} else (
            "EVT_RENEWAL" if _int_text(policy.get("policy_cycle_no"), 1) != "1" else "EVT_ISSUE"
        )
        event_code = event_type_identifier.replace("EVT_", "")
        loss_event_id = f"LOSS_{policy_ref}"
        claim_id = f"CLM_{policy_ref}"
        loss_date = _date_plus(start_date, 30 + _stable_index(policy_ref, 180))
        claim_open_date = _date_plus(loss_date, 2)
        claim_close_date = _date_plus(claim_open_date, 12 + _stable_index(policy_ref, 35))
        has_paid_claim = index % 4 == 0 or status.upper() in {"CANCELLED", "LAPSED"}
        claim_amount = f"{premium_float * (2.0 + (_stable_index(policy_ref, 8) / 2)):.2f}" if has_paid_claim else "0.00"
        paid_amount = f"{float(claim_amount) * 0.85:.2f}" if has_paid_claim else "0.00"

        policy_event_rows.append({
            **_metadata(batch_id, "CRM"),
            "policy_identifier": policy_ref,
            "policy_event_identifier": f"PEVT_{policy_ref}",
            "policy_event_number": f"PE-{index:08d}",
            "policy_event_date": start_date,
            "policy_event_effective_date": start_date,
            "policy_event_type": event_code,
            "policy_event_status": "Completed",
            "premium_impact_amount": "0.00" if event_code == "ISSUE" else f"{premium_float * 0.04:.2f}",
            "event_type_identifier": event_type_identifier,
        })

        policy_participant_rows.append({
            **_metadata(batch_id, "CRM"),
            "policy_participant_identifier": f"PPART_{policy_ref}_{party_ref}",
            "participant_effective_date": start_date,
            "participant_end_date": end_date,
            "participant_status": status,
            "primary_indicator": "Y",
            "ownership_percentage": "100.00",
            "benefit_percentage": "100.00",
            "policy_identifier": policy_ref,
            "person_role_identifier": f"PROLE_{party_ref}",
        })

        risk_rows.append({
            **_metadata(batch_id, "CRM"),
            "risk_assessment_identifier": f"RISK_{policy_ref}",
            "risk_effective_date": start_date,
            "risk_assessment_date": _date_plus(start_date, -3),
            "risk_assessment_method": "Automated",
            "risk_assessment_outcome": "Refer" if risk_rating == "HIGH" else "Accept",
            "insured_object_identifier": asset_ref,
            "risk_number": f"RISK-{index:08d}",
            "risk_type": product_line,
            "risk_status": "Active" if status.upper() == "ACTIVE" else "Closed",
            "risk_end_date": end_date,
            "risk_classification": product_line,
            "risk_rating": risk_rating,
            "risk_score": str(risk_score),
            "insured_value": _money(asset.get("insured_value_amt") or premium_float * 120),
            "sum_insured": _money(asset.get("insured_value_amt") or premium_float * 120),
            "exposure_amount": _money(premium_float * 90),
            "claim_amount": claim_amount,
            "total_incurred_amount": claim_amount,
            "risk_status_date": start_date,
            "policy_identifier": policy_ref,
        })

        loss_rows.append({
            **_metadata(batch_id, "CRM"),
            "loss_event_name": f"{product_line} loss event",
            "loss_event_description": f"Generated {product_line} loss event for policy {policy_ref}",
            "loss_date": loss_date,
            "loss_time": f"{8 + _stable_index(policy_ref, 9):02d}:30:00",
            "loss_event_start_date": loss_date,
            "loss_event_end_date": _date_plus(loss_date, 1),
            "loss_event_status": "Closed" if has_paid_claim else "Recorded",
            "loss_type": product_line,
            "loss_category": "Motor" if product_line.upper().startswith("M") else "Property",
            "loss_cause": "Collision" if product_line.upper().startswith("M") else "Accidental Damage",
            "property_liability_loss_cause": "Third Party" if index % 5 == 0 else "Own Damage",
            "effective_loss_date": loss_date,
            "notification_date": _date_plus(loss_date, 1),
            "notification_channel": "Online" if index % 2 else "Phone",
            "casualty_indicator": "Y" if index % 11 == 0 else "N",
            "minimal_impact_flag": "N" if has_paid_claim else "Y",
            "number_of_injured_parties": "1" if index % 11 == 0 else "0",
            "number_of_people_involved": str(1 + _stable_index(policy_ref, 3)),
            "number_of_vehicles_involved": "2" if product_line.upper().startswith("M") and index % 3 == 0 else "1",
            "drugs_alcohol_indicator": "Y" if index % 37 == 0 else "N",
            "contributory_negligence_flag": "Y" if index % 7 == 0 else "N",
            "loss_event_identifier": loss_event_id,
            "days_to_fnol": "1",
            "catastrophe_flag": "Y" if index % 29 == 0 else "N",
            "natcat_flag": "Y" if index % 31 == 0 else "N",
            "insured_object_identifier": asset_ref,
            "loss_event_number": f"LE-{index:08d}",
            "loss_event_source_system": "CRM",
            "loss_event_created_date": _date_plus(loss_date, 1),
            "loss_event_update_date": _date_plus(loss_date, 2),
            "loss_event_severity": "High" if has_paid_claim and risk_rating == "HIGH" else "Medium" if has_paid_claim else "Low",
            "loss_event_address": asset.get("risk_address_txt") or asset.get("garage_address_txt") or "",
            "catastrophe_code": f"CAT{index:04d}" if index % 29 == 0 else "",
            "catastrophe_name": "Weather Event" if index % 29 == 0 else "",
            "geography_identifier": "",
        })

        claim_rows.append({
            **_metadata(batch_id, "CRM"),
            "claim_identifier": claim_id,
            "claim_status": "Closed" if has_paid_claim else "Open",
            "claim_number": f"CL-{index:08d}",
            "claim_type": product_line,
            "claim_reason": "Accident" if product_line.upper().startswith("M") else "Damage",
            "claim_reported_date": _date_plus(loss_date, 1),
            "claim_settlement_date": claim_close_date if has_paid_claim else "",
            "claim_channel": "Online" if index % 2 else "Phone",
            "claim_handler": f"HANDLER_{1 + _stable_index(policy_ref, 12):02d}",
            "claim_product": product.get("product_cd") or policy.get("product_cd"),
            "claim_process_method": "Straight Through" if risk_rating == "LOW" else "Manual",
            "claim_state": "Settled" if has_paid_claim else "Assessment",
            "claim_band": "Large" if float(claim_amount) > 10000 else "Small",
            "claim_band_sort": "2" if float(claim_amount) > 10000 else "1",
            "claims_rejection_reason": "" if has_paid_claim else "Pending evidence",
            "claim_sensitivity": "High" if risk_rating == "HIGH" else "Normal",
            "coverage_verification_result": "Verified",
            "applicable_deductible_flag": "Y",
            "indemnity_logic": "Standard",
            "claim_specific_flag": "N",
            "cross_border_claim_indicator": "N",
            "bodily_injury_indicator": "Y" if index % 11 == 0 else "N",
            "hospitalization_flag": "Y" if index % 17 == 0 else "N",
            "total_loss_flag": "Y" if float(claim_amount) > 25000 else "N",
            "claims_history_lob": product_line,
            "claim_amt": claim_amount,
            "claims_paid": paid_amount,
            "is_claim_suspicious": "Y" if index % 19 == 0 else "N",
            "is_claim_fraud": policy.get("fraud_ind") or "N",
            "claim_open_date": claim_open_date,
            "claim_approval_date": _date_plus(claim_open_date, 5) if has_paid_claim else "",
            "claim_status_date": claim_close_date if has_paid_claim else claim_open_date,
            "claim_close_date": claim_close_date if has_paid_claim else "",
            "claim_duration": str(14 + _stable_index(policy_ref, 30)) if has_paid_claim else "0",
            "total_incurred": claim_amount,
            "no_claims_discount": policy.get("no_claims_discount_years", ""),
            "policy_identifier": policy_ref,
            "loss_event_identifier": loss_event_id,
        })

    _write_rows(target_dir, "policy_event.csv", policy_event_rows, POLICY_EVENT_COLUMNS)
    _write_rows(target_dir, "policy_participant.csv", policy_participant_rows, POLICY_PARTICIPANT_COLUMNS)
    _write_rows(target_dir, "risk_assessment.csv", risk_rows, RISK_ASSESSMENT_COLUMNS)
    _write_rows(target_dir, "loss_event_register.csv", loss_rows, LOSS_EVENT_REGISTER_COLUMNS)
    _write_rows(target_dir, "claim_register.csv", claim_rows, CLAIM_REGISTER_COLUMNS)


def _clean_geo_part(value: str, default: str = "unknown") -> str:
    text = str(value or "").strip()
    return text if text else default


def _geography_identifier(city: str, state: str, country: str, postcode: str = "") -> str:
    seed = "|".join([
        _clean_geo_part(city).lower(),
        _clean_geo_part(state).lower(),
        _clean_geo_part(country).lower(),
        str(postcode or "").strip().lower(),
    ])
    return f"GEO_{abs(_stable_index(seed, 999999)):06d}"


def _build_geography_row(batch_id: str, city: str, state: str, country: str, postcode: str = "") -> dict:
    geo_id = _geography_identifier(city, state, country, postcode)
    zone_index = _stable_index(geo_id, 90)
    return {
        **_metadata(batch_id, "CRM"),
        "applicable_jurisdiction": _clean_geo_part(country, "UK"),
        "census_zone": f"CZ{zone_index:02d}",
        "commune_code": f"COM{_stable_index(geo_id, 1000):03d}",
        "geocoding_level": "postcode" if postcode else "city",
        "latitude": f"{50.0 + (_stable_index(geo_id, 7000) / 1000):.6f}",
        "longitude": f"{-5.0 + (_stable_index(geo_id[::-1], 9000) / 1000):.6f}",
        "municipal_district": _clean_geo_part(city, "Unknown"),
        "natcat_hazard_zone_scheme": "UK_NATCAT",
        "sub_region": _clean_geo_part(state, "Unknown"),
        "surface_elevation": str(10 + _stable_index(geo_id, 280)),
        "city": _clean_geo_part(city, "Unknown"),
        "country": _clean_geo_part(country, "UK"),
        "state": _clean_geo_part(state, "Unknown"),
        "geography_identifier": geo_id,
    }


def _sales_channel_identifier(value: str) -> str:
    text = str(value or "Direct").strip() or "Direct"
    return f"CHAN_{text.upper().replace(' ', '_')}"


def _logical_policy_rows_from_crm(prd1_dir: Path, batch_id: str) -> dict[str, list[dict]]:
    policies = _read_rows(prd1_dir / "policy_register.csv")
    parties = _read_rows(prd1_dir / "party_master.csv")
    addresses = _read_rows(prd1_dir / "address_book.csv")
    contacts = _read_rows(prd1_dir / "contact_point.csv")
    products = _read_rows(prd1_dir / "product_catalog.csv")
    quotes = _read_rows(prd1_dir / "quote_register.csv")
    billings = _read_rows(prd1_dir / "billing_register.csv")
    homes = _read_rows(prd1_dir / "property_asset.csv")
    motors = _read_rows(prd1_dir / "vehicle_asset.csv")
    coverages = _read_rows(prd1_dir / "coverage_catalog.csv")
    policy_coverages = _read_rows(prd1_dir / "policy_coverage.csv")
    claims = _read_rows(prd1_dir / "claim_register.csv")
    losses = _read_rows(prd1_dir / "loss_event_register.csv")
    events = _read_rows(prd1_dir / "event_type_catalog.csv")
    roles = _read_rows(prd1_dir / "role_catalog.csv")
    policy_events = _read_rows(prd1_dir / "policy_event.csv")
    participants = _read_rows(prd1_dir / "policy_participant.csv")
    risk_assessments = _read_rows(prd1_dir / "risk_assessment.csv")
    sales_channels = _read_rows(prd1_dir / "sales_channel.csv")

    address_by_party = {row.get("party_ref", ""): row for row in addresses}
    contact_by_party = {row.get("party_ref", ""): row for row in contacts}
    party_by_ref = {row.get("party_ref", ""): row for row in parties}
    product_by_ref = {row.get("product_ref", ""): row for row in products}
    policy_by_ref = {row.get("policy_ref", ""): row for row in policies}

    geographies: dict[str, dict] = {}

    def register_geo(city: str, state: str, country: str, postcode: str = "") -> str:
        row = _build_geography_row(batch_id, city, state, country, postcode)
        geographies[row["geography_identifier"]] = row
        return row["geography_identifier"]

    person_rows = []
    for party in parties:
        party_ref = party.get("party_ref", "")
        address = address_by_party.get(party_ref, {})
        contact = contact_by_party.get(party_ref, {})
        geo_id = register_geo(
            address.get("city_nm"),
            address.get("state_cd"),
            address.get("country_cd"),
            address.get("postal_cd"),
        )
        person_rows.append({
            **_metadata(batch_id, "CRM"),
            "person_identifier": party.get("person_identifier") or party_ref,
            "assessed_disability_degree": party.get("assessed_disability_degree") or party.get("disability_degree"),
            "preferred_language": party.get("preferred_language") or party.get("language_pref"),
            "tenant_identifier": party.get("tenant_identifier") or party.get("tenant_cd"),
            "source_identifier": party.get("source_identifier") or party.get("src_party_ref"),
            "source_type": party.get("source_type") or party.get("src_party_type"),
            "is_opt_in_validated": party.get("is_opt_in_validated"),
            "is_opt_in_legitimate_interest": party.get("is_opt_in_legitimate_interest"),
            "is_lead": party.get("is_lead") or party.get("lead_ind"),
            "is_operational_paperless_consent": party.get("is_operational_paperless_consent") or party.get("paperless_ind"),
            "record_version": "1",
            "tenant_id": party.get("tenant_identifier") or party.get("tenant_cd"),
            "person_type": party.get("person_type") or party.get("party_kind"),
            "courtesy_title": party.get("title_txt"),
            "last_name": party.get("family_nm"),
            "full_name": party.get("display_nm"),
            "birth_date": _date_part(party.get("dob")),
            "gender": party.get("gender_txt"),
            "nationality": party.get("nationality_txt"),
            "marital_status": party.get("marital_txt"),
            "occupation": party.get("occupation_txt"),
            "street_address": address.get("street_txt"),
            "postcode": address.get("postal_cd"),
            "home_phone_number": contact.get("phone_home_txt") or party.get("phone_home_txt"),
            "work_phone_number": contact.get("phone_work_txt") or party.get("phone_work_txt"),
            "personal_email": contact.get("email_home_txt") or party.get("email_home_txt"),
            "work_email": contact.get("email_work_txt") or party.get("email_work_txt"),
            "job_title": party.get("job_title_txt") or party.get("legal_job_title_txt"),
            "role": party.get("role_txt"),
            "first_name": party.get("given_nm"),
            "company_name": party.get("legal_name"),
            "date_of_constitution": _date_part(party.get("constitution_dt")),
            "geography_identifier": geo_id,
        })

    product_rows = [
        {
            **_metadata(batch_id, "CRM"),
            "product_identifier": row.get("product_ref"),
            "product_type": row.get("product_type_txt"),
            "record_version": "1",
            "product_name": row.get("product_cd"),
            "line_of_business_code": row.get("product_lob_cd") or row.get("product_line"),
            "product_status": row.get("product_status_txt"),
            "product_variant": row.get("product_variant"),
            "product_launch_date": _date_part(row.get("product_launch_dt")),
            "product_description": row.get("product_line") or row.get("underwriting_group_txt"),
        }
        for row in products
    ]

    quote_rows = [
        {
            **_metadata(batch_id, "CRM"),
            "quote_identifier": row.get("quote_ref"),
            "product_identifier": row.get("product_ref"),
            "person_identifier": (party_by_ref.get(row.get("party_ref", ""), {}) or {}).get("person_identifier") or row.get("party_ref"),
            "quote_number": row.get("quote_no"),
            "quote_status": row.get("quote_status_txt"),
            "quoted_gross_premium_amount": _money(row.get("gross_amt")),
            "quoted_net_premium_amount": _money(row.get("net_amt")),
            "quote_premium": _money(row.get("quoted_premium")),
            "quote_date": _date_part(row.get("quoted_date")),
            "rejection_reason": row.get("rejection_reason"),
            "underwriting_approval_type": row.get("uw_approval_type"),
            "renewal_current_period_amount": _money(row.get("renewal_amt_curr")),
            "renewal_previous_period_amount": _money(row.get("renewal_amt_next")),
            "sales_channel_identifier": "",
            "quote_version_number": "1",
        }
        for row in quotes
    ]

    policy_rows = []
    for row in policies:
        product = product_by_ref.get(row.get("product_ref", ""), {})
        policy_rows.append({
            **_metadata(batch_id, "CRM"),
            "policy_cover_option": row.get("cover_option_txt"),
            "is_fraud": row.get("fraud_ind"),
            "policy_cycle": row.get("policy_cycle_no"),
            "policy_end_date": _date_part(row.get("policy_end_dt")),
            "policy_number": row.get("policy_no"),
            "policy_start_date": _date_part(row.get("policy_start_dt")),
            "policy_status": row.get("policy_status_txt"),
            "renewal_date": _date_part(row.get("renewal_dt")),
            "product_identifier": row.get("product_ref"),
            "record_version": "1",
            "policy_issue_date": _date_part(row.get("policy_start_dt")),
            "policy_sum_insured": _money(float(_money(row.get("gross_amt"))) * 120),
            "policy_tenure": row.get("policy_term_months"),
            "policy_type": product.get("product_type_txt") or product.get("product_line"),
            "is_policy_renewal": "Y" if _int_text(row.get("policy_cycle_no"), 1) != "1" else "N",
            "policy_cancellation_date": _date_part(row.get("policy_end_dt")) if row.get("policy_status_txt", "").upper() in {"CANCELLED", "LAPSED"} else "",
            "policy_cancellation_reason": "Non payment" if row.get("policy_status_txt", "").upper() in {"CANCELLED", "LAPSED"} else "",
            "policy_retention_limit": _money(float(_money(row.get("gross_amt"))) * 1.5),
            "policy_base_premium": _money(row.get("gross_amt")),
            "sales_channel_identifier": _sales_channel_identifier(row.get("sales_channel_txt")),
            "quote_identifier": row.get("quote_ref"),
            "policy_status_date": _date_part(row.get("policy_start_dt")),
            "limit_for_indemnification": _money(float(_money(row.get("gross_amt"))) * 120),
            "policy_expiry_date": _date_part(row.get("policy_end_dt")),
            "is_auto_renew_enabled": row.get("is_auto_renew_enabled"),
            "policy_renewal_satisfaction_score": row.get("policy_renewal_satisfaction_score"),
            "policy_renewal_feedback": row.get("policy_renewal_feedback"),
            "number_of_insured_persons": "1",
            "loyalty_discount_usage": row.get("loyalty_discount_usage"),
            "policy_identifier": row.get("policy_ref"),
            "discount": row.get("no_claims_discount_years"),
            "is_renewal_escalation": row.get("is_renewal_escalation"),
        })

    billing_rows = []
    for row in billings:
        policy = policy_by_ref.get(row.get("policy_ref", ""), {})
        annual = _money(row.get("annual_premium_amt"))
        paid = _money(row.get("paid_premium_amt"))
        outstanding = f"{max(float(annual) - float(paid), 0):.2f}"
        billing_rows.append({
            **_metadata(batch_id, "CRM"),
            "billing_identifier": row.get("billing_ref"),
            "billing_account_number": row.get("billing_account_no"),
            "billing_status": row.get("billing_status_txt"),
            "billing_frequency": row.get("billing_frequency_txt"),
            "payment_method": row.get("payment_method_txt"),
            "annual_premium_amount": annual,
            "billed_premium_amount": _money(row.get("billed_premium_amt")),
            "net_premium_amount": _money(policy.get("net_amt")),
            "outstanding_amount": outstanding,
            "last_payment_date": _date_part(row.get("last_payment_dt")),
            "next_payment_due_date": _date_part(row.get("next_bill_dt")),
            "last_invoice_date": _date_part(row.get("last_payment_dt")),
            "next_invoice_date": _date_part(row.get("next_bill_dt")),
            "payment_default_indicator": policy.get("is_installment_default"),
            "policy_identifier": row.get("policy_ref"),
            "gross_written_premium": _money(policy.get("gross_amt")),
            "gross_earned_premium": _money(policy.get("net_amt")),
            "missed_payment_count": policy.get("missed_payment_count"),
            "policy_renewal_current_period_amount": _money(policy.get("renewal_premium_curr")),
            "policy_renewal_next_period_amount": _money(policy.get("renewal_premium_next")),
            "commission_paid": _money(float(annual) * 0.08),
            "is_installment_default": policy.get("is_installment_default"),
            "policy_gross_revenue_amount": _money(policy.get("gross_amt")),
            "policy_net_revenue_amount": _money(policy.get("net_amt")),
        })

    coverage_rows = [
        {
            **_metadata(batch_id, "CRM"),
            "coverage_identifier": row.get("coverage_ref"),
            "coverage_code": row.get("coverage_ref"),
            "coverage_category": row.get("peril_txt"),
            "coverage_effective_date": _date_part(row.get("coverage_start_dt")),
            "coverage_discontinue_date": _date_part(row.get("coverage_end_dt")),
            "coverage_type": row.get("coverage_type_txt"),
            "maximum_deductible": _money(row.get("deductible_amt")),
            "coverage_name": row.get("coverage_desc"),
            "default_limit": _money(row.get("limit_amt")),
            "waiting_period": "0",
        }
        for row in coverages
    ]

    policy_coverage_rows = [
        {
            **_metadata(batch_id, "CRM"),
            "coverage_start_date": _date_part(row.get("coverage_start_dt")),
            "coverage_amount": _money(row.get("sum_insured_amt")),
            "deductible_amount": _money(row.get("deductible_amt")),
            "coverage_identifier": row.get("coverage_ref"),
            "coverage_status": row.get("coverage_status_txt"),
            "policy_coverage_identifier": row.get("policy_coverage_ref"),
            "coverage_status_code": row.get("coverage_status_txt"),
            "coverage_limit_amount": _money(row.get("sum_insured_amt")),
            "gross_premium": _money(row.get("gross_annual_premium_amt")),
            "coverage_end_date": _date_part(row.get("coverage_end_dt")),
            "policy_identifier": row.get("policy_ref"),
        }
        for row in policy_coverages
    ]

    home_rows = []
    for row in homes:
        geo_id = register_geo(row.get("city_nm"), row.get("state_cd"), row.get("country_cd"), row.get("postal_cd"))
        policy = policy_by_ref.get(row.get("policy_ref", ""), {})
        value = _money(float(_money(policy.get("gross_amt"))) * 120)
        home_rows.append({
            **_metadata(batch_id, "CRM"),
            "is_existing_home_customer": row.get("existing_home_ind"),
            "home_type": row.get("property_type_txt"),
            "roof_construction_material_type": row.get("roof_material_txt"),
            "wall_construction_material_type": row.get("wall_material_txt"),
            "home_risk_address": row.get("risk_address_txt"),
            "value_of_content": _money(float(value) * 0.30),
            "value_of_goods_carried": _money(float(value) * 0.05),
            "replacement_value": value,
            "actual_pre_event_entity_value": _money(float(value) * 0.85),
            "home_state": row.get("property_state_cd") or row.get("state_cd"),
            "insured_object_identifier": row.get("property_ref"),
            "insured_object_type": "Home",
            "insured_object_subtype": row.get("property_type_txt"),
            "insured_object_description": row.get("risk_address_txt"),
            "insured_value": value,
            "insured_object_start_date": _date_part(policy.get("policy_start_dt")),
            "insured_object_end_date": _date_part(policy.get("policy_end_dt")),
            "insured_object_current_status": policy.get("policy_status_txt"),
            "geography_identifier": geo_id,
            "policy_identifier": row.get("policy_ref"),
        })

    motor_rows = []
    for row in motors:
        geo_id = register_geo("", row.get("registration_state_cd"), "UK", "")
        policy = policy_by_ref.get(row.get("policy_ref", ""), {})
        motor_rows.append({
            **_metadata(batch_id, "CRM"),
            "vehicle_risk_class_code": row.get("risk_class_cd"),
            "vehicle_body_type": row.get("body_style_txt"),
            "vehicle_fuel_type": row.get("fuel_type_txt"),
            "vehicle_variant": row.get("variant_nm"),
            "vehicle_age": row.get("vehicle_age_yrs"),
            "vehicle_class": row.get("vehicle_class_txt"),
            "vehicle_model": row.get("model_nm"),
            "vehicle_owner_type": row.get("owner_type_txt"),
            "vehicle_reg_state": row.get("registration_state_cd"),
            "vehicle_type": row.get("vehicle_type_txt"),
            "vehicle_manufacturing_year": row.get("manufacture_yr"),
            "vehicle_risk_address": row.get("garage_address_txt"),
            "insured_object_identifier": row.get("vehicle_ref"),
            "insured_object_type": "Motor",
            "insured_object_subtype": row.get("vehicle_class_txt"),
            "insured_object_description": row.get("model_nm"),
            "insured_value": _money(row.get("insured_value_amt")),
            "insured_object_start_date": _date_part(policy.get("policy_start_dt")),
            "insured_object_end_date": _date_part(policy.get("policy_end_dt")),
            "insured_object_current_status": policy.get("policy_status_txt"),
            "geography_identifier": geo_id,
            "policy_identifier": row.get("policy_ref"),
        })

    claim_rows = [
        {
            **_metadata(batch_id, "CRM"),
            "claim_identifier": row.get("claim_identifier"),
            "claim_status": row.get("claim_status"),
            "claim_number": row.get("claim_number"),
            "claim_type": row.get("claim_type"),
            "claim_reason": row.get("claim_reason"),
            "claim_reported_date": _date_part(row.get("claim_reported_date")),
            "claim_settlement_date": _date_part(row.get("claim_settlement_date")),
            "claim_process_method": row.get("claim_process_method"),
            "claim_state": row.get("claim_state"),
            "claim_sensitivity": row.get("claim_sensitivity"),
            "coverage_verification_result": row.get("coverage_verification_result"),
            "applicable_deductible_flag": row.get("applicable_deductible_flag"),
            "total_loss_flag": row.get("total_loss_flag"),
            "claim_amt": _money(row.get("claim_amt")),
            "claims_paid": _money(row.get("claims_paid")),
            "claim_open_date": _date_part(row.get("claim_open_date")),
            "claim_approval_date": _date_part(row.get("claim_approval_date")),
            "claim_close_date": _date_part(row.get("claim_close_date")),
            "claim_duration": row.get("claim_duration"),
            "total_incurred": _money(row.get("total_incurred")),
            "policy_identifier": row.get("policy_identifier"),
            "loss_event_identifier": row.get("loss_event_identifier"),
            "claim_channel": row.get("claim_channel"),
            "cross_border_claim_indicator": row.get("cross_border_claim_indicator"),
            "bodily_injury_indicator": row.get("bodily_injury_indicator"),
            "hospitalization_flag": row.get("hospitalization_flag"),
        }
        for row in claims
    ]

    loss_rows = []
    for row in losses:
        geo_id = row.get("geography_identifier") or register_geo("", "", "UK", "")
        loss_rows.append({
            **_metadata(batch_id, "CRM"),
            "loss_event_name": row.get("loss_event_name"),
            "loss_event_description": row.get("loss_event_description"),
            "loss_date": _date_part(row.get("loss_date")),
            "loss_time": row.get("loss_time"),
            "loss_event_start_date": _date_part(row.get("loss_event_start_date")),
            "loss_event_end_date": _date_part(row.get("loss_event_end_date")),
            "loss_event_status": row.get("loss_event_status"),
            "loss_type": row.get("loss_type"),
            "loss_category": row.get("loss_category"),
            "loss_cause": row.get("loss_cause"),
            "effective_loss_date": _date_part(row.get("effective_loss_date")),
            "casualty_indicator": row.get("casualty_indicator"),
            "minimal_impact_flag": row.get("minimal_impact_flag"),
            "number_of_injured_parties": row.get("number_of_injured_parties"),
            "number_of_people_involved": row.get("number_of_people_involved"),
            "number_of_vehicles_involved": row.get("number_of_vehicles_involved"),
            "drugs_alcohol_indicator": row.get("drugs_alcohol_indicator"),
            "contributory_negligence_flag": row.get("contributory_negligence_flag"),
            "loss_event_identifier": row.get("loss_event_identifier"),
            "catastrophe_flag": row.get("catastrophe_flag"),
            "natcat_flag": row.get("natcat_flag"),
            "record_version": "1",
            "insured_object_identifier": row.get("insured_object_identifier"),
            "loss_event_number": row.get("loss_event_number"),
            "loss_event_source_system": row.get("loss_event_source_system"),
            "loss_event_created_date": _date_part(row.get("loss_event_created_date")),
            "loss_event_update_date": _date_part(row.get("loss_event_update_date")),
            "loss_event_severity": row.get("loss_event_severity"),
            "loss_event_address": row.get("loss_event_address"),
            "catastrophe_code": row.get("catastrophe_code"),
            "catastrophe_name": row.get("catastrophe_name"),
            "geography_identifier": geo_id,
        })

    policy_event_rows = [
        {
            **_metadata(batch_id, "CRM"),
            "policy_identifier": row.get("policy_identifier"),
            "policy_event_identifier": row.get("policy_event_identifier"),
            "policy_event_number": row.get("policy_event_number"),
            "event_date": _date_part(row.get("policy_event_date")),
            "policy_event_effective_date": _date_part(row.get("policy_event_effective_date")),
            "event_status": row.get("policy_event_status"),
            "premium_impact_amount": _money(row.get("premium_impact_amount")),
            "event_type_identifier": row.get("event_type_identifier"),
            "event_reason": row.get("policy_event_type"),
            "event_outcome": row.get("policy_event_status"),
        }
        for row in policy_events
    ]

    policy_participant_rows = [
        {
            **_metadata(batch_id, "CRM"),
            "participant_effective_date": _date_part(row.get("participant_effective_date")),
            "participant_status": row.get("participant_status"),
            "participant_end_date": _date_part(row.get("participant_end_date")),
            "benefit_percentage": row.get("benefit_percentage"),
            "primary_indicator": row.get("primary_indicator"),
            "policy_identifier": row.get("policy_identifier"),
            "policy_participant_identifier": row.get("policy_participant_identifier"),
            "person_identifier": (
                party_by_ref.get(str(row.get("person_role_identifier", "")).replace("PROLE_", "", 1), {}) or {}
            ).get("person_identifier"),
            "role_identifier": "ROLE_POLICY_HOLDER",
        }
        for row in participants
    ]

    risk_rows = [
        {
            **_metadata(batch_id, "CRM"),
            "risk_assessment_identifier": row.get("risk_assessment_identifier"),
            "risk_effective_date": _date_part(row.get("risk_effective_date")),
            "risk_assessment_date": _date_part(row.get("risk_assessment_date")),
            "risk_assessment_method": row.get("risk_assessment_method"),
            "risk_assessment_outcome": row.get("risk_assessment_outcome"),
            "insured_object_identifier": row.get("insured_object_identifier"),
            "risk_number": row.get("risk_number"),
            "risk_type": row.get("risk_type"),
            "risk_status": row.get("risk_status"),
            "risk_end_date": _date_part(row.get("risk_end_date")),
            "risk_classification": row.get("risk_classification"),
            "risk_rating": row.get("risk_rating"),
            "risk_score": row.get("risk_score"),
            "exposure_amount": _money(row.get("exposure_amount")),
            "claim_amount": _money(row.get("claim_amount")),
            "total_incurred_amount": _money(row.get("total_incurred_amount")),
            "risk_status_date": _date_part(row.get("risk_status_date")),
        }
        for row in risk_assessments
    ]

    return {
        "billing.csv": billing_rows,
        "claim.csv": claim_rows,
        "coverage.csv": coverage_rows,
        "event_type.csv": [
            {
                **_metadata(batch_id, "CRM"),
                "event_type_identifier": row.get("event_type_identifier"),
                "event_type_code": row.get("event_type_code"),
                "event_type_name": row.get("event_type_name"),
                "event_type_description": row.get("event_type_description"),
            }
            for row in events
        ],
        "geography.csv": list(geographies.values()),
        "home.csv": home_rows,
        "loss_event.csv": loss_rows,
        "motor.csv": motor_rows,
        "person.csv": person_rows,
        "policy.csv": policy_rows,
        "policy_coverage.csv": policy_coverage_rows,
        "policy_event.csv": policy_event_rows,
        "policy_participant.csv": policy_participant_rows,
        "product.csv": product_rows,
        "quote.csv": quote_rows,
        "risk_assessment.csv": risk_rows,
        "role.csv": [
            {
                **_metadata(batch_id, "CRM"),
                "role_name": row.get("role_name"),
                "role_description": row.get("role_description"),
                "role_identifier": row.get("role_identifier"),
                "role_code": row.get("role_code"),
            }
            for row in roles
        ],
        "sales_channel.csv": [
            {
                **_metadata(batch_id, "CRM"),
                "channel_name": row.get("channel_name"),
                "channel_type": row.get("channel_type"),
                "distribution_channel": row.get("distribution_channel"),
                "channel_status": row.get("channel_status"),
                "sales_channel_identifier": row.get("sales_channel_identifier"),
            }
            for row in sales_channels
        ],
    }


def _logical_policy_rows_from_sap(prd2_dir: Path, batch_id: str) -> dict[str, list[dict]]:
    policies = _read_rows(prd2_dir / "sap_policy.csv")
    coverages = _read_rows(prd2_dir / "sap_coverage.csv")
    policy_coverages = _read_rows(prd2_dir / "sap_policy_coverage.csv")

    policy_rows = [
        {
            **_metadata(batch_id, "SAP"),
            "policy_cover_option": row.get("cover_option"),
            "is_fraud": row.get("fraud_flag"),
            "policy_cycle": row.get("policy_cycle"),
            "policy_end_date": _date_part(row.get("policy_end_date")),
            "policy_number": row.get("policy_number"),
            "policy_start_date": _date_part(row.get("policy_start_date")),
            "policy_status": row.get("policy_status"),
            "renewal_date": _date_part(row.get("renewal_date")),
            "product_identifier": _remove_sap_prefix(row.get("sap_product_id")),
            "record_version": "1",
            "policy_issue_date": _date_part(row.get("policy_issue_date")),
            "policy_sum_insured": _money(float(_money(row.get("gross_written_premium"))) * 120),
            "policy_tenure": "",
            "policy_type": "",
            "is_policy_renewal": "Y" if _int_text(row.get("policy_cycle"), 1) != "1" else "N",
            "policy_cancellation_date": "",
            "policy_cancellation_reason": "",
            "policy_retention_limit": _money(float(_money(row.get("gross_written_premium"))) * 1.5),
            "policy_base_premium": _money(row.get("gross_written_premium")),
            "sales_channel_identifier": _sales_channel_identifier(row.get("sales_channel")),
            "quote_identifier": _remove_sap_prefix(row.get("sap_quote_id")),
            "policy_status_date": _date_part(row.get("policy_start_date")),
            "limit_for_indemnification": _money(float(_money(row.get("gross_written_premium"))) * 120),
            "policy_expiry_date": _date_part(row.get("policy_end_date")),
            "is_auto_renew_enabled": row.get("auto_renew_flag"),
            "policy_renewal_satisfaction_score": "",
            "policy_renewal_feedback": "",
            "number_of_insured_persons": "1",
            "loyalty_discount_usage": "",
            "policy_identifier": row.get("crm_policy_ref") or _remove_sap_prefix(row.get("sap_policy_id")),
            "discount": "",
            "is_renewal_escalation": "",
        }
        for row in policies
    ]

    coverage_rows = [
        {
            **_metadata(batch_id, "SAP"),
            "coverage_identifier": row.get("crm_coverage_ref") or _remove_sap_prefix(row.get("sap_coverage_id")),
            "coverage_code": row.get("crm_coverage_ref") or _remove_sap_prefix(row.get("sap_coverage_id")),
            "coverage_category": row.get("peril_type"),
            "coverage_effective_date": _date_part(row.get("coverage_start_date")),
            "coverage_discontinue_date": _date_part(row.get("coverage_end_date")),
            "coverage_type": row.get("coverage_type"),
            "maximum_deductible": _money(row.get("deductible_amount")),
            "coverage_name": row.get("coverage_description"),
            "default_limit": _money(row.get("limit_amount")),
            "waiting_period": "0",
        }
        for row in coverages
    ]

    policy_coverage_rows = [
        {
            **_metadata(batch_id, "SAP"),
            "coverage_start_date": _date_part(row.get("coverage_start_date")),
            "coverage_amount": _money(row.get("sum_insured_amount")),
            "deductible_amount": _money(row.get("deductible_amount")),
            "coverage_identifier": _remove_sap_prefix(row.get("sap_coverage_id")),
            "coverage_status": row.get("coverage_status"),
            "policy_coverage_identifier": row.get("crm_policy_coverage_ref") or _remove_sap_prefix(row.get("sap_policy_coverage_id")),
            "coverage_status_code": row.get("coverage_status"),
            "coverage_limit_amount": _money(row.get("sum_insured_amount")),
            "gross_premium": _money(row.get("gross_annual_premium")),
            "coverage_end_date": _date_part(row.get("coverage_end_date")),
            "policy_identifier": _remove_sap_prefix(row.get("sap_policy_id")),
        }
        for row in policy_coverages
    ]

    return {
        "policy.csv": policy_rows,
        "coverage.csv": coverage_rows,
        "policy_coverage.csv": policy_coverage_rows,
    }


def _rewrite_policy_outputs_to_latest_input(prd1_dir: Path, prd2_dir: Path, batch_id: str) -> None:
    prd1_rows = _logical_policy_rows_from_crm(prd1_dir, batch_id)
    prd2_rows = _logical_policy_rows_from_sap(prd2_dir, batch_id)

    for folder in (prd1_dir, prd2_dir):
        for path in folder.glob("*.csv"):
            path.unlink()

    for file_name, schema in LATEST_POLICY_PRD1_SCHEMAS.items():
        _write_rows(prd1_dir, file_name, prd1_rows.get(file_name, []), schema)
    for file_name, schema in LATEST_POLICY_PRD2_SCHEMAS.items():
        _write_rows(prd2_dir, file_name, prd2_rows.get(file_name, []), schema)


def _sap_policy_no(value: str, index: int) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text


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
    _build_excel_policy_support_files(prd1_dir, batch_id)

    _write_sap_view(prd1_dir, prd2_dir, batch_id)
    _rewrite_policy_outputs_to_latest_input(prd1_dir, prd2_dir, batch_id)

    shutil.rmtree(work_root)
    _validate_required_files(prd1_dir, set(LATEST_POLICY_PRD1_SCHEMAS))
    _validate_required_files(prd2_dir, set(LATEST_POLICY_PRD2_SCHEMAS))

    with (root / "_source_manifest.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["source", "folder", "description"])
        writer.writeheader()
        writer.writerows([
            {"source": "prd_01", "folder": str(prd1_dir), "description": "Policy CRM raw source-1 files"},
            {"source": "prd_02", "folder": str(prd2_dir), "description": "Policy SAP raw source-2 files with SAP-specific table names and columns"},
        ])

    return {"prd_01": str(prd1_dir), "prd_02": str(prd2_dir), "root": str(root)}
