from __future__ import annotations

import csv
import os
import random
import re
from datetime import date, datetime, timedelta
from pathlib import Path

from helper.csv_writer import write_csv
from helper.enhanced_ddl import parse_enhanced_ddl
from helper.enhanced_rules import (
    BASE_CUSTOMER_STATUSES,
    BASE_POLICY_CHANNELS,
    CHANNEL_TYPE_BY_NAME,
    claim_product_for_code,
    insurance_category_for_code,
)
from helper.key_factory import md5_hasher


ROOT = Path(__file__).resolve().parents[1]
ENHANCED_EXAMPLE_DIR = ROOT / "enhanced_360" / "data_example"
RS = "CRM"
RECOVERY_SENTINEL_TIMESTAMP = "1900-01-01T00:00:00"


def _norm_name(value: str) -> str:
    value = str(value).strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def _norm_row(row: dict) -> dict:
    return {_norm_name(key): value for key, value in row.items()}


def _ordered(row: dict, columns: list[str]) -> dict:
    return {column: row.get(column, "") for column in columns}


def _rename_keys(row: dict, rename_map: dict[str, str]) -> dict:
    renamed = dict(row)
    for old_key, new_key in rename_map.items():
        if old_key in renamed:
            renamed[new_key] = renamed.pop(old_key)
    return renamed


def _read_example(file_name: str) -> list[dict]:
    path = ENHANCED_EXAMPLE_DIR / file_name
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _sample(rows: list[dict], idx: int) -> dict:
    if not rows:
        return {}
    return rows[idx % len(rows)]


def _date(value: str) -> str:
    if not value:
        return ""
    value = str(value).strip()
    for fmt in ("%d-%b-%y", "%d-%b-%Y", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value[:19]).date().isoformat()
    except ValueError:
        return value


def _timestamp(value: str, default_time: str = "00:00:00") -> str:
    date_value = _date(value)
    if not date_value:
        return ""
    try:
        parsed = datetime.fromisoformat(str(value).strip()[:19])
        return parsed.replace(microsecond=0).isoformat()
    except ValueError:
        return f"{date_value}T{default_time}"


def _timestamp_from_date(value: date | None, default_time: str = "00:00:00") -> str:
    if not value:
        return ""
    return f"{value.isoformat()}T{default_time}"


def _date_obj(value) -> date | None:
    text = _date(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text[:10]).date()
    except ValueError:
        return None


def _int(value, default="") -> str:
    if value in (None, ""):
        return default
    try:
        return str(int(float(str(value).replace(",", "").strip())))
    except ValueError:
        return default


def _float(value, default="") -> str:
    if value in (None, ""):
        return default
    try:
        return str(float(str(value).replace(",", "").strip()))
    except ValueError:
        return default


def _float_value(value, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(str(value).replace(",", "").strip())
    except ValueError:
        return default


def _truthy_text(value) -> bool:
    return str(value or "").strip().upper() in {"Y", "YES", "TRUE", "T", "1"}


def _yn(value, default="N") -> str:
    if value in (None, ""):
        return default
    text = str(value).strip().upper()
    if text in {"Y", "YES", "TRUE", "T"}:
        return "Y"
    if text in {"N", "NO", "FALSE", "F"}:
        return "N"
    try:
        return "Y" if float(text.replace(",", "")) > 0 else "N"
    except ValueError:
        return default


def _insured_value(policy_row: dict, product_code: str, asset_kind: str) -> str:
    policy_sum_insured = _int(policy_row.get("policy_sum_insured"), default="")
    if policy_sum_insured:
        return policy_sum_insured

    product_text = str(product_code).upper()
    if asset_kind == "motor":
        return "75000" if "COMMERCIAL" in product_text else "35000"
    if asset_kind == "home":
        return "750000" if "COMMERCIAL" in product_text or "PROPERTY" in product_text else "300000"
    return "100000"


def _claim_financial_settings(cfg: dict | None) -> dict:
    defaults = {
        "enabled": True,
        "source_priority": ["fact_policy_claim_rows", "derived_from_policy_sum_insured"],
        "band_rules": [
            {"band": "0-6k", "sort": 1, "min": 0, "max": 6000},
            {"band": "6k-8k", "sort": 2, "min": 6000, "max": 8000},
            {"band": "8k-10k", "sort": 3, "min": 8000, "max": 10000},
            {"band": "10k-13k", "sort": 4, "min": 10000, "max": 13000},
            {"band": "13+", "sort": 5, "min": 13000, "max": None},
        ],
        "severity_weights": {"0-6k": 5, "6k-8k": 25, "8k-10k": 25, "10k-13k": 15, "13+": 30},
        "paid_ratio_by_status": {
            "OPEN": [0.35, 0.70],
            "CLOSED": [0.90, 1.00],
            "REPUDIATED": [0.00, 0.20],
            "DEFAULT": [0.45, 0.85],
        },
        "expense_ratio": [0.05, 0.12],
        "recovery_received_ratio": [0.05, 0.35],
        "compensation_ratio": [0.01, 0.08],
        "remediation_ratio": [0.002, 0.02],
        "fraud_amount_ratio": [0.05, 0.30],
        "legal_expense_ratio": [0.01, 0.08],
        "cap_claim_amount_at_policy_sum_insured": True,
    }
    if cfg:
        defaults.update(cfg.get("claim_financial_settings", {}))
    return defaults


def _weighted_choice_from_mapping(weights: dict, rng: random.Random) -> str:
    items = [(str(key), float(value)) for key, value in (weights or {}).items() if float(value) > 0]
    if not items:
        return ""
    total = sum(weight for _, weight in items)
    pick = rng.uniform(0, total)
    cumulative = 0.0
    for key, weight in items:
        cumulative += weight
        if pick <= cumulative:
            return key
    return items[-1][0]


def _ratio_range(settings: dict, key: str, default: tuple[float, float]) -> tuple[float, float]:
    value = settings.get(key, default)
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        return default
    return float(value[0]), float(value[1])


def _band_for_claim_amount(amount: int, settings: dict) -> tuple[str, str]:
    for rule in settings.get("band_rules", []):
        low = int(rule.get("min", 0) or 0)
        high = rule.get("max")
        high_int = int(high) if high not in (None, "") else None
        if amount >= low and (high_int is None or amount < high_int):
            return str(rule.get("band", "")), str(int(rule.get("sort", 0) or 0))
    if settings.get("band_rules"):
        rule = settings["band_rules"][-1]
        return str(rule.get("band", "")), str(int(rule.get("sort", 0) or 0))
    return "", "0"


def _derive_claim_amount(policy_sum_insured: int, settings: dict, rng: random.Random) -> int:
    selected_band = _weighted_choice_from_mapping(settings.get("severity_weights", {}), rng)
    rules_by_band = {str(rule.get("band")): rule for rule in settings.get("band_rules", [])}
    rule = rules_by_band.get(selected_band) or (settings.get("band_rules") or [{}])[0]
    low = int(rule.get("min", 0) or 0)
    high_value = rule.get("max")
    high = int(high_value) if high_value not in (None, "") else max(low + 2000, int(policy_sum_insured * 0.25))
    cap = policy_sum_insured if policy_sum_insured > 0 and settings.get("cap_claim_amount_at_policy_sum_insured", True) else high
    high = max(1, min(high, cap))
    low = max(1, min(low, high))
    return int(rng.uniform(low, high))


def _derive_claim_financials(
    fact: dict,
    policy_row: dict,
    claim_sample: dict,
    settings: dict,
    rng: random.Random,
) -> dict:
    if not settings.get("enabled", True):
        return {
            "claim_amount": _int(fact.get("ClaimAmount")),
            "claims_paid": _int(fact.get("ClaimsPaid")),
            "outstanding_reserve": _int(fact.get("OutstandingReserve")),
            "claims_expenses": _int(fact.get("Claims Expenses")),
            "recovery_received": _int(fact.get("RecoveryReceived")),
            "compensation_offered": _int(fact.get("CompensationOffered")),
            "remediation_amount": _int(fact.get("RemediationAmount")),
            "suspectd_amount": _int(fact.get("SuspectedAmount")),
            "fraud_amount": _int(fact.get("FraudAmount")),
            "legal_expenses": _int(fact.get("LegalExpenses")),
            "claim_band": fact.get("ClaimBand", ""),
            "claim_band_sort": _int(fact.get("ClaimBand Sort")),
        }

    policy_sum_insured = int(_float_value(policy_row.get("policy_sum_insured"), 0.0))
    claim_amount = int(_float_value(fact.get("ClaimAmount"), 0.0))
    if claim_amount <= 0:
        claim_amount = _derive_claim_amount(policy_sum_insured, settings, rng)
    if policy_sum_insured > 0 and settings.get("cap_claim_amount_at_policy_sum_insured", True):
        claim_amount = min(claim_amount, policy_sum_insured)

    claim_status = str(claim_sample.get("ClaimStatus", "") or "").strip().upper()
    paid_ratios = settings.get("paid_ratio_by_status", {})
    paid_range = paid_ratios.get(claim_status, paid_ratios.get("DEFAULT", [0.45, 0.85]))
    paid_low, paid_high = float(paid_range[0]), float(paid_range[1])
    claims_paid = int(_float_value(fact.get("ClaimsPaid"), -1.0))
    if claims_paid < 0:
        claims_paid = int(claim_amount * rng.uniform(float(paid_low), float(paid_high)))
    claims_paid = max(0, min(claims_paid, claim_amount))

    outstanding_reserve = int(_float_value(fact.get("OutstandingReserve"), -1.0))
    if outstanding_reserve < 0:
        outstanding_reserve = max(0, claim_amount - claims_paid)
    if claim_status in {"CLOSED", "SETTLED"}:
        outstanding_reserve = min(outstanding_reserve, int(claim_amount * 0.05))
        claims_paid = claim_amount - outstanding_reserve

    expense_low, expense_high = _ratio_range(settings, "expense_ratio", (0.05, 0.12))
    claims_expenses = int(_float_value(fact.get("Claims Expenses"), -1.0))
    if claims_expenses < 0:
        claims_expenses = int(claim_amount * rng.uniform(expense_low, expense_high))

    recovery_received = int(_float_value(fact.get("RecoveryReceived"), -1.0))
    if recovery_received < 0:
        if _truthy_text(claim_sample.get("Recovery_Happened")):
            low, high = _ratio_range(settings, "recovery_received_ratio", (0.05, 0.35))
            recovery_received = int(claim_amount * rng.uniform(low, high))
        else:
            recovery_received = 0

    compensation_offered = int(_float_value(fact.get("CompensationOffered"), -1.0))
    if compensation_offered < 0:
        low, high = _ratio_range(settings, "compensation_ratio", (0.01, 0.08))
        compensation_offered = int(claim_amount * rng.uniform(low, high))

    remediation_amount = int(_float_value(fact.get("RemediationAmount"), -1.0))
    if remediation_amount < 0:
        low, high = _ratio_range(settings, "remediation_ratio", (0.002, 0.02))
        remediation_amount = int(claim_amount * rng.uniform(low, high))

    fraud_amount = int(_float_value(fact.get("FraudAmount"), -1.0))
    suspected_amount = int(_float_value(fact.get("SuspectedAmount"), -1.0))
    if _truthy_text(claim_sample.get("FraudFlag")):
        low, high = _ratio_range(settings, "fraud_amount_ratio", (0.05, 0.30))
        if fraud_amount < 0:
            fraud_amount = int(claim_amount * rng.uniform(low, high))
        if suspected_amount < 0:
            suspected_amount = max(fraud_amount, int(claim_amount * rng.uniform(low, high)))
    else:
        fraud_amount = max(0, fraud_amount)
        suspected_amount = max(0, suspected_amount)

    legal_expenses = int(_float_value(fact.get("LegalExpenses"), -1.0))
    if legal_expenses < 0:
        if _truthy_text(claim_sample.get("LitigationIndicator")):
            low, high = _ratio_range(settings, "legal_expense_ratio", (0.01, 0.08))
            legal_expenses = int(claim_amount * rng.uniform(low, high))
        else:
            legal_expenses = 0

    claim_band = str(fact.get("ClaimBand", "") or "").strip()
    claim_band_sort = _int(fact.get("ClaimBand Sort"), default="")
    if not claim_band or not claim_band_sort:
        claim_band, claim_band_sort = _band_for_claim_amount(claim_amount, settings)

    return {
        "claim_amount": str(claim_amount),
        "claims_paid": str(claims_paid),
        "outstanding_reserve": str(outstanding_reserve),
        "claims_expenses": str(claims_expenses),
        "recovery_received": str(max(0, recovery_received)),
        "compensation_offered": str(max(0, compensation_offered)),
        "remediation_amount": str(max(0, remediation_amount)),
        "suspectd_amount": str(max(0, suspected_amount)),
        "fraud_amount": str(max(0, fraud_amount)),
        "legal_expenses": str(max(0, legal_expenses)),
        "claim_band": claim_band,
        "claim_band_sort": claim_band_sort,
    }


def _customer_satisfaction_label(value) -> str:
    score = _int(value, default="")
    if not score:
        return "UNKNOWN"
    score_int = int(score)
    if score_int >= 9:
        return "VERY_SATISFIED"
    if score_int >= 7:
        return "SATISFIED"
    if score_int >= 5:
        return "NEUTRAL"
    return "DISSATISFIED"


def _link_row(table_name: str, left_col: str, left_hk: str, right_col: str, right_hk: str, load_date: str) -> dict:
    pk = f"{table_name.removeprefix('link_')}_hash_key"
    return {
        pk: md5_hasher(f"{left_hk}|{right_hk}"),
        "load_date": load_date,
        "record_source": RS,
        left_col: left_hk,
        right_col: right_hk,
    }


def _link_row_with_pk(pk: str, left_col: str, left_hk: str, right_col: str, right_hk: str, load_date: str) -> dict:
    return {
        pk: md5_hasher(f"{left_hk}|{right_hk}"),
        "load_date": load_date,
        "record_source": RS,
        left_col: left_hk,
        right_col: right_hk,
    }


STRING_BOOLEAN_COLUMNS = {
    "sat_campaign": {"is_active"},
    "sat_claim": {
        "is_claim_suspicious",
        "is_claim_fraud",
        "is_litigation",
        "is_recovery_opportunity",
        "is_recovery_happened",
    },
    "sat_complaint": {"is_financial_ombudsman_service_referral"},
    "sat_home": {"is_existing_home_customer"},
    "sat_motor": {"is_existing_motor_customer"},
    "sat_person": {"is_lead"},
    "sat_policy": {"fraud_flag", "is_policy_renewal"},
    "sat_regulation": {"is_regulation_on_time"},
}


def _normalize_string_boolean_columns(tables: dict[str, list[dict]]) -> None:
    for table_name, columns in STRING_BOOLEAN_COLUMNS.items():
        for row in tables.get(table_name, []):
            for column in columns:
                if column in row:
                    row[column] = _yn(row.get(column))


def _normalize_blank_numeric_columns(tables: dict[str, list[dict]], column_types: dict[str, dict[str, str]]) -> None:
    numeric_defaults = {
        "INT": "0",
        "INTEGER": "0",
        "BIGINT": "0",
        "SMALLINT": "0",
        "TINYINT": "0",
        "DOUBLE": "0.0",
        "FLOAT": "0.0",
        "DECIMAL": "0.0",
        "NUMERIC": "0.0",
    }
    for table_name, rows in tables.items():
        table_types = column_types.get(table_name, {})
        numeric_columns = {
            column_name: numeric_defaults[column_type]
            for column_name, column_type in table_types.items()
            if column_type in numeric_defaults
        }
        if not numeric_columns:
            continue
        for row in rows:
            for column_name, default_value in numeric_columns.items():
                if row.get(column_name) in (None, ""):
                    row[column_name] = default_value


def _id_from_hub(row: dict, id_col: str) -> str:
    return str(row.get(id_col, "") or row.get(id_col.replace("_id", "_number"), "") or "")


def _build_base_tables(ctx: dict) -> dict[str, list[dict]]:
    links = dict(ctx.get("links", {}))
    if ctx.get("link_quote_product"):
        links["Link_Quote_Product"] = ctx["link_quote_product"]

    hub_address_rows = [
        _rename_keys(_norm_row(row), {
            "home_address_hash_key": "address_hash_key",
            "home_address_id": "address_id",
        })
        for row in ctx.get("hub_addr_rows", [])
    ]
    sat_address_rows = [
        {
            **_rename_keys(_norm_row(row), {
                "home_address_hash_key": "address_hash_key",
            }),
            "type": "HOME",
        }
        for row in ctx.get("sat_adr", [])
    ]
    link_person_address_rows = [
        _rename_keys(_norm_row(row), {
            "person_home_address_hash_key": "person_address_hash_key",
            "home_address_hash_key": "address_hash_key",
        })
        for row in links.pop("Link_Person_Home_Address", [])
    ]
    hub_home_rows = [
        _rename_keys(_norm_row(row), {"home_id": "insured_object_home_id"})
        for row in ctx.get("hub_home_rows", [])
    ]
    hub_motor_rows = [
        _rename_keys(_norm_row(row), {"motor_id": "insured_object_motor_id"})
        for row in ctx.get("hub_mot_rows", [])
    ]

    sources = {
        "hub_person": ctx.get("hub_person_rows", []),
        "hub_natural_person": ctx.get("hub_nat", []),
        "hub_legal_person": ctx.get("hub_leg", []),
        "hub_product": ctx.get("hub_prod_rows", []),
        "hub_lead": ctx.get("hub_lead_rows", []),
        "hub_customer": ctx.get("hub_cust_rows", []),
        "hub_identities": ctx.get("hub_id_rows", []),
        "hub_contact": ctx.get("hub_con_rows", []),
        "hub_consent": ctx.get("hub_cns_rows", []),
        "hub_account": ctx.get("hub_acc_rows", []),
        "hub_marketing_preference": ctx.get("hub_mpr_rows", []),
        "hub_marketing_engagement": ctx.get("hub_men_rows", []),
        "hub_quote": ctx.get("hub_quo_rows", []),
        "hub_policy": ctx.get("hub_pol_rows", []),
        "hub_motor": hub_motor_rows,
        "hub_home": hub_home_rows,
        "hub_address": hub_address_rows,
        "sat_natural_person": ctx.get("sat_nat", []),
        "sat_legal_person": ctx.get("sat_leg", []),
        "sat_person": ctx.get("sat_per", []),
        "sat_lead": ctx.get("sat_lea", []),
        "sat_customer": ctx.get("sat_cus", []),
        "sat_identities": ctx.get("sat_eci", []),
        "sat_contact": ctx.get("sat_con", []),
        "sat_consent": ctx.get("sat_cns", []),
        "sat_account": ctx.get("sat_acc", []),
        "sat_marketing_preference": ctx.get("sat_mpr", []),
        "sat_marketing_engagement": ctx.get("sat_men", []),
        "sat_quote": ctx.get("sat_quo", []),
        "sat_policy": ctx.get("sat_pol", []),
        "sat_motor": ctx.get("sat_mot", []),
        "sat_home": ctx.get("sat_hom", []),
        "sat_address": sat_address_rows,
        "sat_product": ctx.get("sat_product_rows", []),
        "link_person_address": link_person_address_rows,
    }

    for link_name, rows in links.items():
        sources[_norm_name(link_name)] = rows

    return {
        table_name: [
            row if all(str(key).islower() for key in row.keys()) else _norm_row(row)
            for row in rows
        ]
        for table_name, rows in sources.items()
    }


def _augment_base_satellites(tables: dict[str, list[dict]]) -> None:
    customers = _read_example("DimCustomer.csv")
    policies = _read_example("DimPolicy.csv")
    fact_policies = _read_example("FactPolicy.csv")
    products = _read_example("DimProduct.csv")
    quotes = _read_example("FactQuote.csv")

    for idx, row in enumerate(tables.get("sat_customer", [])):
        sample = _sample(customers, idx)
        row.update({
            "income_band": sample.get("IncomeBand", ""),
            "customer_satisfaction": _customer_satisfaction_label(sample.get("CustomerSatisfaction")),
            "customer_age_band": sample.get("AgeBand", ""),
            "net_promotor_code_segment": sample.get("NPS Segment", ""),
        })

    for idx, row in enumerate(tables.get("sat_policy", [])):
        dim = _sample(policies, idx)
        fact = _sample(fact_policies, idx)
        row.update({
            "quote_id": dim.get("QuoteID", ""),
            "policy_type": dim.get("Policy Type", ""),
            "is_policy_renewal": dim.get("Renewal Indicator", ""),
            "policy_cancellation_reason": dim.get("Cancellation Reason", ""),
            "policy_sum_insured": _int(dim.get("SumInsured")),
            "policy_retention_limit": _int(dim.get("RetentionLimit")),
            "policy_risk_score": _float(dim.get("RiskScore")),
            "policy_risk_band": dim.get("RiskBand", ""),
            "policy_base_premium": _int(fact.get("BasePrem")),
            "gross_written_premium": _int(fact.get("GrossWrittenPremium(GWP)")),
            "earned_premium": _int(fact.get("EarnedPremium")),
            "incurred_but_not_reported": _int(fact.get("IncurredButNotReported(IBNR)")),
            "operating_expenses": _int(fact.get("OperatingExpenses")),
            "administrative_expenses": _int(fact.get("AdministrativeExpenses")),
            "profit_margin": _int(fact.get("ProfitMargin")),
            "taxes_and_levies": _int(fact.get("TaxesandLevies")),
            "amount_approved": _int(fact.get("AmountApproved")),
            "ceded_premium": _int(fact.get("CededPremium")),
            "commission_paid": _int(fact.get("CommissionPaid")),
            "ceded_commission": _int(fact.get("CededCommission")),
            "exposure_amount": _int(fact.get("ExposureAmount")),
            "investment_income": _int(fact.get("InvestmentIncome")),
            "underwriting_cycle_time_in_days": _int(fact.get("UnderwritingCycleTime(days)")),
            "underwriting_expenses": _int(fact.get("UnderwritingExpenses")),
            "transaction_date": _timestamp(fact.get("TransactionDate")),
            "record_type": fact.get("RecordType", ""),
            "discount": _int(fact.get("Discount ")),
            "override_commission": _int(fact.get("Override Comission")),
            "partial_recovery_percentage": _int(fact.get("Partial Recovery%")),
            "policy_issue_date": _timestamp(row.get("policy_start_date")),
        })

    for idx, row in enumerate(tables.get("sat_product", [])):
        sample = _sample(products, idx)
        row.update({
            "product_variant": sample.get("ProductVariant", ""),
            "product_name": sample.get("ProductName", ""),
            "product_launch_date": _timestamp(sample.get("ProductLaunchDate")),
            "product_status": sample.get("ProductStatus", ""),
            "product_line_of_business_code": sample.get("LOBCode", ""),
            "underwriting_group": sample.get("UnderwritingGroup", ""),
            "regulatory_approval_code": sample.get("RegulatoryApprovalCode", ""),
        })

    for idx, row in enumerate(tables.get("sat_quote", [])):
        sample = _sample(quotes, idx)
        row.update({
            "quoted_premium": _int(sample.get("QuotedPremium")),
            "quote_date": _timestamp(sample.get("quote_date")),
            "quote_month_name": sample.get("Month name", ""),
            "risk_score": _int(sample.get("RiskScore")),
            "policy_complexity": sample.get("PolicyComplexity", ""),
            "uw_approval_type": sample.get("UWApprovalType", ""),
            "rejection_reason": sample.get("RejectionReason", ""),
        })


def _settings(cfg: dict | None) -> dict:
    defaults = {
        "broker_count": 20,
        "campaign_count": 10,
        "channel_count": 4,
        "claim_policy_rate": 0.08,
        "complaint_customer_rate": 0.02,
        "override_policy_rate": 0.05,
        "regulation_count": 100,
    }
    if cfg:
        defaults.update(cfg.get("enhanced_settings", {}))
    return defaults


def _add_enhanced_entities(tables: dict[str, list[dict]], ctx: dict, cfg: dict | None) -> None:
    settings = _settings(cfg)
    rng = random.Random((ctx.get("seed") or 42) + 7001)
    fallback_date = ctx.get("extract_ts") or datetime.now().replace(microsecond=0).isoformat()
    hub_date = ctx.get("hub_load_date") or ctx.get("HUB_DATE") or fallback_date
    link_date = ctx.get("link_load_date") or hub_date
    sat_date = ctx.get("sat_load_date") or link_date

    person_hks = list(ctx.get("person_hks", []))
    policy_hks = [row.get("Policy Hash Key") for row in ctx.get("hub_pol_rows", []) if row.get("Policy Hash Key")]
    customer_hks = [row.get("Customer Hash Key") for row in ctx.get("hub_cust_rows", []) if row.get("Customer Hash Key")]
    policy_sat_by_hk = {
        row.get("Policy Hash Key"): row for row in ctx.get("sat_pol", []) if row.get("Policy Hash Key")
    }
    customer_sat_by_hk = {
        row.get("Customer Hash Key"): row for row in ctx.get("sat_cus", []) if row.get("Customer Hash Key")
    }
    policy_to_product_code = {
        policy_hk: ctx.get("policy_to_product_id", {}).get(policy_hk, "")
        for policy_hk in policy_hks
    }
    quote_hks = [row.get("Quote Hash Key") for row in ctx.get("hub_quo_rows", []) if row.get("Quote Hash Key")]
    policy_to_quote_hk = dict(ctx.get("policy_to_quote_map", {}))
    quote_to_policy_hk = {quote_hk: policy_hk for policy_hk, quote_hk in policy_to_quote_hk.items()}
    fact_policies = _read_example("FactPolicy.csv")
    fact_claim_rows = [
        row for row in fact_policies
        if str(row.get("ClaimID", "") or "").strip()
    ]
    fact_claim_by_id = {
        str(row.get("ClaimID", "") or "").strip(): row
        for row in fact_claim_rows
        if str(row.get("ClaimID", "") or "").strip()
    }
    claim_financial_settings = _claim_financial_settings(cfg)
    person_to_customer_hk = {}
    for person_hk, customer_hk_or_hks in ctx.get("person_to_customer", {}).items():
        if isinstance(customer_hk_or_hks, list):
            if customer_hk_or_hks:
                person_to_customer_hk[person_hk] = customer_hk_or_hks[0]
        elif customer_hk_or_hks:
            person_to_customer_hk[person_hk] = customer_hk_or_hks
    policy_to_person_hk = dict(ctx.get("policy_to_person_map", {}))
    if not policy_to_person_hk:
        for person_hk, assigned_policy_hks in ctx.get("policy_person_map", {}).items():
            for assigned_policy_hk in assigned_policy_hks:
                policy_to_person_hk[assigned_policy_hk] = person_hk
    policy_to_customer_hk = {
        policy_hk: person_to_customer_hk.get(policy_to_person_hk.get(policy_hk))
        for policy_hk in policy_hks
    }
    customer_to_policy_hks: dict[str, list[str]] = {}
    for policy_hk, customer_hk in policy_to_customer_hk.items():
        if customer_hk:
            customer_to_policy_hks.setdefault(customer_hk, []).append(policy_hk)
    active_policy_hks = [
        policy_hk
        for policy_hk, row in policy_sat_by_hk.items()
        if row.get("Policy Status") == "ACTIVE"
    ]
    active_customer_hks = [
        customer_hk
        for customer_hk, row in customer_sat_by_hk.items()
        if row.get("Customer Status") in BASE_CUSTOMER_STATUSES
    ]

    agent_policy_persons = sorted({
        policy_to_person_hk[policy_hk]
        for policy_hk in policy_hks
        if policy_sat_by_hk.get(policy_hk, {}).get("Sales Channel") == "AGENT" and policy_to_person_hk.get(policy_hk)
    })

    broker_examples = _read_example("DimBroker.csv")
    broker_target_count = int(settings["broker_count"])
    if agent_policy_persons and broker_target_count <= 0:
        broker_target_count = 1
    broker_count = min(broker_target_count, len(broker_examples) or broker_target_count)
    for idx in range(broker_count):
        sample = _sample(broker_examples, idx)
        agent_id = sample.get("AgentID") or f"BRK{idx + 1:05d}"
        broker_hk = md5_hasher(agent_id)
        tables["hub_broker"].append({
            "broker_hash_key": broker_hk,
            "load_date": hub_date,
            "record_source": RS,
            "agent_id": agent_id,
        })
        tables["sat_broker"].append({
            "broker_hash_key": broker_hk,
            "load_date": sat_date,
            "agent_name": sample.get("AgentName", ""),
            "agent_type": sample.get("AgentType", ""),
            "agent_license_number": sample.get("LicenseNumber", ""),
            "agent_net_promoter_score": _float(sample.get("NPS")),
            "agent_commission_percentage": _float(sample.get("Commission percentage")),
            "agent_status": "ACTIVE",
        })

    broker_hks = [row["broker_hash_key"] for row in tables["hub_broker"]]
    for idx, person_hk in enumerate(agent_policy_persons):
        if broker_hks:
            broker_hk = broker_hks[idx % len(broker_hks)]
            tables["link_broker_person"].append(_link_row(
                "link_broker_person",
                "person_hash_key",
                person_hk,
                "broker_hash_key",
                broker_hk,
                link_date,
            ))
            quote_hks_for_person = ctx.get("person_to_quote", {}).get(person_hk, [])
            for policy_hk in ctx.get("policy_person_map", {}).get(person_hk, []):
                tables["link_policy_broker"].append(_link_row(
                    "link_policy_broker",
                    "broker_hash_key",
                    broker_hk,
                    "policy_hash_key",
                    policy_hk,
                    link_date,
                ))
            for quote_hk in quote_hks_for_person:
                tables["link_quote_broker"].append(_link_row(
                    "link_quote_broker",
                    "quote_hash_key",
                    quote_hk,
                    "broker_hash_key",
                    broker_hk,
                    link_date,
                ))

    campaign_examples = _read_example("DimCampaign.csv")
    quote_examples = _read_example("FactQuote.csv")
    campaign_count = min(int(settings["campaign_count"]), len(campaign_examples) or int(settings["campaign_count"]))
    for idx in range(campaign_count):
        sample = _sample(campaign_examples, idx)
        quote_sample = _sample(quote_examples, idx)
        campaign_id = sample.get("campaign_id") or f"CAM{idx + 1:03d}"
        campaign_hk = md5_hasher(campaign_id)
        tables["hub_campaign"].append({
            "campaign_hash_key": campaign_hk,
            "load_date": hub_date,
            "record_source": RS,
            "campaign_id": campaign_id,
        })
        tables["sat_campaign"].append({
            "campaign_hash_key": campaign_hk,
            "load_date": sat_date,
            "campaign_name": sample.get("campaign_name", ""),
            "campaign_type": sample.get("Campaign_Type", ""),
            "campaign_start_date": _timestamp(sample.get("start_date"), "00:00:00"),
            "campaign_end_date": _timestamp(sample.get("end_date"), "23:59:59"),
            "campaign_status": sample.get("Status", ""),
            "campaign_budget": _int(sample.get("Budget")),
            "campaign_target_audience": sample.get("Target_Audience", ""),
            "campaign_marketing_source": sample.get("marketing_source", ""),
            "campaign_owner_department": sample.get("Owner_Department", ""),
            "campaign_country": sample.get("Country", ""),
            "campaign_conversion_goal": sample.get("Conversion_Goal", ""),
            "number_of_impressions": _int(quote_sample.get("Impressions")),
            "number_of_clicks": _int(quote_sample.get("Clicks")),
            "is_active": _yn(quote_sample.get("Active")),
            "number_of_visits": _int(quote_sample.get("Visits")),
            "number_of_policy_purchases": _int(quote_sample.get("PolicyPurchases")),
            "number_of_emails_sent": _int(quote_sample.get("EmailsSent")),
            "number_of_email_bounced": _int(quote_sample.get("Bounces")),
            "number_of_emails_delivered": _int(quote_sample.get("EmailsDelivered")),
            "number_of_emails_opened": _int(quote_sample.get("EmailsOpened")),
            "click_through_rate": _float(quote_sample.get("CTR")),
            "spend_amount": _float(quote_sample.get("Spend")),
            "incremental_revenue": _int(quote_sample.get("IncrementalRevenue")),
            "survey_wave": quote_sample.get("SurveyWave", ""),
            "total_number_of_respondents": _int(quote_sample.get("TotalRespondents")),
            "number_of_respondents_aware": _int(quote_sample.get("RespondentsAware")),
            "number_of_promoters": _int(quote_sample.get("Promoters")),
            "number_of_passives": _int(quote_sample.get("Passives")),
            "number_of_detractors": _int(quote_sample.get("Detractors")),
            "number_of_followers": _int(quote_sample.get("Followers")),
            "number_of_likes": _int(quote_sample.get("Likes")),
            "number_of_comments": _int(quote_sample.get("Comments")),
            "number_of_shares": _int(quote_sample.get("Shares")),
            "number_of_brand_mentions": _int(quote_sample.get("BrandMentions")),
            "number_of_category_mentions": _int(quote_sample.get("CategoryMentions")),
        })

    campaign_hks = [row["campaign_hash_key"] for row in tables["hub_campaign"]]
    lead_persons = sorted(ctx.get("person_to_lead", {}).keys())
    for idx, person_hk in enumerate(lead_persons):
        if campaign_hks:
            tables["link_person_campaign"].append(_link_row(
                "link_person_campaign",
                "campaign_hash_key",
                campaign_hks[idx % len(campaign_hks)],
                "person_hash_key",
                person_hk,
                link_date,
            ))

    for idx, channel_name in enumerate(BASE_POLICY_CHANNELS, start=1):
        channel_id = f"CHN{idx:03d}"
        channel_hk = md5_hasher(channel_id)
        tables["hub_channel"].append({
            "channel_hash_key": channel_hk,
            "load_date": hub_date,
            "record_source": RS,
            "channel_id": channel_id,
        })
        tables["sat_channel"].append({
            "channel_hash_key": channel_hk,
            "load_date": sat_date,
            "channel_name": channel_name,
            "channel_type": CHANNEL_TYPE_BY_NAME.get(channel_name, ""),
        })

    channel_hk_by_name = {
        row["channel_name"]: row["channel_hash_key"] for row in tables["sat_channel"]
    }
    for policy_hk in policy_hks:
        sales_channel = policy_sat_by_hk.get(policy_hk, {}).get("Sales Channel", "")
        channel_hk = channel_hk_by_name.get(sales_channel)
        if channel_hk:
            tables["link_policy_channel"].append(_link_row(
                "link_policy_channel",
                "channel_hash_key",
                channel_hk,
                "policy_hash_key",
                policy_hk,
                link_date,
            ))
        quote_hk = policy_to_quote_hk.get(policy_hk)
        if quote_hk:
            tables["link_policy_quote"].append(_link_row(
                "link_policy_quote",
                "quote_hash_key",
                quote_hk,
                "policy_hash_key",
                policy_hk,
                link_date,
            ))

    for idx, quote_hk in enumerate(quote_hks):
        policy_hk = quote_to_policy_hk.get(quote_hk)
        sales_channel = policy_sat_by_hk.get(policy_hk, {}).get("Sales Channel", "") if policy_hk else ""
        if not sales_channel:
            sales_channel = BASE_POLICY_CHANNELS[idx % len(BASE_POLICY_CHANNELS)]
        channel_hk = channel_hk_by_name.get(sales_channel)
        if channel_hk:
            tables["link_quote_channel"].append(_link_row(
                "link_quote_channel",
                "quote_hash_key",
                quote_hk,
                "channel_hash_key",
                channel_hk,
                link_date,
            ))

    enhanced_policy_by_hk = {
        row.get("policy_hash_key"): row
        for row in tables.get("sat_policy", [])
        if row.get("policy_hash_key")
    }

    insured_idx = 0
    for policy_hk in policy_hks:
        product_code = policy_to_product_code.get(policy_hk, "")
        policy_row = policy_sat_by_hk.get(policy_hk, {})
        enhanced_policy_row = enhanced_policy_by_hk.get(policy_hk, {})
        asset_pairs = []
        motor_hk = ctx.get("policy_to_motor", {}).get(policy_hk)
        home_hk = ctx.get("policy_to_home", {}).get(policy_hk)
        if motor_hk:
            asset_pairs.append(("MOTOR", motor_hk, "motor"))
        if home_hk:
            asset_pairs.append(("HOME", home_hk, "home"))
        if not asset_pairs:
            continue
        for object_type, asset_hk, asset_kind in asset_pairs:
            insured_idx += 1
            insured_object_id = f"INS_OBJ_{insured_idx:07d}"
            insured_object_hk = md5_hasher(insured_object_id)
            tables["hub_insured_object"].append({
                "insured_object_hash_key": insured_object_hk,
                "load_date": hub_date,
                "record_source": RS,
                "insured_object_id": insured_object_id,
            })
            tables["sat_insured_object"].append({
                "insured_object_hash_key": insured_object_hk,
                "load_date": sat_date,
                "insured_object_type": object_type,
                "insured_object_sub_type": product_code,
                "insured_object_description": f"{object_type} asset for policy",
                "insured_object_current_status": policy_row.get("Policy Status", ""),
                "insured_value": _insured_value(enhanced_policy_row, product_code, asset_kind),
                "currency_code": "GBP",
                "insured_object_start_date": _timestamp(policy_row.get("Policy Start Date")),
                "insured_object_end_date": _timestamp(policy_row.get("Policy End Date")),
            })
            tables["link_policy_insured_object"].append(_link_row(
                "link_policy_insured_object",
                "insured_object_hash_key",
                insured_object_hk,
                "policy_hash_key",
                policy_hk,
                link_date,
            ))
            if asset_kind == "motor":
                tables["link_insured_object_motor"].append(_link_row_with_pk(
                    "insured_object_motor_hash_key",
                    "insured_object_hash_key",
                    insured_object_hk,
                    "motor_hash_key",
                    asset_hk,
                    link_date,
                ))
            else:
                tables["link_insured_object_home"].append(_link_row(
                    "link_insured_object_home",
                    "insured_object_hash_key",
                    insured_object_hk,
                    "home_hash_key",
                    asset_hk,
                    link_date,
                ))

    claim_examples = _read_example("DimClaim.csv")
    claim_pool = active_policy_hks or policy_hks
    claim_count = min(len(claim_pool), max(1, int(len(claim_pool) * float(settings["claim_policy_rate"])))) if claim_pool else 0
    selected_policies = rng.sample(claim_pool, claim_count) if claim_count else []
    for idx, policy_hk in enumerate(selected_policies):
        sample = _sample(claim_examples, idx)
        source_claim_id = str(sample.get("ClaimID", "") or "").strip()
        source_priority = claim_financial_settings.get(
            "source_priority",
            ["fact_policy_claim_rows", "derived_from_policy_sum_insured"],
        )
        fact = {}
        if source_priority and source_priority[0] == "fact_policy_claim_rows":
            fact = fact_claim_by_id.get(source_claim_id) or _sample(fact_claim_rows, idx)
        claim_id = f"ENH_CLM_{idx + 1:07d}"
        claim_hk = md5_hasher(claim_id)
        policy_row = policy_sat_by_hk.get(policy_hk, {})
        enhanced_policy_row = enhanced_policy_by_hk.get(policy_hk, {})
        product_code = policy_to_product_code.get(policy_hk, "")
        policy_start = _date_obj(policy_row.get("Policy Start Date"))
        policy_end = _date_obj(policy_row.get("Policy End Date"))
        reported = _date_obj(sample.get("ClaimReportedDate")) or policy_start
        if policy_start and reported and reported < policy_start:
            reported = policy_start
        if policy_end and reported and reported > policy_end:
            reported = policy_end
        settlement = _date_obj(sample.get("ClaimSettlementDate"))
        if reported and settlement and settlement < reported:
            settlement = reported
        recovery_happened = _yn(sample.get("Recovery_Happened"))
        first_recovery = _date_obj(sample.get("First_Recovery_Date"))
        last_recovery = _date_obj(sample.get("Last_Recovery_Date"))
        if recovery_happened == "Y":
            first_recovery = first_recovery or reported or settlement
            if reported and first_recovery and first_recovery < reported:
                first_recovery = reported
            last_recovery = last_recovery or first_recovery or settlement
            if first_recovery and last_recovery and last_recovery < first_recovery:
                last_recovery = first_recovery
            first_recovery_ts = _timestamp_from_date(first_recovery, "00:00:00")
            last_recovery_ts = _timestamp_from_date(last_recovery, "23:59:59")
        else:
            first_recovery_ts = RECOVERY_SENTINEL_TIMESTAMP
            last_recovery_ts = RECOVERY_SENTINEL_TIMESTAMP
        claim_financials = _derive_claim_financials(
            fact,
            enhanced_policy_row,
            sample,
            claim_financial_settings,
            rng,
        )
        tables["hub_claim"].append({
            "claim_hash_key": claim_hk,
            "load_date": hub_date,
            "record_source": RS,
            "claim_id": claim_id,
        })
        tables["sat_claim"].append({
            "claim_hash_key": claim_hk,
            "load_date": sat_date,
            "claim_number": sample.get("ClaimNo", claim_id),
            "claim_type": sample.get("ClaimType", ""),
            "claim_status": sample.get("ClaimStatus", ""),
            "claim_reason": sample.get("ClaimReason", ""),
            "claim_channel": policy_row.get("Sales Channel", ""),
            "claim_handler": sample.get("ClaimHandler", ""),
            "claim_reported_date": _timestamp_from_date(reported, "00:00:00"),
            "claim_settlement_date": _timestamp_from_date(settlement, "23:59:59"),
            "claim_product": claim_product_for_code(product_code),
            "is_claim_suspicious": sample.get("SuspiciousFlag", ""),
            "is_claim_fraud": sample.get("FraudFlag", ""),
            "claim_fraud_status": sample.get("FraudStatus", ""),
            "claim_fraud_type": sample.get("FraudType", ""),
            "claim_fraud_detection_method": sample.get("FraudDetectionMethod", ""),
            "is_litigation": sample.get("LitigationIndicator", ""),
            "litigation_reason": sample.get("LitigationReason", ""),
            "litigation_start_date": _timestamp(sample.get("LitigationStartDate"), "00:00:00"),
            "litigation_end_date": _timestamp(sample.get("LitigationEndDate"), "23:59:59"),
            "litigation_outcome": sample.get("LitigationOutcome", ""),
            "litigation_duration_days": _int(sample.get("LitigationDurationDays")),
            "claim_fraud_detection_time_in_days": _int(sample.get("FraudDetectionTime_Days")),
            "is_recovery_opportunity": sample.get("RecoveryOpportunityFlag", ""),
            "recovery_priority_score": _int(sample.get("RecoveryPriorityScore"), default="0"),
            "recovery_category": sample.get("RecoveryCategory", ""),
            "recovery_source": sample.get("RecoverySource", ""),
            "first_recovery_date": first_recovery_ts,
            "last_recovery_date": last_recovery_ts,
            "is_recovery_happened": recovery_happened,
            "days_to_first_recovery": _int(sample.get("Days_to_First_Recovery"), default="0"),
            "days_to_last_recovery": _int(sample.get("Days_to_Last_Recovery"), default="0"),
            "avg_days_to_close_claim": _int(sample.get("Avg. Days CloseClaim")),
            "claim_fraud_outcome": sample.get("Fraud_Outcome", ""),
            "recovery_type": sample.get("Recovery type", ""),
            "recovery_band": sample.get("Recovery Band", ""),
            "third_party_involved": sample.get("TPI", ""),
            "third_party_involved_overall_score": _float(sample.get("TPIOverallScore")),
            "solicitor": sample.get("Solicitor", ""),
            **claim_financials,
        })
        tables["link_claim_policy"].append(_link_row(
            "link_claim_policy",
            "policy_hash_key",
            policy_hk,
            "claim_hash_key",
            claim_hk,
            link_date,
        ))

    complaint_examples = _read_example("FactComplaints.csv") or _read_example("DimComplaints.csv")
    category_examples = _read_example("DimInsCategory.csv")
    complaint_pool = active_customer_hks or customer_hks
    complaint_count = min(len(complaint_pool), max(1, int(len(complaint_pool) * float(settings["complaint_customer_rate"])))) if complaint_pool else 0
    selected_customers = rng.sample(complaint_pool, complaint_count) if complaint_count else []
    complaint_hks = []
    for idx, customer_hk in enumerate(selected_customers):
        sample = _sample(complaint_examples, idx)
        category = _sample(category_examples, idx)
        complaint_id = sample.get("Complaint ID") or f"CMP{idx + 1:07d}"
        complaint_hk = md5_hasher(complaint_id)
        customer_row = customer_sat_by_hk.get(customer_hk, {})
        customer_since = _date_obj(customer_row.get("Customer Since"))
        linked_policy_hks = customer_to_policy_hks.get(customer_hk, [])
        preferred_policy_hk = next((hk for hk in linked_policy_hks if hk in active_policy_hks), None)
        if not preferred_policy_hk and linked_policy_hks:
            preferred_policy_hk = linked_policy_hks[0]
        policy_row = policy_sat_by_hk.get(preferred_policy_hk, {})
        product_code = policy_to_product_code.get(preferred_policy_hk, "")
        complaint_date = _date_obj(sample.get("Complaint Date")) or customer_since
        if customer_since and complaint_date and complaint_date < customer_since:
            complaint_date = customer_since
        acknowledgement_date = _date_obj(sample.get("Acknowledgement Date"))
        if complaint_date and acknowledgement_date and acknowledgement_date < complaint_date:
            acknowledgement_date = complaint_date
        resolved_date = _date_obj(sample.get("Resolved Date"))
        min_resolved = acknowledgement_date or complaint_date
        if min_resolved and resolved_date and resolved_date < min_resolved:
            resolved_date = min_resolved
        tables["hub_complaint"].append({
            "complaint_hash_key": complaint_hk,
            "load_date": hub_date,
            "record_source": RS,
            "complaint_id": complaint_id,
        })
        tables["sat_complaint"].append({
            "complaint_hash_key": complaint_hk,
            "load_date": sat_date,
            "complaint_date": _timestamp_from_date(complaint_date, "00:00:00"),
            "complaint_acknowledgement_date": _timestamp_from_date(acknowledgement_date, "12:00:00"),
            "complaint_resolved_date": _timestamp_from_date(resolved_date, "23:59:59"),
            "complaint_upheld_status": sample.get("Upheld Status", ""),
            "is_financial_ombudsman_service_referral": sample.get("FOS Referral", ""),
            "complaint_driver": sample.get("Complaint Driver", ""),
            "complaint_channel": policy_row.get("Sales Channel", ""),
            "compensation_amount": _int(sample.get("Compensation Amount")),
            "insurance_category": insurance_category_for_code(product_code) or category.get("Insurance Category", ""),
            "complaint_status": sample.get("Compaint Status", ""),
        })
        complaint_hks.append(complaint_hk)
        if preferred_policy_hk:
            tables["link_complaint_policy"].append(_link_row(
                "link_complaint_policy",
                "complaint_hash_key",
                complaint_hk,
                "policy_hash_key",
                preferred_policy_hk,
                link_date,
            ))

    override_examples = _read_example("DimOverride.csv")
    override_pool = active_policy_hks or policy_hks
    override_count = min(len(override_pool), max(1, int(len(override_pool) * float(settings["override_policy_rate"])))) if override_pool else 0
    selected_override_policies = rng.sample(override_pool, override_count) if override_count else []
    sat_policy_by_hk = {
        row.get("policy_hash_key"): row for row in tables.get("sat_policy", []) if row.get("policy_hash_key")
    }
    original_override_commission = {
        policy_hk: row.get("override_commission", "")
        for policy_hk, row in sat_policy_by_hk.items()
    }
    for row in sat_policy_by_hk.values():
        row["override_commission"] = ""
    for idx, policy_hk in enumerate(selected_override_policies):
        sample = _sample(override_examples, idx)
        base_override_id = (sample.get("over Ride ID ") or "OVR").strip()
        override_id = f"{base_override_id}_{idx + 1:06d}"
        override_hk = md5_hasher(override_id)
        tables["hub_override"].append({
            "override_hash_key": override_hk,
            "load_date": hub_date,
            "record_source": RS,
            "override_id": override_id,
        })
        tables["sat_override"].append({
            "override_hash_key": override_hk,
            "load_date": sat_date,
            "override_reason": sample.get("over ride reason", ""),
        })
        if policy_hk in sat_policy_by_hk:
            sat_policy_by_hk[policy_hk]["override_commission"] = (
                original_override_commission.get(policy_hk)
                or _int(sample.get("Override Comission"))
                or "0"
            )
        tables["link_policy_override"].append(_link_row(
            "link_policy_override",
            "override_hash_key",
            override_hk,
            "policy_hash_key",
            policy_hk,
            link_date,
        ))

    regulation_examples = _read_example("DimRegulations.csv")
    regulation_count = min(int(settings["regulation_count"]), len(regulation_examples) or int(settings["regulation_count"]))
    used_regulation_ids = set()
    for idx in range(regulation_count):
        sample = _sample(regulation_examples, idx)
        base_regulation_id = sample.get("RegulationID") or f"REG{idx + 1:05d}"
        regulation_id = base_regulation_id
        if regulation_id in used_regulation_ids:
            regulation_id = f"{base_regulation_id}_{idx + 1:05d}"
        used_regulation_ids.add(regulation_id)
        regulation_hk = md5_hasher(regulation_id)
        tables["hub_regulation"].append({
            "regulation_hash_key": regulation_hk,
            "load_date": hub_date,
            "record_source": RS,
            "regulation_id": regulation_id,
        })
        tables["sat_regulation"].append({
            "regulation_hash_key": regulation_hk,
            "load_date": sat_date,
            "regulation_number": sample.get("Regulation Number", ""),
            "regulation_name": sample.get("RegulationName", ""),
            "regulation_department": sample.get("Department", ""),
            "regulation_region": sample.get("Region", ""),
            "regulation_risk_level": sample.get("RiskLevel", ""),
            "regulation_compliance_status": sample.get("ComplianceStatus", ""),
            "regulation_date_raised": _timestamp(sample.get("DateRaised"), "00:00:00"),
            "regulation_date_closed": _timestamp(sample.get("DateClosed"), "23:59:59"),
            "regulation_owner": sample.get("Owner", ""),
            "regulation_deadline_date": _timestamp(sample.get("Deadline_Date"), "23:59:59"),
            "is_regulation_on_time": _yn(sample.get("On_Time_Flag")),
        })
        if complaint_hks:
            tables["link_complaint_regulation"].append(_link_row(
                "link_complaint_regulation",
                "regulation_hash_key",
                regulation_hk,
                "complaint_hash_key",
                complaint_hks[idx % len(complaint_hks)],
                link_date,
            ))


def build_enhanced_synthetic(ctx: dict, output_dir: str, cfg: dict | None = None) -> str:
    ddl = parse_enhanced_ddl()
    schemas = ddl["tables"]
    tables = {table_name: [] for table_name in schemas}
    tables.update(_build_base_tables(ctx))
    tables = {table_name: tables.get(table_name, []) for table_name in schemas}

    _augment_base_satellites(tables)
    _add_enhanced_entities(tables, ctx, cfg)
    _normalize_string_boolean_columns(tables)
    _normalize_blank_numeric_columns(tables, ddl["column_types"])

    os.makedirs(output_dir, exist_ok=True)
    for table_name, columns in schemas.items():
        rows = [_ordered(row, columns) for row in tables.get(table_name, [])]
        write_csv(output_dir, f"{table_name}.csv", rows, fieldnames=columns)

    return output_dir
