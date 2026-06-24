from __future__ import annotations

import csv
import math
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
MLOPS_DDL_PATH = ROOT / "mlops" / "Enhanced_Customer360_DataVault_Model_DDL.sql"
RS = "CRM"
RECOVERY_SENTINEL_TIMESTAMP = "1900-01-01T00:00:00"
ONBOARDING_FEEDBACK_CONTEXTS = [
    "online quote journey",
    "mobile app quote journey",
    "branch assisted quote",
    "broker assisted purchase",
    "renewal invitation review",
    "policy document review",
    "cover comparison step",
    "excess selection step",
    "payment setup step",
    "identity verification step",
    "vehicle details capture",
    "address validation step",
    "document delivery step",
    "email confirmation step",
    "call centre handoff",
    "chat support handoff",
    "first login setup",
    "paperless consent setup",
    "direct debit setup",
    "optional add-on review",
    "final quote acceptance",
    "welcome pack review",
    "policy start confirmation",
    "coverage limit review",
    "customer profile setup",
]


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


def _int_value(value, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(float(str(value).replace(",", "").strip()))
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


def _weighted_value_by_index(weights: dict | None, defaults: dict[str, int], idx: int) -> str:
    active = weights or defaults
    values = []
    for key, weight in active.items():
        values.extend([str(key)] * max(0, int(weight)))
    if not values:
        values = [str(key) for key in defaults]
    return values[(idx * 37) % len(values)]


def _nps_profile(idx: int, cfg: dict | None = None) -> tuple[str, str]:
    value_weights = ((cfg or {}).get("nps_settings") or {}).get("nps_score_value_weights") or {}
    bucket = idx % 100
    if bucket < 30:
        score = _weighted_value_by_index(
            value_weights.get("DETRACTOR"),
            {"0": 18, "1": 18, "2": 9, "3": 8, "4": 9, "5": 19, "6": 19},
            idx,
        )
        return str(score), "DETRACTORS"
    if bucket < 65:
        score = _weighted_value_by_index(value_weights.get("PASSIVE"), {"7": 52, "8": 48}, idx)
        return str(score), "PASSIVE"
    return str(_weighted_value_by_index(value_weights.get("PROMOTER"), {"9": 48, "10": 52}, idx)), "PROMOTERS"


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
        "is_fault_claim",
    },
    "sat_complaint": {"is_financial_ombudsman_service_referral"},
    "sat_home": {"is_existing_home_customer"},
    "sat_motor": {"is_existing_motor_customer"},
    "sat_person": {"is_lead"},
    "sat_policy": {
        "fraud_flag",
        "is_policy_renewal",
        "is_auto_renew_enabled",
        "is_direct_debit_cancellation",
        "is_installment_default",
        "is_renewal_escalation",
    },
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


def _policy_churn_flag(row: dict) -> bool:
    return str(row.get("policy_status") or "").strip().upper() in {"CANCELLED", "LAPSED"}


def _mlops_band_specs(cfg: dict | None, key: str, defaults: list[tuple]) -> list[tuple]:
    if not cfg:
        return defaults
    churn_settings = cfg.get("churn_settings", {})
    ranges = churn_settings.get("mlops_churn_expected_ranges", {}).get(key, {})
    weights = churn_settings.get("mlops_churn_band_weights", {}).get(key, {})
    if not ranges:
        return defaults
    default_by_band = {band: (target, weight, min_rate, max_rate) for band, target, weight, min_rate, max_rate in defaults}
    specs = []
    for band, limits in ranges.items():
        if not isinstance(limits, list) or len(limits) != 2:
            continue
        default = default_by_band.get(band, ((float(limits[0]) + float(limits[1])) / 2, 1, limits[0], limits[1]))
        target = (float(limits[0]) + float(limits[1])) / 2
        weight = float(weights.get(band, default[1]))
        specs.append((band, target, weight, float(limits[0]), float(limits[1])))
    return specs or defaults


def _policy_payment_profile(row: dict) -> dict:
    churned = _policy_churn_flag(row)
    status = str(row.get("policy_status") or "").strip().upper()
    cycle = _int_value(row.get("policy_cycle"), 0)
    active_claims = _int_value(row.get("number_of_active_claim"), 0)
    previous_claims = _int_value(row.get("number_of_previous_claim"), 0)
    declined_claims = _int_value(row.get("declined_claims"), 0)
    total_claims = active_claims + previous_claims + declined_claims
    risk_score = _float_value(row.get("policy_risk_score"), 0.0)
    if churned:
        payment_method = random.choices(["BANK_TRANSFER", "DIRECT_DEBIT", "CARD"], weights=[15, 35, 50], k=1)[0]
    else:
        payment_method = random.choices(["BANK_TRANSFER", "DIRECT_DEBIT", "CARD"], weights=[45, 40, 15], k=1)[0]
    is_direct_debit = payment_method == "DIRECT_DEBIT"
    if churned:
        missed_payment_count = random.choices([0, 1, 2, 3, 4], weights=[15, 25, 30, 22, 8], k=1)[0]
    elif risk_score >= 70 or total_claims >= 3:
        missed_payment_count = random.choices([0, 1, 2, 3], weights=[60, 28, 10, 2], k=1)[0]
    else:
        missed_payment_count = random.choices([0, 1, 2, 3], weights=[72, 20, 6, 2], k=1)[0]
    auto_renew_enabled = "Y" if random.random() < (0.08 if churned else 0.58) else "N"
    dd_cancel_probability = 0.60 if churned else 0.08
    direct_debit_cancel = "Y" if is_direct_debit and missed_payment_count > 0 and random.random() < dd_cancel_probability else "N"
    installment_default = "Y" if missed_payment_count >= 2 else "N"
    ncd_years = max(0, min(cycle, 9) - min(total_claims, 3))
    if not churned and cycle >= 5 and total_claims == 0 and random.random() < 0.35:
        ncd_years = random.choice([9, 10, 11])
    elif churned and ncd_years >= 5 and random.random() < 0.45:
        ncd_years = random.randint(2, 4)
    if churned:
        loyalty_discount_usage = random.choices(["RETAINED", "NOT_APPLIED", "REMOVED"], weights=[15, 35, 50], k=1)[0]
    else:
        loyalty_discount_usage = random.choices(["RETAINED", "NOT_APPLIED", "REMOVED"], weights=[55, 40, 5], k=1)[0]
    if status == "CANCELLED" and random.random() < 0.20:
        loyalty_discount_usage = "REMOVED"
    return {
        "is_auto_renew_enabled": auto_renew_enabled,
        "no_claims_discount_years": str(ncd_years),
        "payment_method": payment_method,
        "is_direct_debit_cancellation": direct_debit_cancel,
        "missed_payment_count": str(missed_payment_count),
        "loyalty_discount_usage": loyalty_discount_usage,
        "is_installment_default": installment_default,
    }


def _driver_experience_from_nat(row: dict) -> int:
    birth = _date_obj(row.get("birth_date"))
    load = _date_obj(row.get("load_date")) or date.today()
    if not birth:
        return 0
    age = load.year - birth.year - ((load.month, load.day) < (birth.month, birth.day))
    return max(0, age - 17)


def _claim_satisfaction_score(row: dict) -> str:
    score = 8
    status = str(row.get("claim_status") or "").upper()
    if status in {"OPEN", "PENDING"}:
        score -= 1
    if _yn(row.get("is_claim_fraud")) == "Y" or _yn(row.get("is_claim_suspicious")) == "Y":
        score -= 3
    if _yn(row.get("is_litigation")) == "Y":
        score -= 2
    if _yn(row.get("is_fault_claim")) == "Y":
        score -= 1
    amount = _int_value(row.get("claim_amount"), 0)
    if amount >= 13000:
        score -= 1
    if _int_value(row.get("outstanding_reserve"), 0) > 0:
        score -= 1
    return str(max(1, min(10, score)))


def _feedback_from_score(score: int) -> str:
    if score >= 4:
        return "POSITIVE"
    if score >= 2:
        return "NEUTRAL"
    return "NEGATIVE"


def _feedback_text_from_score(score: int, cfg: dict | None, section: str, seed_value: str = "") -> str:
    defaults = {
        "onboarding_feedback_text": {
            "POSITIVE": [
                "Comprehensive cover for the price for appropriate policy",
                "Flexible excess options available",
                "Policy documents easy to understand",
                "Good cover options for the premium",
            ],
            "NEUTRAL": [
                "Cover options mostly met expectations",
                "Onboarding completed with minor clarifications",
                "Price and benefits were acceptable",
            ],
            "NEGATIVE": [
                "Policy exclusions not clear",
                "Courtesy car not in standard cover",
                "Additional cover options were difficult to compare",
                "Price felt high for the selected cover",
            ],
        }
    }
    sentiment = _feedback_from_score(score)
    configured = (((cfg or {}).get("nps_settings") or {}).get(section) or defaults.get(section) or {})
    phrases = configured.get(sentiment) or defaults.get(section, {}).get(sentiment) or [sentiment]
    seed = sum(ord(char) for char in str(seed_value)) + score
    return str(phrases[seed % len(phrases)])


def _configured_feedback_phrases(cfg: dict | None, section: str) -> dict[str, list[str]]:
    defaults = {
        "onboarding_feedback_text": {
            "POSITIVE": [
                "Comprehensive cover for the price for appropriate policy",
                "Flexible excess options available",
                "Policy documents easy to understand",
                "Good cover options for the premium",
            ],
            "NEUTRAL": [
                "Cover options mostly met expectations",
                "Onboarding completed with minor clarifications",
                "Price and benefits were acceptable",
            ],
            "NEGATIVE": [
                "Policy exclusions not clear",
                "Courtesy car not in standard cover",
                "Additional cover options were difficult to compare",
                "Price felt high for the selected cover",
            ],
        }
    }
    configured = (((cfg or {}).get("nps_settings") or {}).get(section) or {})
    merged = defaults.get(section, {}).copy()
    for sentiment, phrases in configured.items():
        valid_phrases = [str(phrase) for phrase in phrases if str(phrase).strip()] if isinstance(phrases, list) else []
        if valid_phrases:
            merged[str(sentiment).upper()] = valid_phrases
    return merged


def _expanded_feedback_phrases(cfg: dict | None, section: str, count_section: str) -> dict[str, list[str]]:
    phrase_bank = _configured_feedback_phrases(cfg, section)
    target_counts = (((cfg or {}).get("nps_settings") or {}).get(count_section) or {})
    expanded: dict[str, list[str]] = {}
    for sentiment, phrases in phrase_bank.items():
        target = int(target_counts.get(sentiment, len(phrases)) or len(phrases))
        unique_values = list(dict.fromkeys(str(phrase).strip() for phrase in phrases if str(phrase).strip()))
        if not unique_values:
            expanded[sentiment] = [sentiment]
            continue
        candidate_values = unique_values.copy()
        for context in ONBOARDING_FEEDBACK_CONTEXTS:
            for phrase in unique_values:
                candidate_values.append(f"{phrase} - {context}")
                if len(candidate_values) >= target:
                    break
            if len(candidate_values) >= target:
                break
        expanded[sentiment] = candidate_values[:target]
    return expanded


def _claim_feedback(row: dict) -> str:
    status = str(row.get("claim_status") or "").strip().upper()
    if status in {"OPEN", "PENDING"}:
        return ""
    score = _int_value(row.get("claim_satisfaction_score"), 0)
    if _yn(row.get("is_litigation")) == "Y" or _yn(row.get("is_claim_fraud")) == "Y":
        score = min(score, 3)
    return _feedback_from_score(5 if score >= 8 else 3 if score >= 5 else 1)


def _complaint_resolution_days(row: dict) -> int | None:
    opened = _date_obj(row.get("complaint_date"))
    resolved = _date_obj(row.get("complaint_resolved_date"))
    if not opened or not resolved:
        return None
    return max(0, (resolved - opened).days)


def _sync_complaint_status_with_dates(row: dict) -> None:
    """A resolved timestamp means the complaint is closed, regardless of source label."""
    if str(row.get("complaint_resolved_date") or "").strip():
        row["complaint_status"] = "Closed"


def _complaint_satisfaction_score(row: dict) -> str:
    status = str(row.get("complaint_status") or "").strip().upper()
    if status in {"OPEN", "PENDING"}:
        return ""
    days = _complaint_resolution_days(row)
    score = 5
    if days is None:
        score = 3
    elif days > 60:
        score = 1
    elif days > 30:
        score = 2
    elif days > 7:
        score = 3
    if _yn(row.get("is_financial_ombudsman_service_referral")) == "Y":
        score -= 2
    if str(row.get("complaint_upheld_status") or "").strip().upper() in {"UPHELD", "PARTIALLY_UPHELD"}:
        score -= 1
    return str(max(0, min(5, score)))


def _customer_onboarding_satisfaction_score(row: dict) -> str:
    nps = _int_value(row.get("nps_score"), 5)
    satisfaction = str(row.get("customer_satisfaction") or "").strip().upper()
    if nps >= 9 or satisfaction == "VERY_SATISFIED":
        score = 5
    elif nps >= 7 or satisfaction == "SATISFIED":
        score = 4
    elif nps >= 4 or satisfaction == "NEUTRAL":
        score = 3
    elif nps >= 2:
        score = 2
    else:
        score = 1
    if satisfaction == "DISSATISFIED":
        score = min(score, 1)
    return str(max(0, min(5, score)))


def _policy_renewal_satisfaction_score(row: dict) -> str:
    if str(row.get("is_policy_renewal") or "").strip().upper() != "Y":
        return ""
    score = 5
    current = _float_value(row.get("renewal_amount_current_period"), 0.0)
    next_amt = _float_value(row.get("renewal_amount_next_period"), current)
    increase = next_amt - current
    pct_increase = (increase / current) if current else 0.0
    if increase > 100 or pct_increase > 0.10:
        score -= 3
    elif increase > 50 or pct_increase > 0.05:
        score -= 2
    elif increase > 0:
        score -= 1
    if _policy_churn_flag(row):
        score -= 2
    if _yn(row.get("is_direct_debit_cancellation")) == "Y" or _yn(row.get("is_installment_default")) == "Y":
        score -= 1
    if _int_value(row.get("missed_payment_count"), 0) >= 2:
        score -= 1
    if str(row.get("loyalty_discount_usage") or "").upper() == "RETAINED":
        score += 1
    return str(max(0, min(5, score)))


def _weighted_band_from_config(cfg: dict | None, section: str, default_weights: dict[str, int]) -> str:
    weights = ((cfg or {}).get("nps_settings") or {}).get(section) or default_weights
    values = list(weights.keys())
    return random.choices(values, weights=[float(weights[value]) for value in values], k=1)[0]


def _policy_issue_tat_weights(nps_score: int | None, cfg: dict | None = None) -> dict[str, int]:
    configured = ((cfg or {}).get("nps_settings") or {}).get("policy_issuance_tat_by_nps_band") or {}
    defaults = {
        "PROMOTER": {"DAYS_0_2": 82, "DAYS_3_7": 14, "DAYS_GT_7": 4},
        "PASSIVE": {"DAYS_0_2": 68, "DAYS_3_7": 27, "DAYS_GT_7": 5},
        "DETRACTOR_LOW_NPS": {"DAYS_0_2": 50, "DAYS_3_7": 20, "DAYS_GT_7": 30},
        "DETRACTOR_OTHER": {"DAYS_0_2": 70, "DAYS_3_7": 18, "DAYS_GT_7": 12},
    }
    if nps_score is None:
        return ((cfg or {}).get("nps_settings") or {}).get("policy_issuance_tat_distribution") or {
            "DAYS_0_2": 70,
            "DAYS_3_7": 20,
            "DAYS_GT_7": 10,
        }
    if nps_score >= 9:
        return configured.get("PROMOTER") or defaults["PROMOTER"]
    if nps_score >= 7:
        return configured.get("PASSIVE") or defaults["PASSIVE"]
    if nps_score <= 4:
        return configured.get("DETRACTOR_LOW_NPS") or defaults["DETRACTOR_LOW_NPS"]
    return configured.get("DETRACTOR_OTHER") or defaults["DETRACTOR_OTHER"]


def _policy_issue_date_for_nps(policy_start_value, idx: int, cfg: dict | None = None, nps_score: int | None = None) -> str:
    policy_start = _date_obj(policy_start_value)
    if not policy_start:
        return _timestamp(policy_start_value)
    weights = _policy_issue_tat_weights(nps_score, cfg)
    ordered = ["DAYS_0_2", "DAYS_3_7", "DAYS_GT_7"]
    total = sum(max(0, int(weights.get(band, 0))) for band in ordered) or 100
    position = ((idx * 53) + 17) % total
    cutoff = 0
    selected = "DAYS_0_2"
    for band in ordered:
        cutoff += max(0, int(weights.get(band, 0)))
        if position < cutoff:
            selected = band
            break
    delay_ranges = {
        "DAYS_0_2": (0, 2),
        "DAYS_3_7": (3, 7),
        "DAYS_GT_7": (8, 14),
    }
    min_days, max_days = delay_ranges[selected]
    delay = min_days + (idx % (max_days - min_days + 1))
    return _timestamp_from_date(policy_start - timedelta(days=delay))


def _apply_nps_claim_escalation_distribution(tables_or_claim_rows, cfg: dict | None = None) -> None:
    if isinstance(tables_or_claim_rows, dict):
        claim_rows = tables_or_claim_rows.get("sat_claim", [])
        customer_rows = tables_or_claim_rows.get("sat_customer", [])
        policy_customer = {
            row.get("policy_hash_key"): row.get("customer_hash_key")
            for row in tables_or_claim_rows.get("link_policy_customer", [])
            if row.get("policy_hash_key") and row.get("customer_hash_key")
        }
        claim_customer = {}
        for row in tables_or_claim_rows.get("link_claim_policy", []):
            claim_hk = row.get("claim_hash_key")
            customer_hk = policy_customer.get(row.get("policy_hash_key"))
            if claim_hk and customer_hk:
                claim_customer[claim_hk] = customer_hk
        nps_by_customer = {
            row.get("customer_hash_key"): _int_value(row.get("nps_score"), 0)
            for row in customer_rows
            if row.get("customer_hash_key")
        }
    else:
        claim_rows = tables_or_claim_rows
        claim_customer = {}
        nps_by_customer = {}
    if not claim_rows:
        return
    distribution = (((cfg or {}).get("nps_settings") or {}).get("claim_escalation_distribution")) or {
        "NON_ESCALATED": 92,
        "ESCALATED": 8,
    }
    total_weight = sum(float(weight) for weight in distribution.values()) or 100.0
    escalated_weight = sum(
        float(weight)
        for value, weight in distribution.items()
        if str(value).upper() in {"ESCALATED", "Y", "YES", "TRUE"}
    )
    escalated_target = round(len(claim_rows) * escalated_weight / total_weight)

    for row in claim_rows:
        row["is_litigation"] = "N"

    ranked = []
    for idx, row in enumerate(claim_rows):
        claim_hk = row.get("claim_hash_key")
        customer_hk = claim_customer.get(claim_hk)
        ranked.append((nps_by_customer.get(customer_hk, 0), -idx, row))

    for _, _, row in sorted(ranked)[:escalated_target]:
        row["is_litigation"] = "Y"


def _assign_mlops_bands(rows: list[dict], is_churned, bands: list[tuple]) -> dict[int, str]:
    """Assign rows to bands while keeping the observed churn inside configured limits.

    The optional tuple shape is:
      (band, target_rate, weight, min_rate, max_rate)

    The min/max rates are important for coupled MLOps KPIs. They prevent excess
    churn from being dumped entirely into one high-risk band, and prevent low-risk
    bands from being starved when the overall churn rate is lower than the target
    weighted mix.
    """
    if not rows:
        return {}
    total_weight = sum(band_cfg[2] for band_cfg in bands) or 1
    total_rows = len(rows)
    band_specs = []
    allocated = 0
    for band_idx, band_cfg in enumerate(bands):
        band, target_rate, weight = band_cfg[:3]
        min_rate = band_cfg[3] if len(band_cfg) > 3 else max(0.0, target_rate - 0.04)
        max_rate = band_cfg[4] if len(band_cfg) > 4 else min(1.0, target_rate + 0.10)
        if band_idx == len(bands) - 1:
            size = total_rows - allocated
        else:
            size = round(total_rows * weight / total_weight)
            allocated += size
        band_specs.append({
            "band": band,
            "target_rate": float(target_rate),
            "min_rate": float(min_rate),
            "max_rate": float(max_rate),
            "size": max(0, size),
        })

    total_churn = sum(1 for row in rows if is_churned(row))
    for spec in band_specs:
        spec["min_quota"] = min(spec["size"], math.ceil(spec["size"] * spec["min_rate"]))
        spec["max_quota"] = min(spec["size"], math.floor(spec["size"] * spec["max_rate"]))
        spec["churn_quota"] = min(
            spec["max_quota"],
            max(spec["min_quota"], round(spec["size"] * spec["target_rate"])),
        )

    quota_sum = sum(spec["churn_quota"] for spec in band_specs)
    if quota_sum < total_churn:
        remaining = total_churn - quota_sum
        for spec in sorted(band_specs, key=lambda item: item["target_rate"], reverse=True):
            room = spec["max_quota"] - spec["churn_quota"]
            add = min(room, remaining)
            spec["churn_quota"] += add
            remaining -= add
            if remaining <= 0:
                break
        for spec in sorted(band_specs, key=lambda item: item["target_rate"], reverse=True):
            if remaining <= 0:
                break
            room = spec["size"] - spec["churn_quota"]
            add = min(room, remaining)
            spec["churn_quota"] += add
            remaining -= add
    elif quota_sum > total_churn:
        excess = quota_sum - total_churn
        for spec in sorted(band_specs, key=lambda item: item["target_rate"]):
            removable = max(0, spec["churn_quota"] - spec["min_quota"])
            remove = min(removable, excess)
            spec["churn_quota"] -= remove
            excess -= remove
            if excess <= 0:
                break
        for spec in sorted(band_specs, key=lambda item: item["target_rate"]):
            if excess <= 0:
                break
            remove = min(spec["churn_quota"], excess)
            spec["churn_quota"] -= remove
            excess -= remove

    shuffled = list(enumerate(rows))
    random.shuffle(shuffled)
    churned_rows = [(idx, row) for idx, row in shuffled if is_churned(row)]
    retained_rows = [(idx, row) for idx, row in shuffled if not is_churned(row)]
    assignments: dict[int, str] = {}

    for spec in sorted(band_specs, key=lambda item: item["target_rate"]):
        selected = []
        for _ in range(spec["churn_quota"]):
            if churned_rows:
                selected.append(churned_rows.pop())
        retained_quota = max(0, spec["size"] - len(selected))
        for _ in range(retained_quota):
            if retained_rows:
                selected.append(retained_rows.pop())
        while len(selected) < spec["size"] and churned_rows:
            selected.append(churned_rows.pop())
        while len(selected) < spec["size"] and retained_rows:
            selected.append(retained_rows.pop())
        for idx, _ in selected:
            assignments[idx] = spec["band"]

    for idx, _ in churned_rows + retained_rows:
        assignments[idx] = bands[-1][0]
    return assignments


def _apply_mlops_policy_churn_calibration(policy_rows: list[dict], cfg: dict | None = None) -> None:
    if not policy_rows:
        return

    def churned(row):
        return _policy_churn_flag(row)

    policy_renewal = _assign_mlops_bands(policy_rows, churned, _mlops_band_specs(
        cfg,
        "policy_renewal",
        [("Y", 0.13, 55, 0.08, 0.18), ("N", 0.45, 45, 0.35, 0.55)],
    ))
    auto = _assign_mlops_bands(policy_rows, churned, [("ON", 0.08, 40, 0.05, 0.12), ("OFF", 0.45, 60, 0.35, 0.55)])
    ncd = _assign_mlops_bands(policy_rows, churned, [("0_1", 0.31, 65, 0.25, 0.40), ("2_4", 0.23, 22, 0.18, 0.30), ("5_8", 0.17, 8, 0.15, 0.25), ("9_PLUS", 0.12, 5, 0.10, 0.18)])
    dd_cancel = _assign_mlops_bands(policy_rows, churned, [("NO", 0.16, 78, 0.10, 0.18), ("YES", 0.55, 22, 0.55, 0.60)])
    missed = _assign_mlops_bands(policy_rows, churned, [("0", 0.16, 32, 0.10, 0.18), ("1", 0.28, 18, 0.25, 0.35), ("2", 0.48, 28, 0.40, 0.55), ("3_PLUS", 0.65, 22, 0.60, 0.75)])
    loyalty = _assign_mlops_bands(policy_rows, churned, [("RETAINED", 0.13, 35, 0.08, 0.18), ("NOT_APPLIED", 0.25, 35, 0.18, 0.30), ("REMOVED", 0.48, 30, 0.40, 0.60)])

    ncd_values = {"0_1": ["0", "1"], "2_4": ["2", "3", "4"], "5_8": ["5", "6", "7", "8"], "9_PLUS": ["9", "10", "11"]}
    missed_values = {"0": "0", "1": "1", "2": "2"}
    for idx, row in enumerate(policy_rows):
        renewal_band = policy_renewal.get(idx, "Y")
        row["is_policy_renewal"] = renewal_band
        row["policy_type"] = "RENEWAL" if renewal_band == "Y" else "NEW_BUSINESS"
        row["is_auto_renew_enabled"] = "Y" if auto.get(idx) == "ON" else "N"
        row["no_claims_discount_years"] = random.choice(ncd_values[ncd.get(idx, "0_1")])
        missed_band = missed.get(idx, "0")
        row["missed_payment_count"] = missed_values[missed_band] if missed_band != "3_PLUS" else random.choice(["3", "4"])
        row["is_installment_default"] = "Y" if _int_value(row["missed_payment_count"], 0) >= 2 else "N"
        row["loyalty_discount_usage"] = loyalty.get(idx, "NOT_APPLIED")
        row["is_direct_debit_cancellation"] = "Y" if dd_cancel.get(idx) == "YES" else "N"
        if row["is_direct_debit_cancellation"] == "Y":
            row["payment_method"] = "DIRECT_DEBIT"
        elif churned(row):
            row["payment_method"] = random.choices(["BANK_TRANSFER", "DIRECT_DEBIT", "CARD"], weights=[10, 4, 86], k=1)[0]
        else:
            row["payment_method"] = random.choices(["BANK_TRANSFER", "DIRECT_DEBIT", "CARD"], weights=[10, 70, 20], k=1)[0]
        if row["is_direct_debit_cancellation"] == "Y":
            if _int_value(row["missed_payment_count"], 0) < 1:
                row["missed_payment_count"] = "1"
    _rebalance_mlops_payment_method(policy_rows)
    _rebalance_mlops_missed_zero(policy_rows)
    _rebalance_mlops_missed_one(policy_rows)
    _rebalance_mlops_missed_ranges(policy_rows)


def _rebalance_mlops_payment_method(policy_rows: list[dict]) -> None:
    for row in policy_rows:
        row["payment_method"] = ""
    dd_rows = [row for row in policy_rows if row.get("is_direct_debit_cancellation") == "Y"]
    for row in dd_rows:
        row["payment_method"] = "DIRECT_DEBIT"

    pool = [row for row in policy_rows if row.get("is_direct_debit_cancellation") != "Y"]
    churned_pool = [row for row in pool if _policy_churn_flag(row)]
    retained_pool = [row for row in pool if not _policy_churn_flag(row)]
    random.shuffle(churned_pool)
    random.shuffle(retained_pool)

    dd_churn = sum(1 for row in dd_rows if _policy_churn_flag(row))
    target_dd_total = max(len(dd_rows), math.ceil(dd_churn / 0.245))
    retained_dd = min(len(retained_pool), max(0, target_dd_total - len(dd_rows)))
    for row in retained_pool[:retained_dd]:
        row["payment_method"] = "DIRECT_DEBIT"
    retained_pool = retained_pool[retained_dd:]

    annual_churn = min(len(churned_pool), max(2, round(len(policy_rows) * 0.0004)))
    annual_retained = min(len(retained_pool), max(30, annual_churn * 10))
    for row in churned_pool[:annual_churn]:
        row["payment_method"] = "BANK_TRANSFER"
    for row in retained_pool[:annual_retained]:
        row["payment_method"] = "BANK_TRANSFER"
    churned_pool = churned_pool[annual_churn:]
    retained_pool = retained_pool[annual_retained:]

    for row in churned_pool + retained_pool:
        row["payment_method"] = "CARD"

    # If CARD is still too churn-heavy, move additional retained DIRECT_DEBIT rows
    # into CARD while keeping MONTHLY_DD inside its workbook range.
    for _ in range(3):
        card = [row for row in policy_rows if row.get("payment_method") == "CARD"]
        dd = [row for row in policy_rows if row.get("payment_method") == "DIRECT_DEBIT"]
        if not card or not dd:
            break
        card_rate = sum(1 for row in card if _policy_churn_flag(row)) / len(card)
        dd_rate = sum(1 for row in dd if _policy_churn_flag(row)) / len(dd)
        if card_rate <= 0.40 or dd_rate >= 0.249:
            break
        movable = [row for row in dd if row.get("is_direct_debit_cancellation") != "Y" and not _policy_churn_flag(row)]
        random.shuffle(movable)
        for row in movable[: max(1, round(len(policy_rows) * 0.01))]:
            row["payment_method"] = "CARD"
    return

    for row in policy_rows:
        if row.get("is_direct_debit_cancellation") == "Y":
            row["payment_method"] = "DIRECT_DEBIT"
            continue
        if _policy_churn_flag(row):
            row["payment_method"] = random.choices(["BANK_TRANSFER", "DIRECT_DEBIT", "CARD"], weights=[8, 2, 90], k=1)[0]
        else:
            row["payment_method"] = random.choices(["BANK_TRANSFER", "DIRECT_DEBIT", "CARD"], weights=[8, 86, 6], k=1)[0]


def _rebalance_mlops_missed_zero(policy_rows: list[dict]) -> None:
    zero_rows = [row for row in policy_rows if _int_value(row.get("missed_payment_count"), 0) == 0]
    if not zero_rows:
        return
    zero_churn = sum(1 for row in zero_rows if _policy_churn_flag(row))
    target_churn = math.ceil(len(zero_rows) * 0.12)
    if zero_churn >= target_churn:
        return
    candidates = [
        row for row in policy_rows
        if _policy_churn_flag(row)
        and row.get("is_direct_debit_cancellation") != "Y"
        and _int_value(row.get("missed_payment_count"), 0) == 1
    ]
    random.shuffle(candidates)
    for row in candidates[: max(0, target_churn - zero_churn)]:
        row["missed_payment_count"] = "0"
        row["is_installment_default"] = "N"
    yes_rows = [row for row in policy_rows if row.get("is_installment_default") == "Y"]
    yes_churn = sum(1 for row in yes_rows if _policy_churn_flag(row))
    yes_target = math.ceil(len(yes_rows) * 0.55)
    candidates = [
        row for row in policy_rows
        if _policy_churn_flag(row)
        and row.get("is_direct_debit_cancellation") != "Y"
        and _int_value(row.get("missed_payment_count"), 0) == 1
    ]
    random.shuffle(candidates)
    for row in candidates[: max(0, yes_target - yes_churn)]:
        row["missed_payment_count"] = "2"
        row["is_installment_default"] = "Y"


def _rebalance_mlops_missed_one(policy_rows: list[dict]) -> None:
    one_rows = [row for row in policy_rows if _int_value(row.get("missed_payment_count"), 0) == 1]
    if not one_rows:
        return
    one_churn = sum(1 for row in one_rows if _policy_churn_flag(row))
    target_churn = math.ceil(len(one_rows) * 0.25)
    if one_churn >= target_churn:
        return

    candidates = [
        row for row in policy_rows
        if _policy_churn_flag(row)
        and row.get("is_direct_debit_cancellation") != "Y"
        and _int_value(row.get("missed_payment_count"), 0) == 2
    ]
    random.shuffle(candidates)
    for row in candidates[: max(0, target_churn - one_churn)]:
        row["missed_payment_count"] = "1"
        row["is_installment_default"] = "N"


def _rebalance_mlops_missed_ranges(policy_rows: list[dict]) -> None:
    targets = {
        "0": (0.10, 0.18),
        "1": (0.25, 0.35),
        "2": (0.40, 0.55),
        "3_PLUS": (0.60, 0.75),
    }

    def band(row: dict) -> str:
        value = _int_value(row.get("missed_payment_count"), 0)
        return "3_PLUS" if value >= 3 else str(value)

    def set_band(row: dict, target_band: str) -> None:
        row["missed_payment_count"] = "3" if target_band == "3_PLUS" else target_band
        row["is_installment_default"] = "Y" if _int_value(row.get("missed_payment_count"), 0) >= 2 else "N"

    for target_band in ["0", "1", "2"]:
        for _ in range(5000):
            target_rows = [row for row in policy_rows if band(row) == target_band]
            if not target_rows:
                break
            target_churn = sum(1 for row in target_rows if _policy_churn_flag(row))
            min_rate, _ = targets[target_band]
            if target_churn / len(target_rows) >= min_rate:
                break

            donor_rows = [row for row in policy_rows if band(row) == "3_PLUS"]
            donor_churn = sum(1 for row in donor_rows if _policy_churn_flag(row))
            if not donor_rows or donor_churn / len(donor_rows) <= targets["3_PLUS"][0]:
                break

            churn_donor = next(
                (
                    row for row in donor_rows
                    if _policy_churn_flag(row)
                    and row.get("is_direct_debit_cancellation") != "Y"
                ),
                None,
            )
            retained_target = next((row for row in target_rows if not _policy_churn_flag(row)), None)
            if not churn_donor or not retained_target:
                break
            set_band(churn_donor, target_band)
            set_band(retained_target, "3_PLUS")


def _apply_mlops_marketing_churn_calibration(marketing_rows: list[dict], men_person: dict, churn_by_person: dict) -> None:
    rows = [row for row in marketing_rows if row.get("marketing_engagement_hash_key") in men_person]
    if not rows:
        return

    def churned(row):
        return bool(churn_by_person.get(men_person.get(row.get("marketing_engagement_hash_key")), False))

    retention = _assign_mlops_bands(rows, churned, [("NO", 0.17, 62, 0.12, 0.22), ("YES", 0.45, 38, 0.35, 0.55)])
    sentiment = _assign_mlops_bands(rows, churned, [("POSITIVE", 0.11, 30, 0.08, 0.15), ("NEUTRAL", 0.24, 40, 0.18, 0.30), ("NEGATIVE", 0.52, 30, 0.40, 0.65)])
    engagement = _assign_mlops_bands(rows, churned, [("HIGH", 0.13, 8, 0.08, 0.15), ("MEDIUM", 0.24, 32, 0.18, 0.30), ("LOW", 0.45, 40, 0.35, 0.55), ("VERY_LOW", 0.58, 20, 0.50, 0.70)])
    score_values = {
        "HIGH": ["80", "85", "90"],
        "MEDIUM": ["60", "65", "70"],
        "LOW": ["35", "45", "50"],
        "VERY_LOW": ["10", "15", "20"],
    }
    call_frequency_values = {"POSITIVE": ["0", "1"], "NEUTRAL": ["1", "2", "3"], "NEGATIVE": ["4", "5", "6"]}
    for idx, row in enumerate(rows):
        retention_band = retention.get(idx, "NO")
        sentiment_band = sentiment.get(idx, "NEUTRAL")
        engagement_band = engagement.get(idx, "LOW")
        row["has_retention_team_interaction"] = "Y" if retention_band == "YES" else "N"
        row["average_call_sentiment"] = sentiment_band
        row["engagement_score"] = random.choice(score_values[engagement_band])
        row["customer_service_call_frequency"] = random.choice(call_frequency_values[sentiment_band])


def _apply_mlops_marketing_policy_engagement_calibration(
    marketing_rows: list[dict],
    men_person: dict,
    policy_person_by_policy: dict,
    policy_by_hk: dict,
) -> None:
    rows_by_hk = {
        row.get("marketing_engagement_hash_key"): row
        for row in marketing_rows
        if row.get("marketing_engagement_hash_key")
    }
    men_by_person = {person_hk: men_hk for men_hk, person_hk in men_person.items() if men_hk and person_hk}
    policy_counts = {}
    for policy_hk, person_hk in policy_person_by_policy.items():
        men_hk = men_by_person.get(person_hk)
        if not men_hk or men_hk not in rows_by_hk:
            continue
        stats = policy_counts.setdefault(men_hk, {"total": 0, "churn": 0})
        stats["total"] += 1
        if _policy_churn_flag(policy_by_hk.get(policy_hk, {})):
            stats["churn"] += 1
    units = [
        (men_hk, rows_by_hk[men_hk], stats["total"], stats["churn"])
        for men_hk, stats in policy_counts.items()
        if stats["total"] > 0
    ]
    if not units:
        return

    total_occurrences = sum(total for _, _, total, _ in units)
    band_specs = [
        ("HIGH", 0.12, 0.08, 0.15, 0.12),
        ("MEDIUM", 0.24, 0.18, 0.30, 0.28),
        ("LOW", 0.45, 0.35, 0.55, 0.40),
        ("VERY_LOW", 0.58, 0.50, 0.70, 0.20),
    ]
    score_values = {
        "HIGH": ["80", "85", "90"],
        "MEDIUM": ["60", "65", "70"],
        "LOW": ["35", "45", "50"],
        "VERY_LOW": ["10", "15", "20"],
    }
    assigned = set()
    high_risk = sorted([item for item in units if item[3] > 0], key=lambda item: item[3] / item[2], reverse=True)
    low_risk = sorted([item for item in units if item[3] == 0], key=lambda item: item[2])
    for band, target, min_rate, max_rate, weight in band_specs:
        target_total = max(1, round(total_occurrences * weight))
        selected = []
        total = 0
        churn = 0
        target_churn = max(1, math.ceil(target_total * target))
        while churn < target_churn and high_risk:
            item = high_risk.pop(0)
            if item[0] in assigned:
                continue
            selected.append(item)
            assigned.add(item[0])
            total += item[2]
            churn += item[3]
        while total < target_total and low_risk:
            item = low_risk.pop(0)
            if item[0] in assigned:
                continue
            selected.append(item)
            assigned.add(item[0])
            total += item[2]
        while total and churn / total > max_rate and low_risk:
            item = low_risk.pop(0)
            if item[0] in assigned:
                continue
            selected.append(item)
            assigned.add(item[0])
            total += item[2]
        for _, row, _, _ in selected:
            row["engagement_score"] = random.choice(score_values[band])
    for item in high_risk + low_risk:
        if item[0] not in assigned:
            item[1]["engagement_score"] = random.choice(score_values["LOW"])
    _rebalance_mlops_policy_engagement_scores(marketing_rows, men_person, policy_person_by_policy, policy_by_hk)


def _rebalance_mlops_policy_engagement_scores(
    marketing_rows: list[dict],
    men_person: dict,
    policy_person_by_policy: dict,
    policy_by_hk: dict,
) -> None:
    rows_by_hk = {
        row.get("marketing_engagement_hash_key"): row
        for row in marketing_rows
        if row.get("marketing_engagement_hash_key")
    }
    men_by_person = {person_hk: men_hk for men_hk, person_hk in men_person.items() if men_hk and person_hk}
    stats_by_men = {}
    for policy_hk, person_hk in policy_person_by_policy.items():
        men_hk = men_by_person.get(person_hk)
        if not men_hk or men_hk not in rows_by_hk:
            continue
        stats = stats_by_men.setdefault(men_hk, {"total": 0, "churn": 0})
        stats["total"] += 1
        if _policy_churn_flag(policy_by_hk.get(policy_hk, {})):
            stats["churn"] += 1
    units = [
        {"men_hk": men_hk, "row": rows_by_hk[men_hk], "total": stats["total"], "churn": stats["churn"]}
        for men_hk, stats in stats_by_men.items()
        if stats["total"] > 0
    ]
    score_values = {
        "HIGH": ["80", "85", "90"],
        "MEDIUM": ["60", "65", "70"],
        "LOW": ["35", "45", "50"],
        "VERY_LOW": ["10", "15", "20"],
    }

    def score_band(row: dict) -> str:
        score = _int_value(row.get("engagement_score"), 0)
        if score >= 76:
            return "HIGH"
        if score >= 56:
            return "MEDIUM"
        if score >= 26:
            return "LOW"
        return "VERY_LOW"

    def rate(name: str) -> float:
        total = sum(item["total"] for item in units if score_band(item["row"]) == name)
        if not total:
            return 0.0
        churn = sum(item["churn"] for item in units if score_band(item["row"]) == name)
        return churn / total

    for target_band, min_rate in [("MEDIUM", 0.18), ("VERY_LOW", 0.50)]:
        for _ in range(5000):
            if rate(target_band) >= min_rate:
                break
            donor = next(
                (
                    item for item in units
                    if score_band(item["row"]) == "LOW"
                    and item["churn"] > 0
                    and item["churn"] / item["total"] >= min_rate
                ),
                None,
            )
            receiver = next(
                (
                    item for item in units
                    if score_band(item["row"]) == target_band
                    and item["churn"] == 0
                ),
                None,
            )
            if not donor or not receiver:
                break
            donor["row"]["engagement_score"] = random.choice(score_values[target_band])
            receiver["row"]["engagement_score"] = random.choice(score_values["LOW"])


def _apply_mlops_claim_churn_calibration(tables: dict[str, list[dict]], policy_by_hk: dict[str, dict]) -> None:
    claim_by_hk = {
        row.get("claim_hash_key"): row
        for row in tables.get("sat_claim", [])
        if row.get("claim_hash_key")
    }
    claim_policy = {
        link.get("claim_hash_key"): link.get("policy_hash_key")
        for link in tables.get("link_claim_policy", [])
        if link.get("claim_hash_key") and link.get("policy_hash_key")
    }
    rows = [row for claim_hk, row in claim_by_hk.items() if claim_hk in claim_policy]
    if not rows:
        return

    def churned(row):
        policy_hk = claim_policy.get(row.get("claim_hash_key"))
        return _policy_churn_flag(policy_by_hk.get(policy_hk, {}))

    fault = _assign_mlops_bands(rows, churned, [("NO", 0.16, 50, 0.12, 0.20), ("YES", 0.40, 50, 0.30, 0.50)])
    satisfaction = _assign_mlops_bands(rows, churned, [("HIGH", 0.12, 30, 0.08, 0.15), ("NEUTRAL", 0.24, 45, 0.18, 0.30), ("LOW", 0.50, 25, 0.40, 0.65)])
    score_values = {"HIGH": ["8", "9", "10"], "NEUTRAL": ["5", "6", "7"], "LOW": ["1", "2", "3", "4"]}
    for idx, row in enumerate(rows):
        row["is_fault_claim"] = "Y" if fault.get(idx) == "YES" else "N"
        row["claim_satisfaction_score"] = random.choice(score_values[satisfaction.get(idx, "NEUTRAL")])


def _apply_mlops_customer_churn_calibration(tables: dict[str, list[dict]], policy_by_hk: dict[str, dict], cfg: dict | None = None) -> None:
    customer_by_hk = {
        row.get("customer_hash_key"): row
        for row in tables.get("sat_customer", [])
        if row.get("customer_hash_key")
    }
    customer_policy = {}
    for link in tables.get("link_policy_customer", []):
        customer_hk = link.get("customer_hash_key")
        policy_hk = link.get("policy_hash_key")
        if customer_hk and policy_hk:
            customer_policy.setdefault(customer_hk, []).append(policy_hk)
    rows = [row for customer_hk, row in customer_by_hk.items() if customer_hk in customer_policy]
    if not rows:
        return

    def churned(row):
        policies = customer_policy.get(row.get("customer_hash_key"), [])
        return any(_policy_churn_flag(policy_by_hk.get(policy_hk, {})) for policy_hk in policies)

    satisfaction = _assign_mlops_bands(rows, churned, _mlops_band_specs(
        cfg,
        "customer_satisfaction",
        [
            ("VERY_SATISFIED", 0.12, 20, 0.08, 0.15),
            ("SATISFIED", 0.20, 35, 0.15, 0.25),
            ("NEUTRAL", 0.32, 25, 0.25, 0.40),
            ("DISSATISFIED", 0.55, 20, 0.45, 0.65),
        ],
    ))
    for idx, row in enumerate(rows):
        row["customer_satisfaction"] = satisfaction.get(idx, "NEUTRAL")


def _apply_mlops_complaint_churn_calibration(tables: dict[str, list[dict]], policy_by_hk: dict[str, dict], cfg: dict | None = None) -> None:
    complaint_by_hk = {
        row.get("complaint_hash_key"): row
        for row in tables.get("sat_complaint", [])
        if row.get("complaint_hash_key")
    }
    complaint_policy = {
        link.get("complaint_hash_key"): link.get("policy_hash_key")
        for link in tables.get("link_complaint_policy", [])
        if link.get("complaint_hash_key") and link.get("policy_hash_key")
    }
    rows = [row for complaint_hk, row in complaint_by_hk.items() if complaint_hk in complaint_policy]
    if not rows:
        return

    def churned(row):
        policy_hk = complaint_policy.get(row.get("complaint_hash_key"))
        return _policy_churn_flag(policy_by_hk.get(policy_hk, {}))

    resolution = _assign_mlops_bands(rows, churned, _mlops_band_specs(
        cfg,
        "complaint_resolution_days",
        [
            ("0_7", 0.12, 35, 0.08, 0.15),
            ("8_30", 0.24, 35, 0.18, 0.30),
            ("31_60", 0.42, 20, 0.35, 0.50),
            ("61_PLUS", 0.60, 10, 0.50, 0.70),
        ],
    ))
    day_values = {
        "0_7": (0, 7),
        "8_30": (8, 30),
        "31_60": (31, 60),
        "61_PLUS": (61, 120),
    }
    for idx, row in enumerate(rows):
        start = _date_obj(row.get("complaint_date")) or _date_obj(row.get("load_date")) or date.today()
        min_days, max_days = day_values.get(resolution.get(idx, "8_30"), (8, 30))
        resolved = start + timedelta(days=random.randint(min_days, max_days))
        ack = _date_obj(row.get("complaint_acknowledgement_date")) or start
        if ack < start:
            ack = start
        if resolved < ack:
            resolved = ack
        row["complaint_date"] = _timestamp_from_date(start, "00:00:00")
        row["complaint_acknowledgement_date"] = _timestamp_from_date(ack, "12:00:00")
        row["complaint_resolved_date"] = _timestamp_from_date(resolved, "23:59:59")
        _sync_complaint_status_with_dates(row)
    _rebalance_mlops_complaint_resolution(rows, complaint_policy, policy_by_hk)


def _rebalance_mlops_complaint_resolution(
    complaint_rows: list[dict],
    complaint_policy: dict[str, str],
    policy_by_hk: dict[str, dict],
) -> None:
    def churned(row: dict) -> bool:
        return _policy_churn_flag(policy_by_hk.get(complaint_policy.get(row.get("complaint_hash_key")), {}))

    def band(row: dict) -> str:
        start = _date_obj(row.get("complaint_date"))
        resolved = _date_obj(row.get("complaint_resolved_date"))
        if not start or not resolved:
            return "8_30"
        days = (resolved - start).days
        if days <= 7:
            return "0_7"
        if days <= 30:
            return "8_30"
        if days <= 60:
            return "31_60"
        return "61_PLUS"

    def set_band(row: dict, target_band: str) -> None:
        start = _date_obj(row.get("complaint_date")) or _date_obj(row.get("load_date")) or date.today()
        ranges = {"0_7": (0, 7), "8_30": (8, 30), "31_60": (31, 60), "61_PLUS": (61, 120)}
        min_days, max_days = ranges[target_band]
        resolved = start + timedelta(days=random.randint(min_days, max_days))
        ack = _date_obj(row.get("complaint_acknowledgement_date")) or start
        if ack < start:
            ack = start
        if resolved < ack:
            resolved = ack
        row["complaint_date"] = _timestamp_from_date(start, "00:00:00")
        row["complaint_acknowledgement_date"] = _timestamp_from_date(ack, "12:00:00")
        row["complaint_resolved_date"] = _timestamp_from_date(resolved, "23:59:59")
        _sync_complaint_status_with_dates(row)

    for target_band, max_rate in [("31_60", 0.50), ("61_PLUS", 0.70)]:
        for _ in range(500):
            target_rows = [row for row in complaint_rows if band(row) == target_band]
            if not target_rows:
                break
            target_churn = sum(1 for row in target_rows if churned(row))
            if target_churn / len(target_rows) <= max_rate:
                break
            retained_donor = next((row for row in complaint_rows if band(row) in {"0_7", "8_30"} and not churned(row)), None)
            churned_target = next((row for row in target_rows if churned(row)), None)
            if not retained_donor or not churned_target:
                break
            donor_band = band(retained_donor)
            set_band(retained_donor, target_band)
            set_band(churned_target, donor_band)


def _apply_nps_complaint_resolution_distribution(tables: dict[str, list[dict]], cfg: dict | None = None) -> None:
    complaint_rows = tables.get("sat_complaint", [])
    if not complaint_rows:
        return
    distribution = ((cfg or {}).get("nps_settings") or {}).get("complaint_resolution_distribution") or {
        "DAYS_0_2": 60,
        "DAYS_3_7": 30,
        "DAYS_GT_7": 10,
    }
    total_weight = sum(float(weight) for weight in distribution.values()) or 100.0
    counts = {str(band).upper(): round(len(complaint_rows) * float(weight) / total_weight) for band, weight in distribution.items()}
    while sum(counts.values()) > len(complaint_rows):
        counts[max(counts, key=counts.get)] -= 1
    while sum(counts.values()) < len(complaint_rows):
        counts[max(counts, key=lambda key: distribution.get(key, 0))] += 1

    policy_customer = {
        row.get("policy_hash_key"): row.get("customer_hash_key")
        for row in tables.get("link_policy_customer", [])
        if row.get("policy_hash_key") and row.get("customer_hash_key")
    }
    complaint_customer = {}
    for row in tables.get("link_complaint_policy", []):
        complaint_hk = row.get("complaint_hash_key")
        customer_hk = policy_customer.get(row.get("policy_hash_key"))
        if complaint_hk and customer_hk:
            complaint_customer[complaint_hk] = customer_hk
    nps_by_customer = {
        row.get("customer_hash_key"): _int_value(row.get("nps_score"), 0)
        for row in tables.get("sat_customer", [])
        if row.get("customer_hash_key")
    }

    ranked = []
    for idx, row in enumerate(complaint_rows):
        customer_hk = complaint_customer.get(row.get("complaint_hash_key"))
        ranked.append((nps_by_customer.get(customer_hk, 0), -idx, row))

    ranked_low = sorted(ranked)
    ranked_high = sorted(ranked, reverse=True)
    ranked_mid = sorted(ranked, key=lambda item: (abs(item[0] - 7.5), item[1]))
    used_rows = set()

    def take_rows(candidates: list[tuple[int, int, dict]], count: int) -> list[dict]:
        selected = []
        for _, _, candidate in candidates:
            row_id = id(candidate)
            if row_id in used_rows:
                continue
            selected.append(candidate)
            used_rows.add(row_id)
            if len(selected) == count:
                break
        if len(selected) < count:
            for _, _, candidate in ranked_high:
                row_id = id(candidate)
                if row_id in used_rows:
                    continue
                selected.append(candidate)
                used_rows.add(row_id)
                if len(selected) == count:
                    break
        return selected

    band_rows = {
        "DAYS_GT_7": take_rows([item for item in ranked_low if item[0] <= 6], counts.get("DAYS_GT_7", 0)),
        "DAYS_3_7": take_rows([item for item in ranked_mid if 7 <= item[0] <= 8], counts.get("DAYS_3_7", 0)),
        "DAYS_0_2": take_rows([item for item in ranked_high if item[0] >= 9], counts.get("DAYS_0_2", 0)),
    }
    day_ranges = {
        "DAYS_0_2": (0, 2),
        "DAYS_3_7": (3, 7),
        "DAYS_GT_7": (8, 30),
    }
    for band, rows in band_rows.items():
        min_days, max_days = day_ranges[band]
        for idx, row in enumerate(rows):
            start = _date_obj(row.get("complaint_date")) or _date_obj(row.get("load_date")) or date.today()
            days = min_days + (idx % (max_days - min_days + 1))
            resolved = start + timedelta(days=days)
            ack = _date_obj(row.get("complaint_acknowledgement_date")) or start
            if ack < start:
                ack = start
            if resolved < ack:
                ack = resolved
            row["complaint_date"] = _timestamp_from_date(start, "00:00:00")
            row["complaint_acknowledgement_date"] = _timestamp_from_date(ack, "12:00:00")
            row["complaint_resolved_date"] = _timestamp_from_date(resolved, "23:59:59")
            _sync_complaint_status_with_dates(row)


def _apply_mlops_columns(tables: dict[str, list[dict]], ctx: dict, cfg: dict | None = None) -> None:
    for row in tables.get("sat_address", []):
        row["region"] = row.get("state") or row.get("city") or ""

    for row in tables.get("sat_claim", []):
        suspected_amount = max(0, _int_value(row.get("suspected_amount") or row.get("suspectd_amount"), 0))
        claim_amount = max(0, _int_value(row.get("claim_amount"), 0))
        row["suspected_amount"] = str(min(suspected_amount, claim_amount))
        row.pop("suspectd_amount", None)
        fault_claim = (
            _yn(row.get("is_claim_fraud")) == "Y"
            or _yn(row.get("is_claim_suspicious")) == "Y"
            or str(row.get("third_party_involved") or "").strip().upper() in {"Y", "YES", "TRUE", "1"}
            or _float_value(row.get("third_party_involved_overall_score"), 0.0) >= 70
        )
        row["is_fault_claim"] = "Y" if fault_claim else "N"
        row["claim_satisfaction_score"] = _claim_satisfaction_score(row)

    natural_by_person = {}
    for link in tables.get("link_person_natural_person", []):
        person_hk = link.get("person_hash_key")
        natural_hk = link.get("natural_person_hash_key")
        if person_hk and natural_hk:
            natural_by_person[person_hk] = natural_hk
    driver_experience_by_person = {
        row.get("natural_person_hash_key"): _driver_experience_from_nat(row)
        for row in tables.get("sat_natural_person", [])
        if row.get("natural_person_hash_key")
    }
    customer_person = {
        row.get("customer_hash_key"): row.get("person_hash_key")
        for row in tables.get("link_customer_person", [])
        if row.get("customer_hash_key") and row.get("person_hash_key")
    }
    policy_person_by_policy = {}
    for lpc in tables.get("link_policy_customer", []):
        policy_hk = lpc.get("policy_hash_key")
        customer_hk = lpc.get("customer_hash_key")
        if not policy_hk or not customer_hk:
            continue
        person_hk = customer_person.get(customer_hk)
        if person_hk:
            policy_person_by_policy[policy_hk] = person_hk

    motor_by_hk = {row.get("motor_hash_key"): row for row in tables.get("sat_motor", []) if row.get("motor_hash_key")}
    motor_to_person = {}
    for lpio in tables.get("link_policy_insured_object", []):
        policy_hk = lpio.get("policy_hash_key")
        insured_object_hk = lpio.get("insured_object_hash_key")
        person_hk = policy_person_by_policy.get(policy_hk)
        if not person_hk:
            continue
        for liom in tables.get("link_insured_object_motor", []):
            if liom.get("insured_object_hash_key") == insured_object_hk:
                motor_to_person[liom.get("motor_hash_key")] = person_hk
                break
    for motor_hk, row in motor_by_hk.items():
        person_hk = motor_to_person.get(motor_hk)
        natural_hk = natural_by_person.get(person_hk)
        row["driver_experience_years"] = str(driver_experience_by_person.get(natural_hk, 0))

    policy_by_hk = {row.get("policy_hash_key"): row for row in tables.get("sat_policy", []) if row.get("policy_hash_key")}
    for row in policy_by_hk.values():
        row.update(_policy_payment_profile(row))
    _apply_mlops_policy_churn_calibration(list(policy_by_hk.values()), cfg)
    _apply_nps_claim_escalation_distribution(tables, cfg)
    _apply_mlops_claim_churn_calibration(tables, policy_by_hk)
    _apply_mlops_customer_churn_calibration(tables, policy_by_hk, cfg)
    _apply_mlops_complaint_churn_calibration(tables, policy_by_hk, cfg)
    _apply_nps_complaint_resolution_distribution(tables, cfg)

    for row in tables.get("sat_claim", []):
        row["claims_feedback"] = _claim_feedback(row)

    for row in tables.get("sat_complaint", []):
        _sync_complaint_status_with_dates(row)
        complaint_score = _complaint_satisfaction_score(row)
        row["customer_complaint_satisfaction_score"] = complaint_score
        row["complaint_feedback"] = _feedback_from_score(_int_value(complaint_score, 0)) if complaint_score != "" else ""

    for row in tables.get("sat_customer", []):
        onboarding_score = _customer_onboarding_satisfaction_score(row)
        row["customer_onboarding_satisfaction_score"] = onboarding_score
        row["customer_onboarding_feedback"] = _feedback_text_from_score(
            _int_value(onboarding_score, 0),
            cfg,
            "onboarding_feedback_text",
            row.get("customer_hash_key", ""),
        )

    for row in policy_by_hk.values():
        renewal_score = _policy_renewal_satisfaction_score(row)
        row["policy_renewal_satisfaction_score"] = renewal_score
        row["policy_renewal_feedback"] = _feedback_from_score(_int_value(renewal_score, 0)) if renewal_score != "" else ""
        row["is_renewal_escalation"] = (
            "Y"
            if renewal_score != "" and _int_value(renewal_score, 0) <= 2
            else "N" if renewal_score != "" else ""
        )

    churn_by_person = {}
    for policy_hk, person_hk in policy_person_by_policy.items():
        policy = policy_by_hk.get(policy_hk, {})
        if not person_hk:
            continue
        churn_by_person[person_hk] = churn_by_person.get(person_hk, False) or _policy_churn_flag(policy)
    men_person = {
        link.get("marketing_engagement_hash_key"): link.get("person_hash_key")
        for link in tables.get("link_person_marketing_engagement", [])
    }
    for row in tables.get("sat_marketing_engagement", []):
        person_hk = men_person.get(row.get("marketing_engagement_hash_key"))
        churned = bool(churn_by_person.get(person_hk, False))
        opened = _yn(row.get("opened_email")) == "Y"
        status = str(row.get("marketing_status") or "").upper()
        score = 70 if opened else 35
        if status == "ACTIVE":
            score += 15
        if churned:
            score -= 20
        retention = "Y" if churned or score < 45 else "N"
        call_frequency = random.choices([0, 1, 2, 3, 4, 5, 6], weights=[10, 15, 20, 20, 16, 12, 7], k=1)[0] if churned else random.choices([0, 1, 2, 3], weights=[55, 30, 12, 3], k=1)[0]
        sentiment = "NEGATIVE" if call_frequency >= 4 or score < 35 else "NEUTRAL" if call_frequency >= 2 or score < 65 else "POSITIVE"
        row.update({
            "has_retention_team_interaction": retention,
            "customer_service_call_frequency": str(call_frequency),
            "average_call_sentiment": sentiment,
            "engagement_score": str(max(0, min(100, score))),
        })
    _apply_mlops_marketing_churn_calibration(tables.get("sat_marketing_engagement", []), men_person, churn_by_person)
    _apply_mlops_marketing_policy_engagement_calibration(
        tables.get("sat_marketing_engagement", []),
        men_person,
        policy_person_by_policy,
        policy_by_hk,
    )
    for row in tables.get("sat_marketing_engagement", []):
        call_frequency = _int_value(row.get("customer_service_call_frequency"), 0)
        sentiment = str(row.get("average_call_sentiment") or "").strip().upper()
        engagement_score = _int_value(row.get("engagement_score"), 0)
        row["first_contact_resolution"] = (
            "Y"
            if call_frequency <= 1 and sentiment != "NEGATIVE" and engagement_score >= 60
            else "N"
        )


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


def _augment_base_satellites(tables: dict[str, list[dict]], cfg: dict | None = None) -> None:
    customers = _read_example("DimCustomer.csv")
    policies = _read_example("DimPolicy.csv")
    fact_policies = _read_example("FactPolicy.csv")
    products = _read_example("DimProduct.csv")
    quotes = _read_example("FactQuote.csv")

    for idx, row in enumerate(tables.get("sat_customer", [])):
        sample = _sample(customers, idx)
        nps_score, nps_segment = _nps_profile(idx, cfg)
        row.update({
            "nps_score": nps_score,
            "income_band": sample.get("IncomeBand", ""),
            "customer_satisfaction": _customer_satisfaction_label(sample.get("CustomerSatisfaction")),
            "customer_age_band": sample.get("AgeBand", ""),
            "net_promotor_code_segment": nps_segment,
        })

    customer_nps_by_hk = {
        row.get("customer_hash_key"): _int_value(row.get("nps_score"), None)
        for row in tables.get("sat_customer", [])
        if row.get("customer_hash_key")
    }
    policy_nps_by_hk = {
        link.get("policy_hash_key"): customer_nps_by_hk.get(link.get("customer_hash_key"))
        for link in tables.get("link_policy_customer", [])
        if link.get("policy_hash_key") and link.get("customer_hash_key")
    }

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
            "policy_issue_date": _policy_issue_date_for_nps(
                row.get("policy_start_date"),
                idx,
                cfg,
                policy_nps_by_hk.get(row.get("policy_hash_key")),
            ),
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


def _apply_customer_segment_rating_consistency(tables: dict[str, list[dict]]) -> None:
    """Keep enhanced/MLOps customer segment and rating aligned after NPS enrichment."""
    customer_rows = tables.get("sat_customer", [])
    if not customer_rows:
        return

    customer_to_person = {
        row.get("customer_hash_key"): row.get("person_hash_key")
        for row in tables.get("link_customer_person", [])
        if row.get("customer_hash_key") and row.get("person_hash_key")
    }

    account_to_person = {
        row.get("account_hash_key"): row.get("person_hash_key")
        for row in tables.get("link_person_account", [])
        if row.get("account_hash_key") and row.get("person_hash_key")
    }
    account_status_by_person = {}
    for row in tables.get("sat_account", []):
        account_hk = row.get("account_hash_key")
        person_hk = account_to_person.get(account_hk)
        if person_hk:
            account_status_by_person[person_hk] = str(row.get("account_status", "")).upper().strip()

    customer_to_policies = {}
    for row in tables.get("link_policy_customer", []):
        customer_hk = row.get("customer_hash_key")
        policy_hk = row.get("policy_hash_key")
        if customer_hk and policy_hk:
            customer_to_policies.setdefault(customer_hk, []).append(policy_hk)

    policy_by_hk = {
        row.get("policy_hash_key"): row
        for row in tables.get("sat_policy", [])
        if row.get("policy_hash_key")
    }

    def int_value(value, default: int = 0) -> int:
        try:
            return int(float(str(value or "").replace(",", "").strip()))
        except ValueError:
            return default

    def float_value(value, default: float = 0.0) -> float:
        try:
            return float(str(value or "").replace(",", "").strip())
        except ValueError:
            return default

    for customer_row in customer_rows:
        customer_hk = customer_row.get("customer_hash_key")
        person_hk = customer_to_person.get(customer_hk)
        if not person_hk:
            customer_row["customer_segment"] = "STANDARD"
            customer_row["customer_rating"] = str(max(1, min(5, int_value(customer_row.get("customer_rating"), 3))))
            continue

        policy_summary = {
            "has_active": False,
            "has_lapsed": False,
            "has_cancelled": False,
            "fraud_flag": False,
            "max_revenue": 0.0,
            "declined_claims": 0,
        }
        for policy_hk in customer_to_policies.get(customer_hk, []):
            policy_row = policy_by_hk.get(policy_hk, {})
            status = str(policy_row.get("policy_status", "")).upper().strip()
            if status == "ACTIVE":
                policy_summary["has_active"] = True
            elif status == "LAPSED":
                policy_summary["has_lapsed"] = True
            elif status == "CANCELLED":
                policy_summary["has_cancelled"] = True

            policy_summary["fraud_flag"] = policy_summary["fraud_flag"] or (
                str(policy_row.get("fraud_flag", "")).upper().strip() == "Y"
            )
            policy_summary["max_revenue"] = max(
                policy_summary["max_revenue"],
                float_value(policy_row.get("gross_revenue")),
            )
            policy_summary["declined_claims"] = max(
                policy_summary["declined_claims"],
                int_value(policy_row.get("declined_claims")),
            )

        nps_score = int_value(customer_row.get("nps_score"))
        account_status = account_status_by_person.get(person_hk)

        segment_score = 0
        if policy_summary["has_active"]:
            segment_score += 2
        if account_status == "OPEN":
            segment_score += 1
        if nps_score >= 9:
            segment_score += 1
        if policy_summary["max_revenue"] >= 1800:
            segment_score += 1
        if policy_summary["fraud_flag"]:
            segment_score -= 2
        if policy_summary["has_cancelled"]:
            segment_score -= 1
        elif policy_summary["has_lapsed"]:
            segment_score -= 1

        expected_segment = "PREMIUM" if segment_score >= 4 else "STANDARD"
        customer_row["customer_segment"] = expected_segment

        rating = 2
        if str(customer_row.get("customer_status", "")).upper().strip() == "ACTIVE":
            rating += 1
        if expected_segment == "PREMIUM":
            rating += 1
        if nps_score >= 9:
            rating += 1
        elif nps_score <= 1:
            rating -= 1
        if policy_summary["has_active"]:
            rating += 1
        if policy_summary["has_lapsed"]:
            rating -= 1
        if policy_summary["has_cancelled"]:
            rating -= 1
        if account_status == "OPEN":
            rating += 1
        elif account_status == "SUSPENDED":
            rating -= 1
        elif account_status == "CLOSED":
            rating -= 2
        if policy_summary["fraud_flag"]:
            rating -= 1
        if policy_summary["declined_claims"] > 1:
            rating -= 1

        customer_row["customer_rating"] = str(max(1, min(5, rating)))


def _apply_digital_onboarding_nps_alignment(tables: dict[str, list[dict]], cfg: dict | None = None) -> None:
    account_rows = tables.get("sat_account", [])
    customer_rows = tables.get("sat_customer", [])
    if not account_rows or not customer_rows:
        return

    distribution = ((cfg or {}).get("nps_settings") or {}).get("digital_onboarding_distribution") or {
        "ONLINE": 75,
        "BRANCH": 25,
    }
    online_weight = sum(
        float(weight)
        for value, weight in distribution.items()
        if str(value).upper() in {"ONLINE", "DIGITAL", "SELF_SERVICE", "SELF-SERVICE"}
    )
    total_weight = sum(float(weight) for weight in distribution.values()) or 100.0

    customer_to_person = {
        row.get("customer_hash_key"): row.get("person_hash_key")
        for row in tables.get("link_customer_person", [])
        if row.get("customer_hash_key") and row.get("person_hash_key")
    }
    nps_by_person = {}
    for row in customer_rows:
        person_hk = customer_to_person.get(row.get("customer_hash_key"))
        if not person_hk:
            continue
        nps_by_person[person_hk] = _int_value(row.get("nps_score"), 0)

    account_to_person = {
        row.get("account_hash_key"): row.get("person_hash_key")
        for row in tables.get("link_person_account", [])
        if row.get("account_hash_key") and row.get("person_hash_key")
    }
    ranked = []
    for idx, row in enumerate(account_rows):
        person_hk = account_to_person.get(row.get("account_hash_key"))
        ranked.append((nps_by_person.get(person_hk, 0), -idx, row))

    online_target = round(len(ranked) * online_weight / total_weight)
    overlap = ((cfg or {}).get("nps_settings") or {}).get("digital_onboarding_nps_overlap") or {}
    online_low_rate = float(overlap.get("online_low_nps_1_4_rate", 0.06))
    branch_high_rate = float(overlap.get("branch_high_nps_8_10_rate", 0.05))
    if online_low_rate > 1:
        online_low_rate /= 100.0
    if branch_high_rate > 1:
        branch_high_rate /= 100.0

    high_rows = sorted([item for item in ranked if item[0] >= 8], reverse=True)
    low_rows = sorted([item for item in ranked if 1 <= item[0] <= 4], reverse=True)
    mid_rows = sorted([item for item in ranked if 5 <= item[0] <= 7], reverse=True)
    other_rows = sorted([item for item in ranked if item[0] <= 0], reverse=True)

    online_low_target = min(len(low_rows), round(len(low_rows) * online_low_rate))
    branch_high_target = min(len(high_rows), round(len(high_rows) * branch_high_rate))
    if low_rows and online_low_target == 0:
        online_low_target = 1
    if high_rows and branch_high_target == 0:
        branch_high_target = 1

    branch_high_rows = {id(row) for _, _, row in high_rows[-branch_high_target:]} if branch_high_target else set()
    online_rows: set[int] = set()

    def add_online(candidates: list[tuple[int, int, dict]], limit: int | None = None) -> None:
        added = 0
        for _, _, row in candidates:
            row_id = id(row)
            if row_id in online_rows or row_id in branch_high_rows:
                continue
            if len(online_rows) >= online_target:
                break
            if limit is not None and added >= limit:
                break
            online_rows.add(row_id)
            added += 1

    add_online(low_rows, online_low_target)
    add_online(high_rows)
    add_online(mid_rows)
    add_online(low_rows + other_rows)
    for row in account_rows:
        row["account_creation_type"] = "ONLINE" if id(row) in online_rows else "BRANCH"


def _apply_quote_dropoff_nps_alignment(tables: dict[str, list[dict]], cfg: dict | None = None) -> None:
    quote_rows = tables.get("sat_quote", [])
    customer_rows = tables.get("sat_customer", [])
    if not quote_rows or not customer_rows:
        return

    settings = ((cfg or {}).get("nps_settings") or {})
    distribution = settings.get("quote_dropoff_distribution") or {
        "ACCEPTED": 92,
        "DROPOFF": 8,
    }
    dropoff_weight = sum(
        float(weight)
        for value, weight in distribution.items()
        if str(value).upper() not in {"ACCEPTED", "CONVERTED"}
    )
    total_weight = sum(float(weight) for weight in distribution.values()) or 100.0
    dropoff_target = round(len(quote_rows) * dropoff_weight / total_weight)

    status_distribution = settings.get("quote_dropoff_status_distribution") or {
        "CREATED": 50,
        "SENT": 35,
        "EXPIRED": 15,
    }
    status_total = sum(float(weight) for weight in status_distribution.values()) or 100.0
    status_counts = {
        str(status).upper(): round(dropoff_target * float(weight) / status_total)
        for status, weight in status_distribution.items()
    }
    while sum(status_counts.values()) > dropoff_target:
        status_counts[max(status_counts, key=status_counts.get)] -= 1
    while sum(status_counts.values()) < dropoff_target:
        status_counts[max(status_counts, key=lambda key: status_distribution.get(key, 0))] += 1

    nps_by_customer = {
        row.get("customer_hash_key"): _int_value(row.get("nps_score"), 0)
        for row in customer_rows
        if row.get("customer_hash_key")
    }
    quote_to_customer = {}
    policy_to_quote = {
        row.get("policy_hash_key"): row.get("quote_hash_key")
        for row in tables.get("link_policy_quote", [])
        if row.get("policy_hash_key") and row.get("quote_hash_key")
    }
    for row in tables.get("link_policy_customer", []):
        quote_hk = policy_to_quote.get(row.get("policy_hash_key"))
        customer_hk = row.get("customer_hash_key")
        if quote_hk and customer_hk:
            quote_to_customer[quote_hk] = customer_hk

    ranked = []
    for idx, row in enumerate(quote_rows):
        customer_hk = quote_to_customer.get(row.get("quote_hash_key"))
        ranked.append((nps_by_customer.get(customer_hk, 0), -idx, row))

    for row in quote_rows:
        row["quote_status"] = "ACCEPTED"

    ranked_low = sorted(ranked)
    ranked_high = sorted(ranked, reverse=True)
    ranked_mid = sorted(ranked, key=lambda item: (abs(item[0] - 7.5), item[1]))
    used_rows = set()

    def take_rows(candidates: list[tuple[int, int, dict]], count: int) -> list[dict]:
        selected = []
        for _, _, candidate in candidates:
            row_id = id(candidate)
            if row_id in used_rows:
                continue
            selected.append(candidate)
            used_rows.add(row_id)
            if len(selected) == count:
                break
        if len(selected) < count:
            for _, _, candidate in ranked_low:
                row_id = id(candidate)
                if row_id in used_rows:
                    continue
                selected.append(candidate)
                used_rows.add(row_id)
                if len(selected) == count:
                    break
        return selected

    status_rows = {
        "EXPIRED": take_rows([item for item in ranked_low if item[0] <= 4], status_counts.get("EXPIRED", 0)),
        "SENT": take_rows([item for item in ranked_mid if 7 <= item[0] <= 8], status_counts.get("SENT", 0)),
        "CREATED": take_rows([item for item in ranked_high if item[0] >= 8], status_counts.get("CREATED", 0)),
    }
    for status in ["EXPIRED", "SENT", "CREATED"]:
        for row in status_rows.get(status, []):
            row["quote_status"] = status


def _take_ranked_rows(
    ranked_groups: list[list[tuple[int, int, dict]]],
    count: int,
    fallback: list[tuple[int, int, dict]],
    used_rows: set[int],
) -> list[dict]:
    selected = []
    for candidates in ranked_groups + [fallback]:
        for _, _, candidate in candidates:
            row_id = id(candidate)
            if row_id in used_rows:
                continue
            selected.append(candidate)
            used_rows.add(row_id)
            if len(selected) == count:
                return selected
    return selected


def _quote_customer_nps_rank(tables: dict[str, list[dict]]) -> list[tuple[int, int, dict]]:
    nps_by_customer = {
        row.get("customer_hash_key"): _int_value(row.get("nps_score"), 0)
        for row in tables.get("sat_customer", [])
        if row.get("customer_hash_key")
    }
    policy_to_quote = {
        row.get("policy_hash_key"): row.get("quote_hash_key")
        for row in tables.get("link_policy_quote", [])
        if row.get("policy_hash_key") and row.get("quote_hash_key")
    }
    quote_to_customer = {}
    for row in tables.get("link_policy_customer", []):
        quote_hk = policy_to_quote.get(row.get("policy_hash_key"))
        customer_hk = row.get("customer_hash_key")
        if quote_hk and customer_hk:
            quote_to_customer[quote_hk] = customer_hk
    ranked = []
    for idx, row in enumerate(tables.get("sat_quote", [])):
        customer_hk = quote_to_customer.get(row.get("quote_hash_key"))
        if not customer_hk:
            continue
        ranked.append((nps_by_customer.get(customer_hk, 0), -idx, row))
    return ranked


def _distribution_counts(total_rows: int, distribution: dict, aliases: dict[str, set[str]]) -> dict[str, int]:
    counts = {}
    normalized_weights = {}
    for canonical, names in aliases.items():
        normalized_weights[canonical] = sum(
            float(weight)
            for key, weight in distribution.items()
            if str(key).upper() in names
        )
    if not any(normalized_weights.values()):
        normalized_weights = {key: float(value) for key, value in distribution.items()}
    total_weight = sum(normalized_weights.values()) or 100.0
    for key, weight in normalized_weights.items():
        counts[key] = round(total_rows * weight / total_weight)
    while sum(counts.values()) > total_rows:
        counts[max(counts, key=counts.get)] -= 1
    while sum(counts.values()) < total_rows and counts:
        counts[max(counts, key=lambda key: normalized_weights.get(key, 0))] += 1
    return counts


def _marketing_customer_nps_rank(tables: dict[str, list[dict]]) -> list[tuple[int, int, dict]]:
    customer_to_person = {
        row.get("customer_hash_key"): row.get("person_hash_key")
        for row in tables.get("link_customer_person", [])
        if row.get("customer_hash_key") and row.get("person_hash_key")
    }
    nps_by_person = {}
    for row in tables.get("sat_customer", []):
        person_hk = customer_to_person.get(row.get("customer_hash_key"))
        if person_hk:
            nps_by_person[person_hk] = _int_value(row.get("nps_score"), 0)
    person_by_marketing = {
        row.get("marketing_engagement_hash_key"): row.get("person_hash_key")
        for row in tables.get("link_person_marketing_engagement", [])
        if row.get("marketing_engagement_hash_key") and row.get("person_hash_key")
    }
    ranked = []
    for idx, row in enumerate(tables.get("sat_marketing_engagement", [])):
        person_hk = person_by_marketing.get(row.get("marketing_engagement_hash_key"))
        if person_hk not in nps_by_person:
            continue
        ranked.append((nps_by_person.get(person_hk, 0), -idx, row))
    return ranked


def _claim_customer_nps_rank(tables: dict[str, list[dict]]) -> list[tuple[int, int, dict]]:
    nps_by_customer = {
        row.get("customer_hash_key"): _int_value(row.get("nps_score"), 0)
        for row in tables.get("sat_customer", [])
        if row.get("customer_hash_key")
    }
    policy_customer = {
        row.get("policy_hash_key"): row.get("customer_hash_key")
        for row in tables.get("link_policy_customer", [])
        if row.get("policy_hash_key") and row.get("customer_hash_key")
    }
    claim_customer = {}
    for row in tables.get("link_claim_policy", []):
        customer_hk = policy_customer.get(row.get("policy_hash_key"))
        if row.get("claim_hash_key") and customer_hk:
            claim_customer[row.get("claim_hash_key")] = customer_hk
    return [
        (nps_by_customer.get(claim_customer.get(row.get("claim_hash_key")), 0), -idx, row)
        for idx, row in enumerate(tables.get("sat_claim", []))
    ]


def _complaint_customer_nps_rank(tables: dict[str, list[dict]]) -> list[tuple[int, int, dict]]:
    nps_by_customer = {
        row.get("customer_hash_key"): _int_value(row.get("nps_score"), 0)
        for row in tables.get("sat_customer", [])
        if row.get("customer_hash_key")
    }
    policy_customer = {
        row.get("policy_hash_key"): row.get("customer_hash_key")
        for row in tables.get("link_policy_customer", [])
        if row.get("policy_hash_key") and row.get("customer_hash_key")
    }
    complaint_customer = {}
    for row in tables.get("link_complaint_policy", []):
        customer_hk = policy_customer.get(row.get("policy_hash_key"))
        if row.get("complaint_hash_key") and customer_hk:
            complaint_customer[row.get("complaint_hash_key")] = customer_hk
    return [
        (nps_by_customer.get(complaint_customer.get(row.get("complaint_hash_key")), 0), -idx, row)
        for idx, row in enumerate(tables.get("sat_complaint", []))
    ]


def _spread_ranked_by_score(
    ranked: list[tuple[int, int, str]],
    scores: list[int],
    reverse_within_score: bool = False,
) -> list[tuple[int, int, str]]:
    """Interleave rows across exact NPS scores so a band is not dominated by one score."""
    grouped = {
        score: sorted(
            [item for item in ranked if item[0] == score],
            key=lambda item: item[1],
            reverse=reverse_within_score,
        )
        for score in scores
    }
    output: list[tuple[int, int, str]] = []
    while True:
        added = False
        for score in scores:
            if grouped[score]:
                output.append(grouped[score].pop(0))
                added = True
        if not added:
            return output


def _apply_nps_renewal_contact_distribution(tables: dict[str, list[dict]], cfg: dict | None = None) -> None:
    ranked = _marketing_customer_nps_rank(tables)
    if not ranked:
        return
    distribution = ((cfg or {}).get("nps_settings") or {}).get("renewal_contact_distribution") or {
        "CONTACTS_0_1": 60,
        "CONTACTS_2_3": 30,
        "CONTACTS_GT_3": 10,
    }
    counts = _distribution_counts(
        len(ranked),
        distribution,
        {
            "CONTACTS_0_1": {"CONTACTS_0_1", "0_1", "LOW_CONTACT"},
            "CONTACTS_2_3": {"CONTACTS_2_3", "2_3", "MODERATE_CONTACT"},
            "CONTACTS_GT_3": {"CONTACTS_GT_3", "GT_3", "HIGH_CONTACT"},
        },
    )
    ranked_low = sorted(ranked)
    ranked_high = sorted(ranked, reverse=True)
    ranked_mid = sorted(ranked, key=lambda item: (abs(item[0] - 7.5), item[1]))
    used_rows: set[int] = set()
    band_rows = {
        "CONTACTS_GT_3": _take_ranked_rows([[item for item in ranked_low if item[0] <= 6]], counts.get("CONTACTS_GT_3", 0), ranked_low, used_rows),
        "CONTACTS_2_3": _take_ranked_rows([[item for item in ranked_mid if 7 <= item[0] <= 8]], counts.get("CONTACTS_2_3", 0), ranked_mid, used_rows),
        "CONTACTS_0_1": _take_ranked_rows([[item for item in ranked_high if item[0] >= 9]], counts.get("CONTACTS_0_1", 0), ranked_high, used_rows),
    }
    values = {
        "CONTACTS_0_1": [0, 1],
        "CONTACTS_2_3": [2, 3],
        "CONTACTS_GT_3": [4, 5, 6],
    }
    for band, rows in band_rows.items():
        for idx, row in enumerate(rows):
            call_frequency = values[band][idx % len(values[band])]
            row["customer_service_call_frequency"] = str(call_frequency)
            row["average_call_sentiment"] = "NEGATIVE" if call_frequency >= 4 else "NEUTRAL" if call_frequency >= 2 else "POSITIVE"
            row["first_contact_resolution"] = "Y" if call_frequency <= 1 else "N"


def _apply_nps_customer_satisfaction_distribution(tables: dict[str, list[dict]], cfg: dict | None = None) -> None:
    for row in tables.get("sat_customer", []):
        nps = _int_value(row.get("nps_score"), 0)
        if nps >= 9:
            row["customer_satisfaction"] = "VERY_SATISFIED" if nps == 10 else "SATISFIED"
        elif nps >= 7:
            row["customer_satisfaction"] = "SATISFIED" if nps == 8 else "NEUTRAL"
        elif nps >= 4:
            row["customer_satisfaction"] = "NEUTRAL" if nps >= 5 else "DISSATISFIED"
        else:
            row["customer_satisfaction"] = "DISSATISFIED"
        row["customer_onboarding_satisfaction_score"] = _customer_onboarding_satisfaction_score(row)
        row["customer_onboarding_feedback"] = _feedback_text_from_score(
            _int_value(row["customer_onboarding_satisfaction_score"], 0),
            cfg,
            "onboarding_feedback_text",
            row.get("customer_hash_key", ""),
        )


def _apply_onboarding_feedback_distribution(tables: dict[str, list[dict]], cfg: dict | None = None) -> None:
    customer_rows = tables.get("sat_customer", [])
    if not customer_rows:
        return
    distribution = ((cfg or {}).get("nps_settings") or {}).get("onboarding_feedback_distribution") or {
        "NEGATIVE": 20,
        "NEUTRAL": 30,
        "POSITIVE": 50,
    }
    counts = _distribution_counts(
        len(customer_rows),
        distribution,
        {
            "NEGATIVE": {"NEGATIVE", "LOW", "DETRACTOR"},
            "NEUTRAL": {"NEUTRAL", "MODERATE", "PASSIVE"},
            "POSITIVE": {"POSITIVE", "HIGH", "PROMOTER"},
        },
    )
    ranked = [
        (_int_value(row.get("nps_score"), 0), idx, row)
        for idx, row in enumerate(customer_rows)
    ]
    ranked_low = sorted(ranked)
    ranked_mid = sorted(ranked, key=lambda item: (abs(item[0] - 7.5), item[1]))
    ranked_high = sorted(ranked, reverse=True)
    used_rows: set[int] = set()
    bucket_rows = {
        "NEGATIVE": _take_ranked_rows([[item for item in ranked_low if item[0] <= 6]], counts.get("NEGATIVE", 0), ranked_low, used_rows),
        "NEUTRAL": _take_ranked_rows([[item for item in ranked_mid if 7 <= item[0] <= 8]], counts.get("NEUTRAL", 0), ranked_mid, used_rows),
        "POSITIVE": _take_ranked_rows([[item for item in ranked_high if item[0] >= 9]], counts.get("POSITIVE", 0), ranked_high, used_rows),
    }
    phrase_bank = _expanded_feedback_phrases(cfg, "onboarding_feedback_text", "onboarding_feedback_unique_counts")
    score_by_sentiment = {"NEGATIVE": "1", "NEUTRAL": "3", "POSITIVE": "5"}
    satisfaction_by_sentiment = {"NEGATIVE": "DISSATISFIED", "NEUTRAL": "NEUTRAL", "POSITIVE": "SATISFIED"}
    for sentiment, rows in bucket_rows.items():
        phrases = phrase_bank.get(sentiment) or [sentiment]
        for pos, item in enumerate(rows):
            row = item
            row["customer_onboarding_feedback"] = phrases[pos % len(phrases)]
            row["customer_onboarding_satisfaction_score"] = score_by_sentiment[sentiment]
            row["customer_satisfaction"] = satisfaction_by_sentiment[sentiment]


def _normalize_account_timeline(tables: dict[str, list[dict]]) -> None:
    for row in tables.get("sat_account", []):
        status = str(row.get("account_status") or "").strip().upper()
        load_dt = _date_obj(row.get("load_date"))
        change_dt = _date_obj(row.get("account_last_change"))
        access_dt = _date_obj(row.get("account_last_access"))
        if not change_dt:
            continue
        if load_dt and change_dt > load_dt:
            change_dt = load_dt
            row["account_last_change"] = _timestamp_from_date(change_dt, "08:18:25")
        if not access_dt:
            continue
        if status == "OPEN" and access_dt < change_dt:
            access_dt = change_dt
        elif status != "OPEN" and access_dt > change_dt:
            access_dt = change_dt
        if load_dt and access_dt > load_dt:
            access_dt = load_dt
        row["account_last_access"] = _timestamp_from_date(access_dt, "23:59:59" if status == "OPEN" else "00:00:00")


def _ensure_agent_broker_links(tables: dict[str, list[dict]]) -> None:
    broker_hks = [row.get("broker_hash_key") for row in tables.get("hub_broker", []) if row.get("broker_hash_key")]
    if not broker_hks:
        return
    broker_hk = broker_hks[0]
    customer_person = {
        row.get("customer_hash_key"): row.get("person_hash_key")
        for row in tables.get("link_customer_person", [])
        if row.get("customer_hash_key") and row.get("person_hash_key")
    }
    policy_person = {
        row.get("policy_hash_key"): customer_person.get(row.get("customer_hash_key"))
        for row in tables.get("link_policy_customer", [])
        if row.get("policy_hash_key") and customer_person.get(row.get("customer_hash_key"))
    }
    existing_policy_brokers = {
        row.get("policy_hash_key")
        for row in tables.get("link_policy_broker", [])
        if row.get("policy_hash_key")
    }
    existing_broker_person = {
        (row.get("broker_hash_key"), row.get("person_hash_key"))
        for row in tables.get("link_broker_person", [])
        if row.get("broker_hash_key") and row.get("person_hash_key")
    }
    for row in tables.get("sat_policy", []):
        if str(row.get("sales_channel") or "").strip().upper() != "AGENT":
            continue
        policy_hk = row.get("policy_hash_key")
        person_hk = policy_person.get(policy_hk)
        load_date = row.get("load_date") or ""
        if policy_hk and policy_hk not in existing_policy_brokers:
            tables["link_policy_broker"].append(_link_row(
                "link_policy_broker",
                "broker_hash_key",
                broker_hk,
                "policy_hash_key",
                policy_hk,
                load_date,
            ))
            existing_policy_brokers.add(policy_hk)
        if person_hk and (broker_hk, person_hk) not in existing_broker_person:
            tables["link_broker_person"].append(_link_row(
                "link_broker_person",
                "person_hash_key",
                person_hk,
                "broker_hash_key",
                broker_hk,
                load_date,
            ))
            existing_broker_person.add((broker_hk, person_hk))


def _apply_nps_claim_service_distribution(tables: dict[str, list[dict]], cfg: dict | None = None) -> None:
    ranked = _claim_customer_nps_rank(tables)
    if not ranked:
        return
    settings = ((cfg or {}).get("nps_settings") or {})
    settlement_distribution = settings.get("claim_settlement_tat_distribution") or {
        "DAYS_0_15": 70,
        "DAYS_16_30": 20,
        "DAYS_GT_30": 10,
    }
    channel_distribution = settings.get("claim_channel_distribution") or {
        "ONLINE": 70,
        "AGENT": 20,
        "BRANCH": 10,
    }
    settlement_counts = _distribution_counts(
        len(ranked),
        settlement_distribution,
        {
            "DAYS_0_15": {"DAYS_0_15", "0_15", "FAST"},
            "DAYS_16_30": {"DAYS_16_30", "16_30", "MODERATE"},
            "DAYS_GT_30": {"DAYS_GT_30", "GT_30", "SLOW"},
        },
    )
    channel_counts = _distribution_counts(
        len(ranked),
        channel_distribution,
        {
            "ONLINE": {"ONLINE", "DIGITAL", "WEB"},
            "AGENT": {"AGENT", "BROKER"},
            "BRANCH": {"BRANCH", "STORE"},
        },
    )
    ranked_low = sorted(ranked)
    ranked_high = sorted(ranked, reverse=True)
    ranked_mid = sorted(ranked, key=lambda item: (abs(item[0] - 7.5), item[1]))

    def assign_bands(counts: dict[str, int], order: dict[str, list[list[tuple[int, int, dict]]]]) -> dict[str, list[dict]]:
        used_rows: set[int] = set()
        assigned = {}
        fallback = ranked_high + ranked_mid + ranked_low
        for band, groups in order.items():
            assigned[band] = _take_ranked_rows(groups, counts.get(band, 0), fallback, used_rows)
        return assigned

    settlement_rows = assign_bands(
        settlement_counts,
        {
            "DAYS_GT_30": [[item for item in ranked_low if item[0] <= 6]],
            "DAYS_16_30": [[item for item in ranked_mid if 7 <= item[0] <= 8]],
            "DAYS_0_15": [[item for item in ranked_high if item[0] >= 9]],
        },
    )
    policy_by_claim = {
        row.get("claim_hash_key"): row.get("policy_hash_key")
        for row in tables.get("link_claim_policy", [])
        if row.get("claim_hash_key") and row.get("policy_hash_key")
    }
    policy_by_hk = {row.get("policy_hash_key"): row for row in tables.get("sat_policy", []) if row.get("policy_hash_key")}
    day_ranges = {"DAYS_0_15": (0, 15), "DAYS_16_30": (16, 30), "DAYS_GT_30": (31, 75)}
    for band, rows in settlement_rows.items():
        min_days, max_days = day_ranges[band]
        for idx, row in enumerate(rows):
            days = min_days + (idx % (max_days - min_days + 1))
            policy = policy_by_hk.get(policy_by_claim.get(row.get("claim_hash_key")), {})
            start = _date_obj(row.get("claim_reported_date")) or _date_obj(policy.get("policy_start_date")) or _date_obj(row.get("load_date")) or date.today()
            policy_start = _date_obj(policy.get("policy_start_date"))
            policy_end = _date_obj(policy.get("policy_end_date"))
            if policy_start and start < policy_start:
                start = policy_start
            if policy_end and start + timedelta(days=days) > policy_end:
                start = max(policy_start or policy_end, policy_end - timedelta(days=days))
            settlement = start + timedelta(days=days)
            row["claim_reported_date"] = _timestamp_from_date(start, "00:00:00")
            row["claim_settlement_date"] = _timestamp_from_date(settlement, "23:59:59")

    channel_rows = assign_bands(
        channel_counts,
        {
            "BRANCH": [[item for item in ranked_low if item[0] <= 6]],
            "AGENT": [[item for item in ranked_mid if 7 <= item[0] <= 8]],
            "ONLINE": [[item for item in ranked_high if item[0] >= 9]],
        },
    )
    for channel, rows in channel_rows.items():
        for row in rows:
            row["claim_channel"] = channel

    policy_channel_by_hk = {
        row.get("policy_hash_key"): row
        for row in tables.get("sat_policy", [])
        if row.get("policy_hash_key")
    }
    channel_hk_by_name = {
        str(row.get("channel_name") or "").upper(): row.get("channel_hash_key")
        for row in tables.get("sat_channel", [])
        if row.get("channel_name") and row.get("channel_hash_key")
    }
    policy_channel_links = {
        row.get("policy_hash_key"): row
        for row in tables.get("link_policy_channel", [])
        if row.get("policy_hash_key")
    }
    for row in tables.get("sat_claim", []):
        policy_hk = policy_by_claim.get(row.get("claim_hash_key"))
        policy_row = policy_channel_by_hk.get(policy_hk)
        channel = str(row.get("claim_channel") or "").upper()
        if policy_row is not None and channel:
            policy_row["sales_channel"] = channel
        link = policy_channel_links.get(policy_hk)
        channel_hk = channel_hk_by_name.get(channel)
        if link is not None and channel_hk:
            link["channel_hash_key"] = channel_hk
            link["policy_channel_hash_key"] = md5_hasher(f"{channel_hk}|{policy_hk}")

    for nps, _, row in ranked:
        if nps >= 9:
            row["claim_satisfaction_score"] = "5" if nps == 10 else "4"
        elif nps >= 7:
            row["claim_satisfaction_score"] = "4" if nps == 8 else "3"
        elif nps >= 4:
            row["claim_satisfaction_score"] = "3" if nps >= 5 else "2"
        else:
            row["claim_satisfaction_score"] = "1"
        row["claims_feedback"] = _feedback_from_score(_int_value(row["claim_satisfaction_score"], 0))


def _apply_nps_complaint_outcome_distribution(tables: dict[str, list[dict]], cfg: dict | None = None) -> None:
    ranked = _complaint_customer_nps_rank(tables)
    if not ranked:
        return
    settings = ((cfg or {}).get("nps_settings") or {})
    escalation_distribution = settings.get("complaint_escalation_distribution") or {
        "NON_ESCALATED": 98,
        "ESCALATED": 2,
    }
    status_distribution = settings.get("complaint_status_outcome_distribution") or {
        "NOT_UPHELD": 65,
        "UPHELD": 20,
        "PARTIALLY_UPHELD": 15,
    }
    escalation_counts = _distribution_counts(
        len(ranked),
        escalation_distribution,
        {
            "ESCALATED": {"ESCALATED", "Y", "YES", "TRUE"},
            "NON_ESCALATED": {"NON_ESCALATED", "N", "NO", "FALSE"},
        },
    )
    ranked_low = sorted(ranked)
    ranked_mid_low = sorted(ranked, key=lambda item: (abs(item[0] - 4.5), item[1]))
    ranked_mid = sorted(ranked, key=lambda item: (abs(item[0] - 5.5), item[1]))
    used_rows: set[int] = set()
    escalated_rows = _take_ranked_rows(
        [[item for item in ranked_low if item[0] <= 2]],
        escalation_counts.get("ESCALATED", 0),
        ranked_low,
        used_rows,
    )
    escalated_ids = {id(row) for row in escalated_rows}
    for _, _, row in ranked:
        row["is_financial_ombudsman_service_referral"] = "Y" if id(row) in escalated_ids else "N"

    status_counts = _distribution_counts(
        len(ranked),
        status_distribution,
        {
            "NOT_UPHELD": {"NOT_UPHELD", "NOT UPHELD"},
            "UPHELD": {"UPHELD"},
            "PARTIALLY_UPHELD": {"PARTIALLY_UPHELD", "PARTIALLY UPHELD"},
        },
    )
    used_rows = set()
    status_rows = {
        "NOT_UPHELD": _take_ranked_rows([[item for item in ranked_low if item[0] <= 4]], status_counts.get("NOT_UPHELD", 0), ranked_low, used_rows),
        "PARTIALLY_UPHELD": _take_ranked_rows([[item for item in ranked_mid_low if 3 <= item[0] <= 6]], status_counts.get("PARTIALLY_UPHELD", 0), ranked_mid_low, used_rows),
        "UPHELD": _take_ranked_rows([[item for item in ranked_mid if 4 <= item[0] <= 7]], status_counts.get("UPHELD", 0), ranked_mid, used_rows),
    }
    status_label = {
        "NOT_UPHELD": "Not Upheld",
        "PARTIALLY_UPHELD": "Partially Upheld",
        "UPHELD": "Upheld",
    }
    for status, rows in status_rows.items():
        for row in rows:
            row["complaint_upheld_status"] = status_label[status]
            _sync_complaint_status_with_dates(row)
            complaint_score = _complaint_satisfaction_score(row)
            row["customer_complaint_satisfaction_score"] = complaint_score
            row["complaint_feedback"] = _feedback_from_score(_int_value(complaint_score, 0)) if complaint_score != "" else ""


def _apply_nps_repeat_complaint_distribution(tables: dict[str, list[dict]], cfg: dict | None = None) -> None:
    links = tables.get("link_complaint_policy", [])
    if len(links) < 2:
        return
    nps_settings = ((cfg or {}).get("nps_settings") or {})
    distribution = ((cfg or {}).get("nps_settings") or {}).get("repeat_complaint_distribution") or {
        "NO_REPEAT": 90,
        "REPEAT": 10,
    }
    repeat_weight = sum(float(weight) for key, weight in distribution.items() if str(key).upper() in {"REPEAT", "Y", "YES"})
    total_weight = sum(float(weight) for weight in distribution.values()) or 100.0
    target_repeat_customers = min(len(links) // 2, max(1, round(len(links) * repeat_weight / (total_weight + repeat_weight))))

    nps_by_customer = {
        row.get("customer_hash_key"): _int_value(row.get("nps_score"), 0)
        for row in tables.get("sat_customer", [])
        if row.get("customer_hash_key")
    }
    policy_customer = {
        row.get("policy_hash_key"): row.get("customer_hash_key")
        for row in tables.get("link_policy_customer", [])
        if row.get("policy_hash_key") and row.get("customer_hash_key")
    }
    policies_by_customer: dict[str, list[str]] = {}
    for policy_hk, customer_hk in policy_customer.items():
        policies_by_customer.setdefault(customer_hk, []).append(policy_hk)
    claim_policies = list(dict.fromkeys(
        row.get("policy_hash_key")
        for row in tables.get("link_claim_policy", [])
        if row.get("policy_hash_key") and row.get("policy_hash_key") in policy_customer
    ))
    if not claim_policies:
        return

    claim_distribution = nps_settings.get("claim_complaint_distribution") or {"NO_COMPLAINT": 85, "COMPLAINT": 15}
    complaint_weight = sum(
        float(weight)
        for key, weight in claim_distribution.items()
        if str(key).upper() in {"COMPLAINT", "Y", "YES", "TRUE"}
    )
    claim_total_weight = sum(float(weight) for weight in claim_distribution.values()) or 100.0
    target_claim_policies = min(len(claim_policies), len(links) - target_repeat_customers, round(len(claim_policies) * complaint_weight / claim_total_weight))

    def policy_nps(policy_hk: str) -> int:
        return nps_by_customer.get(policy_customer.get(policy_hk), 0)

    used_policy_hks: set[str] = set()
    used_customer_hks: set[str] = set()
    selected_policies: list[str] = []

    low_claim_policies = sorted(claim_policies, key=lambda policy_hk: (policy_nps(policy_hk), policy_hk))
    for policy_hk in low_claim_policies:
        if len(selected_policies) >= target_claim_policies:
            break
        customer_hk = policy_customer.get(policy_hk)
        if customer_hk in used_customer_hks:
            continue
        selected_policies.append(policy_hk)
        used_policy_hks.add(policy_hk)
        if customer_hk:
            used_customer_hks.add(customer_hk)

    repeat_customers = []
    for policy_hk in selected_policies:
        customer_hk = policy_customer.get(policy_hk)
        if customer_hk and nps_by_customer.get(customer_hk, 0) <= 3 and customer_hk not in repeat_customers:
            repeat_customers.append(customer_hk)
        if len(repeat_customers) >= target_repeat_customers:
            break
    for customer_hk in repeat_customers:
        policy_hks = policies_by_customer.get(customer_hk, [])
        if policy_hks:
            selected_policies.append(policy_hks[0])

    status_distribution = nps_settings.get("complaint_status_outcome_distribution") or {
        "NOT_UPHELD": 65,
        "UPHELD": 20,
        "PARTIALLY_UPHELD": 15,
    }
    status_counts = _distribution_counts(
        len(links),
        status_distribution,
        {
            "NOT_UPHELD": {"NOT_UPHELD", "NOT UPHELD"},
            "UPHELD": {"UPHELD"},
            "PARTIALLY_UPHELD": {"PARTIALLY_UPHELD", "PARTIALLY UPHELD"},
        },
    )

    non_claim_policies = [
        row.get("policy_hash_key")
        for row in tables.get("link_policy_customer", [])
        if row.get("policy_hash_key") and row.get("policy_hash_key") not in set(claim_policies)
    ]
    non_claim_policies = list(dict.fromkeys(non_claim_policies))

    def add_policies(candidates: list[str], limit: int) -> None:
        added = 0
        for policy_hk in candidates:
            if added >= limit or len(selected_policies) >= len(links):
                break
            if policy_hk in used_policy_hks:
                continue
            customer_hk = policy_customer.get(policy_hk)
            if customer_hk in used_customer_hks:
                continue
            selected_policies.append(policy_hk)
            used_policy_hks.add(policy_hk)
            if customer_hk:
                used_customer_hks.add(customer_hk)
            added += 1

    low_non_claim = sorted([policy_hk for policy_hk in non_claim_policies if policy_nps(policy_hk) <= 4], key=lambda policy_hk: (policy_nps(policy_hk), policy_hk))
    partial_non_claim = sorted([policy_hk for policy_hk in non_claim_policies if 3 <= policy_nps(policy_hk) <= 6], key=lambda policy_hk: (abs(policy_nps(policy_hk) - 4.5), policy_hk))
    upheld_non_claim = sorted([policy_hk for policy_hk in non_claim_policies if 4 <= policy_nps(policy_hk) <= 7], key=lambda policy_hk: (abs(policy_nps(policy_hk) - 5.5), policy_hk))
    high_non_claim = sorted([policy_hk for policy_hk in non_claim_policies if policy_nps(policy_hk) >= 8], key=lambda policy_hk: (-policy_nps(policy_hk), policy_hk))
    fallback = sorted(non_claim_policies, key=lambda policy_hk: (-policy_nps(policy_hk), policy_hk))

    low_assigned = len(selected_policies)
    high_resolution_target = max(1, round(len(links) * 0.12))
    add_policies(high_non_claim + fallback, high_resolution_target)
    low_assigned = len(selected_policies)
    remaining_low_needed = max(0, status_counts.get("NOT_UPHELD", 0) - low_assigned)
    add_policies(low_non_claim, remaining_low_needed)
    add_policies(partial_non_claim, status_counts.get("PARTIALLY_UPHELD", 0))
    add_policies(upheld_non_claim, status_counts.get("UPHELD", 0))
    add_policies(high_non_claim + fallback, len(links) - len(selected_policies))

    policy_by_hk = {
        row.get("policy_hash_key"): row
        for row in tables.get("sat_policy", [])
        if row.get("policy_hash_key")
    }
    complaint_by_hk = {
        row.get("complaint_hash_key"): row
        for row in tables.get("sat_complaint", [])
        if row.get("complaint_hash_key")
    }
    customer_since = {
        row.get("customer_hash_key"): _date_obj(row.get("customer_since"))
        for row in tables.get("sat_customer", [])
        if row.get("customer_hash_key")
    }
    product_by_hk = {
        row.get("product_hash_key"): row.get("type")
        for row in tables.get("sat_product", [])
        if row.get("product_hash_key")
    }
    policy_product_code = {
        row.get("policy_hash_key"): product_by_hk.get(row.get("product_hash_key"), "")
        for row in tables.get("link_policy_product", [])
        if row.get("policy_hash_key") and row.get("product_hash_key")
    }
    for link, policy_hk in zip(links, selected_policies):
        complaint_hk = link.get("complaint_hash_key")
        link["policy_hash_key"] = policy_hk
        link["complaint_policy_hash_key"] = md5_hasher(f"{complaint_hk}|{policy_hk}")
        complaint_row = complaint_by_hk.get(complaint_hk)
        policy_row = policy_by_hk.get(policy_hk)
        if complaint_row is None or policy_row is None:
            continue
        complaint_row["complaint_channel"] = policy_row.get("sales_channel", complaint_row.get("complaint_channel", ""))
        category = insurance_category_for_code(policy_product_code.get(policy_hk, ""))
        if category:
            complaint_row["insurance_category"] = category
        min_date = customer_since.get(policy_customer.get(policy_hk))
        opened = _date_obj(complaint_row.get("complaint_date")) or min_date
        if min_date and opened and opened < min_date:
            opened = min_date
            complaint_row["complaint_date"] = _timestamp_from_date(opened, "00:00:00")
        ack = _date_obj(complaint_row.get("complaint_acknowledgement_date")) or opened
        if opened and ack and ack < opened:
            ack = opened
            complaint_row["complaint_acknowledgement_date"] = _timestamp_from_date(ack, "12:00:00")
        resolved = _date_obj(complaint_row.get("complaint_resolved_date"))
        if ack and resolved and resolved < ack:
            complaint_row["complaint_resolved_date"] = _timestamp_from_date(ack, "23:59:59")
        _sync_complaint_status_with_dates(complaint_row)


def _apply_quote_premium_increase_nps_alignment(tables: dict[str, list[dict]], cfg: dict | None = None) -> None:
    ranked = _quote_customer_nps_rank(tables)
    if not ranked:
        return
    distribution = ((cfg or {}).get("nps_settings") or {}).get("premium_increase_distribution") or {
        "LE_5": 70,
        "GT_5_LE_10": 20,
        "GT_10": 10,
    }

    def apply_to_ranked_rows(ranked_rows: list[tuple[int, int, dict]], current_col: str, next_col: str) -> None:
        if not ranked_rows:
            return
        total_weight = sum(float(weight) for weight in distribution.values()) or 100.0
        counts = {band: round(len(ranked_rows) * float(weight) / total_weight) for band, weight in distribution.items()}
        while sum(counts.values()) > len(ranked_rows):
            counts[max(counts, key=counts.get)] -= 1
        while sum(counts.values()) < len(ranked_rows):
            counts[max(counts, key=lambda key: distribution.get(key, 0))] += 1

        ranked_low = sorted(ranked_rows)
        ranked_high = sorted(ranked_rows, reverse=True)
        ranked_mid = sorted(ranked_rows, key=lambda item: (abs(item[0] - 7.5), item[1]))
        used_rows: set[int] = set()
        band_rows = {
            "GT_10": _take_ranked_rows([[item for item in ranked_low if item[0] <= 6]], counts.get("GT_10", 0), ranked_low, used_rows),
            "GT_5_LE_10": _take_ranked_rows([[item for item in ranked_mid if 7 <= item[0] <= 8]], counts.get("GT_5_LE_10", 0), ranked_mid, used_rows),
            "LE_5": _take_ranked_rows([[item for item in ranked_high if item[0] >= 9]], counts.get("LE_5", 0), ranked_high, used_rows),
        }
        rate_ranges = {
            "LE_5": (0.00, 0.05),
            "GT_5_LE_10": (0.055, 0.10),
            "GT_10": (0.12, 0.30),
        }
        for band, rows in band_rows.items():
            low, high = rate_ranges[band]
            spread = high - low
            for idx, row in enumerate(rows):
                current = _float_value(row.get(current_col), 0.0)
                if current <= 0:
                    current = 300.0 + (idx % 900)
                rate = low + (spread * ((idx % 17) / 16))
                row[current_col] = str(round(current, 2))
                row[next_col] = str(round(current * (1 + rate), 2))

    apply_to_ranked_rows(ranked, "renewal_amt_current_period", "renewal_amt_next_period")

    nps_by_customer = {
        row.get("customer_hash_key"): _int_value(row.get("nps_score"), 0)
        for row in tables.get("sat_customer", [])
        if row.get("customer_hash_key")
    }
    policy_customer = {
        row.get("policy_hash_key"): row.get("customer_hash_key")
        for row in tables.get("link_policy_customer", [])
        if row.get("policy_hash_key") and row.get("customer_hash_key")
    }
    policy_ranked = [
        (nps_by_customer.get(policy_customer.get(row.get("policy_hash_key")), 0), -idx, row)
        for idx, row in enumerate(tables.get("sat_policy", []))
        if row.get("policy_hash_key") in policy_customer
    ]
    apply_to_ranked_rows(policy_ranked, "renewal_amount_current_period", "renewal_amount_next_period")


def _apply_digital_renewal_nps_alignment(tables: dict[str, list[dict]], cfg: dict | None = None) -> None:
    policy_rows = []
    for idx, row in enumerate(tables.get("sat_policy", [])):
        if _int_value(row.get("policy_cycle"), 0) > 1:
            policy_rows.append((idx, row))
    if not policy_rows:
        return

    distribution = ((cfg or {}).get("nps_settings") or {}).get("digital_renewal_distribution") or {
        "ONLINE": 70,
        "AGENT": 20,
        "BRANCH": 10,
    }
    total_weight = sum(float(weight) for weight in distribution.values()) or 100.0
    counts = {str(channel).upper(): round(len(policy_rows) * float(weight) / total_weight) for channel, weight in distribution.items()}
    while sum(counts.values()) > len(policy_rows):
        counts[max(counts, key=counts.get)] -= 1
    while sum(counts.values()) < len(policy_rows):
        counts[max(counts, key=lambda key: distribution.get(key, 0))] += 1

    nps_by_customer = {
        row.get("customer_hash_key"): _int_value(row.get("nps_score"), 0)
        for row in tables.get("sat_customer", [])
        if row.get("customer_hash_key")
    }
    policy_customer = {
        row.get("policy_hash_key"): row.get("customer_hash_key")
        for row in tables.get("link_policy_customer", [])
        if row.get("policy_hash_key") and row.get("customer_hash_key")
    }
    ranked = [
        (nps_by_customer.get(policy_customer.get(row.get("policy_hash_key")), 0), -idx, row)
        for idx, row in policy_rows
    ]
    ranked_low = sorted(ranked)
    ranked_high = sorted(ranked, reverse=True)
    ranked_mid = sorted(ranked, key=lambda item: (abs(item[0] - 7.5), item[1]))
    used_rows: set[int] = set()
    existing_agent = [item for item in ranked_mid if str(item[2].get("sales_channel") or "").upper() == "AGENT"]
    channel_rows = {
        "BRANCH": _take_ranked_rows([[item for item in ranked_low if item[0] <= 6]], counts.get("BRANCH", 0), ranked_low, used_rows),
        "AGENT": _take_ranked_rows([existing_agent, [item for item in ranked_mid if 7 <= item[0] <= 8]], counts.get("AGENT", 0), existing_agent, used_rows),
        "ONLINE": _take_ranked_rows([[item for item in ranked_high if item[0] >= 9]], counts.get("ONLINE", 0), ranked_high, used_rows),
    }
    for channel, rows in channel_rows.items():
        for row in rows:
            row["sales_channel"] = channel

    channel_hk_by_name = {
        str(row.get("channel_name") or "").upper(): row.get("channel_hash_key")
        for row in tables.get("sat_channel", [])
        if row.get("channel_name") and row.get("channel_hash_key")
    }
    policy_channel = {
        row.get("policy_hash_key"): row
        for row in tables.get("link_policy_channel", [])
        if row.get("policy_hash_key")
    }
    for _, row in policy_rows:
        link = policy_channel.get(row.get("policy_hash_key"))
        channel_hk = channel_hk_by_name.get(str(row.get("sales_channel") or "").upper())
        if link is not None and channel_hk:
            link["channel_hash_key"] = channel_hk
            link["policy_channel_hash_key"] = md5_hasher(f"{channel_hk}|{row.get('policy_hash_key')}")

    claim_policy = {
        row.get("policy_hash_key"): row.get("claim_hash_key")
        for row in tables.get("link_claim_policy", [])
        if row.get("policy_hash_key") and row.get("claim_hash_key")
    }
    claim_by_hk = {
        row.get("claim_hash_key"): row
        for row in tables.get("sat_claim", [])
        if row.get("claim_hash_key")
    }
    for _, row in policy_rows:
        claim_row = claim_by_hk.get(claim_policy.get(row.get("policy_hash_key")))
        if claim_row is not None:
            claim_row["claim_channel"] = row.get("sales_channel", claim_row.get("claim_channel", ""))

    broker_hks = [row.get("broker_hash_key") for row in tables.get("hub_broker", []) if row.get("broker_hash_key")]
    if broker_hks:
        broker_hk = broker_hks[0]
        policy_broker = {
            row.get("policy_hash_key")
            for row in tables.get("link_policy_broker", [])
            if row.get("policy_hash_key")
        }
        broker_person = {
            (row.get("broker_hash_key"), row.get("person_hash_key"))
            for row in tables.get("link_broker_person", [])
            if row.get("broker_hash_key") and row.get("person_hash_key")
        }
        customer_person = {
            row.get("customer_hash_key"): row.get("person_hash_key")
            for row in tables.get("link_customer_person", [])
            if row.get("customer_hash_key") and row.get("person_hash_key")
        }
        policy_person = {
            row.get("policy_hash_key"): customer_person.get(row.get("customer_hash_key"))
            for row in tables.get("link_policy_customer", [])
            if row.get("policy_hash_key") and customer_person.get(row.get("customer_hash_key"))
        }
        for row in tables.get("sat_policy", []):
            if str(row.get("sales_channel") or "").upper() != "AGENT":
                continue
            policy_hk = row.get("policy_hash_key")
            person_hk = policy_person.get(policy_hk)
            if policy_hk and policy_hk not in policy_broker:
                tables["link_policy_broker"].append(_link_row(
                    "link_policy_broker",
                    "broker_hash_key",
                    broker_hk,
                    "policy_hash_key",
                    policy_hk,
                    row.get("load_date") or "",
                ))
                policy_broker.add(policy_hk)
            if person_hk and (broker_hk, person_hk) not in broker_person:
                tables["link_broker_person"].append(_link_row(
                    "link_broker_person",
                    "person_hash_key",
                    person_hk,
                    "broker_hash_key",
                    broker_hk,
                    row.get("load_date") or "",
                ))
                broker_person.add((broker_hk, person_hk))


def _apply_claim_complaint_nps_alignment(tables: dict[str, list[dict]], cfg: dict | None = None) -> None:
    complaint_links = tables.get("link_complaint_policy", [])
    if not complaint_links:
        return
    distribution = ((cfg or {}).get("nps_settings") or {}).get("claim_complaint_distribution") or {
        "NO_COMPLAINT": 85,
        "COMPLAINT": 15,
    }
    claim_policies = list(dict.fromkeys(
        row.get("policy_hash_key")
        for row in tables.get("link_claim_policy", [])
        if row.get("policy_hash_key")
    ))
    if not claim_policies:
        return
    complaint_weight = sum(
        float(weight)
        for value, weight in distribution.items()
        if str(value).upper() in {"COMPLAINT", "Y", "YES", "TRUE"}
    )
    total_weight = sum(float(weight) for weight in distribution.values()) or 100.0
    target = min(len(complaint_links), round(len(claim_policies) * complaint_weight / total_weight))

    nps_by_customer = {
        row.get("customer_hash_key"): _int_value(row.get("nps_score"), 0)
        for row in tables.get("sat_customer", [])
        if row.get("customer_hash_key")
    }
    policy_customer = {
        row.get("policy_hash_key"): row.get("customer_hash_key")
        for row in tables.get("link_policy_customer", [])
        if row.get("policy_hash_key") and row.get("customer_hash_key")
    }
    ranked_policies = sorted(
        (nps_by_customer.get(policy_customer.get(policy_hk), 0), idx, policy_hk)
        for idx, policy_hk in enumerate(claim_policies)
    )
    detractor_claim_policies = _spread_ranked_by_score(ranked_policies, [0, 1, 2, 3, 4, 5, 6])
    selected_policies = [policy_hk for _, _, policy_hk in detractor_claim_policies[:target]]
    if len(selected_policies) < target:
        selected_policies.extend(
            policy_hk
            for _, _, policy_hk in ranked_policies
            if policy_hk not in set(selected_policies)
        )
        selected_policies = selected_policies[:target]
    selected_set = set(selected_policies)
    non_claim_policies = [
        row.get("policy_hash_key")
        for row in tables.get("link_policy_customer", [])
        if row.get("policy_hash_key") and row.get("policy_hash_key") not in set(claim_policies)
    ]
    ranked_non_claim = sorted(
        (
            nps_by_customer.get(policy_customer.get(policy_hk), 0),
            idx,
            policy_hk,
        )
        for idx, policy_hk in enumerate(dict.fromkeys(non_claim_policies))
    )
    # Fill extra complaint rows with high/moderate-NPS non-claim policies so
    # resolution-TAT NPS rules have representative fast/moderate complaint cases.
    extras_needed = max(0, len(complaint_links) - len(selected_policies))
    passive_extra = [policy_hk for nps, _, policy_hk in _spread_ranked_by_score(ranked_non_claim, [7, 8]) if 7 <= nps <= 8]
    high_extra = [policy_hk for nps, _, policy_hk in _spread_ranked_by_score(ranked_non_claim, [9, 10], reverse_within_score=True) if nps >= 9]
    fallback_extra = [
        policy_hk
        for _, _, policy_hk in (
            _spread_ranked_by_score(ranked_non_claim, [9, 10], reverse_within_score=True)
            + _spread_ranked_by_score(ranked_non_claim, [7, 8])
            + _spread_ranked_by_score(ranked_non_claim, [0, 1, 2, 3, 4, 5, 6])
        )
    ]
    high_target = max(1, round(extras_needed * 0.55)) if extras_needed else 0
    passive_target = max(0, extras_needed - high_target)

    def add_extra_policies(candidates: list[str], limit: int | None = None) -> None:
        added = 0
        for policy_hk in candidates:
            if len(selected_policies) >= len(complaint_links):
                break
            if limit is not None and added >= limit:
                break
            if policy_hk in selected_set:
                continue
            selected_policies.append(policy_hk)
            selected_set.add(policy_hk)
            added += 1

    add_extra_policies(high_extra, high_target)
    add_extra_policies(passive_extra, passive_target)
    add_extra_policies(high_extra + passive_extra + fallback_extra)
    policy_by_hk = {
        row.get("policy_hash_key"): row
        for row in tables.get("sat_policy", [])
        if row.get("policy_hash_key")
    }
    complaint_by_hk = {
        row.get("complaint_hash_key"): row
        for row in tables.get("sat_complaint", [])
        if row.get("complaint_hash_key")
    }
    product_by_hk = {
        row.get("product_hash_key"): row.get("type")
        for row in tables.get("sat_product", [])
        if row.get("product_hash_key")
    }
    policy_product_code = {
        row.get("policy_hash_key"): product_by_hk.get(row.get("product_hash_key"), "")
        for row in tables.get("link_policy_product", [])
        if row.get("policy_hash_key") and row.get("product_hash_key")
    }
    customer_since = {
        row.get("customer_hash_key"): _date_obj(row.get("customer_since"))
        for row in tables.get("sat_customer", [])
        if row.get("customer_hash_key")
    }
    for link, policy_hk in zip(complaint_links, selected_policies):
        complaint_hk = link.get("complaint_hash_key")
        link["policy_hash_key"] = policy_hk
        link["complaint_policy_hash_key"] = md5_hasher(f"{complaint_hk}|{policy_hk}")
        complaint_row = complaint_by_hk.get(complaint_hk)
        policy_row = policy_by_hk.get(policy_hk)
        if complaint_row is not None and policy_row is not None:
            complaint_row["complaint_channel"] = policy_row.get("sales_channel", complaint_row.get("complaint_channel", ""))
            product_code = policy_product_code.get(policy_hk, "")
            category = insurance_category_for_code(product_code)
            if category:
                complaint_row["insurance_category"] = category
            policy_customer_hk = policy_customer.get(policy_hk)
            min_date = customer_since.get(policy_customer_hk)
            complaint_date = _date_obj(complaint_row.get("complaint_date"))
            if min_date and complaint_date and complaint_date < min_date:
                complaint_date = min_date
                complaint_row["complaint_date"] = _timestamp_from_date(complaint_date, "00:00:00")
            ack_date = _date_obj(complaint_row.get("complaint_acknowledgement_date"))
            if complaint_date and ack_date and ack_date < complaint_date:
                ack_date = complaint_date
                complaint_row["complaint_acknowledgement_date"] = _timestamp_from_date(ack_date, "12:00:00")
            resolved_date = _date_obj(complaint_row.get("complaint_resolved_date"))
            min_resolved_date = ack_date or complaint_date
            if min_resolved_date and resolved_date and resolved_date < min_resolved_date:
                complaint_row["complaint_resolved_date"] = _timestamp_from_date(min_resolved_date, "23:59:59")
            _sync_complaint_status_with_dates(complaint_row)


def _apply_self_service_adoption_nps_alignment(tables: dict[str, list[dict]], cfg: dict | None = None) -> None:
    account_rows = tables.get("sat_account", [])
    customer_rows = tables.get("sat_customer", [])
    if not account_rows or not customer_rows:
        return

    distribution = ((cfg or {}).get("nps_settings") or {}).get("self_service_adoption_distribution") or {
        "ADOPTED": 65,
        "NOT_ADOPTED": 35,
    }
    adoption_weight = sum(
        float(weight)
        for value, weight in distribution.items()
        if str(value).upper() in {"ADOPTED", "Y", "YES", "TRUE", "1"}
    )
    total_weight = sum(float(weight) for weight in distribution.values()) or 100.0
    target = round(len(account_rows) * adoption_weight / total_weight)

    customer_to_person = {
        row.get("customer_hash_key"): row.get("person_hash_key")
        for row in tables.get("link_customer_person", [])
        if row.get("customer_hash_key") and row.get("person_hash_key")
    }
    nps_by_person = {}
    for row in customer_rows:
        person_hk = customer_to_person.get(row.get("customer_hash_key"))
        if person_hk:
            nps_by_person[person_hk] = _int_value(row.get("nps_score"), 0)

    consented_persons = {
        row.get("person_hash_key")
        for row in tables.get("link_person_consent", [])
        if row.get("person_hash_key")
    }
    account_to_person = {
        row.get("account_hash_key"): row.get("person_hash_key")
        for row in tables.get("link_person_account", [])
        if row.get("account_hash_key") and row.get("person_hash_key")
    }
    ranked = []
    for idx, row in enumerate(account_rows):
        person_hk = account_to_person.get(row.get("account_hash_key"))
        if person_hk in consented_persons:
            ranked.append((nps_by_person.get(person_hk, 0), -idx, row))

    adopted_rows = {
        id(row)
        for _, _, row in sorted(ranked, reverse=True)[:target]
    }
    for idx, row in enumerate(account_rows):
        load_dt = _date_obj(row.get("load_date"))
        if not load_dt:
            continue
        if id(row) in adopted_rows:
            row["account_creation_type"] = "ONLINE"
            row["account_last_access"] = _timestamp_from_date(load_dt - timedelta(days=idx % 30))
        else:
            row["account_last_access"] = _timestamp_from_date(load_dt - timedelta(days=45 + (idx % 180)))


def _apply_ml_master_grain_nps_alignment(tables: dict[str, list[dict]], cfg: dict | None = None) -> None:
    """Final NPS pass at the policy/customer grain used by the ML notebook.

    This intentionally updates only existing satellite attributes. It does not
    add/remove hubs, links, hash keys, or business keys.
    """
    nps_settings = ((cfg or {}).get("nps_settings") or {})
    customers = {
        row.get("customer_hash_key"): row
        for row in tables.get("sat_customer", [])
        if row.get("customer_hash_key")
    }
    policy_customer = {
        row.get("policy_hash_key"): row.get("customer_hash_key")
        for row in tables.get("link_policy_customer", [])
        if row.get("policy_hash_key") and row.get("customer_hash_key")
    }
    policy_by_hk = {
        row.get("policy_hash_key"): row
        for row in tables.get("sat_policy", [])
        if row.get("policy_hash_key")
    }
    policy_ranked = [
        (_int_value(customers.get(policy_customer.get(row.get("policy_hash_key")), {}).get("nps_score"), 0), -idx, row)
        for idx, row in enumerate(tables.get("sat_policy", []))
        if row.get("policy_hash_key") in policy_customer
    ]
    if not policy_ranked:
        return

    def assign_ranked(
        ranked: list[tuple[int, int, dict]],
        distribution: dict,
        aliases: dict[str, set[str]],
        band_groups: dict[str, list[list[tuple[int, int, dict]]]],
        fallback: list[tuple[int, int, dict]] | None = None,
    ) -> dict[str, list[dict]]:
        counts = _distribution_counts(len(ranked), distribution, aliases)
        used: set[int] = set()
        assigned = {}
        fallback_rows = fallback or ranked
        for band, groups in band_groups.items():
            assigned[band] = _take_ranked_rows(groups, counts.get(band, 0), fallback_rows, used)
        return assigned

    ranked_low = sorted(policy_ranked)
    ranked_high = sorted(policy_ranked, reverse=True)
    ranked_mid = sorted(policy_ranked, key=lambda item: (abs(item[0] - 7.5), item[1]))
    ranked_fallback = ranked_high + ranked_mid + ranked_low

    def apply_premium(rows: list[dict], band: str, current_col: str, next_col: str) -> None:
        ranges = {"LE_5": (0.00, 0.05), "GT_5_LE_10": (0.055, 0.10), "GT_10": (0.12, 0.30)}
        low, high = ranges[band]
        spread = high - low
        for idx, row in enumerate(rows):
            current = _float_value(row.get(current_col), 0.0)
            if current <= 0:
                current = 350.0 + (idx % 850)
            rate = low + spread * ((idx % 17) / 16)
            row[current_col] = str(round(current, 2))
            row[next_col] = str(round(current * (1 + rate), 2))

    premium_distribution = nps_settings.get("premium_increase_distribution") or {
        "LE_5": 70,
        "GT_5_LE_10": 20,
        "GT_10": 10,
    }
    premium_rows = assign_ranked(
        policy_ranked,
        premium_distribution,
        {
            "LE_5": {"LE_5", "0_5", "LOW"},
            "GT_5_LE_10": {"GT_5_LE_10", "5_10", "MEDIUM"},
            "GT_10": {"GT_10", "GT_10_PERCENT", "HIGH"},
        },
        {
            "GT_10": [[item for item in ranked_low if item[0] <= 6]],
            "GT_5_LE_10": [[item for item in ranked_mid if 7 <= item[0] <= 8]],
            "LE_5": [[item for item in ranked_high if item[0] >= 9]],
        },
        ranked_fallback,
    )
    for band, rows in premium_rows.items():
        apply_premium(rows, band, "renewal_amount_current_period", "renewal_amount_next_period")

    quote_by_hk = {
        row.get("quote_hash_key"): row
        for row in tables.get("sat_quote", [])
        if row.get("quote_hash_key")
    }
    quote_by_policy = {
        row.get("policy_hash_key"): quote_by_hk.get(row.get("quote_hash_key"))
        for row in tables.get("link_policy_quote", [])
        if row.get("policy_hash_key") and row.get("quote_hash_key")
    }
    for band, rows in premium_rows.items():
        quote_rows = [quote_by_policy.get(row.get("policy_hash_key")) for row in rows]
        apply_premium([row for row in quote_rows if row is not None], band, "renewal_amt_current_period", "renewal_amt_next_period")

    customer_person = {
        row.get("customer_hash_key"): row.get("person_hash_key")
        for row in tables.get("link_customer_person", [])
        if row.get("customer_hash_key") and row.get("person_hash_key")
    }
    person_by_account = {
        row.get("account_hash_key"): row.get("person_hash_key")
        for row in tables.get("link_person_account", [])
        if row.get("account_hash_key") and row.get("person_hash_key")
    }
    account_by_hk = {
        row.get("account_hash_key"): row
        for row in tables.get("sat_account", [])
        if row.get("account_hash_key")
    }
    consented_persons = {
        row.get("person_hash_key")
        for row in tables.get("link_person_consent", [])
        if row.get("person_hash_key")
    }
    account_for_policy = {}
    for policy_hk, customer_hk in policy_customer.items():
        person_hk = customer_person.get(customer_hk)
        if not person_hk:
            continue
        for account_hk, account_person in person_by_account.items():
            if account_person == person_hk and account_hk in account_by_hk:
                account_for_policy[policy_hk] = account_by_hk[account_hk]
                break
    self_service_distribution = nps_settings.get("self_service_adoption_distribution") or {
        "ADOPTED": 65,
        "NOT_ADOPTED": 35,
    }
    account_ranked_by_id = {}
    for nps, order, row in policy_ranked:
        account = account_for_policy.get(row.get("policy_hash_key"))
        if account is None:
            continue
        row_id = id(account)
        previous = account_ranked_by_id.get(row_id)
        if previous is None or (nps, order) > (previous[0], previous[1]):
            account_ranked_by_id[row_id] = (nps, order, account)
    account_ranked = list(account_ranked_by_id.values())
    account_rows = assign_ranked(
        account_ranked,
        self_service_distribution,
        {"ADOPTED": {"ADOPTED", "Y", "YES", "TRUE"}, "NOT_ADOPTED": {"NOT_ADOPTED", "N", "NO", "FALSE"}},
        {
            "ADOPTED": [[
                item
                for item in sorted(account_ranked, reverse=True)
                if item[0] >= 9 and person_by_account.get(item[2].get("account_hash_key")) in consented_persons
            ]],
            "NOT_ADOPTED": [[item for item in sorted(account_ranked) if item[0] <= 6]],
        },
        sorted(account_ranked, reverse=True) + sorted(account_ranked),
    )
    adopted_ids = {
        id(row)
        for row in account_rows.get("ADOPTED", [])
        if person_by_account.get(row.get("account_hash_key")) in consented_persons
    }
    digital_distribution = nps_settings.get("digital_onboarding_distribution") or {
        "ONLINE": 75,
        "BRANCH": 25,
    }
    digital_rows = assign_ranked(
        account_ranked,
        digital_distribution,
        {"ONLINE": {"ONLINE", "DIGITAL", "SELF_SERVICE", "SELF-SERVICE"}, "BRANCH": {"BRANCH", "ASSISTED"}},
        {
            "ONLINE": [[item for item in sorted(account_ranked, reverse=True) if item[0] >= 8]],
            "BRANCH": [[item for item in sorted(account_ranked) if item[0] <= 4]],
        },
        sorted(account_ranked, reverse=True) + sorted(account_ranked),
    )
    online_ids = {id(row) for row in digital_rows.get("ONLINE", [])}
    online_ids.update(adopted_ids)
    for idx, account in enumerate({id(row): row for _, _, row in account_ranked}.values()):
        load_dt = _date_obj(account.get("load_date")) or _date_obj(account.get("account_last_change")) or date.today()
        if id(account) in adopted_ids:
            account["account_creation_type"] = "ONLINE"
            account["account_status"] = "OPEN"
            access_dt = load_dt - timedelta(days=idx % 29)
            account["account_last_access"] = _timestamp_from_date(access_dt, "23:59:59")
            account["account_last_change"] = _timestamp_from_date(access_dt, "08:18:25")
        elif id(account) in online_ids:
            account["account_creation_type"] = "ONLINE"
            access_dt = load_dt - timedelta(days=45 + (idx % 180))
            account["account_last_access"] = _timestamp_from_date(access_dt, "00:00:00")
            account["account_last_change"] = _timestamp_from_date(access_dt, "08:18:25")
        else:
            account["account_creation_type"] = "BRANCH"
            access_dt = load_dt - timedelta(days=45 + (idx % 180))
            account["account_last_access"] = _timestamp_from_date(access_dt, "00:00:00")
            account["account_last_change"] = _timestamp_from_date(access_dt, "08:18:25")

    marketing_by_hk = {
        row.get("marketing_engagement_hash_key"): row
        for row in tables.get("sat_marketing_engagement", [])
        if row.get("marketing_engagement_hash_key")
    }
    marketing_by_person = {
        row.get("person_hash_key"): marketing_by_hk.get(row.get("marketing_engagement_hash_key"))
        for row in tables.get("link_person_marketing_engagement", [])
        if row.get("person_hash_key") and row.get("marketing_engagement_hash_key")
    }
    marketing_ranked = []
    seen_marketing: set[int] = set()
    for nps, order, policy_row in policy_ranked:
        person_hk = customer_person.get(policy_customer.get(policy_row.get("policy_hash_key")))
        marketing = marketing_by_person.get(person_hk)
        if marketing is not None and id(marketing) not in seen_marketing:
            marketing_ranked.append((nps, order, marketing))
            seen_marketing.add(id(marketing))
    contact_distribution = nps_settings.get("renewal_contact_distribution") or {
        "CONTACTS_0_1": 60,
        "CONTACTS_2_3": 30,
        "CONTACTS_GT_3": 10,
    }
    marketing_rows = assign_ranked(
        marketing_ranked,
        contact_distribution,
        {
            "CONTACTS_0_1": {"CONTACTS_0_1", "0_1", "LOW_CONTACT"},
            "CONTACTS_2_3": {"CONTACTS_2_3", "2_3", "MODERATE_CONTACT"},
            "CONTACTS_GT_3": {"CONTACTS_GT_3", "GT_3", "HIGH_CONTACT"},
        },
        {
            "CONTACTS_GT_3": [[item for item in sorted(marketing_ranked) if item[0] <= 6]],
            "CONTACTS_2_3": [[item for item in sorted(marketing_ranked, key=lambda item: (abs(item[0] - 7.5), item[1])) if 7 <= item[0] <= 8]],
            "CONTACTS_0_1": [[item for item in sorted(marketing_ranked, reverse=True) if item[0] >= 9]],
        },
        sorted(marketing_ranked, reverse=True) + sorted(marketing_ranked),
    )
    contact_values = {"CONTACTS_0_1": [0, 1], "CONTACTS_2_3": [2, 3], "CONTACTS_GT_3": [4, 5, 6]}
    for band, rows in marketing_rows.items():
        for idx, row in enumerate(rows):
            call_frequency = contact_values[band][idx % len(contact_values[band])]
            row["customer_service_call_frequency"] = str(call_frequency)
            row["average_call_sentiment"] = "NEGATIVE" if call_frequency >= 4 else "NEUTRAL" if call_frequency >= 2 else "POSITIVE"
            row["first_contact_resolution"] = "Y" if call_frequency <= 1 else "N"

    claim_by_hk = {
        row.get("claim_hash_key"): row
        for row in tables.get("sat_claim", [])
        if row.get("claim_hash_key")
    }
    claim_ranked = []
    for order, link in enumerate(tables.get("link_claim_policy", [])):
        claim = claim_by_hk.get(link.get("claim_hash_key"))
        customer_hk = policy_customer.get(link.get("policy_hash_key"))
        if claim is not None and customer_hk in customers:
            claim_ranked.append((_int_value(customers[customer_hk].get("nps_score"), 0), -order, claim))
    claim_sorted_low = sorted(claim_ranked)
    claim_sorted_high = sorted(claim_ranked, reverse=True)
    claim_sorted_mid = sorted(claim_ranked, key=lambda item: (abs(item[0] - 7.5), item[1]))
    claim_fallback = claim_sorted_high + claim_sorted_mid + claim_sorted_low
    if claim_ranked:
        settlement_distribution = nps_settings.get("claim_settlement_tat_distribution") or {
            "DAYS_0_15": 70,
            "DAYS_16_30": 20,
            "DAYS_GT_30": 10,
        }
        settlement_rows = assign_ranked(
            claim_ranked,
            settlement_distribution,
            {
                "DAYS_0_15": {"DAYS_0_15", "0_15", "FAST"},
                "DAYS_16_30": {"DAYS_16_30", "16_30", "MODERATE"},
                "DAYS_GT_30": {"DAYS_GT_30", "GT_30", "SLOW"},
            },
            {
                "DAYS_GT_30": [[item for item in claim_sorted_low if item[0] <= 6]],
                "DAYS_16_30": [[item for item in claim_sorted_mid if 7 <= item[0] <= 8]],
                "DAYS_0_15": [[item for item in claim_sorted_high if item[0] >= 9]],
            },
            claim_fallback,
        )
        day_ranges = {"DAYS_0_15": (0, 15), "DAYS_16_30": (16, 30), "DAYS_GT_30": (31, 75)}
        for band, rows in settlement_rows.items():
            min_days, max_days = day_ranges[band]
            for idx, row in enumerate(rows):
                reported = _date_obj(row.get("claim_reported_date")) or _date_obj(row.get("load_date")) or date.today()
                days = min_days + (idx % (max_days - min_days + 1))
                row["claim_reported_date"] = _timestamp_from_date(reported, "00:00:00")
                row["claim_settlement_date"] = _timestamp_from_date(reported + timedelta(days=days), "23:59:59")
        channel_distribution = nps_settings.get("claim_channel_distribution") or {
            "ONLINE": 70,
            "AGENT": 20,
            "BRANCH": 10,
        }
        channel_rows = assign_ranked(
            claim_ranked,
            channel_distribution,
            {"ONLINE": {"ONLINE", "DIGITAL", "WEB"}, "AGENT": {"AGENT", "BROKER"}, "BRANCH": {"BRANCH"}},
            {
                "BRANCH": [[item for item in claim_sorted_low if item[0] <= 6]],
                "AGENT": [[item for item in claim_sorted_mid if 7 <= item[0] <= 8]],
                "ONLINE": [[item for item in claim_sorted_high if item[0] >= 9]],
            },
            claim_fallback,
        )
        for channel, rows in channel_rows.items():
            for row in rows:
                row["claim_channel"] = channel
        for nps, _, row in claim_ranked:
            row["claim_satisfaction_score"] = "5" if nps >= 9 else "3" if nps >= 7 else "1" if nps <= 3 else "2"
            row["claims_feedback"] = _feedback_from_score(_int_value(row["claim_satisfaction_score"], 0))

    complaint_by_hk = {
        row.get("complaint_hash_key"): row
        for row in tables.get("sat_complaint", [])
        if row.get("complaint_hash_key")
    }
    complaint_ranked = []
    for order, link in enumerate(tables.get("link_complaint_policy", [])):
        complaint = complaint_by_hk.get(link.get("complaint_hash_key"))
        customer_hk = policy_customer.get(link.get("policy_hash_key"))
        if complaint is not None and customer_hk in customers:
            complaint_ranked.append((_int_value(customers[customer_hk].get("nps_score"), 0), -order, complaint))
    if complaint_ranked:
        complaint_low = sorted(complaint_ranked)
        complaint_mid_low = sorted(complaint_ranked, key=lambda item: (abs(item[0] - 4.5), item[1]))
        complaint_mid = sorted(complaint_ranked, key=lambda item: (abs(item[0] - 5.5), item[1]))
        complaint_passive = sorted(complaint_ranked, key=lambda item: (abs(item[0] - 7.5), item[1]))
        complaint_high = sorted(complaint_ranked, reverse=True)
        complaint_low_spread = _spread_ranked_by_score(complaint_ranked, [0, 1, 2, 3, 4, 5, 6])
        complaint_passive_spread = _spread_ranked_by_score(complaint_ranked, [7, 8])
        complaint_high_spread = _spread_ranked_by_score(complaint_ranked, [9, 10], reverse_within_score=True)
        complaint_fallback = complaint_low_spread + complaint_passive_spread + complaint_high_spread
        resolution_distribution = nps_settings.get("complaint_resolution_distribution") or {
            "DAYS_0_2": 60,
            "DAYS_3_7": 30,
            "DAYS_GT_7": 10,
        }
        resolution_rows = assign_ranked(
            complaint_ranked,
            resolution_distribution,
            {
                "DAYS_0_2": {"DAYS_0_2", "0_2", "FAST"},
                "DAYS_3_7": {"DAYS_3_7", "3_7", "MODERATE"},
                "DAYS_GT_7": {"DAYS_GT_7", "GT_7", "SLOW"},
            },
            {
                "DAYS_GT_7": [[item for item in complaint_low_spread if item[0] <= 6]],
                "DAYS_3_7": [[item for item in complaint_passive_spread if 7 <= item[0] <= 8]],
                "DAYS_0_2": [[item for item in complaint_high_spread if item[0] >= 9]],
            },
            complaint_high_spread + complaint_passive_spread + complaint_low_spread,
        )
        resolution_ranges = {"DAYS_0_2": (0, 2), "DAYS_3_7": (3, 7), "DAYS_GT_7": (8, 30)}
        for band, rows in resolution_rows.items():
            min_days, max_days = resolution_ranges[band]
            for idx, row in enumerate(rows):
                opened = _date_obj(row.get("complaint_date")) or _date_obj(row.get("load_date")) or date.today()
                days = min_days + (idx % (max_days - min_days + 1))
                row["complaint_date"] = _timestamp_from_date(opened, "00:00:00")
                row["complaint_acknowledgement_date"] = _timestamp_from_date(opened, "12:00:00")
                row["complaint_resolved_date"] = _timestamp_from_date(opened + timedelta(days=days), "23:59:59")
                _sync_complaint_status_with_dates(row)
        escalation_distribution = nps_settings.get("complaint_escalation_distribution") or {
            "NON_ESCALATED": 98,
            "ESCALATED": 2,
        }
        escalation_rows = assign_ranked(
            complaint_ranked,
            escalation_distribution,
            {
                "ESCALATED": {"ESCALATED", "Y", "YES", "TRUE"},
                "NON_ESCALATED": {"NON_ESCALATED", "N", "NO", "FALSE"},
            },
            {
                "ESCALATED": [[item for item in complaint_low_spread if item[0] <= 6]],
                "NON_ESCALATED": [[item for item in complaint_high_spread if item[0] >= 9], [item for item in complaint_passive_spread if 7 <= item[0] <= 8], [item for item in complaint_low_spread if 3 <= item[0] <= 6]],
            },
            complaint_fallback,
        )
        escalated_ids = {id(row) for row in escalation_rows.get("ESCALATED", [])}
        for _, _, row in complaint_ranked:
            row["is_financial_ombudsman_service_referral"] = "Y" if id(row) in escalated_ids else "N"
        outcome_distribution = nps_settings.get("complaint_status_outcome_distribution") or {
            "NOT_UPHELD": 65,
            "UPHELD": 20,
            "PARTIALLY_UPHELD": 15,
        }
        outcome_rows = assign_ranked(
            complaint_ranked,
            outcome_distribution,
            {
                "NOT_UPHELD": {"NOT_UPHELD", "NOT UPHELD"},
                "UPHELD": {"UPHELD"},
                "PARTIALLY_UPHELD": {"PARTIALLY_UPHELD", "PARTIALLY UPHELD"},
            },
            {
                "NOT_UPHELD": [[item for item in complaint_low_spread if item[0] <= 6]],
                "PARTIALLY_UPHELD": [[item for item in complaint_low_spread if 3 <= item[0] <= 6], [item for item in complaint_passive_spread if 7 <= item[0] <= 8]],
                "UPHELD": [[item for item in complaint_passive_spread if 7 <= item[0] <= 8], [item for item in complaint_high_spread if item[0] >= 9]],
            },
            complaint_fallback,
        )
        labels = {"NOT_UPHELD": "Not Upheld", "PARTIALLY_UPHELD": "Partially Upheld", "UPHELD": "Upheld"}
        for band, rows in outcome_rows.items():
            for row in rows:
                row["complaint_upheld_status"] = labels[band]
                _sync_complaint_status_with_dates(row)
                score = _complaint_satisfaction_score(row)
                row["customer_complaint_satisfaction_score"] = score
                row["complaint_feedback"] = _feedback_from_score(_int_value(score, 0)) if score != "" else ""


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
    claim_pool = policy_hks
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
    nps_cfg = ((cfg or {}).get("nps_settings") or {})
    claim_complaint_distribution = nps_cfg.get("claim_complaint_distribution") or {
        "NO_COMPLAINT": 85,
        "COMPLAINT": 15,
    }
    claim_policy_pool = [
        row.get("policy_hash_key")
        for row in tables.get("link_claim_policy", [])
        if row.get("policy_hash_key") and policy_to_customer_hk.get(row.get("policy_hash_key"))
    ]
    claim_policy_pool = list(dict.fromkeys(claim_policy_pool))
    all_complaint_policy_pool = [policy_hk for policy_hk in policy_hks if policy_to_customer_hk.get(policy_hk)]
    complaint_policy_pool = all_complaint_policy_pool
    complaint_weight = sum(
        float(weight)
        for value, weight in claim_complaint_distribution.items()
        if str(value).upper() in {"COMPLAINT", "Y", "YES", "TRUE"}
    )
    total_complaint_weight = sum(float(weight) for weight in claim_complaint_distribution.values()) or 100.0
    if claim_policy_pool:
        claim_complaint_target = min(len(claim_policy_pool), round(len(claim_policy_pool) * complaint_weight / total_complaint_weight))
        supplemental_target = max(claim_complaint_target, round(len(customer_hks) * 0.035))
        complaint_count = min(len(complaint_policy_pool), supplemental_target)
    else:
        complaint_count = min(len(complaint_policy_pool), max(1, int(len(customer_hks) * float(settings["complaint_customer_rate"])))) if complaint_policy_pool else 0
    policy_nps_rows = []
    for idx, policy_hk in enumerate(complaint_policy_pool):
        customer_hk = policy_to_customer_hk.get(policy_hk)
        customer_row = customer_sat_by_hk.get(customer_hk, {})
        nps_score = _int_value(customer_row.get("NPS Score"), 0)
        policy_nps_rows.append((nps_score, idx, policy_hk))
    low_nps_policies = [
        policy_hk
        for nps_score, _, policy_hk in sorted(policy_nps_rows)
        if nps_score <= 6
    ]
    selected_complaint_policies = low_nps_policies[:complaint_count]
    if len(selected_complaint_policies) < complaint_count:
        selected_set = set(selected_complaint_policies)
        remaining = [
            policy_hk
            for _, _, policy_hk in sorted(policy_nps_rows, key=lambda item: (item[0], item[1]))
            if policy_hk not in selected_set
        ]
        selected_complaint_policies.extend(remaining[:complaint_count - len(selected_complaint_policies)])
    rng.shuffle(selected_complaint_policies)
    complaint_hks = []
    for idx, preferred_policy_hk in enumerate(selected_complaint_policies):
        sample = _sample(complaint_examples, idx)
        category = _sample(category_examples, idx)
        base_complaint_id = str(sample.get("Complaint ID") or "CMP").strip() or "CMP"
        complaint_id = f"{base_complaint_id}_{idx + 1:07d}"
        complaint_hk = md5_hasher(complaint_id)
        customer_hk = policy_to_customer_hk.get(preferred_policy_hk, "")
        customer_row = customer_sat_by_hk.get(customer_hk, {})
        customer_since = _date_obj(customer_row.get("Customer Since"))
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


def build_enhanced_synthetic(
    ctx: dict,
    output_dir: str,
    cfg: dict | None = None,
    ddl_path: str | Path | None = None,
    apply_mlops_rules: bool = False,
) -> str:
    ddl = parse_enhanced_ddl(ddl_path) if ddl_path else parse_enhanced_ddl()
    schemas = ddl["tables"]
    tables = {table_name: [] for table_name in schemas}
    tables.update(_build_base_tables(ctx))
    tables = {table_name: tables.get(table_name, []) for table_name in schemas}

    _augment_base_satellites(tables, cfg)
    _add_enhanced_entities(tables, ctx, cfg)
    if apply_mlops_rules:
        _apply_mlops_columns(tables, ctx, cfg)
    _apply_self_service_adoption_nps_alignment(tables, cfg)
    _apply_digital_onboarding_nps_alignment(tables, cfg)
    _apply_quote_dropoff_nps_alignment(tables, cfg)
    _apply_quote_premium_increase_nps_alignment(tables, cfg)
    _apply_digital_renewal_nps_alignment(tables, cfg)
    _apply_nps_claim_service_distribution(tables, cfg)
    _apply_claim_complaint_nps_alignment(tables, cfg)
    _apply_nps_repeat_complaint_distribution(tables, cfg)
    _apply_nps_complaint_resolution_distribution(tables, cfg)
    _apply_nps_complaint_outcome_distribution(tables, cfg)
    _apply_nps_renewal_contact_distribution(tables, cfg)
    _apply_nps_customer_satisfaction_distribution(tables, cfg)
    _apply_ml_master_grain_nps_alignment(tables, cfg)
    _apply_onboarding_feedback_distribution(tables, cfg)
    _normalize_account_timeline(tables)
    _ensure_agent_broker_links(tables)
    if apply_mlops_rules:
        for row in tables.get("sat_complaint", []):
            _sync_complaint_status_with_dates(row)
            complaint_score = _complaint_satisfaction_score(row)
            row["customer_complaint_satisfaction_score"] = complaint_score
            row["complaint_feedback"] = _feedback_from_score(_int_value(complaint_score, 0)) if complaint_score != "" else ""
        for row in tables.get("sat_policy", []):
            renewal_score = _policy_renewal_satisfaction_score(row)
            row["policy_renewal_satisfaction_score"] = renewal_score
            row["policy_renewal_feedback"] = _feedback_from_score(_int_value(renewal_score, 0)) if renewal_score != "" else ""
            row["is_renewal_escalation"] = (
                "Y"
                if renewal_score != "" and _int_value(renewal_score, 0) <= 2
                else "N" if renewal_score != "" else ""
            )
    _apply_customer_segment_rating_consistency(tables)
    _normalize_string_boolean_columns(tables)
    _normalize_blank_numeric_columns(tables, ddl["column_types"])

    os.makedirs(output_dir, exist_ok=True)
    for table_name, columns in schemas.items():
        rows = [_ordered(row, columns) for row in tables.get(table_name, [])]
        write_csv(output_dir, f"{table_name}.csv", rows, fieldnames=columns)

    return output_dir


def build_mlops_synthetic(ctx: dict, output_dir: str, cfg: dict | None = None) -> str:
    return build_enhanced_synthetic(
        ctx,
        output_dir,
        cfg=cfg,
        ddl_path=MLOPS_DDL_PATH,
        apply_mlops_rules=True,
    )
