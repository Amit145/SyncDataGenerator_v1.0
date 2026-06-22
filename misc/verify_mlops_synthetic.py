from __future__ import annotations

import argparse
import os
import sys

import pandas as pd
from pandas.errors import EmptyDataError

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.storage_paths import MLOPS_ROOT
from helper.enhanced_ddl import parse_enhanced_ddl

MLOPS_DDL_PATH = os.path.join(ROOT, "mlops", "Enhanced_Customer360_DataVault_Model_DDL.sql")


def latest_subdir(base_dir: str) -> str | None:
    if not os.path.exists(base_dir):
        return None
    dirs = [
        os.path.join(base_dir, name)
        for name in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, name))
    ]
    if not dirs:
        return None
    return max(dirs, key=lambda path: (os.path.basename(path), os.path.getmtime(path)))


def read_csv_safe(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, keep_default_na=False)
    except EmptyDataError:
        df = pd.DataFrame()
    df.columns = [str(col).strip().lower() for col in df.columns]
    return df


def verify_mlops(folder: str) -> bool:
    ddl = parse_enhanced_ddl(MLOPS_DDL_PATH)
    schemas = ddl["tables"]
    errors = 0
    frames = {}

    for table_name, columns in schemas.items():
        path = os.path.join(folder, f"{table_name}.csv")
        if not os.path.exists(path):
            print(f"MISSING TABLE: {table_name}.csv")
            errors += 1
            continue
        df = read_csv_safe(path)
        frames[table_name] = df
        missing = [column for column in columns if column not in df.columns]
        extra = [column for column in df.columns if column not in columns]
        if missing or extra:
            print(f"SCHEMA FAILED: {table_name} missing={missing} extra={extra}")
            errors += 1

    required_populated = {
        "sat_address": ["region"],
        "sat_claim": ["suspected_amount", "is_fault_claim", "claim_satisfaction_score"],
        "sat_marketing_engagement": [
            "has_retention_team_interaction",
            "customer_service_call_frequency",
            "average_call_sentiment",
            "engagement_score",
            "first_contact_resolution",
        ],
        "sat_motor": ["driver_experience_years"],
        "sat_policy": [
            "is_auto_renew_enabled",
            "no_claims_discount_years",
            "payment_method",
            "is_direct_debit_cancellation",
            "missed_payment_count",
            "loyalty_discount_usage",
            "is_installment_default",
        ],
    }
    for table_name, columns in required_populated.items():
        df = frames.get(table_name)
        if df is None or df.empty:
            continue
        for column in columns:
            if column not in df.columns:
                continue
            blank_count = int(df[column].fillna("").astype(str).str.strip().eq("").sum())
            if blank_count:
                print(f"POPULATION FAILED: {table_name}.{column} blank_rows={blank_count}")
                errors += 1

    bool_checks = {
        "sat_claim": ["is_fault_claim"],
        "sat_policy": [
            "is_auto_renew_enabled",
            "is_direct_debit_cancellation",
            "is_installment_default",
            "is_renewal_escalation",
        ],
        "sat_marketing_engagement": ["has_retention_team_interaction", "first_contact_resolution"],
    }
    for table_name, columns in bool_checks.items():
        df = frames.get(table_name)
        if df is None or df.empty:
            continue
        for column in columns:
            bad = df[~df[column].astype(str).str.upper().isin(["Y", "N"])]
            if not bad.empty:
                print(f"BOOLEAN FAILED: {table_name}.{column} invalid_rows={len(bad)}")
                errors += 1

    numeric_ranges = [
        ("sat_claim", "claim_satisfaction_score", 1, 10),
        ("sat_marketing_engagement", "customer_service_call_frequency", 0, 999),
        ("sat_marketing_engagement", "engagement_score", 0, 100),
        ("sat_motor", "driver_experience_years", 0, 100),
        ("sat_policy", "no_claims_discount_years", 0, 100),
        ("sat_policy", "missed_payment_count", 0, 999),
        ("sat_complaint", "customer_complaint_satisfaction_score", 0, 5),
        ("sat_customer", "customer_onboarding_satisfaction_score", 0, 5),
        ("sat_policy", "policy_renewal_satisfaction_score", 0, 5),
    ]
    for table_name, column, low, high in numeric_ranges:
        df = frames.get(table_name)
        if df is None or df.empty or column not in df.columns:
            continue
        values = pd.to_numeric(df[column], errors="coerce")
        bad = values.isna() | (values < low) | (values > high)
        if int(bad.sum()):
            print(f"RANGE FAILED: {table_name}.{column} invalid_rows={int(bad.sum())}")
            errors += 1

    claim = frames.get("sat_claim")
    if claim is not None and not claim.empty:
        suspected = pd.to_numeric(claim.get("suspected_amount"), errors="coerce")
        claim_amount = pd.to_numeric(claim.get("claim_amount"), errors="coerce")
        bad = suspected.isna() | (suspected < 0) | (suspected > claim_amount)
        if int(bad.sum()):
            print(f"CLAIM FAILED: suspected_amount invalid_rows={int(bad.sum())}")
            errors += 1
        if "suspectd_amount" in claim.columns:
            print("SCHEMA FAILED: old typo column sat_claim.suspectd_amount still present")
            errors += 1

    policy = frames.get("sat_policy")
    if policy is not None and not policy.empty:
        allowed_payment = {"DIRECT_DEBIT", "CARD", "BANK_TRANSFER"}
        bad_payment = ~policy["payment_method"].astype(str).str.upper().isin(allowed_payment)
        if int(bad_payment.sum()):
            print(f"POLICY FAILED: invalid payment_method rows={int(bad_payment.sum())}")
            errors += 1
        bad_dd_cancel = (
            policy["is_direct_debit_cancellation"].astype(str).str.upper().eq("Y")
            & ~policy["payment_method"].astype(str).str.upper().eq("DIRECT_DEBIT")
        )
        if int(bad_dd_cancel.sum()):
            print(f"POLICY FAILED: direct debit cancellation without DIRECT_DEBIT rows={int(bad_dd_cancel.sum())}")
            errors += 1
        allowed_loyalty = {"RETAINED", "NOT_APPLIED", "REMOVED", "Y", "N"}
        bad_loyalty = ~policy["loyalty_discount_usage"].astype(str).str.upper().isin(allowed_loyalty)
        if int(bad_loyalty.sum()):
            print(f"POLICY FAILED: invalid loyalty_discount_usage rows={int(bad_loyalty.sum())}")
            errors += 1

    feedback_checks = {
        "sat_claim": ["claims_feedback"],
        "sat_complaint": ["complaint_feedback"],
        "sat_policy": ["policy_renewal_feedback"],
    }
    allowed_feedback = {"", "POSITIVE", "NEUTRAL", "NEGATIVE"}
    for table_name, columns in feedback_checks.items():
        df = frames.get(table_name)
        if df is None or df.empty:
            continue
        for column in columns:
            if column not in df.columns:
                continue
            bad = ~df[column].fillna("").astype(str).str.upper().isin(allowed_feedback)
            if int(bad.sum()):
                print(f"FEEDBACK FAILED: {table_name}.{column} invalid_rows={int(bad.sum())}")
                errors += 1

    complaint = frames.get("sat_complaint")
    if complaint is not None and not complaint.empty:
        required = ["customer_complaint_satisfaction_score", "complaint_status", "complaint_resolved_date"]
        if all(column in complaint.columns for column in required):
            resolved = ~complaint["complaint_resolved_date"].fillna("").astype(str).str.strip().eq("")
            blank_score = resolved & complaint["customer_complaint_satisfaction_score"].fillna("").astype(str).str.strip().eq("")
            if int(blank_score.sum()):
                print(f"COMPLAINT FAILED: resolved complaints with blank customer_complaint_satisfaction_score rows={int(blank_score.sum())}")
                errors += 1
            open_resolved = resolved & complaint["complaint_status"].fillna("").astype(str).str.upper().isin(["OPEN", "PENDING"])
            if int(open_resolved.sum()):
                print(f"COMPLAINT FAILED: resolved complaints left open rows={int(open_resolved.sum())}")
                errors += 1

    customer = frames.get("sat_customer")
    if customer is not None and not customer.empty:
        for column in ["customer_onboarding_satisfaction_score", "customer_onboarding_feedback"]:
            if column in customer.columns:
                blank = customer[column].fillna("").astype(str).str.strip().eq("")
                if int(blank.sum()):
                    print(f"CUSTOMER FAILED: blank {column} rows={int(blank.sum())}")
                    errors += 1
        if "customer_onboarding_feedback" in customer.columns:
            generic = customer["customer_onboarding_feedback"].fillna("").astype(str).str.upper().isin(allowed_feedback - {""})
            if int(generic.sum()):
                print(f"CUSTOMER FAILED: customer_onboarding_feedback still uses generic sentiment labels rows={int(generic.sum())}")
                errors += 1

    if errors:
        print(f"MLOps verification failed with {errors} issue(s).")
        return False
    print("MLOps verification passed.")
    return True


def parse_args():
    parser = argparse.ArgumentParser(description="Verify MLOps synthetic Data Vault output.")
    parser.add_argument("folder", nargs="?", help="MLOps synthetic folder. Defaults to latest data/synthetic/mlops run.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    target = args.folder or latest_subdir(MLOPS_ROOT)
    if not target:
        raise SystemExit("No MLOps synthetic folder found.")
    ok = verify_mlops(target)
    raise SystemExit(0 if ok else 1)
