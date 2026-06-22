from __future__ import annotations

import argparse
import io
import sys
import zipfile
from pathlib import Path

import pandas as pd


def _read_table(root: Path, name: str) -> pd.DataFrame:
    zip_path = root / f"{name}.zip"
    folder_path = root / name
    frames = []
    if zip_path.exists():
        with zipfile.ZipFile(zip_path) as archive:
            for member in archive.namelist():
                if member.lower().endswith(".csv"):
                    with archive.open(member) as handle:
                        frames.append(pd.read_csv(handle, keep_default_na=False, low_memory=False))
    elif folder_path.exists():
        for path in folder_path.rglob("*.csv"):
            frames.append(pd.read_csv(path, keep_default_na=False, low_memory=False))
    else:
        csv_path = root / f"{name}.csv"
        if csv_path.exists():
            frames.append(pd.read_csv(csv_path, keep_default_na=False, low_memory=False))
    if not frames:
        return pd.DataFrame()
    frame = pd.concat(frames, ignore_index=True)
    frame.columns = [str(col).strip().lower() for col in frame.columns]
    return frame


def _dt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True).dt.tz_convert(None)


def _pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def _check_distribution(label: str, values: pd.Series, ranges: dict[str, tuple[float, float]]) -> int:
    values = values.dropna()
    counts = values.astype(str).value_counts(dropna=False)
    total = int(counts.sum())
    detail = {}
    errors = 0
    for band, (low, high) in ranges.items():
        rate = float(counts.get(band, 0)) / total if total else 0.0
        ok = low <= rate <= high
        detail[band] = {
            "count": int(counts.get(band, 0)),
            "current": _pct(rate),
            "expected": f"{_pct(low)}-{_pct(high)}",
            "status": "PASS" if ok else "FAIL",
        }
        errors += 0 if ok else 1
    print(("PASS" if errors == 0 else "FAIL") + f": {label} - {detail}")
    return errors


def _present(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip()
    return series.notna() & text.ne("") & ~text.str.lower().isin({"nan", "none", "null", "-1", "-1.0"})


def _claim_rows(master: pd.DataFrame) -> pd.DataFrame:
    if "claim_sk" in master.columns:
        return master[_present(master["claim_sk"])]
    claim_id = master["claim_id"] if "claim_id" in master.columns else pd.Series("", index=master.index)
    return master[_present(claim_id)]


def _complaint_rows(master: pd.DataFrame) -> pd.DataFrame:
    complaint_id = master["complaint_id"] if "complaint_id" in master.columns else pd.Series("", index=master.index)
    return master[_present(complaint_id)]


def _build_master(root: Path) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    tables = {
        name: _read_table(root, name)
        for name in [
            "dim_account",
            "dim_claim",
            "dim_customer",
            "dim_person",
            "dim_policy",
            "dim_override",
            "dim_marketing",
            "fact_policy",
            "fact_quote",
            "fact_complaint",
        ]
    }
    fact_policy = tables["fact_policy"]
    if fact_policy.empty:
        return pd.DataFrame(), tables
    master_cols = [
        "customer_sk",
        "policy_sk",
        "claim_sk",
        "person_sk",
        "account_sk",
        "channel_sk",
        "marketing_sk",
        "date_sk",
        "override_sk",
        "person_id",
        "policy_renewal_current_period_amt",
        "policy_renewal_next_period_amt",
    ]
    master = fact_policy[[col for col in master_cols if col in fact_policy.columns]].drop_duplicates().copy()
    joins = [
        ("dim_customer", ["customer_sk"], ["customer_sk", "net_promoter_score", "net_promotor_code_segment", "customer_satisfaction", "customer_onboarding_feedback"]),
        ("dim_policy", ["policy_sk"], ["policy_sk", "policy_start_ts", "policy_issue_date", "policy_sales_channel", "policy_status", "is_policy_renewal", "policy_cycle"]),
        ("dim_claim", ["claim_sk"], ["claim_sk", "claim_id", "claim_status", "claim_channel", "is_litigation", "claim_reported_date", "claim_satisfaction_score", "claim_settlement_date"]),
        ("dim_person", ["person_sk", "person_id"], ["person_sk", "person_id", "is_operational_paperless_consent"]),
        ("dim_account", ["account_sk"], ["account_sk", "account_id", "account_creation_type", "account_last_access_ts", "created_ts"]),
        ("dim_override", ["override_sk"], ["override_sk", "override_reason"]),
        ("dim_marketing", ["marketing_sk"], ["marketing_sk", "customer_service_call_frequency"]),
    ]
    for table_name, keys, cols in joins:
        table = tables[table_name]
        if table.empty or not set(keys).issubset(master.columns) or not set(keys).issubset(table.columns):
            continue
        master = master.merge(table[[col for col in cols if col in table.columns]], on=keys, how="left")
    quote = tables["fact_quote"]
    quote_keys = ["customer_sk", "person_sk", "account_sk", "channel_sk"]
    if not quote.empty and set(quote_keys).issubset(master.columns) and set(quote_keys).issubset(quote.columns):
        cols = quote_keys + [col for col in ["quote_id", "quote_status", "quote_date", "quote_renewal_current_period_amt", "quote_renewal_next_period_amt"] if col in quote.columns]
        master = master.merge(quote[cols].drop_duplicates(), on=quote_keys, how="left")
    complaint = tables["fact_complaint"]
    complaint_keys = ["customer_sk", "person_sk", "channel_sk"]
    if not complaint.empty and set(complaint_keys).issubset(master.columns) and set(complaint_keys).issubset(complaint.columns):
        cols = complaint_keys + [
            col
            for col in [
                "complaint_date",
                "complaint_id",
                "complaint_resolved_date",
                "is_financial_ombudsman_service_referral",
                "complaint_status",
                "complaint_upheld_status",
            ]
            if col in complaint.columns
        ]
        master = master.merge(complaint[cols].drop_duplicates(), on=complaint_keys, how="left")
    return master, tables


def verify(root: Path) -> int:
    master, tables = _build_master(root)
    if master.empty:
        print(f"FAIL: no dim/fact master data found under {root}")
        return 1
    errors = 0
    nps = pd.to_numeric(master.get("net_promoter_score"), errors="coerce")
    nps_band = nps.apply(lambda value: "PROMOTER" if value >= 9 else ("PASSIVE" if value >= 7 else "DETRACTOR"))
    errors += _check_distribution("NPS score at ML master grain", nps_band, {"DETRACTOR": (0.25, 0.35), "PASSIVE": (0.30, 0.40), "PROMOTER": (0.30, 0.40)})

    score_counts = nps.value_counts().to_dict()
    dip_avg = sum(int(score_counts.get(score, 0)) for score in [2, 3, 4]) / 3
    shoulder_avg = sum(int(score_counts.get(score, 0)) for score in [0, 1, 5, 6]) / 4
    print(("PASS" if dip_avg < shoulder_avg else "FAIL") + f": NPS 2-4 dip at ML master grain - avg_2_4={dip_avg:.2f} avg_shoulders={shoulder_avg:.2f}")
    errors += 0 if dip_avg < shoulder_avg else 1

    if "account_creation_type" in master.columns:
        digital = master["account_creation_type"].astype(str).str.upper().isin({"ONLINE", "DIGITAL", "SELF-SERVICE"})
        errors += _check_distribution("Digital onboarding at ML master grain", digital.map({True: "DIGITAL", False: "ASSISTED"}), {"DIGITAL": (0.70, 0.80), "ASSISTED": (0.20, 0.30)})
    if "customer_onboarding_feedback" in master.columns:
        positive = {
            "COMPREHENSIVE COVER FOR THE PRICE FOR APPROPRIATE POLICY",
            "FLEXIBLE EXCESS OPTIONS AVAILABLE",
            "POLICY DOCUMENTS EASY TO UNDERSTAND",
            "GOOD COVER OPTIONS FOR THE PREMIUM",
            "ONLINE QUOTE WAS QUICK AND EASY TO COMPLETE",
            "DOCUMENTS ARRIVED PROMPTLY AFTER PURCHASE",
            "COVER LEVEL MATCHED MY NEEDS CLEARLY",
            "PAYMENT SETUP WAS SIMPLE AND TRANSPARENT",
            "AGENT EXPLAINED THE POLICY OPTIONS WELL",
            "SMOOTH ONBOARDING WITH NO REPEATED QUESTIONS",
        }
        neutral = {
            "COVER OPTIONS MOSTLY MET EXPECTATIONS",
            "ONBOARDING COMPLETED WITH MINOR CLARIFICATIONS",
            "PRICE AND BENEFITS WERE ACCEPTABLE",
            "DOCUMENTS WERE CLEAR AFTER A SECOND REVIEW",
            "QUOTE JOURNEY WAS ACCEPTABLE BUT A LITTLE SLOW",
            "NEEDED HELP TO COMPARE EXCESS OPTIONS",
            "POLICY SETUP WAS FINE AFTER SUPPORT ASSISTED",
            "RENEWAL AND COVER DETAILS NEEDED CLARIFICATION",
            "PAYMENT OPTIONS WERE ADEQUATE",
        }
        negative = {
            "POLICY EXCLUSIONS NOT CLEAR",
            "COURTESY CAR NOT IN STANDARD COVER",
            "ADDITIONAL COVER OPTIONS WERE DIFFICULT TO COMPARE",
            "PRICE FELT HIGH FOR THE SELECTED COVER",
            "TOO MANY STEPS BEFORE QUOTE ACCEPTANCE",
            "POLICY DOCUMENTS WERE HARD TO UNDERSTAND",
            "EXCESS OPTIONS WERE CONFUSING DURING PURCHASE",
            "HAD TO REPEAT PERSONAL DETAILS DURING ONBOARDING",
            "COVER LIMITS WERE NOT EXPLAINED CLEARLY",
            "PAYMENT SETUP FAILED ON FIRST ATTEMPT",
        }
        def feedback_band(value: str) -> str:
            text = str(value).strip().upper()
            for phrase in positive:
                if text == phrase or text.startswith(f"{phrase} - "):
                    return "POSITIVE"
            for phrase in neutral:
                if text == phrase or text.startswith(f"{phrase} - "):
                    return "NEUTRAL"
            for phrase in negative:
                if text == phrase or text.startswith(f"{phrase} - "):
                    return "NEGATIVE"
            return "UNKNOWN"

        feedback = master["customer_onboarding_feedback"].astype(str).str.strip()
        band = feedback.apply(feedback_band)
        errors += _check_distribution("Onboarding feedback at ML master grain", band, {"NEGATIVE": (0.18, 0.22), "NEUTRAL": (0.28, 0.32), "POSITIVE": (0.48, 0.52)})
    if {"account_creation_type", "account_last_access_ts", "is_operational_paperless_consent"}.issubset(master.columns):
        online = master["account_creation_type"].astype(str).str.upper().isin({"ONLINE", "DIGITAL", "SELF-SERVICE"})
        paperless = master["is_operational_paperless_consent"].astype(str).str.upper().eq("Y")
        access = _dt(master["account_last_access_ts"])
        reference_source = master["created_ts"] if "created_ts" in master.columns else pd.Series(index=master.index, dtype=str)
        reference = _dt(reference_source).fillna(pd.Timestamp.utcnow().tz_localize(None))
        recent = (reference - access).dt.days.between(0, 30, inclusive="left")
        errors += _check_distribution("Self-service adoption at ML master grain", (online & paperless & recent).map({True: "ADOPTED", False: "NOT_ADOPTED"}), {"ADOPTED": (0.60, 0.70), "NOT_ADOPTED": (0.30, 0.40)})
    if {"policy_issue_date", "policy_start_ts"}.issubset(master.columns):
        days = (_dt(master["policy_start_ts"]) - _dt(master["policy_issue_date"])).dt.days
        bands = pd.cut(days, bins=[-1, 2, 7, float("inf")], labels=["DAYS_0_2", "DAYS_3_7", "DAYS_GT_7"]).astype(str)
        errors += _check_distribution("Policy issuance TAT at ML master grain", bands, {"DAYS_0_2": (0.65, 0.75), "DAYS_3_7": (0.15, 0.25), "DAYS_GT_7": (0.08, 0.12)})
    if "quote_status" in master.columns:
        status = master["quote_status"].astype(str).str.upper()
        quoted = status.ne("")
        drop = status.isin({"CREATED", "SENT", "EXPIRED", "REJECTED"})
        if int(quoted.sum()):
            errors += _check_distribution("Drop-off during onboarding at ML master grain", drop[quoted].map({True: "DROPOFF", False: "ACCEPTED"}), {"DROPOFF": (0.05, 0.10), "ACCEPTED": (0.90, 0.95)})
            errors += _check_distribution("Drop-off status at ML master grain", status[status.isin({"CREATED", "SENT", "EXPIRED"})], {"CREATED": (0.45, 0.55), "SENT": (0.30, 0.40), "EXPIRED": (0.10, 0.20)})
    for label, current_col, next_col in [
        ("Policy premium increase at ML master grain", "policy_renewal_current_period_amt", "policy_renewal_next_period_amt"),
        ("Quote premium increase at ML master grain", "quote_renewal_current_period_amt", "quote_renewal_next_period_amt"),
    ]:
        if {current_col, next_col}.issubset(master.columns):
            current = pd.to_numeric(master[current_col], errors="coerce")
            next_amt = pd.to_numeric(master[next_col], errors="coerce")
            pct_increase = ((next_amt - current) / current.replace(0, pd.NA)) * 100
            band = pct_increase.apply(lambda value: "LE_5" if value <= 5 else ("GT_5_LE_10" if value <= 10 else "GT_10"))
            errors += _check_distribution(label, band, {"LE_5": (0.65, 0.75), "GT_5_LE_10": (0.15, 0.25), "GT_10": (0.08, 0.12)})
    if {"policy_sales_channel", "is_policy_renewal"}.issubset(master.columns):
        renewal = master[master["is_policy_renewal"].astype(str).str.upper().eq("Y")]
        channel = renewal["policy_sales_channel"].astype(str).str.upper().replace({"BROKER": "AGENT", "DIGITAL": "ONLINE"})
        errors += _check_distribution("Digital renewal at ML master grain", channel, {"ONLINE": (0.65, 0.75), "AGENT": (0.15, 0.25), "BRANCH": (0.08, 0.12)})
    if {"claim_reported_date", "claim_settlement_date"}.issubset(master.columns):
        claim_rows = _claim_rows(master)
        days = (_dt(claim_rows["claim_settlement_date"]) - _dt(claim_rows["claim_reported_date"])).dt.days
        band = pd.cut(days, bins=[-1, 15, 30, float("inf")], labels=["DAYS_0_15", "DAYS_16_30", "DAYS_GT_30"]).astype(str)
        errors += _check_distribution("Claim settlement TAT at ML master grain", band, {"DAYS_0_15": (0.65, 0.75), "DAYS_16_30": (0.15, 0.25), "DAYS_GT_30": (0.08, 0.12)})
    if "claim_channel" in master.columns:
        claim_rows = _claim_rows(master)
        channel = claim_rows["claim_channel"].astype(str).str.upper().replace({"BROKER": "AGENT", "DIGITAL": "ONLINE"})
        errors += _check_distribution("Claim channel at ML master grain", channel, {"ONLINE": (0.65, 0.75), "AGENT": (0.15, 0.25), "BRANCH": (0.08, 0.12)})
    if "is_litigation" in master.columns:
        claim_rows = _claim_rows(master)
        escalated = claim_rows["is_litigation"].astype(str).str.upper().eq("Y")
        errors += _check_distribution("Claim escalation at ML master grain", escalated.map({True: "ESCALATED", False: "NON_ESCALATED"}), {"NON_ESCALATED": (0.88, 0.95), "ESCALATED": (0.05, 0.12)})
    if "customer_service_call_frequency" in master.columns:
        calls = pd.to_numeric(master["customer_service_call_frequency"], errors="coerce").fillna(0)
        band = calls.apply(lambda value: "CONTACTS_0_1" if value <= 1 else ("CONTACTS_2_3" if value <= 3 else "CONTACTS_GT_3"))
        errors += _check_distribution("Renewal contact count at ML master grain", band, {"CONTACTS_0_1": (0.55, 0.65), "CONTACTS_2_3": (0.25, 0.35), "CONTACTS_GT_3": (0.08, 0.12)})
    if {"complaint_date", "complaint_resolved_date"}.issubset(master.columns):
        complaint_rows = _complaint_rows(master)
        days = (_dt(complaint_rows["complaint_resolved_date"]) - _dt(complaint_rows["complaint_date"])).dt.days
        band = pd.cut(days, bins=[-1, 2, 7, float("inf")], labels=["DAYS_0_2", "DAYS_3_7", "DAYS_GT_7"]).astype(str)
        errors += _check_distribution("Complaint resolution TAT at ML master grain", band, {"DAYS_0_2": (0.55, 0.65), "DAYS_3_7": (0.25, 0.35), "DAYS_GT_7": (0.08, 0.12)})
    if "is_financial_ombudsman_service_referral" in master.columns:
        complaint_rows = _complaint_rows(master)
        escalated = complaint_rows["is_financial_ombudsman_service_referral"].astype(str).str.upper().eq("Y")
        errors += _check_distribution("Complaint escalation at ML master grain", escalated.map({True: "ESCALATED", False: "NON_ESCALATED"}), {"NON_ESCALATED": (0.975, 0.985), "ESCALATED": (0.015, 0.025)})
    if "complaint_upheld_status" in master.columns:
        complaint_rows = _complaint_rows(master)
        status = complaint_rows["complaint_upheld_status"].astype(str).str.upper().str.replace(" ", "_")
        errors += _check_distribution("Complaint outcome at ML master grain", status, {"NOT_UPHELD": (0.60, 0.70), "UPHELD": (0.15, 0.25), "PARTIALLY_UPHELD": (0.10, 0.20)})
    if errors:
        print(f"NPS dim/fact validation failed with {errors} issue(s).")
    else:
        print("NPS dim/fact validation passed.")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate NPS workbook ratios at the ML dim/fact master_df grain.")
    parser.add_argument("path", nargs="?", default="nps_june/data", help="Folder containing dim/fact CSV folders or zipped CSV exports.")
    args = parser.parse_args()
    return 1 if verify(Path(args.path)) else 0


if __name__ == "__main__":
    raise SystemExit(main())
