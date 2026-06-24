from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.storage_paths import MLOPS_ROOT


def latest_subdir(base_dir: str) -> str | None:
    if not os.path.exists(base_dir):
        return None
    dirs = [
        os.path.join(base_dir, name)
        for name in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, name))
    ]
    return max(dirs, key=lambda path: (os.path.basename(path), os.path.getmtime(path))) if dirs else None


def read_csv_safe(folder: Path, name: str) -> pd.DataFrame:
    path = folder / name
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, keep_default_na=False)
    except EmptyDataError:
        return pd.DataFrame()
    df.columns = [str(col).strip().lower() for col in df.columns]
    return df


def parse_dt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def in_range(value: float, low: float, high: float) -> bool:
    return low <= value <= high


def check_distribution(frame: pd.DataFrame, column: str, ranges: dict[str, tuple[float, float]], label: str) -> int:
    if frame.empty or column not in frame.columns:
        print(f"SKIP: {label} - required data missing")
        return 0
    counts = frame[column].astype(str).value_counts(dropna=False)
    total = int(counts.sum())
    errors = 0
    detail = {}
    for band, limits in ranges.items():
        rate = float(counts.get(band, 0)) / total if total else 0.0
        ok = in_range(rate, limits[0], limits[1])
        detail[band] = {"count": int(counts.get(band, 0)), "pct": pct(rate), "expected": f"{pct(limits[0])}-{pct(limits[1])}", "status": "PASS" if ok else "FAIL"}
        if not ok:
            errors += 1
    print(("PASS" if errors == 0 else "FAIL") + f": {label} - {detail}")
    return errors


def report_distribution(frame: pd.DataFrame, column: str, bands: dict[str, tuple[float, float]], label: str) -> None:
    if frame.empty or column not in frame.columns:
        print(f"SKIP: {label} - required data missing")
        return
    counts = frame[column].astype(str).value_counts(dropna=False)
    total = int(counts.sum())
    detail = {}
    for band, limits in bands.items():
        rate = float(counts.get(band, 0)) / total if total else 0.0
        detail[band] = {
            "count": int(counts.get(band, 0)),
            "pct": pct(rate),
            "workbook": f"{pct(limits[0])}-{pct(limits[1])}",
        }
    print(f"INFO: {label} - {detail}")


def derive_policy_customer(folder: Path) -> pd.DataFrame:
    policy = read_csv_safe(folder, "sat_policy.csv")
    customer = read_csv_safe(folder, "sat_customer.csv")
    link_pc = read_csv_safe(folder, "link_policy_customer.csv")
    if any(df.empty for df in [policy, customer, link_pc]):
        return pd.DataFrame()
    frame = (
        link_pc[["policy_hash_key", "customer_hash_key"]]
        .merge(policy, on="policy_hash_key", how="inner", suffixes=("", "_policy"))
        .merge(customer, on="customer_hash_key", how="inner", suffixes=("", "_customer"))
    )
    return frame


def derive_account_customer(folder: Path) -> pd.DataFrame:
    account = read_csv_safe(folder, "sat_account.csv")
    customer = read_csv_safe(folder, "sat_customer.csv")
    link_pa = read_csv_safe(folder, "link_person_account.csv")
    link_cp = read_csv_safe(folder, "link_customer_person.csv")
    if any(df.empty for df in [account, customer, link_pa, link_cp]):
        return pd.DataFrame()
    return (
        link_pa[["person_hash_key", "account_hash_key"]]
        .merge(account, on="account_hash_key", how="inner", suffixes=("", "_account"))
        .merge(link_cp[["person_hash_key", "customer_hash_key"]], on="person_hash_key", how="inner")
        .merge(customer[["customer_hash_key", "nps_score"]], on="customer_hash_key", how="inner")
    )


def derive_self_service_customer(folder: Path) -> pd.DataFrame:
    account_customer = derive_account_customer(folder)
    person = read_csv_safe(folder, "sat_person.csv")
    if account_customer.empty or person.empty or "person_hash_key" not in person.columns:
        return pd.DataFrame()
    cols = ["person_hash_key", "operational_paperless_consent"]
    if not set(cols).issubset(person.columns):
        return pd.DataFrame()
    return account_customer.merge(person[cols], on="person_hash_key", how="inner")


def derive_quote_customer(folder: Path) -> pd.DataFrame:
    quote = read_csv_safe(folder, "sat_quote.csv")
    customer = read_csv_safe(folder, "sat_customer.csv")
    link_pq = read_csv_safe(folder, "link_policy_quote.csv")
    link_pc = read_csv_safe(folder, "link_policy_customer.csv")
    if any(df.empty for df in [quote, customer, link_pq, link_pc]):
        return pd.DataFrame()
    return (
        link_pq[["policy_hash_key", "quote_hash_key"]]
        .merge(quote, on="quote_hash_key", how="inner", suffixes=("", "_quote"))
        .merge(link_pc[["policy_hash_key", "customer_hash_key"]], on="policy_hash_key", how="inner")
        .merge(customer[["customer_hash_key", "nps_score"]], on="customer_hash_key", how="inner")
    )


def derive_claim_customer(folder: Path) -> pd.DataFrame:
    claim = read_csv_safe(folder, "sat_claim.csv")
    customer = read_csv_safe(folder, "sat_customer.csv")
    link_cp = read_csv_safe(folder, "link_claim_policy.csv")
    link_pc = read_csv_safe(folder, "link_policy_customer.csv")
    if any(df.empty for df in [claim, customer, link_cp, link_pc]):
        return pd.DataFrame()
    return (
        link_cp[["claim_hash_key", "policy_hash_key"]]
        .merge(claim, on="claim_hash_key", how="inner", suffixes=("", "_claim"))
        .merge(link_pc[["policy_hash_key", "customer_hash_key"]], on="policy_hash_key", how="inner")
        .merge(customer[["customer_hash_key", "nps_score"]], on="customer_hash_key", how="inner")
    )


def derive_claim_complaint_customer(folder: Path) -> pd.DataFrame:
    claim_customer = derive_claim_customer(folder)
    link_complaint_policy = read_csv_safe(folder, "link_complaint_policy.csv")
    if claim_customer.empty or link_complaint_policy.empty or "policy_hash_key" not in link_complaint_policy.columns:
        return pd.DataFrame()
    complaint_policies = set(link_complaint_policy["policy_hash_key"].astype(str))
    frame = claim_customer.copy()
    frame["has_claim_complaint"] = frame["policy_hash_key"].astype(str).isin(complaint_policies)
    return frame


def derive_marketing_customer(folder: Path) -> pd.DataFrame:
    marketing = read_csv_safe(folder, "sat_marketing_engagement.csv")
    customer = read_csv_safe(folder, "sat_customer.csv")
    link_pm = read_csv_safe(folder, "link_person_marketing_engagement.csv")
    link_cp = read_csv_safe(folder, "link_customer_person.csv")
    if any(df.empty for df in [marketing, customer, link_pm, link_cp]):
        return pd.DataFrame()
    return (
        link_pm[["person_hash_key", "marketing_engagement_hash_key"]]
        .merge(marketing, on="marketing_engagement_hash_key", how="inner", suffixes=("", "_marketing"))
        .merge(link_cp[["person_hash_key", "customer_hash_key"]], on="person_hash_key", how="inner")
        .merge(customer[["customer_hash_key", "nps_score", "customer_satisfaction"]], on="customer_hash_key", how="inner")
    )


def derive_complaint_customer(folder: Path) -> pd.DataFrame:
    complaint = read_csv_safe(folder, "sat_complaint.csv")
    customer = read_csv_safe(folder, "sat_customer.csv")
    link_complaint_policy = read_csv_safe(folder, "link_complaint_policy.csv")
    link_policy_customer = read_csv_safe(folder, "link_policy_customer.csv")
    if any(df.empty for df in [complaint, customer, link_complaint_policy, link_policy_customer]):
        return pd.DataFrame()
    return (
        link_complaint_policy[["complaint_hash_key", "policy_hash_key"]]
        .merge(complaint, on="complaint_hash_key", how="inner", suffixes=("", "_complaint"))
        .merge(link_policy_customer[["policy_hash_key", "customer_hash_key"]], on="policy_hash_key", how="inner")
        .merge(customer[["customer_hash_key", "nps_score"]], on="customer_hash_key", how="inner")
    )


def verify_nps(folder: str) -> bool:
    base = Path(folder)
    errors = 0

    customer = read_csv_safe(base, "sat_customer.csv")
    account = read_csv_safe(base, "sat_account.csv")
    policy = read_csv_safe(base, "sat_policy.csv")
    quote = read_csv_safe(base, "sat_quote.csv")
    link_policy_quote = read_csv_safe(base, "link_policy_quote.csv")
    link_complaint_policy = read_csv_safe(base, "link_complaint_policy.csv")
    claim = read_csv_safe(base, "sat_claim.csv")
    complaint = read_csv_safe(base, "sat_complaint.csv")

    if customer.empty:
        print("FAIL: NPS customer data missing")
        return False

    nps = pd.to_numeric(customer.get("nps_score"), errors="coerce")
    invalid_nps = int((nps.isna() | (nps < 0) | (nps > 10)).sum())
    if invalid_nps:
        print(f"FAIL: nps_score must be 0-10 - invalid_rows={invalid_nps}")
        errors += 1
    else:
        print("PASS: nps_score 0-10 range")

    segment_expected = nps.apply(lambda value: "PROMOTERS" if value >= 9 else ("PASSIVE" if value >= 7 else "DETRACTORS"))
    segment_actual = customer.get("net_promotor_code_segment", pd.Series(index=customer.index, dtype=str)).astype(str).str.upper()
    bad_segment = int((segment_actual != segment_expected).sum())
    if bad_segment:
        print(f"FAIL: net_promotor_code_segment must follow nps_score - invalid_rows={bad_segment}")
        errors += 1
    else:
        print("PASS: net_promotor_code_segment follows nps_score")

    nps_frame = pd.DataFrame({
        "nps_band": nps.apply(lambda value: "PROMOTER" if value >= 9 else ("PASSIVE" if value >= 7 else "DETRACTOR"))
    })
    errors += check_distribution(
        nps_frame,
        "nps_band",
        {"DETRACTOR": (0.25, 0.35), "PASSIVE": (0.30, 0.40), "PROMOTER": (0.30, 0.40)},
        "NPS score distribution from workbook",
    )
    score_counts = nps.value_counts().to_dict()
    dip_avg = sum(int(score_counts.get(score, 0)) for score in [2, 3, 4]) / 3
    shoulder_avg = sum(int(score_counts.get(score, 0)) for score in [0, 1, 5, 6]) / 4
    dip_ok = dip_avg < shoulder_avg
    print(
        ("PASS" if dip_ok else "FAIL")
        + f": NPS detractor ripple 2-4 dip - avg_2_4={dip_avg:.2f} avg_0_1_5_6={shoulder_avg:.2f}"
    )
    errors += 0 if dip_ok else 1

    if not account.empty and "account_creation_type" in account.columns:
        digital = account["account_creation_type"].astype(str).str.upper().isin({"ONLINE", "DIGITAL", "SELF-SERVICE"})
        digital_frame = pd.DataFrame({"digital_onboarding": digital.map({True: "DIGITAL", False: "ASSISTED"})})
        errors += check_distribution(
            digital_frame,
            "digital_onboarding",
            {"DIGITAL": (0.70, 0.80), "ASSISTED": (0.20, 0.30)},
            "Digital onboarding proxy from workbook",
        )
        account_customer = derive_account_customer(base)
        if not account_customer.empty and {"account_creation_type", "nps_score"}.issubset(account_customer.columns):
            nps_values = pd.to_numeric(account_customer["nps_score"], errors="coerce")
            channel = account_customer["account_creation_type"].astype(str).str.upper()
            high_nps = nps_values >= 8
            low_nps = nps_values <= 4
            online = channel.isin({"ONLINE", "DIGITAL", "SELF-SERVICE"})
            branch = ~online
            online_high_rate = float(online[high_nps].mean()) if int(high_nps.sum()) else 0.0
            online_low_rate = float(online[low_nps].mean()) if int(low_nps.sum()) else 0.0
            branch_low_rate = float(branch[low_nps].mean()) if int(low_nps.sum()) else 0.0
            branch_high_rate = float(branch[high_nps].mean()) if int(high_nps.sum()) else 0.0
            overlap_ok = online_low_rate > 0 and branch_high_rate > 0
            shape_ok = online_high_rate > online_low_rate and branch_low_rate > branch_high_rate and overlap_ok
            print(
                ("PASS" if shape_ok else "FAIL")
                + ": Digital onboarding by NPS shape - "
                + str({
                    "online_high_nps_8_10": pct(online_high_rate),
                    "online_low_nps_0_4": pct(online_low_rate),
                    "branch_low_nps_0_4": pct(branch_low_rate),
                    "branch_high_nps_8_10": pct(branch_high_rate),
                    "overlap_present": overlap_ok,
                })
            )
            errors += 0 if shape_ok else 1
        else:
            print("SKIP: Digital onboarding by NPS shape - account/customer links missing")
    else:
        print("SKIP: Digital onboarding proxy - account_creation_type missing")

    self_service = derive_self_service_customer(base)
    required_self_service = {
        "account_creation_type",
        "account_last_access",
        "load_date",
        "operational_paperless_consent",
        "nps_score",
    }
    if not self_service.empty and required_self_service.issubset(self_service.columns):
        online = self_service["account_creation_type"].astype(str).str.upper().isin({"ONLINE", "DIGITAL", "SELF-SERVICE"})
        paperless = self_service["operational_paperless_consent"].astype(str).str.upper().eq("Y")
        last_access = parse_dt(self_service["account_last_access"])
        reference_dt = parse_dt(self_service["load_date"])
        recent_access = (reference_dt - last_access).dt.days.lt(30) & (reference_dt - last_access).dt.days.ge(0)
        adopted = online & paperless & recent_access
        errors += check_distribution(
            pd.DataFrame({"self_service": adopted.map({True: "ADOPTED", False: "NOT_ADOPTED"})}),
            "self_service",
            {"ADOPTED": (0.60, 0.70), "NOT_ADOPTED": (0.30, 0.40)},
            "Self-service adoption workbook distribution",
        )
        nps_values = pd.to_numeric(self_service["nps_score"], errors="coerce")
        high_nps = nps_values >= 9
        low_nps = nps_values <= 6
        adopted_high = float(adopted[high_nps].mean()) if int(high_nps.sum()) else 0.0
        adopted_low = float(adopted[low_nps].mean()) if int(low_nps.sum()) else 0.0
        not_adopted_low = float((~adopted)[low_nps].mean()) if int(low_nps.sum()) else 0.0
        not_adopted_high = float((~adopted)[high_nps].mean()) if int(high_nps.sum()) else 0.0
        shape_ok = adopted_high > adopted_low and not_adopted_low > not_adopted_high
        print(
            ("PASS" if shape_ok else "FAIL")
            + ": Self-service adoption by NPS shape - "
            + str({
                "adopted_high_nps_9_10": pct(adopted_high),
                "adopted_low_nps_0_6": pct(adopted_low),
                "not_adopted_low_nps_0_6": pct(not_adopted_low),
                "not_adopted_high_nps_9_10": pct(not_adopted_high),
            })
        )
        errors += 0 if shape_ok else 1
    else:
        print("SKIP: Self-service adoption proxy - account/person/customer fields missing")

    if not policy.empty and {"policy_issue_date", "policy_start_date"}.issubset(policy.columns):
        issue = parse_dt(policy["policy_issue_date"])
        start = parse_dt(policy["policy_start_date"])
        tat_days = (start - issue).dt.days
        invalid = int((tat_days.isna() | (tat_days < 0)).sum())
        if invalid:
            print(f"FAIL: Policy issuance TAT must be non-negative - invalid_rows={invalid}")
            errors += 1
        else:
            bands = pd.cut(
                tat_days,
                bins=[-1, 2, 7, float("inf")],
                labels=["DAYS_0_2", "DAYS_3_7", "DAYS_GT_7"],
            )
            print("PASS: Policy issuance TAT non-negative")
            errors += check_distribution(
                pd.DataFrame({"tat_band": bands.astype(str)}),
                "tat_band",
                {"DAYS_0_2": (0.65, 0.75), "DAYS_3_7": (0.15, 0.25), "DAYS_GT_7": (0.08, 0.12)},
                "Policy issuance TAT workbook distribution",
            )
    else:
        print("SKIP: Policy issuance TAT proxy - dates missing")

    if not quote.empty and "quote_hash_key" in quote.columns:
        quoted = quote["quote_hash_key"].astype(str).str.strip().ne("")
        if "quote_status" in quote.columns:
            accepted = quote["quote_status"].astype(str).str.upper().eq("ACCEPTED")
        else:
            linked_quotes = set()
            if not link_policy_quote.empty and "quote_hash_key" in link_policy_quote.columns:
                linked_quotes = set(link_policy_quote["quote_hash_key"].astype(str))
            accepted = quote["quote_hash_key"].astype(str).isin(linked_quotes)
        if int(quoted.sum()):
            drop_rate = float((quoted & ~accepted).sum()) / int(quoted.sum())
            ok = in_range(drop_rate, 0.05, 0.10)
            print(("PASS" if ok else "FAIL") + f": Drop-off during onboarding proxy - current={pct(drop_rate)} expected=5.00%-10.00%")
            errors += 0 if ok else 1
            if "quote_status" in quote.columns:
                status = quote["quote_status"].astype(str).str.upper()
                drop_status = pd.DataFrame({"quote_status": status[status.ne("ACCEPTED")]})
                errors += check_distribution(
                    drop_status,
                    "quote_status",
                    {"CREATED": (0.45, 0.55), "SENT": (0.30, 0.40), "EXPIRED": (0.10, 0.20)},
                    "Drop-off status workbook distribution",
                )
                quote_customer = derive_quote_customer(base)
                if not quote_customer.empty and {"quote_status", "nps_score"}.issubset(quote_customer.columns):
                    nps_values = pd.to_numeric(quote_customer["nps_score"], errors="coerce")
                    quote_status = quote_customer["quote_status"].astype(str).str.upper()
                    low_nps = nps_values <= 4
                    moderate_nps = nps_values.between(7, 8, inclusive="both")
                    high_nps = nps_values >= 8
                    expired_low = float(quote_status[low_nps].eq("EXPIRED").mean()) if int(low_nps.sum()) else 0.0
                    sent_moderate = float(quote_status[moderate_nps].eq("SENT").mean()) if int(moderate_nps.sum()) else 0.0
                    created_high = float(quote_status[high_nps].eq("CREATED").mean()) if int(high_nps.sum()) else 0.0
                    expired_high = float(quote_status[high_nps].eq("EXPIRED").mean()) if int(high_nps.sum()) else 0.0
                    shape_ok = expired_low > expired_high and created_high > expired_high and sent_moderate > expired_high
                    print(
                        ("PASS" if shape_ok else "FAIL")
                        + ": Drop-off status by NPS shape - "
                        + str({
                            "expired_low_nps_0_4": pct(expired_low),
                            "sent_moderate_nps_7_8": pct(sent_moderate),
                            "created_high_nps_8_10": pct(created_high),
                            "expired_high_nps_8_10": pct(expired_high),
                        })
                    )
                    errors += 0 if shape_ok else 1
                else:
                    print("SKIP: Drop-off status by NPS shape - quote/customer links missing")
        else:
            print("SKIP: Drop-off during onboarding proxy - quote_status blank")
    else:
        print("SKIP: Drop-off during onboarding proxy - quote data missing")

    quote_customer = derive_quote_customer(base)
    premium_columns = {"renewal_amt_current_period", "renewal_amt_next_period", "nps_score"}
    if not quote_customer.empty and premium_columns.issubset(quote_customer.columns):
        current_amt = pd.to_numeric(quote_customer["renewal_amt_current_period"], errors="coerce")
        next_amt = pd.to_numeric(quote_customer["renewal_amt_next_period"], errors="coerce")
        premium_pct = ((next_amt - current_amt) / current_amt) * 100
        valid_premium = current_amt.gt(0) & next_amt.ge(0) & premium_pct.notna()
        if not bool(valid_premium.all()):
            invalid = int((~valid_premium).sum())
            print(f"FAIL: Premium increase NPS proxy valid amounts - invalid_rows={invalid}")
            errors += 1
        premium_band = premium_pct.apply(lambda value: "LE_5" if value <= 5 else ("GT_5_LE_10" if value <= 10 else "GT_10"))
        errors += check_distribution(
            pd.DataFrame({"premium_band": premium_band}),
            "premium_band",
            {"LE_5": (0.65, 0.75), "GT_5_LE_10": (0.15, 0.25), "GT_10": (0.08, 0.12)},
            "Premium increase workbook distribution",
        )
        nps_values = pd.to_numeric(quote_customer["nps_score"], errors="coerce")
        high_nps = nps_values >= 9
        moderate_nps = nps_values.between(7, 8, inclusive="both")
        low_nps = nps_values <= 6
        le5_high = float(premium_band[high_nps].eq("LE_5").mean()) if int(high_nps.sum()) else 0.0
        mid_moderate = float(premium_band[moderate_nps].eq("GT_5_LE_10").mean()) if int(moderate_nps.sum()) else 0.0
        gt10_low = float(premium_band[low_nps].eq("GT_10").mean()) if int(low_nps.sum()) else 0.0
        gt10_high = float(premium_band[high_nps].eq("GT_10").mean()) if int(high_nps.sum()) else 0.0
        shape_ok = le5_high > gt10_high and gt10_low > gt10_high and mid_moderate > gt10_high
        print(
            ("PASS" if shape_ok else "FAIL")
            + ": Premium increase by NPS shape - "
            + str({
                "le5_high_nps_9_10": pct(le5_high),
                "mid_moderate_nps_7_8": pct(mid_moderate),
                "gt10_low_nps_0_6": pct(gt10_low),
                "gt10_high_nps_9_10": pct(gt10_high),
            })
        )
        errors += 0 if shape_ok else 1
    else:
        print("SKIP: Premium increase NPS proxy - quote/customer renewal fields missing")

    pc = derive_policy_customer(base)
    policy_premium_columns = {"renewal_amount_current_period", "renewal_amount_next_period", "nps_score"}
    if not pc.empty and policy_premium_columns.issubset(pc.columns):
        current_amt = pd.to_numeric(pc["renewal_amount_current_period"], errors="coerce")
        next_amt = pd.to_numeric(pc["renewal_amount_next_period"], errors="coerce")
        premium_pct = ((next_amt - current_amt) / current_amt) * 100
        valid_premium = current_amt.gt(0) & next_amt.ge(0) & premium_pct.notna()
        if not bool(valid_premium.all()):
            invalid = int((~valid_premium).sum())
            print(f"FAIL: Policy premium increase NPS proxy valid amounts - invalid_rows={invalid}")
            errors += 1
        premium_band = premium_pct.apply(lambda value: "LE_5" if value <= 5 else ("GT_5_LE_10" if value <= 10 else "GT_10"))
        errors += check_distribution(
            pd.DataFrame({"premium_band": premium_band}),
            "premium_band",
            {"LE_5": (0.65, 0.75), "GT_5_LE_10": (0.15, 0.25), "GT_10": (0.08, 0.12)},
            "Policy premium increase workbook distribution",
        )
        nps_values = pd.to_numeric(pc["nps_score"], errors="coerce")
        high_nps = nps_values >= 9
        moderate_nps = nps_values.between(7, 8, inclusive="both")
        low_nps = nps_values <= 6
        le5_high = float(premium_band[high_nps].eq("LE_5").mean()) if int(high_nps.sum()) else 0.0
        mid_moderate = float(premium_band[moderate_nps].eq("GT_5_LE_10").mean()) if int(moderate_nps.sum()) else 0.0
        gt10_low = float(premium_band[low_nps].eq("GT_10").mean()) if int(low_nps.sum()) else 0.0
        gt10_high = float(premium_band[high_nps].eq("GT_10").mean()) if int(high_nps.sum()) else 0.0
        shape_ok = le5_high > gt10_high and gt10_low > gt10_high and mid_moderate > gt10_high
        print(
            ("PASS" if shape_ok else "FAIL")
            + ": Policy premium increase by NPS shape - "
            + str({
                "le5_high_nps_9_10": pct(le5_high),
                "mid_moderate_nps_7_8": pct(mid_moderate),
                "gt10_low_nps_0_6": pct(gt10_low),
                "gt10_high_nps_9_10": pct(gt10_high),
            })
        )
        errors += 0 if shape_ok else 1
    else:
        print("SKIP: Policy premium increase NPS proxy - policy/customer renewal fields missing")

    if not pc.empty and {"policy_issue_date", "policy_start_date", "nps_score"}.issubset(pc.columns):
        issue = parse_dt(pc["policy_issue_date"])
        start = parse_dt(pc["policy_start_date"])
        tat_days = (start - issue).dt.days
        tat_band = pd.cut(
            tat_days,
            bins=[-1, 2, 7, float("inf")],
            labels=["DAYS_0_2", "DAYS_3_7", "DAYS_GT_7"],
        ).astype(str)
        nps_values = pd.to_numeric(pc["nps_score"], errors="coerce")
        nps_band = nps_values.apply(lambda value: "PROMOTER" if value >= 9 else ("PASSIVE" if value >= 7 else "DETRACTOR"))
        shape = pd.crosstab(tat_band, nps_band, normalize="columns")
        promoter_fast = float(shape.get("PROMOTER", {}).get("DAYS_0_2", 0.0))
        passive_mid = float(shape.get("PASSIVE", {}).get("DAYS_3_7", 0.0))
        detractor_slow = float(shape.get("DETRACTOR", {}).get("DAYS_GT_7", 0.0))
        shape_ok = (
            promoter_fast > float(shape.get("PASSIVE", {}).get("DAYS_0_2", 0.0))
            and promoter_fast > float(shape.get("DETRACTOR", {}).get("DAYS_0_2", 0.0))
            and passive_mid > float(shape.get("PROMOTER", {}).get("DAYS_3_7", 0.0))
            and passive_mid > float(shape.get("DETRACTOR", {}).get("DAYS_3_7", 0.0))
            and detractor_slow > float(shape.get("PROMOTER", {}).get("DAYS_GT_7", 0.0))
            and detractor_slow > float(shape.get("PASSIVE", {}).get("DAYS_GT_7", 0.0))
        )
        print(
            ("PASS" if shape_ok else "FAIL")
            + ": Policy issuance TAT by NPS shape - "
            + str((shape * 100).round(2).to_dict())
        )
        errors += 0 if shape_ok else 1

    if not pc.empty and {"sales_channel", "policy_cycle", "nps_score"}.issubset(pc.columns):
        renewal = pc[pd.to_numeric(pc["policy_cycle"], errors="coerce").fillna(0) > 1].copy()
        if renewal.empty:
            print("SKIP: Digital renewal proxy - no renewal policies with policy_cycle > 1")
        else:
            channel = renewal["sales_channel"].astype(str).str.upper()
            channel = channel.replace({"DIGITAL": "ONLINE", "WEB": "ONLINE", "BROKER": "AGENT"})
            errors += check_distribution(
                pd.DataFrame({"renewal_channel": channel}),
                "renewal_channel",
                {"ONLINE": (0.65, 0.75), "AGENT": (0.15, 0.25), "BRANCH": (0.08, 0.12)},
                "Digital renewal workbook distribution",
            )
            nps_values = pd.to_numeric(renewal["nps_score"], errors="coerce")
            high_nps = nps_values >= 9
            moderate_nps = nps_values.between(7, 8, inclusive="both")
            low_nps = nps_values <= 6
            online_high = float(channel[high_nps].eq("ONLINE").mean()) if int(high_nps.sum()) else 0.0
            agent_moderate = float(channel[moderate_nps].eq("AGENT").mean()) if int(moderate_nps.sum()) else 0.0
            branch_low = float(channel[low_nps].eq("BRANCH").mean()) if int(low_nps.sum()) else 0.0
            branch_high = float(channel[high_nps].eq("BRANCH").mean()) if int(high_nps.sum()) else 0.0
            shape_ok = online_high > branch_high and agent_moderate > branch_high and branch_low > branch_high
            print(
                ("PASS" if shape_ok else "FAIL")
                + ": Digital renewal by NPS shape - "
                + str({
                    "online_high_nps_9_10": pct(online_high),
                    "agent_moderate_nps_7_8": pct(agent_moderate),
                    "branch_low_nps_0_6": pct(branch_low),
                    "branch_high_nps_9_10": pct(branch_high),
                })
            )
            errors += 0 if shape_ok else 1
    else:
        print("SKIP: Digital renewal proxy - policy/customer data missing")

    if not claim.empty:
        if {"claim_reported_date", "claim_settlement_date"}.issubset(claim.columns):
            reported = parse_dt(claim["claim_reported_date"])
            settled = parse_dt(claim["claim_settlement_date"])
            tat = (settled - reported).dt.days
            valid = tat.dropna()
            if valid.empty:
                print("SKIP: Claim settlement TAT - no settled claims")
            else:
                invalid = int((valid < 0).sum())
                if invalid:
                    print(f"FAIL: Claim settlement TAT valid range - invalid_rows={invalid}")
                    errors += 1
                else:
                    print(f"PASS: Claim settlement TAT valid range - median_days={valid.median():.0f}")
                    claim_customer = derive_claim_customer(base)
                    if not claim_customer.empty and {"claim_reported_date", "claim_settlement_date", "nps_score"}.issubset(claim_customer.columns):
                        claim_tat = (parse_dt(claim_customer["claim_settlement_date"]) - parse_dt(claim_customer["claim_reported_date"])).dt.days
                        claim_band = pd.cut(
                            claim_tat,
                            bins=[-1, 15, 30, float("inf")],
                            labels=["DAYS_0_15", "DAYS_16_30", "DAYS_GT_30"],
                        ).astype(str)
                        errors += check_distribution(
                            pd.DataFrame({"claim_tat_band": claim_band}),
                            "claim_tat_band",
                            {"DAYS_0_15": (0.65, 0.75), "DAYS_16_30": (0.15, 0.25), "DAYS_GT_30": (0.08, 0.12)},
                            "Claim settlement TAT workbook distribution",
                        )
                        nps_values = pd.to_numeric(claim_customer["nps_score"], errors="coerce")
                        high_nps = nps_values >= 9
                        moderate_nps = nps_values.between(7, 8, inclusive="both")
                        low_nps = nps_values <= 6
                        fast_high = float(claim_band[high_nps].eq("DAYS_0_15").mean()) if int(high_nps.sum()) else 0.0
                        mid_moderate = float(claim_band[moderate_nps].eq("DAYS_16_30").mean()) if int(moderate_nps.sum()) else 0.0
                        slow_low = float(claim_band[low_nps].eq("DAYS_GT_30").mean()) if int(low_nps.sum()) else 0.0
                        slow_high = float(claim_band[high_nps].eq("DAYS_GT_30").mean()) if int(high_nps.sum()) else 0.0
                        shape_ok = fast_high > slow_high and mid_moderate > slow_high and slow_low > slow_high
                        print(
                            ("PASS" if shape_ok else "FAIL")
                            + ": Claim settlement TAT by NPS shape - "
                            + str({
                                "fast_high_nps_9_10": pct(fast_high),
                                "mid_moderate_nps_7_8": pct(mid_moderate),
                                "slow_low_nps_0_6": pct(slow_low),
                                "slow_high_nps_9_10": pct(slow_high),
                            })
                        )
                        errors += 0 if shape_ok else 1
        if "is_litigation" in claim.columns:
            lit_rate = claim["is_litigation"].astype(str).str.upper().eq("Y").mean()
            ok = in_range(lit_rate, 0.05, 0.12)
            print(("PASS" if ok else "FAIL") + f": Claim escalation proxy is_litigation - current={pct(lit_rate)} expected=8.00% with tolerance 5.00%-12.00%")
            errors += 0 if ok else 1
            claim_customer = derive_claim_customer(base)
            if not claim_customer.empty and {"is_litigation", "nps_score"}.issubset(claim_customer.columns):
                nps_values = pd.to_numeric(claim_customer["nps_score"], errors="coerce")
                escalated = claim_customer["is_litigation"].astype(str).str.upper().eq("Y")
                low_nps = nps_values <= 6
                high_nps = nps_values >= 9
                escalated_low = float(escalated[low_nps].mean()) if int(low_nps.sum()) else 0.0
                escalated_high = float(escalated[high_nps].mean()) if int(high_nps.sum()) else 0.0
                non_escalated_high = float((~escalated)[high_nps].mean()) if int(high_nps.sum()) else 0.0
                shape_ok = escalated_low > escalated_high and non_escalated_high > escalated_high
                print(
                    ("PASS" if shape_ok else "FAIL")
                    + ": Claim escalation by NPS shape - "
                    + str({
                        "escalated_low_nps_0_6": pct(escalated_low),
                        "escalated_high_nps_9_10": pct(escalated_high),
                        "non_escalated_high_nps_9_10": pct(non_escalated_high),
                    })
                )
                errors += 0 if shape_ok else 1
            else:
                print("SKIP: Claim escalation by NPS shape - claim/customer links missing")
            claim_complaint = derive_claim_complaint_customer(base)
            if not claim_complaint.empty and {"has_claim_complaint", "nps_score"}.issubset(claim_complaint.columns):
                complaint_rate = float(claim_complaint["has_claim_complaint"].mean())
                ok = in_range(complaint_rate, 0.12, 0.18)
                print(("PASS" if ok else "FAIL") + f": Claim complaint flag distribution - current={pct(complaint_rate)} expected=15.00% with tolerance 12.00%-18.00%")
                errors += 0 if ok else 1
                nps_values = pd.to_numeric(claim_complaint["nps_score"], errors="coerce")
                has_complaint = claim_complaint["has_claim_complaint"].astype(bool)
                low_nps = nps_values <= 6
                high_nps = nps_values >= 9
                complaint_low = float(has_complaint[low_nps].mean()) if int(low_nps.sum()) else 0.0
                complaint_high = float(has_complaint[high_nps].mean()) if int(high_nps.sum()) else 0.0
                no_complaint_high = float((~has_complaint)[high_nps].mean()) if int(high_nps.sum()) else 0.0
                shape_ok = complaint_low > complaint_high and no_complaint_high > complaint_high
                print(
                    ("PASS" if shape_ok else "FAIL")
                    + ": Claim complaint flag by NPS shape - "
                    + str({
                        "complaint_low_nps_0_6": pct(complaint_low),
                        "complaint_high_nps_9_10": pct(complaint_high),
                        "no_complaint_high_nps_9_10": pct(no_complaint_high),
                    })
                )
                errors += 0 if shape_ok else 1
            else:
                print("SKIP: Claim complaint flag - claim/complaint/customer links missing")
        if "claim_channel" in claim.columns:
            claim_customer = derive_claim_customer(base)
            if not claim_customer.empty and {"claim_channel", "nps_score"}.issubset(claim_customer.columns):
                channel = claim_customer["claim_channel"].astype(str).str.upper().replace({"DIGITAL": "ONLINE", "WEB": "ONLINE", "BROKER": "AGENT"})
                errors += check_distribution(
                    pd.DataFrame({"claim_channel": channel}),
                    "claim_channel",
                    {"ONLINE": (0.65, 0.75), "AGENT": (0.15, 0.25), "BRANCH": (0.08, 0.12)},
                    "Claim channel used workbook distribution",
                )
                nps_values = pd.to_numeric(claim_customer["nps_score"], errors="coerce")
                high_nps = nps_values >= 9
                moderate_nps = nps_values.between(7, 8, inclusive="both")
                low_nps = nps_values <= 6
                online_high = float(channel[high_nps].eq("ONLINE").mean()) if int(high_nps.sum()) else 0.0
                agent_moderate = float(channel[moderate_nps].eq("AGENT").mean()) if int(moderate_nps.sum()) else 0.0
                branch_low = float(channel[low_nps].eq("BRANCH").mean()) if int(low_nps.sum()) else 0.0
                agent_nps10 = float(channel[nps_values.eq(10)].eq("AGENT").mean()) if int(nps_values.eq(10).sum()) else 0.0
                shape_ok = online_high > branch_low and branch_low > agent_nps10 and agent_moderate > agent_nps10
                print(
                    ("PASS" if shape_ok else "FAIL")
                    + ": Claim channel by NPS shape - "
                    + str({
                        "online_high_nps_9_10": pct(online_high),
                        "agent_moderate_nps_7_8": pct(agent_moderate),
                        "branch_low_nps_0_6": pct(branch_low),
                        "agent_nps_10": pct(agent_nps10),
                    })
                )
                errors += 0 if shape_ok else 1
            else:
                print("PASS: Claim channel used proxy populated")
        claim_customer = derive_claim_customer(base)
        if not claim_customer.empty and {"claim_satisfaction_score", "nps_score"}.issubset(claim_customer.columns):
            score = pd.to_numeric(claim_customer["claim_satisfaction_score"], errors="coerce")
            nps_values = pd.to_numeric(claim_customer["nps_score"], errors="coerce")
            high_score = score.ge(4)
            low_score = score.le(2)
            high_nps = nps_values >= 9
            low_nps = nps_values <= 3
            high_csat_high_nps = float(high_score[high_nps].mean()) if int(high_nps.sum()) else 0.0
            low_csat_low_nps = float(low_score[low_nps].mean()) if int(low_nps.sum()) else 0.0
            low_csat_high_nps = float(low_score[high_nps].mean()) if int(high_nps.sum()) else 0.0
            shape_ok = high_csat_high_nps > low_csat_high_nps and low_csat_low_nps > low_csat_high_nps
            print(
                ("PASS" if shape_ok else "FAIL")
                + ": Claim CSAT by NPS shape - "
                + str({
                    "claim_csat_4_5_high_nps_9_10": pct(high_csat_high_nps),
                    "claim_csat_1_2_low_nps_0_3": pct(low_csat_low_nps),
                    "claim_csat_1_2_high_nps_9_10": pct(low_csat_high_nps),
                })
            )
            errors += 0 if shape_ok else 1
    else:
        print("SKIP: Claim NPS proxies - claim data missing")

    marketing_customer = derive_marketing_customer(base)
    if not marketing_customer.empty and {"customer_service_call_frequency", "nps_score"}.issubset(marketing_customer.columns):
        call_freq = pd.to_numeric(marketing_customer["customer_service_call_frequency"], errors="coerce").fillna(0)
        contact_band = call_freq.apply(lambda value: "CONTACTS_0_1" if value <= 1 else ("CONTACTS_2_3" if value <= 3 else "CONTACTS_GT_3"))
        errors += check_distribution(
            pd.DataFrame({"contact_band": contact_band}),
            "contact_band",
            {"CONTACTS_0_1": (0.55, 0.65), "CONTACTS_2_3": (0.25, 0.35), "CONTACTS_GT_3": (0.08, 0.12)},
            "Renewal contact count workbook distribution",
        )
        nps_values = pd.to_numeric(marketing_customer["nps_score"], errors="coerce")
        high_nps = nps_values >= 9
        moderate_nps = nps_values.between(7, 8, inclusive="both")
        low_nps = nps_values <= 6
        low_contact_high = float(contact_band[high_nps].eq("CONTACTS_0_1").mean()) if int(high_nps.sum()) else 0.0
        mid_contact_moderate = float(contact_band[moderate_nps].eq("CONTACTS_2_3").mean()) if int(moderate_nps.sum()) else 0.0
        high_contact_low = float(contact_band[low_nps].eq("CONTACTS_GT_3").mean()) if int(low_nps.sum()) else 0.0
        high_contact_high = float(contact_band[high_nps].eq("CONTACTS_GT_3").mean()) if int(high_nps.sum()) else 0.0
        shape_ok = low_contact_high > high_contact_high and mid_contact_moderate > high_contact_high and high_contact_low > high_contact_high
        print(
            ("PASS" if shape_ok else "FAIL")
            + ": Renewal contacts by NPS shape - "
            + str({
                "contacts_0_1_high_nps_9_10": pct(low_contact_high),
                "contacts_2_3_moderate_nps_7_8": pct(mid_contact_moderate),
                "contacts_gt_3_low_nps_0_6": pct(high_contact_low),
                "contacts_gt_3_high_nps_9_10": pct(high_contact_high),
            })
        )
        errors += 0 if shape_ok else 1
    else:
        print("SKIP: Renewal contact count proxy - marketing/customer links missing")

    if not customer.empty and {"customer_satisfaction", "nps_score"}.issubset(customer.columns):
        satisfaction = customer["customer_satisfaction"].astype(str).str.upper()
        nps_values = pd.to_numeric(customer["nps_score"], errors="coerce")
        high_csat = satisfaction.isin({"VERY_SATISFIED", "SATISFIED"})
        low_csat = satisfaction.eq("DISSATISFIED")
        high_nps = nps_values >= 9
        low_nps = nps_values <= 3
        high_csat_high_nps = float(high_csat[high_nps].mean()) if int(high_nps.sum()) else 0.0
        low_csat_low_nps = float(low_csat[low_nps].mean()) if int(low_nps.sum()) else 0.0
        low_csat_high_nps = float(low_csat[high_nps].mean()) if int(high_nps.sum()) else 0.0
        shape_ok = high_csat_high_nps > low_csat_high_nps and low_csat_low_nps > low_csat_high_nps
        print(
            ("PASS" if shape_ok else "FAIL")
            + ": Customer CSAT by NPS shape - "
            + str({
                "satisfied_high_nps_9_10": pct(high_csat_high_nps),
                "dissatisfied_low_nps_0_3": pct(low_csat_low_nps),
                "dissatisfied_high_nps_9_10": pct(low_csat_high_nps),
            })
        )
        errors += 0 if shape_ok else 1

    if not complaint.empty:
        if {"complaint_date", "complaint_resolved_date"}.issubset(complaint.columns):
            start = parse_dt(complaint["complaint_date"])
            resolved = parse_dt(complaint["complaint_resolved_date"])
            tat = (resolved - start).dt.days.dropna()
            invalid = int(((tat < 0) | (tat > 365)).sum())
            if invalid:
                print(f"FAIL: Complaint resolution TAT valid range - invalid_rows={invalid}")
                errors += 1
            else:
                print(f"PASS: Complaint resolution TAT valid range - median_days={tat.median():.0f}")
                bands = pd.cut(
                    tat,
                    bins=[-1, 2, 7, 365],
                    labels=["DAYS_0_2", "DAYS_3_7", "DAYS_GT_7"],
                )
                errors += check_distribution(
                    pd.DataFrame({"tat_band": bands.astype(str)}),
                    "tat_band",
                    {"DAYS_0_2": (0.55, 0.65), "DAYS_3_7": (0.25, 0.35), "DAYS_GT_7": (0.08, 0.12)},
                    "Complaint resolution TAT workbook distribution",
                )
                complaint_customer = (
                    link_complaint_policy[["complaint_hash_key", "policy_hash_key"]]
                    .merge(complaint[["complaint_hash_key", "complaint_date", "complaint_resolved_date"]], on="complaint_hash_key", how="inner")
                    .merge(read_csv_safe(base, "link_policy_customer.csv")[["policy_hash_key", "customer_hash_key"]], on="policy_hash_key", how="inner")
                    .merge(customer[["customer_hash_key", "nps_score"]], on="customer_hash_key", how="inner")
                    if not link_complaint_policy.empty and not customer.empty
                    else pd.DataFrame()
                )
                if not complaint_customer.empty:
                    resolution_days = (
                        parse_dt(complaint_customer["complaint_resolved_date"])
                        - parse_dt(complaint_customer["complaint_date"])
                    ).dt.days
                    resolution_band = pd.cut(
                        resolution_days,
                        bins=[-1, 2, 7, 365],
                        labels=["DAYS_0_2", "DAYS_3_7", "DAYS_GT_7"],
                    ).astype(str)
                    nps_values = pd.to_numeric(complaint_customer["nps_score"], errors="coerce")
                    high_nps = nps_values >= 9
                    moderate_nps = nps_values.between(7, 8, inclusive="both")
                    low_nps = nps_values <= 6
                    fast_high = float(resolution_band[high_nps].eq("DAYS_0_2").mean()) if int(high_nps.sum()) else 0.0
                    mid_moderate = float(resolution_band[moderate_nps].eq("DAYS_3_7").mean()) if int(moderate_nps.sum()) else 0.0
                    slow_low = float(resolution_band[low_nps].eq("DAYS_GT_7").mean()) if int(low_nps.sum()) else 0.0
                    slow_high = float(resolution_band[high_nps].eq("DAYS_GT_7").mean()) if int(high_nps.sum()) else 0.0
                    shape_ok = fast_high > slow_high and mid_moderate > slow_high and slow_low > slow_high
                    print(
                        ("PASS" if shape_ok else "FAIL")
                        + ": Complaint resolution TAT by NPS shape - "
                        + str({
                            "fast_high_nps_9_10": pct(fast_high),
                            "mid_moderate_nps_7_8": pct(mid_moderate),
                            "slow_low_nps_0_6": pct(slow_low),
                            "slow_high_nps_9_10": pct(slow_high),
                        })
                    )
                    errors += 0 if shape_ok else 1
                else:
                    print("SKIP: Complaint resolution TAT by NPS shape - complaint/customer links missing")
        if "is_financial_ombudsman_service_referral" in complaint.columns:
            fos_rate = complaint["is_financial_ombudsman_service_referral"].astype(str).str.upper().eq("Y").mean()
            ok = in_range(fos_rate, 0.015, 0.025)
            print(("PASS" if ok else "FAIL") + f": Complaint escalation proxy FOS referral - current={pct(fos_rate)} expected=2.00% with tolerance 1.50%-2.50%")
            errors += 0 if ok else 1
            complaint_customer = derive_complaint_customer(base)
            if not complaint_customer.empty and {"is_financial_ombudsman_service_referral", "nps_score"}.issubset(complaint_customer.columns):
                nps_values = pd.to_numeric(complaint_customer["nps_score"], errors="coerce")
                escalated = complaint_customer["is_financial_ombudsman_service_referral"].astype(str).str.upper().eq("Y")
                low_nps = nps_values <= 2
                high_nps = nps_values >= 9
                escalated_low = float(escalated[low_nps].mean()) if int(low_nps.sum()) else 0.0
                escalated_high = float(escalated[high_nps].mean()) if int(high_nps.sum()) else 0.0
                shape_ok = escalated_low > escalated_high and escalated_high == 0.0
                print(
                    ("PASS" if shape_ok else "FAIL")
                    + ": Complaint escalation by NPS shape - "
                    + str({
                        "fos_low_nps_0_2": pct(escalated_low),
                        "fos_high_nps_9_10": pct(escalated_high),
                    })
                )
                errors += 0 if shape_ok else 1
        if {"complaint_status", "complaint_upheld_status"}.issubset(complaint.columns):
            blank = complaint["complaint_status"].astype(str).str.strip().eq("").sum() + complaint["complaint_upheld_status"].astype(str).str.strip().eq("").sum()
            if int(blank):
                print(f"FAIL: Complaint status outcome proxy populated - blank_cells={int(blank)}")
                errors += 1
            else:
                print("PASS: Complaint status outcome proxy populated")
                status = complaint["complaint_upheld_status"].astype(str).str.upper().str.replace(" ", "_")
                errors += check_distribution(
                    pd.DataFrame({"complaint_outcome": status}),
                    "complaint_outcome",
                    {"NOT_UPHELD": (0.60, 0.70), "UPHELD": (0.15, 0.25), "PARTIALLY_UPHELD": (0.10, 0.20)},
                    "Complaint status outcome workbook distribution",
                )
                complaint_customer = derive_complaint_customer(base)
                if not complaint_customer.empty and {"complaint_upheld_status", "nps_score"}.issubset(complaint_customer.columns):
                    nps_values = pd.to_numeric(complaint_customer["nps_score"], errors="coerce")
                    status_customer = complaint_customer["complaint_upheld_status"].astype(str).str.upper().str.replace(" ", "_")
                    not_upheld_low = float(status_customer[nps_values <= 4].eq("NOT_UPHELD").mean()) if int((nps_values <= 4).sum()) else 0.0
                    partial_mid = float(status_customer[nps_values.between(3, 6, inclusive="both")].eq("PARTIALLY_UPHELD").mean()) if int(nps_values.between(3, 6, inclusive="both").sum()) else 0.0
                    upheld_mid = float(status_customer[nps_values.between(4, 7, inclusive="both")].eq("UPHELD").mean()) if int(nps_values.between(4, 7, inclusive="both").sum()) else 0.0
                    not_upheld_high = float(status_customer[nps_values >= 9].eq("NOT_UPHELD").mean()) if int((nps_values >= 9).sum()) else 0.0
                    shape_ok = not_upheld_low > not_upheld_high and partial_mid > 0 and upheld_mid > 0
                    print(
                        ("PASS" if shape_ok else "FAIL")
                        + ": Complaint status by NPS shape - "
                        + str({
                            "not_upheld_low_nps_0_4": pct(not_upheld_low),
                            "partially_upheld_nps_3_6": pct(partial_mid),
                            "upheld_nps_4_7": pct(upheld_mid),
                            "not_upheld_high_nps_9_10": pct(not_upheld_high),
                        })
                    )
                    errors += 0 if shape_ok else 1
        complaint_customer = derive_complaint_customer(base)
        if not complaint_customer.empty and {"customer_hash_key", "complaint_hash_key", "nps_score"}.issubset(complaint_customer.columns):
            complaint_counts = complaint_customer.groupby("customer_hash_key")["complaint_hash_key"].nunique()
            repeat = complaint_counts.gt(1)
            repeat_rate = float(repeat.mean()) if len(repeat) else 0.0
            ok = in_range(repeat_rate, 0.07, 0.13)
            print(("PASS" if ok else "FAIL") + f": Repeat complaint workbook distribution - current={pct(repeat_rate)} expected=10.00% with tolerance 7.00%-13.00%")
            errors += 0 if ok else 1
            nps_by_customer = complaint_customer.groupby("customer_hash_key")["nps_score"].first()
            low_repeat = float(repeat[nps_by_customer <= 3].mean()) if int((nps_by_customer <= 3).sum()) else 0.0
            high_repeat = float(repeat[nps_by_customer >= 9].mean()) if int((nps_by_customer >= 9).sum()) else 0.0
            shape_ok = low_repeat > high_repeat
            print(
                ("PASS" if shape_ok else "FAIL")
                + ": Repeat complaint by NPS shape - "
                + str({
                    "repeat_low_nps_0_3": pct(low_repeat),
                    "repeat_high_nps_9_10": pct(high_repeat),
                })
            )
            errors += 0 if shape_ok else 1
    else:
        print("SKIP: Complaint NPS proxies - complaint data missing")

    if errors:
        print(f"NPS feature validation failed with {errors} issue(s).")
        return False
    print("NPS feature validation passed.")
    return True


def parse_args():
    parser = argparse.ArgumentParser(description="Validate NPS feature proxies against workbook rules.")
    parser.add_argument("folder", nargs="?", help="MLOps synthetic folder. Defaults to latest data/synthetic/mlops run.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    folder = args.folder or latest_subdir(MLOPS_ROOT)
    if not folder:
        print("No MLOps synthetic folder found.")
        return 1
    return 0 if verify_nps(folder) else 1


if __name__ == "__main__":
    raise SystemExit(main())
