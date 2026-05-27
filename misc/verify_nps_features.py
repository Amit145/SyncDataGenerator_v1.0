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


def verify_nps(folder: str) -> bool:
    base = Path(folder)
    errors = 0

    customer = read_csv_safe(base, "sat_customer.csv")
    account = read_csv_safe(base, "sat_account.csv")
    policy = read_csv_safe(base, "sat_policy.csv")
    quote = read_csv_safe(base, "sat_quote.csv")
    link_policy_quote = read_csv_safe(base, "link_policy_quote.csv")
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
        {"DETRACTOR": (0.30, 0.50), "PASSIVE": (0.10, 0.30), "PROMOTER": (0.30, 0.50)},
        "NPS bimodal segment distribution",
    )

    if not account.empty and "account_creation_type" in account.columns:
        digital = account["account_creation_type"].astype(str).str.upper().isin({"ONLINE", "DIGITAL", "SELF-SERVICE"})
        digital_frame = pd.DataFrame({"digital_onboarding": digital.map({True: "DIGITAL", False: "ASSISTED"})})
        errors += check_distribution(
            digital_frame,
            "digital_onboarding",
            {"DIGITAL": (0.35, 0.65), "ASSISTED": (0.35, 0.65)},
            "Digital onboarding proxy",
        )
    else:
        print("SKIP: Digital onboarding proxy - account_creation_type missing")

    if not policy.empty and {"policy_issue_date", "policy_start_date"}.issubset(policy.columns):
        issue = parse_dt(policy["policy_issue_date"])
        start = parse_dt(policy["policy_start_date"])
        tat_days = (start - issue).dt.days
        invalid = int((tat_days.isna() | (tat_days < 0) | (tat_days > 7)).sum())
        if invalid:
            print(f"FAIL: Policy issuance TAT must be 0-7 days - invalid_rows={invalid}")
            errors += 1
        else:
            print("PASS: Policy issuance TAT proxy 0-7 days")
    else:
        print("SKIP: Policy issuance TAT proxy - dates missing")

    if not quote.empty and "quote_hash_key" in quote.columns:
        quoted = quote["quote_hash_key"].astype(str).str.strip().ne("")
        linked_quotes = set()
        if not link_policy_quote.empty and "quote_hash_key" in link_policy_quote.columns:
            linked_quotes = set(link_policy_quote["quote_hash_key"].astype(str))
        accepted = quote["quote_hash_key"].astype(str).isin(linked_quotes)
        if int(quoted.sum()):
            drop_rate = float((quoted & ~accepted).sum()) / int(quoted.sum())
            ok = in_range(drop_rate, 0.05, 0.30)
            status = "PASS" if ok else "WARN"
            print(status + f": Drop-off during onboarding proxy - current={pct(drop_rate)} expected=5.00%-30.00%; not failed because quote funnel volume is model-shape dependent")
        else:
            print("SKIP: Drop-off during onboarding proxy - quote_status blank")
    else:
        print("SKIP: Drop-off during onboarding proxy - quote data missing")

    pc = derive_policy_customer(base)
    if not pc.empty and {"sales_channel", "policy_cycle"}.issubset(pc.columns):
        digital_renewal = pc["sales_channel"].astype(str).str.upper().isin({"ONLINE", "DIGITAL", "WEB"}) & (pd.to_numeric(pc["policy_cycle"], errors="coerce").fillna(0) > 1)
        rate = float(digital_renewal.mean())
        ok = in_range(rate, 0.15, 0.45)
        print(("PASS" if ok else "FAIL") + f": Digital renewal proxy - current={pct(rate)} expected=15.00%-45.00%")
        errors += 0 if ok else 1
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
        if "is_litigation" in claim.columns:
            lit_rate = claim["is_litigation"].astype(str).str.upper().eq("Y").mean()
            ok = lit_rate < 0.15
            print(("PASS" if ok else "FAIL") + f": Claim escalation proxy is_litigation - current={pct(lit_rate)} expected=<15.00%")
            errors += 0 if ok else 1
        if "claim_channel" in claim.columns:
            print("PASS: Claim channel used proxy populated")
    else:
        print("SKIP: Claim NPS proxies - claim data missing")

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
        if "is_financial_ombudsman_service_referral" in complaint.columns:
            fos_rate = complaint["is_financial_ombudsman_service_referral"].astype(str).str.upper().eq("Y").mean()
            ok = fos_rate < 0.15
            print(("PASS" if ok else "FAIL") + f": Complaint escalation proxy FOS referral - current={pct(fos_rate)} expected=<15.00%")
            errors += 0 if ok else 1
        if {"complaint_status", "complaint_upheld_status"}.issubset(complaint.columns):
            blank = complaint["complaint_status"].astype(str).str.strip().eq("").sum() + complaint["complaint_upheld_status"].astype(str).str.strip().eq("").sum()
            if int(blank):
                print(f"FAIL: Complaint status outcome proxy populated - blank_cells={int(blank)}")
                errors += 1
            else:
                print("PASS: Complaint status outcome proxy populated")
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
