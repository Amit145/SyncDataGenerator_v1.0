from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.storage_paths import SYNTHETIC_MLOPS_ROOT


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


def load_ranges() -> dict:
    path = ROOT / "config" / "scenario_v1.json"
    cfg = json.loads(path.read_text(encoding="utf-8"))
    return cfg.get("churn_settings", {}).get("mlops_churn_expected_ranges", {})


def churn_flag(series: pd.Series) -> pd.Series:
    return series.astype(str).str.upper().isin(["CANCELLED", "LAPSED"])


def in_range(value: float, limits: list[float]) -> bool:
    return limits[0] <= value <= limits[1]


def check_rates(frame: pd.DataFrame, band_col: str, ranges: dict, label: str, min_count: int = 20) -> int:
    if frame.empty or band_col not in frame.columns or "churn_flag" not in frame.columns:
        print(f"SKIP: {label} - required data missing")
        return 0

    grouped = frame.groupby(band_col)["churn_flag"].agg(["count", "mean"])
    detail = {}
    errors = 0
    for band, limits in ranges.items():
        if band not in grouped.index:
            detail[band] = {"count": 0, "churn_pct": None, "expected": pct_range(limits), "status": "SKIP"}
            continue
        count = int(grouped.loc[band, "count"])
        rate = float(grouped.loc[band, "mean"])
        status = "PASS" if count >= min_count and in_range(rate, limits) else "FAIL"
        if count < min_count:
            status = "SKIP"
        if status == "FAIL":
            errors += 1
        detail[band] = {
            "count": count,
            "churn_pct": round(rate * 100, 2),
            "expected": pct_range(limits),
            "status": status,
        }
    ok = errors == 0
    print(("PASS" if ok else "FAIL") + f": {label} - rates={detail}")
    return errors


def pct_range(limits: list[float]) -> str:
    return f"{limits[0] * 100:.0f}-{limits[1] * 100:.0f}%"


def policy_frame(folder: Path) -> pd.DataFrame:
    policy = read_csv_safe(folder, "sat_policy.csv")
    if policy.empty:
        return policy
    policy = policy.copy()
    policy["churn_flag"] = churn_flag(policy["policy_status"])
    policy["auto_renew_enabled_band"] = policy["is_auto_renew_enabled"].astype(str).str.upper().map({"Y": "ON", "N": "OFF"})
    ncd = pd.to_numeric(policy["no_claims_discount_years"], errors="coerce").fillna(0)
    policy["ncd_years_band"] = pd.cut(
        ncd,
        bins=[-1, 1, 4, 8, 999],
        labels=["0_1", "2_4", "5_8", "9_PLUS"],
    ).astype(str)
    policy["payment_method_band"] = policy["payment_method"].astype(str).str.upper().map({
        "BANK_TRANSFER": "ANNUAL",
        "DIRECT_DEBIT": "MONTHLY_DD",
        "CARD": "CARD_MANUAL",
    })
    policy["direct_debit_cancellation_band"] = policy["is_direct_debit_cancellation"].astype(str).str.upper().map({"Y": "YES", "N": "NO"})
    missed = pd.to_numeric(policy["missed_payment_count"], errors="coerce").fillna(0)
    policy["missed_payments_band"] = missed.apply(lambda value: "3_PLUS" if value >= 3 else str(int(value)))
    policy["loyalty_discount_band"] = policy["loyalty_discount_usage"].astype(str).str.upper().map({
        "RETAINED": "RETAINED",
        "NOT_APPLIED": "NOT_APPLIED",
        "REMOVED": "REMOVED",
        "Y": "RETAINED",
        "N": "NOT_APPLIED",
    })
    policy["installment_default_band"] = policy["is_installment_default"].astype(str).str.upper().map({"Y": "YES", "N": "NO"})
    return policy


def claim_policy_frame(folder: Path, policy: pd.DataFrame) -> pd.DataFrame:
    claim = read_csv_safe(folder, "sat_claim.csv")
    link = read_csv_safe(folder, "link_claim_policy.csv")
    if any(df.empty for df in [claim, link, policy]):
        return pd.DataFrame()
    frame = (
        link[["claim_hash_key", "policy_hash_key"]]
        .merge(claim[["claim_hash_key", "is_fault_claim", "claim_satisfaction_score"]], on="claim_hash_key", how="inner")
        .merge(policy[["policy_hash_key", "churn_flag"]], on="policy_hash_key", how="inner")
    )
    frame["fault_claim_band"] = frame["is_fault_claim"].astype(str).str.upper().map({"Y": "YES", "N": "NO"})
    score = pd.to_numeric(frame["claim_satisfaction_score"], errors="coerce")
    frame["claim_satisfaction_band"] = pd.cut(score, bins=[0, 4, 7, 10], labels=["LOW", "NEUTRAL", "HIGH"]).astype(str)
    return frame


def marketing_policy_frame(folder: Path, policy: pd.DataFrame) -> pd.DataFrame:
    marketing = read_csv_safe(folder, "sat_marketing_engagement.csv")
    link_pm = read_csv_safe(folder, "link_person_marketing_engagement.csv")
    link_pc = read_csv_safe(folder, "link_policy_customer.csv")
    link_cp = read_csv_safe(folder, "link_customer_person.csv")
    if any(df.empty for df in [marketing, link_pm, link_pc, link_cp, policy]):
        return pd.DataFrame()
    frame = (
        link_pc[["policy_hash_key", "customer_hash_key"]]
        .merge(link_cp[["customer_hash_key", "person_hash_key"]], on="customer_hash_key", how="inner")
        .merge(link_pm[["person_hash_key", "marketing_engagement_hash_key"]], on="person_hash_key", how="inner")
        .merge(
            marketing[[
                "marketing_engagement_hash_key",
                "has_retention_team_interaction",
                "average_call_sentiment",
                "engagement_score",
            ]],
            on="marketing_engagement_hash_key",
            how="inner",
        )
        .merge(policy[["policy_hash_key", "churn_flag"]], on="policy_hash_key", how="inner")
    )
    frame["retention_contacted_band"] = frame["has_retention_team_interaction"].astype(str).str.upper().map({"Y": "YES", "N": "NO"})
    frame["call_sentiment_band"] = frame["average_call_sentiment"].astype(str).str.upper()
    score = pd.to_numeric(frame["engagement_score"], errors="coerce")
    frame["engagement_score_band"] = pd.cut(
        score,
        bins=[-1, 25, 55, 75, 100],
        labels=["VERY_LOW", "LOW", "MEDIUM", "HIGH"],
    ).astype(str)
    return frame


def verify(folder: str) -> bool:
    base = Path(folder)
    ranges = load_ranges()
    policy = policy_frame(base)
    errors = 0
    policy_checks = [
        ("auto_renew_enabled", "auto_renew_enabled_band", "auto-renew enabled churn"),
        ("ncd_years", "ncd_years_band", "NCD years churn"),
        ("payment_method", "payment_method_band", "payment method churn"),
        ("direct_debit_cancellation", "direct_debit_cancellation_band", "direct debit cancellation churn"),
        ("missed_payments", "missed_payments_band", "missed payments churn"),
        ("loyalty_discount", "loyalty_discount_band", "loyalty discount churn"),
        ("installment_default", "installment_default_band", "installment default churn"),
    ]
    for key, column, label in policy_checks:
        errors += check_rates(policy, column, ranges.get(key, {}), label)

    claim = claim_policy_frame(base, policy)
    errors += check_rates(claim, "fault_claim_band", ranges.get("fault_claim", {}), "fault claim churn", min_count=10)
    errors += check_rates(claim, "claim_satisfaction_band", ranges.get("claim_satisfaction", {}), "claim satisfaction churn", min_count=10)

    marketing = marketing_policy_frame(base, policy)
    errors += check_rates(marketing, "retention_contacted_band", ranges.get("retention_contacted", {}), "retention contacted churn")
    errors += check_rates(marketing, "call_sentiment_band", ranges.get("call_sentiment", {}), "call sentiment churn")
    errors += check_rates(marketing, "engagement_score_band", ranges.get("engagement_score", {}), "engagement score churn")

    if errors:
        print(f"MLOps churn KPI validation failed with {errors} issue(s).")
        return False
    print("MLOps churn KPI validation passed.")
    return True


def parse_args():
    parser = argparse.ArgumentParser(description="Validate MLOps churn KPI ratios against workbook ranges.")
    parser.add_argument("folder", nargs="?", help="MLOps synthetic folder. Defaults to latest data/synthetic/mlops run.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    target = args.folder or latest_subdir(SYNTHETIC_MLOPS_ROOT)
    if not target:
        raise SystemExit("No MLOps synthetic folder found.")
    raise SystemExit(0 if verify(target) else 1)
