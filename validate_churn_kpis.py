from __future__ import annotations

import argparse
import math
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd


REQUIRED_POLICY_COLS = {
    "policy_hash_key",
    "load_date",
    "cover_option",
    "declined_claims",
    "number_of_active_claim",
    "number_of_previous_claim",
    "policy_cycle",
    "policy_start_date",
    "policy_status",
    "renewal_amount_current_period",
    "renewal_amount_next_period",
    "sales_channel",
}

REQUIRED_MARKETING_COLS = {
    "email_subscriptions",
    "commercial_email",
    "email",
    "sms",
    "call",
}

STANDARD_VEHICLES = {"Focus", "Corsa", "Corolla"}
PREMIUM_VEHICLES = {"Qashqai", "3 Series", "A3"}
HIGH_RISK_VEHICLES = {"Sport Bike", "Superbike", "High Performance SUV"}
COVER_OPTIONS = {"BASE_ONLY", "ONE_ADD_ON", "TWO_ADD_ONS", "THREE_PLUS_ADD_ONS"}


def latest_subdir(root: Path) -> Path | None:
    if not root.exists():
        return None
    dirs = [path for path in root.iterdir() if path.is_dir()]
    if not dirs:
        return None
    return max(dirs, key=lambda path: (path.name, path.stat().st_mtime))


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(col).strip().lower().replace(" ", "_") for col in df.columns]
    return df


def read_table(base_path: Path, file_name: str) -> pd.DataFrame:
    path = base_path / file_name
    if not path.exists():
        return pd.DataFrame()
    return normalize_columns(pd.read_csv(path))


def yn_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.upper().map(
        lambda value: "Y" if value in {"Y", "YES", "TRUE", "T", "1"} else "N"
    )


def parse_dt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def completed_policy_cycles(start: pd.Series, as_of: pd.Series) -> pd.Series:
    start_dt = parse_dt(start)
    as_of_dt = parse_dt(as_of)
    days = (as_of_dt.dt.date - start_dt.dt.date).map(lambda value: value.days if pd.notna(value) else math.nan)
    return (days.fillna(-1).clip(lower=0) // 365).astype(int)


def band_counts(values: pd.Series, band_fn) -> Counter:
    counts = Counter()
    for value in values.dropna():
        counts[band_fn(value)] += 1
    return counts


def print_check(ok: bool, label: str, detail: str = "") -> int:
    status = "PASS" if ok else "FAIL"
    suffix = f" - {detail}" if detail else ""
    print(f"{status}: {label}{suffix}")
    return 0 if ok else 1


def require_bands(label: str, counts: Counter, expected: set[str], rows: int, min_rows: int = 200) -> int:
    if rows < min_rows:
        print(f"SKIP: {label} distribution rows={rows}, need>={min_rows}; counts={dict(counts)}")
        return 0
    missing = sorted(expected - set(counts))
    return print_check(not missing, f"{label} distribution", f"counts={dict(counts)} missing={missing}")


def validate_policy(policy: pd.DataFrame) -> int:
    errors = 0
    if policy.empty:
        return print_check(False, "sat_policy exists", "sat_policy.csv missing or empty")

    missing = REQUIRED_POLICY_COLS - set(policy.columns)
    errors += print_check(not missing, "sat_policy required churn columns", f"missing={sorted(missing)}")
    if missing:
        return errors

    current = pd.to_numeric(policy["renewal_amount_current_period"], errors="coerce")
    next_amt = pd.to_numeric(policy["renewal_amount_next_period"], errors="coerce")
    valid_premium = current.gt(0) & next_amt.ge(0)
    errors += print_check(valid_premium.all(), "renewal premium amounts are valid positive/current and nonnegative/next")

    pct = ((next_amt - current) / current) * 100
    pct_counts = band_counts(
        pct,
        lambda value: "<0%" if value < 0 else "0-5%" if value <= 5 else "5-10%" if value <= 10 else ">10%",
    )
    errors += require_bands("renewal premium percent", pct_counts, {"<0%", "0-5%", "5-10%", ">10%"}, len(policy))

    abs_increase = next_amt - current
    abs_counts = band_counts(
        abs_increase,
        lambda value: "<=0" if value <= 0 else "1-50" if value <= 50 else "51-100" if value <= 100 else ">100",
    )
    errors += require_bands("absolute premium increase", abs_counts, {"<=0", "1-50", "51-100", ">100"}, len(policy))

    current_counts = band_counts(
        current,
        lambda value: "LOW" if value <= 600 else "MEDIUM" if value <= 900 else "HIGH" if value <= 1200 else "VERY_HIGH",
    )
    errors += require_bands("current premium amount", current_counts, {"LOW", "MEDIUM", "HIGH", "VERY_HIGH"}, len(policy))

    active = pd.to_numeric(policy["number_of_active_claim"], errors="coerce").fillna(-1)
    previous = pd.to_numeric(policy["number_of_previous_claim"], errors="coerce").fillna(-1)
    declined = pd.to_numeric(policy["declined_claims"], errors="coerce").fillna(-1)
    claim_nonnegative_int = (
        active.ge(0) & previous.ge(0) & declined.ge(0)
        & active.mod(1).eq(0) & previous.mod(1).eq(0) & declined.mod(1).eq(0)
    )
    errors += print_check(claim_nonnegative_int.all(), "claim count fields are nonnegative integers")
    claim_total = active + previous + declined
    claim_counts = band_counts(claim_total, lambda value: "0" if value == 0 else "1" if value == 1 else "2" if value == 2 else "3+")
    errors += require_bands("claim count", claim_counts, {"0", "1", "2", "3+"}, len(policy))

    expected_cycle = completed_policy_cycles(policy["policy_start_date"], policy["load_date"])
    actual_cycle = pd.to_numeric(policy["policy_cycle"], errors="coerce").fillna(-1).astype(int)
    errors += print_check(actual_cycle.eq(expected_cycle).all(), "policy_cycle equals completed annual cycles")
    tenure_band = actual_cycle.map(lambda value: "<1" if value < 1 else "1-2" if value <= 2 else "3-5" if value <= 5 else ">5")
    tenure_counts = Counter(tenure_band)
    errors += require_bands("customer tenure", tenure_counts, {"<1", "1-2", "3-5", ">5"}, len(policy))

    cover_valid = policy["cover_option"].isin(COVER_OPTIONS)
    errors += print_check(cover_valid.all(), "policy cover_option add-on categories")
    cover_counts = Counter(policy["cover_option"].dropna().astype(str))
    errors += require_bands("policy add-ons", cover_counts, COVER_OPTIONS, len(policy))

    churn_flag = policy["policy_status"].fillna("").astype(str).str.upper().isin({"CANCELLED", "LAPSED"}).astype(int)
    tenure_summary = (
        pd.DataFrame({"tenure_band": tenure_band, "churn_flag": churn_flag})
        .groupby("tenure_band")["churn_flag"]
        .agg(["count", "mean"])
        .reindex(["<1", "1-2", "3-5", ">5"])
    )
    if tenure_summary["count"].ge(20).all():
        tenure_detail = {
            idx: {"count": int(row["count"]), "churn_pct": round(float(row["mean"]) * 100, 2)}
            for idx, row in tenure_summary.iterrows()
        }
        rates = tenure_summary["mean"]
        errors += print_check(
            rates.loc["<1"] > rates.loc["1-2"] > rates.loc["3-5"] > rates.loc[">5"],
            "policy_cycle churn decreases with completed tenure",
            f"rates={tenure_detail}",
        )
    else:
        print(f"SKIP: policy_cycle churn decreases with completed tenure - summary={tenure_summary.fillna(0).to_dict('index')}")

    channel_summary = (
        pd.DataFrame({"sales_channel": policy["sales_channel"], "churn_flag": churn_flag})
        .dropna(subset=["sales_channel"])
        .groupby("sales_channel")["churn_flag"]
        .agg(["count", "mean"])
    )
    if {"ONLINE", "BRANCH", "AGENT"}.issubset(set(channel_summary.index)):
        direct_rate = channel_summary.loc[["ONLINE", "BRANCH"], "mean"].mean()
        agent_rate = channel_summary.loc["AGENT", "mean"]
        rate_spread = channel_summary["mean"].max() - channel_summary["mean"].min()
        channel_detail = {
            idx: {"count": int(row["count"]), "churn_pct": round(float(row["mean"]) * 100, 2)}
            for idx, row in channel_summary.iterrows()
        }
        errors += print_check(
            agent_rate > direct_rate and rate_spread >= 0.10,
            "sales channel churn variance",
            f"rates={channel_detail}",
        )
    else:
        print(f"SKIP: sales channel churn variance - channels={list(channel_summary.index)}")
    return errors


def validate_marketing(marketing: pd.DataFrame) -> int:
    errors = 0
    if marketing.empty:
        print("SKIP: sat_marketing_preference missing or empty")
        return 0
    missing = REQUIRED_MARKETING_COLS - set(marketing.columns)
    errors += print_check(not missing, "sat_marketing_preference required proxy columns", f"missing={sorted(missing)}")
    if missing:
        return errors

    flags = {col: yn_series(marketing[col]) for col in REQUIRED_MARKETING_COLS}
    flag_valid = all(series.isin({"Y", "N"}).all() for series in flags.values())
    errors += print_check(flag_valid, "marketing proxy flags are Y/N")

    engagement_score = (
        flags["email_subscriptions"].eq("Y").astype(int)
        + flags["commercial_email"].eq("Y").astype(int)
        + flags["email"].eq("Y").astype(int)
        + flags["sms"].eq("Y").astype(int)
    )
    engagement_counts = band_counts(
        engagement_score,
        lambda value: "NONE" if value == 0 else "LOW" if value == 1 else "MEDIUM" if value in {2, 3} else "HIGH",
    )
    errors += require_bands("email/sms engagement proxy", engagement_counts, {"NONE", "LOW", "MEDIUM", "HIGH"}, len(marketing))
    call_counts = Counter(flags["call"])
    errors += require_bands("service call proxy", call_counts, {"Y", "N"}, len(marketing))
    return errors


def validate_person(person: pd.DataFrame) -> int:
    if person.empty:
        return print_check(False, "sat_natural_person exists", "sat_natural_person.csv missing or empty")
    if not {"birth_date", "load_date"}.issubset(person.columns):
        return print_check(False, "driver experience proxy columns", "birth_date/load_date missing")

    birth = parse_dt(person["birth_date"])
    load = parse_dt(person["load_date"])
    age_years = ((load - birth).dt.days / 365.25).dropna()
    experience_years = (age_years - 17).clip(lower=0)
    invalid = birth.isna() | load.isna() | birth.gt(load)
    errors = print_check(not invalid.any(), "birth_date is parseable and before load_date")
    counts = band_counts(
        experience_years,
        lambda value: "<2y" if value < 2 else "2-5y" if value <= 5 else "6-10y" if value <= 10 else ">10y",
    )
    errors += require_bands("driver experience proxy", counts, {"<2y", "2-5y", "6-10y", ">10y"}, len(person))
    return errors


def validate_motor(motor: pd.DataFrame) -> int:
    if motor.empty:
        print("SKIP: sat_motor missing or empty")
        return 0
    if "vehicle_model" not in motor.columns:
        return print_check(False, "vehicle model column exists")

    allowed = STANDARD_VEHICLES | PREMIUM_VEHICLES | HIGH_RISK_VEHICLES
    valid = motor["vehicle_model"].isin(allowed)
    errors = print_check(valid.all(), "vehicle_model values map to churn segments")
    counts = band_counts(
        motor["vehicle_model"],
        lambda value: "STANDARD" if value in STANDARD_VEHICLES else "PREMIUM" if value in PREMIUM_VEHICLES else "HIGH_RISK" if value in HIGH_RISK_VEHICLES else "UNKNOWN",
    )
    errors += require_bands("vehicle model segment", counts, {"STANDARD", "PREMIUM", "HIGH_RISK"}, len(motor))
    return errors


def default_base_path() -> Path:
    synthetic = latest_subdir(Path("data") / "synthetic" / "base")
    if synthetic:
        return synthetic
    output = latest_subdir(Path("data") / "output")
    if output:
        return output
    raise FileNotFoundError("No data/synthetic/base or data/output run folder found")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate churn KPI source fields in a generated base run.")
    parser.add_argument("--path", help="Run folder containing sat_policy.csv, sat_natural_person.csv, sat_marketing_preference.csv, and sat_motor.csv")
    args = parser.parse_args()

    base_path = Path(args.path) if args.path else default_base_path()
    print(f"Checking churn KPI fields in: {base_path.resolve()}")
    errors = 0
    errors += validate_policy(read_table(base_path, "sat_policy.csv"))
    errors += validate_marketing(read_table(base_path, "sat_marketing_preference.csv"))
    errors += validate_person(read_table(base_path, "sat_natural_person.csv"))
    errors += validate_motor(read_table(base_path, "sat_motor.csv"))

    if errors:
        print(f"CHURN KPI VALIDATION FAILED: {errors} failed checks")
        return 1
    print("CHURN KPI VALIDATION PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
