from __future__ import annotations

import argparse
import math
from collections import Counter
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
ADDON_CHURN_RANGES = {
    "BASE_ONLY": (0.25, 0.40),
    "ONE_ADD_ON": (0.18, 0.28),
    "TWO_ADD_ONS": (0.12, 0.22),
    "THREE_PLUS_ADD_ONS": (0.08, 0.18),
}
CLAIM_COUNT_CHURN_RANGES = {
    "0": (0.12, 0.18),
    "1": (0.20, 0.30),
    "2": (0.30, 0.45),
    "3+": (0.45, 0.60),
}
PREMIUM_ABS_CHURN_RANGES = {
    "<=0": (0.08, 0.12),
    "1-50": (0.15, 0.22),
    "51-100": (0.25, 0.38),
    ">100": (0.45, 0.65),
}
PREMIUM_PCT_CHURN_RANGES = {
    "<0%": (0.08, 0.12),
    "0-5%": (0.15, 0.20),
    "5-10%": (0.25, 0.35),
    ">10%": (0.45, 0.65),
}
CURRENT_PREMIUM_CHURN_RANGES = {
    "LOW": (0.10, 0.18),
    "MEDIUM": (0.15, 0.25),
    "HIGH": (0.25, 0.40),
    "VERY_HIGH": (0.40, 0.55),
}
TENURE_CHURN_RANGES = {
    "<1": (0.35, 0.50),
    "1-2": (0.25, 0.35),
    "3-5": (0.15, 0.25),
    ">5": (0.08, 0.15),
}
MARKETING_ENGAGEMENT_CHURN_RANGES = {
    "HIGH": (0.08, 0.15),
    "MEDIUM": (0.18, 0.30),
    "LOW": (0.35, 0.55),
    "NONE": (0.50, 0.70),
}
VEHICLE_SEGMENT_CHURN_RANGES = {
    "STANDARD": (0.12, 0.22),
    "PREMIUM": (0.20, 0.35),
    "HIGH_RISK": (0.30, 0.50),
}
DRIVER_EXPERIENCE_CHURN_RANGES = {
    "<2y": (0.25, 0.40),
    "2-5y": (0.18, 0.30),
    "6-10y": (0.15, 0.25),
    ">10y": (0.10, 0.18),
}


def vehicle_segment_from_model(value: str) -> str:
    if value in STANDARD_VEHICLES:
        return "STANDARD"
    if value in PREMIUM_VEHICLES:
        return "PREMIUM"
    if value in HIGH_RISK_VEHICLES:
        return "HIGH_RISK"
    return "UNKNOWN"


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
    claim_band = claim_total.map(lambda value: "0" if value == 0 else "1" if value == 1 else "2" if value == 2 else "3+")
    claim_counts = Counter(claim_band)
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
    current_premium_band = current.map(
        lambda value: "LOW" if value <= 600 else "MEDIUM" if value <= 900 else "HIGH" if value <= 1200 else "VERY_HIGH"
    )
    current_premium_summary = (
        pd.DataFrame({"current_premium_band": current_premium_band, "churn_flag": churn_flag})
        .groupby("current_premium_band")["churn_flag"]
        .agg(["count", "mean"])
        .reindex(["LOW", "MEDIUM", "HIGH", "VERY_HIGH"])
    )
    if current_premium_summary["count"].ge(20).all():
        current_premium_detail = {
            idx: {"count": int(row["count"]), "churn_pct": round(float(row["mean"]) * 100, 2)}
            for idx, row in current_premium_summary.iterrows()
        }
        rates = current_premium_summary["mean"]
        ranges_ok = all(
            CURRENT_PREMIUM_CHURN_RANGES[idx][0] <= float(rates.loc[idx]) <= CURRENT_PREMIUM_CHURN_RANGES[idx][1]
            for idx in CURRENT_PREMIUM_CHURN_RANGES
        )
        direction_ok = rates.loc["LOW"] < rates.loc["MEDIUM"] < rates.loc["HIGH"] < rates.loc["VERY_HIGH"]
        errors += print_check(
            ranges_ok and direction_ok,
            "current premium amount churn increases with higher premium",
            f"rates={current_premium_detail}",
        )
    else:
        print(f"SKIP: current premium amount churn increases with higher premium - summary={current_premium_summary.fillna(0).to_dict('index')}")

    premium_pct_band = pct.map(
        lambda value: "<0%" if value < 0 else "0-5%" if value <= 5 else "5-10%" if value <= 10 else ">10%"
    )
    premium_pct_summary = (
        pd.DataFrame({"premium_pct_band": premium_pct_band, "churn_flag": churn_flag})
        .groupby("premium_pct_band")["churn_flag"]
        .agg(["count", "mean"])
        .reindex(["<0%", "0-5%", "5-10%", ">10%"])
    )
    if premium_pct_summary["count"].ge(20).all():
        premium_pct_detail = {
            idx: {"count": int(row["count"]), "churn_pct": round(float(row["mean"]) * 100, 2)}
            for idx, row in premium_pct_summary.iterrows()
        }
        rates = premium_pct_summary["mean"]
        ranges_ok = all(
            PREMIUM_PCT_CHURN_RANGES[idx][0] <= float(rates.loc[idx]) <= PREMIUM_PCT_CHURN_RANGES[idx][1]
            for idx in PREMIUM_PCT_CHURN_RANGES
        )
        direction_ok = rates.loc["<0%"] < rates.loc["0-5%"] < rates.loc["5-10%"] < rates.loc[">10%"]
        errors += print_check(
            ranges_ok and direction_ok,
            "premium percent increase churn increases with higher percent increase",
            f"rates={premium_pct_detail}",
        )
    else:
        print(f"SKIP: premium percent increase churn increases with higher percent increase - summary={premium_pct_summary.fillna(0).to_dict('index')}")

    premium_abs_band = abs_increase.map(
        lambda value: "<=0" if value <= 0 else "1-50" if value <= 50 else "51-100" if value <= 100 else ">100"
    )
    premium_abs_summary = (
        pd.DataFrame({"premium_abs_band": premium_abs_band, "churn_flag": churn_flag})
        .groupby("premium_abs_band")["churn_flag"]
        .agg(["count", "mean"])
        .reindex(["<=0", "1-50", "51-100", ">100"])
    )
    if premium_abs_summary["count"].ge(20).all():
        premium_abs_detail = {
            idx: {"count": int(row["count"]), "churn_pct": round(float(row["mean"]) * 100, 2)}
            for idx, row in premium_abs_summary.iterrows()
        }
        rates = premium_abs_summary["mean"]
        ranges_ok = all(
            PREMIUM_ABS_CHURN_RANGES[idx][0] <= float(rates.loc[idx]) <= PREMIUM_ABS_CHURN_RANGES[idx][1]
            for idx in PREMIUM_ABS_CHURN_RANGES
        )
        direction_ok = rates.loc["<=0"] < rates.loc["1-50"] < rates.loc["51-100"] < rates.loc[">100"]
        errors += print_check(
            ranges_ok and direction_ok,
            "absolute premium increase churn increases with higher increase",
            f"rates={premium_abs_detail}",
        )
    else:
        print(f"SKIP: absolute premium increase churn increases with higher increase - summary={premium_abs_summary.fillna(0).to_dict('index')}")

    claim_summary = (
        pd.DataFrame({"claim_band": claim_band, "churn_flag": churn_flag})
        .groupby("claim_band")["churn_flag"]
        .agg(["count", "mean"])
        .reindex(["0", "1", "2", "3+"])
    )
    if claim_summary["count"].ge(20).all():
        claim_detail = {
            idx: {"count": int(row["count"]), "churn_pct": round(float(row["mean"]) * 100, 2)}
            for idx, row in claim_summary.iterrows()
        }
        rates = claim_summary["mean"]
        ranges_ok = all(
            CLAIM_COUNT_CHURN_RANGES[idx][0] <= float(rates.loc[idx]) <= CLAIM_COUNT_CHURN_RANGES[idx][1]
            for idx in CLAIM_COUNT_CHURN_RANGES
        )
        direction_ok = rates.loc["0"] < rates.loc["1"] < rates.loc["2"] < rates.loc["3+"]
        errors += print_check(
            ranges_ok and direction_ok,
            "claim count churn increases with more claims",
            f"rates={claim_detail}",
        )
    else:
        print(f"SKIP: claim count churn increases with more claims - summary={claim_summary.fillna(0).to_dict('index')}")

    addon_summary = (
        pd.DataFrame({"cover_option": policy["cover_option"], "churn_flag": churn_flag})
        .groupby("cover_option")["churn_flag"]
        .agg(["count", "mean"])
        .reindex(["BASE_ONLY", "ONE_ADD_ON", "TWO_ADD_ONS", "THREE_PLUS_ADD_ONS"])
    )
    if addon_summary["count"].ge(20).all():
        addon_detail = {
            idx: {"count": int(row["count"]), "churn_pct": round(float(row["mean"]) * 100, 2)}
            for idx, row in addon_summary.iterrows()
        }
        rates = addon_summary["mean"]
        ranges_ok = all(
            ADDON_CHURN_RANGES[idx][0] <= float(rates.loc[idx]) <= ADDON_CHURN_RANGES[idx][1]
            for idx in ADDON_CHURN_RANGES
        )
        direction_ok = (
            rates.loc["BASE_ONLY"]
            > rates.loc["ONE_ADD_ON"]
            > rates.loc["TWO_ADD_ONS"]
            > rates.loc["THREE_PLUS_ADD_ONS"]
        )
        errors += print_check(
            ranges_ok and direction_ok,
            "policy add-on churn decreases with more add-ons",
            f"rates={addon_detail}",
        )
    else:
        print(f"SKIP: policy add-on churn decreases with more add-ons - summary={addon_summary.fillna(0).to_dict('index')}")

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
        ranges_ok = all(
            TENURE_CHURN_RANGES[idx][0] <= float(rates.loc[idx]) <= TENURE_CHURN_RANGES[idx][1]
            for idx in TENURE_CHURN_RANGES
        )
        direction_ok = rates.loc["<1"] > rates.loc["1-2"] > rates.loc["3-5"] > rates.loc[">5"]
        errors += print_check(
            ranges_ok and direction_ok,
            "policy_cycle churn decreases with completed tenure and matches workbook ranges",
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


def marketing_engagement_band(marketing: pd.DataFrame) -> pd.Series:
    flags = {col: yn_series(marketing[col]) for col in REQUIRED_MARKETING_COLS}
    engagement_score = (
        flags["email_subscriptions"].eq("Y").astype(int)
        + flags["commercial_email"].eq("Y").astype(int)
        + flags["email"].eq("Y").astype(int)
        + flags["sms"].eq("Y").astype(int)
    )
    return engagement_score.map(
        lambda value: "NONE" if value == 0 else "LOW" if value == 1 else "MEDIUM" if value in {2, 3} else "HIGH"
    )


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

    engagement_band = marketing_engagement_band(marketing)
    engagement_counts = band_counts(
        engagement_band,
        lambda value: value,
    )
    errors += require_bands("email/sms engagement proxy", engagement_counts, {"NONE", "LOW", "MEDIUM", "HIGH"}, len(marketing))
    call_counts = Counter(flags["call"])
    errors += require_bands("service call proxy", call_counts, {"Y", "N"}, len(marketing))
    return errors


def validate_marketing_churn(
    policy: pd.DataFrame,
    marketing: pd.DataFrame,
    link_policy_customer: pd.DataFrame,
    link_customer_person: pd.DataFrame,
    link_person_marketing_preference: pd.DataFrame,
) -> int:
    if any(df.empty for df in (policy, marketing, link_policy_customer, link_customer_person, link_person_marketing_preference)):
        print("SKIP: marketing engagement churn rate - required policy/person/marketing links missing")
        return 0
    required_marketing = REQUIRED_MARKETING_COLS | {"marketing_preference_hash_key"}
    required_links = [
        (link_policy_customer, {"policy_hash_key", "customer_hash_key"}),
        (link_customer_person, {"customer_hash_key", "person_hash_key"}),
        (link_person_marketing_preference, {"person_hash_key", "marketing_preference_hash_key"}),
    ]
    if not required_marketing.issubset(marketing.columns) or any(not cols.issubset(df.columns) for df, cols in required_links):
        print("SKIP: marketing engagement churn rate - required columns missing")
        return 0

    marketing = marketing.copy()
    marketing["marketing_engagement_band"] = marketing_engagement_band(marketing)
    policy_marketing = (
        policy[["policy_hash_key", "policy_status"]]
        .merge(link_policy_customer[["policy_hash_key", "customer_hash_key"]], on="policy_hash_key", how="left")
        .merge(link_customer_person[["customer_hash_key", "person_hash_key"]], on="customer_hash_key", how="left")
        .merge(link_person_marketing_preference[["person_hash_key", "marketing_preference_hash_key"]], on="person_hash_key", how="left")
        .merge(marketing[["marketing_preference_hash_key", "marketing_engagement_band"]], on="marketing_preference_hash_key", how="left")
        .dropna(subset=["marketing_engagement_band"])
    )
    if policy_marketing.empty:
        print("SKIP: marketing engagement churn rate - no linked policies")
        return 0
    policy_marketing["churn_flag"] = (
        policy_marketing["policy_status"].fillna("").astype(str).str.upper().isin({"CANCELLED", "LAPSED"}).astype(int)
    )
    summary = (
        policy_marketing.groupby("marketing_engagement_band")["churn_flag"]
        .agg(["count", "mean"])
        .reindex(["HIGH", "MEDIUM", "LOW", "NONE"])
    )
    if summary["count"].ge(20).all():
        detail = {
            idx: {"count": int(row["count"]), "churn_pct": round(float(row["mean"]) * 100, 2)}
            for idx, row in summary.iterrows()
        }
        rates = summary["mean"]
        ranges_ok = all(
            MARKETING_ENGAGEMENT_CHURN_RANGES[idx][0] <= float(rates.loc[idx]) <= MARKETING_ENGAGEMENT_CHURN_RANGES[idx][1]
            for idx in MARKETING_ENGAGEMENT_CHURN_RANGES
        )
        direction_ok = rates.loc["HIGH"] < rates.loc["MEDIUM"] < rates.loc["LOW"] < rates.loc["NONE"]
        return print_check(
            ranges_ok and direction_ok,
            "marketing engagement churn increases as engagement decreases",
            f"rates={detail}",
        )
    print(f"SKIP: marketing engagement churn rate - summary={summary.fillna(0).to_dict('index')}")
    return 0


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


def driver_experience_band_from_person(person: pd.DataFrame) -> pd.Series:
    birth = parse_dt(person["birth_date"])
    load = parse_dt(person["load_date"])
    age_years = ((load - birth).dt.days / 365.25).clip(lower=0)
    experience_years = (age_years - 17).clip(lower=0)
    return experience_years.map(
        lambda value: "<2y" if value < 2 else "2-5y" if value <= 5 else "6-10y" if value <= 10 else ">10y"
    )


def validate_driver_experience_churn(
    policy: pd.DataFrame,
    person: pd.DataFrame,
    link_policy_customer: pd.DataFrame,
    link_customer_person: pd.DataFrame,
    link_person_natural_person: pd.DataFrame,
) -> int:
    if any(df.empty for df in (policy, person, link_policy_customer, link_customer_person, link_person_natural_person)):
        print("SKIP: driver experience churn rate - required policy/person links missing")
        return 0
    required = [
        (policy, {"policy_hash_key", "policy_status"}),
        (person, {"natural_person_hash_key", "birth_date", "load_date"}),
        (link_policy_customer, {"policy_hash_key", "customer_hash_key"}),
        (link_customer_person, {"customer_hash_key", "person_hash_key"}),
        (link_person_natural_person, {"person_hash_key", "natural_person_hash_key"}),
    ]
    if any(not cols.issubset(df.columns) for df, cols in required):
        print("SKIP: driver experience churn rate - required columns missing")
        return 0
    natural = person[["natural_person_hash_key", "birth_date", "load_date"]].copy()
    natural["driver_experience_band"] = driver_experience_band_from_person(natural)
    policy_driver = (
        policy[["policy_hash_key", "policy_status"]]
        .merge(link_policy_customer[["policy_hash_key", "customer_hash_key"]], on="policy_hash_key", how="left")
        .merge(link_customer_person[["customer_hash_key", "person_hash_key"]], on="customer_hash_key", how="left")
        .merge(link_person_natural_person[["person_hash_key", "natural_person_hash_key"]], on="person_hash_key", how="left")
        .merge(natural[["natural_person_hash_key", "driver_experience_band"]], on="natural_person_hash_key", how="left")
        .dropna(subset=["driver_experience_band"])
    )
    if policy_driver.empty:
        print("SKIP: driver experience churn rate - no linked natural-person policies")
        return 0
    policy_driver["churn_flag"] = (
        policy_driver["policy_status"].fillna("").astype(str).str.upper().isin({"CANCELLED", "LAPSED"}).astype(int)
    )
    summary = (
        policy_driver.groupby("driver_experience_band")["churn_flag"]
        .agg(["count", "mean"])
        .reindex(["<2y", "2-5y", "6-10y", ">10y"])
    )
    if summary["count"].ge(20).all():
        detail = {
            idx: {"count": int(row["count"]), "churn_pct": round(float(row["mean"]) * 100, 2)}
            for idx, row in summary.iterrows()
        }
        rates = summary["mean"]
        ranges_ok = all(
            DRIVER_EXPERIENCE_CHURN_RANGES[idx][0] <= float(rates.loc[idx]) <= DRIVER_EXPERIENCE_CHURN_RANGES[idx][1]
            for idx in DRIVER_EXPERIENCE_CHURN_RANGES
        )
        direction_ok = rates.loc["<2y"] > rates.loc["2-5y"] > rates.loc["6-10y"] > rates.loc[">10y"]
        return print_check(
            ranges_ok and direction_ok,
            "driver experience churn decreases with more experience",
            f"rates={detail}",
        )
    print(f"SKIP: driver experience churn rate - summary={summary.fillna(0).to_dict('index')}")
    return 0


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
        vehicle_segment_from_model,
    )
    errors += require_bands("vehicle model segment", counts, {"STANDARD", "PREMIUM", "HIGH_RISK"}, len(motor))
    return errors


def validate_vehicle_segment_churn(
    policy: pd.DataFrame,
    motor: pd.DataFrame,
    link_policy_insured_object: pd.DataFrame,
    link_insured_object_motor: pd.DataFrame,
) -> int:
    if any(df.empty for df in (policy, motor, link_policy_insured_object, link_insured_object_motor)):
        print("SKIP: vehicle segment churn rate - direct policy/motor links missing")
        return 0
    required = [
        (policy, {"policy_hash_key", "policy_status"}),
        (motor, {"motor_hash_key", "vehicle_model"}),
        (link_policy_insured_object, {"policy_hash_key", "insured_object_hash_key"}),
        (link_insured_object_motor, {"insured_object_hash_key", "motor_hash_key"}),
    ]
    if any(not cols.issubset(df.columns) for df, cols in required):
        print("SKIP: vehicle segment churn rate - required columns missing")
        return 0
    policy_motor = (
        policy[["policy_hash_key", "policy_status"]]
        .merge(link_policy_insured_object[["policy_hash_key", "insured_object_hash_key"]], on="policy_hash_key", how="inner")
        .merge(link_insured_object_motor[["insured_object_hash_key", "motor_hash_key"]], on="insured_object_hash_key", how="inner")
        .merge(motor[["motor_hash_key", "vehicle_model"]], on="motor_hash_key", how="inner")
    )
    if policy_motor.empty:
        print("SKIP: vehicle segment churn rate - no linked motor policies")
        return 0
    policy_motor["vehicle_segment"] = policy_motor["vehicle_model"].map(vehicle_segment_from_model)
    policy_motor["churn_flag"] = (
        policy_motor["policy_status"].fillna("").astype(str).str.upper().isin({"CANCELLED", "LAPSED"}).astype(int)
    )
    summary = (
        policy_motor.groupby("vehicle_segment")["churn_flag"]
        .agg(["count", "mean"])
        .reindex(["STANDARD", "PREMIUM", "HIGH_RISK"])
    )
    if summary["count"].ge(20).all():
        detail = {
            idx: {"count": int(row["count"]), "churn_pct": round(float(row["mean"]) * 100, 2)}
            for idx, row in summary.iterrows()
        }
        rates = summary["mean"]
        ranges_ok = all(
            VEHICLE_SEGMENT_CHURN_RANGES[idx][0] <= float(rates.loc[idx]) <= VEHICLE_SEGMENT_CHURN_RANGES[idx][1]
            for idx in VEHICLE_SEGMENT_CHURN_RANGES
        )
        direction_ok = rates.loc["STANDARD"] < rates.loc["PREMIUM"] < rates.loc["HIGH_RISK"]
        return print_check(
            ranges_ok and direction_ok,
            "vehicle segment churn increases with higher vehicle risk",
            f"rates={detail}",
        )
    print(f"SKIP: vehicle segment churn rate - summary={summary.fillna(0).to_dict('index')}")
    return 0


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
    policy = read_table(base_path, "sat_policy.csv")
    marketing = read_table(base_path, "sat_marketing_preference.csv")
    errors += validate_policy(policy)
    errors += validate_marketing(marketing)
    errors += validate_marketing_churn(
        policy,
        marketing,
        read_table(base_path, "link_policy_customer.csv"),
        read_table(base_path, "link_customer_person.csv"),
        read_table(base_path, "link_person_marketing_preference.csv"),
    )
    natural_person = read_table(base_path, "sat_natural_person.csv")
    errors += validate_person(natural_person)
    errors += validate_driver_experience_churn(
        policy,
        natural_person,
        read_table(base_path, "link_policy_customer.csv"),
        read_table(base_path, "link_customer_person.csv"),
        read_table(base_path, "link_person_natural_person.csv"),
    )
    motor = read_table(base_path, "sat_motor.csv")
    errors += validate_motor(motor)
    errors += validate_vehicle_segment_churn(
        policy,
        motor,
        read_table(base_path, "link_policy_insured_object.csv"),
        read_table(base_path, "link_insured_object_motor.csv"),
    )

    if errors:
        print(f"CHURN KPI VALIDATION FAILED: {errors} failed checks")
        return 1
    print("CHURN KPI VALIDATION PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
