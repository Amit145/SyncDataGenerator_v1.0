from __future__ import annotations

import argparse
import os
import sys

import pandas as pd
from pandas.errors import EmptyDataError

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.storage_paths import SYNTHETIC_ENHANCED_ROOT
from helper.enhanced_ddl import parse_enhanced_ddl
from helper.enhanced_rules import (
    BASE_POLICY_CHANNELS,
    PRODUCT_CODES,
    claim_product_for_code,
    insurance_category_for_code,
)
from enums.sat_enums import SAT_ENUMS

RECOVERY_SENTINEL_TIMESTAMP = pd.Timestamp("1900-01-01T00:00:00")


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


def parse_dt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def parse_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def count_invalid_enum(df: pd.DataFrame, column: str, allowed_values) -> tuple[int, list[str]]:
    if column not in df.columns or not allowed_values:
        return 0, []
    allowed_normalized = {str(item).strip().upper() for item in allowed_values}
    normalized = df[column].fillna("").astype(str).str.strip()
    compare_values = normalized.str.upper()
    mask = (normalized != "") & (~compare_values.isin(allowed_normalized))
    invalid_values = sorted(normalized[mask].unique().tolist())
    return int(mask.sum()), invalid_values[:5]


def build_link_map(df: pd.DataFrame, left_col: str, right_col: str) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    if left_col not in df.columns or right_col not in df.columns:
        return mapping
    for left, right in df[[left_col, right_col]].itertuples(index=False, name=None):
        if pd.isna(left) or pd.isna(right):
            continue
        mapping.setdefault(str(left), []).append(str(right))
    return mapping


def verify_timestamp_columns(frames: dict[str, pd.DataFrame], ddl: dict) -> int:
    errors = 0
    timestamp_columns: dict[str, list[str]] = {}
    for table_name, column_defs in ddl.get("column_types", {}).items():
        cols = [
            column_name
            for column_name, column_type in column_defs.items()
            if str(column_type).upper() == "TIMESTAMP"
        ]
        if cols:
            timestamp_columns[table_name] = cols

    for table_name, columns in timestamp_columns.items():
        df = frames.get(table_name)
        if df is None or df.empty:
            continue
        for column_name in columns:
            if column_name not in df.columns:
                continue
            values = df[column_name].fillna("").astype(str).str.strip()
            populated = values[values != ""]
            if populated.empty:
                continue
            parsed = pd.to_datetime(populated, errors="coerce")
            invalid_count = int(parsed.isna().sum())
            missing_time_count = int((~populated.str.contains(r"[T ]\d{2}:\d{2}:\d{2}", regex=True)).sum())
            if invalid_count or missing_time_count:
                print(
                    f"TIMESTAMP CHECK FAILED: {table_name}.{column_name} "
                    f"invalid_rows={invalid_count} missing_time_rows={missing_time_count}"
                )
                errors += 1
    return errors


def verify_numeric_columns(frames: dict[str, pd.DataFrame], ddl: dict) -> int:
    errors = 0
    numeric_types = {"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "DOUBLE", "FLOAT", "DECIMAL", "NUMERIC"}
    integer_types = {"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT"}
    for table_name, column_defs in ddl.get("column_types", {}).items():
        df = frames.get(table_name)
        if df is None or df.empty:
            continue
        for column_name, column_type in column_defs.items():
            if column_type not in numeric_types or column_name not in df.columns:
                continue
            values = df[column_name].fillna("").astype(str).str.strip()
            blank_count = int(values.eq("").sum())
            numeric_values = pd.to_numeric(values, errors="coerce")
            invalid_count = int(numeric_values.isna().sum())
            fractional_count = 0
            if column_type in integer_types:
                fractional_count = int((numeric_values.dropna() % 1 != 0).sum())
            if blank_count or invalid_count or fractional_count:
                print(
                    f"NUMERIC CHECK FAILED: {table_name}.{column_name} type={column_type} "
                    f"blank_rows={blank_count} invalid_rows={invalid_count} fractional_rows={fractional_count}"
                )
                errors += 1
    return errors


def verify_base_enum_alignment(frames: dict[str, pd.DataFrame]) -> int:
    errors = 0
    controlled_columns = {
        ("sat_account", "account_type"),
        ("sat_account", "account_creation_type"),
        ("sat_account", "account_status"),
        ("sat_consent", "opt_in_validated"),
        ("sat_consent", "opt_in_legitimate_interest"),
        ("sat_customer", "customer_status"),
        ("sat_customer", "customer_status_reason"),
        ("sat_customer", "customer_segment"),
        ("sat_customer", "line_of_business"),
        ("sat_lead", "interested_level"),
        ("sat_lead", "preferred_contact_method"),
        ("sat_lead", "person_status"),
        ("sat_legal_person", "person_status"),
        ("sat_legal_person", "source_id"),
        ("sat_legal_person", "source_type"),
        ("sat_person", "tenant_id"),
        ("sat_person", "is_lead"),
        ("sat_person", "operational_paperless_consent"),
        ("sat_person", "source_id"),
        ("sat_person", "source_type"),
        ("sat_natural_person", "assesed_disability_degree"),
        ("sat_natural_person", "preferred_language"),
        ("sat_natural_person", "role"),
        ("sat_natural_person", "occupation"),
        ("sat_marketing_preference", "sms"),
        ("sat_marketing_preference", "email"),
        ("sat_marketing_preference", "email_subscriptions"),
        ("sat_marketing_preference", "call"),
        ("sat_marketing_preference", "any"),
        ("sat_marketing_preference", "commercial_email"),
        ("sat_marketing_preference", "postal_mail"),
        ("sat_marketing_engagement", "promotion_code"),
        ("sat_marketing_engagement", "opened_email"),
        ("sat_marketing_engagement", "marketing_status"),
        ("sat_quote", "quote_status"),
        ("sat_policy", "cover_option"),
        ("sat_policy", "fraud_flag"),
        ("sat_policy", "policy_status"),
        ("sat_policy", "sales_channel"),
        ("sat_home", "wall_construction"),
        ("sat_home", "roof_construction"),
        ("sat_home", "home_type"),
        ("sat_home", "is_existing_home_customer"),
        ("sat_motor", "auto_decline_vehicle"),
        ("sat_motor", "body_type"),
        ("sat_motor", "fuel_type"),
        ("sat_motor", "license_status"),
        ("sat_motor", "is_existing_motor_customer"),
        ("sat_motor", "risk_class_code"),
        ("sat_motor", "variant"),
        ("sat_motor", "vehicle_owner_type"),
        ("sat_motor", "vehicle_class"),
        ("sat_motor", "vehicle_model"),
        ("sat_motor", "vehicle_type"),
    }
    for table_name, enum_columns in SAT_ENUMS.items():
        df = frames.get(table_name)
        if df is None or df.empty:
            continue
        for column_name, allowed_values in enum_columns.items():
            if (table_name, column_name) not in controlled_columns:
                continue
            invalid_count, sample_values = count_invalid_enum(df, column_name, allowed_values)
            if invalid_count:
                print(
                    f"ENUM CHECK FAILED: {table_name}.{column_name} invalid_rows={invalid_count} "
                    f"sample={sample_values}"
                )
                errors += 1
    sat_product = frames.get("sat_product")
    if sat_product is not None and not sat_product.empty and "type" in sat_product.columns:
        invalid_count, sample_values = count_invalid_enum(sat_product, "type", PRODUCT_CODES)
        if invalid_count:
            print(
                f"ENUM CHECK FAILED: sat_product.type invalid_rows={invalid_count} sample={sample_values}"
            )
            errors += 1
    sat_customer = frames.get("sat_customer")
    if sat_customer is not None and not sat_customer.empty and "customer_satisfaction" in sat_customer.columns:
        invalid_count, sample_values = count_invalid_enum(
            sat_customer,
            "customer_satisfaction",
            {"VERY_SATISFIED", "SATISFIED", "NEUTRAL", "DISSATISFIED", "UNKNOWN"},
        )
        if invalid_count:
            print(
                f"ENUM CHECK FAILED: sat_customer.customer_satisfaction "
                f"invalid_rows={invalid_count} sample={sample_values}"
            )
            errors += 1
    for table_name, column_name in (
        ("sat_channel", "channel_name"),
        ("sat_claim", "claim_channel"),
        ("sat_complaint", "complaint_channel"),
        ("sat_policy", "sales_channel"),
    ):
        df = frames.get(table_name)
        if df is None or df.empty:
            continue
        invalid_count, sample_values = count_invalid_enum(df, column_name, BASE_POLICY_CHANNELS)
        if invalid_count:
            print(
                f"ENUM CHECK FAILED: {table_name}.{column_name} invalid_rows={invalid_count} "
                f"sample={sample_values}"
                )
            errors += 1
    string_boolean_columns = {
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
    for table_name, columns in string_boolean_columns.items():
        df = frames.get(table_name)
        if df is None or df.empty:
            continue
        for column_name in columns:
            if column_name not in df.columns:
                continue
            invalid_count, sample_values = count_invalid_enum(df, column_name, {"Y", "N"})
            if invalid_count:
                print(
                    f"ENUM CHECK FAILED: {table_name}.{column_name} invalid_rows={invalid_count} "
                    f"sample={sample_values}"
                )
                errors += 1
    return errors


def verify_base_timelines(frames: dict[str, pd.DataFrame]) -> int:
    errors = 0
    sat_policy = frames.get("sat_policy")
    if sat_policy is not None and not sat_policy.empty:
        required = ["policy_start_date", "policy_end_date", "renewal_date"]
        if all(col in sat_policy.columns for col in required):
            start = parse_dt(sat_policy["policy_start_date"])
            end = parse_dt(sat_policy["policy_end_date"])
            renewal = parse_dt(sat_policy["renewal_date"])
            if ((~start.isna()) & (~end.isna()) & (start > end)).any():
                print("TIMELINE CHECK FAILED: sat_policy policy_start_date > policy_end_date")
                errors += 1
            if ((~renewal.isna()) & (~end.isna()) & (renewal > end)).any():
                print("TIMELINE CHECK FAILED: sat_policy renewal_date > policy_end_date")
                errors += 1
        if {"policy_start_date", "policy_issue_date"}.issubset(sat_policy.columns):
            start_date = parse_dt(sat_policy["policy_start_date"])
            issue_date = parse_dt(sat_policy["policy_issue_date"])
            mismatch = start_date.notna() & issue_date.notna() & (start_date != issue_date)
            if mismatch.any():
                print("TIMELINE CHECK FAILED: sat_policy policy_issue_date != policy_start_date")
                errors += 1

    sat_lead = frames.get("sat_lead")
    link_person_lead = frames.get("link_person_lead")
    link_customer_person = frames.get("link_customer_person")
    link_policy_customer = frames.get("link_policy_customer")
    if all(df is not None for df in (sat_lead, sat_policy, link_person_lead, link_customer_person, link_policy_customer)):
        lead_dates = sat_lead[["lead_hash_key", "converted_date"]].copy()
        lead_dates["converted_date"] = parse_dt(lead_dates["converted_date"])
        policy_dates = sat_policy[["policy_hash_key", "policy_start_date"]].copy()
        policy_dates["policy_start_date"] = parse_dt(policy_dates["policy_start_date"])
        person_lead_dates = link_person_lead.merge(lead_dates, on="lead_hash_key", how="left")
        latest_lead_by_person = person_lead_dates.groupby("person_hash_key", as_index=False)["converted_date"].max()
        policy_person = (
            link_policy_customer.merge(link_customer_person, on="customer_hash_key", how="left")
            .merge(policy_dates, on="policy_hash_key", how="left")
            .merge(latest_lead_by_person, on="person_hash_key", how="left")
        )
        valid = (~policy_person["policy_start_date"].isna()) & (~policy_person["converted_date"].isna())
        if (policy_person.loc[valid, "converted_date"] >= policy_person.loc[valid, "policy_start_date"]).any():
            print("TIMELINE CHECK FAILED: sat_lead converted_date >= linked policy_start_date")
            errors += 1
    return errors


def verify_enhanced_business_rules(frames: dict[str, pd.DataFrame]) -> int:
    errors = 0
    sat_policy = frames.get("sat_policy", pd.DataFrame())
    sat_claim = frames.get("sat_claim", pd.DataFrame())
    sat_complaint = frames.get("sat_complaint", pd.DataFrame())
    sat_override = frames.get("sat_override", pd.DataFrame())
    sat_regulation = frames.get("sat_regulation", pd.DataFrame())
    sat_channel = frames.get("sat_channel", pd.DataFrame())
    sat_customer = frames.get("sat_customer", pd.DataFrame())
    sat_product = frames.get("sat_product", pd.DataFrame())
    link_claim_policy = frames.get("link_claim_policy", pd.DataFrame())
    link_complaint_policy = frames.get("link_complaint_policy", pd.DataFrame())
    link_complaint_regulation = frames.get("link_complaint_regulation", pd.DataFrame())
    link_policy_override = frames.get("link_policy_override", pd.DataFrame())
    link_policy_channel = frames.get("link_policy_channel", pd.DataFrame())
    link_policy_product = frames.get("link_policy_product", pd.DataFrame())
    link_policy_insured_object = frames.get("link_policy_insured_object", pd.DataFrame())
    link_insured_object_motor = frames.get("link_insured_object_motor", pd.DataFrame())
    link_policy_customer = frames.get("link_policy_customer", pd.DataFrame())
    link_customer_person = frames.get("link_customer_person", pd.DataFrame())
    link_broker_person = frames.get("link_broker_person", pd.DataFrame())
    sat_motor = frames.get("sat_motor", pd.DataFrame())

    policy_cols = {"policy_hash_key", "policy_status", "policy_start_date", "policy_end_date", "sales_channel", "override_commission"}
    if not sat_policy.empty and policy_cols.issubset(set(sat_policy.columns)):
        policy_lookup = sat_policy.set_index("policy_hash_key")
    else:
        policy_lookup = pd.DataFrame().set_index(pd.Index([]))

    product_lookup = sat_product.set_index("product_hash_key") if not sat_product.empty and "product_hash_key" in sat_product.columns else pd.DataFrame()
    channel_lookup = sat_channel.set_index("channel_hash_key") if not sat_channel.empty and "channel_hash_key" in sat_channel.columns else pd.DataFrame()
    customer_lookup = sat_customer.set_index("customer_hash_key") if not sat_customer.empty and "customer_hash_key" in sat_customer.columns else pd.DataFrame()

    if not link_policy_channel.empty and not channel_lookup.empty and not policy_lookup.empty:
        merged = (
            link_policy_channel[["policy_hash_key", "channel_hash_key"]]
            .merge(sat_policy[["policy_hash_key", "sales_channel"]], on="policy_hash_key", how="left")
            .merge(sat_channel[["channel_hash_key", "channel_name"]], on="channel_hash_key", how="left")
        )
        if merged["policy_hash_key"].duplicated().any():
            print("CHANNEL CHECK FAILED: link_policy_channel has multiple rows for a policy")
            errors += 1
        mismatch = merged[
            merged["sales_channel"].fillna("").astype(str).str.strip()
            != merged["channel_name"].fillna("").astype(str).str.strip()
        ]
        if not mismatch.empty:
            print(f"CHANNEL CHECK FAILED: policy-channel mapping mismatch rows={len(mismatch)}")
            errors += 1

    if not sat_policy.empty and not link_policy_customer.empty and not link_customer_person.empty:
        agent_policies = sat_policy[
            sat_policy["sales_channel"].fillna("").astype(str).str.strip() == "AGENT"
        ][["policy_hash_key"]].drop_duplicates()
        if not agent_policies.empty:
            person_broker_pairs = set()
            if not link_broker_person.empty and {"person_hash_key", "broker_hash_key"}.issubset(link_broker_person.columns):
                person_broker_pairs = {
                    (str(person_hk), str(broker_hk))
                    for person_hk, broker_hk in link_broker_person[["person_hash_key", "broker_hash_key"]].itertuples(index=False, name=None)
                    if str(person_hk).strip() and str(broker_hk).strip()
                }
            broker_people = {pair[0] for pair in person_broker_pairs}
            agent_policy_people = (
                agent_policies
                .merge(link_policy_customer[["policy_hash_key", "customer_hash_key"]], on="policy_hash_key", how="left")
                .merge(link_customer_person[["customer_hash_key", "person_hash_key"]], on="customer_hash_key", how="left")
            )
            missing_broker = agent_policy_people[
                agent_policy_people["person_hash_key"].fillna("").astype(str).str.strip().eq("")
            ]
            if not missing_broker.empty:
                print("BROKER CHECK FAILED: AGENT policy missing linked person")
                errors += 1
            elif person_broker_pairs:
                invalid_people = agent_policy_people[
                    ~agent_policy_people["person_hash_key"].astype(str).isin(broker_people)
                ]
                if not invalid_people.empty:
                    print("BROKER CHECK FAILED: AGENT policy person missing broker reference")
                    errors += 1
            else:
                print("BROKER CHECK FAILED: AGENT policies exist but no person-broker links were generated")
                errors += 1

    if (
        not sat_policy.empty
        and not sat_motor.empty
        and not link_policy_insured_object.empty
        and not link_insured_object_motor.empty
        and {"policy_hash_key", "policy_status"}.issubset(sat_policy.columns)
        and {"motor_hash_key", "vehicle_model"}.issubset(sat_motor.columns)
        and {"policy_hash_key", "insured_object_hash_key"}.issubset(link_policy_insured_object.columns)
        and {"insured_object_hash_key", "motor_hash_key"}.issubset(link_insured_object_motor.columns)
    ):
        standard = {"Focus", "Corsa", "Corolla"}
        premium = {"Qashqai", "3 Series", "A3"}
        high_risk = {"Sport Bike", "Superbike", "High Performance SUV"}

        def vehicle_segment(value):
            if value in standard:
                return "STANDARD"
            if value in premium:
                return "PREMIUM"
            if value in high_risk:
                return "HIGH_RISK"
            return "UNKNOWN"

        policy_motor = (
            sat_policy[["policy_hash_key", "policy_status"]]
            .merge(link_policy_insured_object[["policy_hash_key", "insured_object_hash_key"]], on="policy_hash_key", how="inner")
            .merge(link_insured_object_motor[["insured_object_hash_key", "motor_hash_key"]], on="insured_object_hash_key", how="inner")
            .merge(sat_motor[["motor_hash_key", "vehicle_model"]], on="motor_hash_key", how="inner")
        )
        if not policy_motor.empty:
            policy_motor["vehicle_segment"] = policy_motor["vehicle_model"].map(vehicle_segment)
            policy_motor["churn_flag"] = policy_motor["policy_status"].isin(["CANCELLED", "LAPSED"]).astype(int)
            summary = (
                policy_motor.groupby("vehicle_segment")["churn_flag"]
                .agg(["count", "mean"])
                .reindex(["STANDARD", "PREMIUM", "HIGH_RISK"])
            )
            if summary["count"].ge(20).all():
                ranges = {
                    "STANDARD": (0.12, 0.22),
                    "PREMIUM": (0.20, 0.35),
                    "HIGH_RISK": (0.30, 0.50),
                }
                rates = summary["mean"]
                ranges_ok = all(ranges[idx][0] <= float(rates.loc[idx]) <= ranges[idx][1] for idx in ranges)
                direction_ok = rates.loc["STANDARD"] < rates.loc["PREMIUM"] < rates.loc["HIGH_RISK"]
                if not (ranges_ok and direction_ok):
                    detail = {
                        idx: {"count": int(row["count"]), "churn_pct": round(float(row["mean"]) * 100, 2)}
                        for idx, row in summary.iterrows()
                    }
                    print(f"VEHICLE CHURN CHECK FAILED: vehicle segment churn outside workbook ranges {detail}")
                    errors += 1

    if not link_claim_policy.empty and not sat_claim.empty and not policy_lookup.empty:
        claim_product_by_policy = {}
        if not link_policy_product.empty and not product_lookup.empty:
            merged_products = (
                link_policy_product[["policy_hash_key", "product_hash_key"]]
                .merge(sat_product[["product_hash_key", "type"]], on="product_hash_key", how="left")
            )
            for policy_hk, product_code in merged_products[["policy_hash_key", "type"]].itertuples(index=False, name=None):
                claim_product_by_policy[str(policy_hk)] = claim_product_for_code(str(product_code))

        merged = (
            link_claim_policy[["policy_hash_key", "claim_hash_key"]]
            .merge(sat_policy[["policy_hash_key", "policy_status", "policy_start_date", "policy_end_date", "sales_channel"]], on="policy_hash_key", how="left")
            .merge(
                sat_claim[[
                    "claim_hash_key",
                    "claim_reported_date",
                    "claim_settlement_date",
                    "claim_product",
                    "claim_channel",
                    "is_recovery_happened",
                    "recovery_priority_score",
                    "first_recovery_date",
                    "last_recovery_date",
                ]],
                on="claim_hash_key",
                how="left",
            )
        )
        if (merged["policy_status"] != "ACTIVE").any():
            print("CLAIM CHECK FAILED: linked claim found on non-active policy")
            errors += 1
        reported = parse_date(merged["claim_reported_date"])
        settlement = parse_date(merged["claim_settlement_date"])
        policy_start = parse_date(merged["policy_start_date"])
        policy_end = parse_date(merged["policy_end_date"])
        if ((~reported.isna()) & (~policy_start.isna()) & (reported < policy_start)).any():
            print("CLAIM CHECK FAILED: claim_reported_date < policy_start_date")
            errors += 1
        if ((~reported.isna()) & (~policy_end.isna()) & (reported > policy_end)).any():
            print("CLAIM CHECK FAILED: claim_reported_date > policy_end_date")
            errors += 1
        if ((~settlement.isna()) & (~reported.isna()) & (settlement < reported)).any():
            print("CLAIM CHECK FAILED: claim_settlement_date < claim_reported_date")
            errors += 1
        product_compare = merged.assign(
            expected_product=merged["policy_hash_key"].astype(str).map(claim_product_by_policy)
        )
        product_mismatch = (
            product_compare["expected_product"].notna()
            & (
                product_compare["claim_product"].fillna("").astype(str).str.strip()
                != product_compare["expected_product"].fillna("").astype(str).str.strip()
            )
        )
        if product_mismatch.any():
            print("CLAIM CHECK FAILED: claim_product does not match linked policy product")
            errors += 1
        if (
            merged["claim_channel"].fillna("").astype(str).str.strip()
            != merged["sales_channel"].fillna("").astype(str).str.strip()
        ).any():
            print("CLAIM CHECK FAILED: claim_channel does not match linked policy sales_channel")
            errors += 1
        recovery_happened = merged["is_recovery_happened"].fillna("").astype(str).str.strip().str.upper()
        recovery_priority = merged["recovery_priority_score"].fillna("").astype(str).str.strip()
        priority_numeric = pd.to_numeric(recovery_priority, errors="coerce")
        priority_bad = recovery_priority.eq("") | priority_numeric.isna()
        if priority_bad.any():
            print("CLAIM CHECK FAILED: recovery_priority_score must be populated integer")
            errors += 1
        first_recovery = parse_dt(merged["first_recovery_date"])
        last_recovery = parse_dt(merged["last_recovery_date"])
        missing_recovery_dates = first_recovery.isna() | last_recovery.isna()
        if missing_recovery_dates.any():
            print("CLAIM CHECK FAILED: first_recovery_date/last_recovery_date must be populated timestamps")
            errors += 1
        no_recovery = recovery_happened == "N"
        has_recovery = recovery_happened == "Y"
        no_recovery_bad = no_recovery & (
            first_recovery.ne(RECOVERY_SENTINEL_TIMESTAMP)
            | last_recovery.ne(RECOVERY_SENTINEL_TIMESTAMP)
        )
        if no_recovery_bad.any():
            print("CLAIM CHECK FAILED: no-recovery claims must use 1900-01-01T00:00:00 recovery sentinel")
            errors += 1
        has_recovery_bad = has_recovery & (
            first_recovery.eq(RECOVERY_SENTINEL_TIMESTAMP)
            | last_recovery.eq(RECOVERY_SENTINEL_TIMESTAMP)
            | (last_recovery < first_recovery)
            | ((~reported.isna()) & (first_recovery < reported))
        )
        if has_recovery_bad.any():
            print("CLAIM CHECK FAILED: recovery dates must follow claim lifecycle for recovery claims")
            errors += 1

    if not link_complaint_policy.empty and not sat_complaint.empty and not policy_lookup.empty:
        policy_categories: dict[str, str] = {}
        policy_channels: dict[str, str] = {}
        if not link_policy_customer.empty and not link_policy_product.empty and not sat_policy.empty and not sat_product.empty:
            policy_products = (
                link_policy_product[["policy_hash_key", "product_hash_key"]]
                .merge(sat_product[["product_hash_key", "type"]], on="product_hash_key", how="left")
                .merge(sat_policy[["policy_hash_key", "sales_channel"]], on="policy_hash_key", how="left")
            )
            for policy_hk, product_code, sales_channel in policy_products[["policy_hash_key", "type", "sales_channel"]].itertuples(index=False, name=None):
                policy_key = str(policy_hk)
                if pd.notna(product_code):
                    policy_categories[policy_key] = insurance_category_for_code(str(product_code))
                if pd.notna(sales_channel):
                    policy_channels[policy_key] = str(sales_channel)

        merged = (
            link_complaint_policy[["complaint_hash_key", "policy_hash_key"]]
            .merge(sat_complaint[["complaint_hash_key", "complaint_date", "complaint_acknowledgement_date", "complaint_resolved_date", "complaint_channel", "insurance_category"]], on="complaint_hash_key", how="left")
            .merge(link_policy_customer[["policy_hash_key", "customer_hash_key"]], on="policy_hash_key", how="left")
            .merge(sat_customer[["customer_hash_key", "customer_since"]], on="customer_hash_key", how="left")
        )
        complaint_date = parse_date(merged["complaint_date"])
        acknowledgement_date = parse_date(merged["complaint_acknowledgement_date"])
        resolved_date = parse_date(merged["complaint_resolved_date"])
        customer_since = parse_date(merged["customer_since"])
        if ((~complaint_date.isna()) & (~customer_since.isna()) & (complaint_date < customer_since)).any():
            print("COMPLAINT CHECK FAILED: complaint_date < customer_since")
            errors += 1
        if ((~acknowledgement_date.isna()) & (~complaint_date.isna()) & (acknowledgement_date < complaint_date)).any():
            print("COMPLAINT CHECK FAILED: acknowledgement_date < complaint_date")
            errors += 1
        base_ack = acknowledgement_date.where(~acknowledgement_date.isna(), complaint_date)
        if ((~resolved_date.isna()) & (~base_ack.isna()) & (resolved_date < base_ack)).any():
            print("COMPLAINT CHECK FAILED: resolved_date < acknowledgement_date/complaint_date")
            errors += 1
        invalid_category = 0
        invalid_channel = 0
        for policy_hk, insurance_category, complaint_channel in merged[["policy_hash_key", "insurance_category", "complaint_channel"]].itertuples(index=False, name=None):
            policy_key = str(policy_hk)
            allowed_category = policy_categories.get(policy_key)
            allowed_channel = policy_channels.get(policy_key)
            if allowed_category and str(insurance_category).strip() != allowed_category:
                invalid_category += 1
            if allowed_channel and str(complaint_channel).strip() != allowed_channel:
                invalid_channel += 1
        if invalid_category:
            print(f"COMPLAINT CHECK FAILED: insurance_category not in customer policy portfolio rows={invalid_category}")
            errors += 1
        if invalid_channel:
            print(f"COMPLAINT CHECK FAILED: complaint_channel not in customer policy channels rows={invalid_channel}")
            errors += 1

    if not link_policy_override.empty and not sat_override.empty and not policy_lookup.empty:
        merged = (
            link_policy_override[["override_hash_key", "policy_hash_key"]]
            .merge(sat_override[["override_hash_key", "override_reason"]], on="override_hash_key", how="left")
            .merge(sat_policy[["policy_hash_key", "policy_status", "override_commission"]], on="policy_hash_key", how="left")
        )
        if (merged["policy_status"] != "ACTIVE").any():
            print("OVERRIDE CHECK FAILED: override linked to non-active policy")
            errors += 1
        reason_blank = merged["override_reason"].fillna("").astype(str).str.strip().eq("")
        if reason_blank.any():
            print("OVERRIDE CHECK FAILED: blank override_reason on linked override")
            errors += 1
        commission_blank = merged["override_commission"].fillna("").astype(str).str.strip().eq("")
        if commission_blank.any():
            print("OVERRIDE CHECK FAILED: blank override_commission on linked policy override")
            errors += 1

    sat_insured_object = frames.get("sat_insured_object", pd.DataFrame())
    if not sat_insured_object.empty and "insured_value" in sat_insured_object.columns:
        insured_values = sat_insured_object["insured_value"].fillna("").astype(str).str.strip()
        blank_count = int(insured_values.eq("").sum())
        numeric_values = pd.to_numeric(insured_values, errors="coerce")
        invalid_count = int((insured_values.ne("") & numeric_values.isna()).sum())
        non_positive_count = int((numeric_values.fillna(0) <= 0).sum())
        if blank_count or invalid_count or non_positive_count:
            print(
                "INSURED OBJECT CHECK FAILED: insured_value must be populated positive integer "
                f"blank_rows={blank_count} invalid_rows={invalid_count} non_positive_rows={non_positive_count}"
            )
            errors += 1

    if not sat_regulation.empty:
        if "is_regulation_on_time" in sat_regulation.columns:
            invalid_count, sample_values = count_invalid_enum(
                sat_regulation,
                "is_regulation_on_time",
                {"Y", "N"},
            )
            if invalid_count:
                print(
                    f"REGULATION CHECK FAILED: is_regulation_on_time invalid_rows={invalid_count} "
                    f"sample={sample_values}"
                )
                errors += 1
        regs = sat_regulation[[
            "regulation_hash_key",
            "regulation_date_raised",
            "regulation_deadline_date",
            "regulation_date_closed",
        ]].copy()
        raised = parse_date(regs["regulation_date_raised"])
        deadline = parse_date(regs["regulation_deadline_date"])
        closed = parse_date(regs["regulation_date_closed"])
        if ((~raised.isna()) & (~deadline.isna()) & (raised > deadline)).any():
            print("REGULATION CHECK FAILED: regulation_date_raised > regulation_deadline_date")
            errors += 1
        if ((~raised.isna()) & (~closed.isna()) & (raised > closed)).any():
            print("REGULATION CHECK FAILED: regulation_date_raised > regulation_date_closed")
            errors += 1

    return errors


def verify_enhanced(base_path: str) -> bool:
    ddl = parse_enhanced_ddl()
    schemas = ddl["tables"]
    primary_keys = ddl["primary_keys"]
    foreign_keys = ddl["foreign_keys"]

    frames: dict[str, pd.DataFrame] = {}
    errors = 0

    print(f"Checking enhanced synthetic folder: {base_path}")

    for table_name, expected_cols in schemas.items():
        file_name = f"{table_name}.csv"
        path = os.path.join(base_path, file_name)
        if not os.path.exists(path):
            print(f"MISSING FILE: {file_name}")
            errors += 1
            continue

        df = read_csv_safe(path)
        frames[table_name] = df
        actual_cols = list(df.columns)
        missing = [col for col in expected_cols if col not in actual_cols]
        extra = [col for col in actual_cols if col not in expected_cols]
        if missing or extra:
            print(f"COLUMN MISMATCH: {file_name}")
            if missing:
                print(f"  missing={missing}")
            if extra:
                print(f"  extra={extra}")
            errors += 1
        else:
            print(f"{file_name}: columns ok | rows={len(df)}")

    for table_name, pk_cols in primary_keys.items():
        df = frames.get(table_name)
        if df is None:
            continue
        missing_pk = [col for col in pk_cols if col not in df.columns]
        if missing_pk:
            print(f"PK CHECK FAILED: {table_name} missing {missing_pk}")
            errors += 1
            continue
        if df.empty:
            print(f"{table_name}: PK skipped (empty)")
            continue
        duplicate_count = int(df.duplicated(subset=pk_cols, keep=False).sum())
        null_count = int(df[pk_cols].isna().any(axis=1).sum())
        blank_count = int((df[pk_cols].astype(str).apply(lambda col: col.str.strip() == "")).any(axis=1).sum())
        if duplicate_count or null_count or blank_count:
            print(
                f"PK CHECK FAILED: {table_name} pk={pk_cols} "
                f"duplicates={duplicate_count} nulls={null_count} blanks={blank_count}"
            )
            errors += 1
        else:
            print(f"{table_name}: PK ok")

    for fk in foreign_keys:
        child_table = fk["child_table"]
        parent_table = fk["parent_table"]
        child_cols = fk["child_columns"]
        parent_cols = fk["parent_columns"]
        child_df = frames.get(child_table)
        parent_df = frames.get(parent_table)
        if child_df is None or parent_df is None:
            continue
        if child_df.empty:
            print(f"{child_table}->{parent_table}: FK skipped (empty child)")
            continue

        missing_child = [col for col in child_cols if col not in child_df.columns]
        missing_parent = [col for col in parent_cols if col not in parent_df.columns]
        if missing_child or missing_parent:
            print(f"FK CHECK FAILED: {child_table}->{parent_table} missing child={missing_child} parent={missing_parent}")
            errors += 1
            continue

        child_keys = set(tuple(row) for row in child_df[child_cols].dropna().astype(str).itertuples(index=False, name=None))
        parent_keys = set(tuple(row) for row in parent_df[parent_cols].dropna().astype(str).itertuples(index=False, name=None))
        missing = child_keys - parent_keys
        if missing:
            print(f"FK CHECK FAILED: {child_table}->{parent_table} missing={len(missing)} sample={list(missing)[:5]}")
            errors += 1
        else:
            print(f"{child_table}->{parent_table}: FK ok")

    errors += verify_timestamp_columns(frames, ddl)
    errors += verify_numeric_columns(frames, ddl)
    errors += verify_base_enum_alignment(frames)
    errors += verify_base_timelines(frames)
    errors += verify_enhanced_business_rules(frames)

    group_counts = {"hub": 0, "link": 0, "sat": 0}
    for table_name in schemas:
        prefix = table_name.split("_", 1)[0]
        if prefix in group_counts:
            group_counts[prefix] += 1

    print("")
    print(f"Expected tables: {len(schemas)}")
    print(f"Group counts: hubs={group_counts['hub']} links={group_counts['link']} sats={group_counts['sat']}")

    if errors:
        print(f"Enhanced verification failed with {errors} issue(s).")
        return False

    print("Enhanced verification passed.")
    return True


def parse_args():
    parser = argparse.ArgumentParser(description="Verify enhanced synthetic Data Vault output.")
    parser.add_argument("folder", nargs="?", help="Enhanced synthetic folder. Defaults to latest data/synthetic/enhanced run.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    target = args.folder or latest_subdir(SYNTHETIC_ENHANCED_ROOT)
    if not target:
        raise SystemExit("No enhanced synthetic folder found.")
    ok = verify_enhanced(target)
    raise SystemExit(0 if ok else 1)
