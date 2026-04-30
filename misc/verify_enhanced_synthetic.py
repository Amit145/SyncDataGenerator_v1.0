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
    normalized = df[column].fillna("").astype(str).str.strip()
    mask = (normalized != "") & (~normalized.isin(allowed_values))
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
    for table_name, column_name in (
        ("sat_channel", "channel_name"),
        ("sat_claim", "claim_channel"),
        ("sat_complaints", "complaint_channel"),
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
    sat_complaints = frames.get("sat_complaints", pd.DataFrame())
    sat_override = frames.get("sat_override", pd.DataFrame())
    sat_regulations = frames.get("sat_regulations", pd.DataFrame())
    sat_channel = frames.get("sat_channel", pd.DataFrame())
    sat_customer = frames.get("sat_customer", pd.DataFrame())
    sat_product = frames.get("sat_product", pd.DataFrame())
    link_policy_claim = frames.get("link_policy_claim", pd.DataFrame())
    link_complaints_customer = frames.get("link_complaints_customer", pd.DataFrame())
    link_override_policy = frames.get("link_override_policy", pd.DataFrame())
    link_policy_channel = frames.get("link_policy_channel", pd.DataFrame())
    link_regulations_product = frames.get("link_regulations_product", pd.DataFrame())
    link_policy_product = frames.get("link_policy_product", pd.DataFrame())
    link_policy_customer = frames.get("link_policy_customer", pd.DataFrame())

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

    if not link_policy_claim.empty and not sat_claim.empty and not policy_lookup.empty:
        claim_product_by_policy = {}
        if not link_policy_product.empty and not product_lookup.empty:
            merged_products = (
                link_policy_product[["policy_hash_key", "product_hash_key"]]
                .merge(sat_product[["product_hash_key", "type"]], on="product_hash_key", how="left")
            )
            for policy_hk, product_code in merged_products[["policy_hash_key", "type"]].itertuples(index=False, name=None):
                claim_product_by_policy[str(policy_hk)] = claim_product_for_code(str(product_code))

        merged = (
            link_policy_claim[["policy_hash_key", "claim_hash_key"]]
            .merge(sat_policy[["policy_hash_key", "policy_status", "policy_start_date", "policy_end_date", "sales_channel"]], on="policy_hash_key", how="left")
            .merge(sat_claim[["claim_hash_key", "claim_reported_date", "claim_settlement_date", "claim_product", "claim_channel"]], on="claim_hash_key", how="left")
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

    if not link_complaints_customer.empty and not sat_complaints.empty and not customer_lookup.empty:
        customer_policy_categories: dict[str, set[str]] = {}
        customer_policy_channels: dict[str, set[str]] = {}
        if not link_policy_customer.empty and not link_policy_product.empty and not sat_policy.empty and not sat_product.empty:
            policy_products = (
                link_policy_customer[["customer_hash_key", "policy_hash_key"]]
                .merge(link_policy_product[["policy_hash_key", "product_hash_key"]], on="policy_hash_key", how="left")
                .merge(sat_product[["product_hash_key", "type"]], on="product_hash_key", how="left")
                .merge(sat_policy[["policy_hash_key", "sales_channel"]], on="policy_hash_key", how="left")
            )
            for customer_hk, product_code, sales_channel in policy_products[["customer_hash_key", "type", "sales_channel"]].itertuples(index=False, name=None):
                customer_key = str(customer_hk)
                if pd.notna(product_code):
                    customer_policy_categories.setdefault(customer_key, set()).add(
                        insurance_category_for_code(str(product_code))
                    )
                if pd.notna(sales_channel):
                    customer_policy_channels.setdefault(customer_key, set()).add(str(sales_channel))

        merged = (
            link_complaints_customer[["complaints_hash_key", "customer_hash_key"]]
            .merge(sat_complaints[["complaints_hash_key", "complaint_date", "complaint_acknowledgement_date", "complaint_resolved_date", "complaint_channel", "insurance_category"]], on="complaints_hash_key", how="left")
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
        for customer_hk, insurance_category, complaint_channel in merged[["customer_hash_key", "insurance_category", "complaint_channel"]].itertuples(index=False, name=None):
            customer_key = str(customer_hk)
            allowed_categories = customer_policy_categories.get(customer_key, set())
            allowed_channels = customer_policy_channels.get(customer_key, set())
            if allowed_categories and str(insurance_category).strip() not in allowed_categories:
                invalid_category += 1
            if allowed_channels and str(complaint_channel).strip() not in allowed_channels:
                invalid_channel += 1
        if invalid_category:
            print(f"COMPLAINT CHECK FAILED: insurance_category not in customer policy portfolio rows={invalid_category}")
            errors += 1
        if invalid_channel:
            print(f"COMPLAINT CHECK FAILED: complaint_channel not in customer policy channels rows={invalid_channel}")
            errors += 1

    if not link_override_policy.empty and not sat_override.empty and not policy_lookup.empty:
        merged = (
            link_override_policy[["override_hash_key", "policy_hash_key"]]
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

    if not link_regulations_product.empty and not sat_regulations.empty:
        regs = sat_regulations[[
            "regulations_hash_key",
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
