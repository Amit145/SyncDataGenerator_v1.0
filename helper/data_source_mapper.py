from __future__ import annotations

import csv
import os
from datetime import datetime, timedelta

from config.storage_paths import RAW_DATA_SOURCE_CANONICAL_ROOT
from helper.canonical_raw_schema import CANONICAL_RAW_SCHEMAS
from helper.csv_writer import write_csv


def _read_csv(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _dedupe(rows: list[dict], key_fields: list[str]) -> list[dict]:
    seen = set()
    out = []
    for row in rows:
        key = tuple(row.get(field, "") for field in key_fields)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _parse_dt(value: str) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _fmt_dt(value: datetime | None) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S") if value else ""


def _base_row(batch_id: str, extract_ts: str) -> dict:
    return {
        "_batch_id": batch_id,
        "_extract_ts": extract_ts,
        "_source_system": "iCRM",
    }


def map_data_source_to_canonical(run_id: str, source_dirs: dict[str, str]) -> str:
    out_dir = os.path.join(RAW_DATA_SOURCE_CANONICAL_ROOT, run_id)
    os.makedirs(out_dir, exist_ok=True)

    combined = {name: [] for name in CANONICAL_RAW_SCHEMAS}

    for domain, source_dir in source_dirs.items():
        party_file = os.path.join(source_dir, f"{domain}_party_extract.csv")
        policy_file = os.path.join(source_dir, f"{domain}_policy_extract.csv")
        asset_file = os.path.join(source_dir, f"{domain}_{'vehicle' if domain == 'motor' else 'property'}_extract.csv")

        party_rows = _read_csv(party_file)
        policy_rows = _read_csv(policy_file)
        asset_rows = _read_csv(asset_file)

        batch_id = run_id
        extract_ts = ""
        for row in party_rows:
            batch_id = batch_id or row.get("_batch_id", run_id)
            extract_ts = extract_ts or row.get("_extract_ts", "")
            base = _base_row(run_id, extract_ts)
            combined["crm_person.csv"].append({**base,
                "person_id": row.get("src_party_ref", ""),
                "person_type": row.get("party_category", ""),
                "tenant_id": row.get("tenant_code", ""),
                "is_lead": row.get("lead_flag", ""),
                "operational_paperless_consent": row.get("paperless_flag", ""),
                "source_id": row.get("origin_id", ""),
                "source_type": row.get("origin_type", ""),
                "natural_person_id": row.get("natural_party_ref", ""),
                "first_name": row.get("given_name", ""),
                "last_name": row.get("family_name", ""),
                "full_name": row.get("display_name", ""),
                "courtesy_title": row.get("title_text", ""),
                "occupation": row.get("occupation_desc", ""),
                "birth_date": row.get("dob", ""),
                "birth_year": row.get("birth_year_no", ""),
                "nationality": row.get("nationality_desc", ""),
                "gender": row.get("gender_desc", ""),
                "marital_status": row.get("marital_desc", ""),
                "preferred_language": row.get("language_pref", ""),
                "role": row.get("role_desc", ""),
                "job_title": row.get("job_title_desc", ""),
                "legal_person_id": row.get("legal_party_ref", ""),
                "company_name": row.get("company_legal_name", ""),
                "legal_person_score": row.get("legal_score", ""),
                "legal_person_status": row.get("legal_status", ""),
                "legal_person_job_title": row.get("legal_job_title", ""),
                "legal_source_id": row.get("legal_origin_id", ""),
                "legal_source_type": row.get("legal_origin_type", ""),
                "date_of_constitution": row.get("constitution_dt", ""),
                "lead_converted_date": row.get("lead_converted_dt", ""),
            })
            if row.get("contact_ref"):
                combined["crm_contact.csv"].append({**base,
                    "person_id": row.get("src_party_ref", ""),
                    "contact_id": row.get("contact_ref", ""),
                    "personal_email": row.get("email_personal_txt", ""),
                    "work_email": row.get("email_work_txt", ""),
                    "work_phone": row.get("phone_work_txt", ""),
                    "home_phone": row.get("phone_home_txt", ""),
                })
            if row.get("identity_ref"):
                combined["crm_identity.csv"].append({**base,
                    "person_id": row.get("src_party_ref", ""),
                    "identities_id": row.get("identity_ref", ""),
                    "ecid": row.get("ecid_txt", ""),
                    "hashed_email": row.get("hashed_email_txt", ""),
                })
            if row.get("address_ref"):
                combined["crm_person_address.csv"].append({**base,
                    "person_id": row.get("src_party_ref", ""),
                    "address_id": row.get("address_ref", ""),
                    "street": row.get("street_line_txt", ""),
                    "postcode": row.get("postal_cd", ""),
                    "city": row.get("city_name", ""),
                    "state": row.get("state_code", ""),
                    "country": row.get("country_code", ""),
                })
            if row.get("lead_ref"):
                combined["crm_lead.csv"].append({**base,
                    "person_id": row.get("src_party_ref", ""),
                    "lead_id": row.get("lead_ref", ""),
                    "interested_level": row.get("interest_bucket", ""),
                    "preferred_contact_method": row.get("contact_pref", ""),
                    "person_score": row.get("lead_score_no", ""),
                    "person_status": row.get("lead_status", ""),
                    "converted_date": row.get("lead_converted_date", ""),
                })
            if row.get("customer_ref"):
                combined["crm_customer.csv"].append({**base,
                    "person_id": row.get("src_party_ref", ""),
                    "customer_id": row.get("customer_ref", ""),
                    "customer_number": row.get("customer_no_txt", ""),
                    "customer_status": row.get("customer_status_txt", ""),
                    "customer_status_reason": row.get("customer_status_reason_txt", ""),
                    "customer_since": row.get("customer_since_dt", ""),
                    "customer_rating": row.get("customer_rating_no", ""),
                    "customer_segment": row.get("customer_segment_txt", ""),
                    "line_of_business": row.get("line_of_business_txt", ""),
                    "nps_score": row.get("nps_score_no", ""),
                })
            if row.get("customer_ref") and row.get("customer_lead_ref"):
                combined["crm_customer_lead.csv"].append({**base,
                    "customer_id": row.get("customer_ref", ""),
                    "lead_id": row.get("customer_lead_ref", ""),
                })
            if row.get("consent_ref"):
                combined["crm_consent.csv"].append({**base,
                    "person_id": row.get("src_party_ref", ""),
                    "consent_id": row.get("consent_ref", ""),
                    "opt_in_validated": row.get("opt_in_valid_ind", ""),
                    "opt_in_legitimate_interest": row.get("opt_in_legitimate_ind", ""),
                })
            if row.get("mpr_ref"):
                combined["crm_marketing_preference.csv"].append({**base,
                    "person_id": row.get("src_party_ref", ""),
                    "marketing_preference_id": row.get("mpr_ref", ""),
                    "sms": row.get("sms_pref_ind", ""),
                    "email": row.get("email_pref_ind", ""),
                    "email_subscriptions": row.get("email_sub_pref_ind", ""),
                    "call": row.get("call_pref_ind", ""),
                    "any": row.get("any_pref_ind", ""),
                    "commercial_email": row.get("commercial_email_ind", ""),
                    "postal_mail": row.get("postal_mail_ind", ""),
                })
            if row.get("engagement_ref"):
                combined["crm_marketing_engagement.csv"].append({**base,
                    "person_id": row.get("src_party_ref", ""),
                    "marketing_engagement_id": row.get("engagement_ref", ""),
                    "promotion_code": row.get("promo_code_txt", ""),
                    "opened_email": row.get("opened_email_ind", ""),
                    "marketing_status": row.get("marketing_status_txt", ""),
                })
            if row.get("account_ref"):
                combined["crm_account.csv"].append({**base,
                    "person_id": row.get("src_party_ref", ""),
                    "account_id": row.get("account_ref", ""),
                    "account_number": row.get("account_no_txt", ""),
                    "account_type": row.get("account_type_txt", ""),
                    "account_last_access": row.get("last_access_dt", ""),
                    "account_last_change": row.get("last_change_dt", ""),
                    "account_creation_type": row.get("account_create_type_txt", ""),
                    "account_status": row.get("account_status_txt", ""),
                })

        for row in policy_rows:
            base = _base_row(run_id, extract_ts)
            combined["crm_product.csv"].append({**base,
                "product_id": row.get("src_product_ref", ""),
                "product_code": row.get("lob_policy_code", ""),
                "product_type": row.get("lob_policy_type", ""),
            })
            combined["crm_quote.csv"].append({**base,
                "person_id": row.get("src_party_ref", ""),
                "quote_id": row.get("src_quote_ref", ""),
                "product_id": row.get("src_product_ref", ""),
                "product_code": row.get("lob_policy_code", ""),
                "gross_revenue": row.get("quoted_gwp", ""),
                "net_revenue": row.get("quoted_gwp", ""),
                "quote_number": row.get("src_quote_ref", ""),
                "quote_status": row.get("source_quote_status", ""),
                "renewal_amt_current_period": row.get("renewal_premium_current", ""),
                "renewal_amt_next_period": row.get("renewal_premium_next", ""),
            })
            combined["crm_policy.csv"].append({**base,
                "person_id": row.get("src_party_ref", ""),
                "customer_id": row.get("src_customer_ref", ""),
                "policy_id": row.get("src_policy_ref", ""),
                "quote_id": row.get("src_quote_ref", ""),
                "product_id": row.get("src_product_ref", ""),
                "product_code": row.get("lob_policy_code", ""),
                "cover_option": row.get("coverage_tier", ""),
                "declined_claims": row.get("claim_decline_count", ""),
                "fraud_flag": row.get("fraud_ind", ""),
                "gross_revenue": row.get("gross_written_premium", ""),
                "net_revenue": row.get("net_written_premium", ""),
                "number_of_active_claim": row.get("open_claim_count", ""),
                "number_of_previous_claim": row.get("historical_claim_count", ""),
                "policy_cicle": row.get("policy_cycle_no", ""),
                "policy_end_date": row.get("policy_expiry_dt", ""),
                "policy_length": row.get("term_months", ""),
                "policy_number": row.get("source_policy_no", ""),
                "policy_start_date": row.get("policy_inception_dt", ""),
                "policy_status": row.get("policy_state", ""),
                "renewal_amount_current_period": row.get("renewal_premium_current", ""),
                "renewal_amount_next_period": row.get("renewal_premium_next", ""),
                "renewal_date": row.get("renewal_notice_dt", ""),
                "sales_channel": row.get("distribution_channel", ""),
            })

        for row in asset_rows:
            base = _base_row(run_id, extract_ts)
            if domain == "motor":
                combined["crm_motor.csv"].append({**base,
                    "policy_id": row.get("src_policy_ref", ""),
                    "product_id": row.get("src_product_ref", ""),
                    "product_code": "PRD_MOTOR_PERSONAL" if "COMMERCIAL" not in row.get("src_product_ref", "") else "PRD_COMMERCIAL_MOTOR",
                    "motor_id": row.get("vehicle_asset_ref", ""),
                    "body_type": row.get("vehicle_body_style", ""),
                    "fuel_type": row.get("fuel_desc", ""),
                    "license_status": row.get("license_state", ""),
                    "is_existing_motor_customer": row.get("existing_motor_flag", ""),
                    "motor_lapsed_policies": row.get("lapsed_motor_policies", ""),
                    "motor_risk_address": row.get("garage_risk_location", ""),
                    "risk_class_code": row.get("risk_band_code", ""),
                    "variant": row.get("vehicle_variant_name", ""),
                    "vehicle_owner_type": row.get("owner_type_desc", ""),
                    "vehicle_regstate": row.get("registration_state", ""),
                    "vehicle_class": row.get("vehicle_segment", ""),
                    "vehicle_model": row.get("vehicle_model_name", ""),
                    "vehicle_type": row.get("vehicle_usage_type", ""),
                    "motor_sum_insrd": row.get("insured_value_amt", ""),
                    "vehicle_year": row.get("manufacture_year", ""),
                    "vehicle_age": row.get("vehicle_age_years", ""),
                })
            else:
                combined["crm_home.csv"].append({**base,
                    "policy_id": row.get("src_policy_ref", ""),
                    "product_id": row.get("src_product_ref", ""),
                    "product_code": "PRD_HOME_PERSONAL",
                    "home_id": row.get("property_asset_ref", ""),
                    "wall_construction": row.get("wall_material", ""),
                    "home_risk_address": row.get("risk_property_address", ""),
                    "roof_construction": row.get("roof_material", ""),
                    "home_type": row.get("property_usage_type", ""),
                    "home_state": row.get("property_state", ""),
                    "is_existing_home_customer": row.get("existing_home_flag", ""),
                    "street": row.get("property_street", ""),
                    "postcode": row.get("property_postcode", ""),
                    "city": row.get("property_city", ""),
                    "country": row.get("property_country", ""),
                })

    dedupe_keys = {
        "crm_person.csv": ["person_id"],
        "crm_contact.csv": ["contact_id"],
        "crm_identity.csv": ["identities_id"],
        "crm_person_address.csv": ["address_id"],
        "crm_lead.csv": ["lead_id"],
        "crm_customer.csv": ["customer_id"],
        "crm_customer_lead.csv": ["customer_id", "lead_id"],
        "crm_consent.csv": ["consent_id"],
        "crm_marketing_preference.csv": ["marketing_preference_id"],
        "crm_marketing_engagement.csv": ["marketing_engagement_id"],
        "crm_account.csv": ["account_id"],
        "crm_product.csv": ["product_id"],
        "crm_quote.csv": ["quote_id"],
        "crm_policy.csv": ["policy_id"],
        "crm_home.csv": ["home_id"],
        "crm_motor.csv": ["motor_id"],
    }

    combined["crm_person.csv"] = _dedupe(combined["crm_person.csv"], dedupe_keys["crm_person.csv"])
    combined["crm_lead.csv"] = _dedupe(combined["crm_lead.csv"], dedupe_keys["crm_lead.csv"])
    combined["crm_customer.csv"] = _dedupe(combined["crm_customer.csv"], dedupe_keys["crm_customer.csv"])
    combined["crm_product.csv"] = _dedupe(combined["crm_product.csv"], dedupe_keys["crm_product.csv"])
    combined["crm_quote.csv"] = _dedupe(combined["crm_quote.csv"], dedupe_keys["crm_quote.csv"])
    combined["crm_policy.csv"] = _dedupe(combined["crm_policy.csv"], dedupe_keys["crm_policy.csv"])
    combined["crm_home.csv"] = _dedupe(combined["crm_home.csv"], dedupe_keys["crm_home.csv"])
    combined["crm_motor.csv"] = _dedupe(combined["crm_motor.csv"], dedupe_keys["crm_motor.csv"])

    latest_policy_by_person = {}
    for row in combined["crm_policy.csv"]:
        person_id = row.get("person_id", "")
        policy_start = _parse_dt(row.get("policy_start_date", ""))
        existing = latest_policy_by_person.get(person_id)
        if not existing:
            latest_policy_by_person[person_id] = row
            continue
        existing_dt = _parse_dt(existing.get("policy_start_date", ""))
        if policy_start and (not existing_dt or policy_start > existing_dt):
            latest_policy_by_person[person_id] = row

    combined["crm_policy.csv"] = list(latest_policy_by_person.values())
    selected_policy_ids = {row.get("policy_id", "") for row in combined["crm_policy.csv"]}
    selected_quote_ids = {row.get("quote_id", "") for row in combined["crm_policy.csv"]}
    selected_product_ids = {row.get("product_id", "") for row in combined["crm_policy.csv"]}

    combined["crm_quote.csv"] = [row for row in combined["crm_quote.csv"] if row.get("quote_id", "") in selected_quote_ids]
    combined["crm_product.csv"] = [row for row in combined["crm_product.csv"] if row.get("product_id", "") in selected_product_ids]
    combined["crm_home.csv"] = [row for row in combined["crm_home.csv"] if row.get("policy_id", "") in selected_policy_ids]
    combined["crm_motor.csv"] = [row for row in combined["crm_motor.csv"] if row.get("policy_id", "") in selected_policy_ids]

    person_ids = {row.get("person_id", "") for row in combined["crm_person.csv"]}
    lead_ids = {row.get("lead_id", "") for row in combined["crm_lead.csv"]}
    customer_ids = {row.get("customer_id", "") for row in combined["crm_customer.csv"]}
    product_ids = {row.get("product_id", "") for row in combined["crm_product.csv"]}
    quote_ids = {row.get("quote_id", "") for row in combined["crm_quote.csv"]}
    policy_ids = {row.get("policy_id", "") for row in combined["crm_policy.csv"]}

    combined["crm_contact.csv"] = [row for row in combined["crm_contact.csv"] if row.get("person_id", "") in person_ids]
    combined["crm_identity.csv"] = [row for row in combined["crm_identity.csv"] if row.get("person_id", "") in person_ids]
    combined["crm_person_address.csv"] = [row for row in combined["crm_person_address.csv"] if row.get("person_id", "") in person_ids]
    combined["crm_customer_lead.csv"] = [
        row for row in combined["crm_customer_lead.csv"]
        if row.get("customer_id", "") in customer_ids and row.get("lead_id", "") in lead_ids
    ]
    combined["crm_consent.csv"] = [row for row in combined["crm_consent.csv"] if row.get("person_id", "") in person_ids]
    combined["crm_marketing_preference.csv"] = [row for row in combined["crm_marketing_preference.csv"] if row.get("person_id", "") in person_ids]
    combined["crm_marketing_engagement.csv"] = [row for row in combined["crm_marketing_engagement.csv"] if row.get("person_id", "") in person_ids]
    combined["crm_account.csv"] = [row for row in combined["crm_account.csv"] if row.get("person_id", "") in person_ids]
    combined["crm_quote.csv"] = [
        row for row in combined["crm_quote.csv"]
        if row.get("person_id", "") in person_ids and row.get("product_id", "") in product_ids
    ]
    combined["crm_policy.csv"] = [
        row for row in combined["crm_policy.csv"]
        if row.get("person_id", "") in person_ids
        and row.get("customer_id", "") in customer_ids
        and row.get("product_id", "") in product_ids
        and row.get("quote_id", "") in quote_ids
    ]
    combined["crm_home.csv"] = [
        row for row in combined["crm_home.csv"]
        if row.get("policy_id", "") in policy_ids and row.get("product_id", "") in product_ids
    ]
    combined["crm_motor.csv"] = [
        row for row in combined["crm_motor.csv"]
        if row.get("policy_id", "") in policy_ids and row.get("product_id", "") in product_ids
    ]

    policy_by_person = {row.get("person_id", ""): row for row in combined["crm_policy.csv"]}
    for row in combined["crm_lead.csv"]:
        policy = policy_by_person.get(row.get("person_id", ""))
        policy_start = _parse_dt(policy.get("policy_start_date", "") if policy else "")
        if policy_start:
            row["converted_date"] = _fmt_dt(policy_start - timedelta(days=30))

    lead_by_person = {row.get("person_id", ""): row for row in combined["crm_lead.csv"]}
    for row in combined["crm_person.csv"]:
        lead = lead_by_person.get(row.get("person_id", ""))
        if lead:
            row["lead_converted_date"] = lead.get("converted_date", "")

    account_by_person = {row.get("person_id", ""): row for row in combined["crm_account.csv"]}
    policy_summary_by_person = {}
    for row in combined["crm_policy.csv"]:
        person_id = row.get("person_id", "")
        summary = policy_summary_by_person.setdefault(person_id, {
            "has_active": False,
            "has_lapsed": False,
            "has_cancelled": False,
            "fraud_flag": False,
            "max_revenue": 0.0,
            "declined_claims": 0,
        })
        status = row.get("policy_status", "")
        if status == "ACTIVE":
            summary["has_active"] = True
        elif status == "LAPSED":
            summary["has_lapsed"] = True
        elif status == "CANCELLED":
            summary["has_cancelled"] = True
        summary["fraud_flag"] = summary["fraud_flag"] or str(row.get("fraud_flag", "")).upper().strip() == "Y"
        try:
            summary["max_revenue"] = max(summary["max_revenue"], float(row.get("gross_revenue", 0) or 0))
        except ValueError:
            pass
        try:
            summary["declined_claims"] = max(summary["declined_claims"], int(float(row.get("declined_claims", 0) or 0)))
        except ValueError:
            pass

    for row in combined["crm_customer.csv"]:
        person_id = row.get("person_id", "")
        policy_summary = policy_summary_by_person.get(person_id, {})
        account_status = account_by_person.get(person_id, {}).get("account_status", "")
        try:
            nps_score = int(float(row.get("nps_score", 0) or 0))
        except ValueError:
            nps_score = 0

        segment_score = 0
        if policy_summary.get("has_active"):
            segment_score += 2
        if account_status == "OPEN":
            segment_score += 1
        if nps_score >= 9:
            segment_score += 1
        if policy_summary.get("max_revenue", 0) >= 1800:
            segment_score += 1
        if policy_summary.get("fraud_flag"):
            segment_score -= 2
        if policy_summary.get("has_cancelled"):
            segment_score -= 1
        elif policy_summary.get("has_lapsed"):
            segment_score -= 1

        expected_segment = "PREMIUM" if segment_score >= 4 else "STANDARD"
        row["customer_segment"] = expected_segment

        rating = 2
        if row.get("customer_status") == "ACTIVE":
            rating += 1
        if expected_segment == "PREMIUM":
            rating += 1
        if nps_score >= 9:
            rating += 1
        elif nps_score <= 1:
            rating -= 1
        if policy_summary.get("has_active"):
            rating += 1
        if policy_summary.get("has_lapsed"):
            rating -= 1
        if policy_summary.get("has_cancelled"):
            rating -= 1
        if account_status == "OPEN":
            rating += 1
        elif account_status == "SUSPENDED":
            rating -= 1
        elif account_status == "CLOSED":
            rating -= 2
        if policy_summary.get("fraud_flag"):
            rating -= 1
        if policy_summary.get("declined_claims", 0) > 1:
            rating -= 1
        row["customer_rating"] = str(max(1, min(5, rating)))

    for file_name, schema in CANONICAL_RAW_SCHEMAS.items():
        rows = _dedupe(combined[file_name], dedupe_keys.get(file_name, []))
        write_csv(out_dir, file_name, rows, fieldnames=schema)

    return out_dir
