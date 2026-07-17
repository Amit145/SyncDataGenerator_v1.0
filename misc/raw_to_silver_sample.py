import argparse
import csv
import hashlib
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.storage_paths import RAW_CRM_ROOT, SILVER_REBUILT_ROOT

RAW_BASE = RAW_CRM_ROOT
SAMPLE_SILVER_BASE = SILVER_REBUILT_ROOT


def md5_hasher(value):
    return hashlib.md5(str(value).encode("utf-8")).hexdigest()


def write_csv(folder, name, rows, fieldnames=None):
    os.makedirs(folder, exist_ok=True)
    if not rows and not fieldnames:
        return
    if rows and not fieldnames:
        fieldnames = list(rows[0].keys())
    with open(os.path.join(folder, name.lower()), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if rows:
            writer.writerows(rows)


HUB_SCHEMAS = {
    "hub_person.csv": ("person_hash_key", "person_id"),
    "hub_natural_person.csv": ("natural_person_hash_key", "natural_person_id"),
    "hub_legal_person.csv": ("legal_person_hash_key", "legal_person_id"),
    "hub_product.csv": ("product_hash_key", "product_id"),
    "hub_lead.csv": ("lead_hash_key", "lead_id"),
    "hub_customer.csv": ("customer_hash_key", "customer_id"),
    "hub_identities.csv": ("identities_hash_key", "identities_id"),
    "hub_contact.csv": ("contact_hash_key", "contact_id"),
    "hub_consent.csv": ("consent_hash_key", "consent_id"),
    "hub_account.csv": ("account_hash_key", "account_id"),
    "hub_marketing_preference.csv": ("marketing_preference_hash_key", "marketing_preference_id"),
    "hub_marketing_engagement.csv": ("marketing_engagement_hash_key", "marketing_engagement_id"),
    "hub_quote.csv": ("quote_hash_key", "quote_id"),
    "hub_policy.csv": ("policy_hash_key", "policy_id"),
    "hub_motor.csv": ("motor_hash_key", "motor_id"),
    "hub_home.csv": ("home_hash_key", "home_id"),
    "hub_home_address.csv": ("home_address_hash_key", "home_address_id"),
}

SCHEMAS = {
    **{name: [pk, "load_date", "record_source", bid] for name, (pk, bid) in HUB_SCHEMAS.items()},
    "link_person_natural_person.csv": ["person_natural_person_hash_key", "load_date", "record_source", "person_hash_key", "natural_person_hash_key"],
    "link_person_legal_person.csv": ["person_legal_person_hash_key", "load_date", "record_source", "person_hash_key", "legal_person_hash_key"],
    "link_person_identities.csv": ["person_identities_hash_key", "load_date", "record_source", "person_hash_key", "identities_hash_key"],
    "link_person_contact.csv": ["person_contact_hash_key", "load_date", "record_source", "person_hash_key", "contact_hash_key"],
    "link_person_consent.csv": ["person_consent_hash_key", "load_date", "record_source", "person_hash_key", "consent_hash_key"],
    "link_person_home_address.csv": ["person_home_address_hash_key", "load_date", "record_source", "person_hash_key", "home_address_hash_key"],
    "link_person_account.csv": ["person_account_hash_key", "load_date", "record_source", "person_hash_key", "account_hash_key"],
    "link_person_lead.csv": ["person_lead_hash_key", "load_date", "record_source", "person_hash_key", "lead_hash_key"],
    "link_person_marketing_preference.csv": ["person_marketing_preference_hash_key", "load_date", "record_source", "person_hash_key", "marketing_preference_hash_key"],
    "link_person_marketing_engagement.csv": ["person_marketing_engagement_hash_key", "load_date", "record_source", "person_hash_key", "marketing_engagement_hash_key"],
    "link_customer_person.csv": ["customer_person_hash_key", "load_date", "record_source", "customer_hash_key", "person_hash_key"],
    "link_customer_lead.csv": ["customer_lead_hash_key", "load_date", "record_source", "customer_hash_key", "lead_hash_key"],
    "link_quote_person.csv": ["quote_person_hash_key", "load_date", "record_source", "quote_hash_key", "person_hash_key"],
    "link_quote_product.csv": ["quote_product_hash_key", "load_date", "record_source", "quote_hash_key", "product_hash_key"],
    "link_policy_customer.csv": ["policy_customer_hash_key", "load_date", "record_source", "policy_hash_key", "customer_hash_key"],
    "link_policy_product.csv": ["policy_customer_hash_key", "load_date", "record_source", "policy_hash_key", "product_hash_key"],
    "link_product_motor.csv": ["product_motor_hash_key", "load_date", "record_source", "product_hash_key", "motor_hash_key"],
    "link_product_home.csv": ["product_home_hash_key", "load_date", "record_source", "product_hash_key", "home_hash_key"],
    "sat_natural_person.csv": ["natural_person_hash_key", "load_date", "first_name", "last_name", "full_name", "courtesy_title", "occupation", "birth_date", "birth_year", "nationality", "gender", "marital_status", "assesed_disability_degree", "preferred_language", "role", "job_title"],
    "sat_legal_person.csv": ["legal_person_hash_key", "load_date", "person_score", "job_title", "source_id", "source_type", "person_status", "converted_date", "date_of_constitution", "company_name"],
    "sat_person.csv": ["person_hash_key", "load_date", "tenant_id", "is_lead", "type", "operational_paperless_consent", "source_id", "source_type"],
    "sat_lead.csv": ["lead_hash_key", "load_date", "interested_level", "preferred_contact_method", "person_score", "person_status", "converted_date"],
    "sat_customer.csv": ["customer_hash_key", "load_date", "customer_number", "customer_status", "customer_status_reason", "customer_since", "customer_rating", "customer_segment", "line_of_business", "nps_score"],
    "sat_identities.csv": ["identities_hash_key", "load_date", "ecid", "hashed_email"],
    "sat_contact.csv": ["contact_hash_key", "load_date", "personal_email", "work_email", "work_phone", "home_phone"],
    "sat_consent.csv": ["consent_hash_key", "load_date", "opt_in_validated", "opt_in_legitimate_interest"],
    "sat_account.csv": ["account_hash_key", "load_date", "account_number", "account_type", "account_last_access", "account_last_change", "account_creation_type", "account_status"],
    "sat_marketing_preference.csv": ["marketing_preference_hash_key", "load_date", "sms", "email", "email_subscriptions", "call", "any", "commercial_email", "postal_mail"],
    "sat_marketing_engagement.csv": ["marketing_engagement_hash_key", "load_date", "promotion_code", "opened_email", "marketing_status"],
    "sat_quote.csv": ["quote_hash_key", "load_date", "gross_revenue", "net_revenue", "quote_number", "quote_status", "renewal_amt_current_period", "renewal_amt_next_period"],
    "sat_policy.csv": ["policy_hash_key", "load_date", "cover_option", "declined_claims", "fraud_flag", "gross_revenue", "net_revenue", "number_of_active_claim", "number_of_previous_claim", "policy_cycle", "policy_end_date", "policy_length", "policy_number", "policy_start_date", "policy_status", "renewal_amount_current_period", "renewal_amount_next_period", "renewal_date", "sales_channel"],
    "sat_motor.csv": ["motor_hash_key", "load_date", "auto_decline_vehicle", "body_type", "fuel_type", "license_status", "is_existing_motor_customer", "motor_lapsed_policies", "motor_risk_address", "risk_class_code", "variant", "vehicle_owner_type", "vehicle_regstate", "vehicle_class", "vehicle_model", "vehicle_type", "motor_sum_insrd", "vehicle_year", "vehicle_age"],
    "sat_home.csv": ["home_hash_key", "load_date", "wall_construction", "home_risk_address", "roof_construction", "home_type", "home_state", "is_existing_home_customer"],
    "sat_home_address.csv": ["home_address_hash_key", "load_date", "street", "postcode", "city", "state", "country"],
    "sat_product.csv": ["product_hash_key", "load_date", "type"],
}


def read_rows(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def read_jsonl_rows(path):
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def read_manifest(raw_dir):
    path = os.path.join(raw_dir, "manifest.json")
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def first_extract_ts(raw):
    for rows in raw.values():
        if rows:
            return rows[0].get("_extract_ts", "") or rows[0].get("extract_ts", "") or rows[0].get("pull_ts", "")
    return ""


def first_record_source(raw, default="CRM"):
    for rows in raw.values():
        if rows:
            return rows[0].get("_source_system", "") or rows[0].get("source_system", "") or rows[0].get("origin_sys", "") or default
    return default


def _map_rows(rows, mapping):
    return [
        {target: row.get(source, "") for target, source in mapping.items()}
        for row in rows
    ]


def _timestamp_value(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "T" in text or " " in text:
        return text
    return f"{text} 00:00:00"


def read_prd1_raw(raw_dir):
    """Convert PRD1 source extracts into the canonical raw shape used by build_silver."""
    return {
        "person": _map_rows(
            read_rows(os.path.join(raw_dir, "party_master.csv")),
            {
                "person_id": "party_ref",
                "tenant_id": "tenant_cd",
                "is_lead": "lead_ind",
                "person_type": "party_kind",
                "operational_paperless_consent": "paperless_ind",
                "source_id": "src_party_ref",
                "source_type": "src_party_type",
                "natural_person_id": "natural_ref",
                "first_name": "given_nm",
                "last_name": "family_nm",
                "full_name": "display_nm",
                "courtesy_title": "title_txt",
                "occupation": "occupation_txt",
                "birth_date": "dob",
                "birth_year": "birth_yr",
                "nationality": "nationality_txt",
                "gender": "gender_txt",
                "marital_status": "marital_txt",
                "assesed_disability_degree": "disability_degree",
                "preferred_language": "language_pref",
                "role": "role_txt",
                "job_title": "job_title_txt",
                "legal_person_id": "legal_ref",
                "company_name": "legal_name",
                "legal_person_score": "legal_score_no",
                "legal_person_status": "legal_status_txt",
                "legal_person_job_title": "legal_job_title_txt",
                "legal_source_id": "legal_src_ref",
                "legal_source_type": "legal_src_type",
                "date_of_constitution": "constitution_dt",
                "lead_converted_date": "lead_conv_dt",
                "pull_ts": "pull_ts",
                "origin_sys": "origin_sys",
            },
        ),
        "contact": _map_rows(
            read_rows(os.path.join(raw_dir, "contact_point.csv")),
            {
                "person_id": "party_ref",
                "contact_id": "contact_ref",
                "personal_email": "email_home_txt",
                "work_email": "email_work_txt",
                "work_phone": "phone_work_txt",
                "home_phone": "phone_home_txt",
                "pull_ts": "pull_ts",
                "origin_sys": "origin_sys",
            },
        ),
        "identity": _map_rows(
            read_rows(os.path.join(raw_dir, "identity_registry.csv")),
            {
                "person_id": "party_ref",
                "identities_id": "identity_ref",
                "ecid": "ecid_txt",
                "hashed_email": "hashed_email_txt",
                "pull_ts": "pull_ts",
                "origin_sys": "origin_sys",
            },
        ),
        "address": _map_rows(
            read_rows(os.path.join(raw_dir, "address_book.csv")),
            {
                "person_id": "party_ref",
                "address_id": "address_ref",
                "street": "street_txt",
                "postcode": "postal_cd",
                "city": "city_nm",
                "state": "state_cd",
                "country": "country_cd",
                "address_type": "address_type_txt",
                "region": "region_txt",
                "pull_ts": "pull_ts",
                "origin_sys": "origin_sys",
            },
        ),
        "lead": _map_rows(
            read_rows(os.path.join(raw_dir, "lead_register.csv")),
            {
                "person_id": "party_ref",
                "lead_id": "lead_ref",
                "interested_level": "interest_bucket",
                "preferred_contact_method": "contact_pref",
                "person_score": "person_score_no",
                "person_status": "person_status_txt",
                "converted_date": "converted_dt",
                "pull_ts": "pull_ts",
                "origin_sys": "origin_sys",
            },
        ),
        "customer": _map_rows(
            read_rows(os.path.join(raw_dir, "customer_portfolio.csv")),
            {
                "person_id": "party_ref",
                "customer_id": "customer_ref",
                "customer_number": "customer_no",
                "customer_status": "customer_status_txt",
                "customer_status_reason": "customer_status_reason_txt",
                "customer_since": "customer_since_dt",
                "customer_rating": "customer_rating_no",
                "customer_segment": "customer_segment_txt",
                "line_of_business": "lob_txt",
                "nps_score": "nps_score_no",
                "pull_ts": "pull_ts",
                "origin_sys": "origin_sys",
            },
        ),
        "customer_lead": _map_rows(
            read_rows(os.path.join(raw_dir, "customer_lead_bridge.csv")),
            {
                "customer_id": "customer_ref",
                "lead_id": "lead_ref",
                "pull_ts": "pull_ts",
                "origin_sys": "origin_sys",
            },
        ),
        "consent": _map_rows(
            read_rows(os.path.join(raw_dir, "consent_snapshot.csv")),
            {
                "person_id": "party_ref",
                "consent_id": "consent_ref",
                "opt_in_validated": "opt_in_valid_ind",
                "opt_in_legitimate_interest": "opt_in_legit_ind",
                "pull_ts": "pull_ts",
                "origin_sys": "origin_sys",
            },
        ),
        "mpr": _map_rows(
            read_rows(os.path.join(raw_dir, "comm_preference.csv")),
            {
                "person_id": "party_ref",
                "marketing_preference_id": "preference_ref",
                "sms": "sms_ind",
                "email": "email_ind",
                "email_subscriptions": "email_sub_ind",
                "call": "call_ind",
                "any": "any_ind",
                "commercial_email": "commercial_email_ind",
                "postal_mail": "postal_mail_ind",
                "pull_ts": "pull_ts",
                "origin_sys": "origin_sys",
            },
        ),
        "men": _map_rows(
            read_rows(os.path.join(raw_dir, "campaign_touch.csv")),
            {
                "person_id": "party_ref",
                "marketing_engagement_id": "engagement_ref",
                "promotion_code": "promo_cd",
                "opened_email": "email_opened_ind",
                "marketing_status": "campaign_status_txt",
                "pull_ts": "pull_ts",
                "origin_sys": "origin_sys",
            },
        ),
        "account": _map_rows(
            read_rows(os.path.join(raw_dir, "account_book.csv")),
            {
                "person_id": "party_ref",
                "account_id": "account_ref",
                "account_number": "account_no",
                "account_type": "account_type_txt",
                "account_last_access": "last_access_dt",
                "account_last_change": "last_change_dt",
                "account_creation_type": "account_create_type_txt",
                "account_status": "account_status_txt",
                "pull_ts": "pull_ts",
                "origin_sys": "origin_sys",
            },
        ),
        "product": _map_rows(
            read_rows(os.path.join(raw_dir, "product_catalog.csv")),
            {
                "product_id": "product_ref",
                "product_type": "product_line",
                "product_type_text": "product_type_txt",
                "underwriting_group": "underwriting_group_txt",
                "regulatory_approval_code": "regulatory_approval_cd",
                "product_status": "product_status_txt",
                "product_line_of_business_code": "product_lob_cd",
                "product_launch_date": "product_launch_dt",
                "pull_ts": "pull_ts",
                "origin_sys": "origin_sys",
            },
        ),
        "quote": _map_rows(
            read_rows(os.path.join(raw_dir, "quote_register.csv")),
            {
                "person_id": "party_ref",
                "quote_id": "quote_ref",
                "product_id": "product_ref",
                "gross_revenue": "gross_amt",
                "net_revenue": "net_amt",
                "quote_number": "quote_no",
                "quote_status": "quote_status_txt",
                "renewal_amt_current_period": "renewal_amt_curr",
                "renewal_amt_next_period": "renewal_amt_next",
                "pull_ts": "pull_ts",
                "origin_sys": "origin_sys",
            },
        ),
        "policy": _map_rows(
            read_rows(os.path.join(raw_dir, "policy_register.csv")),
            {
                "policy_id": "policy_ref",
                "customer_id": "customer_ref",
                "product_id": "product_ref",
                "cover_option": "cover_option_txt",
                "declined_claims": "declined_claim_cnt",
                "fraud_flag": "fraud_ind",
                "gross_revenue": "gross_amt",
                "net_revenue": "net_amt",
                "number_of_active_claim": "active_claim_cnt",
                "number_of_previous_claim": "previous_claim_cnt",
                "policy_cycle": "policy_cycle_no",
                "policy_end_date": "policy_end_dt",
                "policy_length": "policy_term_months",
                "policy_number": "policy_no",
                "policy_start_date": "policy_start_dt",
                "policy_status": "policy_status_txt",
                "renewal_amount_current_period": "renewal_premium_curr",
                "renewal_amount_next_period": "renewal_premium_next",
                "renewal_date": "renewal_dt",
                "sales_channel": "sales_channel_txt",
                "pull_ts": "pull_ts",
                "origin_sys": "origin_sys",
            },
        ),
        "home": _map_rows(
            read_rows(os.path.join(raw_dir, "property_asset.csv")),
            {
                "product_id": "product_ref",
                "home_id": "property_ref",
                "wall_construction": "wall_material_txt",
                "home_risk_address": "risk_address_txt",
                "roof_construction": "roof_material_txt",
                "home_type": "property_type_txt",
                "home_state": "property_state_cd",
                "is_existing_home_customer": "existing_home_ind",
                "pull_ts": "pull_ts",
                "origin_sys": "origin_sys",
            },
        ),
        "motor": _map_rows(
            read_rows(os.path.join(raw_dir, "vehicle_asset.csv")),
            {
                "product_id": "product_ref",
                "motor_id": "vehicle_ref",
                "auto_decline_vehicle": "auto_decline_ind",
                "body_type": "body_style_txt",
                "fuel_type": "fuel_type_txt",
                "license_status": "license_status_txt",
                "is_existing_motor_customer": "existing_motor_ind",
                "motor_lapsed_policies": "motor_lapse_cnt",
                "motor_risk_address": "garage_address_txt",
                "risk_class_code": "risk_class_cd",
                "variant": "variant_nm",
                "vehicle_owner_type": "owner_type_txt",
                "vehicle_regstate": "registration_state_cd",
                "vehicle_class": "vehicle_class_txt",
                "vehicle_model": "model_nm",
                "vehicle_type": "vehicle_type_txt",
                "motor_sum_insrd": "insured_value_amt",
                "vehicle_year": "manufacture_yr",
                "vehicle_age": "vehicle_age_yrs",
                "pull_ts": "pull_ts",
                "origin_sys": "origin_sys",
            },
        ),
    }


def plus_days(ts, days):
    if not ts:
        return ts
    from datetime import datetime, timedelta
    dt = datetime.fromisoformat(ts)
    dt = dt + timedelta(days=days)
    return dt.replace(microsecond=0).isoformat()


def hk_map(rows, field):
    return {r[field]: md5_hasher(r[field]) for r in rows if r.get(field)}


def hub_row(pk, bid_field, bid, load_date, record_source):
    return {pk: md5_hasher(bid), "load_date": load_date, "record_source": record_source, bid_field: bid}


def link_row(pk, left_name, left_value, right_name, right_value, load_date, record_source):
    return {pk: md5_hasher(f"{left_value}|{right_value}"), "load_date": load_date, "record_source": record_source, left_name: left_value, right_name: right_value}


def dedupe(rows, fields):
    seen = set()
    out = []
    for row in rows:
        key = tuple(row.get(f, "") for f in fields)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def ordered(rows, schema):
    return [{col: row.get(col, "") for col in schema} for row in rows]


def latest_subdir(base_dir):
    if not os.path.exists(base_dir):
        return None
    dirs = [
        os.path.join(base_dir, name)
        for name in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, name))
    ]
    if not dirs:
        return None
    return max(dirs, key=os.path.getmtime)


def build_silver(raw_dir, out_dir, hub_load_date=None, link_load_date=None, sat_load_date=None, source_type=None):
    if source_type is None:
        if os.path.exists(os.path.join(raw_dir, "person.jsonl")):
            source_type = "api"
        elif os.path.exists(os.path.join(raw_dir, "party_master.csv")):
            source_type = "prd1"
        else:
            source_type = "csv"

    if source_type == "api":
        raw = {
            "person": read_jsonl_rows(os.path.join(raw_dir, "person.jsonl")),
            "contact": read_jsonl_rows(os.path.join(raw_dir, "contact.jsonl")),
            "identity": read_jsonl_rows(os.path.join(raw_dir, "identity.jsonl")),
            "address": read_jsonl_rows(os.path.join(raw_dir, "person_address.jsonl")),
            "lead": read_jsonl_rows(os.path.join(raw_dir, "lead.jsonl")),
            "customer": read_jsonl_rows(os.path.join(raw_dir, "customer.jsonl")),
            "customer_lead": read_jsonl_rows(os.path.join(raw_dir, "customer_lead.jsonl")),
            "consent": read_jsonl_rows(os.path.join(raw_dir, "consent.jsonl")),
            "mpr": read_jsonl_rows(os.path.join(raw_dir, "marketing_preference.jsonl")),
            "men": read_jsonl_rows(os.path.join(raw_dir, "marketing_engagement.jsonl")),
            "account": read_jsonl_rows(os.path.join(raw_dir, "account.jsonl")),
            "product": read_jsonl_rows(os.path.join(raw_dir, "product.jsonl")),
            "quote": read_jsonl_rows(os.path.join(raw_dir, "quote.jsonl")),
            "policy": read_jsonl_rows(os.path.join(raw_dir, "policy.jsonl")),
            "home": read_jsonl_rows(os.path.join(raw_dir, "home.jsonl")),
            "motor": read_jsonl_rows(os.path.join(raw_dir, "motor.jsonl")),
        }
    elif source_type == "prd1":
        raw = read_prd1_raw(raw_dir)
    else:
        raw = {
            "person": read_rows(os.path.join(raw_dir, "crm_person.csv")),
            "contact": read_rows(os.path.join(raw_dir, "crm_contact.csv")),
            "identity": read_rows(os.path.join(raw_dir, "crm_identity.csv")),
            "address": read_rows(os.path.join(raw_dir, "crm_person_address.csv")),
            "lead": read_rows(os.path.join(raw_dir, "crm_lead.csv")),
            "customer": read_rows(os.path.join(raw_dir, "crm_customer.csv")),
            "customer_lead": read_rows(os.path.join(raw_dir, "crm_customer_lead.csv")),
            "consent": read_rows(os.path.join(raw_dir, "crm_consent.csv")),
            "mpr": read_rows(os.path.join(raw_dir, "crm_marketing_preference.csv")),
            "men": read_rows(os.path.join(raw_dir, "crm_marketing_engagement.csv")),
            "account": read_rows(os.path.join(raw_dir, "crm_account.csv")),
            "product": read_rows(os.path.join(raw_dir, "crm_product.csv")),
            "quote": read_rows(os.path.join(raw_dir, "crm_quote.csv")),
            "policy": read_rows(os.path.join(raw_dir, "crm_policy.csv")),
            "home": read_rows(os.path.join(raw_dir, "crm_home.csv")),
            "motor": read_rows(os.path.join(raw_dir, "crm_motor.csv")),
        }
    manifest = read_manifest(raw_dir)
    default_ts = first_extract_ts(raw)
    record_source = manifest.get("source_system") or first_record_source(raw, default="API" if source_type == "api" else "CRM")
    hub_load_date = hub_load_date or manifest.get("hub_load_date") or plus_days(default_ts, 7)
    link_load_date = link_load_date or manifest.get("link_load_date") or plus_days(default_ts, 12)
    sat_load_date = sat_load_date or manifest.get("sat_load_date") or plus_days(default_ts, 17)

    ids = {
        "person": hk_map(raw["person"], "person_id"),
        "nat": hk_map(raw["person"], "natural_person_id"),
        "leg": hk_map(raw["person"], "legal_person_id"),
        "contact": hk_map(raw["contact"], "contact_id"),
        "identity": hk_map(raw["identity"], "identities_id"),
        "address": hk_map(raw["address"], "address_id"),
        "lead": hk_map(raw["lead"], "lead_id"),
        "customer": hk_map(raw["customer"], "customer_id"),
        "consent": hk_map(raw["consent"], "consent_id"),
        "mpr": hk_map(raw["mpr"], "marketing_preference_id"),
        "men": hk_map(raw["men"], "marketing_engagement_id"),
        "account": hk_map(raw["account"], "account_id"),
        "product": hk_map(raw["product"], "product_id"),
        "quote": hk_map(raw["quote"], "quote_id"),
        "policy": hk_map(raw["policy"], "policy_id"),
        "home": hk_map(raw["home"], "home_id"),
        "motor": hk_map(raw["motor"], "motor_id"),
    }
    out = {name: [] for name in SCHEMAS}

    def add_hub(file_name, row, key):
        pk, bid_field = HUB_SCHEMAS[file_name]
        if row.get(key):
            out[file_name].append(hub_row(pk, bid_field, row[key], hub_load_date, record_source))

    for row in raw["person"]:
        pid = row.get("person_id")
        if not pid:
            continue
        add_hub("hub_person.csv", row, "person_id")
        out["sat_person.csv"].append({"person_hash_key": ids["person"][pid], "load_date": sat_load_date, "tenant_id": row.get("tenant_id", ""), "is_lead": row.get("is_lead", ""), "type": row.get("person_type", ""), "operational_paperless_consent": row.get("operational_paperless_consent", ""), "source_id": row.get("source_id", ""), "source_type": row.get("source_type", "")})
        if row.get("natural_person_id"):
            add_hub("hub_natural_person.csv", row, "natural_person_id")
            out["link_person_natural_person.csv"].append(link_row("person_natural_person_hash_key", "person_hash_key", ids["person"][pid], "natural_person_hash_key", ids["nat"][row["natural_person_id"]], link_load_date, record_source))
            out["sat_natural_person.csv"].append({"natural_person_hash_key": ids["nat"][row["natural_person_id"]], "load_date": sat_load_date, "first_name": row.get("first_name", ""), "last_name": row.get("last_name", ""), "full_name": row.get("full_name", ""), "courtesy_title": row.get("courtesy_title", ""), "occupation": row.get("occupation", ""), "birth_date": row.get("birth_date", ""), "birth_year": row.get("birth_year", ""), "nationality": row.get("nationality", ""), "gender": row.get("gender", ""), "marital_status": row.get("marital_status", ""), "assesed_disability_degree": row.get("assesed_disability_degree", ""), "preferred_language": row.get("preferred_language", ""), "role": row.get("role", ""), "job_title": row.get("job_title", "")})
        if row.get("legal_person_id"):
            add_hub("hub_legal_person.csv", row, "legal_person_id")
            out["link_person_legal_person.csv"].append(link_row("person_legal_person_hash_key", "person_hash_key", ids["person"][pid], "legal_person_hash_key", ids["leg"][row["legal_person_id"]], link_load_date, record_source))
            out["sat_legal_person.csv"].append({"legal_person_hash_key": ids["leg"][row["legal_person_id"]], "load_date": sat_load_date, "person_score": row.get("legal_person_score", ""), "job_title": row.get("legal_person_job_title", ""), "source_id": row.get("legal_source_id", ""), "source_type": row.get("legal_source_type", ""), "person_status": row.get("legal_person_status", ""), "converted_date": row.get("lead_converted_date", ""), "date_of_constitution": _timestamp_value(row.get("date_of_constitution", "")), "company_name": row.get("company_name", "")})

    simple = [
        ("contact", "hub_contact.csv", "contact_id", "person_id", "contact", "link_person_contact.csv", "person_contact_hash_key", "contact_hash_key", "sat_contact.csv", ["personal_email", "work_email", "work_phone", "home_phone"]),
        ("identity", "hub_identities.csv", "identities_id", "person_id", "identity", "link_person_identities.csv", "person_identities_hash_key", "identities_hash_key", "sat_identities.csv", ["ecid", "hashed_email"]),
        ("address", "hub_home_address.csv", "address_id", "person_id", "address", "link_person_home_address.csv", "person_home_address_hash_key", "home_address_hash_key", "sat_home_address.csv", ["street", "postcode", "city", "state", "country"]),
        ("lead", "hub_lead.csv", "lead_id", "person_id", "lead", "link_person_lead.csv", "person_lead_hash_key", "lead_hash_key", "sat_lead.csv", ["interested_level", "preferred_contact_method", "person_score", "person_status", "converted_date"]),
        ("consent", "hub_consent.csv", "consent_id", "person_id", "consent", "link_person_consent.csv", "person_consent_hash_key", "consent_hash_key", "sat_consent.csv", ["opt_in_validated", "opt_in_legitimate_interest"]),
        ("mpr", "hub_marketing_preference.csv", "marketing_preference_id", "person_id", "mpr", "link_person_marketing_preference.csv", "person_marketing_preference_hash_key", "marketing_preference_hash_key", "sat_marketing_preference.csv", ["sms", "email", "email_subscriptions", "call", "any", "commercial_email", "postal_mail"]),
        ("men", "hub_marketing_engagement.csv", "marketing_engagement_id", "person_id", "men", "link_person_marketing_engagement.csv", "person_marketing_engagement_hash_key", "marketing_engagement_hash_key", "sat_marketing_engagement.csv", ["promotion_code", "opened_email", "marketing_status"]),
        ("account", "hub_account.csv", "account_id", "person_id", "account", "link_person_account.csv", "person_account_hash_key", "account_hash_key", "sat_account.csv", ["account_number", "account_type", "account_last_access", "account_last_change", "account_creation_type", "account_status"]),
    ]
    for raw_name, hub_file, id_field, person_field, hk_name, link_file, link_pk, right_col, sat_file, sat_fields in simple:
        for row in raw[raw_name]:
            if not row.get(id_field) or not row.get(person_field):
                continue
            add_hub(hub_file, row, id_field)
            out[link_file].append(link_row(link_pk, "person_hash_key", ids["person"][row[person_field]], right_col, ids[hk_name][row[id_field]], link_load_date, record_source))
            sat_row = {SCHEMAS[sat_file][0]: ids[hk_name][row[id_field]], "load_date": sat_load_date}
            for field in sat_fields:
                sat_row[field] = row.get(field, "")
            out[sat_file].append(sat_row)

    for row in raw["customer"]:
        if not row.get("customer_id") or not row.get("person_id"):
            continue
        add_hub("hub_customer.csv", row, "customer_id")
        out["link_customer_person.csv"].append(link_row("customer_person_hash_key", "customer_hash_key", ids["customer"][row["customer_id"]], "person_hash_key", ids["person"][row["person_id"]], link_load_date, record_source))
        out["sat_customer.csv"].append({"customer_hash_key": ids["customer"][row["customer_id"]], "load_date": sat_load_date, "customer_number": row.get("customer_number", ""), "customer_status": row.get("customer_status", ""), "customer_status_reason": row.get("customer_status_reason", ""), "customer_since": row.get("customer_since", ""), "customer_rating": row.get("customer_rating", ""), "customer_segment": row.get("customer_segment", ""), "line_of_business": row.get("line_of_business", ""), "nps_score": row.get("nps_score", "")})
    for row in raw["customer_lead"]:
        if row.get("customer_id") and row.get("lead_id"):
            out["link_customer_lead.csv"].append(link_row("customer_lead_hash_key", "customer_hash_key", ids["customer"][row["customer_id"]], "lead_hash_key", ids["lead"][row["lead_id"]], link_load_date, record_source))

    add_quote_like = [
        ("product", "hub_product.csv", "product_id", "product", "sat_product.csv", {"type": "product_type"}),
        ("quote", "hub_quote.csv", "quote_id", "quote", "sat_quote.csv", {"gross_revenue": "gross_revenue", "net_revenue": "net_revenue", "quote_number": "quote_number", "quote_status": "quote_status", "renewal_amt_current_period": "renewal_amt_current_period", "renewal_amt_next_period": "renewal_amt_next_period"}),
        ("policy", "hub_policy.csv", "policy_id", "policy", "sat_policy.csv", {"cover_option": "cover_option", "declined_claims": "declined_claims", "fraud_flag": "fraud_flag", "gross_revenue": "gross_revenue", "net_revenue": "net_revenue", "number_of_active_claim": "number_of_active_claim", "number_of_previous_claim": "number_of_previous_claim", "policy_cycle": "policy_cycle", "policy_end_date": "policy_end_date", "policy_length": "policy_length", "policy_number": "policy_number", "policy_start_date": "policy_start_date", "policy_status": "policy_status", "renewal_amount_current_period": "renewal_amount_current_period", "renewal_amount_next_period": "renewal_amount_next_period", "renewal_date": "renewal_date", "sales_channel": "sales_channel"}),
        ("home", "hub_home.csv", "home_id", "home", "sat_home.csv", {"wall_construction": "wall_construction", "home_risk_address": "home_risk_address", "roof_construction": "roof_construction", "home_type": "home_type", "home_state": "home_state", "is_existing_home_customer": "is_existing_home_customer"}),
        ("motor", "hub_motor.csv", "motor_id", "motor", "sat_motor.csv", {"auto_decline_vehicle": "auto_decline_vehicle", "body_type": "body_type", "fuel_type": "fuel_type", "license_status": "license_status", "is_existing_motor_customer": "is_existing_motor_customer", "motor_lapsed_policies": "motor_lapsed_policies", "motor_risk_address": "motor_risk_address", "risk_class_code": "risk_class_code", "variant": "variant", "vehicle_owner_type": "vehicle_owner_type", "vehicle_regstate": "vehicle_regstate", "vehicle_class": "vehicle_class", "vehicle_model": "vehicle_model", "vehicle_type": "vehicle_type", "motor_sum_insrd": "motor_sum_insrd", "vehicle_year": "vehicle_year", "vehicle_age": "vehicle_age"}),
    ]
    for raw_name, hub_file, id_field, hk_name, sat_file, mapping in add_quote_like:
        for row in raw[raw_name]:
            if not row.get(id_field):
                continue
            add_hub(hub_file, row, id_field)
            sat_row = {SCHEMAS[sat_file][0]: ids[hk_name][row[id_field]], "load_date": sat_load_date}
            for out_field, in_field in mapping.items():
                sat_row[out_field] = row.get(in_field, "")
            out[sat_file].append(sat_row)

    for row in raw["quote"]:
        if row.get("quote_id") and row.get("person_id"):
            out["link_quote_person.csv"].append(link_row("quote_person_hash_key", "quote_hash_key", ids["quote"][row["quote_id"]], "person_hash_key", ids["person"][row["person_id"]], link_load_date, record_source))
        if row.get("quote_id") and row.get("product_id"):
            out["link_quote_product.csv"].append(link_row("quote_product_hash_key", "quote_hash_key", ids["quote"][row["quote_id"]], "product_hash_key", ids["product"][row["product_id"]], link_load_date, record_source))
    for row in raw["policy"]:
        if row.get("policy_id") and row.get("customer_id"):
            out["link_policy_customer.csv"].append(link_row("policy_customer_hash_key", "policy_hash_key", ids["policy"][row["policy_id"]], "customer_hash_key", ids["customer"][row["customer_id"]], link_load_date, record_source))
        if row.get("policy_id") and row.get("product_id"):
            out["link_policy_product.csv"].append(link_row("policy_customer_hash_key", "policy_hash_key", ids["policy"][row["policy_id"]], "product_hash_key", ids["product"][row["product_id"]], link_load_date, record_source))
    for row in raw["home"]:
        if row.get("home_id") and row.get("product_id"):
            out["link_product_home.csv"].append(link_row("product_home_hash_key", "product_hash_key", ids["product"][row["product_id"]], "home_hash_key", ids["home"][row["home_id"]], link_load_date, record_source))
    for row in raw["motor"]:
        if row.get("motor_id") and row.get("product_id"):
            out["link_product_motor.csv"].append(link_row("product_motor_hash_key", "product_hash_key", ids["product"][row["product_id"]], "motor_hash_key", ids["motor"][row["motor_id"]], link_load_date, record_source))

    for file_name, schema in SCHEMAS.items():
        key_fields = [field for field in schema if field.endswith("_id") or field.endswith("_hash_key")]
        write_csv(out_dir, file_name, ordered(dedupe(out[file_name], key_fields), schema), fieldnames=schema)
    return out_dir


def parse_args():
    parser = argparse.ArgumentParser(description="Build sample silver vault tables from raw CRM files.")
    parser.add_argument("raw_dir", nargs="?")
    parser.add_argument("out_dir", nargs="?")
    parser.add_argument("--hub-load-date")
    parser.add_argument("--link-load-date")
    parser.add_argument("--sat-load-date")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    raw_dir = args.raw_dir or latest_subdir(RAW_BASE)
    if not raw_dir:
        raise SystemExit(f"No raw batch folder found under {RAW_BASE}")

    out_dir = args.out_dir
    if not out_dir:
        out_dir = os.path.join(SAMPLE_SILVER_BASE, os.path.basename(raw_dir))

    print(build_silver(raw_dir, out_dir, args.hub_load_date, args.link_load_date, args.sat_load_date))
