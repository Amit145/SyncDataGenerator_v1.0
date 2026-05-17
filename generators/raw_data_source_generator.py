from __future__ import annotations

import csv
import os
from collections import defaultdict

from config.storage_paths import RAW_DATA_SOURCE_ROOT
from helper.csv_writer import write_csv
from helper.scd2_diff_engine import latest_subdir


def _read_csv(path: str) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _index_by(rows: list[dict], key: str) -> dict[str, dict]:
    return {row.get(key, ""): row for row in rows if row.get(key)}


def _group_by(rows: list[dict], key: str) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[row.get(key, "")].append(row)
    return grouped


def _group_by_two(rows: list[dict], left_key: str, right_key: str) -> dict[tuple[str, str], dict]:
    grouped: dict[tuple[str, str], dict] = {}
    for row in rows:
        left = row.get(left_key, "")
        right = row.get(right_key, "")
        if left and right:
            grouped[(left, right)] = row
    return grouped


def _domain_from_product(product_code: str) -> str | None:
    code = (product_code or "").upper()
    if "MOTOR" in code:
        return "motor"
    if "HOME" in code or "PROPERTY" in code:
        return "home"
    return None


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


def _as_list(value):
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _invert_single_map(mapping):
    return {value: key for key, value in mapping.items()}


def _invert_multi_map(mapping):
    out = {}
    for left, right_values in mapping.items():
        for right in _as_list(right_values):
            out[right] = left
    return out


def _source_id(value, source_prefix="DS"):
    if value is None:
        return ""
    value = str(value)
    if not value:
        return ""
    return f"{source_prefix}_{value}"


def _generate_data_source_raw_from_ctx(run_id: str, ctx: dict) -> dict[str, str]:
    extract_ts = ctx.get("extract_ts", "")
    out_dirs = {}

    hub_person_by_hk = _index_by(ctx["hub_person_rows"], "Person Hash Key")
    hub_nat_by_hk = _index_by(ctx["hub_nat"], "Natural Person Hash Key")
    hub_leg_by_hk = _index_by(ctx["hub_leg"], "Legal Person Hash Key")
    hub_contact_by_hk = _index_by(ctx["hub_con_rows"], "Contact Hash Key")
    hub_identity_by_hk = _index_by(ctx["hub_id_rows"], "Identities Hash Key")
    hub_consent_by_hk = _index_by(ctx["hub_cns_rows"], "Consent Hash Key")
    hub_mpr_by_hk = _index_by(ctx["hub_mpr_rows"], "Marketing Preference Hash Key")
    hub_men_by_hk = _index_by(ctx["hub_men_rows"], "Marketing Engagement Hash Key")
    hub_account_by_hk = _index_by(ctx["hub_acc_rows"], "Account Hash Key")
    hub_customer_by_hk = _index_by(ctx["hub_cust_rows"], "Customer Hash Key")
    hub_lead_by_hk = _index_by(ctx["hub_lead_rows"], "Lead Hash Key")
    hub_quote_by_hk = _index_by(ctx["hub_quo_rows"], "Quote Hash Key")
    hub_policy_by_hk = _index_by(ctx["hub_pol_rows"], "Policy Hash Key")
    hub_product_by_hk = _index_by(ctx["hub_prod_rows"], "Product Hash Key")
    hub_motor_by_hk = _index_by(ctx["hub_mot_rows"], "Motor Hash Key")
    hub_home_by_hk = _index_by(ctx["hub_home_rows"], "Home Hash Key")
    hub_addr_by_hk = _index_by(ctx["hub_addr_rows"], "Home Address Hash Key")

    sat_person_by_hk = _index_by(ctx["sat_per"], "Person Hash Key")
    sat_nat_by_hk = _index_by(ctx["sat_nat"], "Natural Person Hash Key")
    sat_leg_by_hk = _index_by(ctx["sat_leg"], "Legal Person Hash Key")
    sat_contact_by_hk = _index_by(ctx["sat_con"], "Contact Hash Key")
    sat_identity_by_hk = _index_by(ctx["sat_eci"], "Identities Hash Key")
    sat_consent_by_hk = _index_by(ctx["sat_cns"], "Consent Hash Key")
    sat_mpr_by_hk = _index_by(ctx["sat_mpr"], "Marketing Preference Hash Key")
    sat_men_by_hk = _index_by(ctx["sat_men"], "Marketing Engagement Hash Key")
    sat_account_by_hk = _index_by(ctx["sat_acc"], "Account Hash Key")
    sat_customer_by_hk = _index_by(ctx["sat_cus"], "Customer Hash Key")
    sat_lead_by_hk = _index_by(ctx["sat_lea"], "Lead Hash Key")
    sat_quote_by_hk = _index_by(ctx["sat_quo"], "Quote Hash Key")
    sat_policy_by_hk = _index_by(ctx["sat_pol"], "Policy Hash Key")
    sat_home_by_hk = _index_by(ctx["sat_hom"], "Home Hash Key")
    sat_motor_by_hk = _index_by(ctx["sat_mot"], "Motor Hash Key")
    sat_addr_by_hk = _index_by(ctx["sat_adr"], "Home Address Hash Key")

    person_by_contact_hk = _invert_multi_map(ctx["person_to_contact"])
    person_by_identity_hk = _invert_multi_map(ctx["person_to_identity"])
    person_by_consent_hk = _invert_multi_map(ctx["person_to_consent"])
    person_by_mpr_hk = _invert_multi_map(ctx["person_to_mpr"])
    person_by_men_hk = _invert_multi_map(ctx["person_to_men"])
    person_by_account_hk = _invert_multi_map(ctx["person_to_account"])
    person_by_quote_hk = _invert_multi_map(ctx["person_to_quote"])
    person_by_customer_hk = _invert_single_map(ctx["person_to_customer"])
    person_by_policy_hk = _invert_multi_map(ctx["policy_person_map"])
    person_by_addr_hk = _invert_multi_map(ctx["person_to_home_address"])
    person_by_lead_hk = _invert_multi_map(ctx["person_to_lead"])

    product_hk_by_code = {code: hk for hk, code in ctx["product_code_by_hk"].items()}
    policy_by_home_hk = {home_hk: policy_hk for policy_hk, home_hk in ctx["policy_to_home"].items()}
    policy_by_motor_hk = {}
    for policy_hk, motor_hks in ctx["policy_to_motor"].items():
        for motor_hk in _as_list(motor_hks):
            policy_by_motor_hk[motor_hk] = policy_hk

    domains = {
        "motor": {"policy_rows": [], "asset_rows": [], "party_rows": []},
        "home": {"policy_rows": [], "asset_rows": [], "party_rows": []},
    }
    person_hks_by_domain = {"motor": set(), "home": set()}

    for policy_hk, hub_policy in hub_policy_by_hk.items():
        product_code = ctx["policy_to_product_id"].get(policy_hk, "")
        domain = _domain_from_product(product_code)
        if not domain:
            continue
        person_hk = person_by_policy_hk.get(policy_hk)
        if not person_hk:
            continue
        customer_hk = ctx["person_to_customer"].get(person_hk)
        quote_hk = ctx["policy_to_quote_map"].get(policy_hk)
        product_hk = product_hk_by_code.get(product_code)
        product = hub_product_by_hk.get(product_hk, {})
        sat_policy = sat_policy_by_hk.get(policy_hk, {})
        sat_quote = sat_quote_by_hk.get(quote_hk, {})
        person_hks_by_domain[domain].add(person_hk)

        domains[domain]["policy_rows"].append({
            "_batch_id": run_id,
            "_extract_ts": extract_ts,
            "_source_system": "DATA_SOURCE",
            "src_policy_ref": _source_id(hub_policy["Policy Id"]),
            "src_customer_ref": _source_id(hub_customer_by_hk.get(customer_hk, {}).get("Customer Id")),
            "src_party_ref": _source_id(hub_person_by_hk[person_hk]["Person Id"]),
            "src_quote_ref": _source_id(hub_quote_by_hk.get(quote_hk, {}).get("Quote Id")),
            "src_product_ref": _source_id(product.get("Product Id")),
            "lob_policy_code": product_code,
            "lob_policy_type": product_code,
            "coverage_tier": sat_policy.get("Cover Option", ""),
            "claim_decline_count": sat_policy.get("Declined Claims", ""),
            "fraud_ind": sat_policy.get("Fraud Flag", ""),
            "gross_written_premium": sat_policy.get("Gross Revenue", ""),
            "net_written_premium": sat_policy.get("Net Revenue", ""),
            "open_claim_count": sat_policy.get("Number of Active Claim", ""),
            "historical_claim_count": sat_policy.get("Number of Previous Claim", ""),
            "policy_cycle_no": sat_policy.get("Policy Cycle", ""),
            "policy_expiry_dt": sat_policy.get("Policy End Date", ""),
            "term_months": sat_policy.get("Policy Length", ""),
            "source_policy_no": _source_id(sat_policy.get("Policy Number", "")),
            "policy_inception_dt": sat_policy.get("Policy Start Date", ""),
            "policy_state": sat_policy.get("Policy Status", ""),
            "renewal_premium_current": sat_policy.get("Renewal Amount Current Period", ""),
            "renewal_premium_next": sat_policy.get("Renewal Amount Next Period", ""),
            "renewal_notice_dt": sat_policy.get("Renewal Date", ""),
            "distribution_channel": sat_policy.get("Sales Channel", ""),
            "source_quote_status": sat_quote.get("Quote Status", ""),
            "quoted_gwp": sat_quote.get("Gross Revenue", ""),
        })

    for motor_hk, hub_motor in hub_motor_by_hk.items():
        policy_hk = policy_by_motor_hk.get(motor_hk)
        if not policy_hk or _domain_from_product(ctx["policy_to_product_id"].get(policy_hk, "")) != "motor":
            continue
        product_hk = product_hk_by_code.get(ctx["policy_to_product_id"][policy_hk])
        sat_motor = sat_motor_by_hk.get(motor_hk, {})
        domains["motor"]["asset_rows"].append({
            "_batch_id": run_id,
            "_extract_ts": extract_ts,
            "_source_system": "DATA_SOURCE",
            "src_policy_ref": _source_id(hub_policy_by_hk[policy_hk]["Policy Id"]),
            "src_product_ref": _source_id(hub_product_by_hk.get(product_hk, {}).get("Product Id")),
            "vehicle_asset_ref": _source_id(hub_motor["Motor Id"]),
            "vehicle_body_style": sat_motor.get("Body Type", ""),
            "fuel_desc": sat_motor.get("Fuel Type", ""),
            "license_state": sat_motor.get("License Status", ""),
            "existing_motor_flag": sat_motor.get("Is Existing Motor Customer", ""),
            "lapsed_motor_policies": sat_motor.get("Motor Lapsed Policies", ""),
            "garage_risk_location": sat_motor.get("Motor Risk Address", ""),
            "risk_band_code": sat_motor.get("Risk Class Code", ""),
            "vehicle_variant_name": sat_motor.get("Variant", ""),
            "owner_type_desc": sat_motor.get("Vehicle Owner Type", ""),
            "registration_state": sat_motor.get("Vehicle RegState", ""),
            "vehicle_segment": sat_motor.get("Vehicle Class", ""),
            "vehicle_model_name": sat_motor.get("Vehicle Model", ""),
            "vehicle_usage_type": sat_motor.get("Vehicle Type", ""),
            "insured_value_amt": sat_motor.get("Motor Sum Insrd", ""),
            "manufacture_year": sat_motor.get("Vehicle Year", ""),
            "vehicle_age_years": sat_motor.get("Vehicle Age", ""),
        })

    for home_hk, hub_home in hub_home_by_hk.items():
        policy_hk = policy_by_home_hk.get(home_hk)
        if not policy_hk or _domain_from_product(ctx["policy_to_product_id"].get(policy_hk, "")) != "home":
            continue
        product_hk = product_hk_by_code.get(ctx["policy_to_product_id"][policy_hk])
        addr_hk = ctx["home_to_addr"].get(home_hk)
        sat_home = sat_home_by_hk.get(home_hk, {})
        sat_addr = sat_addr_by_hk.get(addr_hk, {})
        domains["home"]["asset_rows"].append({
            "_batch_id": run_id,
            "_extract_ts": extract_ts,
            "_source_system": "DATA_SOURCE",
            "src_policy_ref": _source_id(hub_policy_by_hk[policy_hk]["Policy Id"]),
            "src_product_ref": _source_id(hub_product_by_hk.get(product_hk, {}).get("Product Id")),
            "property_asset_ref": _source_id(hub_home["Home Id"]),
            "wall_material": sat_home.get("Wall Construction", ""),
            "risk_property_address": sat_home.get("Home Risk Address", ""),
            "roof_material": sat_home.get("Roof Construction", ""),
            "property_usage_type": sat_home.get("Home Type", ""),
            "property_state": sat_home.get("Home State", ""),
            "existing_home_flag": sat_home.get("Is Existing Home Customer", ""),
            "property_street": sat_addr.get("Street", ""),
            "property_postcode": sat_addr.get("Postcode", ""),
            "property_city": sat_addr.get("City", ""),
            "property_country": sat_addr.get("Country", ""),
        })

    for domain, person_hks in person_hks_by_domain.items():
        for person_hk in sorted(person_hks):
            hub_person = hub_person_by_hk[person_hk]
            sat_person = sat_person_by_hk.get(person_hk, {})
            nat_hk = ctx["person_to_nat"].get(person_hk)
            leg_hk = ctx["person_to_leg"].get(person_hk)
            sat_nat = sat_nat_by_hk.get(nat_hk, {})
            sat_leg = sat_leg_by_hk.get(leg_hk, {})
            contact_hk = next((hk for hk, owner in person_by_contact_hk.items() if owner == person_hk), None)
            identity_hk = next((hk for hk, owner in person_by_identity_hk.items() if owner == person_hk), None)
            addr_hk = next((hk for hk, owner in person_by_addr_hk.items() if owner == person_hk), None)
            lead_hks = _as_list(ctx["person_to_lead"].get(person_hk))
            lead_hk = lead_hks[0] if lead_hks else None
            customer_hk = ctx["person_to_customer"].get(person_hk)
            consent_hk = next((hk for hk, owner in person_by_consent_hk.items() if owner == person_hk), None)
            mpr_hk = next((hk for hk, owner in person_by_mpr_hk.items() if owner == person_hk), None)
            men_hk = next((hk for hk, owner in person_by_men_hk.items() if owner == person_hk), None)
            account_hk = next((hk for hk, owner in person_by_account_hk.items() if owner == person_hk), None)
            sat_contact = sat_contact_by_hk.get(contact_hk, {})
            sat_identity = sat_identity_by_hk.get(identity_hk, {})
            sat_addr = sat_addr_by_hk.get(addr_hk, {})
            sat_lead = sat_lead_by_hk.get(lead_hk, {})
            sat_customer = sat_customer_by_hk.get(customer_hk, {})
            sat_consent = sat_consent_by_hk.get(consent_hk, {})
            sat_mpr = sat_mpr_by_hk.get(mpr_hk, {})
            sat_men = sat_men_by_hk.get(men_hk, {})
            sat_account = sat_account_by_hk.get(account_hk, {})

            domains[domain]["party_rows"].append({
                "_batch_id": run_id,
                "_extract_ts": extract_ts,
                "_source_system": "DATA_SOURCE",
                "src_party_ref": _source_id(hub_person["Person Id"]),
                "party_category": sat_person.get("Type", ""),
                "tenant_code": sat_person.get("Tenant Id", ""),
                "lead_flag": sat_person.get("Is Lead", ""),
                "paperless_flag": sat_person.get("Operational Paperless Consent", ""),
                "origin_id": _source_id(sat_person.get("Source Id", "")),
                "origin_type": sat_person.get("Source Type", ""),
                "natural_party_ref": _source_id(hub_nat_by_hk.get(nat_hk, {}).get("Natural Person Id")),
                "given_name": sat_nat.get("First Name", ""),
                "family_name": sat_nat.get("Last Name", ""),
                "display_name": sat_nat.get("Full Name", ""),
                "title_text": sat_nat.get("Courtesy Title", ""),
                "occupation_desc": sat_nat.get("Occupation", ""),
                "dob": sat_nat.get("Birth Date", ""),
                "birth_year_no": sat_nat.get("Birth Year", ""),
                "nationality_desc": sat_nat.get("Nationality", ""),
                "gender_desc": sat_nat.get("Gender", ""),
                "marital_desc": sat_nat.get("Marital Status", ""),
                "language_pref": sat_nat.get("Preferred Language", ""),
                "role_desc": sat_nat.get("Role", ""),
                "job_title_desc": sat_nat.get("Job Title", ""),
                "legal_party_ref": _source_id(hub_leg_by_hk.get(leg_hk, {}).get("Legal Person Id")),
                "company_legal_name": sat_leg.get("Company Name", ""),
                "legal_score": sat_leg.get("Person Score", ""),
                "legal_status": sat_leg.get("Person Status", ""),
                "legal_job_title": sat_leg.get("Job Title", ""),
                "legal_origin_id": _source_id(sat_leg.get("Source Id", "")),
                "legal_origin_type": sat_leg.get("Source Type", ""),
                "constitution_dt": sat_leg.get("Date of Constitution", ""),
                "lead_converted_dt": sat_leg.get("Converted Date", ""),
                "contact_ref": _source_id(hub_contact_by_hk.get(contact_hk, {}).get("Contact Id")),
                "email_personal_txt": sat_contact.get("Personal Email", ""),
                "email_work_txt": sat_contact.get("Work Email", ""),
                "phone_work_txt": sat_contact.get("Work Phone", ""),
                "phone_home_txt": sat_contact.get("Home Phone", ""),
                "identity_ref": _source_id(hub_identity_by_hk.get(identity_hk, {}).get("Identities Id")),
                "ecid_txt": sat_identity.get("ECID", ""),
                "hashed_email_txt": sat_identity.get("Hashed Email", ""),
                "address_ref": _source_id(hub_addr_by_hk.get(addr_hk, {}).get("Home Address Id")),
                "street_line_txt": sat_addr.get("Street", ""),
                "postal_cd": sat_addr.get("Postcode", ""),
                "city_name": sat_addr.get("City", ""),
                "state_code": sat_addr.get("State", ""),
                "country_code": sat_addr.get("Country", ""),
                "lead_ref": _source_id(hub_lead_by_hk.get(lead_hk, {}).get("Lead Id")),
                "interest_bucket": sat_lead.get("Interested Level", ""),
                "contact_pref": sat_lead.get("Preferred Contact Method", ""),
                "lead_score_no": sat_lead.get("Person Score", ""),
                "lead_status": sat_lead.get("Person Status", ""),
                "lead_converted_date": sat_lead.get("Converted Date", ""),
                "customer_ref": _source_id(hub_customer_by_hk.get(customer_hk, {}).get("Customer Id")),
                "customer_no_txt": _source_id(sat_customer.get("Customer Number", "")),
                "customer_status_txt": sat_customer.get("Customer Status", ""),
                "customer_status_reason_txt": sat_customer.get("Customer Status Reason", ""),
                "customer_since_dt": sat_customer.get("Customer Since", ""),
                "customer_rating_no": sat_customer.get("Customer Rating", ""),
                "customer_segment_txt": sat_customer.get("Customer Segment", ""),
                "line_of_business_txt": sat_customer.get("Line Of Business", ""),
                "nps_score_no": sat_customer.get("NPS Score", ""),
                "customer_lead_ref": _source_id(hub_lead_by_hk.get(lead_hk, {}).get("Lead Id")),
                "consent_ref": _source_id(hub_consent_by_hk.get(consent_hk, {}).get("Consent Id")),
                "opt_in_valid_ind": sat_consent.get("Opt In Validated", ""),
                "opt_in_legitimate_ind": sat_consent.get("Opt In Legitimate Interest", ""),
                "mpr_ref": _source_id(hub_mpr_by_hk.get(mpr_hk, {}).get("Marketing Preference Id")),
                "sms_pref_ind": sat_mpr.get("SMS", ""),
                "email_pref_ind": sat_mpr.get("Email", ""),
                "email_sub_pref_ind": sat_mpr.get("Email Subscriptions", ""),
                "call_pref_ind": sat_mpr.get("Call", ""),
                "any_pref_ind": sat_mpr.get("Any", ""),
                "commercial_email_ind": sat_mpr.get("Commercial Email", ""),
                "postal_mail_ind": sat_mpr.get("Postal Mail", ""),
                "engagement_ref": _source_id(hub_men_by_hk.get(men_hk, {}).get("Marketing Engagement Id")),
                "promo_code_txt": sat_men.get("Promotion Code", ""),
                "opened_email_ind": sat_men.get("Opened Email", ""),
                "marketing_status_txt": sat_men.get("Marketing Status", ""),
                "account_ref": _source_id(hub_account_by_hk.get(account_hk, {}).get("Account Id")),
                "account_no_txt": _source_id(sat_account.get("Account Number", "")),
                "account_type_txt": sat_account.get("Account Type", ""),
                "last_access_dt": sat_account.get("Account Last Access", ""),
                "last_change_dt": sat_account.get("Account Last Change", ""),
                "account_create_type_txt": sat_account.get("Account Creation Type", ""),
                "account_status_txt": sat_account.get("Account Status", ""),
            })

    filenames = {
        "motor": {
            "party_rows": "motor_party_extract.csv",
            "policy_rows": "motor_policy_extract.csv",
            "asset_rows": "motor_vehicle_extract.csv",
        },
        "home": {
            "party_rows": "home_party_extract.csv",
            "policy_rows": "home_policy_extract.csv",
            "asset_rows": "home_property_extract.csv",
        },
    }
    dedupe_keys = {
        "party_rows": ["src_party_ref"],
        "policy_rows": ["src_policy_ref"],
        "asset_rows": ["src_policy_ref"],
    }
    for domain, content in domains.items():
        out_dir = os.path.join(RAW_DATA_SOURCE_ROOT, domain, run_id)
        out_dirs[domain] = out_dir
        os.makedirs(out_dir, exist_ok=True)
        for bucket, rows in content.items():
            rows = _dedupe(rows, dedupe_keys[bucket])
            fieldnames = list(rows[0].keys()) if rows else None
            write_csv(out_dir, filenames[domain][bucket], rows, fieldnames=fieldnames)
    return out_dirs


def generate_data_source_raw(run_id: str, crm_raw_dir: str | None = None, ctx: dict | None = None) -> dict[str, str]:
    if ctx is not None:
        return _generate_data_source_raw_from_ctx(run_id, ctx)

    if not crm_raw_dir:
        raise ValueError("crm_raw_dir is required when ctx is not provided")
    product_rows = _read_csv(os.path.join(crm_raw_dir, "crm_product.csv"))
    quote_rows = _read_csv(os.path.join(crm_raw_dir, "crm_quote.csv"))
    policy_rows = _read_csv(os.path.join(crm_raw_dir, "crm_policy.csv"))
    motor_rows = _read_csv(os.path.join(crm_raw_dir, "crm_motor.csv"))
    home_rows = _read_csv(os.path.join(crm_raw_dir, "crm_home.csv"))

    person_rows = _read_csv(os.path.join(crm_raw_dir, "crm_person.csv"))
    contact_rows = _read_csv(os.path.join(crm_raw_dir, "crm_contact.csv"))
    identity_rows = _read_csv(os.path.join(crm_raw_dir, "crm_identity.csv"))
    address_rows = _read_csv(os.path.join(crm_raw_dir, "crm_person_address.csv"))
    lead_rows = _read_csv(os.path.join(crm_raw_dir, "crm_lead.csv"))
    customer_rows = _read_csv(os.path.join(crm_raw_dir, "crm_customer.csv"))
    customer_lead_rows = _read_csv(os.path.join(crm_raw_dir, "crm_customer_lead.csv"))
    consent_rows = _read_csv(os.path.join(crm_raw_dir, "crm_consent.csv"))
    mpr_rows = _read_csv(os.path.join(crm_raw_dir, "crm_marketing_preference.csv"))
    men_rows = _read_csv(os.path.join(crm_raw_dir, "crm_marketing_engagement.csv"))
    account_rows = _read_csv(os.path.join(crm_raw_dir, "crm_account.csv"))
    extract_ts = ""
    for rows in (product_rows, quote_rows, policy_rows, person_rows):
        if rows:
            extract_ts = rows[0].get("_extract_ts", "")
            if extract_ts:
                break

    quotes_by_person_quote = _group_by_two(quote_rows, "person_id", "quote_id")
    leads_by_person = _group_by(lead_rows, "person_id")
    contacts_by_person = _group_by(contact_rows, "person_id")
    identities_by_person = _group_by(identity_rows, "person_id")
    addresses_by_person = _group_by(address_rows, "person_id")
    customers_by_person = _group_by(customer_rows, "person_id")
    consents_by_person = _group_by(consent_rows, "person_id")
    mprs_by_person = _group_by(mpr_rows, "person_id")
    mens_by_person = _group_by(men_rows, "person_id")
    accounts_by_person = _group_by(account_rows, "person_id")
    customer_leads_by_customer = _group_by(customer_lead_rows, "customer_id")

    products_by_id = _index_by(product_rows, "product_id")

    domains = {
        "motor": {
            "policy_rows": [],
            "asset_rows": [],
            "party_rows": [],
        },
        "home": {
            "policy_rows": [],
            "asset_rows": [],
            "party_rows": [],
        },
    }

    person_ids_by_domain = {"motor": set(), "home": set()}

    for row in policy_rows:
        domain = _domain_from_product(row.get("product_code", ""))
        if not domain:
            continue
        product = products_by_id.get(row.get("product_id", ""), {})
        quote = quotes_by_person_quote.get((row.get("person_id", ""), row.get("quote_id", "")), {})
        domains[domain]["policy_rows"].append({
            "_batch_id": run_id,
            "_extract_ts": extract_ts,
            "_source_system": "DATA_SOURCE",
            "src_policy_ref": row.get("policy_id", ""),
            "src_customer_ref": row.get("customer_id", ""),
            "src_party_ref": row.get("person_id", ""),
            "src_quote_ref": row.get("quote_id", ""),
            "src_product_ref": row.get("product_id", ""),
            "lob_policy_code": row.get("product_code", ""),
            "lob_policy_type": product.get("product_type", ""),
            "coverage_tier": row.get("cover_option", ""),
            "claim_decline_count": row.get("declined_claims", ""),
            "fraud_ind": row.get("fraud_flag", ""),
            "gross_written_premium": row.get("gross_revenue", ""),
            "net_written_premium": row.get("net_revenue", ""),
            "open_claim_count": row.get("number_of_active_claim", ""),
            "historical_claim_count": row.get("number_of_previous_claim", ""),
            "policy_cycle_no": row.get("policy_cycle", ""),
            "policy_expiry_dt": row.get("policy_end_date", ""),
            "term_months": row.get("policy_length", ""),
            "source_policy_no": row.get("policy_number", ""),
            "policy_inception_dt": row.get("policy_start_date", ""),
            "policy_state": row.get("policy_status", ""),
            "renewal_premium_current": row.get("renewal_amount_current_period", ""),
            "renewal_premium_next": row.get("renewal_amount_next_period", ""),
            "renewal_notice_dt": row.get("renewal_date", ""),
            "distribution_channel": row.get("sales_channel", ""),
            "source_quote_status": quote.get("quote_status", ""),
            "quoted_gwp": quote.get("gross_revenue", ""),
        })
        person_ids_by_domain[domain].add(row.get("person_id", ""))

    for row in motor_rows:
        domains["motor"]["asset_rows"].append({
            "_batch_id": run_id,
            "_extract_ts": extract_ts,
            "_source_system": "DATA_SOURCE",
            "src_policy_ref": row.get("policy_id", ""),
            "src_product_ref": row.get("product_id", ""),
            "vehicle_asset_ref": row.get("motor_id", ""),
            "vehicle_body_style": row.get("body_type", ""),
            "fuel_desc": row.get("fuel_type", ""),
            "license_state": row.get("license_status", ""),
            "existing_motor_flag": row.get("is_existing_motor_customer", ""),
            "lapsed_motor_policies": row.get("motor_lapsed_policies", ""),
            "garage_risk_location": row.get("motor_risk_address", ""),
            "risk_band_code": row.get("risk_class_code", ""),
            "vehicle_variant_name": row.get("variant", ""),
            "owner_type_desc": row.get("vehicle_owner_type", ""),
            "registration_state": row.get("vehicle_regstate", ""),
            "vehicle_segment": row.get("vehicle_class", ""),
            "vehicle_model_name": row.get("vehicle_model", ""),
            "vehicle_usage_type": row.get("vehicle_type", ""),
            "insured_value_amt": row.get("motor_sum_insrd", ""),
            "manufacture_year": row.get("vehicle_year", ""),
            "vehicle_age_years": row.get("vehicle_age", ""),
        })

    for row in home_rows:
        domains["home"]["asset_rows"].append({
            "_batch_id": run_id,
            "_extract_ts": extract_ts,
            "_source_system": "DATA_SOURCE",
            "src_policy_ref": row.get("policy_id", ""),
            "src_product_ref": row.get("product_id", ""),
            "property_asset_ref": row.get("home_id", ""),
            "wall_material": row.get("wall_construction", ""),
            "risk_property_address": row.get("home_risk_address", ""),
            "roof_material": row.get("roof_construction", ""),
            "property_usage_type": row.get("home_type", ""),
            "property_state": row.get("home_state", ""),
            "existing_home_flag": row.get("is_existing_home_customer", ""),
            "property_street": row.get("street", ""),
            "property_postcode": row.get("postcode", ""),
            "property_city": row.get("city", ""),
            "property_country": row.get("country", ""),
        })

    person_by_id = _index_by(person_rows, "person_id")
    for domain, person_ids in person_ids_by_domain.items():
        for person_id in sorted(person_ids):
            person = person_by_id.get(person_id, {})
            customer = customers_by_person.get(person_id, [{}])[0]
            contact = contacts_by_person.get(person_id, [{}])[0]
            identity = identities_by_person.get(person_id, [{}])[0]
            address = addresses_by_person.get(person_id, [{}])[0]
            lead = leads_by_person.get(person_id, [{}])[0]
            consent = consents_by_person.get(person_id, [{}])[0]
            mpr = mprs_by_person.get(person_id, [{}])[0]
            men = mens_by_person.get(person_id, [{}])[0]
            account = accounts_by_person.get(person_id, [{}])[0]
            customer_lead = customer_leads_by_customer.get(customer.get("customer_id", ""), [{}])[0]

            domains[domain]["party_rows"].append({
                "_batch_id": run_id,
                "_extract_ts": extract_ts,
                "_source_system": "DATA_SOURCE",
                "src_party_ref": person_id,
                "party_category": person.get("person_type", ""),
                "tenant_code": person.get("tenant_id", ""),
                "lead_flag": person.get("is_lead", ""),
                "paperless_flag": person.get("operational_paperless_consent", ""),
                "origin_id": person.get("source_id", ""),
                "origin_type": person.get("source_type", ""),
                "natural_party_ref": person.get("natural_person_id", ""),
                "given_name": person.get("first_name", ""),
                "family_name": person.get("last_name", ""),
                "display_name": person.get("full_name", ""),
                "title_text": person.get("courtesy_title", ""),
                "occupation_desc": person.get("occupation", ""),
                "dob": person.get("birth_date", ""),
                "birth_year_no": person.get("birth_year", ""),
                "nationality_desc": person.get("nationality", ""),
                "gender_desc": person.get("gender", ""),
                "marital_desc": person.get("marital_status", ""),
                "language_pref": person.get("preferred_language", ""),
                "role_desc": person.get("role", ""),
                "job_title_desc": person.get("job_title", ""),
                "legal_party_ref": person.get("legal_person_id", ""),
                "company_legal_name": person.get("company_name", ""),
                "legal_score": person.get("legal_person_score", ""),
                "legal_status": person.get("legal_person_status", ""),
                "legal_job_title": person.get("legal_person_job_title", ""),
                "legal_origin_id": person.get("legal_source_id", ""),
                "legal_origin_type": person.get("legal_source_type", ""),
                "constitution_dt": person.get("date_of_constitution", ""),
                "lead_converted_dt": person.get("lead_converted_date", ""),
                "contact_ref": contact.get("contact_id", ""),
                "email_personal_txt": contact.get("personal_email", ""),
                "email_work_txt": contact.get("work_email", ""),
                "phone_work_txt": contact.get("work_phone", ""),
                "phone_home_txt": contact.get("home_phone", ""),
                "identity_ref": identity.get("identities_id", ""),
                "ecid_txt": identity.get("ecid", ""),
                "hashed_email_txt": identity.get("hashed_email", ""),
                "address_ref": address.get("address_id", ""),
                "street_line_txt": address.get("street", ""),
                "postal_cd": address.get("postcode", ""),
                "city_name": address.get("city", ""),
                "state_code": address.get("state", ""),
                "country_code": address.get("country", ""),
                "lead_ref": lead.get("lead_id", ""),
                "interest_bucket": lead.get("interested_level", ""),
                "contact_pref": lead.get("preferred_contact_method", ""),
                "lead_score_no": lead.get("person_score", ""),
                "lead_status": lead.get("person_status", ""),
                "lead_converted_date": lead.get("converted_date", ""),
                "customer_ref": customer.get("customer_id", ""),
                "customer_no_txt": customer.get("customer_number", ""),
                "customer_status_txt": customer.get("customer_status", ""),
                "customer_status_reason_txt": customer.get("customer_status_reason", ""),
                "customer_since_dt": customer.get("customer_since", ""),
                "customer_rating_no": customer.get("customer_rating", ""),
                "customer_segment_txt": customer.get("customer_segment", ""),
                "line_of_business_txt": customer.get("line_of_business", ""),
                "nps_score_no": customer.get("nps_score", ""),
                "customer_lead_ref": customer_lead.get("lead_id", ""),
                "consent_ref": consent.get("consent_id", ""),
                "opt_in_valid_ind": consent.get("opt_in_validated", ""),
                "opt_in_legitimate_ind": consent.get("opt_in_legitimate_interest", ""),
                "mpr_ref": mpr.get("marketing_preference_id", ""),
                "sms_pref_ind": mpr.get("sms", ""),
                "email_pref_ind": mpr.get("email", ""),
                "email_sub_pref_ind": mpr.get("email_subscriptions", ""),
                "call_pref_ind": mpr.get("call", ""),
                "any_pref_ind": mpr.get("any", ""),
                "commercial_email_ind": mpr.get("commercial_email", ""),
                "postal_mail_ind": mpr.get("postal_mail", ""),
                "engagement_ref": men.get("marketing_engagement_id", ""),
                "promo_code_txt": men.get("promotion_code", ""),
                "opened_email_ind": men.get("opened_email", ""),
                "marketing_status_txt": men.get("marketing_status", ""),
                "account_ref": account.get("account_id", ""),
                "account_no_txt": account.get("account_number", ""),
                "account_type_txt": account.get("account_type", ""),
                "last_access_dt": account.get("account_last_access", ""),
                "last_change_dt": account.get("account_last_change", ""),
                "account_create_type_txt": account.get("account_creation_type", ""),
                "account_status_txt": account.get("account_status", ""),
            })

    output_dirs = {}
    schemas = {
        "party_rows": None,
        "policy_rows": None,
        "asset_rows": None,
    }
    filenames = {
        "motor": {
            "party_rows": "motor_party_extract.csv",
            "policy_rows": "motor_policy_extract.csv",
            "asset_rows": "motor_vehicle_extract.csv",
        },
        "home": {
            "party_rows": "home_party_extract.csv",
            "policy_rows": "home_policy_extract.csv",
            "asset_rows": "home_property_extract.csv",
        },
    }
    dedupe_keys = {
        "party_rows": ["src_party_ref"],
        "policy_rows": ["src_policy_ref"],
        "asset_rows": ["src_policy_ref"],
    }

    for domain, content in domains.items():
        out_dir = os.path.join(RAW_DATA_SOURCE_ROOT, domain, run_id)
        output_dirs[domain] = out_dir
        os.makedirs(out_dir, exist_ok=True)
        for bucket, rows in content.items():
            rows = _dedupe(rows, dedupe_keys[bucket])
            fieldnames = list(rows[0].keys()) if rows else schemas[bucket]
            write_csv(out_dir, filenames[domain][bucket], rows, fieldnames=fieldnames)

    return output_dirs


def latest_crm_raw() -> str | None:
    return latest_subdir(os.path.join(os.path.dirname(RAW_DATA_SOURCE_ROOT), "crm"))
