import json
import os


def _as_list(value):
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _index_by(rows, key):
    return {row[key]: row for row in rows}


def _invert_single_map(mapping):
    return {value: key for key, value in mapping.items()}


def _invert_multi_map(mapping):
    out = {}
    for left, right_values in mapping.items():
        for right in _as_list(right_values):
            out[right] = left
    return out


def _stable_pick(person_hk, modulo=4):
    return int(person_hk[:8], 16) % modulo == 0


def _source_id(value, source_prefix="API"):
    if value is None:
        return None
    value = str(value)
    if not value:
        return None
    return f"{source_prefix}_{value}"


def _write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def write_raw_api_batch(base_folder, batch_id, ctx):
    out_dir = os.path.join(base_folder, "api", batch_id)
    extract_ts = ctx["extract_ts"]

    include_persons = set(ctx["person_hks"])

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
    person_by_lead_hk = _invert_multi_map(ctx["person_to_lead"])
    person_by_customer_hk = _invert_single_map(ctx["person_to_customer"])
    person_by_policy_hk = _invert_multi_map(ctx["policy_person_map"])
    person_by_addr_hk = _invert_multi_map(ctx["person_to_home_address"])

    selected_contact_hks = {hk for hk, person_hk in person_by_contact_hk.items() if person_hk in include_persons}
    selected_identity_hks = {hk for hk, person_hk in person_by_identity_hk.items() if person_hk in include_persons}
    selected_addr_hks = {hk for hk, person_hk in person_by_addr_hk.items() if person_hk in include_persons}
    selected_lead_hks = {hk for person_hk, hks in ctx["person_to_lead"].items() if person_hk in include_persons for hk in _as_list(hks)}
    selected_customer_hks = {hk for person_hk, hk in ctx["person_to_customer"].items() if person_hk in include_persons}
    selected_consent_hks = {hk for hk, person_hk in person_by_consent_hk.items() if person_hk in include_persons}
    selected_mpr_hks = {hk for hk, person_hk in person_by_mpr_hk.items() if person_hk in include_persons}
    selected_men_hks = {hk for hk, person_hk in person_by_men_hk.items() if person_hk in include_persons}
    selected_account_hks = {hk for hk, person_hk in person_by_account_hk.items() if person_hk in include_persons}
    selected_quote_hks = {hk for hk, person_hk in person_by_quote_hk.items() if person_hk in include_persons}
    selected_policy_hks = {hk for hk, person_hk in person_by_policy_hk.items() if person_hk in include_persons}
    selected_home_hks = {ctx["policy_to_home"][hk] for hk in selected_policy_hks if hk in ctx["policy_to_home"]}
    selected_motor_hks = {motor_hk for hk in selected_policy_hks if hk in ctx["policy_to_motor"] for motor_hk in _as_list(ctx["policy_to_motor"][hk])}
    selected_product_hks = {
        ctx["prod_hk_by_code"][code]
        for quote_hk, code in ctx["quote_to_product_id"].items()
        if quote_hk in selected_quote_hks
    }
    selected_product_hks.update(
        ctx["prod_hk_by_code"][code]
        for policy_hk, code in ctx["policy_to_product_id"].items()
        if policy_hk in selected_policy_hks
    )
    policy_by_home_hk = {home_hk: policy_hk for policy_hk, home_hk in ctx["policy_to_home"].items()}
    policy_by_motor_hk = {}
    for policy_hk, motor_hks in ctx["policy_to_motor"].items():
        for motor_hk in _as_list(motor_hks):
            policy_by_motor_hk[motor_hk] = policy_hk

    raw_files = {
        "person.jsonl": [],
        "contact.jsonl": [],
        "identity.jsonl": [],
        "person_address.jsonl": [],
        "lead.jsonl": [],
        "customer.jsonl": [],
        "customer_lead.jsonl": [],
        "consent.jsonl": [],
        "marketing_preference.jsonl": [],
        "marketing_engagement.jsonl": [],
        "account.jsonl": [],
        "product.jsonl": [],
        "quote.jsonl": [],
        "policy.jsonl": [],
        "home.jsonl": [],
        "motor.jsonl": [],
    }

    for person_hk in include_persons:
        hub_person = hub_person_by_hk[person_hk]
        sat_person = sat_person_by_hk.get(person_hk, {})
        nat_hk = ctx["person_to_nat"].get(person_hk)
        leg_hk = ctx["person_to_leg"].get(person_hk)
        raw_files["person.jsonl"].append({
            "_batch_id": batch_id,
            "_extract_ts": extract_ts,
            "_source_system": "API",
            "person_id": _source_id(hub_person["Person Id"]),
            "person_type": sat_person.get("Type"),
            "tenant_id": sat_person.get("Tenant Id"),
            "is_lead": sat_person.get("Is Lead"),
            "operational_paperless_consent": sat_person.get("Operational Paperless Consent"),
            "source_id": _source_id(sat_person.get("Source Id")),
            "source_type": sat_person.get("Source Type"),
            "natural_person_id": _source_id(hub_nat_by_hk.get(nat_hk, {}).get("Natural Person Id")) if nat_hk else None,
            "first_name": sat_nat_by_hk.get(nat_hk, {}).get("First Name") if nat_hk else None,
            "last_name": sat_nat_by_hk.get(nat_hk, {}).get("Last Name") if nat_hk else None,
            "full_name": sat_nat_by_hk.get(nat_hk, {}).get("Full Name") if nat_hk else None,
            "courtesy_title": sat_nat_by_hk.get(nat_hk, {}).get("Courtesy Title") if nat_hk else None,
            "occupation": sat_nat_by_hk.get(nat_hk, {}).get("Occupation") if nat_hk else None,
            "birth_date": sat_nat_by_hk.get(nat_hk, {}).get("Birth Date") if nat_hk else None,
            "birth_year": sat_nat_by_hk.get(nat_hk, {}).get("Birth Year") if nat_hk else None,
            "nationality": sat_nat_by_hk.get(nat_hk, {}).get("Nationality") if nat_hk else None,
            "gender": sat_nat_by_hk.get(nat_hk, {}).get("Gender") if nat_hk else None,
            "marital_status": sat_nat_by_hk.get(nat_hk, {}).get("Marital Status") if nat_hk else None,
            "assesed_disability_degree": sat_nat_by_hk.get(nat_hk, {}).get("Assesed Disability Degree") if nat_hk else None,
            "preferred_language": sat_nat_by_hk.get(nat_hk, {}).get("Preferred Language") if nat_hk else None,
            "role": sat_nat_by_hk.get(nat_hk, {}).get("Role") if nat_hk else None,
            "job_title": sat_nat_by_hk.get(nat_hk, {}).get("Job Title") if nat_hk else None,
            "legal_person_id": _source_id(hub_leg_by_hk.get(leg_hk, {}).get("Legal Person Id")) if leg_hk else None,
            "company_name": sat_leg_by_hk.get(leg_hk, {}).get("Company Name") if leg_hk else None,
            "legal_person_score": sat_leg_by_hk.get(leg_hk, {}).get("Person Score") if leg_hk else None,
            "legal_person_status": sat_leg_by_hk.get(leg_hk, {}).get("Person Status") if leg_hk else None,
            "legal_person_job_title": sat_leg_by_hk.get(leg_hk, {}).get("Job Title") if leg_hk else None,
            "legal_source_id": _source_id(sat_leg_by_hk.get(leg_hk, {}).get("Source Id")) if leg_hk else None,
            "legal_source_type": sat_leg_by_hk.get(leg_hk, {}).get("Source Type") if leg_hk else None,
            "date_of_constitution": sat_leg_by_hk.get(leg_hk, {}).get("Date of Constitution") if leg_hk else None,
            "lead_converted_date": sat_leg_by_hk.get(leg_hk, {}).get("Converted Date") if leg_hk else None,
        })

    for contact_hk in selected_contact_hks:
        raw_files["contact.jsonl"].append({
            "_batch_id": batch_id,
            "_extract_ts": extract_ts,
            "_source_system": "API",
            "person_id": _source_id(hub_person_by_hk[person_by_contact_hk[contact_hk]]["Person Id"]),
            "contact_id": _source_id(hub_contact_by_hk[contact_hk]["Contact Id"]),
            "personal_email": sat_contact_by_hk[contact_hk]["Personal Email"],
            "work_email": sat_contact_by_hk[contact_hk]["Work Email"],
            "work_phone": sat_contact_by_hk[contact_hk]["Work Phone"],
            "home_phone": sat_contact_by_hk[contact_hk]["Home Phone"],
        })

    for identity_hk in selected_identity_hks:
        raw_files["identity.jsonl"].append({
            "_batch_id": batch_id,
            "_extract_ts": extract_ts,
            "_source_system": "API",
            "person_id": _source_id(hub_person_by_hk[person_by_identity_hk[identity_hk]]["Person Id"]),
            "identities_id": _source_id(hub_identity_by_hk[identity_hk]["Identities Id"]),
            "ecid": sat_identity_by_hk[identity_hk]["ECID"],
            "hashed_email": sat_identity_by_hk[identity_hk]["Hashed Email"],
        })

    for addr_hk in selected_addr_hks:
        raw_files["person_address.jsonl"].append({
            "_batch_id": batch_id,
            "_extract_ts": extract_ts,
            "_source_system": "API",
            "person_id": _source_id(hub_person_by_hk[person_by_addr_hk[addr_hk]]["Person Id"]),
            "address_id": _source_id(hub_addr_by_hk[addr_hk]["Home Address Id"]),
            "street": sat_addr_by_hk[addr_hk]["Street"],
            "postcode": sat_addr_by_hk[addr_hk]["Postcode"],
            "city": sat_addr_by_hk[addr_hk]["City"],
            "state": sat_addr_by_hk[addr_hk]["State"],
            "country": sat_addr_by_hk[addr_hk]["Country"],
        })

    for lead_hk in selected_lead_hks:
        person_hk = person_by_lead_hk[lead_hk]
        raw_files["lead.jsonl"].append({
            "_batch_id": batch_id,
            "_extract_ts": extract_ts,
            "_source_system": "API",
            "person_id": _source_id(hub_person_by_hk[person_hk]["Person Id"]),
            "lead_id": _source_id(hub_lead_by_hk[lead_hk]["Lead Id"]),
            "interested_level": sat_lead_by_hk[lead_hk]["Interested Level"],
            "preferred_contact_method": sat_lead_by_hk[lead_hk]["Preferred Contact Method"],
            "person_score": sat_lead_by_hk[lead_hk]["Person Score"],
            "person_status": sat_lead_by_hk[lead_hk]["Person Status"],
            "converted_date": sat_lead_by_hk[lead_hk]["Converted Date"],
        })

    for customer_hk in selected_customer_hks:
        person_hk = person_by_customer_hk[customer_hk]
        raw_files["customer.jsonl"].append({
            "_batch_id": batch_id,
            "_extract_ts": extract_ts,
            "_source_system": "API",
            "person_id": _source_id(hub_person_by_hk[person_hk]["Person Id"]),
            "customer_id": _source_id(hub_customer_by_hk[customer_hk]["Customer Id"]),
            "customer_number": _source_id(sat_customer_by_hk[customer_hk]["Customer Number"]),
            "customer_status": sat_customer_by_hk[customer_hk]["Customer Status"],
            "customer_status_reason": sat_customer_by_hk[customer_hk]["Customer Status Reason"],
            "customer_since": sat_customer_by_hk[customer_hk]["Customer Since"],
            "customer_rating": sat_customer_by_hk[customer_hk]["Customer Rating"],
            "customer_segment": sat_customer_by_hk[customer_hk]["Customer Segment"],
            "line_of_business": sat_customer_by_hk[customer_hk]["Line Of Business"],
            "nps_score": sat_customer_by_hk[customer_hk]["NPS Score"],
        })

    for row in ctx["links"].get("Link_Customer_Lead", []):
        if row["Customer Hash Key"] in selected_customer_hks and row["Lead Hash Key"] in selected_lead_hks:
            raw_files["customer_lead.jsonl"].append({
                "_batch_id": batch_id,
                "_extract_ts": extract_ts,
                "_source_system": "API",
                "customer_id": _source_id(hub_customer_by_hk[row["Customer Hash Key"]]["Customer Id"]),
                "lead_id": _source_id(hub_lead_by_hk[row["Lead Hash Key"]]["Lead Id"]),
            })

    for consent_hk in selected_consent_hks:
        raw_files["consent.jsonl"].append({
            "_batch_id": batch_id,
            "_extract_ts": extract_ts,
            "_source_system": "API",
            "person_id": _source_id(hub_person_by_hk[person_by_consent_hk[consent_hk]]["Person Id"]),
            "consent_id": _source_id(hub_consent_by_hk[consent_hk]["Consent Id"]),
            "opt_in_validated": sat_consent_by_hk[consent_hk]["Opt In Validated"],
            "opt_in_legitimate_interest": sat_consent_by_hk[consent_hk]["Opt In Legitimate Interest"],
        })

    for mpr_hk in selected_mpr_hks:
        sat = sat_mpr_by_hk[mpr_hk]
        raw_files["marketing_preference.jsonl"].append({
            "_batch_id": batch_id,
            "_extract_ts": extract_ts,
            "_source_system": "API",
            "person_id": _source_id(hub_person_by_hk[person_by_mpr_hk[mpr_hk]]["Person Id"]),
            "marketing_preference_id": _source_id(hub_mpr_by_hk[mpr_hk]["Marketing Preference Id"]),
            "sms": sat["SMS"],
            "email": sat["Email"],
            "email_subscriptions": sat["Email Subscriptions"],
            "call": sat["Call"],
            "any": sat["Any"],
            "commercial_email": sat["Commercial Email"],
            "postal_mail": sat["Postal Mail"],
        })

    for men_hk in selected_men_hks:
        sat = sat_men_by_hk[men_hk]
        raw_files["marketing_engagement.jsonl"].append({
            "_batch_id": batch_id,
            "_extract_ts": extract_ts,
            "_source_system": "API",
            "person_id": _source_id(hub_person_by_hk[person_by_men_hk[men_hk]]["Person Id"]),
            "marketing_engagement_id": _source_id(hub_men_by_hk[men_hk]["Marketing Engagement Id"]),
            "promotion_code": sat["Promotion Code"],
            "opened_email": sat["Opened Email"],
            "marketing_status": sat["Marketing Status"],
        })

    for account_hk in selected_account_hks:
        sat = sat_account_by_hk[account_hk]
        raw_files["account.jsonl"].append({
            "_batch_id": batch_id,
            "_extract_ts": extract_ts,
            "_source_system": "API",
            "person_id": _source_id(hub_person_by_hk[person_by_account_hk[account_hk]]["Person Id"]),
            "account_id": _source_id(hub_account_by_hk[account_hk]["Account Id"]),
            "account_number": _source_id(sat["Account Number"]),
            "account_type": sat["Account Type"],
            "account_last_access": sat["Account Last Access"],
            "account_last_change": sat["Account Last Change"],
            "account_creation_type": sat["Account Creation Type"],
            "account_status": sat["Account Status"],
        })

    for product_hk in selected_product_hks:
        raw_files["product.jsonl"].append({
            "_batch_id": batch_id,
            "_extract_ts": extract_ts,
            "_source_system": "API",
            "product_id": _source_id(hub_product_by_hk[product_hk]["Product Id"]),
            "product_code": ctx["product_code_by_hk"][product_hk],
            "product_type": ctx["product_code_by_hk"][product_hk],
        })

    for quote_hk in selected_quote_hks:
        product_code = ctx["quote_to_product_id"][quote_hk]
        product_hk = ctx["prod_hk_by_code"][product_code]
        sat = sat_quote_by_hk[quote_hk]
        raw_files["quote.jsonl"].append({
            "_batch_id": batch_id,
            "_extract_ts": extract_ts,
            "_source_system": "API",
            "person_id": _source_id(hub_person_by_hk[person_by_quote_hk[quote_hk]]["Person Id"]),
            "quote_id": _source_id(hub_quote_by_hk[quote_hk]["Quote Id"]),
            "product_id": _source_id(hub_product_by_hk[product_hk]["Product Id"]),
            "product_code": product_code,
            "gross_revenue": sat["Gross Revenue"],
            "net_revenue": sat["Net Revenue"],
            "quote_number": _source_id(sat["Quote Number"]),
            "quote_status": sat["Quote Status"],
            "renewal_amt_current_period": sat["Renewal Amt Current Period"],
            "renewal_amt_next_period": sat["Renewal Amt Next Period"],
        })

    for policy_hk in selected_policy_hks:
        product_code = ctx["policy_to_product_id"][policy_hk]
        product_hk = ctx["prod_hk_by_code"][product_code]
        person_hk = person_by_policy_hk[policy_hk]
        customer_hk = ctx["person_to_customer"][person_hk]
        sat = sat_policy_by_hk[policy_hk]
        raw_files["policy.jsonl"].append({
            "_batch_id": batch_id,
            "_extract_ts": extract_ts,
            "_source_system": "API",
            "person_id": _source_id(hub_person_by_hk[person_hk]["Person Id"]),
            "customer_id": _source_id(hub_customer_by_hk[customer_hk]["Customer Id"]),
            "policy_id": _source_id(hub_policy_by_hk[policy_hk]["Policy Id"]),
            "quote_id": _source_id(hub_quote_by_hk[ctx["policy_to_quote_map"][policy_hk]]["Quote Id"]),
            "product_id": _source_id(hub_product_by_hk[product_hk]["Product Id"]),
            "product_code": product_code,
            "cover_option": sat["Cover Option"],
            "declined_claims": sat["Declined Claims"],
            "fraud_flag": sat["Fraud Flag"],
            "gross_revenue": sat["Gross Revenue"],
            "net_revenue": sat["Net Revenue"],
            "number_of_active_claim": sat["Number of Active Claim"],
            "number_of_previous_claim": sat["Number of Previous Claim"],
            "policy_cicle": sat["Policy Cicle"],
            "policy_end_date": sat["Policy End Date"],
            "policy_length": sat["Policy Length"],
            "policy_number": _source_id(sat["Policy Number"]),
            "policy_start_date": sat["Policy Start Date"],
            "policy_status": sat["Policy Status"],
            "renewal_amount_current_period": sat["Renewal Amount Current Period"],
            "renewal_amount_next_period": sat["Renewal Amount Next Period"],
            "renewal_date": sat["Renewal Date"],
            "sales_channel": sat["Sales Channel"],
        })

    for home_hk in selected_home_hks:
        policy_hk = policy_by_home_hk[home_hk]
        product_code = ctx["policy_to_product_id"][policy_hk]
        product_hk = ctx["prod_hk_by_code"][product_code]
        addr_hk = ctx["home_to_addr"][home_hk]
        sat_home = sat_home_by_hk[home_hk]
        sat_addr = sat_addr_by_hk[addr_hk]
        raw_files["home.jsonl"].append({
            "_batch_id": batch_id,
            "_extract_ts": extract_ts,
            "_source_system": "API",
            "policy_id": _source_id(hub_policy_by_hk[policy_hk]["Policy Id"]),
            "product_id": _source_id(hub_product_by_hk[product_hk]["Product Id"]),
            "product_code": product_code,
            "home_id": _source_id(hub_home_by_hk[home_hk]["Home Id"]),
            "wall_construction": sat_home["Wall Construction"],
            "home_risk_address": sat_home["Home Risk Address"],
            "roof_construction": sat_home["Roof Construction"],
            "home_type": sat_home["Home Type"],
            "home_state": sat_home["Home State"],
            "is_existing_home_customer": sat_home["Is Existing Home Customer"],
            "street": sat_addr["Street"],
            "postcode": sat_addr["Postcode"],
            "city": sat_addr["City"],
            "state": sat_addr["State"],
            "country": sat_addr["Country"],
        })

    for motor_hk in selected_motor_hks:
        policy_hk = policy_by_motor_hk[motor_hk]
        product_code = ctx["policy_to_product_id"][policy_hk]
        product_hk = ctx["prod_hk_by_code"][product_code]
        sat = sat_motor_by_hk[motor_hk]
        raw_files["motor.jsonl"].append({
            "_batch_id": batch_id,
            "_extract_ts": extract_ts,
            "_source_system": "API",
            "policy_id": _source_id(hub_policy_by_hk[policy_hk]["Policy Id"]),
            "product_id": _source_id(hub_product_by_hk[product_hk]["Product Id"]),
            "product_code": product_code,
            "motor_id": _source_id(hub_motor_by_hk[motor_hk]["Motor Id"]),
            "auto_decline_vehicle": sat["Auto Decline Vehicle"],
            "body_type": sat["Body Type"],
            "fuel_type": sat["Fuel Type"],
            "license_status": sat["License Status"],
            "is_existing_motor_customer": sat["Is Existing Motor Customer"],
            "motor_lapsed_policies": sat["Motor Lapsed Policies"],
            "motor_risk_address": sat["Motor Risk Address"],
            "risk_class_code": sat["Risk Class Code"],
            "variant": sat["Variant"],
            "vehicle_owner_type": sat["Vehicle Owner Type"],
            "vehicle_regstate": sat["Vehicle RegState"],
            "vehicle_class": sat["Vehicle Class"],
            "vehicle_model": sat["Vehicle Model"],
            "vehicle_type": sat["Vehicle Type"],
            "motor_sum_insrd": sat["Motor Sum Insrd"],
            "vehicle_year": sat["Vehicle Year"],
            "vehicle_age": sat["Vehicle Age"],
        })

    for file_name, rows in raw_files.items():
        _write_jsonl(os.path.join(out_dir, file_name), rows)

    return out_dir
