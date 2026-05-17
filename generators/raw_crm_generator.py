import os

from helper.crm_raw_layout import to_crm_raw_column, to_crm_raw_file
from helper.csv_writer import write_csv
from helper.key_factory import get_now_iso


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


def _with_meta(row, batch_id, extract_ts, source_system):
    return {
        "_batch_id": batch_id,
        "_extract_ts": extract_ts,
        "_source_system": source_system,
        **row,
    }


def _safe_get(index, key, column):
    row = index.get(key)
    return row.get(column) if row else None


def write_raw_crm_batch(base_folder, batch_id, ctx, source_dir_name="crm", source_system="CRM"):
    out_dir = os.path.join(base_folder, source_dir_name, batch_id)
    extract_ts = ctx.get("extract_ts") or get_now_iso()

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

    product_code_by_hk = ctx["product_code_by_hk"]
    product_hk_by_code = {code: hk for hk, code in product_code_by_hk.items()}

    nat_hk_by_person = dict(ctx["person_to_nat"])
    leg_hk_by_person = dict(ctx["person_to_leg"])
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
    customer_by_person_hk = ctx["person_to_customer"]

    raw_rows = {
        "crm_person.csv": [],
        "crm_contact.csv": [],
        "crm_identity.csv": [],
        "crm_person_address.csv": [],
        "crm_lead.csv": [],
        "crm_customer.csv": [],
        "crm_customer_lead.csv": [],
        "crm_consent.csv": [],
        "crm_marketing_preference.csv": [],
        "crm_marketing_engagement.csv": [],
        "crm_account.csv": [],
        "crm_product.csv": [],
        "crm_quote.csv": [],
        "crm_policy.csv": [],
        "crm_home.csv": [],
        "crm_motor.csv": [],
    }

    schemas = {
        "crm_person.csv": [
            "_batch_id", "_extract_ts", "_source_system", "person_id", "person_type",
            "tenant_id", "is_lead", "operational_paperless_consent", "source_id",
            "source_type", "natural_person_id", "first_name", "last_name", "full_name",
            "courtesy_title", "occupation", "birth_date", "birth_year", "nationality",
            "gender", "marital_status", "assesed_disability_degree", "preferred_language",
            "role", "job_title", "legal_person_id", "company_name", "legal_person_score",
            "legal_person_status", "legal_person_job_title", "legal_source_id",
            "legal_source_type", "date_of_constitution", "lead_converted_date",
        ],
        "crm_contact.csv": [
            "_batch_id", "_extract_ts", "_source_system", "person_id", "contact_id",
            "personal_email", "work_email", "work_phone", "home_phone",
        ],
        "crm_identity.csv": [
            "_batch_id", "_extract_ts", "_source_system", "person_id", "identities_id",
            "ecid", "hashed_email",
        ],
        "crm_person_address.csv": [
            "_batch_id", "_extract_ts", "_source_system", "person_id", "address_id",
            "street", "postcode", "city", "state", "country",
        ],
        "crm_lead.csv": [
            "_batch_id", "_extract_ts", "_source_system", "person_id", "lead_id",
            "interested_level", "preferred_contact_method", "person_score",
            "person_status", "converted_date",
        ],
        "crm_customer.csv": [
            "_batch_id", "_extract_ts", "_source_system", "person_id", "customer_id",
            "customer_number", "customer_status", "customer_status_reason",
            "customer_since", "customer_rating", "customer_segment",
            "line_of_business", "nps_score",
        ],
        "crm_customer_lead.csv": [
            "_batch_id", "_extract_ts", "_source_system", "customer_id", "lead_id",
        ],
        "crm_consent.csv": [
            "_batch_id", "_extract_ts", "_source_system", "person_id", "consent_id",
            "opt_in_validated", "opt_in_legitimate_interest",
        ],
        "crm_marketing_preference.csv": [
            "_batch_id", "_extract_ts", "_source_system", "person_id",
            "marketing_preference_id", "sms", "email", "email_subscriptions", "call",
            "any", "commercial_email", "postal_mail",
        ],
        "crm_marketing_engagement.csv": [
            "_batch_id", "_extract_ts", "_source_system", "person_id",
            "marketing_engagement_id", "promotion_code", "opened_email",
            "marketing_status",
        ],
        "crm_account.csv": [
            "_batch_id", "_extract_ts", "_source_system", "person_id", "account_id",
            "account_number", "account_type", "account_last_access",
            "account_last_change", "account_creation_type", "account_status",
        ],
        "crm_product.csv": [
            "_batch_id", "_extract_ts", "_source_system", "product_id", "product_code",
            "product_type",
        ],
        "crm_quote.csv": [
            "_batch_id", "_extract_ts", "_source_system", "person_id", "quote_id",
            "product_id", "product_code", "gross_revenue", "net_revenue", "quote_number",
            "quote_status", "renewal_amt_current_period", "renewal_amt_next_period",
        ],
        "crm_policy.csv": [
            "_batch_id", "_extract_ts", "_source_system", "person_id", "customer_id",
            "policy_id", "quote_id", "product_id", "product_code", "cover_option",
            "declined_claims", "fraud_flag", "gross_revenue", "net_revenue",
            "number_of_active_claim", "number_of_previous_claim", "policy_cycle",
            "policy_end_date", "policy_length", "policy_number", "policy_start_date",
            "policy_status", "renewal_amount_current_period",
            "renewal_amount_next_period", "renewal_date", "sales_channel",
        ],
        "crm_home.csv": [
            "_batch_id", "_extract_ts", "_source_system", "policy_id", "product_id",
            "product_code", "home_id", "wall_construction", "home_risk_address",
            "roof_construction", "home_type", "home_state",
            "is_existing_home_customer", "street", "postcode", "city", "state",
            "country",
        ],
        "crm_motor.csv": [
            "_batch_id", "_extract_ts", "_source_system", "policy_id", "product_id",
            "product_code", "motor_id", "auto_decline_vehicle", "body_type",
            "fuel_type", "license_status", "is_existing_motor_customer",
            "motor_lapsed_policies", "motor_risk_address", "risk_class_code",
            "variant", "vehicle_owner_type", "vehicle_regstate", "vehicle_class",
            "vehicle_model", "vehicle_type", "motor_sum_insrd", "vehicle_year",
            "vehicle_age",
        ],
    }

    for person_hk, hub_person in hub_person_by_hk.items():
        sat_person = sat_person_by_hk.get(person_hk, {})
        nat_hk = nat_hk_by_person.get(person_hk)
        leg_hk = leg_hk_by_person.get(person_hk)
        sat_nat = sat_nat_by_hk.get(nat_hk, {})
        sat_leg = sat_leg_by_hk.get(leg_hk, {})

        raw_rows["crm_person.csv"].append(_with_meta({
            "person_id": hub_person["Person Id"],
            "person_type": sat_person.get("Type"),
            "tenant_id": sat_person.get("Tenant Id"),
            "is_lead": sat_person.get("Is Lead"),
            "operational_paperless_consent": sat_person.get("Operational Paperless Consent"),
            "source_id": sat_person.get("Source Id"),
            "source_type": sat_person.get("Source Type"),
            "natural_person_id": _safe_get(hub_nat_by_hk, nat_hk, "Natural Person Id"),
            "first_name": sat_nat.get("First Name"),
            "last_name": sat_nat.get("Last Name"),
            "full_name": sat_nat.get("Full Name"),
            "courtesy_title": sat_nat.get("Courtesy Title"),
            "occupation": sat_nat.get("Occupation"),
            "birth_date": sat_nat.get("Birth Date"),
            "birth_year": sat_nat.get("Birth Year"),
            "nationality": sat_nat.get("Nationality"),
            "gender": sat_nat.get("Gender"),
            "marital_status": sat_nat.get("Marital Status"),
            "assesed_disability_degree": sat_nat.get("Assesed Disability Degree"),
            "preferred_language": sat_nat.get("Preferred Language"),
            "role": sat_nat.get("Role"),
            "job_title": sat_nat.get("Job Title"),
            "legal_person_id": _safe_get(hub_leg_by_hk, leg_hk, "Legal Person Id"),
            "company_name": sat_leg.get("Company Name"),
            "legal_person_score": sat_leg.get("Person Score"),
            "legal_person_status": sat_leg.get("Person Status"),
            "legal_person_job_title": sat_leg.get("Job Title"),
            "legal_source_id": sat_leg.get("Source Id"),
            "legal_source_type": sat_leg.get("Source Type"),
            "date_of_constitution": sat_leg.get("Date of Constitution"),
            "lead_converted_date": sat_leg.get("Converted Date"),
        }, batch_id, extract_ts, source_system))

    for contact_hk, hub_contact in hub_contact_by_hk.items():
        person_hk = person_by_contact_hk.get(contact_hk)
        raw_rows["crm_contact.csv"].append(_with_meta({
            "person_id": _safe_get(hub_person_by_hk, person_hk, "Person Id"),
            "contact_id": hub_contact["Contact Id"],
            "personal_email": _safe_get(sat_contact_by_hk, contact_hk, "Personal Email"),
            "work_email": _safe_get(sat_contact_by_hk, contact_hk, "Work Email"),
            "work_phone": _safe_get(sat_contact_by_hk, contact_hk, "Work Phone"),
            "home_phone": _safe_get(sat_contact_by_hk, contact_hk, "Home Phone"),
        }, batch_id, extract_ts, source_system))

    for identity_hk, hub_identity in hub_identity_by_hk.items():
        person_hk = person_by_identity_hk.get(identity_hk)
        raw_rows["crm_identity.csv"].append(_with_meta({
            "person_id": _safe_get(hub_person_by_hk, person_hk, "Person Id"),
            "identities_id": hub_identity["Identities Id"],
            "ecid": _safe_get(sat_identity_by_hk, identity_hk, "ECID"),
            "hashed_email": _safe_get(sat_identity_by_hk, identity_hk, "Hashed Email"),
        }, batch_id, extract_ts, source_system))

    for addr_hk, hub_addr in hub_addr_by_hk.items():
        person_hk = person_by_addr_hk.get(addr_hk)
        sat_addr = sat_addr_by_hk.get(addr_hk, {})
        raw_rows["crm_person_address.csv"].append(_with_meta({
            "person_id": _safe_get(hub_person_by_hk, person_hk, "Person Id"),
            "address_id": hub_addr["Home Address Id"],
            "street": sat_addr.get("Street"),
            "postcode": sat_addr.get("Postcode"),
            "city": sat_addr.get("City"),
            "state": sat_addr.get("State"),
            "country": sat_addr.get("Country"),
        }, batch_id, extract_ts, source_system))

    for person_hk, lead_hks in ctx["person_to_lead"].items():
        for lead_hk in _as_list(lead_hks):
            raw_rows["crm_lead.csv"].append(_with_meta({
                "person_id": _safe_get(hub_person_by_hk, person_hk, "Person Id"),
                "lead_id": _safe_get(hub_lead_by_hk, lead_hk, "Lead Id"),
                "interested_level": _safe_get(sat_lead_by_hk, lead_hk, "Interested Level"),
                "preferred_contact_method": _safe_get(sat_lead_by_hk, lead_hk, "Preferred Contact Method"),
                "person_score": _safe_get(sat_lead_by_hk, lead_hk, "Person Score"),
                "person_status": _safe_get(sat_lead_by_hk, lead_hk, "Person Status"),
                "converted_date": _safe_get(sat_lead_by_hk, lead_hk, "Converted Date"),
            }, batch_id, extract_ts, source_system))

    for customer_hk, hub_customer in hub_customer_by_hk.items():
        person_hk = person_by_customer_hk.get(customer_hk)
        sat_customer = sat_customer_by_hk.get(customer_hk, {})
        raw_rows["crm_customer.csv"].append(_with_meta({
            "person_id": _safe_get(hub_person_by_hk, person_hk, "Person Id"),
            "customer_id": hub_customer["Customer Id"],
            "customer_number": sat_customer.get("Customer Number"),
            "customer_status": sat_customer.get("Customer Status"),
            "customer_status_reason": sat_customer.get("Customer Status Reason"),
            "customer_since": sat_customer.get("Customer Since"),
            "customer_rating": sat_customer.get("Customer Rating"),
            "customer_segment": sat_customer.get("Customer Segment"),
            "line_of_business": sat_customer.get("Line Of Business"),
            "nps_score": sat_customer.get("NPS Score"),
        }, batch_id, extract_ts, source_system))

    for row in ctx["links"].get("Link_Customer_Lead", []):
        customer_hk = row.get("Customer Hash Key")
        lead_hk = row.get("Lead Hash Key")
        raw_rows["crm_customer_lead.csv"].append(_with_meta({
            "customer_id": _safe_get(hub_customer_by_hk, customer_hk, "Customer Id"),
            "lead_id": _safe_get(hub_lead_by_hk, lead_hk, "Lead Id"),
        }, batch_id, extract_ts, source_system))

    for consent_hk, hub_consent in hub_consent_by_hk.items():
        person_hk = person_by_consent_hk.get(consent_hk)
        raw_rows["crm_consent.csv"].append(_with_meta({
            "person_id": _safe_get(hub_person_by_hk, person_hk, "Person Id"),
            "consent_id": hub_consent["Consent Id"],
            "opt_in_validated": _safe_get(sat_consent_by_hk, consent_hk, "Opt In Validated"),
            "opt_in_legitimate_interest": _safe_get(sat_consent_by_hk, consent_hk, "Opt In Legitimate Interest"),
        }, batch_id, extract_ts, source_system))

    for mpr_hk, hub_mpr in hub_mpr_by_hk.items():
        person_hk = person_by_mpr_hk.get(mpr_hk)
        sat_mpr = sat_mpr_by_hk.get(mpr_hk, {})
        raw_rows["crm_marketing_preference.csv"].append(_with_meta({
            "person_id": _safe_get(hub_person_by_hk, person_hk, "Person Id"),
            "marketing_preference_id": hub_mpr["Marketing Preference Id"],
            "sms": sat_mpr.get("SMS"),
            "email": sat_mpr.get("Email"),
            "email_subscriptions": sat_mpr.get("Email Subscriptions"),
            "call": sat_mpr.get("Call"),
            "any": sat_mpr.get("Any"),
            "commercial_email": sat_mpr.get("Commercial Email"),
            "postal_mail": sat_mpr.get("Postal Mail"),
        }, batch_id, extract_ts, source_system))

    for men_hk, hub_men in hub_men_by_hk.items():
        person_hk = person_by_men_hk.get(men_hk)
        sat_men = sat_men_by_hk.get(men_hk, {})
        raw_rows["crm_marketing_engagement.csv"].append(_with_meta({
            "person_id": _safe_get(hub_person_by_hk, person_hk, "Person Id"),
            "marketing_engagement_id": hub_men["Marketing Engagement Id"],
            "promotion_code": sat_men.get("Promotion Code"),
            "opened_email": sat_men.get("Opened Email"),
            "marketing_status": sat_men.get("Marketing Status"),
        }, batch_id, extract_ts, source_system))

    for account_hk, hub_account in hub_account_by_hk.items():
        person_hk = person_by_account_hk.get(account_hk)
        sat_account = sat_account_by_hk.get(account_hk, {})
        raw_rows["crm_account.csv"].append(_with_meta({
            "person_id": _safe_get(hub_person_by_hk, person_hk, "Person Id"),
            "account_id": hub_account["Account Id"],
            "account_number": sat_account.get("Account Number"),
            "account_type": sat_account.get("Account Type"),
            "account_last_access": sat_account.get("Account Last Access"),
            "account_last_change": sat_account.get("Account Last Change"),
            "account_creation_type": sat_account.get("Account Creation Type"),
            "account_status": sat_account.get("Account Status"),
        }, batch_id, extract_ts, source_system))

    for product_hk, hub_product in hub_product_by_hk.items():
        raw_rows["crm_product.csv"].append(_with_meta({
            "product_id": hub_product["Product Id"],
            "product_code": product_code_by_hk.get(product_hk),
            "product_type": product_code_by_hk.get(product_hk),
        }, batch_id, extract_ts, source_system))

    for quote_hk, hub_quote in hub_quote_by_hk.items():
        person_hk = person_by_quote_hk.get(quote_hk)
        product_code = ctx["quote_to_product_id"].get(quote_hk)
        product_hk = product_hk_by_code.get(product_code)
        sat_quote = sat_quote_by_hk.get(quote_hk, {})
        raw_rows["crm_quote.csv"].append(_with_meta({
            "person_id": _safe_get(hub_person_by_hk, person_hk, "Person Id"),
            "quote_id": hub_quote["Quote Id"],
            "product_id": _safe_get(hub_product_by_hk, product_hk, "Product Id"),
            "product_code": product_code,
            "gross_revenue": sat_quote.get("Gross Revenue"),
            "net_revenue": sat_quote.get("Net Revenue"),
            "quote_number": sat_quote.get("Quote Number"),
            "quote_status": sat_quote.get("Quote Status"),
            "renewal_amt_current_period": sat_quote.get("Renewal Amt Current Period"),
            "renewal_amt_next_period": sat_quote.get("Renewal Amt Next Period"),
        }, batch_id, extract_ts, source_system))

    for policy_hk, hub_policy in hub_policy_by_hk.items():
        person_hk = person_by_policy_hk.get(policy_hk)
        customer_hk = customer_by_person_hk.get(person_hk)
        quote_hk = ctx["policy_to_quote_map"].get(policy_hk)
        product_code = ctx["policy_to_product_id"].get(policy_hk)
        product_hk = product_hk_by_code.get(product_code)
        sat_policy = sat_policy_by_hk.get(policy_hk, {})
        raw_rows["crm_policy.csv"].append(_with_meta({
            "person_id": _safe_get(hub_person_by_hk, person_hk, "Person Id"),
            "customer_id": _safe_get(hub_customer_by_hk, customer_hk, "Customer Id"),
            "policy_id": hub_policy["Policy Id"],
            "quote_id": _safe_get(hub_quote_by_hk, quote_hk, "Quote Id"),
            "product_id": _safe_get(hub_product_by_hk, product_hk, "Product Id"),
            "product_code": product_code,
            "cover_option": sat_policy.get("Cover Option"),
            "declined_claims": sat_policy.get("Declined Claims"),
            "fraud_flag": sat_policy.get("Fraud Flag"),
            "gross_revenue": sat_policy.get("Gross Revenue"),
            "net_revenue": sat_policy.get("Net Revenue"),
            "number_of_active_claim": sat_policy.get("Number of Active Claim"),
            "number_of_previous_claim": sat_policy.get("Number of Previous Claim"),
            "policy_cycle": sat_policy.get("Policy Cycle"),
            "policy_end_date": sat_policy.get("Policy End Date"),
            "policy_length": sat_policy.get("Policy Length"),
            "policy_number": sat_policy.get("Policy Number"),
            "policy_start_date": sat_policy.get("Policy Start Date"),
            "policy_status": sat_policy.get("Policy Status"),
            "renewal_amount_current_period": sat_policy.get("Renewal Amount Current Period"),
            "renewal_amount_next_period": sat_policy.get("Renewal Amount Next Period"),
            "renewal_date": sat_policy.get("Renewal Date"),
            "sales_channel": sat_policy.get("Sales Channel"),
        }, batch_id, extract_ts, source_system))

    for policy_hk, home_hk in ctx["policy_to_home"].items():
        product_code = ctx["policy_to_product_id"].get(policy_hk)
        product_hk = product_hk_by_code.get(product_code)
        addr_hk = ctx["home_to_addr"].get(home_hk)
        sat_home = sat_home_by_hk.get(home_hk, {})
        sat_addr = sat_addr_by_hk.get(addr_hk, {})
        raw_rows["crm_home.csv"].append(_with_meta({
            "policy_id": _safe_get(hub_policy_by_hk, policy_hk, "Policy Id"),
            "product_id": _safe_get(hub_product_by_hk, product_hk, "Product Id"),
            "product_code": product_code,
            "home_id": _safe_get(hub_home_by_hk, home_hk, "Home Id"),
            "wall_construction": sat_home.get("Wall Construction"),
            "home_risk_address": sat_home.get("Home Risk Address"),
            "roof_construction": sat_home.get("Roof Construction"),
            "home_type": sat_home.get("Home Type"),
            "home_state": sat_home.get("Home State"),
            "is_existing_home_customer": sat_home.get("Is Existing Home Customer"),
            "street": sat_addr.get("Street"),
            "postcode": sat_addr.get("Postcode"),
            "city": sat_addr.get("City"),
            "state": sat_addr.get("State"),
            "country": sat_addr.get("Country"),
        }, batch_id, extract_ts, source_system))

    for policy_hk, motor_hks in ctx["policy_to_motor"].items():
        product_code = ctx["policy_to_product_id"].get(policy_hk)
        product_hk = product_hk_by_code.get(product_code)
        for motor_hk in _as_list(motor_hks):
            sat_motor = sat_motor_by_hk.get(motor_hk, {})
            raw_rows["crm_motor.csv"].append(_with_meta({
                "policy_id": _safe_get(hub_policy_by_hk, policy_hk, "Policy Id"),
                "product_id": _safe_get(hub_product_by_hk, product_hk, "Product Id"),
                "product_code": product_code,
                "motor_id": _safe_get(hub_motor_by_hk, motor_hk, "Motor Id"),
                "auto_decline_vehicle": sat_motor.get("Auto Decline Vehicle"),
                "body_type": sat_motor.get("Body Type"),
                "fuel_type": sat_motor.get("Fuel Type"),
                "license_status": sat_motor.get("License Status"),
                "is_existing_motor_customer": sat_motor.get("Is Existing Motor Customer"),
                "motor_lapsed_policies": sat_motor.get("Motor Lapsed Policies"),
                "motor_risk_address": sat_motor.get("Motor Risk Address"),
                "risk_class_code": sat_motor.get("Risk Class Code"),
                "variant": sat_motor.get("Variant"),
                "vehicle_owner_type": sat_motor.get("Vehicle Owner Type"),
                "vehicle_regstate": sat_motor.get("Vehicle RegState"),
                "vehicle_class": sat_motor.get("Vehicle Class"),
                "vehicle_model": sat_motor.get("Vehicle Model"),
                "vehicle_type": sat_motor.get("Vehicle Type"),
                "motor_sum_insrd": sat_motor.get("Motor Sum Insrd"),
                "vehicle_year": sat_motor.get("Vehicle Year"),
                "vehicle_age": sat_motor.get("Vehicle Age"),
            }, batch_id, extract_ts, source_system))

    os.makedirs(out_dir, exist_ok=True)
    for file_name, rows in raw_rows.items():
        raw_file_name = to_crm_raw_file(file_name)
        raw_fieldnames = [to_crm_raw_column(field) for field in schemas[file_name]]
        raw_rows_out = [
            {to_crm_raw_column(field): row.get(field, "") for field in schemas[file_name]}
            for row in rows
        ]
        write_csv(out_dir, raw_file_name, raw_rows_out, fieldnames=raw_fieldnames)

    return out_dir
