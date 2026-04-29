import os

from helper.csv_writer import write_csv


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


def _source_id(value, prefix="CLM"):
    if value is None:
        return ""
    value = str(value)
    if not value:
        return ""
    return f"{prefix}_{value}"


def write_raw_claims_batch(base_folder, batch_id, ctx):
    out_dir = os.path.join(base_folder, "claims", batch_id)
    os.makedirs(out_dir, exist_ok=True)
    extract_ts = ctx.get("extract_ts", "")

    hub_person_by_hk = _index_by(ctx["hub_person_rows"], "Person Hash Key")
    hub_contact_by_hk = _index_by(ctx["hub_con_rows"], "Contact Hash Key")
    hub_identity_by_hk = _index_by(ctx["hub_id_rows"], "Identities Hash Key")
    hub_customer_by_hk = _index_by(ctx["hub_cust_rows"], "Customer Hash Key")
    hub_policy_by_hk = _index_by(ctx["hub_pol_rows"], "Policy Hash Key")
    hub_quote_by_hk = _index_by(ctx["hub_quo_rows"], "Quote Hash Key")
    hub_product_by_hk = _index_by(ctx["hub_prod_rows"], "Product Hash Key")
    hub_motor_by_hk = _index_by(ctx["hub_mot_rows"], "Motor Hash Key")
    hub_home_by_hk = _index_by(ctx["hub_home_rows"], "Home Hash Key")
    hub_addr_by_hk = _index_by(ctx["hub_addr_rows"], "Home Address Hash Key")

    sat_person_by_hk = _index_by(ctx["sat_per"], "Person Hash Key")
    sat_contact_by_hk = _index_by(ctx["sat_con"], "Contact Hash Key")
    sat_identity_by_hk = _index_by(ctx["sat_eci"], "Identities Hash Key")
    sat_customer_by_hk = _index_by(ctx["sat_cus"], "Customer Hash Key")
    sat_policy_by_hk = _index_by(ctx["sat_pol"], "Policy Hash Key")
    sat_motor_by_hk = _index_by(ctx["sat_mot"], "Motor Hash Key")
    sat_home_by_hk = _index_by(ctx["sat_hom"], "Home Hash Key")
    sat_addr_by_hk = _index_by(ctx["sat_adr"], "Home Address Hash Key")

    person_by_contact_hk = _invert_multi_map(ctx["person_to_contact"])
    person_by_identity_hk = _invert_multi_map(ctx["person_to_identity"])
    person_by_customer_hk = _invert_single_map(ctx["person_to_customer"])
    person_by_policy_hk = _invert_multi_map(ctx["policy_person_map"])
    person_by_addr_hk = _invert_multi_map(ctx["person_to_home_address"])

    product_hk_by_code = {code: hk for hk, code in ctx["product_code_by_hk"].items()}
    policy_by_home_hk = {home_hk: policy_hk for policy_hk, home_hk in ctx["policy_to_home"].items()}
    policy_by_motor_hk = {}
    for policy_hk, motor_hks in ctx["policy_to_motor"].items():
        for motor_hk in _as_list(motor_hks):
            policy_by_motor_hk[motor_hk] = policy_hk

    party_rows = []
    policy_rows = []
    asset_rows = []
    claim_rows = []
    payment_rows = []
    schemas = {
        "claims_party_profile.csv": [
            "batch_ref", "pull_ts", "origin_sys", "claim_party_ref", "claim_customer_ref",
            "party_kind", "lead_ind", "paperless_ind", "customer_status_txt",
            "customer_segment_txt", "contact_ref", "email_home_txt", "email_work_txt",
            "phone_work_txt", "phone_home_txt", "identity_ref", "ecid_txt",
            "hashed_email_txt", "address_ref", "street_txt", "postal_cd", "city_nm",
            "state_cd", "country_cd",
        ],
        "claims_policy_context.csv": [
            "batch_ref", "pull_ts", "origin_sys", "claim_policy_ref", "claim_customer_ref",
            "claim_party_ref", "claim_quote_ref", "claim_product_ref", "product_line",
            "policy_status_txt", "policy_start_dt", "policy_end_dt", "active_claim_cnt",
            "previous_claim_cnt", "fraud_ind",
        ],
        "claims_asset_context.csv": [
            "batch_ref", "pull_ts", "origin_sys", "claim_policy_ref", "claim_asset_ref",
            "asset_kind", "risk_address_txt", "asset_state_cd", "street_txt",
            "postal_cd", "city_nm",
        ],
        "claims_header.csv": [
            "batch_ref", "pull_ts", "origin_sys", "claim_ref", "claim_policy_ref",
            "claim_customer_ref", "claim_party_ref", "claim_status_txt", "claim_type_txt",
            "incident_dt", "reported_dt", "closed_dt", "fraud_ind", "declined_claim_cnt",
        ],
        "claims_payment_event.csv": [
            "batch_ref", "pull_ts", "origin_sys", "payment_ref", "claim_ref",
            "payment_status_txt", "payment_amt", "payment_dt",
        ],
    }

    for person_hk in ctx["person_hks"]:
        hub_person = hub_person_by_hk[person_hk]
        sat_person = sat_person_by_hk.get(person_hk, {})
        customer_hk = ctx["person_to_customer"].get(person_hk)
        customer = hub_customer_by_hk.get(customer_hk, {})
        customer_sat = sat_customer_by_hk.get(customer_hk, {})
        party_rows.append({
            "batch_ref": batch_id,
            "pull_ts": extract_ts,
            "origin_sys": "CLAIMS",
            "claim_party_ref": _source_id(hub_person["Person Id"]),
            "claim_customer_ref": _source_id(customer.get("Customer Id")),
            "party_kind": sat_person.get("Type", ""),
            "lead_ind": sat_person.get("Is Lead", ""),
            "paperless_ind": sat_person.get("Operational Paperless Consent", ""),
            "customer_status_txt": customer_sat.get("Customer Status", ""),
            "customer_segment_txt": customer_sat.get("Customer Segment", ""),
        })

    for contact_hk, hub_contact in hub_contact_by_hk.items():
        person_hk = person_by_contact_hk.get(contact_hk)
        if not person_hk:
            continue
        sat_contact = sat_contact_by_hk.get(contact_hk, {})
        party_rows.append({
            "batch_ref": batch_id,
            "pull_ts": extract_ts,
            "origin_sys": "CLAIMS",
            "claim_party_ref": _source_id(hub_person_by_hk[person_hk]["Person Id"]),
            "claim_customer_ref": "",
            "party_kind": "CONTACT",
            "lead_ind": "",
            "paperless_ind": "",
            "customer_status_txt": "",
            "customer_segment_txt": "",
            "contact_ref": _source_id(hub_contact["Contact Id"]),
            "email_home_txt": sat_contact.get("Personal Email", ""),
            "email_work_txt": sat_contact.get("Work Email", ""),
            "phone_work_txt": sat_contact.get("Work Phone", ""),
            "phone_home_txt": sat_contact.get("Home Phone", ""),
        })

    for identity_hk, hub_identity in hub_identity_by_hk.items():
        person_hk = person_by_identity_hk.get(identity_hk)
        if not person_hk:
            continue
        sat_identity = sat_identity_by_hk.get(identity_hk, {})
        party_rows.append({
            "batch_ref": batch_id,
            "pull_ts": extract_ts,
            "origin_sys": "CLAIMS",
            "claim_party_ref": _source_id(hub_person_by_hk[person_hk]["Person Id"]),
            "claim_customer_ref": "",
            "party_kind": "IDENTITY",
            "lead_ind": "",
            "paperless_ind": "",
            "customer_status_txt": "",
            "customer_segment_txt": "",
            "identity_ref": _source_id(hub_identity["Identities Id"]),
            "ecid_txt": sat_identity.get("ECID", ""),
            "hashed_email_txt": sat_identity.get("Hashed Email", ""),
        })

    for addr_hk, hub_addr in hub_addr_by_hk.items():
        person_hk = person_by_addr_hk.get(addr_hk)
        if not person_hk:
            continue
        sat_addr = sat_addr_by_hk.get(addr_hk, {})
        party_rows.append({
            "batch_ref": batch_id,
            "pull_ts": extract_ts,
            "origin_sys": "CLAIMS",
            "claim_party_ref": _source_id(hub_person_by_hk[person_hk]["Person Id"]),
            "claim_customer_ref": "",
            "party_kind": "ADDRESS",
            "lead_ind": "",
            "paperless_ind": "",
            "customer_status_txt": "",
            "customer_segment_txt": "",
            "address_ref": _source_id(hub_addr["Home Address Id"]),
            "street_txt": sat_addr.get("Street", ""),
            "postal_cd": sat_addr.get("Postcode", ""),
            "city_nm": sat_addr.get("City", ""),
            "state_cd": sat_addr.get("State", ""),
            "country_cd": sat_addr.get("Country", ""),
        })

    claim_sequence = 1
    payment_sequence = 1
    for policy_hk, hub_policy in hub_policy_by_hk.items():
        person_hk = person_by_policy_hk.get(policy_hk)
        if not person_hk:
            continue
        customer_hk = ctx["person_to_customer"].get(person_hk)
        quote_hk = ctx["policy_to_quote_map"].get(policy_hk)
        product_code = ctx["policy_to_product_id"].get(policy_hk, "")
        product_hk = product_hk_by_code.get(product_code)
        sat_policy = sat_policy_by_hk.get(policy_hk, {})

        claims_count = max(1, int(sat_policy.get("Number of Previous Claim", 0) or 0) + int(sat_policy.get("Number of Active Claim", 0) or 0))
        policy_rows.append({
            "batch_ref": batch_id,
            "pull_ts": extract_ts,
            "origin_sys": "CLAIMS",
            "claim_policy_ref": _source_id(hub_policy["Policy Id"]),
            "claim_customer_ref": _source_id(hub_customer_by_hk.get(customer_hk, {}).get("Customer Id")),
            "claim_party_ref": _source_id(hub_person_by_hk[person_hk]["Person Id"]),
            "claim_quote_ref": _source_id(hub_quote_by_hk.get(quote_hk, {}).get("Quote Id")),
            "claim_product_ref": _source_id(hub_product_by_hk.get(product_hk, {}).get("Product Id")),
            "product_line": product_code,
            "policy_status_txt": sat_policy.get("Policy Status", ""),
            "policy_start_dt": sat_policy.get("Policy Start Date", ""),
            "policy_end_dt": sat_policy.get("Policy End Date", ""),
            "active_claim_cnt": sat_policy.get("Number of Active Claim", ""),
            "previous_claim_cnt": sat_policy.get("Number of Previous Claim", ""),
            "fraud_ind": sat_policy.get("Fraud Flag", ""),
        })

        home_hk = ctx["policy_to_home"].get(policy_hk)
        if home_hk:
            addr_hk = ctx["home_to_addr"].get(home_hk)
            sat_home = sat_home_by_hk.get(home_hk, {})
            sat_addr = sat_addr_by_hk.get(addr_hk, {})
            asset_rows.append({
                "batch_ref": batch_id,
                "pull_ts": extract_ts,
                "origin_sys": "CLAIMS",
                "claim_policy_ref": _source_id(hub_policy["Policy Id"]),
                "claim_asset_ref": _source_id(hub_home_by_hk[home_hk]["Home Id"]),
                "asset_kind": "HOME",
                "risk_address_txt": sat_home.get("Home Risk Address", ""),
                "asset_state_cd": sat_home.get("Home State", ""),
                "street_txt": sat_addr.get("Street", ""),
                "postal_cd": sat_addr.get("Postcode", ""),
                "city_nm": sat_addr.get("City", ""),
            })

        for motor_hk in _as_list(ctx["policy_to_motor"].get(policy_hk)):
            sat_motor = sat_motor_by_hk.get(motor_hk, {})
            asset_rows.append({
                "batch_ref": batch_id,
                "pull_ts": extract_ts,
                "origin_sys": "CLAIMS",
                "claim_policy_ref": _source_id(hub_policy["Policy Id"]),
                "claim_asset_ref": _source_id(hub_motor_by_hk[motor_hk]["Motor Id"]),
                "asset_kind": "MOTOR",
                "risk_address_txt": sat_motor.get("Motor Risk Address", ""),
                "asset_state_cd": sat_motor.get("Vehicle RegState", ""),
                "street_txt": sat_motor.get("Motor Risk Address", ""),
                "postal_cd": "",
                "city_nm": "",
            })

        for idx in range(claims_count):
            claim_id = f"CLM_{batch_id}_{claim_sequence:06d}"
            claim_status = "OPEN" if idx < int(sat_policy.get("Number of Active Claim", 0) or 0) else "CLOSED"
            claim_rows.append({
                "batch_ref": batch_id,
                "pull_ts": extract_ts,
                "origin_sys": "CLAIMS",
                "claim_ref": claim_id,
                "claim_policy_ref": _source_id(hub_policy["Policy Id"]),
                "claim_customer_ref": _source_id(hub_customer_by_hk.get(customer_hk, {}).get("Customer Id")),
                "claim_party_ref": _source_id(hub_person_by_hk[person_hk]["Person Id"]),
                "claim_status_txt": claim_status,
                "claim_type_txt": "PROPERTY" if home_hk else "MOTOR",
                "incident_dt": sat_policy.get("Policy Start Date", ""),
                "reported_dt": sat_policy.get("Policy Start Date", ""),
                "closed_dt": sat_policy.get("Policy End Date", "") if claim_status == "CLOSED" else "",
                "fraud_ind": sat_policy.get("Fraud Flag", ""),
                "declined_claim_cnt": sat_policy.get("Declined Claims", ""),
            })
            payment_rows.append({
                "batch_ref": batch_id,
                "pull_ts": extract_ts,
                "origin_sys": "CLAIMS",
                "payment_ref": f"PMT_{batch_id}_{payment_sequence:06d}",
                "claim_ref": claim_id,
                "payment_status_txt": "PAID" if claim_status == "CLOSED" else "RESERVED",
                "payment_amt": sat_policy.get("Net Revenue", ""),
                "payment_dt": sat_policy.get("Policy End Date", "") if claim_status == "CLOSED" else sat_policy.get("Policy Start Date", ""),
            })
            claim_sequence += 1
            payment_sequence += 1

    write_csv(out_dir, "claims_party_profile.csv", party_rows, fieldnames=schemas["claims_party_profile.csv"])
    write_csv(out_dir, "claims_policy_context.csv", policy_rows, fieldnames=schemas["claims_policy_context.csv"])
    write_csv(out_dir, "claims_asset_context.csv", asset_rows, fieldnames=schemas["claims_asset_context.csv"])
    write_csv(out_dir, "claims_header.csv", claim_rows, fieldnames=schemas["claims_header.csv"])
    write_csv(out_dir, "claims_payment_event.csv", payment_rows, fieldnames=schemas["claims_payment_event.csv"])
    return out_dir
