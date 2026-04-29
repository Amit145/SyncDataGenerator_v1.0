from __future__ import annotations

import csv
import json
import os

from config.storage_paths import RAW_CLAIMS_CANONICAL_ROOT
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


def _base_row(batch_id: str, extract_ts: str) -> dict:
    return {
        "_batch_id": batch_id,
        "_extract_ts": extract_ts,
        "_source_system": "CLAIMS",
    }


def _product_type(product_line: str) -> str:
    value = (product_line or "").upper()
    if "HOME" in value or "PROPERTY" in value:
        return "HOME"
    if "MOTOR" in value or "AUTO" in value or "VEHICLE" in value:
        return "MOTOR"
    return value


def _natural_or_legal(party_kind: str) -> tuple[str, str]:
    value = (party_kind or "").upper()
    if value == "NATURAL":
        return "NATURAL", ""
    if value == "LEGAL":
        return "", "LEGAL"
    return "", ""


def _derived_ref(prefix: str, value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    return f"{prefix}_{value}"


def map_claims_to_canonical(run_id: str, claims_raw_dir: str) -> str:
    out_dir = os.path.join(RAW_CLAIMS_CANONICAL_ROOT, run_id)
    os.makedirs(out_dir, exist_ok=True)

    party_rows = _read_csv(os.path.join(claims_raw_dir, "claims_party_profile.csv"))
    policy_rows = _read_csv(os.path.join(claims_raw_dir, "claims_policy_context.csv"))
    asset_rows = _read_csv(os.path.join(claims_raw_dir, "claims_asset_context.csv"))
    claim_rows = _read_csv(os.path.join(claims_raw_dir, "claims_header.csv"))
    payment_rows = _read_csv(os.path.join(claims_raw_dir, "claims_payment_event.csv"))

    batch_id = run_id
    extract_ts = ""
    for rowset in (party_rows, policy_rows, asset_rows, claim_rows, payment_rows):
        if rowset:
            batch_id = rowset[0].get("batch_ref", run_id) or run_id
            extract_ts = rowset[0].get("pull_ts", "") or extract_ts
            if extract_ts:
                break

    combined = {name: [] for name in CANONICAL_RAW_SCHEMAS}
    base = _base_row(batch_id, extract_ts)

    latest_claim_by_policy = {}
    for row in claim_rows:
        policy_id = row.get("claim_policy_ref", "")
        if policy_id:
            latest_claim_by_policy[policy_id] = row

    lead_person_ids = set()
    consent_person_ids = set()
    customer_policy_summary = {}
    policy_meta = {}
    for row in policy_rows:
        policy_id = row.get("claim_policy_ref", "")
        if not policy_id:
            continue
        person_id = row.get("claim_party_ref", "")
        customer_id = row.get("claim_customer_ref", "")
        if row.get("claim_quote_ref"):
            lead_person_ids.add(person_id)
        if person_id:
            consent_person_ids.add(person_id)
        policy_meta[policy_id] = {
            "product_id": row.get("claim_product_ref", ""),
            "product_code": row.get("product_line", ""),
            "person_id": person_id,
            "customer_id": customer_id,
        }
        if customer_id:
            summary = customer_policy_summary.setdefault(customer_id, {
                "has_active": False,
                "has_lapsed": False,
                "has_cancelled": False,
                "max_declined_claims": 0,
                "max_active_claims": 0,
                "max_previous_claims": 0,
                "has_fraud": False,
            })
            status = (row.get("policy_status_txt", "") or "").upper()
            if status == "ACTIVE":
                summary["has_active"] = True
            elif status == "LAPSED":
                summary["has_lapsed"] = True
            elif status == "CANCELLED":
                summary["has_cancelled"] = True
            summary["max_active_claims"] = max(summary["max_active_claims"], int(row.get("active_claim_cnt", 0) or 0))
            summary["max_previous_claims"] = max(summary["max_previous_claims"], int(row.get("previous_claim_cnt", 0) or 0))

    for row in claim_rows:
        customer_id = row.get("claim_customer_ref", "")
        if not customer_id:
            continue
        summary = customer_policy_summary.setdefault(customer_id, {
            "has_active": False,
            "has_lapsed": False,
            "has_cancelled": False,
            "max_declined_claims": 0,
            "max_active_claims": 0,
            "max_previous_claims": 0,
            "has_fraud": False,
        })
        summary["max_declined_claims"] = max(summary["max_declined_claims"], int(row.get("declined_claim_cnt", 0) or 0))

    account_by_customer = {}
    account_status_by_customer = {}
    for customer_id, summary in customer_policy_summary.items():
        account_id = _derived_ref("ACC", customer_id)
        account_by_customer[customer_id] = account_id
        if summary["has_active"]:
            account_status_by_customer[customer_id] = "OPEN"
        elif summary["has_cancelled"]:
            account_status_by_customer[customer_id] = "CLOSED"
        else:
            account_status_by_customer[customer_id] = "SUSPENDED"

    for row in party_rows:
        party_id = row.get("claim_party_ref", "")
        if not party_id:
            continue

        party_kind = row.get("party_kind", "")
        natural_person_id, legal_person_id = _natural_or_legal(party_kind)
        if natural_person_id:
            natural_person_id = party_id
        if legal_person_id:
            legal_person_id = party_id

        if party_kind not in {"CONTACT", "IDENTITY", "ADDRESS"}:
            combined["crm_person.csv"].append({**base,
                "person_id": party_id,
                "person_type": party_kind,
                "tenant_id": "",
                "is_lead": "Y" if party_id in lead_person_ids else "N",
                "operational_paperless_consent": "Y" if party_id in consent_person_ids else "N",
                "source_id": party_id,
                "source_type": "CLAIMS",
                "natural_person_id": natural_person_id,
                "first_name": "",
                "last_name": "",
                "full_name": "",
                "courtesy_title": "",
                "occupation": "",
                "birth_date": "",
                "birth_year": "",
                "nationality": "",
                "gender": "",
                "marital_status": "",
                "assesed_disability_degree": "",
                "preferred_language": "",
                "role": "",
                "job_title": "",
                "legal_person_id": legal_person_id,
                "company_name": "",
                "legal_person_score": "",
                "legal_person_status": "",
                "legal_person_job_title": "",
                "legal_source_id": "",
                "legal_source_type": "",
                "date_of_constitution": "",
                "lead_converted_date": "",
            })

        if row.get("claim_customer_ref"):
            customer_id = row.get("claim_customer_ref", "")
            policy_summary = customer_policy_summary.get(customer_id, {})
            account_status = account_status_by_customer.get(customer_id, "")
            customer_status = row.get("customer_status_txt", "") or ("ACTIVE" if policy_summary.get("has_active") else "INACTIVE")
            nps_score = 0
            segment_score = 0
            if policy_summary.get("has_active"):
                segment_score += 2
            if account_status == "OPEN":
                segment_score += 1
            if nps_score >= 9:
                segment_score += 1
            if policy_summary.get("has_cancelled"):
                segment_score -= 1
            elif policy_summary.get("has_lapsed"):
                segment_score -= 1
            if policy_summary.get("has_fraud"):
                segment_score -= 2
            expected_segment = "PREMIUM" if segment_score >= 4 else "STANDARD"

            rating = 2
            if customer_status == "ACTIVE":
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
            if policy_summary.get("has_fraud"):
                rating -= 1
            if policy_summary.get("max_declined_claims", 0) > 1:
                rating -= 1

            combined["crm_customer.csv"].append({**base,
                "person_id": party_id,
                "customer_id": customer_id,
                "customer_number": customer_id,
                "customer_status": customer_status,
                "customer_status_reason": "",
                "customer_since": "",
                "customer_rating": "",
                "customer_segment": expected_segment,
                "line_of_business": "",
                "nps_score": str(nps_score),
            })

        if row.get("contact_ref"):
            combined["crm_contact.csv"].append({**base,
                "person_id": party_id,
                "contact_id": row.get("contact_ref", ""),
                "personal_email": row.get("email_home_txt", ""),
                "work_email": row.get("email_work_txt", ""),
                "work_phone": row.get("phone_work_txt", ""),
                "home_phone": row.get("phone_home_txt", ""),
            })

        if row.get("identity_ref"):
            combined["crm_identity.csv"].append({**base,
                "person_id": party_id,
                "identities_id": row.get("identity_ref", ""),
                "ecid": row.get("ecid_txt", ""),
                "hashed_email": row.get("hashed_email_txt", ""),
            })

        if row.get("address_ref"):
            combined["crm_person_address.csv"].append({**base,
                "person_id": party_id,
                "address_id": row.get("address_ref", ""),
                "street": row.get("street_txt", ""),
                "postcode": row.get("postal_cd", ""),
                "city": row.get("city_nm", ""),
                "state": row.get("state_cd", ""),
                "country": row.get("country_cd", ""),
            })

        if party_id in lead_person_ids:
            combined["crm_lead.csv"].append({**base,
                "person_id": party_id,
                "lead_id": _derived_ref("LED", party_id),
                "interested_level": "HIGH",
                "preferred_contact_method": "",
                "person_score": "90",
                "person_status": "OPEN",
                "converted_date": "",
            })
            combined["crm_marketing_preference.csv"].append({**base,
                "person_id": party_id,
                "marketing_preference_id": _derived_ref("MPR", party_id),
                "sms": "Y",
                "email": "Y",
                "email_subscriptions": "Y",
                "call": "N",
                "any": "Y",
                "commercial_email": "Y",
                "postal_mail": "N",
            })

        if party_id in consent_person_ids:
            combined["crm_consent.csv"].append({**base,
                "person_id": party_id,
                "consent_id": _derived_ref("CNS", party_id),
                "opt_in_validated": "Y",
                "opt_in_legitimate_interest": "Y",
            })

        customer_id = row.get("claim_customer_ref", "")
        account_id = account_by_customer.get(customer_id, "")
        if customer_id and account_id:
            combined["crm_account.csv"].append({**base,
                "person_id": party_id,
                "account_id": account_id,
                "account_number": account_id,
                "account_type": "CUSTOMER_PORTAL",
                "account_last_access": extract_ts,
                "account_last_change": extract_ts,
                "account_creation_type": "CLAIMS",
                "account_status": account_status_by_customer.get(customer_id, ""),
            })

    for row in policy_rows:
        product_id = row.get("claim_product_ref", "")
        product_code = row.get("product_line", "")
        if product_id:
            combined["crm_product.csv"].append({**base,
                "product_id": product_id,
                "product_code": product_code,
                "product_type": _product_type(product_code),
            })

        if row.get("claim_quote_ref"):
            combined["crm_quote.csv"].append({**base,
                "person_id": row.get("claim_party_ref", ""),
                "quote_id": row.get("claim_quote_ref", ""),
                "product_id": product_id,
                "product_code": product_code,
                "gross_revenue": "",
                "net_revenue": "",
                "quote_number": row.get("claim_quote_ref", ""),
                "quote_status": "",
                "renewal_amt_current_period": "",
                "renewal_amt_next_period": "",
            })

        claim_header = latest_claim_by_policy.get(row.get("claim_policy_ref", ""), {})
        customer_id = row.get("claim_customer_ref", "")
        account_status = account_status_by_customer.get(customer_id, "")
        declined_claims = int(claim_header.get("declined_claim_cnt", 0) or 0)
        active_claims = int(row.get("active_claim_cnt", 0) or 0)
        previous_claims = int(row.get("previous_claim_cnt", 0) or 0)
        policy_status = (row.get("policy_status_txt", "") or "").upper()
        fraud_score = 0
        if declined_claims >= 2:
            fraud_score += 2
        if previous_claims >= 4:
            fraud_score += 1
        if active_claims >= 2:
            fraud_score += 1
        if policy_status == "CANCELLED":
            fraud_score += 1
        if account_status == "SUSPENDED":
            fraud_score += 1
        elif account_status == "CLOSED":
            fraud_score += 2
        if customer_id:
            customer_policy_summary.setdefault(customer_id, {
                "has_active": False,
                "has_lapsed": False,
                "has_cancelled": False,
                "max_declined_claims": 0,
                "max_active_claims": 0,
                "max_previous_claims": 0,
                "has_fraud": False,
            })["has_fraud"] = customer_policy_summary.get(customer_id, {}).get("has_fraud", False) or fraud_score >= 4
        combined["crm_policy.csv"].append({**base,
            "person_id": row.get("claim_party_ref", ""),
            "customer_id": customer_id,
            "policy_id": row.get("claim_policy_ref", ""),
            "quote_id": row.get("claim_quote_ref", ""),
            "product_id": product_id,
            "product_code": product_code,
            "cover_option": "",
            "declined_claims": str(declined_claims),
            "fraud_flag": "Y" if fraud_score >= 4 else "N",
            "gross_revenue": "",
            "net_revenue": "",
            "number_of_active_claim": row.get("active_claim_cnt", ""),
            "number_of_previous_claim": row.get("previous_claim_cnt", ""),
            "policy_cicle": "",
            "policy_end_date": row.get("policy_end_dt", ""),
            "policy_length": "",
            "policy_number": row.get("claim_policy_ref", ""),
            "policy_start_date": row.get("policy_start_dt", ""),
            "policy_status": row.get("policy_status_txt", ""),
            "renewal_amount_current_period": "",
            "renewal_amount_next_period": "",
            "renewal_date": "",
            "sales_channel": "",
        })

    for row in asset_rows:
        policy_id = row.get("claim_policy_ref", "")
        meta = policy_meta.get(policy_id, {})
        product_id = meta.get("product_id", "")
        product_code = meta.get("product_code", "")

        if (row.get("asset_kind", "").upper()) == "HOME":
            combined["crm_home.csv"].append({**base,
                "policy_id": policy_id,
                "product_id": product_id,
                "product_code": product_code,
                "home_id": row.get("claim_asset_ref", ""),
                "wall_construction": "",
                "home_risk_address": row.get("risk_address_txt", ""),
                "roof_construction": "",
                "home_type": "HOME",
                "home_state": row.get("asset_state_cd", ""),
                "is_existing_home_customer": "",
                "street": row.get("street_txt", ""),
                "postcode": row.get("postal_cd", ""),
                "city": row.get("city_nm", ""),
                "state": row.get("asset_state_cd", ""),
                "country": "",
            })
        elif (row.get("asset_kind", "").upper()) == "MOTOR":
            combined["crm_motor.csv"].append({**base,
                "policy_id": policy_id,
                "product_id": product_id,
                "product_code": product_code,
                "motor_id": row.get("claim_asset_ref", ""),
                "auto_decline_vehicle": "",
                "body_type": "",
                "fuel_type": "",
                "license_status": "",
                "is_existing_motor_customer": "",
                "motor_lapsed_policies": "",
                "motor_risk_address": row.get("risk_address_txt", ""),
                "risk_class_code": "",
                "variant": "",
                "vehicle_owner_type": "",
                "vehicle_regstate": row.get("asset_state_cd", ""),
                "vehicle_class": "",
                "vehicle_model": "",
                "vehicle_type": "MOTOR",
                "motor_sum_insrd": "",
                "vehicle_year": "",
                "vehicle_age": "",
            })

    dedupe_keys = {
        "crm_person.csv": ["person_id"],
        "crm_contact.csv": ["contact_id"],
        "crm_identity.csv": ["identities_id"],
        "crm_person_address.csv": ["address_id"],
        "crm_lead.csv": ["lead_id"],
        "crm_customer.csv": ["customer_id"],
        "crm_consent.csv": ["consent_id"],
        "crm_marketing_preference.csv": ["marketing_preference_id"],
        "crm_account.csv": ["account_id"],
        "crm_product.csv": ["product_id"],
        "crm_quote.csv": ["quote_id"],
        "crm_policy.csv": ["policy_id"],
        "crm_home.csv": ["home_id"],
        "crm_motor.csv": ["motor_id"],
    }

    manifest = {
        "source_system": "CLAIMS",
        "batch_id": batch_id,
        "extract_ts": extract_ts,
        "tables_generated": sorted([name for name, rows in combined.items() if rows]),
    }
    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    for name, schema in CANONICAL_RAW_SCHEMAS.items():
        rows = _dedupe(combined[name], dedupe_keys.get(name, []))
        write_csv(out_dir, name, rows, fieldnames=schema)

    return out_dir
