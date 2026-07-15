from __future__ import annotations

import csv
import shutil
from datetime import datetime
from pathlib import Path

from generators.raw_crm_generator import write_raw_crm_batch


def write_raw_prd1_batch(base_folder: str, batch_id: str, ctx: dict, mode: str | None = None) -> str:
    """Write the existing base CRM raw shape into the PRD1 source folder."""
    source_dir_name = f"{mode}/prd_01" if mode else "prd_01"
    return write_raw_crm_batch(
        base_folder,
        batch_id,
        ctx,
        source_dir_name=source_dir_name,
        source_system="CRM",
    )


def mirror_source1_delta_into_prd1(delta_folder: str, prd1_folder: str, prefix: str = "source1_") -> list[str]:
    """Mirror enhanced/MLOps source-1 delta files into PRD1 without overwriting CRM files."""
    delta_dir = Path(delta_folder)
    prd1_dir = Path(prd1_folder)
    if not delta_dir.exists():
        raise FileNotFoundError(f"Delta folder not found: {delta_dir}")
    if not prd1_dir.exists():
        raise FileNotFoundError(f"PRD1 folder not found: {prd1_dir}")
    written = []
    for source_file in sorted(delta_dir.glob("*.csv")):
        target_file = prd1_dir / f"{prefix}{source_file.name}"
        shutil.copy2(source_file, target_file)
        written.append(str(target_file))
    return written


BASE_PRD2_SAP_TABLES = {
    "person.csv": [
        "batch_ref",
        "pull_ts",
        "origin_sys",
        "person_id",
        "person_type",
        "organization",
        "org_establishment_date",
        "first_name",
        "middle_name",
        "last_name",
        "date_of_birth",
        "gender",
        "occupation",
        "email_address",
        "phone_number",
    ],
    "address.csv": [
        "batch_ref",
        "pull_ts",
        "origin_sys",
        "address_id",
        "person_id",
        "address_line_1",
        "address_line_2",
        "city",
        "state",
        "country",
        "zipcode",
    ],
    "product.csv": [
        "batch_ref",
        "pull_ts",
        "origin_sys",
        "product_id",
        "product_type",
        "product_sub_type",
        "product_name",
        "product_start_date",
        "line_of_business",
    ],
    "home.csv": [
        "batch_ref",
        "pull_ts",
        "origin_sys",
        "home_id",
        "policy_id",
        "product_id",
        "home_type",
        "home_location",
        "wall_type",
        "roof_material",
    ],
    "motor.csv": [
        "batch_ref",
        "pull_ts",
        "origin_sys",
        "motor_id",
        "policy_id",
        "product_id",
        "motor_class",
        "motor_model",
        "motor_type",
        "manufacturing_date",
        "body_colour",
        "fuel_type",
        "gear_type",
        "motor_parked_location",
    ],
    "insured_object.csv": [
        "batch_ref",
        "pull_ts",
        "origin_sys",
        "insured_object_id",
        "policy_id",
        "motor_id",
        "home_id",
        "insured_object_variant",
        "insured_object_sub_variant",
        "insured_amount",
        "insured_object_begin_date",
        "insured_object_finish_date",
    ],
}
BUSINESS_VAULT_PERSON_MATCH_RULES = {
    "NATURAL": {
        "CRM": ["given_nm", "family_nm", "dob"],
        "SAP": ["first_name", "last_name", "date_of_birth"],
    },
    "LEGAL": {
        "CRM": ["legal_name", "constitution_dt"],
        "SAP": ["organization", "org_establishment_date"],
    },
}
BUSINESS_VAULT_EXCLUDED_MATCH_COLUMNS = {
    "email_address",
    "phone_number",
    "gender",
    "gender_txt",
}


def _sap_id(value: str) -> str:
    value = str(value or "").strip()
    if not value:
        return ""
    return value if value.startswith("SAP_") else f"SAP_{value}"


def _date_part(value: str) -> str:
    return str(value or "").strip().split(" ")[0].split("T")[0]


def _first_value(*values: str) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _stable_index(value: str, modulo: int) -> int:
    text = str(value or "")
    return sum(ord(char) for char in text) % modulo if modulo else 0


def _sap_middle_name(row: dict) -> str:
    if str(row.get("party_kind", "")).upper() == "LEGAL":
        return ""
    names = [
        "James", "Marie", "Lee", "Grace", "Anne", "David", "Rose", "John",
        "Claire", "Michael", "Louise", "Peter", "Jane", "Thomas", "May",
    ]
    return names[_stable_index(row.get("party_ref", ""), len(names))]


def _sap_address_line_2(row: dict) -> str:
    options = ["Flat", "Unit", "Suite", "Apartment", "Floor"]
    suffix = 1 + _stable_index(row.get("address_ref", ""), 90)
    return f"{options[_stable_index(row.get('party_ref', ''), len(options))]} {suffix}"


def _money_value(*values: str) -> str:
    for value in values:
        text = str(value or "").strip().replace(",", "")
        if not text:
            continue
        try:
            return f"{float(text):.2f}"
        except ValueError:
            continue
    return ""


def _date_obj(value: str):
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[:19], fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _int_value(value, default=0) -> int:
    try:
        return int(float(str(value).replace(",", "").strip()))
    except (TypeError, ValueError):
        return default


def _yn(value: str) -> str:
    text = str(value or "").strip().upper()
    return "Y" if text in {"Y", "YES", "TRUE", "1"} else "N"


def _feedback_from_score(score: int) -> str:
    if score >= 4:
        return "POSITIVE"
    if score >= 2:
        return "NEUTRAL"
    return "NEGATIVE"


def _complaint_resolution_days(row: dict) -> int | None:
    opened = _date_obj(row.get("complaint_date"))
    resolved = _date_obj(row.get("complaint_resolved_date"))
    if not opened or not resolved:
        return None
    return max(0, (resolved - opened).days)


def _complaint_satisfaction_score(row: dict) -> str:
    status = str(row.get("complaint_status") or "").strip().upper()
    if status in {"OPEN", "PENDING"}:
        return ""
    days = _complaint_resolution_days(row)
    score = 5
    if days is None:
        score = 3
    elif days > 60:
        score = 1
    elif days > 30:
        score = 2
    elif days > 7:
        score = 3
    if _yn(row.get("is_financial_ombudsman_service_referral")) == "Y":
        score -= 2
    if str(row.get("complaint_upheld_status") or "").strip().upper() in {"UPHELD", "PARTIALLY_UPHELD"}:
        score -= 1
    return str(max(0, min(5, score)))


def _with_complaint_feedback(row: dict) -> dict:
    enriched = dict(row)
    if str(enriched.get("complaint_resolved_date") or "").strip():
        enriched["complaint_status"] = "Closed"
    score = _complaint_satisfaction_score(enriched)
    enriched["customer_complaint_satisfaction_score"] = score
    enriched["complaint_feedback"] = _feedback_from_score(_int_value(score, 0)) if score != "" else ""
    return enriched


def _claim_fault_flag(row: dict) -> str:
    explicit = str(row.get("is_fault_claim") or "").strip()
    if explicit:
        return _yn(explicit)
    status = str(row.get("claim_status") or "").strip().upper()
    if (
        _yn(row.get("is_claim_fraud")) == "Y"
        or _yn(row.get("is_claim_suspicious")) == "Y"
        or _yn(row.get("is_litigation")) == "Y"
        or status in {"REPUDIATED", "DENIED", "REJECTED", "DECLINED"}
    ):
        return "Y"
    return "Y" if _stable_index(row.get("claim_id") or row.get("claim_hash_key"), 100) < 25 else "N"


def _claim_satisfaction_score_raw(row: dict) -> str:
    status = str(row.get("claim_status") or "").strip().upper()
    if status in {"OPEN", "PENDING"} and not str(row.get("claim_settlement_date") or "").strip():
        return ""
    score = 5
    if status in {"REPUDIATED", "DENIED", "REJECTED", "DECLINED"}:
        score -= 3
    elif status in {"OPEN", "PENDING"}:
        score -= 1
    if _yn(row.get("is_claim_fraud")) == "Y" or _yn(row.get("is_claim_suspicious")) == "Y":
        score -= 2
    if _yn(row.get("is_litigation")) == "Y":
        score -= 2
    if _yn(row.get("is_fault_claim")) == "Y":
        score -= 1
    if _int_value(row.get("outstanding_reserve"), 0) > 0:
        score -= 1
    if _int_value(row.get("claim_amount"), 0) >= 13000:
        score -= 1
    return str(max(0, min(5, score)))


def _claim_feedback_text(score: int) -> str:
    if score >= 4:
        return "Claim settled clearly with helpful updates"
    if score >= 2:
        return "Claim progressed with some follow-up needed"
    return "Claim experience delayed or disputed"


def _with_claim_experience(row: dict, has_policy_complaint: bool) -> dict:
    enriched = dict(row)
    enriched["is_fault_claim"] = _claim_fault_flag(enriched)
    score = _claim_satisfaction_score_raw(enriched)
    enriched["claim_satisfaction_score"] = score
    enriched["claims_feedback"] = _claim_feedback_text(_int_value(score, 0)) if score != "" else ""
    enriched["is_claim_complaint_raised"] = "Y" if has_policy_complaint else "N"
    return enriched


def _home_insured_amount(row: dict, policy: dict) -> str:
    amount = _money_value(
        row.get("home_sum_insrd", ""),
        policy.get("renewal_premium_curr", ""),
        policy.get("renewal_premium_next", ""),
    )
    if amount:
        value = float(amount)
        if value < 10000:
            value *= 250
        return f"{value:.2f}"
    return f"{100000 + _stable_index(row.get('property_ref', ''), 900000):.2f}"


def write_raw_base_prd2_variant(prd1_folder: str, raw_root: str, batch_id: str, mode: str = "base") -> str:
    """Write the base PRD2 raw feed using the workbook Source2/SAP structure.

    PRD1 is the CRM raw extract. PRD2 is a SAP-style source with different
    source IDs but matchable business attributes for later Business Vault
    mastering.
    """
    source_dir = Path(prd1_folder)
    out_dir = Path(raw_root) / mode / "prd_02" / batch_id
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    if not source_dir.exists():
        raise FileNotFoundError(f"Base PRD1 raw folder not found: {source_dir}")

    party_rows = _read_rows(source_dir, "party_master.csv")
    contact_by_party = _index(_read_rows(source_dir, "contact_point.csv"), "party_ref")
    policy_by_id = _index(_read_rows(source_dir, "policy_register.csv"), "policy_ref")

    def metadata(row: dict) -> dict:
        return {
            "batch_ref": row.get("batch_ref", batch_id),
            "pull_ts": row.get("pull_ts", ""),
            "origin_sys": "SAP",
        }

    person_rows = []
    for row in party_rows:
        contact = contact_by_party.get(row.get("party_ref", ""), {})
        is_legal = str(row.get("party_kind", "")).upper() == "LEGAL"
        person_rows.append({**metadata(row),
            "person_id": _sap_id(row.get("party_ref", "")),
            "person_type": row.get("party_kind", ""),
            "organization": row.get("legal_name", "") if is_legal else "",
            "org_establishment_date": _date_part(row.get("constitution_dt", "")) if is_legal else "",
            "first_name": row.get("given_nm", ""),
            "middle_name": _sap_middle_name(row),
            "last_name": row.get("family_nm", ""),
            "date_of_birth": _date_part(row.get("dob", "")),
            "gender": row.get("gender_txt", ""),
            "occupation": _first_value(row.get("occupation_txt", ""), row.get("job_title_txt", ""), row.get("legal_job_title_txt", "")),
            "email_address": _first_value(contact.get("email_home_txt", ""), contact.get("email_work_txt", "")),
            "phone_number": _first_value(contact.get("phone_home_txt", ""), contact.get("phone_work_txt", "")),
        })

    address_rows = [
        {**metadata(row),
            "address_id": _sap_id(row.get("address_ref", "")),
            "person_id": _sap_id(row.get("party_ref", "")),
            "address_line_1": row.get("street_txt", ""),
            "address_line_2": _sap_address_line_2(row),
            "city": row.get("city_nm", ""),
            "state": row.get("state_cd", ""),
            "country": row.get("country_cd", ""),
            "zipcode": row.get("postal_cd", ""),
        }
        for row in _read_rows(source_dir, "address_book.csv")
    ]

    product_rows = [
        {**metadata(row),
            "product_id": _sap_id(row.get("product_ref", "")),
            "product_type": row.get("product_cd", ""),
            "product_sub_type": row.get("product_line", ""),
            "product_name": row.get("product_line", ""),
            "product_start_date": _date_part(row.get("pull_ts", "")),
            "line_of_business": row.get("product_line", ""),
        }
        for row in _read_rows(source_dir, "product_catalog.csv")
    ]

    home_rows = [
        {**metadata(row),
            "home_id": _sap_id(row.get("property_ref", "")),
            "policy_id": _sap_id(row.get("policy_ref", "")),
            "product_id": _sap_id(row.get("product_ref", "")),
            "home_type": row.get("property_type_txt", ""),
            "home_location": _first_value(row.get("risk_address_txt", ""), row.get("city_nm", "")),
            "wall_type": row.get("wall_material_txt", ""),
            "roof_material": row.get("roof_material_txt", ""),
        }
        for row in _read_rows(source_dir, "property_asset.csv")
    ]

    motor_rows = [
        {**metadata(row),
            "motor_id": _sap_id(row.get("vehicle_ref", "")),
            "policy_id": _sap_id(row.get("policy_ref", "")),
            "product_id": _sap_id(row.get("product_ref", "")),
            "motor_class": row.get("vehicle_class_txt", ""),
            "motor_model": row.get("model_nm", ""),
            "motor_type": row.get("vehicle_type_txt", ""),
            "manufacturing_date": row.get("manufacture_yr", ""),
            "body_colour": row.get("body_style_txt", ""),
            "fuel_type": row.get("fuel_type_txt", ""),
            "gear_type": row.get("variant_nm", ""),
            "motor_parked_location": row.get("garage_address_txt", ""),
        }
        for row in _read_rows(source_dir, "vehicle_asset.csv")
    ]

    insured_object_rows = []
    for row in _read_rows(source_dir, "vehicle_asset.csv"):
        policy = policy_by_id.get(row.get("policy_ref", ""), {})
        insured_object_rows.append({**metadata(row),
            "insured_object_id": _sap_id(f"IO_{row.get('vehicle_ref', '')}"),
            "policy_id": _sap_id(row.get("policy_ref", "")),
            "motor_id": _sap_id(row.get("vehicle_ref", "")),
            "home_id": "",
            "insured_object_variant": row.get("vehicle_type_txt", ""),
            "insured_object_sub_variant": _first_value(row.get("model_nm", ""), row.get("variant_nm", "")),
            "insured_amount": _money_value(row.get("insured_value_amt", ""), policy.get("renewal_premium_curr", "")),
            "insured_object_begin_date": _date_part(policy.get("policy_start_dt", "")),
            "insured_object_finish_date": _date_part(policy.get("policy_end_dt", "")),
        })
    for row in _read_rows(source_dir, "property_asset.csv"):
        policy = policy_by_id.get(row.get("policy_ref", ""), {})
        insured_object_rows.append({**metadata(row),
            "insured_object_id": _sap_id(f"IO_{row.get('property_ref', '')}"),
            "policy_id": _sap_id(row.get("policy_ref", "")),
            "motor_id": "",
            "home_id": _sap_id(row.get("property_ref", "")),
            "insured_object_variant": row.get("property_type_txt", ""),
            "insured_object_sub_variant": _first_value(row.get("wall_material_txt", ""), row.get("roof_material_txt", "")),
            "insured_amount": _home_insured_amount(row, policy),
            "insured_object_begin_date": _date_part(policy.get("policy_start_dt", "")),
            "insured_object_finish_date": _date_part(policy.get("policy_end_dt", "")),
        })

    outputs = {
        "person.csv": person_rows,
        "address.csv": address_rows,
        "product.csv": product_rows,
        "home.csv": home_rows,
        "motor.csv": motor_rows,
        "insured_object.csv": insured_object_rows,
    }
    for file_name, rows in outputs.items():
        _write_rows(out_dir, file_name, rows, BASE_PRD2_SAP_TABLES[file_name])
    return str(out_dir)


def _base_table_names(base_folder: str | None) -> set[str]:
    if not base_folder:
        return set()
    base_dir = Path(base_folder)
    if not base_dir.exists():
        return set()
    return {path.name.lower() for path in base_dir.glob("*.csv")}


def _read_rows(folder: Path, name: str) -> list[dict]:
    path = folder / name
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _index(rows: list[dict], key: str) -> dict[str, dict]:
    return {row.get(key, ""): row for row in rows if row.get(key, "")}


def _write_rows(folder: Path, name: str, rows: list[dict], fieldnames: list[str]) -> None:
    with (folder / name).open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows({field: row.get(field, "") for field in fieldnames} for row in rows)


def _to_source_column(column: str) -> str:
    if column == "load_date":
        return "src_extract_ts"
    if column == "record_source":
        return "src_system"
    if column.endswith("_hash_key"):
        return f"src_{column[:-9]}_ref"
    return f"src_{column}"


def _source_ref_value(row: dict, column: str, ref_lookup: dict[str, str]) -> str:
    value = row.get(column, "")
    if value in ref_lookup:
        return ref_lookup[value]

    base_name = column[:-9] if column.endswith("_hash_key") else column
    direct_id = row.get(f"{base_name}_id", "")
    if direct_id:
        return direct_id
    if base_name == "broker" and row.get("agent_id"):
        return row["agent_id"]

    row_ids = [item_value for item_key, item_value in row.items() if item_key.endswith("_id") and item_value]
    if row_ids:
        return "|".join(row_ids)
    return value


def _source_shape_rows(rows: list[dict], ref_lookup: dict[str, str]) -> list[dict]:
    shaped = []
    for row in rows:
        shaped.append({
            _to_source_column(column): (
                _source_ref_value(row, column, ref_lookup) if column.endswith("_hash_key") else value
            )
            for column, value in row.items()
        })
    return shaped


def _read_header(folder: Path, name: str) -> list[str]:
    path = folder / name
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        return next(csv.reader(f), [])


def _merge(hub: dict | None, sat: dict | None, extra: dict | None = None) -> dict:
    row = {}
    if hub:
        row.update(hub)
    if sat:
        row.update(sat)
    if extra:
        row.update(extra)
    return row


def _id_lookup(rows: list[dict], hash_key: str, id_key: str) -> dict[str, str]:
    return {row.get(hash_key, ""): row.get(id_key, "") for row in rows if row.get(hash_key, "")}


def _id_lookup_first(rows: list[dict], hash_key: str, id_keys: list[str]) -> dict[str, str]:
    lookup = {}
    for row in rows:
        hash_value = row.get(hash_key, "")
        if not hash_value:
            continue
        for id_key in id_keys:
            id_value = row.get(id_key, "")
            if id_value:
                lookup[hash_value] = id_value
                break
    return lookup


def _add_id_lookup(target: dict[str, str], rows: list[dict], hash_key: str, id_key: str) -> None:
    target.update(_id_lookup(rows, hash_key, id_key))


def _add_id_lookup_first(target: dict[str, str], rows: list[dict], hash_key: str, id_keys: list[str]) -> None:
    target.update(_id_lookup_first(rows, hash_key, id_keys))


def write_raw_prd2_from_mlops(
    mlops_folder: str,
    raw_root: str,
    batch_id: str,
    base_folder: str | None = None,
    mode: str | None = None,
    product_folder: str = "prd_02",
    target_folder: str | None = None,
    output_prefix: str = "",
    clean_output: bool = True,
) -> str:
    """Write PRD2 raw as the enhanced/MLOps table delta over base.

    PRD1 carries the base CRM raw extract. PRD2 carries only MLOps/enhanced
    vault tables that are not already present in the base synthetic vault.
    """
    source_dir = Path(mlops_folder)
    out_dir = Path(target_folder) if target_folder else (
        Path(raw_root) / mode / product_folder / batch_id if mode else Path(raw_root) / product_folder / batch_id
    )
    if clean_output and out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not source_dir.exists():
        raise FileNotFoundError(f"MLOps source folder not found: {source_dir}")

    hub_person = _read_rows(source_dir, "hub_person.csv")
    hub_customer = _read_rows(source_dir, "hub_customer.csv")
    hub_marketing_engagement = _read_rows(source_dir, "hub_marketing_engagement.csv")
    hub_policy = _read_rows(source_dir, "hub_policy.csv")
    hub_quote = _read_rows(source_dir, "hub_quote.csv")
    hub_home = _read_rows(source_dir, "hub_home.csv")
    hub_motor = _read_rows(source_dir, "hub_motor.csv")

    ref_lookup: dict[str, str] = {}
    _add_id_lookup(ref_lookup, hub_person, "person_hash_key", "person_id")
    _add_id_lookup(ref_lookup, hub_customer, "customer_hash_key", "customer_id")
    _add_id_lookup(ref_lookup, hub_marketing_engagement, "marketing_engagement_hash_key", "marketing_engagement_id")
    _add_id_lookup(ref_lookup, hub_policy, "policy_hash_key", "policy_id")
    _add_id_lookup(ref_lookup, hub_quote, "quote_hash_key", "quote_id")
    _add_id_lookup_first(ref_lookup, hub_home, "home_hash_key", ["home_id", "insured_object_home_id"])
    _add_id_lookup_first(ref_lookup, hub_motor, "motor_hash_key", ["motor_id", "insured_object_motor_id"])

    person_id = _id_lookup(hub_person, "person_hash_key", "person_id")
    policy_id = _id_lookup(hub_policy, "policy_hash_key", "policy_id")
    quote_id = _id_lookup(hub_quote, "quote_hash_key", "quote_id")
    home_id = _id_lookup_first(hub_home, "home_hash_key", ["home_id", "insured_object_home_id"])
    motor_id = _id_lookup_first(hub_motor, "motor_hash_key", ["motor_id", "insured_object_motor_id"])

    hub_address = _read_rows(source_dir, "hub_address.csv")
    _add_id_lookup(ref_lookup, hub_address, "address_hash_key", "address_id")
    sat_address = _index(_read_rows(source_dir, "sat_address.csv"), "address_hash_key")
    person_address = _index(_read_rows(source_dir, "link_person_address.csv"), "address_hash_key")
    address_rows = [
        _merge(
            hub,
            sat_address.get(hub["address_hash_key"]),
            {"person_id": person_id.get(person_address.get(hub["address_hash_key"], {}).get("person_hash_key", ""), "")},
        )
        for hub in hub_address
    ]

    hub_broker = _read_rows(source_dir, "hub_broker.csv")
    _add_id_lookup(ref_lookup, hub_broker, "broker_hash_key", "agent_id")
    sat_broker = _index(_read_rows(source_dir, "sat_broker.csv"), "broker_hash_key")
    broker_person = _index(_read_rows(source_dir, "link_broker_person.csv"), "broker_hash_key")
    broker_rows = [
        _merge(
            hub,
            sat_broker.get(hub["broker_hash_key"]),
            {"person_id": person_id.get(broker_person.get(hub["broker_hash_key"], {}).get("person_hash_key", ""), "")},
        )
        for hub in hub_broker
    ]

    hub_campaign = _read_rows(source_dir, "hub_campaign.csv")
    _add_id_lookup(ref_lookup, hub_campaign, "campaign_hash_key", "campaign_id")
    sat_campaign = _index(_read_rows(source_dir, "sat_campaign.csv"), "campaign_hash_key")
    person_campaign = _index(_read_rows(source_dir, "link_person_campaign.csv"), "campaign_hash_key")
    campaign_rows = [
        _merge(
            hub,
            sat_campaign.get(hub["campaign_hash_key"]),
            {"person_id": person_id.get(person_campaign.get(hub["campaign_hash_key"], {}).get("person_hash_key", ""), "")},
        )
        for hub in hub_campaign
    ]

    hub_channel = _read_rows(source_dir, "hub_channel.csv")
    _add_id_lookup(ref_lookup, hub_channel, "channel_hash_key", "channel_id")
    sat_channel = _index(_read_rows(source_dir, "sat_channel.csv"), "channel_hash_key")
    channel_rows = [_merge(hub, sat_channel.get(hub["channel_hash_key"])) for hub in hub_channel]

    hub_claim = _read_rows(source_dir, "hub_claim.csv")
    _add_id_lookup(ref_lookup, hub_claim, "claim_hash_key", "claim_id")
    sat_claim = _index(_read_rows(source_dir, "sat_claim.csv"), "claim_hash_key")
    claim_policy = _index(_read_rows(source_dir, "link_claim_policy.csv"), "claim_hash_key")
    complaint_policy_links = _read_rows(source_dir, "link_complaint_policy.csv")
    policies_with_complaints = {
        row.get("policy_hash_key", "")
        for row in complaint_policy_links
        if row.get("policy_hash_key")
    }
    claim_rows = []
    for hub in hub_claim:
        policy_hash_key = claim_policy.get(hub["claim_hash_key"], {}).get("policy_hash_key", "")
        claim_rows.append(
            _with_claim_experience(
                _merge(
                    hub,
                    sat_claim.get(hub["claim_hash_key"]),
                    {"policy_id": policy_id.get(policy_hash_key, "")},
                ),
                policy_hash_key in policies_with_complaints,
            )
        )

    hub_complaint = _read_rows(source_dir, "hub_complaint.csv")
    _add_id_lookup(ref_lookup, hub_complaint, "complaint_hash_key", "complaint_id")
    sat_complaint = _index(_read_rows(source_dir, "sat_complaint.csv"), "complaint_hash_key")
    complaint_policy = _index(complaint_policy_links, "complaint_hash_key")
    complaint_regulation = _index(_read_rows(source_dir, "link_complaint_regulation.csv"), "complaint_hash_key")
    regulation_by_hash = _id_lookup(_read_rows(source_dir, "hub_regulation.csv"), "regulation_hash_key", "regulation_id")
    complaint_rows = [
        _with_complaint_feedback(_merge(
            hub,
            sat_complaint.get(hub["complaint_hash_key"]),
            {
                "policy_id": policy_id.get(complaint_policy.get(hub["complaint_hash_key"], {}).get("policy_hash_key", ""), ""),
                "regulation_id": regulation_by_hash.get(
                    complaint_regulation.get(hub["complaint_hash_key"], {}).get("regulation_hash_key", ""),
                    "",
                ),
            },
        ))
        for hub in hub_complaint
    ]

    hub_insured_object = _read_rows(source_dir, "hub_insured_object.csv")
    _add_id_lookup(ref_lookup, hub_insured_object, "insured_object_hash_key", "insured_object_id")
    sat_insured_object = _index(_read_rows(source_dir, "sat_insured_object.csv"), "insured_object_hash_key")
    policy_insured_object = _index(_read_rows(source_dir, "link_policy_insured_object.csv"), "insured_object_hash_key")
    insured_object_home = _index(_read_rows(source_dir, "link_insured_object_home.csv"), "insured_object_hash_key")
    insured_object_motor = _index(_read_rows(source_dir, "link_insured_object_motor.csv"), "insured_object_hash_key")
    insured_object_rows = [
        _merge(
            hub,
            sat_insured_object.get(hub["insured_object_hash_key"]),
            {
                "policy_id": policy_id.get(
                    policy_insured_object.get(hub["insured_object_hash_key"], {}).get("policy_hash_key", ""),
                    "",
                ),
                "home_id": home_id.get(insured_object_home.get(hub["insured_object_hash_key"], {}).get("home_hash_key", ""), ""),
                "motor_id": motor_id.get(insured_object_motor.get(hub["insured_object_hash_key"], {}).get("motor_hash_key", ""), ""),
            },
        )
        for hub in hub_insured_object
    ]

    hub_override = _read_rows(source_dir, "hub_override.csv")
    _add_id_lookup(ref_lookup, hub_override, "override_hash_key", "override_id")
    sat_override = _index(_read_rows(source_dir, "sat_override.csv"), "override_hash_key")
    policy_override = _index(_read_rows(source_dir, "link_policy_override.csv"), "override_hash_key")
    override_rows = [
        _merge(
            hub,
            sat_override.get(hub["override_hash_key"]),
            {"policy_id": policy_id.get(policy_override.get(hub["override_hash_key"], {}).get("policy_hash_key", ""), "")},
        )
        for hub in hub_override
    ]

    hub_regulation = _read_rows(source_dir, "hub_regulation.csv")
    _add_id_lookup(ref_lookup, hub_regulation, "regulation_hash_key", "regulation_id")
    sat_regulation = _index(_read_rows(source_dir, "sat_regulation.csv"), "regulation_hash_key")
    regulation_rows = [_merge(hub, sat_regulation.get(hub["regulation_hash_key"])) for hub in hub_regulation]

    broker_id = _id_lookup(hub_broker, "broker_hash_key", "agent_id")
    channel_id = _id_lookup(hub_channel, "channel_hash_key", "channel_id")
    claim_id = _id_lookup(hub_claim, "claim_hash_key", "claim_id")
    complaint_id = _id_lookup(hub_complaint, "complaint_hash_key", "complaint_id")
    address_id = _id_lookup(hub_address, "address_hash_key", "address_id")
    campaign_id = _id_lookup(hub_campaign, "campaign_hash_key", "campaign_id")
    insured_object_id = _id_lookup(hub_insured_object, "insured_object_hash_key", "insured_object_id")
    override_id = _id_lookup(hub_override, "override_hash_key", "override_id")

    policy_broker_rows = [
        _merge(row, None, {"policy_id": policy_id.get(row.get("policy_hash_key", ""), ""), "agent_id": broker_id.get(row.get("broker_hash_key", ""), "")})
        for row in _read_rows(source_dir, "link_policy_broker.csv")
    ]
    broker_person_rows = [
        _merge(row, None, {"agent_id": broker_id.get(row.get("broker_hash_key", ""), ""), "person_id": person_id.get(row.get("person_hash_key", ""), "")})
        for row in _read_rows(source_dir, "link_broker_person.csv")
    ]
    policy_channel_rows = [
        _merge(row, None, {"policy_id": policy_id.get(row.get("policy_hash_key", ""), ""), "channel_id": channel_id.get(row.get("channel_hash_key", ""), "")})
        for row in _read_rows(source_dir, "link_policy_channel.csv")
    ]
    policy_quote_rows = [
        _merge(row, None, {"policy_id": policy_id.get(row.get("policy_hash_key", ""), ""), "quote_id": quote_id.get(row.get("quote_hash_key", ""), "")})
        for row in _read_rows(source_dir, "link_policy_quote.csv")
    ]
    claim_policy_rows = [
        _merge(row, None, {"claim_id": claim_id.get(row.get("claim_hash_key", ""), ""), "policy_id": policy_id.get(row.get("policy_hash_key", ""), "")})
        for row in _read_rows(source_dir, "link_claim_policy.csv")
    ]
    complaint_policy_rows = [
        _merge(row, None, {"complaint_id": complaint_id.get(row.get("complaint_hash_key", ""), ""), "policy_id": policy_id.get(row.get("policy_hash_key", ""), "")})
        for row in _read_rows(source_dir, "link_complaint_policy.csv")
    ]
    complaint_regulation_rows = [
        _merge(row, None, {"complaint_id": complaint_id.get(row.get("complaint_hash_key", ""), ""), "regulation_id": regulation_by_hash.get(row.get("regulation_hash_key", ""), "")})
        for row in _read_rows(source_dir, "link_complaint_regulation.csv")
    ]
    person_address_rows = [
        _merge(row, None, {"person_id": person_id.get(row.get("person_hash_key", ""), ""), "address_id": address_id.get(row.get("address_hash_key", ""), "")})
        for row in _read_rows(source_dir, "link_person_address.csv")
    ]
    person_campaign_rows = [
        _merge(row, None, {"person_id": person_id.get(row.get("person_hash_key", ""), ""), "campaign_id": campaign_id.get(row.get("campaign_hash_key", ""), "")})
        for row in _read_rows(source_dir, "link_person_campaign.csv")
    ]
    policy_insured_object_rows = [
        _merge(row, None, {"policy_id": policy_id.get(row.get("policy_hash_key", ""), ""), "insured_object_id": insured_object_id.get(row.get("insured_object_hash_key", ""), "")})
        for row in _read_rows(source_dir, "link_policy_insured_object.csv")
    ]
    policy_override_rows = [
        _merge(row, None, {"policy_id": policy_id.get(row.get("policy_hash_key", ""), ""), "override_id": override_id.get(row.get("override_hash_key", ""), "")})
        for row in _read_rows(source_dir, "link_policy_override.csv")
    ]
    insured_object_home_rows = [
        _merge(row, None, {"insured_object_id": insured_object_id.get(row.get("insured_object_hash_key", ""), ""), "home_id": home_id.get(row.get("home_hash_key", ""), "")})
        for row in _read_rows(source_dir, "link_insured_object_home.csv")
    ]
    insured_object_motor_rows = [
        _merge(row, None, {"insured_object_id": insured_object_id.get(row.get("insured_object_hash_key", ""), ""), "motor_id": motor_id.get(row.get("motor_hash_key", ""), "")})
        for row in _read_rows(source_dir, "link_insured_object_motor.csv")
    ]
    quote_broker_rows = [
        _merge(row, None, {"quote_id": quote_id.get(row.get("quote_hash_key", ""), ""), "agent_id": broker_id.get(row.get("broker_hash_key", ""), "")})
        for row in _read_rows(source_dir, "link_quote_broker.csv")
    ]
    quote_channel_rows = [
        _merge(row, None, {"quote_id": quote_id.get(row.get("quote_hash_key", ""), ""), "channel_id": channel_id.get(row.get("channel_hash_key", ""), "")})
        for row in _read_rows(source_dir, "link_quote_channel.csv")
    ]

    base_dir = Path(base_folder) if base_folder else None

    def enrichment_rows(table_name: str, key_column: str) -> list[dict]:
        mlops_header = _read_header(source_dir, table_name)
        base_header = [column.strip().lower() for column in _read_header(base_dir, table_name)] if base_dir else []
        reserved = {key_column, "load_date"}
        extra_columns = [
            column
            for column in mlops_header
            if column not in base_header and column not in reserved
        ]
        columns = [key_column, "load_date", *extra_columns]
        return [{column: row.get(column, "") for column in columns} for row in _read_rows(source_dir, table_name)]

    policy_enrichment_rows = enrichment_rows("sat_policy.csv", "policy_hash_key")
    customer_enrichment_rows = enrichment_rows("sat_customer.csv", "customer_hash_key")
    marketing_engagement_enrichment_rows = enrichment_rows(
        "sat_marketing_engagement.csv",
        "marketing_engagement_hash_key",
    )
    motor_enrichment_rows = enrichment_rows("sat_motor.csv", "motor_hash_key")

    # The entity registers are the business entities added by enhanced/MLOps.
    # Bridge/enrichment extracts are required to rebuild the wider vault from
    # PRD1 without losing relationships or added satellite fields.
    outputs = {
        "address_book.csv": address_rows,
        "broker_book.csv": broker_rows,
        "campaign_register.csv": campaign_rows,
        "channel_catalog.csv": channel_rows,
        "claim_register.csv": claim_rows,
        "complaint_register.csv": complaint_rows,
        "insured_object_register.csv": insured_object_rows,
        "override_register.csv": override_rows,
        "regulation_register.csv": regulation_rows,
        "broker_person_bridge.csv": broker_person_rows,
        "policy_broker_bridge.csv": policy_broker_rows,
        "policy_channel_bridge.csv": policy_channel_rows,
        "policy_quote_bridge.csv": policy_quote_rows,
        "claim_policy_bridge.csv": claim_policy_rows,
        "complaint_policy_bridge.csv": complaint_policy_rows,
        "complaint_regulation_bridge.csv": complaint_regulation_rows,
        "person_address_bridge.csv": person_address_rows,
        "person_campaign_bridge.csv": person_campaign_rows,
        "policy_insured_object_bridge.csv": policy_insured_object_rows,
        "policy_override_bridge.csv": policy_override_rows,
        "insured_object_home_bridge.csv": insured_object_home_rows,
        "insured_object_motor_bridge.csv": insured_object_motor_rows,
        "quote_broker_bridge.csv": quote_broker_rows,
        "quote_channel_bridge.csv": quote_channel_rows,
        "policy_enrichment.csv": policy_enrichment_rows,
        "customer_enrichment.csv": customer_enrichment_rows,
        "marketing_engagement_enrichment.csv": marketing_engagement_enrichment_rows,
        "motor_enrichment.csv": motor_enrichment_rows,
    }

    for file_name, rows in outputs.items():
        source_rows = _source_shape_rows(rows, ref_lookup)
        fieldnames = list(source_rows[0].keys()) if source_rows else []
        if not fieldnames:
            continue
        _write_rows(out_dir, f"{output_prefix}{file_name}", source_rows, fieldnames)

    return str(out_dir)


def copy_raw_prd2_folder(
    mlops_folder: str,
    raw_root: str,
    batch_id: str,
    base_folder: str | None = None,
    mode: str | None = None,
    product_folder: str = "prd_02",
) -> str:
    """Backward-compatible entrypoint for writing source-style PRD2 raw files."""
    return write_raw_prd2_from_mlops(
        mlops_folder,
        raw_root,
        batch_id,
        base_folder=base_folder,
        mode=mode,
        product_folder=product_folder,
    )


def write_source1_delta_into_prd1(
    mlops_folder: str,
    prd1_folder: str,
    batch_id: str,
    base_folder: str | None = None,
    addon_folder: str = "addons",
) -> list[str]:
    """Write enhanced source tables under PRD1 without colliding with CRM files."""
    target_folder = Path(prd1_folder) / addon_folder
    out_dir = write_raw_prd2_from_mlops(
        mlops_folder,
        raw_root="",
        batch_id=batch_id,
        base_folder=base_folder,
        target_folder=str(target_folder),
        output_prefix="",
        clean_output=True,
    )
    return [str(path) for path in sorted(Path(out_dir).glob("*.csv"))]


VAULT_READY_ENTITY_ADDONS = {
    "address_book.csv": "enhanced_address_book.csv",
    "broker_book.csv": "broker_book.csv",
    "campaign_register.csv": "campaign_register.csv",
    "channel_catalog.csv": "channel_catalog.csv",
    "claim_register.csv": "claim_register.csv",
    "complaint_register.csv": "complaint_register.csv",
    "insured_object_register.csv": "insured_object_register.csv",
    "override_register.csv": "override_register.csv",
    "regulation_register.csv": "regulation_register.csv",
}

VAULT_READY_RELATIONSHIP_GROUPS = {
    "enhanced_person_relationships.csv": [
        "broker_person_bridge.csv",
        "person_address_bridge.csv",
        "person_campaign_bridge.csv",
    ],
    "enhanced_policy_relationships.csv": [
        "claim_policy_bridge.csv",
        "complaint_policy_bridge.csv",
        "complaint_regulation_bridge.csv",
        "insured_object_home_bridge.csv",
        "insured_object_motor_bridge.csv",
        "policy_broker_bridge.csv",
        "policy_channel_bridge.csv",
        "policy_insured_object_bridge.csv",
        "policy_override_bridge.csv",
        "policy_quote_bridge.csv",
        "quote_broker_bridge.csv",
        "quote_channel_bridge.csv",
    ],
}

VAULT_READY_ENRICHMENT_GROUPS = {
    "enhanced_enrichments.csv": [
        "customer_enrichment.csv",
        "marketing_engagement_enrichment.csv",
        "motor_enrichment.csv",
        "policy_enrichment.csv",
    ],
}


def _read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _write_grouped_rows(addon_files: dict[str, Path], out_dir: Path, output_file: str, logical_files: list[str]) -> None:
    grouped_rows: list[dict] = []
    columns = ["source_extract"]
    seen_columns = {"source_extract"}
    for logical_file in logical_files:
        source_path = addon_files.get(logical_file)
        if not source_path:
            continue
        for row in _read_csv_rows(source_path):
            grouped_row = {"source_extract": logical_file, **row}
            grouped_rows.append(grouped_row)
            for column in grouped_row:
                if column not in seen_columns:
                    columns.append(column)
                    seen_columns.add(column)

    with (out_dir / output_file).open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in grouped_rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _ensure_raw_metadata_columns(folder: Path, batch_id: str) -> None:
    metadata_columns = ["batch_ref", "pull_ts", "origin_sys"]
    for path in sorted(folder.glob("*.csv")):
        rows = _read_csv_rows(path)
        with path.open("r", newline="", encoding="utf-8") as f:
            fieldnames = next(csv.reader(f), [])
        if all(column in fieldnames for column in metadata_columns):
            continue

        output_fields = metadata_columns + [column for column in fieldnames if column not in metadata_columns]
        enriched_rows = []
        for row in rows:
            enriched = dict(row)
            enriched["batch_ref"] = enriched.get("batch_ref") or batch_id
            enriched["pull_ts"] = enriched.get("pull_ts") or enriched.get("src_extract_ts") or ""
            enriched["origin_sys"] = enriched.get("origin_sys") or enriched.get("src_system") or "CRM"
            enriched_rows.append(enriched)

        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=output_fields)
            writer.writeheader()
            for row in enriched_rows:
                writer.writerow({column: row.get(column, "") for column in output_fields})


def write_enhanced_vault_ready_28(prd1_folder: str, output_folder: str = "vault_ready_28") -> str:
    """Create a 28-file enhanced raw package that can rebuild the full vault."""
    prd1_dir = Path(prd1_folder)
    addons_dir = prd1_dir / "addons"
    out_dir = prd1_dir / output_folder
    if not prd1_dir.exists():
        raise FileNotFoundError(f"Enhanced PRD1 folder not found: {prd1_dir}")
    if not addons_dir.exists():
        raise FileNotFoundError(f"Enhanced PRD1 addons folder not found: {addons_dir}")

    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    root_files = sorted(path for path in prd1_dir.glob("*.csv") if not path.name.startswith("source1_"))
    addon_files = {path.name: path for path in addons_dir.glob("*.csv")}

    for source_file in root_files:
        shutil.copy2(source_file, out_dir / source_file.name)

    for source_name, output_name in VAULT_READY_ENTITY_ADDONS.items():
        source_file = addon_files.get(source_name)
        if source_file:
            shutil.copy2(source_file, out_dir / output_name)

    for output_file, logical_files in VAULT_READY_RELATIONSHIP_GROUPS.items():
        _write_grouped_rows(addon_files, out_dir, output_file, logical_files)
    for output_file, logical_files in VAULT_READY_ENRICHMENT_GROUPS.items():
        _write_grouped_rows(addon_files, out_dir, output_file, logical_files)

    _ensure_raw_metadata_columns(out_dir, prd1_dir.name)

    output_count = len(list(out_dir.glob("*.csv")))
    if output_count != 28:
        raise RuntimeError(f"Enhanced vault-ready raw must contain 28 files, found {output_count} in {out_dir}")
    return str(out_dir)


def write_enhanced_consolidated_raw_vault(
    vault_ready_folder: str,
    prd2_folder: str,
    raw_root: str,
    batch_id: str,
    mode: str = "enhanced",
    output_folder: str = "raw_vault",
) -> str:
    """Write one flat enhanced raw-vault package from source-1 and source-2 raw.

    The enhanced source-1 contract is the 28-file ``vault_ready_28`` package.
    Source-2 is the SAP-style six-table PRD2 feed. Keeping the consolidated
    folder flat makes it easy to upload as one bronze/raw-vault input while
    preserving the individual source table names.
    """
    source1_dir = Path(vault_ready_folder)
    source2_dir = Path(prd2_folder)
    out_dir = Path(raw_root) / mode / output_folder / batch_id

    if not source1_dir.exists():
        raise FileNotFoundError(f"Enhanced vault-ready folder not found: {source1_dir}")
    if not source2_dir.exists():
        raise FileNotFoundError(f"Enhanced PRD2 folder not found: {source2_dir}")

    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for source_file in sorted(source1_dir.glob("*.csv")):
        shutil.copy2(source_file, out_dir / source_file.name)

    for file_name in BASE_PRD2_SAP_TABLES:
        source_file = source2_dir / file_name
        if not source_file.exists():
            raise FileNotFoundError(f"Enhanced PRD2 file missing: {source_file}")
        shutil.copy2(source_file, out_dir / source_file.name)

    expected_count = 28 + len(BASE_PRD2_SAP_TABLES)
    output_count = len(list(out_dir.glob("*.csv")))
    if output_count != expected_count:
        raise RuntimeError(
            f"Enhanced raw_vault must contain {expected_count} files, found {output_count} in {out_dir}"
        )
    return str(out_dir)
