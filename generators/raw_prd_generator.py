from __future__ import annotations

import csv
import shutil
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
            "middle_name": "",
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
            "address_line_2": "",
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

    outputs = {
        "person.csv": person_rows,
        "address.csv": address_rows,
        "product.csv": product_rows,
        "home.csv": home_rows,
        "motor.csv": motor_rows,
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
) -> str:
    """Write PRD2 raw as the enhanced/MLOps table delta over base.

    PRD1 carries the base CRM raw extract. PRD2 carries only MLOps/enhanced
    vault tables that are not already present in the base synthetic vault.
    """
    source_dir = Path(mlops_folder)
    out_dir = Path(raw_root) / mode / product_folder / batch_id if mode else Path(raw_root) / product_folder / batch_id
    if out_dir.exists():
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
    claim_rows = [
        _merge(
            hub,
            sat_claim.get(hub["claim_hash_key"]),
            {"policy_id": policy_id.get(claim_policy.get(hub["claim_hash_key"], {}).get("policy_hash_key", ""), "")},
        )
        for hub in hub_claim
    ]

    hub_complaint = _read_rows(source_dir, "hub_complaint.csv")
    _add_id_lookup(ref_lookup, hub_complaint, "complaint_hash_key", "complaint_id")
    sat_complaint = _index(_read_rows(source_dir, "sat_complaint.csv"), "complaint_hash_key")
    complaint_policy = _index(_read_rows(source_dir, "link_complaint_policy.csv"), "complaint_hash_key")
    complaint_regulation = _index(_read_rows(source_dir, "link_complaint_regulation.csv"), "complaint_hash_key")
    regulation_by_hash = _id_lookup(_read_rows(source_dir, "hub_regulation.csv"), "regulation_hash_key", "regulation_id")
    complaint_rows = [
        _merge(
            hub,
            sat_complaint.get(hub["complaint_hash_key"]),
            {
                "policy_id": policy_id.get(complaint_policy.get(hub["complaint_hash_key"], {}).get("policy_hash_key", ""), ""),
                "regulation_id": regulation_by_hash.get(
                    complaint_regulation.get(hub["complaint_hash_key"], {}).get("regulation_hash_key", ""),
                    "",
                ),
            },
        )
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

    # The 7 entity registers are the business entities added by PRD2. The
    # remaining bridge/enrichment extracts are required to rebuild the vault
    # from PRD1+PRD2 without losing relationships or added satellite fields.
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
        _write_rows(out_dir, file_name, source_rows, fieldnames)

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
