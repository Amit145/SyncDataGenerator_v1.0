from __future__ import annotations

import copy
import os
import random
from datetime import datetime, timedelta

from config.runConfig import OUTPUT_BASE, SYNTHETIC_DATA
from helper.csv_writer import append_csv, normalize_csv_streaming
from helper.key_factory import get_folder_run_id, get_run_id
from helper.source_context_builder import build_source_context


BASE_CONTEXT_TABLES = [
    ("Hub_Person.csv", "hub_person_rows"),
    ("Hub_Natural_Person.csv", "hub_nat"),
    ("Hub_Legal_Person.csv", "hub_leg"),
    ("Hub_Product.csv", "hub_prod_rows"),
    ("Hub_Lead.csv", "hub_lead_rows"),
    ("Hub_Customer.csv", "hub_cust_rows"),
    ("Hub_Identities.csv", "hub_id_rows"),
    ("Hub_Contact.csv", "hub_con_rows"),
    ("Hub_Consent.csv", "hub_cns_rows"),
    ("Hub_Account.csv", "hub_acc_rows"),
    ("Hub_Marketing_Preference.csv", "hub_mpr_rows"),
    ("Hub_Marketing_Engagement.csv", "hub_men_rows"),
    ("Hub_Quote.csv", "hub_quo_rows"),
    ("Hub_Policy.csv", "hub_pol_rows"),
    ("Hub_Motor.csv", "hub_mot_rows"),
    ("Hub_Home.csv", "hub_home_rows"),
    ("Hub_Home_Address.csv", "hub_addr_rows"),
    ("Sat_Natural_Person.csv", "sat_nat"),
    ("Sat_Legal_Person.csv", "sat_leg"),
    ("Sat_Person.csv", "sat_per"),
    ("Sat_Lead.csv", "sat_lea"),
    ("Sat_Customer.csv", "sat_cus"),
    ("Sat_Identities.csv", "sat_eci"),
    ("Sat_Contact.csv", "sat_con"),
    ("Sat_Consent.csv", "sat_cns"),
    ("Sat_Account.csv", "sat_acc"),
    ("Sat_Marketing_Preference.csv", "sat_mpr"),
    ("Sat_Marketing_Engagement.csv", "sat_men"),
    ("Sat_Quote.csv", "sat_quo"),
    ("Sat_Policy.csv", "sat_pol"),
    ("Sat_Motor.csv", "sat_mot"),
    ("Sat_Home.csv", "sat_hom"),
    ("Sat_Home_Address.csv", "sat_adr"),
    ("Sat_Product.csv", "sat_product_rows"),
]

EXPECTED_LINKS = [
    "Link_Person_Natural_Person",
    "Link_Person_Legal_Person",
    "Link_Person_Identities",
    "Link_Person_Contact",
    "Link_Person_Consent",
    "Link_Person_Home_Address",
    "Link_Person_Account",
    "Link_Person_Lead",
    "Link_Person_Marketing_Preference",
    "Link_Person_Marketing_Engagement",
    "Link_Customer_Person",
    "Link_Customer_Lead",
    "Link_Quote_Person",
    "Link_Quote_Product",
    "Link_Policy_Customer",
    "Link_Policy_Product",
    "Link_Product_Motor",
    "Link_Product_Home",
]


def _chunk_sizes(total_people: int, chunk_size: int):
    remaining = total_people
    while remaining > 0:
        current = min(chunk_size, remaining)
        remaining -= current
        yield current


def _write_rows(output_dir: str, file_name: str, rows: list[dict], counts: dict[str, int]):
    append_csv(output_dir, file_name, rows)
    counts[file_name.lower()] = counts.get(file_name.lower(), 0) + len(rows)


def _write_context(output_dir: str, context: dict, counts: dict[str, int], link_date: str):
    for file_name, context_key in BASE_CONTEXT_TABLES:
        _write_rows(output_dir, file_name, context.get(context_key, []), counts)

    links = context.get("links", {})
    for table_name in EXPECTED_LINKS:
        rows = []
        for row in links.get(table_name, []):
            row = dict(row)
            row["Load Date"] = link_date
            rows.append(row)
        _write_rows(output_dir, f"{table_name}.csv", rows, counts)


def generate_streaming_base(
    cfg: dict,
    total_people: int,
    chunk_size: int,
    business_start_date: str,
    as_of_date: str | None,
    hub_date: str,
    link_date: str,
    sat_date: str,
    normalize_output: bool = True,
) -> dict:
    if total_people <= 0:
        raise ValueError("total_people must be greater than 0")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than 0")

    base_seed = int(cfg["run_settings"]["random_seed"])
    folder_run_id = get_folder_run_id()
    base_run_id = get_run_id(base_seed)
    output_dir = os.path.join(OUTPUT_BASE, folder_run_id)
    synthetic_dir = os.path.join(SYNTHETIC_DATA, folder_run_id)
    counts: dict[str, int] = {}

    start_time = datetime.now()
    generated_people = 0

    for chunk_index, current_size in enumerate(_chunk_sizes(total_people, chunk_size), 1):
        cfg_chunk = copy.deepcopy(cfg)
        cfg_chunk["run_settings"]["total_people"] = current_size
        cfg_chunk["run_settings"]["random_seed"] = base_seed + chunk_index

        chunk_run_id = f"{base_run_id}_chunk_{chunk_index:06d}"
        random.seed(base_seed + chunk_index)
        context = build_source_context(
            cfg=cfg_chunk,
            run_id=chunk_run_id,
            hub_date=hub_date,
            link_date=link_date,
            sat_date=sat_date,
            business_start_date=business_start_date,
            as_of_date=as_of_date,
            seed_override=base_seed + chunk_index,
        )
        _write_context(output_dir, context, counts, link_date)

        generated_people += current_size
        elapsed = datetime.now() - start_time
        print(
            f"STREAM chunk={chunk_index} people={generated_people}/{total_people} "
            f"chunk_size={current_size} elapsed={elapsed}"
        )

        del context

    if normalize_output:
        normalize_csv_streaming(output_dir, synthetic_dir)

    return {
        "folder_run_id": folder_run_id,
        "output_dir": output_dir,
        "synthetic_dir": synthetic_dir if normalize_output else None,
        "total_people": total_people,
        "chunk_size": chunk_size,
        "counts": counts,
    }
