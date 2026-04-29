from __future__ import annotations

import copy
import random
from datetime import datetime, timedelta

from enums.product_catalog import (
    get_product_codes_for_person_type,
    get_product_weights_for_person_type,
)
from generators.lifecycle_generator import hub_customer
from generators.supporting_generator import hub_account
from generators.transaction_generator import hub_assets_from_policies, hub_policy
from helper.hub_builder import build_hubs
from helper.link_builder import build_links, make_link
from helper.satellite_builder import (
    apply_customer_ratings,
    apply_customer_segments,
    apply_lead_interest_levels,
    sat_account,
    sat_consent,
    sat_contact,
    sat_customer,
    sat_home,
    sat_home_address,
    sat_identities,
    sat_lead,
    sat_legal_person,
    sat_marketing_engagement,
    sat_marketing_preference,
    sat_motor,
    sat_natural_person,
    sat_person,
    sat_policy,
    sat_product,
    sat_quote,
)


def build_default_source_context(
    cfg: dict,
    run_id: str,
    business_start_date: str = "2020-01-01",
    as_of_date: str | None = None,
    seed_offset: int = 0,
) -> dict:
    base_seed = cfg["run_settings"]["random_seed"] + seed_offset
    random.seed(base_seed)
    root = datetime.now().replace(microsecond=0)
    hub_dt = root + timedelta(days=random.randint(0, 100))
    link_dt = hub_dt + timedelta(days=5)
    sat_dt = link_dt + timedelta(days=5)
    return build_source_context(
        cfg=cfg,
        run_id=run_id,
        hub_date=hub_dt.isoformat(),
        link_date=link_dt.isoformat(),
        sat_date=sat_dt.isoformat(),
        business_start_date=business_start_date,
        as_of_date=as_of_date,
        seed_override=base_seed,
    )


def build_source_context(
    cfg: dict,
    run_id: str,
    hub_date: str,
    link_date: str,
    sat_date: str,
    business_start_date: str,
    as_of_date: str | None = None,
    seed_override: int | None = None,
) -> dict:
    cfg_local = copy.deepcopy(cfg)
    if seed_override is not None:
        cfg_local["run_settings"]["random_seed"] = seed_override
    random.seed(cfg_local["run_settings"]["random_seed"])

    ctx = build_hubs(cfg_local, run_id, hub_date)

    hub_person_rows = ctx["hub_person_rows"]
    person_hks = ctx["person_hks"]
    person_type = ctx["person_type"]

    hub_nat = ctx["hub_nat"]
    hub_leg = ctx["hub_leg"]
    person_to_nat = ctx["person_to_nat"]
    person_to_leg = ctx["person_to_leg"]

    hub_prod_rows = ctx["hub_prod_rows"]
    prod_hk_by_code = ctx["prod_hk_by_code"]
    product_code_by_hk = ctx["product_code_by_hk"]

    hub_lead_rows = ctx["hub_lead_rows"]
    person_to_lead = ctx["person_to_lead"]

    hub_id_rows = ctx["hub_id_rows"]
    person_to_identity = ctx["person_to_identity"]

    hub_con_rows = ctx["hub_con_rows"]
    person_to_contact = ctx["person_to_contact"]

    hub_cns_rows = ctx["hub_cns_rows"]
    person_to_consent = ctx["person_to_consent"]

    hub_mpr_rows = ctx["hub_mpr_rows"]
    person_to_mpr = ctx["person_to_mpr"]

    hub_men_rows = ctx["hub_men_rows"]
    person_to_men = ctx["person_to_men"]

    hub_quo_rows = ctx["hub_quo_rows"]
    person_to_quote = ctx["person_to_quote"]

    hub_addr_rows = ctx["hub_addr_rows"]
    person_to_home_address = ctx["person_to_home_address"]

    quote_to_product_id = {}
    for person_hk, quote_hks in person_to_quote.items():
        current_person_type = person_type[person_hk]
        eligible_codes = get_product_codes_for_person_type(current_person_type)
        eligible_weights = get_product_weights_for_person_type(current_person_type)
        assigned_codes = []

        for quote_hk in quote_hks:
            remaining_codes = [code for code in eligible_codes if code not in assigned_codes]
            if remaining_codes:
                remaining_weights = [
                    eligible_weights[eligible_codes.index(code)]
                    for code in remaining_codes
                ]
                selected_code = random.choices(remaining_codes, remaining_weights)[0]
            else:
                selected_code = random.choices(eligible_codes, eligible_weights)[0]
            quote_to_product_id[quote_hk] = selected_code
            assigned_codes.append(selected_code)

    hub_pol_rows, policy_person_map, policy_to_quote_map = hub_policy(
        person_to_quote, run_id, hub_date
    )
    policy_holder_persons = set(policy_person_map.keys())

    hub_acc_rows, person_to_account = hub_account(policy_holder_persons, run_id, hub_date)
    hub_cust_rows, person_to_customer = hub_customer(policy_holder_persons, run_id, hub_date)

    policy_to_person_map = {}
    for person_hk, policy_hks in policy_person_map.items():
        for policy_hk in policy_hks:
            policy_to_person_map[policy_hk] = person_hk

    policy_to_product_id = {
        policy_hk: quote_to_product_id[quote_hk]
        for policy_hk, quote_hk in policy_to_quote_map.items()
    }

    hub_mot_rows, hub_home_rows, _, policy_to_motor, policy_to_home, home_to_addr = \
        hub_assets_from_policies(policy_to_product_id, run_id, hub_date)

    link_quote_product = [
        make_link(
            "Link_Quote_Product",
            {
                "Quote Hash Key": quote_hk,
                "Product Hash Key": prod_hk_by_code[product_id],
            },
            link_date,
        )
        for quote_hk, product_id in quote_to_product_id.items()
    ]

    links = build_links(
        person_to_nat,
        person_to_leg,
        person_to_lead,
        person_to_customer,
        person_to_identity,
        person_to_contact,
        person_to_consent,
        person_to_home_address,
        person_to_mkt_pref=person_to_mpr,
        person_to_mkt_eng=person_to_men,
        person_to_account=person_to_account,
        person_to_quote=person_to_quote,
        policy_person_map=policy_person_map,
        policy_to_product_id=policy_to_product_id,
        policy_to_motor=policy_to_motor,
        policy_to_home=policy_to_home,
        home_to_addr=home_to_addr,
        product_hk_by_id=prod_hk_by_code,
        link_load_date=link_date,
    )
    links["Link_Quote_Product"] = link_quote_product

    sat_nat = sat_natural_person(person_to_nat, sat_date)
    sat_leg = sat_legal_person(person_to_leg, sat_date)
    sat_per = sat_person(person_hks, sat_date, person_type, person_to_lead, person_to_consent)
    sat_lea = sat_lead(
        person_to_lead,
        sat_date,
        business_start_date=business_start_date,
        as_of_date=as_of_date,
    )
    sat_eci = sat_identities(person_to_identity, sat_date)
    sat_con = sat_contact(person_to_contact, sat_date)
    sat_cns = sat_consent(person_to_consent, sat_date)
    sat_acc = sat_account(person_to_account, sat_date)
    sat_mpr = sat_marketing_preference(person_to_mpr, sat_date)
    sat_men = sat_marketing_engagement(person_to_men, sat_date)
    sat_quo = sat_quote(person_to_quote, sat_date)

    account_to_person_map = {}
    for person_hk, account_hks in person_to_account.items():
        for account_hk in (account_hks if isinstance(account_hks, list) else [account_hks]):
            account_to_person_map[account_hk] = person_hk

    person_account_status_by_person = {}
    for row in sat_acc:
        account_hk = row.get("Account Hash Key")
        account_status = row.get("Account Status")
        if not account_hk or not account_status:
            continue
        person_hk = account_to_person_map.get(account_hk)
        if person_hk:
            person_account_status_by_person[person_hk] = account_status

    lead_to_person_map = {}
    for person_hk, lead_hks in person_to_lead.items():
        for lead_hk in lead_hks:
            lead_to_person_map[lead_hk] = person_hk

    latest_lead_converted_by_person = {}
    for row in sat_lea:
        lead_hk = row.get("Lead Hash Key")
        converted_date = row.get("Converted Date")
        if not lead_hk or not converted_date:
            continue
        person_hk = lead_to_person_map.get(lead_hk)
        if not person_hk:
            continue
        if person_hk not in latest_lead_converted_by_person:
            latest_lead_converted_by_person[person_hk] = converted_date
        elif converted_date > latest_lead_converted_by_person[person_hk]:
            latest_lead_converted_by_person[person_hk] = converted_date

    all_policy_hks = [r["Policy Hash Key"] for r in hub_pol_rows]
    sat_pol = sat_policy(
        all_policy_hks,
        sat_date,
        business_start_date=business_start_date,
        as_of_date=as_of_date,
        policy_to_person_map=policy_to_person_map,
        latest_lead_converted_by_person=latest_lead_converted_by_person,
        person_account_status_by_person=person_account_status_by_person,
    )
    sat_lea = apply_lead_interest_levels(
        sat_lea,
        person_to_lead,
        policy_holder_persons=policy_holder_persons,
        quote_persons=set(person_to_quote.keys()),
        engaged_persons=set(person_to_men.keys()),
    )

    earliest_policy_start_by_person = {}
    for row in sat_pol:
        policy_hk = row.get("Policy Hash Key")
        if not policy_hk:
            continue
        person_hk = policy_to_person_map.get(policy_hk)
        if not person_hk:
            continue
        policy_start = row.get("Policy Start Date")
        if not policy_start:
            continue
        if person_hk not in earliest_policy_start_by_person:
            earliest_policy_start_by_person[person_hk] = policy_start
        elif policy_start < earliest_policy_start_by_person[person_hk]:
            earliest_policy_start_by_person[person_hk] = policy_start

    for row in sat_lea:
        lead_hk = row.get("Lead Hash Key")
        converted_date = row.get("Converted Date")
        if not lead_hk or not converted_date:
            continue
        person_hk = lead_to_person_map.get(lead_hk)
        if not person_hk:
            continue
        earliest_policy_start = earliest_policy_start_by_person.get(person_hk)
        if not earliest_policy_start:
            continue
        converted_dt = datetime.fromisoformat(converted_date)
        earliest_policy_dt = datetime.fromisoformat(earliest_policy_start)
        min_allowed_converted_dt = earliest_policy_dt - timedelta(days=1)
        if converted_dt > min_allowed_converted_dt:
            row["Converted Date"] = min_allowed_converted_dt.strftime("%Y-%m-%d %H:%M:%S")

    sat_cus = sat_customer(
        person_to_customer,
        sat_date,
        business_start_date=business_start_date,
        as_of_date=as_of_date,
        earliest_policy_start_by_person=earliest_policy_start_by_person,
    )
    sat_cus = apply_customer_segments(
        sat_cus,
        person_to_customer,
        person_to_account_hk=person_to_account,
        sat_policy_rows=sat_pol,
        sat_account_rows=sat_acc,
        policy_to_person_map=policy_to_person_map,
    )
    sat_cus = apply_customer_ratings(
        sat_cus,
        person_to_customer,
        person_to_account_hk=person_to_account,
        sat_policy_rows=sat_pol,
        sat_account_rows=sat_acc,
        policy_to_person_map=policy_to_person_map,
    )

    motor_hks = [r["Motor Hash Key"] for r in hub_mot_rows]
    home_hks = [r["Home Hash Key"] for r in hub_home_rows]
    addr_hks = list({r["Home Address Hash Key"] for r in hub_addr_rows}.union(set(home_to_addr.values())))
    sat_adr = sat_home_address(addr_hks, sat_date)
    sat_hom = sat_home(home_hks, sat_date, home_to_addr)

    motor_to_addr = {}
    for policy_hk, motor_hk in policy_to_motor.items():
        home_hk = policy_to_home.get(policy_hk)
        if home_hk:
            addr_hk = home_to_addr.get(home_hk)
            if addr_hk:
                motor_to_addr[motor_hk] = addr_hk
    fallback_addr = addr_hks[0] if addr_hks else None
    for motor_hk in motor_hks:
        motor_to_addr.setdefault(motor_hk, fallback_addr)
    sat_mot = sat_motor(motor_hks, sat_date, motor_to_addr)

    extract_ts = (datetime.fromisoformat(hub_date) - timedelta(days=7)).isoformat()

    return {
        "extract_ts": extract_ts,
        "hub_person_rows": hub_person_rows,
        "person_hks": person_hks,
        "person_type": person_type,
        "hub_nat": hub_nat,
        "hub_leg": hub_leg,
        "person_to_nat": person_to_nat,
        "person_to_leg": person_to_leg,
        "hub_prod_rows": hub_prod_rows,
        "prod_hk_by_code": prod_hk_by_code,
        "product_code_by_hk": product_code_by_hk,
        "hub_lead_rows": hub_lead_rows,
        "person_to_lead": person_to_lead,
        "hub_id_rows": hub_id_rows,
        "person_to_identity": person_to_identity,
        "hub_con_rows": hub_con_rows,
        "person_to_contact": person_to_contact,
        "hub_cns_rows": hub_cns_rows,
        "person_to_consent": person_to_consent,
        "hub_mpr_rows": hub_mpr_rows,
        "person_to_mpr": person_to_mpr,
        "hub_men_rows": hub_men_rows,
        "person_to_men": person_to_men,
        "hub_quo_rows": hub_quo_rows,
        "person_to_quote": person_to_quote,
        "hub_addr_rows": hub_addr_rows,
        "person_to_home_address": person_to_home_address,
        "hub_pol_rows": hub_pol_rows,
        "policy_person_map": policy_person_map,
        "policy_to_quote_map": policy_to_quote_map,
        "hub_acc_rows": hub_acc_rows,
        "person_to_account": person_to_account,
        "hub_cust_rows": hub_cust_rows,
        "person_to_customer": person_to_customer,
        "policy_to_product_id": policy_to_product_id,
        "hub_mot_rows": hub_mot_rows,
        "hub_home_rows": hub_home_rows,
        "policy_to_motor": policy_to_motor,
        "policy_to_home": policy_to_home,
        "home_to_addr": home_to_addr,
        "quote_to_product_id": quote_to_product_id,
        "links": links,
        "sat_nat": sat_nat,
        "sat_leg": sat_leg,
        "sat_per": sat_per,
        "sat_lea": sat_lea,
        "sat_cus": sat_cus,
        "sat_eci": sat_eci,
        "sat_con": sat_con,
        "sat_cns": sat_cns,
        "sat_acc": sat_acc,
        "sat_mpr": sat_mpr,
        "sat_men": sat_men,
        "sat_quo": sat_quo,
        "sat_pol": sat_pol,
        "sat_mot": sat_mot,
        "sat_hom": sat_hom,
        "sat_adr": sat_adr,
        "sat_product_rows": sat_product(hub_prod_rows, sat_date, product_code_by_hk),
    }
