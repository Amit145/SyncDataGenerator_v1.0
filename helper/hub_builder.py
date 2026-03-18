# helper/hub_builder.py

import random

from generators.lifecycle_generator import assign_lifecycle, hub_lead
from generators.person_generator import hub_person
from generators.product_generator import hub_product
from generators.supporting_generator import (
    hub_identities,
    hub_contact,
    hub_consent,
    hub_marketing_preference,
    hub_marketing_engagement
)
from generators.transaction_generator import hub_quote
from helper.key_factory import make_business_id, md5_hasher

RS = "CRM"


def build_hubs(cfg, run_id, hub_date):
    seed = cfg["run_settings"]["random_seed"]
    random.seed(seed)

    # --- HUBS: Person
    hub_person_rows = hub_person(cfg["run_settings"]["total_people"], run_id, hub_date)
    person_hks = [r["Person Hash Key"] for r in hub_person_rows]

    # --- Person type split from config (deterministic)
    random.shuffle(person_hks)

    natural_pct = cfg["run_settings"].get("natural_person_pct", 0.95)
    nat_count = int(round(len(person_hks) * natural_pct))

    nat_persons = person_hks[:nat_count]
    leg_persons = person_hks[nat_count:]

    assert len(nat_persons) + len(leg_persons) == len(person_hks)

    person_type = {p: "NATURAL" for p in nat_persons}
    person_type.update({p: "LEGAL" for p in leg_persons})

    ld = hub_date

    # --- HUBS: Natural Person
    hub_nat = []
    person_to_nat = {}
    for i, p in enumerate(nat_persons, 1):
        bid = make_business_id("NAT", run_id, i)
        hk = md5_hasher(bid)
        hub_nat.append({
            "Natural Person Hash Key": hk,
            "Load Date": ld,
            "Record Source": RS,
            "Natural Person Id": bid
        })
        person_to_nat[p] = hk

    # --- HUBS: Legal Person
    hub_leg = []
    person_to_leg = {}
    for i, p in enumerate(leg_persons, 1):
        bid = make_business_id("LEG", run_id, i)
        hk = md5_hasher(bid)
        hub_leg.append({
            "Legal Person Hash Key": hk,
            "Load Date": ld,
            "Record Source": RS,
            "Legal Person Id": bid
        })
        person_to_leg[p] = hk

    # --- HUBS: Product
    hub_prod_rows, prod_hk_by_code, product_code_by_hk = hub_product(hub_date, run_id)

    # --- Lifecycle
    leads, prospects, customers_initial = assign_lifecycle(person_hks.copy(), cfg)

    ch_choices = ["online", "branch", "call_center"]
    ch_weights = [
        cfg["sales_channel_distribution"]["online"],
        cfg["sales_channel_distribution"]["branch"],
        cfg["sales_channel_distribution"]["call_center"]
    ]
    channels = {p: random.choices(ch_choices, ch_weights)[0] for p in leads}

    hub_lead_rows, person_to_lead = hub_lead(leads, run_id, hub_date)

    # use actual mapped lead persons everywhere downstream
    lead_persons = list(person_to_lead.keys())

    customers_all = []
    hub_cust_rows = []
    person_to_customer = {}

    # --- Supporting hubs
    # mandatory for all persons
    hub_con_rows, person_to_contact = hub_contact(person_hks, run_id, hub_date, coverage=1.0)
    hub_id_rows, person_to_identity = hub_identities(person_hks, run_id, hub_date, coverage=1.0)

    # lead-dependent objects: must align to actual person_to_lead mapping
    hub_cns_rows, person_to_consent = hub_consent(lead_persons, run_id, hub_date, coverage=1.0)
    hub_mpr_rows, person_to_mpr = hub_marketing_preference(lead_persons, run_id, hub_date)
    hub_men_rows, person_to_men = hub_marketing_engagement(lead_persons, run_id, hub_date, coverage=0.7)

    # --- Home Address (1 per person)
    hub_addr_rows = []
    person_to_home_address = {}

    for i, person_hk in enumerate(person_hks, 1):
        bid = make_business_id("ADDR", run_id, i)
        hk = md5_hasher(bid)

        hub_addr_rows.append({
            "Home Address Hash Key": hk,
            "Load Date": ld,
            "Record Source": RS,
            "Home Address Id": bid
        })

        person_to_home_address[person_hk] = hk

    # created later after policy stage
    hub_acc_rows = []
    person_to_account = {}

    # --- Quotes: only actual linked leads
    quote_persons = lead_persons
    hub_quo_rows, person_to_quote = hub_quote(quote_persons, run_id, hub_date)

    return {
        "seed": seed,
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

        "leads": lead_persons,
        "prospects": prospects,
        "customers_all": customers_all,
        "channels": channels,
        "hub_lead_rows": hub_lead_rows,
        "person_to_lead": person_to_lead,
        "hub_cust_rows": hub_cust_rows,
        "person_to_customer": person_to_customer,

        "hub_id_rows": hub_id_rows,
        "person_to_identity": person_to_identity,
        "hub_con_rows": hub_con_rows,
        "person_to_contact": person_to_contact,
        "hub_cns_rows": hub_cns_rows,
        "person_to_consent": person_to_consent,

        "hub_addr_rows": hub_addr_rows,
        "person_to_home_address": person_to_home_address,

        "hub_acc_rows": hub_acc_rows,
        "person_to_account": person_to_account,
        "hub_mpr_rows": hub_mpr_rows,
        "person_to_mpr": person_to_mpr,
        "hub_men_rows": hub_men_rows,
        "person_to_men": person_to_men,

        "hub_quo_rows": hub_quo_rows,
        "person_to_quote": person_to_quote,
        "quote_persons": quote_persons
    }
