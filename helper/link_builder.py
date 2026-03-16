import json
import os
import random
from .key_factory import md5_hasher, get_now_iso

RS = "CRM"


def _load_cardinality_config() -> dict:
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(here)
    path = os.path.join(root, "config", "cardinality.json")
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f) or {}


def _as_list(v):
    if v is None:
        return []
    return v if isinstance(v, list) else [v]


def _pick(rng: random.Random, items: list, mn: int, mx: int) -> list:
    items = list(items)
    if not items:
        return []
    if mn < 0:
        mn = 0
    if mx < mn:
        mx = mn
    k = rng.randint(mn, mx)
    k = max(0, min(k, len(items)))
    return rng.sample(items, k) if k else []


def make_link(table_name: str, row_cols: dict):
    pk_name = table_name.replace("Link_", "").replace("_", " ") + " Hash Key"
    link_pk = md5_hasher("|".join(str(v) for v in row_cols.values()))
    return {
        pk_name: link_pk,
        "Load Date": get_now_iso(),
        "Record Source": RS,
        **row_cols
    }


def build_links(person_to_nat, person_to_leg,
                person_to_lead, person_to_customer,
                person_to_identity, person_to_contact, person_to_consent,
                person_to_home_address,
                person_to_mkt_pref, person_to_mkt_eng,
                person_to_account,
                person_to_quote,
                policy_person_map, policy_to_product_id,
                policy_to_motor, policy_to_home, home_to_addr,
                product_hk_by_id,
                link_load_date,
                seed: int = 42):
    links = {}
    cfg = _load_cardinality_config()
    rng = random.Random(seed)

    def add(table, row):
        links.setdefault(table, []).append(row)

    def rule(table: str, default_min: int = 1, default_max: int = 1):
        r = cfg.get(table, None)
        if not isinstance(r, dict):
            return default_min, default_max
        return int(r.get("min", default_min)), int(r.get("max", default_max))

    person_account_done = set()

    def ensure_person_account_link(person_hk):
        if person_hk in person_account_done:
            return

        acct_list = _as_list(person_to_account.get(person_hk))
        if not acct_list:
            return

        for acct_hk in acct_list:
            add(
                "Link_Person_Account",
                make_link(
                    "Link_Person_Account",
                    {"Person Hash Key": person_hk, "Account Hash Key": acct_hk}
                )
            )
        person_account_done.add(person_hk)

    # ---- Person type links ----
    for p, nat in person_to_nat.items():
        add(
            "Link_Person_Natural_Person",
            make_link(
                "Link_Person_Natural_Person",
                {"Person Hash Key": p, "Natural Person Hash Key": nat}
            )
        )

    for p, leg in person_to_leg.items():
        add(
            "Link_Person_Legal_Person",
            make_link(
                "Link_Person_Legal_Person",
                {"Person Hash Key": p, "Legal Person Hash Key": leg}
            )
        )

    # ---- Supporting links ----
    # mandatory for all persons
    for p, hk_list in person_to_identity.items():
        mn, mx = rule("Link_Person_Identities", 1, 1)
        for hk in _pick(rng, _as_list(hk_list), mn, mx):
            add(
                "Link_Person_Identities",
                make_link(
                    "Link_Person_Identities",
                    {"Person Hash Key": p, "Identities Hash Key": hk}
                )
            )

    for p, hk_list in person_to_contact.items():
        mn, mx = rule("Link_Person_Contact", 1, 1)
        for hk in _pick(rng, _as_list(hk_list), mn, mx):
            add(
                "Link_Person_Contact",
                make_link(
                    "Link_Person_Contact",
                    {"Person Hash Key": p, "Contact Hash Key": hk}
                )
            )

    for p, hk_list in person_to_home_address.items():
        mn, mx = rule("Link_Person_Home_Address", 1, 1)
        for hk in _pick(rng, _as_list(hk_list), mn, mx):
            add(
                "Link_Person_Home_Address",
                make_link(
                    "Link_Person_Home_Address",
                    {"Person Hash Key": p, "Home Address Hash Key": hk}
                )
            )

    # lead-dependent: link every mapped HK directly
    for p, hk_list in person_to_consent.items():
        for hk in _as_list(hk_list):
            add(
                "Link_Person_Consent",
                make_link(
                    "Link_Person_Consent",
                    {"Person Hash Key": p, "Consent Hash Key": hk}
                )
            )

    for p, hk_list in person_to_mkt_pref.items():
        for hk in _as_list(hk_list):
            add(
                "Link_Person_Marketing_Preference",
                make_link(
                    "Link_Person_Marketing_Preference",
                    {"Person Hash Key": p, "Marketing Preference Hash Key": hk}
                )
            )

    for p, hk_list in person_to_mkt_eng.items():
        for hk in _as_list(hk_list):
            add(
                "Link_Person_Marketing_Engagement",
                make_link(
                    "Link_Person_Marketing_Engagement",
                    {"Person Hash Key": p, "Marketing Engagement Hash Key": hk}
                )
            )

    # ---- Optional Person -> Account links ----
    # direct linking from mapping; policy-holder enforcement still handled below
    for p, hk_list in person_to_account.items():
        for hk in _as_list(hk_list):
            add(
                "Link_Person_Account",
                make_link(
                    "Link_Person_Account",
                    {"Person Hash Key": p, "Account Hash Key": hk}
                )
            )
        if _as_list(hk_list):
            person_account_done.add(p)

    # ---- Lifecycle ----
    for p, hk_list in person_to_lead.items():
        for hk in _as_list(hk_list):
            add(
                "Link_Person_Lead",
                make_link(
                    "Link_Person_Lead",
                    {"Person Hash Key": p, "Lead Hash Key": hk}
                )
            )

    for p, cust_hk in person_to_customer.items():
        add(
            "Link_Customer_Person",
            make_link(
                "Link_Customer_Person",
                {"Customer Hash Key": cust_hk, "Person Hash Key": p}
            )
        )

    mn, mx = rule("Link_Customer_Lead", 0, 1)
    for p in set(person_to_customer.keys()).intersection(set(person_to_lead.keys())):
        for lead_hk in _pick(rng, _as_list(person_to_lead[p]), mn, mx):
            add(
                "Link_Customer_Lead",
                make_link(
                    "Link_Customer_Lead",
                    {
                        "Customer Hash Key": person_to_customer[p],
                        "Lead Hash Key": lead_hk
                    }
                )
            )

    # ---- Quotes ----
    for p, qhk_list in person_to_quote.items():
        for qhk in _as_list(qhk_list):
            add(
                "Link_Quote_Person",
                make_link(
                    "Link_Quote_Person",
                    {"Quote Hash Key": qhk, "Person Hash Key": p}
                )
            )

    # ---- Policies ----
    for p, policies in policy_person_map.items():
        policy_list = _as_list(policies)
        if not policy_list:
            continue

        ensure_person_account_link(p)

        cust_hk = person_to_customer.get(p)
        if not cust_hk:
            continue

        for pol_hk in policy_list:
            add(
                "Link_Policy_Customer",
                make_link(
                    "Link_Policy_Customer",
                    {"Policy Hash Key": pol_hk, "Customer Hash Key": cust_hk}
                )
            )

            prod_id = policy_to_product_id.get(pol_hk)
            if prod_id is None:
                continue

            prod_hk = product_hk_by_id.get(prod_id)
            if prod_hk is None:
                continue

            add(
                "Link_Policy_Product",
                make_link(
                    "Policy_Customer",
                    {"Policy Hash Key": pol_hk, "Product Hash Key": prod_hk}
                )
            )

            if pol_hk in policy_to_motor:
                mn, mx = rule("Link_Product_Motor", 0, 1)
                for motor_hk in _pick(rng, _as_list(policy_to_motor[pol_hk]), mn, mx):
                    add(
                        "Link_Product_Motor",
                        make_link(
                            "Link_Product_Motor",
                            {"Product Hash Key": prod_hk, "Motor Hash Key": motor_hk}
                        )
                    )

            if pol_hk in policy_to_home:
                home_hk = policy_to_home[pol_hk]
                add(
                    "Link_Product_Home",
                    make_link(
                        "Link_Product_Home",
                        {"Product Hash Key": prod_hk, "Home Hash Key": home_hk}
                    )
                )

    return links
