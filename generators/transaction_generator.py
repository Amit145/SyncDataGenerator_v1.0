import os
import json
import random
from helper.key_factory import make_business_id, md5_hasher

# Constant value to show where this data is coming from
RS = "CRM"


# ----------------------------
# Cardinality Loader
# ----------------------------
def _load_cardinality():
    """
    Loads config/cardinality.json from project root.
    If missing, returns {} (functions fallback to defaults).
    """
    here = os.path.dirname(os.path.abspath(__file__))   # generators/
    root = os.path.dirname(here)                        # project root
    path = os.path.join(root, "config", "cardinality.json")

    if not os.path.exists(path):
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f) or {}


_CARD = _load_cardinality()


def _get_min_max(link_name: str, default_min: int, default_max: int):
    """
    Returns (min,max) from cardinality.json for given link.
    If link missing -> defaults.
    If max < min -> coerces max=min.
    """
    rule = _CARD.get(link_name)
    if not isinstance(rule, dict):
        return default_min, default_max

    mn = int(rule.get("min", default_min))
    mx = int(rule.get("max", default_max))

    if mx < mn:
        mx = mn

    return mn, mx


# ----------------------------
# Hubs
# ----------------------------
def hub_quote(persons, run_id, ld, max_per_person=2):
    """
    Create Hub_Quote rows.

    Cardinality source:
      Link_Quote_Person (min/max) if present in cardinality.json

    Fallback:
      1..max_per_person (original behavior)
    """
    # Prefer config if available
    mn, mx = _get_min_max("Link_Quote_Person", 1, max_per_person)

    rows = []
    mapping = {}
    seq = 0

    for p in persons:
        cnt = random.randint(mn, mx)
        if cnt <= 0:
            continue

        for _ in range(cnt):
            seq += 1
            bid = make_business_id("QUO", run_id, seq)
            hk = md5_hasher(bid)

            rows.append({
                "Quote Hash Key": hk,
                "Load Date": ld,
                "Record Source": RS,
                "Quote Id": bid
            })

            mapping.setdefault(p, []).append(hk)

    return rows, mapping


def hub_policy(person_to_quote, run_id, ld):
    """
    Create policies from quotes.
    Each quote may convert into a policy.

    Returns:
        rows                -> hub policy rows
        policy_person_map   -> {person_hk: [policy_hk, ...]}
        policy_to_quote_map -> {policy_hk: quote_hk}
    """

    rows = []
    policy_person_map = {}
    policy_to_quote_map = {}
    seq = 1

    for person_hk, quote_list in person_to_quote.items():
        for quote_hk in quote_list:

            # conversion probability
            if random.random() > 0.5:
                continue

            bid = make_business_id("POL", run_id, seq)
            seq += 1
            hk = md5_hasher(bid)

            rows.append({
                "Policy Hash Key": hk,
                "Load Date": ld,
                "Record Source": RS,
                "Policy Id": bid
            })

            policy_person_map.setdefault(person_hk, []).append(hk)
            policy_to_quote_map[hk] = quote_hk

    return rows, policy_person_map, policy_to_quote_map
# ----------------------------
# Assignments / Assets (unchanged)
# ----------------------------
def assign_product_for_policy(policy_person_map, person_type_by_person_hk):
    policy_to_product = {}

    for person_hk, policies in policy_person_map.items():
        ptype = person_type_by_person_hk[person_hk]

        for pol_hk in policies:
            if ptype == "NATURAL":
                product = random.choices(
                    ["PRD_MOTOR_PERSONAL", "PRD_HOME_PERSONAL","PRD_HEALTH_PERSONAL"],
                    [0.4, 0.4,0.2]
                )[0]
            else:
                product = random.choices(
                    ["PRD_COMMERCIAL_MOTOR", "PRD_PROPERTY_COMMERCIAL"],
                    [0.6, 0.4]
                )[0]

            policy_to_product[pol_hk] = product

    return policy_to_product


def hub_assets_from_policies(policy_to_product, run_id, ld):
    motor_rows = []
    home_rows = []
    addr_rows = []

    policy_to_motor = {}
    policy_to_home = {}
    home_to_addr = {}

    mot_seq = 1
    home_seq = 1
    addr_seq = 1

    for pol_hk, prod_id in policy_to_product.items():
        is_motor = "MOTOR" in prod_id
        is_home = ("HOME" in prod_id) or ("PROPERTY" in prod_id)

        if is_motor:
            bid = make_business_id("MOT", run_id, mot_seq)
            mot_seq += 1
            hk = md5_hasher(bid)

            motor_rows.append({
                "Motor Hash Key": hk,
                "Load Date": ld,
                "Record Source": RS,
                "Motor Id": bid
            })

            policy_to_motor[pol_hk] = hk

        if is_home:
            bid = make_business_id("HOM", run_id, home_seq)
            home_seq += 1
            home_hk = md5_hasher(bid)

            home_rows.append({
                "Home Hash Key": home_hk,
                "Load Date": ld,
                "Record Source": RS,
                "Home Id": bid
            })

            policy_to_home[pol_hk] = home_hk

            abid = make_business_id("ADDR", run_id, addr_seq)
            addr_seq += 1
            addr_hk = md5_hasher(abid)

            addr_rows.append({
                "Home Address Hash Key": addr_hk,
                "Load Date": ld,
                "Record Source": RS,
                "Home Address Id": abid
            })

            home_to_addr[home_hk] = addr_hk

    return motor_rows, home_rows, addr_rows, policy_to_motor, policy_to_home, home_to_addr