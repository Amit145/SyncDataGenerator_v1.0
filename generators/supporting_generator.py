import os
import json
import random
from helper.key_factory import make_business_id, md5_hasher

# Constant to indicate source system
RS = "CRM"


# ----------------------------
# Cardinality Loader
# ----------------------------
def _load_cardinality():
    """
    Loads config/cardinality.json from project root.
    If file missing, returns {} and we fallback to defaults.
    """
    here = os.path.dirname(os.path.abspath(__file__))  # generators/
    root = os.path.dirname(here)                       # project root
    path = os.path.join(root, "config", "cardinality.json")

    if not os.path.exists(path):
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f) or {}


_CARD = _load_cardinality()


def _get_min_max(link_name: str, default_min: int = 1, default_max: int = 1):
    """
    Reads min/max for a link from cardinality.json.
    If link not present -> defaults.
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


def select_coverage(all_person_hks, coverage: float):
    """
    Select only a percentage of persons based on coverage.
    """
    k = int(round(len(all_person_hks) * coverage))
    k = max(0, min(len(all_person_hks), k))
    return random.sample(list(all_person_hks), k)


def _hub_rows(entity_name, prefix, persons, run_id, ld, max_per_person=3, link_name: str = None):
    """
    Generic function to create Hub records for any entity.
    """
    rows = []
    mapping = {}
    seq = 0

    if link_name:
        mn, mx = _get_min_max(link_name, default_min=1, default_max=max_per_person)
    else:
        mn, mx = 1, max_per_person

    if mx < mn:
        mx = mn

    for p in persons:
        cnt = random.randint(mn, mx)

        if cnt <= 0:
            continue

        for _ in range(cnt):
            seq += 1

            bid = make_business_id(prefix, run_id, seq)
            hk = md5_hasher(bid)

            rows.append({
                f"{entity_name} Hash Key": hk,
                "Load Date": ld,
                "Record Source": RS,
                f"{entity_name} Id": bid
            })

            mapping.setdefault(p, []).append(hk)

    return rows, mapping


def _hub_rows_exactly_one(entity_name, prefix, persons, run_id, ld):
    """
    Create exactly one hub row per passed person.
    Used where business rules require guaranteed existence
    for the supplied population.
    """
    rows = []
    mapping = {}
    seq = 0

    for p in persons:
        seq += 1
        bid = make_business_id(prefix, run_id, seq)
        hk = md5_hasher(bid)

        rows.append({
            f"{entity_name} Hash Key": hk,
            "Load Date": ld,
            "Record Source": RS,
            f"{entity_name} Id": bid
        })

        mapping[p] = hk

    return rows, mapping


def hub_identities(all_person_hks, run_id, ld, coverage=1.0):
    """
    Create Hub_Identities records.
    For current rules, every person must have identity.
    """
    persons = list(all_person_hks) if coverage >= 1.0 else select_coverage(all_person_hks, coverage)
    return _hub_rows(
        "Identities",
        "ECI",
        persons,
        run_id,
        ld,
        max_per_person=3,
        link_name="Link_Person_Identities"
    )


def hub_contact(all_person_hks, run_id, ld, coverage=1.0):
    """
    Create Hub_Contact records.
    For current rules, every person must have contact.
    """
    persons = list(all_person_hks) if coverage >= 1.0 else select_coverage(all_person_hks, coverage)
    return _hub_rows(
        "Contact",
        "CON",
        persons,
        run_id,
        ld,
        max_per_person=3,
        link_name="Link_Person_Contact"
    )


def hub_consent(all_person_hks, run_id, ld, coverage=1.0):
    """
    Create Hub_Consent records.
    When called for lead persons, every passed person must get exactly one consent.
    """
    persons = list(all_person_hks) if coverage >= 1.0 else select_coverage(all_person_hks, coverage)
    return _hub_rows_exactly_one(
        "Consent",
        "CNS",
        persons,
        run_id,
        ld
    )


def hub_account(customer_person_hks, run_id, ld):
    """
    Create Hub_Account records.
    Only policy-holder/customer persons can have accounts.
    Each passed person gets exactly one account.
    """
    persons = list(customer_person_hks)
    return _hub_rows_exactly_one(
        "Account",
        "ACC",
        persons,
        run_id,
        ld
    )


def hub_marketing_preference(consented_person_hks, run_id, ld):
    """
    Create Hub_Marketing_Preference records.
    Each passed person gets exactly one marketing preference.
    """
    persons = list(consented_person_hks)
    return _hub_rows_exactly_one(
        "Marketing Preference",
        "MPR",
        persons,
        run_id,
        ld
    )


def hub_marketing_engagement(eligible_person_hks, run_id, ld, coverage=1.0):
    """
    Create Hub_Marketing_Engagement records.
    For current rules, when called for lead persons, all should get engagement.
    """
    persons = list(eligible_person_hks) if coverage >= 1.0 else select_coverage(list(eligible_person_hks), coverage)
    return _hub_rows(
        "Marketing Engagement",
        "MEN",
        persons,
        run_id,
        ld,
        max_per_person=10,
        link_name="Link_Person_Marketing_Engagement",
    )
