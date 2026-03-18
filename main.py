import random
import os
import csv
from datetime import datetime, timedelta

from config.runConfig import (
    PARSED_DDL_PATH,
    ORDERED_TABLE_METADATA_PATH,
    RAW_DDL,
    DDL_JSON_PATH,
    OUTPUT_BASE,
    SYNTHETIC_DATA,
)
from generators.transaction_generator import (
    hub_policy,
    hub_assets_from_policies,
)

from helper.config_loader import load_config
from helper.csv_writer import write_csv, normalize_csv
from helper.hub_builder import build_hubs
from helper.key_factory import get_run_id
from helper.scd2_generator import create_scd_data
from helper.link_builder import build_links, make_link
from modules.inference import inference_module
from modules.module_parser import parse_ddl_module, file_ready
from helper.pk_validator import assert_unique

from helper.satellite_builder import (
    sat_natural_person,
    sat_legal_person,
    sat_customer,
    sat_person,
    sat_lead,
    sat_identities,
    sat_contact,
    sat_consent,
    sat_account,
    sat_marketing_preference,
    sat_marketing_engagement,
    sat_quote,
    sat_policy,
    sat_home_address,
    sat_home,
    sat_motor,
    sat_product,
    apply_lead_interest_levels,
    apply_customer_segments,
    apply_customer_ratings,
)

from misc.ref_check import latest_run, second_latest_run
from validators.file_cols_validator import check_file_and_cols
from validators.integrity_checker import validate_integrity
from generators.supporting_generator import hub_account
from generators.lifecycle_generator import hub_customer
from enums.product_catalog import (
    get_product_codes_for_person_type,
    get_product_weights_for_person_type,
)

start_time = datetime.now()

# ---------------- Inputs ----------------
BUSINESS_START_DATE = "2020-01-01"
AS_OF_DATE = None
SATELLITE_PATH = r".\scd2_sat"

# ---------------- Load date setup ----------------
load_date = "2025-03-12T00:10:21"

prev_run_path = latest_run()
if prev_run_path:
    for d in os.listdir(prev_run_path):
        if "sat_" in d.lower():
            with open(os.path.join(prev_run_path, d), "r", encoding="utf-8") as f:
                data = csv.DictReader(f)
                for row in data:
                    load_date = row["Load Date"]
                    break

root = datetime.fromisoformat(load_date)
current_time = datetime.now().time().replace(microsecond=0)
root = datetime.combine(root.date(), current_time)

random_offset = random.randint(0, 100)

hub_dt = root + timedelta(days=random_offset)
link_dt = hub_dt + timedelta(days=5)
sat_dt = link_dt + timedelta(days=5)

HUB_DATE = hub_dt.isoformat()
LINK_DATE = link_dt.isoformat()
SAT_DATE = sat_dt.isoformat()

# ---------------- Create Metadata ordered Table ----------------
if not file_ready(PARSED_DDL_PATH):
    parse_ddl_module(RAW_DDL)
else:
    print(f"⏭️  Skipping parse_ddl_module: {PARSED_DDL_PATH} already exists")

if not file_ready(ORDERED_TABLE_METADATA_PATH):
    inference_module()
else:
    print(f"⏭️  Skipping inference_module: {ORDERED_TABLE_METADATA_PATH} already exists")

# ---------------- Run settings ----------------
cfg = load_config()
seed = cfg["run_settings"]["random_seed"]
random.seed(seed)

run_id = get_run_id(seed)
out = f"output/{run_id}"

# =========================================================
# 1) HUB GENERATION
# Person -> Type -> Contact/Identity/Address -> Lead -> Marketing -> Quote
# =========================================================
ctx = build_hubs(cfg, run_id, HUB_DATE)

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

assert_unique(hub_person_rows, "Person Hash Key")

# =========================================================
# 2) QUOTE -> PRODUCT MAP
# Every quote picks exactly one product
# =========================================================
quote_to_product_id = {}

for person_hk, quote_hks in person_to_quote.items():
    current_person_type = person_type[person_hk]
    eligible_codes = get_product_codes_for_person_type(current_person_type)
    eligible_weights = get_product_weights_for_person_type(current_person_type)
    assigned_codes = []

    for idx, quote_hk in enumerate(quote_hks):
        remaining_codes = [code for code in eligible_codes if code not in assigned_codes]

        # Diversify products across a person's quotes where possible so a
        # customer can legitimately end up with multiple eligible products.
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

# =========================================================
# 3) POLICY GENERATION
# Quote -> Policy (0..1)
# Only policy holders get Account and Customer
# =========================================================
hub_pol_rows, policy_person_map, policy_to_quote_map = hub_policy(
    person_to_quote, run_id, HUB_DATE
)

policy_holder_persons = set(policy_person_map.keys())

hub_acc_rows, person_to_account = hub_account(policy_holder_persons, run_id, HUB_DATE)
hub_cust_rows, person_to_customer = hub_customer(policy_holder_persons, run_id, HUB_DATE)

policy_to_person_map = {}
for person_hk, policy_hks in policy_person_map.items():
    for policy_hk in policy_hks:
        policy_to_person_map[policy_hk] = person_hk

# Policy inherits product from converted quote
policy_to_product_id = {
    policy_hk: quote_to_product_id[quote_hk]
    for policy_hk, quote_hk in policy_to_quote_map.items()
}

# =========================================================
# 4) ASSET HUBS
# Product -> Motor / Home
# =========================================================
hub_mot_rows, hub_home_rows, _, policy_to_motor, policy_to_home, home_to_addr = \
    hub_assets_from_policies(policy_to_product_id, run_id, HUB_DATE)

# =========================================================
# 5) LINK GENERATION
# =========================================================
link_quote_product = [
    make_link(
        "Link_Quote_Product",
        {
            "Quote Hash Key": quote_hk,
            "Product Hash Key": prod_hk_by_code[product_id],
        },
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
    link_load_date=LINK_DATE,
)

# =========================================================
# 6) SATELLITE GENERATION
# =========================================================
sat_nat = sat_natural_person(person_to_nat, SAT_DATE)
sat_leg = sat_legal_person(person_to_leg, SAT_DATE)
sat_per = sat_person(
    person_hks,
    SAT_DATE,
    person_type,
    person_to_lead,
    person_to_consent,
)

sat_lea = sat_lead(
    person_to_lead,
    SAT_DATE,
    business_start_date=BUSINESS_START_DATE,
    as_of_date=AS_OF_DATE,
)

sat_eci = sat_identities(person_to_identity, SAT_DATE)
sat_con = sat_contact(person_to_contact, SAT_DATE)
sat_cns = sat_consent(person_to_consent, SAT_DATE)
sat_acc = sat_account(person_to_account, SAT_DATE)
sat_mpr = sat_marketing_preference(person_to_mpr, SAT_DATE)
sat_men = sat_marketing_engagement(person_to_men, SAT_DATE)
sat_quo = sat_quote(person_to_quote, SAT_DATE)

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
    SAT_DATE,
    business_start_date=BUSINESS_START_DATE,
    as_of_date=AS_OF_DATE,
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
    SAT_DATE,
    business_start_date=BUSINESS_START_DATE,
    as_of_date=AS_OF_DATE,
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

addr_hks = list({
    r["Home Address Hash Key"] for r in hub_addr_rows
}.union(set(home_to_addr.values())))

sat_adr = sat_home_address(addr_hks, SAT_DATE)
sat_hom = sat_home(home_hks, SAT_DATE, home_to_addr)

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

sat_mot = sat_motor(motor_hks, SAT_DATE, motor_to_addr)

# =========================================================
# 7) WRITE OUTPUTS
# =========================================================
write_csv(out, "Hub_Person.csv", hub_person_rows)
write_csv(out, "Hub_Natural_Person.csv", hub_nat)
write_csv(out, "Hub_Legal_Person.csv", hub_leg)

write_csv(out, "Hub_Product.csv", hub_prod_rows)
write_csv(out, "Hub_Lead.csv", hub_lead_rows)
write_csv(out, "Hub_Customer.csv", hub_cust_rows)

write_csv(out, "Hub_Identities.csv", hub_id_rows)
write_csv(out, "Hub_Contact.csv", hub_con_rows)
write_csv(out, "Hub_Consent.csv", hub_cns_rows)
write_csv(out, "Hub_Account.csv", hub_acc_rows)
write_csv(out, "Hub_Marketing_Preference.csv", hub_mpr_rows)
write_csv(out, "Hub_Marketing_Engagement.csv", hub_men_rows)

write_csv(out, "Hub_Quote.csv", hub_quo_rows)
write_csv(out, "Hub_Policy.csv", hub_pol_rows)

write_csv(out, "Hub_Motor.csv", hub_mot_rows)
write_csv(out, "Hub_Home.csv", hub_home_rows)
write_csv(out, "Hub_Home_Address.csv", hub_addr_rows)

write_csv(out, "Link_Quote_Product.csv", link_quote_product)

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
    "Link_Policy_Customer",
    "Link_Policy_Product",
    "Link_Product_Motor",
    "Link_Product_Home",
]

for table in EXPECTED_LINKS:
    rows = links.get(table, [])
    rows_with_load_date = []
    for item in rows:
        item["Load Date"] = LINK_DATE
        rows_with_load_date.append(item)
    write_csv(out, f"{table}.csv", rows_with_load_date)

write_csv(out, "Sat_Natural_Person.csv", sat_nat)
write_csv(out, "Sat_Legal_Person.csv", sat_leg)
write_csv(out, "Sat_Person.csv", sat_per)
write_csv(out, "Sat_Lead.csv", sat_lea)
write_csv(out, "Sat_Customer.csv", sat_cus)

write_csv(out, "Sat_Identities.csv", sat_eci)
write_csv(out, "Sat_Contact.csv", sat_con)
write_csv(out, "Sat_Consent.csv", sat_cns)
write_csv(out, "Sat_Account.csv", sat_acc)
write_csv(out, "Sat_Marketing_Preference.csv", sat_mpr)
write_csv(out, "Sat_Marketing_Engagement.csv", sat_men)

write_csv(out, "Sat_Quote.csv", sat_quo)
write_csv(out, "Sat_Policy.csv", sat_pol)
write_csv(out, "Sat_Motor.csv", sat_mot)
write_csv(out, "Sat_Home.csv", sat_hom)
write_csv(out, "Sat_Home_Address.csv", sat_adr)
write_csv(out, "Sat_Product.csv", sat_product(hub_prod_rows, SAT_DATE, product_code_by_hk))

# =========================================================
# 8) VALIDATIONS + LOAD
# =========================================================
assert_unique(hub_person_rows, "Person Hash Key")
assert_unique(hub_lead_rows, "Lead Hash Key")
assert_unique(hub_cust_rows, "Customer Hash Key")
assert_unique(hub_pol_rows, "Policy Hash Key")
assert_unique(hub_quo_rows, "Quote Hash Key")

print("Basic PK validation OK")
print("DONE:", out)

are_files_checked = check_file_and_cols(DDL_JSON_PATH, OUTPUT_BASE)
if are_files_checked:
    is_valid_integrity = validate_integrity(OUTPUT_BASE)
    if is_valid_integrity:
        print("Data is valid and maintains Referential Integrity. Data can be loaded")

        latest_run_path = latest_run()
        latest_run_path_new = latest_run_path.replace("output", "")
        synthetic_data = SYNTHETIC_DATA + "\\" + latest_run_path_new

        normalize_csv(latest_run_path, synthetic_data)

        second_latest_run_path = second_latest_run()
        if second_latest_run_path:
            current_run_name = latest_run_path_new.lstrip("\\/")
            scd2_input = SYNTHETIC_DATA
            scd2_output = SATELLITE_PATH + "\\" + current_run_name + "\\"
            create_scd_data(scd2_input, scd2_output, SAT_DATE, exclude_run_name=current_run_name)

end_time = datetime.now()
print("Total time taken:", end_time - start_time)
