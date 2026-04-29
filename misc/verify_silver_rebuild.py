import argparse
import csv
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.storage_paths import SYNTHETIC_BASE_ROOT, SILVER_REBUILT_ROOT

BASELINE_BASE = SYNTHETIC_BASE_ROOT
REBUILT_BASE = SILVER_REBUILT_ROOT


PRIMARY_KEYS = {
    "hub_account.csv": ["account_hash_key"],
    "hub_consent.csv": ["consent_hash_key"],
    "hub_contact.csv": ["contact_hash_key"],
    "hub_customer.csv": ["customer_hash_key"],
    "hub_home.csv": ["home_hash_key"],
    "hub_home_address.csv": ["home_address_hash_key"],
    "hub_identities.csv": ["identities_hash_key"],
    "hub_lead.csv": ["lead_hash_key"],
    "hub_legal_person.csv": ["legal_person_hash_key"],
    "hub_marketing_engagement.csv": ["marketing_engagement_hash_key"],
    "hub_marketing_preference.csv": ["marketing_preference_hash_key"],
    "hub_motor.csv": ["motor_hash_key"],
    "hub_natural_person.csv": ["natural_person_hash_key"],
    "hub_person.csv": ["person_hash_key"],
    "hub_policy.csv": ["policy_hash_key"],
    "hub_product.csv": ["product_hash_key"],
    "hub_quote.csv": ["quote_hash_key"],
    "link_customer_lead.csv": ["customer_lead_hash_key"],
    "link_customer_person.csv": ["customer_person_hash_key"],
    "link_person_account.csv": ["person_account_hash_key"],
    "link_person_consent.csv": ["person_consent_hash_key"],
    "link_person_contact.csv": ["person_contact_hash_key"],
    "link_person_home_address.csv": ["person_home_address_hash_key"],
    "link_person_identities.csv": ["person_identities_hash_key"],
    "link_person_lead.csv": ["person_lead_hash_key"],
    "link_person_legal_person.csv": ["person_legal_person_hash_key"],
    "link_person_marketing_engagement.csv": ["person_marketing_engagement_hash_key"],
    "link_person_marketing_preference.csv": ["person_marketing_preference_hash_key"],
    "link_person_natural_person.csv": ["person_natural_person_hash_key"],
    "link_policy_customer.csv": ["policy_customer_hash_key"],
    "link_policy_product.csv": ["policy_customer_hash_key"],
    "link_product_home.csv": ["product_home_hash_key"],
    "link_product_motor.csv": ["product_motor_hash_key"],
    "link_quote_person.csv": ["quote_person_hash_key"],
    "link_quote_product.csv": ["quote_product_hash_key"],
    "sat_account.csv": ["account_hash_key", "load_date"],
    "sat_consent.csv": ["consent_hash_key", "load_date"],
    "sat_contact.csv": ["contact_hash_key", "load_date"],
    "sat_customer.csv": ["customer_hash_key", "load_date"],
    "sat_home.csv": ["home_hash_key", "load_date"],
    "sat_home_address.csv": ["home_address_hash_key", "load_date"],
    "sat_identities.csv": ["identities_hash_key", "load_date"],
    "sat_lead.csv": ["lead_hash_key", "load_date"],
    "sat_legal_person.csv": ["legal_person_hash_key", "load_date"],
    "sat_marketing_engagement.csv": ["marketing_engagement_hash_key", "load_date"],
    "sat_marketing_preference.csv": ["marketing_preference_hash_key", "load_date"],
    "sat_motor.csv": ["motor_hash_key", "load_date"],
    "sat_natural_person.csv": ["natural_person_hash_key", "load_date"],
    "sat_person.csv": ["person_hash_key", "load_date"],
    "sat_policy.csv": ["policy_hash_key", "load_date"],
    "sat_product.csv": ["product_hash_key", "load_date"],
    "sat_quote.csv": ["quote_hash_key", "load_date"],
}


def read_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return reader.fieldnames or [], list(reader)


def csv_files(folder):
    return sorted(p for p in os.listdir(folder) if p.lower().endswith(".csv"))


def latest_subdir(base_dir):
    if not os.path.exists(base_dir):
        return None
    dirs = [
        os.path.join(base_dir, name)
        for name in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, name))
    ]
    if not dirs:
        return None
    return max(dirs, key=os.path.getmtime)


def subdir_by_name(base_dir, name):
    path = os.path.join(base_dir, name)
    return path if os.path.isdir(path) else None


def key_for(row, fields):
    return tuple(row.get(field, "") for field in fields)


def compare_file(file_name, left_dir, right_dir):
    left_path = os.path.join(left_dir, file_name)
    right_path = os.path.join(right_dir, file_name)
    left_header, left_rows = read_csv(left_path)
    right_header, right_rows = read_csv(right_path)

    errors = []
    if left_header != right_header:
        errors.append(f"{file_name}: header mismatch")
        return errors

    if len(left_rows) != len(right_rows):
        errors.append(f"{file_name}: row count mismatch baseline={len(left_rows)} rebuilt={len(right_rows)}")

    pk_fields = PRIMARY_KEYS.get(file_name)
    if not pk_fields:
        errors.append(f"{file_name}: missing primary key definition")
        return errors

    left_keys = set()
    for row in left_rows:
        pk = key_for(row, pk_fields)
        if pk in left_keys:
            errors.append(f"{file_name}: duplicate key in baseline {pk}")
            break
        left_keys.add(pk)

    right_keys = set()
    for row in right_rows:
        pk = key_for(row, pk_fields)
        if pk in right_keys:
            errors.append(f"{file_name}: duplicate key in rebuilt {pk}")
            break
        right_keys.add(pk)

    left_map = {key_for(row, pk_fields): row for row in left_rows}
    right_map = {key_for(row, pk_fields): row for row in right_rows}

    missing = sorted(set(left_map) - set(right_map))
    extra = sorted(set(right_map) - set(left_map))
    if missing:
        errors.append(f"{file_name}: missing keys in rebuilt count={len(missing)} sample={missing[:3]}")
    if extra:
        errors.append(f"{file_name}: extra keys in rebuilt count={len(extra)} sample={extra[:3]}")

    shared = sorted(set(left_map) & set(right_map))
    value_mismatches = 0
    samples = []
    for pk in shared:
        left_row = left_map[pk]
        right_row = right_map[pk]
        for col in left_header:
            if left_row.get(col, "") != right_row.get(col, ""):
                value_mismatches += 1
                if len(samples) < 3:
                    samples.append((pk, col, left_row.get(col, ""), right_row.get(col, "")))
                break
    if value_mismatches:
        errors.append(f"{file_name}: row value mismatches count={value_mismatches} sample={samples}")
    return errors


def verify(left_dir, right_dir):
    errors = []
    left_files = csv_files(left_dir)
    right_files = csv_files(right_dir)

    if left_files != right_files:
        missing = sorted(set(left_files) - set(right_files))
        extra = sorted(set(right_files) - set(left_files))
        if missing:
            errors.append(f"missing files in rebuilt: {missing}")
        if extra:
            errors.append(f"extra files in rebuilt: {extra}")

    for file_name in sorted(set(left_files) & set(right_files)):
        errors.extend(compare_file(file_name, left_dir, right_dir))
    return errors


def parse_args():
    parser = argparse.ArgumentParser(description="Compare baseline silver output with rebuilt silver output.")
    parser.add_argument("baseline_dir", nargs="?")
    parser.add_argument("rebuilt_dir", nargs="?")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    baseline_dir = args.baseline_dir or latest_subdir(BASELINE_BASE)
    if not baseline_dir:
        raise SystemExit(f"No baseline folder found under {BASELINE_BASE}")

    rebuilt_dir = args.rebuilt_dir
    if not rebuilt_dir:
        rebuilt_dir = subdir_by_name(REBUILT_BASE, os.path.basename(baseline_dir))
    if not rebuilt_dir:
        rebuilt_dir = latest_subdir(REBUILT_BASE)
    if not rebuilt_dir:
        raise SystemExit(f"No rebuilt folder found under {REBUILT_BASE}")

    issues = verify(baseline_dir, rebuilt_dir)
    if issues:
        print("VERIFY FAILED")
        print(f"baseline={baseline_dir}")
        print(f"rebuilt={rebuilt_dir}")
        for issue in issues:
            print(issue)
        sys.exit(1)
    print("VERIFY OK")
    print(f"baseline={baseline_dir}")
    print(f"rebuilt={rebuilt_dir}")
