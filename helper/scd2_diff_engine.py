from __future__ import annotations

import csv
import json
import os
from pathlib import Path


RAW_PRIMARY_KEYS = {
    "crm_person.csv": ["person_id"],
    "crm_contact.csv": ["contact_id"],
    "crm_identity.csv": ["identities_id"],
    "crm_person_address.csv": ["address_id"],
    "crm_lead.csv": ["lead_id"],
    "crm_customer.csv": ["customer_id"],
    "crm_customer_lead.csv": ["customer_id", "lead_id"],
    "crm_consent.csv": ["consent_id"],
    "crm_marketing_preference.csv": ["marketing_preference_id"],
    "crm_marketing_engagement.csv": ["marketing_engagement_id"],
    "crm_account.csv": ["account_id"],
    "crm_product.csv": ["product_id"],
    "crm_quote.csv": ["quote_id"],
    "crm_policy.csv": ["policy_id"],
    "crm_home.csv": ["home_id"],
    "crm_motor.csv": ["motor_id"],
    "person.jsonl": ["person_id"],
    "contact.jsonl": ["contact_id"],
    "identity.jsonl": ["identities_id"],
    "person_address.jsonl": ["address_id"],
    "lead.jsonl": ["lead_id"],
    "customer.jsonl": ["customer_id"],
    "customer_lead.jsonl": ["customer_id", "lead_id"],
    "consent.jsonl": ["consent_id"],
    "marketing_preference.jsonl": ["marketing_preference_id"],
    "marketing_engagement.jsonl": ["marketing_engagement_id"],
    "account.jsonl": ["account_id"],
    "product.jsonl": ["product_id"],
    "quote.jsonl": ["quote_id"],
    "policy.jsonl": ["policy_id"],
    "home.jsonl": ["home_id"],
    "motor.jsonl": ["motor_id"],
}


def latest_subdir(base_dir: str) -> str | None:
    if not os.path.exists(base_dir):
        return None
    dirs = [
        os.path.join(base_dir, name)
        for name in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, name))
    ]
    if not dirs:
        return None
    return max(dirs, key=lambda path: (os.path.basename(path), os.path.getmtime(path)))


def previous_subdir(base_dir: str, exclude_name: str | None = None) -> str | None:
    if not os.path.exists(base_dir):
        return None
    dirs = [
        os.path.join(base_dir, name)
        for name in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, name))
    ]
    if exclude_name:
        dirs = [path for path in dirs if os.path.basename(path) != exclude_name]
    if not dirs:
        return None
    dirs.sort(key=lambda path: (os.path.basename(path), os.path.getmtime(path)))
    return dirs[-1]


def _read_rows(path: str) -> list[dict]:
    if path.lower().endswith(".jsonl"):
        rows = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows

    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _write_rows(path: str, rows: list[dict], fieldnames: list[str] | None = None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if path.lower().endswith(".jsonl"):
        with open(path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")
        return

    fields = fieldnames or (list(rows[0].keys()) if rows else [])
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _key_for(row: dict, key_fields: list[str]) -> tuple:
    return tuple(str(row.get(field, "")) for field in key_fields)


def _compare_rows(previous_row: dict, current_row: dict, key_fields: list[str]) -> list[str]:
    changed = []
    all_fields = sorted(set(previous_row) | set(current_row))
    ignored = set(key_fields) | {"_batch_id", "_extract_ts", "_dataset_name", "batch_id", "extract_ts"}
    for field in all_fields:
        if field in ignored:
            continue
        if str(previous_row.get(field, "")) != str(current_row.get(field, "")):
            changed.append(field)
    return changed


def diff_folder(previous_dir: str, current_dir: str, output_dir: str) -> dict:
    previous_files = {
        name: os.path.join(previous_dir, name)
        for name in os.listdir(previous_dir)
        if os.path.isfile(os.path.join(previous_dir, name))
    }
    current_files = {
        name: os.path.join(current_dir, name)
        for name in os.listdir(current_dir)
        if os.path.isfile(os.path.join(current_dir, name))
    }

    summary = {
        "previous_dir": previous_dir,
        "current_dir": current_dir,
        "output_dir": output_dir,
        "files": {},
    }

    for file_name in sorted(set(previous_files) & set(current_files)):
        key_fields = RAW_PRIMARY_KEYS.get(file_name)
        if not key_fields:
            continue

        previous_rows = _read_rows(previous_files[file_name])
        current_rows = _read_rows(current_files[file_name])

        prev_map = {_key_for(row, key_fields): row for row in previous_rows}
        curr_map = {_key_for(row, key_fields): row for row in current_rows}

        new_keys = sorted(set(curr_map) - set(prev_map))
        changed_keys = []
        changed_columns = {}
        delta_rows = []

        for key in sorted(set(curr_map) & set(prev_map)):
            changed = _compare_rows(prev_map[key], curr_map[key], key_fields)
            if changed:
                changed_keys.append(key)
                delta_rows.append(curr_map[key])
                for column in changed:
                    changed_columns[column] = changed_columns.get(column, 0) + 1

        for key in new_keys:
            delta_rows.append(curr_map[key])

        if delta_rows:
            sample_row = delta_rows[0]
            fieldnames = list(sample_row.keys())
            _write_rows(os.path.join(output_dir, file_name), delta_rows, fieldnames=fieldnames)

        summary["files"][file_name] = {
            "key_fields": key_fields,
            "previous_rows": len(previous_rows),
            "current_rows": len(current_rows),
            "new_rows": len(new_keys),
            "changed_rows": len(changed_keys),
            "delta_rows": len(delta_rows),
            "changed_columns": changed_columns,
        }

    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return summary
