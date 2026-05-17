from __future__ import annotations

import csv
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.storage_paths import SCD2_BASE_ROOT, SCD2_RAW_ROOT, SYNTHETIC_BASE_ROOT
from helper.scd2_diff_engine import latest_subdir, previous_subdir


def _read_csv_rows(path: str) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _first_hash_key(columns: list[str]) -> str | None:
    for column in columns:
        normalized = column.strip().lower()
        if normalized.endswith("hash_key") or normalized.endswith("hash key"):
            return column
    return None


def _compare_base_scd2_folder(scd2_dir: str, previous_base_dir: str) -> dict:
    summary = {
        "source_label": "synthetic_base",
        "previous_dir": previous_base_dir,
        "current_dir": scd2_dir,
        "files": {},
    }

    for file_name in sorted(name for name in os.listdir(scd2_dir) if name.lower().endswith(".csv")):
        delta_path = os.path.join(scd2_dir, file_name)
        previous_path = os.path.join(previous_base_dir, file_name)
        if not os.path.exists(previous_path):
            continue

        delta_rows = _read_csv_rows(delta_path)
        previous_rows = _read_csv_rows(previous_path)
        if not delta_rows:
            continue

        key_column = _first_hash_key(list(delta_rows[0].keys()))
        if not key_column:
            continue

        previous_map = {row.get(key_column, ""): row for row in previous_rows}
        changed_columns = {}
        changed_rows = 0

        for row in delta_rows:
            key = row.get(key_column, "")
            previous_row = previous_map.get(key, {})
            row_changed = False
            for column, value in row.items():
                normalized = column.strip().lower()
                if column == key_column or normalized == "load_date":
                    continue
                if str(previous_row.get(column, "")) != str(value):
                    changed_columns[column] = changed_columns.get(column, 0) + 1
                    row_changed = True
            if row_changed:
                changed_rows += 1

        summary["files"][file_name] = {
            "key_field": key_column,
            "delta_rows": len(delta_rows),
            "changed_rows": changed_rows,
            "changed_columns": changed_columns,
        }

    return summary


def _print_summary(summary: dict):
    print(f"SOURCE: {summary['source_label']}")
    print(f"previous={summary.get('previous_dir', '')}")
    print(f"current={summary.get('current_dir', '')}")
    files = summary.get("files", {})
    if not files:
        print("no changes detected")
        print("")
        return

    for file_name, info in sorted(files.items()):
        print(f"{file_name}: delta_rows={info.get('delta_rows', 0)} changed_rows={info.get('changed_rows', 0)} new_rows={info.get('new_rows', 0)}")
        changed_columns = info.get("changed_columns", {})
        if changed_columns:
            top_changes = ", ".join(f"{column}={count}" for column, count in sorted(changed_columns.items()))
            print(f"  changed_columns: {top_changes}")
    print("")


if __name__ == "__main__":
    printed = False

    if os.path.exists(SCD2_RAW_ROOT):
        for source_name in sorted(name for name in os.listdir(SCD2_RAW_ROOT) if os.path.isdir(os.path.join(SCD2_RAW_ROOT, name))):
            source_root = os.path.join(SCD2_RAW_ROOT, source_name)
            if source_name == "kaggle":
                for dataset_name in sorted(name for name in os.listdir(source_root) if os.path.isdir(os.path.join(source_root, name))):
                    dataset_root = os.path.join(source_root, dataset_name)
                    latest_dir = latest_subdir(dataset_root)
                    if not latest_dir:
                        continue
                    summary_path = os.path.join(latest_dir, "_summary.json")
                    if not os.path.exists(summary_path):
                        continue
                    with open(summary_path, "r", encoding="utf-8") as f:
                        summary = json.load(f)
                    summary["source_label"] = f"kaggle:{dataset_name}"
                    _print_summary(summary)
                    printed = True
                continue

            latest_dir = latest_subdir(source_root)
            if not latest_dir:
                continue
            summary_path = os.path.join(latest_dir, "_summary.json")
            if not os.path.exists(summary_path):
                continue
            with open(summary_path, "r", encoding="utf-8") as f:
                summary = json.load(f)
            summary["source_label"] = source_name
            _print_summary(summary)
            printed = True

    latest_base_scd2 = latest_subdir(SCD2_BASE_ROOT)
    previous_base = previous_subdir(SYNTHETIC_BASE_ROOT, exclude_name=os.path.basename(latest_base_scd2) if latest_base_scd2 else None)
    if latest_base_scd2 and previous_base:
        _print_summary(_compare_base_scd2_folder(latest_base_scd2, previous_base))
        printed = True

    if not printed:
        raise SystemExit("No SCD2 comparison data found.")
