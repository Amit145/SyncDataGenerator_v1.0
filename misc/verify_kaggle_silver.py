import argparse
import csv
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import verify_csv
from config.storage_paths import RAW_KAGGLE_ROOT, SILVER_KAGGLE_ROOT


RAW_KAGGLE_BASE = RAW_KAGGLE_ROOT
SILVER_BASE = SILVER_KAGGLE_ROOT


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


def latest_kaggle_batch():
    dataset_root = latest_subdir(RAW_KAGGLE_BASE)
    if not dataset_root:
        return None
    return latest_subdir(dataset_root)


def csv_row_count(path):
    with open(path, "r", encoding="utf-8") as f:
        return max(sum(1 for _ in csv.reader(f)) - 1, 0)


def print_raw_summary(raw_dir):
    print("KAGGLE RAW SUMMARY")
    print(f"raw_batch={raw_dir}")
    for file_name in sorted(name for name in os.listdir(raw_dir) if name.lower().endswith(".csv")):
        count = csv_row_count(os.path.join(raw_dir, file_name))
        print(f"{file_name}: {count} rows")


def parse_args():
    parser = argparse.ArgumentParser(description="Validate silver rebuilt from a Kaggle raw batch.")
    parser.add_argument("raw_dir", nargs="?")
    parser.add_argument("silver_dir", nargs="?")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    raw_dir = args.raw_dir or latest_kaggle_batch()
    if not raw_dir:
        raise SystemExit(f"No Kaggle raw batch found under {RAW_KAGGLE_BASE}")

    silver_dir = args.silver_dir
    if not silver_dir:
        silver_dir = os.path.join(SILVER_BASE, os.path.basename(raw_dir))
    if not os.path.isdir(silver_dir):
        raise SystemExit(f"Silver folder not found: {silver_dir}")

    print_raw_summary(raw_dir)
    print("")
    print("KAGGLE SILVER VALIDATION")
    print(f"silver={silver_dir}")
    verify_csv.main(silver_dir)
