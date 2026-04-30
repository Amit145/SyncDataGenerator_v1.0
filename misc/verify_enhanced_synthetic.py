from __future__ import annotations

import argparse
import os
import sys

import pandas as pd
from pandas.errors import EmptyDataError

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.storage_paths import SYNTHETIC_ENHANCED_ROOT
from helper.enhanced_ddl import parse_enhanced_ddl


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


def read_csv_safe(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
    except EmptyDataError:
        df = pd.DataFrame()
    df.columns = [str(col).strip().lower() for col in df.columns]
    return df


def verify_enhanced(base_path: str) -> bool:
    ddl = parse_enhanced_ddl()
    schemas = ddl["tables"]
    primary_keys = ddl["primary_keys"]
    foreign_keys = ddl["foreign_keys"]

    frames: dict[str, pd.DataFrame] = {}
    errors = 0

    print(f"Checking enhanced synthetic folder: {base_path}")

    for table_name, expected_cols in schemas.items():
        file_name = f"{table_name}.csv"
        path = os.path.join(base_path, file_name)
        if not os.path.exists(path):
            print(f"MISSING FILE: {file_name}")
            errors += 1
            continue

        df = read_csv_safe(path)
        frames[table_name] = df
        actual_cols = list(df.columns)
        missing = [col for col in expected_cols if col not in actual_cols]
        extra = [col for col in actual_cols if col not in expected_cols]
        if missing or extra:
            print(f"COLUMN MISMATCH: {file_name}")
            if missing:
                print(f"  missing={missing}")
            if extra:
                print(f"  extra={extra}")
            errors += 1
        else:
            print(f"{file_name}: columns ok | rows={len(df)}")

    for table_name, pk_cols in primary_keys.items():
        df = frames.get(table_name)
        if df is None:
            continue
        missing_pk = [col for col in pk_cols if col not in df.columns]
        if missing_pk:
            print(f"PK CHECK FAILED: {table_name} missing {missing_pk}")
            errors += 1
            continue
        if df.empty:
            print(f"{table_name}: PK skipped (empty)")
            continue
        duplicate_count = int(df.duplicated(subset=pk_cols, keep=False).sum())
        null_count = int(df[pk_cols].isna().any(axis=1).sum())
        blank_count = int((df[pk_cols].astype(str).apply(lambda col: col.str.strip() == "")).any(axis=1).sum())
        if duplicate_count or null_count or blank_count:
            print(
                f"PK CHECK FAILED: {table_name} pk={pk_cols} "
                f"duplicates={duplicate_count} nulls={null_count} blanks={blank_count}"
            )
            errors += 1
        else:
            print(f"{table_name}: PK ok")

    for fk in foreign_keys:
        child_table = fk["child_table"]
        parent_table = fk["parent_table"]
        child_cols = fk["child_columns"]
        parent_cols = fk["parent_columns"]
        child_df = frames.get(child_table)
        parent_df = frames.get(parent_table)
        if child_df is None or parent_df is None:
            continue
        if child_df.empty:
            print(f"{child_table}->{parent_table}: FK skipped (empty child)")
            continue

        missing_child = [col for col in child_cols if col not in child_df.columns]
        missing_parent = [col for col in parent_cols if col not in parent_df.columns]
        if missing_child or missing_parent:
            print(f"FK CHECK FAILED: {child_table}->{parent_table} missing child={missing_child} parent={missing_parent}")
            errors += 1
            continue

        child_keys = set(tuple(row) for row in child_df[child_cols].dropna().astype(str).itertuples(index=False, name=None))
        parent_keys = set(tuple(row) for row in parent_df[parent_cols].dropna().astype(str).itertuples(index=False, name=None))
        missing = child_keys - parent_keys
        if missing:
            print(f"FK CHECK FAILED: {child_table}->{parent_table} missing={len(missing)} sample={list(missing)[:5]}")
            errors += 1
        else:
            print(f"{child_table}->{parent_table}: FK ok")

    group_counts = {"hub": 0, "link": 0, "sat": 0}
    for table_name in schemas:
        prefix = table_name.split("_", 1)[0]
        if prefix in group_counts:
            group_counts[prefix] += 1

    print("")
    print(f"Expected tables: {len(schemas)}")
    print(f"Group counts: hubs={group_counts['hub']} links={group_counts['link']} sats={group_counts['sat']}")

    if errors:
        print(f"Enhanced verification failed with {errors} issue(s).")
        return False

    print("Enhanced verification passed.")
    return True


def parse_args():
    parser = argparse.ArgumentParser(description="Verify enhanced synthetic Data Vault output.")
    parser.add_argument("folder", nargs="?", help="Enhanced synthetic folder. Defaults to latest data/synthetic/enhanced run.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    target = args.folder or latest_subdir(SYNTHETIC_ENHANCED_ROOT)
    if not target:
        raise SystemExit("No enhanced synthetic folder found.")
    ok = verify_enhanced(target)
    raise SystemExit(0 if ok else 1)
