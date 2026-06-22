from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from misc.generate_direct_dim_fact import _parse_ddl_columns


EXPECTED_FACT_REFS = {
    "fact_policy": {
        "policy_sk": "dim_policy",
        "person_sk": "dim_person",
        "customer_sk": "dim_customer",
        "account_sk": "dim_account",
        "date_sk": "dim_date",
        "marketing_sk": "dim_marketing",
        "channel_sk": "dim_channel",
        "broker_sk": "dim_broker",
        "claim_sk": "dim_claim",
        "override_sk": "dim_override",
        "insured_object_sk": "dim_insured_object",
    },
    "fact_quote": {
        "person_sk": "dim_person",
        "customer_sk": "dim_customer",
        "account_sk": "dim_account",
        "marketing_sk": "dim_marketing",
        "date_sk": "dim_date",
        "campaign_sk": "dim_campaign",
        "channel_sk": "dim_channel",
        "broker_sk": "dim_broker",
        "insured_object_sk": "dim_insured_object",
    },
    "fact_complaint": {
        "person_sk": "dim_person",
        "customer_sk": "dim_customer",
        "regulation_sk": "dim_regulation",
        "channel_sk": "dim_channel",
        "date_sk": "dim_date",
        "insured_object_sk": "dim_insured_object",
    },
    "fact_lead": {
        "person_sk": "dim_person",
        "date_sk": "dim_date",
        "marketing_sk": "dim_marketing",
    },
}


def _read(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, keep_default_na=False, low_memory=False)
    frame.columns = [str(col).strip().lower() for col in frame.columns]
    return frame


def _sk_col(table: str) -> str | None:
    if table == "dim_date":
        return "date_sk"
    if table in {"dim_home", "dim_motor"}:
        return "insured_object_sk"
    return f"{table.removeprefix('dim_')}_sk"


def verify(root: Path) -> int:
    schemas = _parse_ddl_columns()
    errors = 0
    frames: dict[str, pd.DataFrame] = {}

    for table, expected_cols in schemas.items():
        path = root / f"{table}.csv"
        if not path.exists():
            print(f"FAIL: missing {table}.csv")
            errors += 1
            continue
        frame = _read(path)
        frames[table] = frame
        got_cols = list(frame.columns)
        if got_cols != expected_cols:
            print(f"FAIL: {table} columns do not match DDL")
            print(f"  missing={sorted(set(expected_cols) - set(got_cols))}")
            print(f"  extra={sorted(set(got_cols) - set(expected_cols))}")
            errors += 1

    if len(frames) == len(schemas):
        print(f"PASS: found all {len(schemas)} dim/fact tables")

    for table, frame in frames.items():
        if not table.startswith("dim_"):
            continue
        sk_col = _sk_col(table)
        if sk_col not in frame.columns:
            print(f"FAIL: {table} missing surrogate key {sk_col}")
            errors += 1
            continue
        duplicates = frame[sk_col].astype(str).duplicated().sum()
        if duplicates:
            print(f"FAIL: {table}.{sk_col} has {duplicates} duplicate values")
            errors += 1
        if {"effective_from_ts", "effective_to_ts", "record_version"}.issubset(frame.columns):
            missing_scd = frame[["effective_from_ts", "effective_to_ts", "record_version"]].astype(str).eq("").any(axis=1).sum()
            if missing_scd:
                print(f"FAIL: {table} has {missing_scd} rows with incomplete SCD2 fields")
                errors += 1
        if "attr_hash" in frame.columns:
            missing_hash = frame["attr_hash"].astype(str).str.strip().eq("").sum()
            if missing_hash:
                print(f"FAIL: {table} has {missing_hash} rows with blank attr_hash")
                errors += 1

        if {"effective_from_ts", "effective_to_ts", "record_version", "attr_hash"}.issubset(frame.columns):
            business_cols = [col for col in frame.columns if col.endswith("_id") and col != sk_col]
            if business_cols:
                business_col = business_cols[0]
                closed = frame[
                    frame[sk_col].astype(str).ne("-1")
                    & frame[business_col].astype(str).str.strip().ne("")
                    & frame["effective_to_ts"].astype(str).ne("9999-12-31T00:00:00")
                ]
                if not closed.empty:
                    active = frame[
                        frame["effective_to_ts"].astype(str).eq("9999-12-31T00:00:00")
                        & pd.to_numeric(frame["record_version"], errors="coerce").ge(2)
                    ]
                    active_keys = set(active[business_col].astype(str))
                    missing_active = ~closed[business_col].astype(str).isin(active_keys)
                    if missing_active.any():
                        print(f"FAIL: {table} has {int(missing_active.sum())} closed SCD2 rows without active version >=2")
                        errors += 1
                    else:
                        print(f"PASS: {table} has {len(closed)} closed SCD2 sample rows with active version >=2")

    for fact, refs in EXPECTED_FACT_REFS.items():
        frame = frames.get(fact)
        if frame is None or frame.empty:
            continue
        for fk_col, dim in refs.items():
            dim_frame = frames.get(dim)
            dim_sk = _sk_col(dim)
            if dim_frame is None or dim_sk not in dim_frame.columns or fk_col not in frame.columns:
                continue
            valid = set(dim_frame[dim_sk].astype(str))
            fk_values = frame[fk_col].astype(str)
            optional_unknown = fk_values.eq("-1")
            invalid = ~(fk_values.isin(valid) | optional_unknown)
            count = int(invalid.sum())
            if count:
                print(f"FAIL: {fact}.{fk_col} has {count} values missing from {dim}.{dim_sk}")
                errors += 1

    if errors:
        print(f"Direct dim/fact structural validation failed with {errors} issue(s).")
    else:
        print("Direct dim/fact structural validation passed.")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate direct MLOps dim/fact output structure and referential integrity.")
    parser.add_argument("path", help="Folder containing direct dim/fact CSV files.")
    args = parser.parse_args()
    return 1 if verify(Path(args.path)) else 0


if __name__ == "__main__":
    raise SystemExit(main())
