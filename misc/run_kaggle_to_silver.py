import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from generators.raw_kaggle_generator import write_kaggle_raw_batch
from misc.raw_to_silver_sample import SCHEMAS, build_silver
import verify_csv
from config.storage_paths import SILVER_KAGGLE_ROOT


SILVER_BASE = SILVER_KAGGLE_ROOT


def parse_args():
    parser = argparse.ArgumentParser(
        description="Convert a Kaggle insurance dataset into canonical raw files and full vault-style silver tables."
    )
    parser.add_argument("--input-dir", required=True, help="Folder containing source Kaggle CSV files.")
    parser.add_argument("--config", required=True, help="JSON mapping config for the dataset.")
    parser.add_argument("--raw-output-dir", help="Optional Kaggle raw output folder.")
    parser.add_argument("--silver-output-dir", help="Optional silver output folder.")
    parser.add_argument("--batch-id", help="Optional batch identifier.")
    parser.add_argument("--extract-ts", help="Optional extract timestamp in ISO format.")
    parser.add_argument("--hub-load-date", help="Optional hub load timestamp.")
    parser.add_argument("--link-load-date", help="Optional link load timestamp.")
    parser.add_argument("--sat-load-date", help="Optional satellite load timestamp.")
    parser.add_argument("--skip-verify", action="store_true", help="Skip silver validation.")
    return parser.parse_args()


def main():
    args = parse_args()

    raw_dir = write_kaggle_raw_batch(
        input_dir=args.input_dir,
        config_path=args.config,
        output_dir=args.raw_output_dir,
        batch_id=args.batch_id,
        extract_ts=args.extract_ts,
    )

    silver_dir = args.silver_output_dir
    if not silver_dir:
        silver_dir = os.path.join(SILVER_BASE, os.path.basename(raw_dir))

    build_silver(
        raw_dir=raw_dir,
        out_dir=silver_dir,
        hub_load_date=args.hub_load_date,
        link_load_date=args.link_load_date,
        sat_load_date=args.sat_load_date,
    )

    print(f"raw={raw_dir}")
    print(f"silver={silver_dir}")
    print(f"vault_tables={len(SCHEMAS)}")

    if not args.skip_verify:
        verify_csv.main(silver_dir)


if __name__ == "__main__":
    main()
