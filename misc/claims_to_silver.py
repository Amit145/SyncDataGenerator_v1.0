import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import verify_csv
from config.storage_paths import RAW_CLAIMS_ROOT, SILVER_CLAIMS_ROOT
from helper.claims_mapper import map_claims_to_canonical
from helper.scd2_diff_engine import latest_subdir
from misc.raw_to_silver_sample import build_silver


def parse_args():
    parser = argparse.ArgumentParser(description="Convert claims raw to canonical raw, build silver, and verify.")
    parser.add_argument("--run-id", help="Optional run id. Defaults to latest claims raw batch name.")
    parser.add_argument("--claims-raw-dir", help="Optional source claims raw folder. Defaults to latest data/raw/claims batch.")
    parser.add_argument("--skip-verify", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    claims_raw_dir = args.claims_raw_dir or latest_subdir(RAW_CLAIMS_ROOT)
    if not claims_raw_dir:
        raise SystemExit(f"No claims raw batch found under {RAW_CLAIMS_ROOT}")

    run_id = args.run_id or os.path.basename(claims_raw_dir)
    canonical_dir = map_claims_to_canonical(run_id, claims_raw_dir)
    silver_dir = os.path.join(SILVER_CLAIMS_ROOT, run_id)
    build_silver(canonical_dir, silver_dir)

    print(f"claims_raw={claims_raw_dir}")
    print(f"claims_canonical={canonical_dir}")
    print(f"claims_silver={silver_dir}")

    if not args.skip_verify:
        verify_csv.main(silver_dir)
