import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import verify_csv
from config.storage_paths import RAW_CRM_ROOT, SILVER_DATA_SOURCE_ROOT
from generators.raw_data_source_generator import generate_data_source_raw
from helper.config_loader import load_config
from helper.data_source_mapper import map_data_source_to_canonical
from helper.source_context_builder import build_default_source_context
from helper.scd2_diff_engine import latest_subdir
from misc.raw_to_silver_sample import build_silver


def parse_args():
    parser = argparse.ArgumentParser(description="Generate data_source raw files, convert to canonical raw, build silver, and verify.")
    parser.add_argument("--run-id", help="Optional run id. Defaults to latest CRM raw batch name.")
    parser.add_argument("--crm-raw-dir", help="Optional source CRM raw folder. Defaults to latest data/raw/crm batch.")
    parser.add_argument("--skip-verify", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    crm_raw_dir = args.crm_raw_dir or latest_subdir(RAW_CRM_ROOT)
    if not crm_raw_dir:
        raise SystemExit(f"No CRM raw batch found under {RAW_CRM_ROOT}")

    run_id = args.run_id or os.path.basename(crm_raw_dir)
    cfg = load_config()
    data_source_ctx = build_default_source_context(cfg, f"{run_id}_icrm", seed_offset=202)
    source_dirs = generate_data_source_raw(run_id, ctx=data_source_ctx)
    canonical_dir = map_data_source_to_canonical(run_id, source_dirs)
    silver_dir = os.path.join(SILVER_DATA_SOURCE_ROOT, run_id)
    build_silver(canonical_dir, silver_dir)

    print(f"data_source_raw_motor={source_dirs.get('motor', '')}")
    print(f"data_source_raw_home={source_dirs.get('home', '')}")
    print(f"data_source_canonical={canonical_dir}")
    print(f"data_source_silver={silver_dir}")

    if not args.skip_verify:
        verify_csv.main(silver_dir)
