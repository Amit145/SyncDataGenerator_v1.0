import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from helper.api_silver_builder import build_api_silver
from config.storage_paths import RAW_API_ROOT, SILVER_API_ROOT

RAW_BASE = RAW_API_ROOT
SAMPLE_SILVER_BASE = SILVER_API_ROOT


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


def parse_args():
    parser = argparse.ArgumentParser(description="Build sample silver vault tables from raw API files.")
    parser.add_argument("raw_dir", nargs="?")
    parser.add_argument("out_dir", nargs="?")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    raw_dir = args.raw_dir or latest_subdir(RAW_BASE)
    if not raw_dir:
        raise SystemExit(f"No raw batch folder found under {RAW_BASE}")

    out_dir = args.out_dir
    if not out_dir:
        out_dir = os.path.join(SAMPLE_SILVER_BASE, os.path.basename(raw_dir))

    print(build_api_silver(raw_dir, out_dir))
