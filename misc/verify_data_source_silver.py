import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import verify_csv
from config.storage_paths import SILVER_DATA_SOURCE_ROOT
from helper.scd2_diff_engine import latest_subdir


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else latest_subdir(SILVER_DATA_SOURCE_ROOT)
    if not target:
        raise SystemExit(f"No data_source silver folder found under {SILVER_DATA_SOURCE_ROOT}")
    verify_csv.main(target)
