import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import misc.verify_silver_rebuild as base_verify
from config.storage_paths import SILVER_API_ROOT

base_verify.BASELINE_BASE = SILVER_API_ROOT
base_verify.REBUILT_BASE = SILVER_API_ROOT


if __name__ == "__main__":
    args = base_verify.parse_args()
    baseline_dir = args.baseline_dir or base_verify.latest_subdir(base_verify.BASELINE_BASE)
    if not baseline_dir:
        raise SystemExit(f"No baseline folder found under {base_verify.BASELINE_BASE}")

    rebuilt_dir = args.rebuilt_dir
    if not rebuilt_dir:
        rebuilt_dir = base_verify.subdir_by_name(base_verify.REBUILT_BASE, os.path.basename(baseline_dir))
    if not rebuilt_dir:
        rebuilt_dir = base_verify.latest_subdir(base_verify.REBUILT_BASE)
    if not rebuilt_dir:
        raise SystemExit(f"No rebuilt folder found under {base_verify.REBUILT_BASE}")

    issues = base_verify.verify(baseline_dir, rebuilt_dir)
    if issues:
        print("VERIFY FAILED")
        print(f"baseline={baseline_dir}")
        print(f"rebuilt={rebuilt_dir}")
        for issue in issues:
            print(issue)
        sys.exit(1)
    print("VERIFY OK")
    print(f"baseline={baseline_dir}")
    print(f"rebuilt={rebuilt_dir}")
