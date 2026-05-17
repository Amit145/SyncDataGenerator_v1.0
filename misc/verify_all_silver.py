import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import verify_csv
from config.storage_paths import SILVER_API_ROOT, SILVER_CLAIMS_ROOT, SILVER_DATA_SOURCE_ROOT, SILVER_KAGGLE_ROOT, SILVER_REBUILT_ROOT


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
    return max(dirs, key=lambda path: (os.path.basename(path), os.path.getmtime(path)))


def verify_target(label, folder):
    print(f"VERIFY {label}")
    print(f"path={folder}")
    verify_csv.main(folder)
    print("")


if __name__ == "__main__":
    targets = []

    crm_silver = latest_subdir(SILVER_REBUILT_ROOT)
    if crm_silver:
        targets.append(("crm", crm_silver))

    api_silver = latest_subdir(SILVER_API_ROOT)
    if api_silver:
        targets.append(("api", api_silver))

    claims_silver = latest_subdir(SILVER_CLAIMS_ROOT)
    if claims_silver:
        targets.append(("claims", claims_silver))

    data_source_silver = latest_subdir(SILVER_DATA_SOURCE_ROOT)
    if data_source_silver:
        targets.append(("data_source", data_source_silver))

    if os.path.exists(SILVER_KAGGLE_ROOT):
        for run_name in sorted(os.listdir(SILVER_KAGGLE_ROOT)):
            run_path = os.path.join(SILVER_KAGGLE_ROOT, run_name)
            if os.path.isdir(run_path):
                targets.append((f"kaggle:{run_name}", run_path))

    if not targets:
        raise SystemExit("No silver folders found to verify.")

    for label, folder in targets:
        verify_target(label, folder)
