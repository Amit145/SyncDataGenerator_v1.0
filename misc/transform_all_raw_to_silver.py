import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.storage_paths import (
    RAW_API_ROOT,
    RAW_CLAIMS_ROOT,
    RAW_CRM_CANONICAL_ROOT,
    RAW_CRM_ROOT,
    RAW_KAGGLE_ROOT,
    SILVER_API_ROOT,
    SILVER_CLAIMS_ROOT,
    SILVER_DATA_SOURCE_ROOT,
    SILVER_KAGGLE_ROOT,
    SILVER_REBUILT_ROOT,
    ensure_data_roots,
)
from helper.api_silver_builder import build_api_silver
from helper.claims_mapper import map_claims_to_canonical
from helper.config_loader import load_config
from generators.raw_data_source_generator import generate_data_source_raw
from helper.data_source_mapper import map_data_source_to_canonical
from helper.source_context_builder import build_default_source_context
from misc.raw_to_silver_sample import build_silver


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


def transform_all():
    ensure_data_roots()
    results = []

    crm_raw = latest_subdir(RAW_CRM_ROOT)
    crm_canonical = latest_subdir(RAW_CRM_CANONICAL_ROOT)
    if crm_canonical:
        crm_silver = os.path.join(SILVER_REBUILT_ROOT, os.path.basename(crm_canonical))
        build_silver(crm_canonical, crm_silver)
        results.append(("crm", crm_canonical, crm_silver))

    api_raw = latest_subdir(RAW_API_ROOT)
    if api_raw:
        api_silver = os.path.join(SILVER_API_ROOT, os.path.basename(api_raw))
        build_api_silver(api_raw, api_silver)
        results.append(("api", api_raw, api_silver))

    claims_raw = latest_subdir(RAW_CLAIMS_ROOT)
    if claims_raw:
        claims_run_id = os.path.basename(claims_raw)
        claims_canonical = map_claims_to_canonical(claims_run_id, claims_raw)
        claims_silver = os.path.join(SILVER_CLAIMS_ROOT, claims_run_id)
        build_silver(claims_canonical, claims_silver)
        results.append(("claims", claims_canonical, claims_silver))

    if os.path.exists(RAW_KAGGLE_ROOT):
        for dataset_name in sorted(os.listdir(RAW_KAGGLE_ROOT)):
            dataset_root = os.path.join(RAW_KAGGLE_ROOT, dataset_name)
            if not os.path.isdir(dataset_root):
                continue
            kaggle_raw = latest_subdir(dataset_root)
            if not kaggle_raw:
                continue
            kaggle_silver = os.path.join(SILVER_KAGGLE_ROOT, os.path.basename(kaggle_raw))
            build_silver(kaggle_raw, kaggle_silver)
            results.append((f"kaggle:{dataset_name}", kaggle_raw, kaggle_silver))

    if crm_raw:
        run_id = os.path.basename(crm_raw)
        cfg = load_config()
        data_source_ctx = build_default_source_context(cfg, f"{run_id}_icrm", seed_offset=202)
        source_dirs = generate_data_source_raw(run_id, ctx=data_source_ctx)
        canonical_dir = map_data_source_to_canonical(run_id, source_dirs)
        data_source_silver = os.path.join(SILVER_DATA_SOURCE_ROOT, run_id)
        build_silver(canonical_dir, data_source_silver)
        results.append(("data_source", canonical_dir, data_source_silver))

    return results


if __name__ == "__main__":
    outputs = transform_all()
    print(outputs)
    if not outputs:
        raise SystemExit("No raw batches found to transform.")

    for source_name, raw_dir, silver_dir in outputs:
        print(f"{source_name} raw={raw_dir}")
        print(f"{source_name} silver={silver_dir}")
