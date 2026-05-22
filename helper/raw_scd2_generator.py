from __future__ import annotations

import os

from config.storage_paths import (
    RAW_API_ROOT,
    RAW_CRM_ROOT,
    SCD2_RAW_ROOT,
)
from helper.scd2_diff_engine import diff_folder, previous_subdir


def _generate_for_pair(source_label: str, previous_dir: str | None, current_dir: str, output_dir: str) -> dict | None:
    if not previous_dir or not current_dir:
        return None
    if os.path.basename(previous_dir) == os.path.basename(current_dir):
        return None
    summary = diff_folder(previous_dir, current_dir, output_dir)
    summary["source_label"] = source_label
    return summary


def generate_raw_scd2(run_id: str, crm_raw_dir: str | None = None, api_raw_dir: str | None = None) -> list[dict]:
    results = []

    crm_current = crm_raw_dir or os.path.join(RAW_CRM_ROOT, run_id)
    crm_previous = previous_subdir(RAW_CRM_ROOT, exclude_name=run_id)
    crm_output = os.path.join(SCD2_RAW_ROOT, "crm", run_id)
    crm_summary = _generate_for_pair("crm", crm_previous, crm_current, crm_output)
    if crm_summary:
        results.append(crm_summary)

    api_current = api_raw_dir or os.path.join(RAW_API_ROOT, run_id)
    api_previous = previous_subdir(RAW_API_ROOT, exclude_name=run_id)
    api_output = os.path.join(SCD2_RAW_ROOT, "api", run_id)
    api_summary = _generate_for_pair("api", api_previous, api_current, api_output)
    if api_summary:
        results.append(api_summary)

    return results
