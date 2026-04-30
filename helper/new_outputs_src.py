from __future__ import annotations

import os

from config.storage_paths import NEW_OUTPUTS_SRC_ROOT
from generators.raw_api_generator import write_raw_api_batch
from generators.raw_crm_generator import write_raw_crm_batch
from helper.scd2_diff_engine import diff_folder, previous_subdir
from helper.source_context_builder import build_source_context


SRC_SEED_OFFSETS = {
    "adp": 401,
    "transunion": 402,
    "experian": 403,
}


def _source_root(source_name: str) -> str:
    return os.path.join(NEW_OUTPUTS_SRC_ROOT, source_name)


def _source_data_root(source_name: str) -> str:
    return os.path.join(_source_root(source_name), "data")


def _source_scd2_root(source_name: str) -> str:
    return os.path.join(_source_root(source_name), "scd2")


def generate_new_outputs_src(
    run_id: str,
    cfg: dict,
    base_context: dict,
    base_run_id: str,
    hub_date: str,
    link_date: str,
    sat_date: str,
    business_start_date: str,
    as_of_date: str | None = None,
) -> dict[str, dict]:
    outputs: dict[str, dict] = {}

    crm_dir = write_raw_crm_batch(
        _source_root("crm"),
        run_id,
        base_context,
        source_dir_name="data",
        source_system="CRM",
    )
    outputs["crm"] = {
        "data_dir": crm_dir,
        "source_type": "csv",
    }

    for source_name, seed_offset in SRC_SEED_OFFSETS.items():
        source_ctx = build_source_context(
            cfg,
            f"{base_run_id}_{source_name}",
            hub_date,
            link_date,
            sat_date,
            business_start_date,
            as_of_date=as_of_date,
            seed_override=cfg["run_settings"]["random_seed"] + seed_offset,
        )
        if source_name == "adp":
            data_dir = write_raw_api_batch(
                _source_root(source_name),
                run_id,
                source_ctx,
                source_dir_name="data",
                source_system="ADP",
                source_prefix="ADP",
            )
            source_type = "json"
        else:
            data_dir = write_raw_crm_batch(
                _source_root(source_name),
                run_id,
                source_ctx,
                source_dir_name="data",
                source_system=source_name.upper(),
            )
            source_type = "csv"

        outputs[source_name] = {
            "data_dir": data_dir,
            "source_type": source_type,
        }

    return outputs


def generate_new_outputs_src_scd2(run_id: str, source_outputs: dict[str, dict]) -> list[dict]:
    results = []

    for source_name, payload in source_outputs.items():
        current_dir = payload["data_dir"]
        previous_dir = previous_subdir(_source_data_root(source_name), exclude_name=run_id)
        if not previous_dir or os.path.basename(previous_dir) == os.path.basename(current_dir):
            continue

        output_dir = os.path.join(_source_scd2_root(source_name), run_id)
        summary = diff_folder(previous_dir, current_dir, output_dir)
        summary["source_label"] = source_name
        results.append(summary)

    return results
