from __future__ import annotations

import csv
import os

from config.storage_paths import RAW_CRM_CANONICAL_ROOT
from helper.canonical_raw_schema import CANONICAL_RAW_SCHEMAS
from helper.crm_raw_layout import (
    CRM_RAW_FILE_MAP,
    to_canonical_column,
)
from helper.csv_writer import write_csv


def _read_csv(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        rows = []
        for row in csv.DictReader(f):
            rows.append({to_canonical_column(col): value for col, value in row.items()})
        return rows


def map_crm_raw_to_canonical(run_id: str, crm_raw_dir: str) -> str:
    out_dir = os.path.join(RAW_CRM_CANONICAL_ROOT, run_id)
    os.makedirs(out_dir, exist_ok=True)

    for canonical_file, schema in CANONICAL_RAW_SCHEMAS.items():
        raw_file = CRM_RAW_FILE_MAP[canonical_file]
        rows = _read_csv(os.path.join(crm_raw_dir, raw_file))
        write_csv(out_dir, canonical_file, rows, fieldnames=schema)

    return out_dir
