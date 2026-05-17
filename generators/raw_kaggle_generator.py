from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.storage_paths import KAGGLE_INPUT_ROOT, RAW_KAGGLE_ROOT, ensure_data_roots
from helper.canonical_raw_schema import CANONICAL_RAW_SCHEMAS
from helper.csv_writer import write_csv
from helper.kaggle_mapper import build_canonical_raw_rows

RAW_KAGGLE_BASE = Path(RAW_KAGGLE_ROOT)
DEFAULT_KAGGLE_INPUT_ROOT = Path(KAGGLE_INPUT_ROOT)
LEGACY_KAGGLE_INPUT_ROOT = ROOT / "kaggle_input"


def default_output_dir(config_path: str, manifest: dict) -> Path:
    dataset_name = manifest["dataset_name"]
    batch_id = manifest["batch_id"]
    return RAW_KAGGLE_BASE / dataset_name / batch_id


def discover_input_dir_for_config(config_path: str, search_root: str | None = None) -> str | None:
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    required_files = [source["path"] for source in config.get("sources", [])]
    if not required_files:
        return None

    roots = [Path(search_root)] if search_root else [DEFAULT_KAGGLE_INPUT_ROOT, LEGACY_KAGGLE_INPUT_ROOT]
    candidates = []

    for root in roots:
        if not root.exists():
            continue
        for candidate in [root] + [path for path in root.rglob("*") if path.is_dir()]:
            if all((candidate / file_name).exists() for file_name in required_files):
                candidates.append(candidate)

    if not candidates:
        return None

    candidates.sort(key=lambda path: (len(path.parts), str(path)))
    return str(candidates[0])


def write_kaggle_raw_batch(input_dir: str, config_path: str, output_dir: str | None = None, batch_id: str | None = None, extract_ts: str | None = None) -> str:
    rows_by_file, manifest = build_canonical_raw_rows(
        input_dir=input_dir,
        config_path=config_path,
        batch_id=batch_id,
        extract_ts=extract_ts,
    )

    out_dir = Path(output_dir) if output_dir else default_output_dir(config_path, manifest)
    ensure_data_roots()
    out_dir.mkdir(parents=True, exist_ok=True)

    for file_name, fieldnames in CANONICAL_RAW_SCHEMAS.items():
        rows = rows_by_file[file_name]
        if rows:
            write_csv(str(out_dir), file_name, rows, fieldnames=fieldnames)

    return str(out_dir)


def parse_args():
    parser = argparse.ArgumentParser(description="Convert a Kaggle insurance dataset into canonical raw CRM files.")
    parser.add_argument("--input-dir", required=True, help="Folder containing source Kaggle CSV files.")
    parser.add_argument("--config", required=True, help="JSON mapping config for the dataset.")
    parser.add_argument("--output-dir", help="Optional output folder. Defaults to data/raw/kaggle/<dataset>/<batch_id>.")
    parser.add_argument("--batch-id", help="Optional batch identifier.")
    parser.add_argument("--extract-ts", help="Optional extract timestamp in ISO format.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result = write_kaggle_raw_batch(
        input_dir=args.input_dir,
        config_path=args.config,
        output_dir=args.output_dir,
        batch_id=args.batch_id,
        extract_ts=args.extract_ts,
    )
    print(result)
