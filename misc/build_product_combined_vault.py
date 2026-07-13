from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.storage_paths import MLOPS_ROOT, PRODUCT_COMBINED_ROOT

MLOPS_DDL_PATH = ROOT / "mlops" / "Enhanced_Customer360_DataVault_Model_DDL.sql"


ENTITY_MAP = {
    "address_book.csv": ("hub_address", "sat_address"),
    "broker_book.csv": ("hub_broker", "sat_broker"),
    "campaign_register.csv": ("hub_campaign", "sat_campaign"),
    "channel_catalog.csv": ("hub_channel", "sat_channel"),
    "claim_register.csv": ("hub_claim", "sat_claim"),
    "complaint_register.csv": ("hub_complaint", "sat_complaint"),
    "insured_object_register.csv": ("hub_insured_object", "sat_insured_object"),
    "override_register.csv": ("hub_override", "sat_override"),
    "regulation_register.csv": ("hub_regulation", "sat_regulation"),
}

BRIDGE_MAP = {
    "claim_policy_bridge.csv": "link_claim_policy",
    "broker_person_bridge.csv": "link_broker_person",
    "complaint_policy_bridge.csv": "link_complaint_policy",
    "complaint_regulation_bridge.csv": "link_complaint_regulation",
    "insured_object_home_bridge.csv": "link_insured_object_home",
    "insured_object_motor_bridge.csv": "link_insured_object_motor",
    "person_address_bridge.csv": "link_person_address",
    "person_campaign_bridge.csv": "link_person_campaign",
    "policy_broker_bridge.csv": "link_policy_broker",
    "policy_channel_bridge.csv": "link_policy_channel",
    "policy_insured_object_bridge.csv": "link_policy_insured_object",
    "policy_override_bridge.csv": "link_policy_override",
    "policy_quote_bridge.csv": "link_policy_quote",
    "quote_broker_bridge.csv": "link_quote_broker",
    "quote_channel_bridge.csv": "link_quote_channel",
}

ENRICHMENT_MAP = {
    "policy_enrichment.csv": ("sat_policy", "policy_hash_key"),
    "customer_enrichment.csv": ("sat_customer", "customer_hash_key"),
    "marketing_engagement_enrichment.csv": ("sat_marketing_engagement", "marketing_engagement_hash_key"),
    "motor_enrichment.csv": ("sat_motor", "motor_hash_key"),
}


def md5_hasher(value: str) -> str:
    return hashlib.md5(str(value).encode("utf-8")).hexdigest()


def parse_enhanced_ddl(path: str | Path) -> dict:
    text = Path(path).read_text(encoding="utf-8")
    tables: dict[str, list[str]] = {}
    for match in re.finditer(r"CREATE\s+TABLE\s+(\w+)\s*\((.*?)\);", text, flags=re.IGNORECASE | re.DOTALL):
        table_name = match.group(1).lower()
        columns = []
        for raw_line in match.group(2).splitlines():
            line = raw_line.strip().rstrip(",")
            if not line or line.upper().startswith("CONSTRAINT"):
                continue
            columns.append(line.split()[0].strip('"').lower())
        tables[table_name] = columns
    return {"tables": tables}


def latest_subdir(base_dir: Path) -> Path:
    dirs = [path for path in base_dir.iterdir() if path.is_dir()] if base_dir.exists() else []
    if not dirs:
        raise FileNotFoundError(f"No run folders found under {base_dir}")
    return sorted(dirs, key=lambda path: path.name)[-1]


def read_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def raw_to_vault_column(column: str) -> str:
    if column == "src_extract_ts":
        return "load_date"
    if column == "src_system":
        return "record_source"
    if column.startswith("src_"):
        raw_name = column[4:]
        if raw_name.endswith("_ref"):
            return f"{raw_name[:-4]}_hash_key"
        return raw_name
    return column


def _is_hash(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-f]{32}", str(value or "").strip()))


def _hash_ref(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text if _is_hash(text) else md5_hasher(text)


def _vault_hash_columns(row: dict) -> list[str]:
    return [column for column in row if column.endswith("_hash_key")]


def _hydrate_vault_hashes(row: dict) -> dict:
    hydrated = dict(row)
    hash_columns = _vault_hash_columns(hydrated)
    if not hash_columns:
        return hydrated

    pk_column = hash_columns[0] if len(hash_columns) > 1 else None
    for column in hash_columns:
        if column == pk_column:
            continue
        hydrated[column] = _hash_ref(hydrated.get(column, ""))

    if pk_column:
        component_hashes = [hydrated.get(column, "") for column in hash_columns[1:] if hydrated.get(column, "")]
        if component_hashes:
            hydrated[pk_column] = md5_hasher("|".join(component_hashes))
        else:
            hydrated[pk_column] = _hash_ref(hydrated.get(pk_column, ""))
    else:
        only_column = hash_columns[0]
        hydrated[only_column] = _hash_ref(hydrated.get(only_column, ""))

    return hydrated


def read_prd2_rows(prd2_dir: Path, file_name: str) -> list[dict]:
    direct_aliases = {
        "address_book.csv": "enhanced_address_book.csv",
    }
    alias_name = direct_aliases.get(file_name)
    if alias_name:
        alias_path = prd2_dir / alias_name
        if alias_path.exists():
            return [
                _hydrate_vault_hashes({raw_to_vault_column(column): value for column, value in row.items()})
                for row in read_rows(alias_path)
            ]

    for grouped_file in [
        "enhanced_person_relationships.csv",
        "enhanced_policy_relationships.csv",
        "enhanced_enrichments.csv",
    ]:
        grouped_path = prd2_dir / grouped_file
        if grouped_path.exists():
            grouped_rows = []
            for row in read_rows(grouped_path):
                if row.get("source_extract") == file_name:
                    grouped_rows.append({column: value for column, value in row.items() if column != "source_extract"})
            if grouped_rows:
                return [
                    _hydrate_vault_hashes({raw_to_vault_column(column): value for column, value in row.items()})
                    for row in grouped_rows
                ]

    bundle_path = prd2_dir / "enhanced_addon_bundle.csv"
    if bundle_path.exists():
        bundled_rows = []
        for row in read_rows(bundle_path):
            if row.get("file_name") == file_name and row.get("row_json"):
                bundled_rows.append(json.loads(row["row_json"]))
        if bundled_rows:
            return [
                _hydrate_vault_hashes({raw_to_vault_column(column): value for column, value in row.items()})
                for row in bundled_rows
            ]

    path = prd2_dir / file_name
    if not path.exists():
        prefixed_path = prd2_dir / f"source1_{file_name}"
        path = prefixed_path if prefixed_path.exists() else path
    return [
        _hydrate_vault_hashes({raw_to_vault_column(column): value for column, value in row.items()})
        for row in read_rows(path)
    ]


def write_rows(folder: Path, table_name: str, rows: list[dict], columns: list[str]) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    with (folder / f"{table_name}.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows({column: row.get(column, "") for column in columns} for row in rows)


def read_csv_schemas(folder: str | Path) -> dict[str, list[str]]:
    schemas: dict[str, list[str]] = {}
    for path in Path(folder).glob("*.csv"):
        with path.open("r", newline="", encoding="utf-8") as f:
            schemas[path.stem] = next(csv.reader(f), [])
    return schemas


def index_by(rows: list[dict], key: str) -> dict[str, dict]:
    return {row.get(key, ""): row for row in rows if row.get(key, "")}


def _supplemental_rows_by_key(schema_dir: str | Path | None, table_name: str, columns: list[str]) -> tuple[str, dict[str, dict]]:
    if not schema_dir or not columns:
        return "", {}
    schema_path = Path(schema_dir) / f"{table_name}.csv"
    if not schema_path.exists():
        return "", {}
    key_column = columns[0]
    return key_column, index_by(read_rows(schema_path), key_column)


def _fill_missing_from_supplement(row: dict, supplement: dict | None, columns: list[str]) -> dict:
    if not supplement:
        return row
    merged = dict(row)
    for column in columns:
        if str(merged.get(column, "")).strip() == "" and str(supplement.get(column, "")).strip() != "":
            merged[column] = supplement[column]
    return merged


def load_enrichments(prd2_dir: Path) -> dict[str, tuple[str, dict[str, dict]]]:
    enrichments = {}
    for file_name, (table_name, key_column) in ENRICHMENT_MAP.items():
        rows = read_prd2_rows(prd2_dir, file_name)
        enrichments[table_name] = (key_column, index_by(rows, key_column))
    return enrichments


def build_product_combined(
    run_id: str | None = None,
    output_run_id: str | None = None,
    output_root: str | Path | None = None,
    mode: str = "mlops",
    base_dir: str | Path | None = None,
    schema_dir: str | Path | None = None,
    delta_folder: str = "prd_delta",
) -> Path:
    if schema_dir:
        schemas = read_csv_schemas(schema_dir)
    else:
        ddl = parse_enhanced_ddl(str(MLOPS_DDL_PATH))
        schemas: dict[str, list[str]] = ddl["tables"]

    if run_id:
        base_dir = Path(base_dir) if base_dir else ROOT / "data" / "synthetic" / "base" / run_id
        if delta_folder == "prd_01_addons":
            prd2_dir = ROOT / "data" / "raw" / mode / "prd_01" / run_id / "addons"
        elif delta_folder == "vault_ready_28":
            prd2_dir = ROOT / "data" / "raw" / mode / "prd_01" / run_id / "vault_ready_28"
        else:
            prd2_dir = ROOT / "data" / "raw" / mode / delta_folder / run_id
    else:
        if delta_folder == "prd_01_addons":
            latest_prd1 = latest_subdir(ROOT / "data" / "raw" / mode / "prd_01")
            prd2_dir = latest_prd1 / "addons"
            run_id = latest_prd1.name
        elif delta_folder == "vault_ready_28":
            latest_prd1 = latest_subdir(ROOT / "data" / "raw" / mode / "prd_01")
            prd2_dir = latest_prd1 / "vault_ready_28"
            run_id = latest_prd1.name
        else:
            prd2_dir = latest_subdir(ROOT / "data" / "raw" / mode / delta_folder)
            run_id = prd2_dir.name
        base_dir = Path(base_dir) if base_dir else ROOT / "data" / "synthetic" / "base" / run_id

    if not base_dir.exists():
        raise FileNotFoundError(f"Base synthetic folder not found: {base_dir}")
    if not prd2_dir.exists():
        raise FileNotFoundError(f"Product delta raw folder not found: {prd2_dir}")

    output_run_id = output_run_id or datetime.now().strftime("%Y%m%d%H%M%S")
    out_root = Path(output_root) if output_root else Path(PRODUCT_COMBINED_ROOT)
    out_dir = out_root / output_run_id
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    enrichments = load_enrichments(prd2_dir)
    written: set[str] = set()

    for table_name, columns in schemas.items():
        base_path = base_dir / f"{table_name}.csv"
        if not base_path.exists():
            continue
        rows = read_rows(base_path)
        supplemental_key, supplemental_by_key = _supplemental_rows_by_key(schema_dir, table_name, columns)
        if table_name in enrichments:
            key_column, enrichment_by_key = enrichments[table_name]
            merged_rows = []
            for row in rows:
                merged = dict(row)
                merged.update(enrichment_by_key.get(row.get(key_column, ""), {}))
                merged = _fill_missing_from_supplement(merged, supplemental_by_key.get(merged.get(supplemental_key, "")), columns)
                merged_rows.append(merged)
            rows = merged_rows
        elif supplemental_by_key:
            rows = [
                _fill_missing_from_supplement(row, supplemental_by_key.get(row.get(supplemental_key, "")), columns)
                for row in rows
            ]
        write_rows(out_dir, table_name, rows, columns)
        written.add(table_name)

    for raw_file, (hub_table, sat_table) in ENTITY_MAP.items():
        rows = read_prd2_rows(prd2_dir, raw_file)
        if hub_table in schemas:
            write_rows(out_dir, hub_table, rows, schemas[hub_table])
            written.add(hub_table)
        if sat_table in schemas:
            write_rows(out_dir, sat_table, rows, schemas[sat_table])
            written.add(sat_table)

    for raw_file, link_table in BRIDGE_MAP.items():
        rows = read_prd2_rows(prd2_dir, raw_file)
        if link_table in schemas:
            write_rows(out_dir, link_table, rows, schemas[link_table])
            written.add(link_table)

    missing = sorted(set(schemas) - written)
    if missing:
        raise RuntimeError(f"Product combined vault missing tables: {missing}")

    with (out_dir / "_source_run_id.txt").open("w", encoding="utf-8") as f:
        f.write(f"source_run_id={run_id}\n")
        f.write(f"base_dir={base_dir}\n")
        f.write(f"delta_dir={prd2_dir}\n")

    print(f"Product combined vault written to: {out_dir}")
    return out_dir


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a MLOps-shaped product_combined vault from PRD1/base and product delta raw.")
    parser.add_argument("--run-id", help="Source run id. Defaults to latest data/raw/<mode>/prd_delta run.")
    parser.add_argument("--output-run-id", help="Output folder name. Defaults to current timestamp.")
    parser.add_argument("--mode", choices=["enhanced", "mlops"], default="mlops")
    parser.add_argument("--delta-folder", default="prd_delta", help="Raw delta folder under data/raw/<mode>. Defaults to prd_delta.")
    parser.add_argument(
        "--output-root",
        choices=["product_combined", "synthetic_mlops"],
        default="product_combined",
        help="Write to data/product_combined or data/synthetic/mlops.",
    )
    args = parser.parse_args()
    output_root = PRODUCT_COMBINED_ROOT if args.output_root == "product_combined" else MLOPS_ROOT
    build_product_combined(
        run_id=args.run_id,
        output_run_id=args.output_run_id,
        output_root=output_root,
        mode=args.mode,
        delta_folder=args.delta_folder,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
