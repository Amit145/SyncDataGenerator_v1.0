from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SAP_PRD2_FILES = {
    "person.csv",
    "address.csv",
    "product.csv",
    "home.csv",
    "motor.csv",
    "insured_object.csv",
}


def latest_run(root: Path) -> str:
    runs = [path.name for path in root.iterdir() if path.is_dir()] if root.exists() else []
    if not runs:
        raise FileNotFoundError(f"No run folders found under {root}")
    return sorted(runs)[-1]


def read_header_and_keys(path: Path) -> tuple[list[str], int, set[str] | None, str | None]:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        key_col = next(
            (
                col
                for col in fieldnames
                if col.strip().lower().replace(" ", "_").endswith("hash_key")
            ),
            None,
        )
        row_count = 0
        keys: set[str] | None = set() if key_col else None
        for row in reader:
            row_count += 1
            if key_col and keys is not None:
                keys.add(row.get(key_col, ""))
    return fieldnames, row_count, keys, key_col


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


def find_hashed_source_refs(path: Path) -> list[dict]:
    hashed_refs = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        ref_columns = [column for column in (reader.fieldnames or []) if column.startswith("src_") and column.endswith("_ref")]
        for idx, row in enumerate(reader, start=2):
            for column in ref_columns:
                value = str(row.get(column, "")).strip()
                if re.fullmatch(r"[0-9a-f]{32}", value):
                    hashed_refs.append({"line": idx, "column": column, "value": value})
                    if len(hashed_refs) >= 5:
                        return hashed_refs
    return hashed_refs


def compare_folders_exact(source: Path, target: Path, allowed_extra_prefixes: tuple[str, ...] = ()) -> list[str]:
    issues: list[str] = []
    source_files = {path.name.lower(): path for path in source.glob("*.csv")}
    target_files = {path.name.lower(): path for path in target.glob("*.csv")}

    missing = sorted(source_files.keys() - target_files.keys())
    extra = sorted(
        name
        for name in target_files.keys() - source_files.keys()
        if not any(name.startswith(prefix) for prefix in allowed_extra_prefixes)
    )
    for name in missing:
        issues.append(f"missing in {target}: {name}")
    for name in extra:
        issues.append(f"extra in {target}: {name}")

    for name in sorted(source_files.keys() & target_files.keys()):
        src_header, src_rows, src_keys, src_key_col = read_header_and_keys(source_files[name])
        tgt_header, tgt_rows, tgt_keys, tgt_key_col = read_header_and_keys(target_files[name])

        if src_header != tgt_header:
            issues.append(f"{name}: header mismatch")
        if src_rows != tgt_rows:
            issues.append(f"{name}: row count mismatch source={src_rows} target={tgt_rows}")
        if src_key_col != tgt_key_col:
            issues.append(f"{name}: hash key column mismatch source={src_key_col} target={tgt_key_col}")
        if src_keys is not None and tgt_keys is not None and src_keys != tgt_keys:
            issues.append(f"{name}: hash key set mismatch")

    return issues


def compare_prd2_delta(base_dir: Path, mlops_dir: Path, prd2_dir: Path) -> list[str]:
    issues: list[str] = []
    if not mlops_dir.exists():
        return [f"MLOps folder not found: {mlops_dir}"]
    if not prd2_dir.exists():
        return [f"PRD2 raw folder not found: {prd2_dir}"]

    expected_sources = {
        "address_book.csv": ("hub_address.csv", "address_id"),
        "broker_book.csv": ("hub_broker.csv", "agent_id"),
        "campaign_register.csv": ("hub_campaign.csv", "campaign_id"),
        "channel_catalog.csv": ("hub_channel.csv", "channel_id"),
        "claim_register.csv": ("hub_claim.csv", "claim_id"),
        "complaint_register.csv": ("hub_complaint.csv", "complaint_id"),
        "insured_object_register.csv": ("hub_insured_object.csv", "insured_object_id"),
        "override_register.csv": ("hub_override.csv", "override_id"),
        "regulation_register.csv": ("hub_regulation.csv", "regulation_id"),
        "policy_broker_bridge.csv": ("link_policy_broker.csv", "policy_broker_hash_key"),
        "broker_person_bridge.csv": ("link_broker_person.csv", "broker_person_hash_key"),
        "policy_channel_bridge.csv": ("link_policy_channel.csv", "policy_channel_hash_key"),
        "policy_quote_bridge.csv": ("link_policy_quote.csv", "policy_quote_hash_key"),
        "claim_policy_bridge.csv": ("link_claim_policy.csv", "claim_policy_hash_key"),
        "complaint_policy_bridge.csv": ("link_complaint_policy.csv", "complaint_policy_hash_key"),
        "complaint_regulation_bridge.csv": ("link_complaint_regulation.csv", "complaint_regulation_hash_key"),
        "person_address_bridge.csv": ("link_person_address.csv", "person_address_hash_key"),
        "person_campaign_bridge.csv": ("link_person_campaign.csv", "person_campaign_hash_key"),
        "policy_insured_object_bridge.csv": ("link_policy_insured_object.csv", "policy_insured_object_hash_key"),
        "policy_override_bridge.csv": ("link_policy_override.csv", "policy_override_hash_key"),
        "insured_object_home_bridge.csv": ("link_insured_object_home.csv", "insured_object_home_hash_key"),
        "insured_object_motor_bridge.csv": ("link_insured_object_motor.csv", "insured_object_motor_hash_key"),
        "quote_broker_bridge.csv": ("link_quote_broker.csv", "quote_broker_hash_key"),
        "quote_channel_bridge.csv": ("link_quote_channel.csv", "quote_channel_hash_key"),
        "policy_enrichment.csv": ("sat_policy.csv", "policy_hash_key"),
        "customer_enrichment.csv": ("sat_customer.csv", "customer_hash_key"),
        "marketing_engagement_enrichment.csv": ("sat_marketing_engagement.csv", "marketing_engagement_hash_key"),
        "motor_enrichment.csv": ("sat_motor.csv", "motor_hash_key"),
    }
    prd2_files = {path.name.lower(): path for path in prd2_dir.glob("*.csv")}

    missing = sorted(expected_sources.keys() - prd2_files.keys())
    extra = sorted(prd2_files.keys() - expected_sources.keys())
    for name in missing:
        issues.append(f"missing PRD2 raw file: {name}")
    for name in extra:
        issues.append(f"unexpected PRD2 raw file: {name}")

    for raw_name, (mlops_name, id_column) in sorted(expected_sources.items()):
        if raw_name not in prd2_files:
            continue
        mlops_path = mlops_dir / mlops_name
        if not mlops_path.exists():
            issues.append(f"{raw_name}: source MLOps table missing: {mlops_name}")
            continue

        mlops_header, mlops_rows, _, _ = read_header_and_keys(mlops_path)
        raw_header, raw_rows, _, _ = read_header_and_keys(prd2_files[raw_name])
        translated_raw_header = [raw_to_vault_column(column) for column in raw_header]
        hashed_refs = find_hashed_source_refs(prd2_files[raw_name])
        if hashed_refs:
            issues.append(f"{raw_name}: source ref columns contain hash-looking values: {hashed_refs}")
        if mlops_rows != raw_rows:
            issues.append(f"{raw_name}: row count mismatch mlops {mlops_name}={mlops_rows} prd2={raw_rows}")
        if id_column not in translated_raw_header:
            issues.append(f"{raw_name}: missing raw id/source column for {id_column}")
        if mlops_header and mlops_header[0] not in translated_raw_header:
            issues.append(f"{raw_name}: missing source lineage column for {mlops_header[0]}")
        if any(column.endswith("_hash_key") or column.endswith(" hash key") for column in raw_header):
            issues.append(f"{raw_name}: PRD2 raw exposes vault hash key column names")
        if any(column in {"load_date", "record_source"} for column in raw_header):
            issues.append(f"{raw_name}: PRD2 raw exposes vault audit column names")

    if any(name.startswith(("hub_", "link_", "sat_")) for name in prd2_files):
        issues.append("PRD2 contains vault-shaped hub/link/sat files; expected source-style raw files only")

    return issues


def compare_enhanced_raw_vault(prd1_dir: Path, prd2_dir: Path, raw_vault_dir: Path) -> list[str]:
    issues: list[str] = []
    vault_ready_dir = prd1_dir / "vault_ready_28"
    if not vault_ready_dir.exists():
        return [f"enhanced vault_ready_28 folder not found: {vault_ready_dir}"]
    if not prd2_dir.exists():
        return [f"enhanced SAP PRD2 folder not found: {prd2_dir}"]
    if not raw_vault_dir.exists():
        return [f"enhanced raw_vault folder not found: {raw_vault_dir}"]

    prd2_files = {path.name.lower(): path for path in prd2_dir.glob("*.csv")}
    missing_sap = sorted(SAP_PRD2_FILES - set(prd2_files))
    extra_sap = sorted(set(prd2_files) - SAP_PRD2_FILES)
    for name in missing_sap:
        issues.append(f"enhanced PRD2 missing SAP file: {name}")
    for name in extra_sap:
        issues.append(f"enhanced PRD2 unexpected file: {name}")

    expected_raw_vault = {path.name.lower() for path in vault_ready_dir.glob("*.csv")} | SAP_PRD2_FILES
    actual_raw_vault = {path.name.lower() for path in raw_vault_dir.glob("*.csv")}
    missing_raw_vault = sorted(expected_raw_vault - actual_raw_vault)
    extra_raw_vault = sorted(actual_raw_vault - expected_raw_vault)
    for name in missing_raw_vault:
        issues.append(f"enhanced raw_vault missing file: {name}")
    for name in extra_raw_vault:
        issues.append(f"enhanced raw_vault unexpected file: {name}")

    if len(expected_raw_vault) != 34:
        issues.append(f"enhanced raw_vault expected contract should be 34 files, computed {len(expected_raw_vault)}")

    return issues


def compare_crm_to_prd1(crm_dir: Path, prd1_dir: Path) -> list[str]:
    if not crm_dir.exists():
        return [f"CRM raw folder not found: {crm_dir}"]
    if not prd1_dir.exists():
        return [f"PRD1 raw folder not found: {prd1_dir}"]
    return compare_folders_exact(crm_dir, prd1_dir)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify mode-scoped PRD raw folders against synthetic vault output.")
    parser.add_argument("--run-id", help="Run id to validate. Defaults to latest MLOps run.")
    parser.add_argument("--mode", choices=["base", "enhanced", "mlops"], default="mlops")
    parser.add_argument("--skip-prd1", action="store_true", help="Skip CRM raw vs PRD1 comparison.")
    args = parser.parse_args()

    synthetic_root = ROOT / "data" / "synthetic" / args.mode
    run_id = args.run_id or latest_run(synthetic_root)
    base_prd1_dir = ROOT / "data" / "raw" / "base" / "prd_01" / run_id
    prd1_dir = ROOT / "data" / "raw" / args.mode / "prd_01" / run_id
    prd2_dir = ROOT / "data" / "raw" / args.mode / "prd_02" / run_id
    product_delta_dir = ROOT / "data" / "raw" / args.mode / "prd_delta" / run_id
    raw_vault_dir = ROOT / "data" / "raw" / args.mode / "raw_vault" / run_id
    base_dir = ROOT / "data" / "synthetic" / "base" / run_id
    target_dir = ROOT / "data" / "synthetic" / args.mode / run_id

    issues: list[str] = []

    if not args.skip_prd1:
        if args.mode == "base":
            prd1_issues = []
            if not prd1_dir.exists():
                prd1_issues.append(f"PRD1 raw folder not found: {prd1_dir}")
        else:
            allowed_extra_prefixes = ("source1_",) if args.mode == "mlops" else ()
            prd1_issues = compare_folders_exact(base_prd1_dir, prd1_dir, allowed_extra_prefixes=allowed_extra_prefixes)
        if prd1_issues:
            print("FAIL: PRD1 raw does not match base PRD1 raw")
            issues.extend(f"PRD1: {issue}" for issue in prd1_issues)
        else:
            print("PASS: PRD1 raw matches base PRD1 raw")

    if args.mode == "enhanced":
        enhanced_issues = compare_enhanced_raw_vault(prd1_dir, prd2_dir, raw_vault_dir)
        if enhanced_issues:
            print("FAIL: enhanced PRD raw_vault contract failed")
            issues.extend(f"ENHANCED_RAW: {issue}" for issue in enhanced_issues)
        else:
            print("PASS: enhanced SAP PRD2 and consolidated raw_vault are present")
    elif args.mode != "base":
        prd2_issues = compare_prd2_delta(base_dir, target_dir, product_delta_dir)
        if prd2_issues:
            print(f"FAIL: product delta raw does not match {args.mode} added entities/bridges")
            issues.extend(f"PRD_DELTA: {issue}" for issue in prd2_issues)
        else:
            print(f"PASS: product delta source-style raw files reconcile to {args.mode} added entities/relationships")

    if issues:
        print(f"PRD raw validation failed with {len(issues)} issue(s).")
        for issue in issues:
            print(f"- {issue}")
        return 1

    print(f"PRD raw validation passed for mode={args.mode} run={run_id}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
