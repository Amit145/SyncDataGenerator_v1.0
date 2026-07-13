from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from generators.raw_prd_generator import (
    BASE_PRD2_SAP_TABLES,
    BUSINESS_VAULT_EXCLUDED_MATCH_COLUMNS,
    BUSINESS_VAULT_PERSON_MATCH_RULES,
)


def _latest_run(path: Path) -> str:
    runs = [item for item in path.iterdir() if item.is_dir()]
    if not runs:
        raise FileNotFoundError(f"No runs found under {path}")
    return max(runs, key=lambda item: (item.name, item.stat().st_mtime)).name


def _read(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, keep_default_na=False, low_memory=False)
    frame.columns = [str(col).strip() for col in frame.columns]
    return frame


def _sap_id_series(series: pd.Series, prefix: str = "") -> set[str]:
    values = series.astype(str).str.strip()
    values = values[values.ne("")]
    if prefix:
        values = prefix + values
    return {value if value.startswith("SAP_") else f"SAP_{value}" for value in values}


def _nonblank_key(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    return frame[columns].astype(str).apply(lambda col: col.str.strip()).ne("").all(axis=1)


def _date_only(frame: pd.DataFrame, columns: list[str]) -> bool:
    date_columns = [col for col in columns if col in {"dob", "date_of_birth", "constitution_dt", "org_establishment_date"}]
    for col in date_columns:
        values = frame[col].astype(str).str.strip()
        values = values[values.ne("")]
        if not values.map(lambda value: bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", value))).all():
            return False
    return True


def verify(run_id: str | None = None, mode: str = "base") -> int:
    if run_id is None:
        run_id = _latest_run(ROOT / "data" / "raw" / mode / "prd_01")
    prd1 = ROOT / "data" / "raw" / mode / "prd_01" / run_id
    prd2 = ROOT / "data" / "raw" / mode / "prd_02" / run_id
    errors = 0

    crm_sources = {
        "party_master.csv": _read(prd1 / "party_master.csv"),
        "address_book.csv": _read(prd1 / "address_book.csv"),
        "product_catalog.csv": _read(prd1 / "product_catalog.csv"),
        "property_asset.csv": _read(prd1 / "property_asset.csv"),
        "vehicle_asset.csv": _read(prd1 / "vehicle_asset.csv"),
    }
    sap_prd2_available = prd2.exists()
    missing_files = []
    sap_frames: dict[str, pd.DataFrame] = {}
    if sap_prd2_available:
        for file_name, expected_columns in BASE_PRD2_SAP_TABLES.items():
            path = prd2 / file_name
            if not path.exists():
                missing_files.append(file_name)
                continue
            frame = _read(path)
            sap_frames[file_name] = frame
            missing_columns = [col for col in expected_columns if col not in frame.columns]
            row_status = "PASS" if len(frame) else "FAIL"
            print(f"{row_status}: SAP PRD2 {file_name} rows={len(frame)}")
            if not len(frame):
                errors += 1
            if missing_columns:
                print(f"FAIL: SAP PRD2 {file_name} missing columns {missing_columns}")
                errors += 1
        if missing_files:
            print(f"FAIL: SAP PRD2 missing files {missing_files}")
            errors += len(missing_files)
            return errors

        expected_counts = {
            "person.csv": len(crm_sources["party_master.csv"]),
            "address.csv": len(crm_sources["address_book.csv"]),
            "product.csv": len(crm_sources["product_catalog.csv"]),
            "home.csv": len(crm_sources["property_asset.csv"]),
            "motor.csv": len(crm_sources["vehicle_asset.csv"]),
            "insured_object.csv": len(crm_sources["property_asset.csv"]) + len(crm_sources["vehicle_asset.csv"]),
        }
        for file_name, expected in expected_counts.items():
            actual = len(sap_frames[file_name])
            if actual == expected:
                print(f"PASS: SAP PRD2 {file_name} count matches CRM source {actual}/{expected}")
            else:
                print(f"FAIL: SAP PRD2 {file_name} count mismatch actual={actual} expected={expected}")
                errors += 1
    elif mode == "enhanced":
        print("PASS: enhanced mode uses PRD1-only raw; SAP PRD2 checks skipped")
    else:
        print(f"FAIL: SAP PRD2 folder missing: {prd2}")
        return 1

    crm_party_type = crm_sources["party_master.csv"].set_index("party_ref")["party_kind"].astype(str).str.upper().to_dict()
    crm_address = crm_sources["address_book.csv"].copy()
    crm_address_required = ["address_type_txt", "region_txt"]
    missing_crm_address = [col for col in crm_address_required if col not in crm_address.columns]
    if missing_crm_address:
        print(f"FAIL: CRM PRD1 address_book.csv missing columns {missing_crm_address}")
        errors += 1
    else:
        expected_address_type = crm_address["party_ref"].map(crm_party_type).map(
            lambda value: "business address" if value == "LEGAL" else "personal"
        )
        actual_address_type = crm_address["address_type_txt"].astype(str).str.strip()
        address_type_ok = actual_address_type.eq(expected_address_type)
        country = crm_address["country_cd"].astype(str).str.strip().str.upper()
        region = crm_address["region_txt"].astype(str).str.strip()
        uk_region_ok = region[country.eq("UK")].eq("Europe").all()
        populated = int(crm_address[crm_address_required].astype(str).apply(lambda col: col.str.strip().ne("")).all(axis=1).sum())
        if address_type_ok.all() and uk_region_ok and populated == len(crm_address):
            print(f"PASS: CRM PRD1 address_book.csv address_type/region valid {len(crm_address)}/{len(crm_address)}")
        else:
            print(
                "FAIL: CRM PRD1 address_book.csv address_type/region invalid "
                f"address_type_bad={int((~address_type_ok).sum())} "
                f"uk_region_ok={uk_region_ok} populated={populated}/{len(crm_address)}"
            )
            errors += 1

    crm_product = crm_sources["product_catalog.csv"].copy()
    crm_policy = _read(prd1 / "policy_register.csv")
    crm_product_required = [
        "product_type_txt",
        "underwriting_group_txt",
        "regulatory_approval_cd",
        "product_status_txt",
        "product_lob_cd",
        "product_launch_dt",
    ]
    missing_crm_product = [col for col in crm_product_required if col not in crm_product.columns]
    if missing_crm_product:
        print(f"FAIL: CRM PRD1 product_catalog.csv missing columns {missing_crm_product}")
        errors += 1
    else:
        populated = int(crm_product[crm_product_required].astype(str).apply(lambda col: col.str.strip().ne("")).all(axis=1).sum())
        group_ok = crm_product["underwriting_group_txt"].astype(str).str.strip().isin(["GroupA", "GroupB", "GroupC"])
        product_type_ok = crm_product["product_type_txt"].astype(str).str.strip().eq(crm_product["product_cd"].astype(str).str.strip())
        rac_ok = crm_product["regulatory_approval_cd"].astype(str).str.strip().str.fullmatch(r"RAC\d{3}")
        lob_ok = crm_product["product_lob_cd"].astype(str).str.strip().str.fullmatch(r"LOB\d{3}")
        status_ok = crm_product["product_status_txt"].astype(str).str.strip().isin(["ACTIVE", "INACTIVE", "RETIRED"])

        first_policy_start = (
            crm_policy.assign(policy_start_dt=pd.to_datetime(crm_policy["policy_start_dt"], errors="coerce"))
            .groupby("product_ref", dropna=False)["policy_start_dt"]
            .min()
            .to_dict()
        )
        product_launch = pd.to_datetime(crm_product["product_launch_dt"], errors="coerce")
        first_start = crm_product["product_ref"].map(first_policy_start)
        launch_before_policy = product_launch.lt(first_start)
        if (
            populated == len(crm_product)
            and product_type_ok.all()
            and group_ok.all()
            and rac_ok.all()
            and lob_ok.all()
            and status_ok.all()
            and launch_before_policy.all()
        ):
            print(f"PASS: CRM PRD1 product_catalog.csv product metadata valid {len(crm_product)}/{len(crm_product)}")
        else:
            print(
                "FAIL: CRM PRD1 product_catalog.csv product metadata invalid "
                f"populated={populated}/{len(crm_product)} "
                f"product_type_bad={int((~product_type_ok).sum())} "
                f"group_bad={int((~group_ok).sum())} "
                f"rac_bad={int((~rac_ok).sum())} "
                f"lob_bad={int((~lob_ok).sum())} "
                f"status_bad={int((~status_ok).sum())} "
                f"launch_bad={int((~launch_before_policy).sum())}"
            )
            errors += 1

    if sap_prd2_available:
        id_checks = [
            ("person.csv", "person_id", crm_sources["party_master.csv"], "party_ref", ""),
            ("address.csv", "address_id", crm_sources["address_book.csv"], "address_ref", ""),
            ("product.csv", "product_id", crm_sources["product_catalog.csv"], "product_ref", ""),
            ("home.csv", "home_id", crm_sources["property_asset.csv"], "property_ref", ""),
            ("motor.csv", "motor_id", crm_sources["vehicle_asset.csv"], "vehicle_ref", ""),
        ]
        for file_name, sap_col, crm_frame, crm_col, prefix in id_checks:
            actual_ids = set(sap_frames[file_name][sap_col].astype(str).str.strip())
            expected_ids = _sap_id_series(crm_frame[crm_col], prefix=prefix)
            if actual_ids == expected_ids:
                print(f"PASS: SAP PRD2 {file_name}.{sap_col} reconciles to CRM {crm_col}")
            else:
                print(
                    f"FAIL: SAP PRD2 {file_name}.{sap_col} does not reconcile to CRM {crm_col}; "
                    f"missing={len(expected_ids - actual_ids)} extra={len(actual_ids - expected_ids)}"
                )
                errors += 1

    crm = crm_sources["party_master.csv"]
    sap = sap_frames.get("person.csv")
    sap_address = sap_frames.get("address.csv")
    sap_insured_object = sap_frames.get("insured_object.csv")

    match_sources = [("CRM", crm, "party_kind", BUSINESS_VAULT_PERSON_MATCH_RULES)]
    if sap_prd2_available:
        match_sources.append(("SAP", sap, "person_type", BUSINESS_VAULT_PERSON_MATCH_RULES))
    for source_name, frame, type_col, rules in match_sources:
        for entity_type in ["NATURAL", "LEGAL"]:
            required = rules[entity_type][source_name]
            missing = [col for col in required if col not in frame.columns]
            if missing:
                print(f"FAIL: {source_name} {entity_type} missing match columns {missing}")
                errors += 1
                continue
            subset = frame[frame[type_col].astype(str).str.upper().eq(entity_type)]
            valid = int(_nonblank_key(subset, required).sum()) if len(subset) else 0
            dates_ok = _date_only(subset, required)
            status = "PASS" if valid == len(subset) and dates_ok else "FAIL"
            print(f"{status}: {source_name} {entity_type} match keys {required} populated {valid}/{len(subset)}")
            if not dates_ok:
                print(f"FAIL: {source_name} {entity_type} match-key date columns must be YYYY-MM-DD only")
            errors += 0 if valid == len(subset) and dates_ok else 1

    excluded_present = sorted(BUSINESS_VAULT_EXCLUDED_MATCH_COLUMNS)
    print(f"PASS: excluded from match-key contract {excluded_present}")

    if not sap_prd2_available:
        print(f"Business Vault raw source matching contract for {mode}/{run_id} " + ("passed." if errors == 0 else f"failed with {errors} issue(s)."))
        return errors

    natural_sap = sap[sap["person_type"].astype(str).str.upper().eq("NATURAL")]
    middle_populated = int(natural_sap["middle_name"].astype(str).str.strip().ne("").sum()) if "middle_name" in natural_sap else 0
    if middle_populated == len(natural_sap):
        print(f"PASS: SAP person.middle_name populated for natural persons {middle_populated}/{len(natural_sap)}")
    else:
        print(f"FAIL: SAP person.middle_name populated for natural persons {middle_populated}/{len(natural_sap)}")
        errors += 1

    address_line_2_populated = int(sap_address["address_line_2"].astype(str).str.strip().ne("").sum()) if "address_line_2" in sap_address else 0
    if address_line_2_populated == len(sap_address):
        print(f"PASS: SAP address.address_line_2 populated {address_line_2_populated}/{len(sap_address)}")
    else:
        print(f"FAIL: SAP address.address_line_2 populated {address_line_2_populated}/{len(sap_address)}")
        errors += 1

    insured_required = [
        "insured_object_id",
        "policy_id",
        "insured_object_variant",
        "insured_object_sub_variant",
        "insured_amount",
        "insured_object_begin_date",
        "insured_object_finish_date",
    ]
    missing_insured = [col for col in insured_required + ["motor_id", "home_id"] if col not in sap_insured_object.columns]
    if missing_insured:
        print(f"FAIL: SAP insured_object.csv missing columns {missing_insured}")
        errors += 1
    else:
        populated = int(_nonblank_key(sap_insured_object, insured_required).sum()) if len(sap_insured_object) else 0
        one_asset = (
            sap_insured_object["motor_id"].astype(str).str.strip().ne("")
            ^ sap_insured_object["home_id"].astype(str).str.strip().ne("")
        )
        expected_insured_ids = (
            _sap_id_series(crm_sources["vehicle_asset.csv"]["vehicle_ref"], prefix="IO_")
            | _sap_id_series(crm_sources["property_asset.csv"]["property_ref"], prefix="IO_")
        )
        actual_insured_ids = set(sap_insured_object["insured_object_id"].astype(str).str.strip())
        policy_ids = set(sap_insured_object["policy_id"].astype(str).str.strip())
        expected_policy_ids = _sap_id_series(crm_sources["vehicle_asset.csv"]["policy_ref"]) | _sap_id_series(
            crm_sources["property_asset.csv"]["policy_ref"]
        )
        motor_ids = set(sap_insured_object["motor_id"].astype(str).str.strip())
        motor_ids.discard("")
        home_ids = set(sap_insured_object["home_id"].astype(str).str.strip())
        home_ids.discard("")
        expected_motor_ids = _sap_id_series(crm_sources["vehicle_asset.csv"]["vehicle_ref"])
        expected_home_ids = _sap_id_series(crm_sources["property_asset.csv"]["property_ref"])
        amount_ok = pd.to_numeric(sap_insured_object["insured_amount"], errors="coerce").fillna(0).gt(0)
        date_ok = sap_insured_object[["insured_object_begin_date", "insured_object_finish_date"]].astype(str).apply(
            lambda col: col.str.strip().map(lambda value: bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", value)))
        ).all(axis=1)
        ok_rows = int((one_asset & amount_ok & date_ok).sum())
        refs_ok = (
            actual_insured_ids == expected_insured_ids
            and policy_ids.issubset(expected_policy_ids)
            and motor_ids == expected_motor_ids
            and home_ids == expected_home_ids
        )
        if populated == len(sap_insured_object) and ok_rows == len(sap_insured_object) and refs_ok:
            print(f"PASS: SAP insured_object.csv relationships, dates, and amounts valid {ok_rows}/{len(sap_insured_object)}")
        else:
            print(
                "FAIL: SAP insured_object.csv relationships, dates, and amounts valid "
                f"{ok_rows}/{len(sap_insured_object)}; required populated {populated}/{len(sap_insured_object)}"
            )
            if not refs_ok:
                print(
                    "FAIL: SAP insured_object.csv ID/reference reconciliation "
                    f"insured_missing={len(expected_insured_ids - actual_insured_ids)} "
                    f"insured_extra={len(actual_insured_ids - expected_insured_ids)} "
                    f"policy_extra={len(policy_ids - expected_policy_ids)} "
                    f"motor_mismatch={motor_ids != expected_motor_ids} "
                    f"home_mismatch={home_ids != expected_home_ids}"
                )
            errors += 1

    print(f"Business Vault raw source matching contract for {mode}/{run_id} " + ("passed." if errors == 0 else f"failed with {errors} issue(s)."))
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify CRM/SAP raw files support the Business Vault person matching contract.")
    parser.add_argument("--run-id", default=None, help="Raw base run id. Defaults to latest PRD1 run.")
    parser.add_argument("--mode", choices=["base", "enhanced", "mlops"], default="base", help="Raw mode to validate.")
    args = parser.parse_args()
    return 1 if verify(args.run_id, mode=args.mode) else 0


if __name__ == "__main__":
    raise SystemExit(main())
