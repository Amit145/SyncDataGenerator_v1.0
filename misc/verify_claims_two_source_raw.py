import argparse
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from generators.claim_ldm_generator import (  # noqa: E402
    CLAIMS_LDM_SCHEMAS,
    CLAIMS_RAW_FILE_NAMES,
    _load_claims_two_source_spec,
)


def _latest_run(path: Path) -> str:
    runs = [item for item in path.iterdir() if item.is_dir()]
    if not runs:
        raise FileNotFoundError(f"No runs found under {path}")
    return max(runs, key=lambda item: (item.name, item.stat().st_mtime)).name


def _read(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _header(path: Path) -> list[str]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        return next(csv.reader(f), [])


def _key_counts(rows: list[dict], columns: list[str]) -> dict[tuple[str, ...], int]:
    counts: dict[tuple[str, ...], int] = {}
    for row in rows:
        key = tuple(str(row.get(column, "")).strip() for column in columns)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _matched_row_count(
    left_rows: list[dict],
    left_columns: list[str],
    right_rows: list[dict],
    right_columns: list[str],
) -> tuple[int, int, int]:
    left_counts = _key_counts(left_rows, left_columns)
    right_counts = _key_counts(right_rows, right_columns)
    common_keys = set(left_counts) & set(right_counts)
    matched = sum(min(left_counts[key], right_counts[key]) for key in common_keys)
    left_unmatched = sum(left_counts[key] for key in set(left_counts) - set(right_counts))
    right_unmatched = sum(right_counts[key] for key in set(right_counts) - set(left_counts))
    return matched, left_unmatched, right_unmatched


def verify(run_id: str | None = None) -> int:
    base = ROOT / "data" / "raw" / "claims"
    run_id = run_id or _latest_run(base)
    root = base / run_id
    source_raw = root / "raw"
    prd1 = root / "prd_01"
    prd2 = root / "prd_02"
    raw_vault = root / "raw_vault"
    errors = 0

    for label, folder in [("source raw", source_raw), ("prd_01", prd1), ("prd_02", prd2), ("raw_vault", raw_vault)]:
        if folder.exists():
            print(f"PASS: claims {label} exists: {folder}")
        else:
            print(f"FAIL: claims {label} missing: {folder}")
            errors += 1
    if errors:
        return errors

    mappings = _load_claims_two_source_spec()
    split_files = ["claim.csv", "loss_event.csv", "claim_investigation.csv"]
    raw_file_names = {value: key for key, value in CLAIMS_RAW_FILE_NAMES.items()}

    expected_prd1_files = sorted(CLAIMS_RAW_FILE_NAMES.values())
    prd1_files = sorted(path.name for path in prd1.glob("*.csv"))
    if prd1_files == expected_prd1_files:
        print(f"PASS: claims PRD1 has all {len(expected_prd1_files)} LDM raw files")
    else:
        print(
            "FAIL: claims PRD1 file set mismatch "
            f"missing={sorted(set(expected_prd1_files) - set(prd1_files))} "
            f"extra={sorted(set(prd1_files) - set(expected_prd1_files))}"
        )
        errors += 1

    for raw_file in split_files:
        source_name = CLAIMS_RAW_FILE_NAMES[raw_file]
        source_rows = _read(source_raw / source_name)
        prd1_rows = _read(prd1 / source_name)
        prd2_rows = _read(prd2 / raw_file)
        if len(source_rows) == len(prd1_rows) == len(prd2_rows):
            print(f"PASS: claims {raw_file} PRD1/PRD2 counts match source rows={len(source_rows)}")
        else:
            print(
                f"FAIL: claims {raw_file} row count mismatch "
                f"source={len(source_rows)} prd1={len(prd1_rows)} prd2={len(prd2_rows)}"
            )
            errors += 1

        expected_prd1 = CLAIMS_LDM_SCHEMAS[raw_file]
        expected_prd2 = ["batch_ref", "pull_ts", "origin_sys"] + [row["src2"] for row in mappings[raw_file] if row["src2"]]
        actual_prd1 = _header(prd1 / source_name)
        actual_prd2 = _header(prd2 / raw_file)
        if actual_prd1 == expected_prd1 and actual_prd2 == expected_prd2:
            print(f"PASS: claims {raw_file} PRD1 LDM schema and PRD2 workbook schema match")
        else:
            print(f"FAIL: claims {raw_file} schema mismatch")
            if actual_prd1 != expected_prd1:
                print(f"  PRD1 LDM expected={expected_prd1}")
                print(f"  PRD1 actual={actual_prd1}")
            if actual_prd2 != expected_prd2:
                print(f"  PRD2 expected={expected_prd2}")
                print(f"  PRD2 actual={actual_prd2}")
            errors += 1

        blank_pull_ts = sum(1 for row in prd2_rows if not str(row.get("pull_ts", "")).strip())
        unique_pull_ts = {str(row.get("pull_ts", "")).strip() for row in prd2_rows if str(row.get("pull_ts", "")).strip()}
        if blank_pull_ts == 0 and len(unique_pull_ts) == 1:
            print(f"PASS: claims {raw_file} PRD2 pull_ts populated and consistent {len(prd2_rows)}/{len(prd2_rows)} value={next(iter(unique_pull_ts), '')}")
        else:
            print(
                f"FAIL: claims {raw_file} PRD2 pull_ts blank rows={blank_pull_ts}/{len(prd2_rows)} "
                f"unique_nonblank={len(unique_pull_ts)}"
            )
            errors += 1

    claim_prd1_rows = _read(prd1 / "claim_register.csv")
    claim_prd2_rows = _read(prd2 / "claim.csv")
    matched, prd1_unmatched, prd2_unmatched = _matched_row_count(
        claim_prd1_rows,
        ["policy_identifier", "coverage_identifier", "claim_type", "claim_open_date"],
        claim_prd2_rows,
        ["policy_reference", "coverage_reference", "loss_category", "first_notice_date"],
    )
    differing_claim_refs = sum(
        1
        for left, right in zip(claim_prd1_rows, claim_prd2_rows)
        if str(left.get("claim_identifier", "")).strip() != str(right.get("claim_ref_id", "")).strip()
    )
    if matched == len(claim_prd1_rows) == len(claim_prd2_rows) and differing_claim_refs > 0:
        print(
            "PASS: claims PRD1/PRD2 claim business match works "
            f"rows={matched}, differing_claim_refs={differing_claim_refs}"
        )
    else:
        print(
            "FAIL: claims PRD1/PRD2 claim business match issue "
            f"matched={matched} prd1_unmatched={prd1_unmatched} prd2_unmatched={prd2_unmatched} "
            f"differing_claim_refs={differing_claim_refs}"
        )
        errors += 1

    loss_prd1_rows = _read(prd1 / "loss_event_register.csv")
    loss_prd2_rows = _read(prd2 / "loss_event.csv")
    matched, prd1_unmatched, prd2_unmatched = _matched_row_count(
        loss_prd1_rows,
        ["loss_event_identifier"],
        loss_prd2_rows,
        ["incident_id"],
    )
    if matched == len(loss_prd1_rows) == len(loss_prd2_rows):
        print(f"PASS: claims PRD1/PRD2 loss event reference match works rows={matched}")
    else:
        print(
            "FAIL: claims PRD1/PRD2 loss event reference match issue "
            f"matched={matched} prd1_unmatched={prd1_unmatched} prd2_unmatched={prd2_unmatched}"
        )
        errors += 1

    investigation_prd1_rows = _read(prd1 / "claim_investigation_register.csv")
    investigation_prd2_rows = _read(prd2 / "claim_investigation.csv")
    matched, prd1_unmatched, prd2_unmatched = _matched_row_count(
        investigation_prd1_rows,
        ["claim_event_identifier", "claim_investigation_start_date"],
        investigation_prd2_rows,
        ["case_event_id", "case_open_date"],
    )
    if matched == len(investigation_prd1_rows) == len(investigation_prd2_rows):
        print(f"PASS: claims PRD1/PRD2 investigation event/start-date match works rows={matched}")
    else:
        print(
            "FAIL: claims PRD1/PRD2 investigation event/start-date match issue "
            f"matched={matched} prd1_unmatched={prd1_unmatched} prd2_unmatched={prd2_unmatched}"
        )
        errors += 1

    raw_vault_files = sorted(path.name for path in raw_vault.glob("*.csv"))
    expected_raw_vault = sorted(CLAIMS_RAW_FILE_NAMES.values())
    if raw_vault_files == expected_raw_vault:
        print(f"PASS: claims raw_vault has all {len(expected_raw_vault)} LDM raw files")
    else:
        print(
            "FAIL: claims raw_vault file set mismatch "
            f"missing={sorted(set(expected_raw_vault) - set(raw_vault_files))} "
            f"extra={sorted(set(raw_vault_files) - set(expected_raw_vault))}"
        )
        errors += 1

    for csv_name in expected_raw_vault:
        raw_file = raw_file_names[csv_name]
        header = _header(raw_vault / csv_name)
        if header != CLAIMS_LDM_SCHEMAS[raw_file]:
            print(f"FAIL: claims raw_vault {csv_name} header does not match LDM schema")
            errors += 1

    claim_rows = _read(raw_vault / "claim_register.csv")
    claim_ids = {row.get("claim_identifier", "") for row in claim_rows if row.get("claim_identifier")}
    loss_rows = _read(raw_vault / "loss_event_register.csv")
    loss_claim_refs = {row.get("claim_identifier", "") for row in loss_rows if row.get("claim_identifier")}
    event_rows = _read(raw_vault / "claim_event_log.csv")
    event_ids = {row.get("claim_event_identifier", "") for row in event_rows if row.get("claim_event_identifier")}
    investigation_rows = _read(raw_vault / "claim_investigation_register.csv")
    investigation_event_refs = {
        row.get("claim_event_identifier", "")
        for row in investigation_rows
        if row.get("claim_event_identifier")
    }
    if loss_claim_refs.issubset(claim_ids) and investigation_event_refs.issubset(event_ids):
        print("PASS: claims raw_vault core claim/loss/investigation references are valid")
    else:
        print(
            "FAIL: claims raw_vault reference mismatch "
            f"loss_claim_missing={len(loss_claim_refs - claim_ids)} "
            f"investigation_event_missing={len(investigation_event_refs - event_ids)}"
        )
        errors += 1

    print(f"Claims two-source raw validation for {run_id} " + ("passed." if errors == 0 else f"failed with {errors} issue(s)."))
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify claims PRD1 LDM raw, PRD2 source-2 raw, and raw_vault consolidation.")
    parser.add_argument("--run-id", help="Claims run id. Defaults to latest data/raw/claims run.")
    args = parser.parse_args()
    return verify(args.run_id)


if __name__ == "__main__":
    raise SystemExit(main())
