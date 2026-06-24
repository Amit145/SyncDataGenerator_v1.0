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
    crm = _read(prd1 / "party_master.csv")
    sap = _read(prd2 / "person.csv")
    errors = 0

    for source_name, frame, type_col, rules in [
        ("CRM", crm, "party_kind", BUSINESS_VAULT_PERSON_MATCH_RULES),
        ("SAP", sap, "person_type", BUSINESS_VAULT_PERSON_MATCH_RULES),
    ]:
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
