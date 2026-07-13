import argparse
import csv
import os
import re
import sys
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.storage_paths import BRONZE_CLAIMS_ROOT, GOLD_CLAIMS_ROOT, SILVER_CLAIMS_ROOT
from generators.claim_ldm_generator import CLAIMS_LDM_SCHEMAS
from helper.scd2_diff_engine import latest_subdir
from misc.claims_product_pipeline import HUB_DEFS, LINK_DEFS, SAT_DEFS, TABLE_TO_HUB_HASH


RAW_PK = {
    "coverage.csv": "coverage_identifier",
    "policy_coverage.csv": "policy_coverage_identifier",
    "person.csv": "person_identifier",
    "claim_participant.csv": "claim_participant_identifier",
    "insured_entity.csv": "insured_entity_identifier",
    "policy.csv": "policy_identifier",
    "claim.csv": "claim_identifier",
    "loss_event.csv": "loss_event_identifier",
    "claim_event.csv": "claim_event_identifier",
    "physical_place.csv": "physical_place_identifier",
    "catastrophe.csv": "catastrophe_identifier",
    "treatment.csv": "treatment_identifier",
    "health_insurance_claim.csv": "health_insurance_claim_identifier",
    "police_report.csv": "police_report_identifier",
    "medical_report.csv": "medical_report_identifier",
    "settlement.csv": "settlement_identifier",
    "injury.csv": "injury_identifier",
    "claim_investigation.csv": "claim_investigation_identifier",
    "repair.csv": "repair_identifier",
    "litigation.csv": "litigation_identifier",
    "medical_assessment.csv": "medical_assessment_identifier",
    "medical_condition.csv": "medical_condition_identifier",
    "diagnosis.csv": "diagnosis_identifier",
}

RAW_FK = [
    ("policy_coverage.csv", "coverage_identifier", "coverage.csv", "coverage_identifier"),
    ("policy_coverage.csv", "insured_entity_identifier", "insured_entity.csv", "insured_entity_identifier"),
    ("claim.csv", "policy_identifier", "policy.csv", "policy_identifier"),
    ("claim.csv", "policy_coverage_identifier", "policy_coverage.csv", "policy_coverage_identifier"),
    ("claim.csv", "insured_entity_identifier", "insured_entity.csv", "insured_entity_identifier"),
    ("loss_event.csv", "claim_identifier", "claim.csv", "claim_identifier"),
    ("loss_event.csv", "physical_place_identifier", "physical_place.csv", "physical_place_identifier"),
    ("claim_event.csv", "claim_identifier", "claim.csv", "claim_identifier"),
    ("catastrophe.csv", "loss_event_identifier", "loss_event.csv", "loss_event_identifier"),
    ("claim_participant.csv", "claim_identifier", "claim.csv", "claim_identifier"),
    ("claim_participant.csv", "person_identifier", "person.csv", "person_identifier"),
    ("treatment.csv", "claim_event_identifier", "claim_event.csv", "claim_event_identifier"),
    ("treatment.csv", "claim_participant_identifier", "claim_participant.csv", "claim_participant_identifier"),
    ("treatment.csv", "medical_condition_identifier", "medical_condition.csv", "medical_condition_identifier"),
    ("health_insurance_claim.csv", "claim_identifier", "claim.csv", "claim_identifier"),
    ("police_report.csv", "claim_identifier", "claim.csv", "claim_identifier"),
    ("medical_report.csv", "claim_identifier", "claim.csv", "claim_identifier"),
    ("settlement.csv", "claim_event_identifier", "claim_event.csv", "claim_event_identifier"),
    ("injury.csv", "claim_event_identifier", "claim_event.csv", "claim_event_identifier"),
    ("claim_investigation.csv", "claim_event_identifier", "claim_event.csv", "claim_event_identifier"),
    ("repair.csv", "claim_event_identifier", "claim_event.csv", "claim_event_identifier"),
    ("repair.csv", "claim_participant_identifier", "claim_participant.csv", "claim_participant_identifier"),
    ("litigation.csv", "claim_event_identifier", "claim_event.csv", "claim_event_identifier"),
    ("litigation.csv", "claim_identifier", "claim.csv", "claim_identifier"),
    ("medical_assessment.csv", "claim_event_identifier", "claim_event.csv", "claim_event_identifier"),
    ("diagnosis.csv", "claim_event_identifier", "claim_event.csv", "claim_event_identifier"),
    ("diagnosis.csv", "injury_identifier", "injury.csv", "injury_identifier"),
    ("diagnosis.csv", "medical_condition_identifier", "medical_condition.csv", "medical_condition_identifier"),
]


FORBIDDEN_DUPLICATE_ID_COLUMNS = {
    "coverage_id", "policy_coverage_id", "claim_id", "loss_event_id", "claim_event_id",
    "physical_place_id", "catastrophe_id", "treatment_id", "medical_report_id",
    "settlement_id", "injury_id", "litigation_id", "diagnosis_id",
}


def _normalize_name(value):
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return (
        value.replace("pyschological", "psychological")
        .replace("geocodong", "geocoding")
        .replace("negliegence", "negligence")
        .replace("calaiamnt", "claimant")
    )


def _identifier_column(value):
    value = _normalize_name(value)
    return re.sub(r"(^|_)id($|_)", lambda match: f"{match.group(1)}identifier{match.group(2)}", value)


def _logical_csv_schema():
    path = os.path.join(ROOT, "claims", "claim_logical_model_tables_columns.csv")
    if not os.path.exists(path):
        return {}
    expected = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            table = f"{_normalize_name(row.get('table_name'))}.csv"
            column = _identifier_column(row.get("column_name"))
            expected.setdefault(table, [])
            if column and column not in expected[table]:
                expected[table].append(column)
    return expected


def _read(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _parse_dt(value):
    if not value:
        return None
    return datetime.fromisoformat(str(value)[:19])


def _require(condition, message, failures):
    if not condition:
        failures.append(message)


def verify_claims_product(run_id=None, bronze_dir=None, silver_dir=None, gold_dir=None):
    bronze_dir = bronze_dir or (os.path.join(BRONZE_CLAIMS_ROOT, run_id) if run_id else latest_subdir(BRONZE_CLAIMS_ROOT))
    if not bronze_dir:
        raise SystemExit(f"No claims bronze run found under {BRONZE_CLAIMS_ROOT}")
    run_id = run_id or os.path.basename(bronze_dir)
    silver_dir = silver_dir or os.path.join(SILVER_CLAIMS_ROOT, run_id)
    gold_dir = gold_dir or os.path.join(GOLD_CLAIMS_ROOT, run_id)

    failures = []
    raw = {}
    logical_schema = _logical_csv_schema()
    if logical_schema:
        for name, expected_cols in logical_schema.items():
            actual_cols = CLAIMS_LDM_SCHEMAS.get(name)
            _require(actual_cols is not None, f"logical model table {name} missing from generated schema", failures)
            if actual_cols is not None:
                missing = [col for col in expected_cols if col not in actual_cols]
                extra = [col for col in actual_cols if col not in expected_cols]
                _require(not missing, f"{name} missing logical-model columns {missing}", failures)
                _require(not extra, f"{name} has columns outside logical model {extra}", failures)

    for name, schema in CLAIMS_LDM_SCHEMAS.items():
        path = os.path.join(bronze_dir, name)
        _require(os.path.exists(path), f"missing bronze file {name}", failures)
        rows = _read(path)
        raw[name] = rows
        if rows:
            actual_cols = set(rows[0])
            missing_cols = [col for col in schema if col not in actual_cols]
            forbidden = sorted(actual_cols & FORBIDDEN_DUPLICATE_ID_COLUMNS)
            _require(not missing_cols, f"{name} missing columns {missing_cols}", failures)
            _require(not forbidden, f"{name} has forbidden duplicate id columns {forbidden}", failures)

    for name, pk in RAW_PK.items():
        seen = set()
        for row in raw.get(name, []):
            value = row.get(pk, "")
            _require(bool(value), f"{name}.{pk} has blank value", failures)
            _require(value not in seen, f"{name}.{pk} duplicate value {value}", failures)
            seen.add(value)

    indexes = {
        name: {row.get(pk, "") for row in raw.get(name, []) if row.get(pk, "")}
        for name, pk in RAW_PK.items()
    }
    for child, child_col, parent, parent_col in RAW_FK:
        parent_values = indexes.get(parent, set())
        for row in raw.get(child, []):
            value = row.get(child_col, "")
            if not value:
                continue
            _require(value in parent_values, f"{child}.{child_col}={value} missing parent {parent}.{parent_col}", failures)

    policies = {row["policy_identifier"]: row for row in raw.get("policy.csv", [])}
    for row in raw.get("claim.csv", []):
        open_dt = _parse_dt(row.get("claim_open_date"))
        close_dt = _parse_dt(row.get("claim_close_date"))
        policy = policies.get(row.get("policy_identifier", ""), {})
        policy_start = _parse_dt(policy.get("policy_inception_date"))
        policy_end = _parse_dt(policy.get("policy_end_date"))
        if open_dt and policy_start:
            _require(open_dt >= policy_start, f"claim {row['claim_identifier']} opens before policy starts", failures)
        if open_dt and policy_end:
            _require(open_dt <= policy_end, f"claim {row['claim_identifier']} opens after policy ends", failures)
        if close_dt and open_dt:
            _require(close_dt >= open_dt, f"claim {row['claim_identifier']} closes before opening", failures)
        if row.get("claim_status", "").lower() == "closed":
            _require(bool(close_dt), f"closed claim {row['claim_identifier']} missing close date", failures)

    for row in raw.get("loss_event.csv", []):
        loss_dt = _parse_dt(row.get("loss_date"))
        effective_dt = _parse_dt(row.get("date_of_effective_loss"))
        start_dt = _parse_dt(row.get("loss_event_start_date"))
        end_dt = _parse_dt(row.get("loss_event_end_date"))
        notification_dt = _parse_dt(row.get("notification_date"))
        if loss_dt and notification_dt:
            _require(notification_dt >= loss_dt, f"loss_event {row['loss_event_identifier']} notification before loss", failures)
        if effective_dt and loss_dt:
            _require(effective_dt == loss_dt, f"loss_event {row['loss_event_identifier']} effective loss date differs from loss date", failures)
        if start_dt and end_dt:
            _require(end_dt >= start_dt, f"loss_event {row['loss_event_identifier']} ends before it starts", failures)

    for row in raw.get("litigation.csv", []):
        rep_dt = _parse_dt(row.get("date_of_legal_representation"))
        first_dt = _parse_dt(row.get("first_litigation_date"))
        if rep_dt and first_dt:
            _require(rep_dt <= first_dt, f"litigation {row['litigation_identifier']} legal representation after first litigation date", failures)

    silver_files = list(HUB_DEFS) + list(SAT_DEFS) + list(LINK_DEFS)
    for name in silver_files:
        path = os.path.join(silver_dir, name)
        _require(os.path.exists(path), f"missing silver file {name}", failures)

    for hub_name, (hash_key, business_key, _) in HUB_DEFS.items():
        rows = _read(os.path.join(silver_dir, hub_name))
        seen = set()
        for row in rows:
            hk = row.get(hash_key, "")
            _require(bool(hk), f"{hub_name}.{hash_key} blank", failures)
            _require(hk not in seen, f"{hub_name}.{hash_key} duplicate {hk}", failures)
            _require(bool(row.get(business_key, "")), f"{hub_name}.{business_key} blank", failures)
            seen.add(hk)

    hub_hashes = {}
    for hub_name, (hash_key, _, raw_file) in HUB_DEFS.items():
        table_name = raw_file.removesuffix(".csv")
        hub_hashes[TABLE_TO_HUB_HASH[table_name]] = {
            row.get(hash_key, "") for row in _read(os.path.join(silver_dir, hub_name))
        }

    for link_name, endpoints in LINK_DEFS.items():
        for row in _read(os.path.join(silver_dir, link_name)):
            for table_name, _, _ in endpoints:
                hash_col = TABLE_TO_HUB_HASH[table_name]
                _require(row.get(hash_col, "") in hub_hashes.get(hash_col, set()), f"{link_name}.{hash_col} missing hub parent", failures)

    _require(os.path.exists(os.path.join(gold_dir, "fact_claim.csv")), "missing gold fact_claim.csv", failures)
    _require(os.path.exists(os.path.join(gold_dir, "claim_status_summary.csv")), "missing gold claim_status_summary.csv", failures)

    if failures:
        for failure in failures[:50]:
            print(f"FAIL: {failure}")
        if len(failures) > 50:
            print(f"... {len(failures) - 50} more failures")
        return False

    print(f"PASS: claims product verified for run_id={run_id}")
    print(f"bronze={bronze_dir}")
    print(f"silver={silver_dir}")
    print(f"gold={gold_dir}")
    return True


def parse_args():
    parser = argparse.ArgumentParser(description="Verify claims raw/bronze/silver/gold product outputs.")
    parser.add_argument("--run-id")
    parser.add_argument("--bronze-dir")
    parser.add_argument("--silver-dir")
    parser.add_argument("--gold-dir")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    ok = verify_claims_product(
        run_id=args.run_id,
        bronze_dir=args.bronze_dir,
        silver_dir=args.silver_dir,
        gold_dir=args.gold_dir,
    )
    raise SystemExit(0 if ok else 1)
