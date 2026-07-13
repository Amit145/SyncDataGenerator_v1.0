import csv
import hashlib
import os
import shutil
from datetime import datetime

from config.storage_paths import BRONZE_CLAIMS_ROOT, GOLD_CLAIMS_ROOT, SILVER_CLAIMS_ROOT
from generators.claim_ldm_generator import CLAIMS_LDM_SCHEMAS, CLAIMS_RAW_FILE_NAMES
from helper.csv_writer import write_csv


HUB_DEFS = {
    "hub_claim.csv": ("claim_hash_key", "claim_identifier", "claim.csv"),
    "hub_claim_event.csv": ("claim_event_hash_key", "claim_event_identifier", "claim_event.csv"),
    "hub_loss_event.csv": ("loss_event_hash_key", "loss_event_identifier", "loss_event.csv"),
    "hub_catastrophe.csv": ("catastrophe_hash_key", "catastrophe_identifier", "catastrophe.csv"),
    "hub_claim_participant.csv": ("claim_participant_hash_key", "claim_participant_identifier", "claim_participant.csv"),
    "hub_person.csv": ("person_hash_key", "person_identifier", "person.csv"),
    "hub_policy.csv": ("policy_hash_key", "policy_identifier", "policy.csv"),
    "hub_policy_coverage.csv": ("policy_coverage_hash_key", "policy_coverage_identifier", "policy_coverage.csv"),
    "hub_coverage.csv": ("coverage_hash_key", "coverage_identifier", "coverage.csv"),
    "hub_insured_entity.csv": ("insured_entity_hash_key", "insured_entity_identifier", "insured_entity.csv"),
    "hub_physical_place.csv": ("physical_place_hash_key", "physical_place_identifier", "physical_place.csv"),
    "hub_treatment.csv": ("treatment_hash_key", "treatment_identifier", "treatment.csv"),
    "hub_health_insurance_claim.csv": ("health_insurance_claim_hash_key", "health_insurance_claim_identifier", "health_insurance_claim.csv"),
    "hub_police_report.csv": ("police_report_hash_key", "police_report_identifier", "police_report.csv"),
    "hub_medical_report.csv": ("medical_report_hash_key", "medical_report_identifier", "medical_report.csv"),
    "hub_settlement.csv": ("settlement_hash_key", "settlement_identifier", "settlement.csv"),
    "hub_injury.csv": ("injury_hash_key", "injury_identifier", "injury.csv"),
    "hub_claim_investigation.csv": ("claim_investigation_hash_key", "claim_investigation_identifier", "claim_investigation.csv"),
    "hub_repair.csv": ("repair_hash_key", "repair_identifier", "repair.csv"),
    "hub_litigation.csv": ("litigation_hash_key", "litigation_identifier", "litigation.csv"),
    "hub_medical_assessment.csv": ("medical_assessment_hash_key", "medical_assessment_identifier", "medical_assessment.csv"),
    "hub_medical_condition.csv": ("medical_condition_hash_key", "medical_condition_identifier", "medical_condition.csv"),
    "hub_diagnosis.csv": ("diagnosis_hash_key", "diagnosis_identifier", "diagnosis.csv"),
}


def _build_sat_defs():
    sat_defs = {}
    for raw_file, schema in CLAIMS_LDM_SCHEMAS.items():
        table_name = raw_file.removesuffix(".csv")
        hub_name = f"hub_{table_name}.csv"
        if hub_name not in HUB_DEFS:
            continue
        hash_key, business_key, _ = HUB_DEFS[hub_name]
        attrs = [col for col in schema if col != business_key]
        sat_defs[f"sat_{table_name}.csv"] = (hash_key, raw_file, business_key, attrs)
    return sat_defs


SAT_DEFS = _build_sat_defs()
LINK_DEFS = {
    "link_claim_policy.csv": [("claim", "claim", "claim_identifier"), ("policy", "claim", "policy_identifier")],
    "link_claim_policy_coverage.csv": [("claim", "claim", "claim_identifier"), ("policy_coverage", "claim", "policy_coverage_identifier")],
    "link_claim_insured_entity.csv": [("claim", "claim", "claim_identifier"), ("insured_entity", "claim", "insured_entity_identifier")],
    "link_policy_coverage_coverage.csv": [("policy_coverage", "policy_coverage", "policy_coverage_identifier"), ("coverage", "policy_coverage", "coverage_identifier")],
    "link_policy_coverage_insured_entity.csv": [("policy_coverage", "policy_coverage", "policy_coverage_identifier"), ("insured_entity", "policy_coverage", "insured_entity_identifier")],
    "link_claim_loss_event.csv": [("loss_event", "loss_event", "loss_event_identifier"), ("claim", "loss_event", "claim_identifier")],
    "link_loss_event_physical_place.csv": [("loss_event", "loss_event", "loss_event_identifier"), ("physical_place", "loss_event", "physical_place_identifier")],
    "link_catastrophe_loss_event.csv": [("catastrophe", "catastrophe", "catastrophe_identifier"), ("loss_event", "catastrophe", "loss_event_identifier")],
    "link_claim_event_claim.csv": [("claim_event", "claim_event", "claim_event_identifier"), ("claim", "claim_event", "claim_identifier")],
    "link_claim_participant_claim.csv": [("claim_participant", "claim_participant", "claim_participant_identifier"), ("claim", "claim_participant", "claim_identifier")],
    "link_claim_participant_person.csv": [("claim_participant", "claim_participant", "claim_participant_identifier"), ("person", "claim_participant", "person_identifier")],
    "link_treatment_claim_event.csv": [("treatment", "treatment", "treatment_identifier"), ("claim_event", "treatment", "claim_event_identifier")],
    "link_treatment_claim_participant.csv": [("treatment", "treatment", "treatment_identifier"), ("claim_participant", "treatment", "claim_participant_identifier")],
    "link_treatment_medical_condition.csv": [("treatment", "treatment", "treatment_identifier"), ("medical_condition", "treatment", "medical_condition_identifier")],
    "link_health_claim_claim.csv": [("health_insurance_claim", "health_insurance_claim", "health_insurance_claim_identifier"), ("claim", "health_insurance_claim", "claim_identifier")],
    "link_police_report_claim.csv": [("police_report", "police_report", "police_report_identifier"), ("claim", "police_report", "claim_identifier")],
    "link_medical_report_claim.csv": [("medical_report", "medical_report", "medical_report_identifier"), ("claim", "medical_report", "claim_identifier")],
    "link_settlement_claim_event.csv": [("settlement", "settlement", "settlement_identifier"), ("claim_event", "settlement", "claim_event_identifier")],
    "link_injury_claim_event.csv": [("injury", "injury", "injury_identifier"), ("claim_event", "injury", "claim_event_identifier")],
    "link_claim_investigation_claim_event.csv": [("claim_investigation", "claim_investigation", "claim_investigation_identifier"), ("claim_event", "claim_investigation", "claim_event_identifier")],
    "link_repair_claim_event.csv": [("repair", "repair", "repair_identifier"), ("claim_event", "repair", "claim_event_identifier")],
    "link_repair_claim_participant.csv": [("repair", "repair", "repair_identifier"), ("claim_participant", "repair", "claim_participant_identifier")],
    "link_litigation_claim_event.csv": [("litigation", "litigation", "litigation_identifier"), ("claim_event", "litigation", "claim_event_identifier")],
    "link_litigation_claim.csv": [("litigation", "litigation", "litigation_identifier"), ("claim", "litigation", "claim_identifier")],
    "link_medical_assessment_claim_event.csv": [("medical_assessment", "medical_assessment", "medical_assessment_identifier"), ("claim_event", "medical_assessment", "claim_event_identifier")],
    "link_diagnosis_claim_event.csv": [("diagnosis", "diagnosis", "diagnosis_identifier"), ("claim_event", "diagnosis", "claim_event_identifier")],
    "link_diagnosis_injury.csv": [("diagnosis", "diagnosis", "diagnosis_identifier"), ("injury", "diagnosis", "injury_identifier")],
    "link_diagnosis_medical_condition.csv": [("diagnosis", "diagnosis", "diagnosis_identifier"), ("medical_condition", "diagnosis", "medical_condition_identifier")],
}


TABLE_FILE = {name.removesuffix(".csv"): name for name in CLAIMS_LDM_SCHEMAS}
TABLE_TO_HUB_HASH = {
    raw_file.removesuffix(".csv"): hash_key
    for hash_key, _, raw_file in HUB_DEFS.values()
}


def _read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _hash(value):
    return hashlib.md5(str(value or "").encode("utf-8")).hexdigest()


def _link_hash(values):
    return _hash("||".join(str(value or "") for value in values))


def _load_date(value=None):
    return value or datetime.now().replace(microsecond=0).isoformat()


def build_claims_bronze(raw_dir, run_id):
    bronze_dir = os.path.join(BRONZE_CLAIMS_ROOT, run_id)
    os.makedirs(bronze_dir, exist_ok=True)
    for name in CLAIMS_LDM_SCHEMAS:
        src = os.path.join(raw_dir, CLAIMS_RAW_FILE_NAMES.get(name, name))
        dst = os.path.join(bronze_dir, name)
        if os.path.exists(src):
            shutil.copyfile(src, dst)
        else:
            write_csv(bronze_dir, name, [], fieldnames=CLAIMS_LDM_SCHEMAS[name])
    return bronze_dir


def build_claims_silver(bronze_dir, run_id, load_date=None):
    silver_dir = os.path.join(SILVER_CLAIMS_ROOT, run_id)
    os.makedirs(silver_dir, exist_ok=True)
    load_date = _load_date(load_date)
    source = "CLAIMS_LDM"
    raw = {name: _read_csv(os.path.join(bronze_dir, name)) for name in CLAIMS_LDM_SCHEMAS}

    hub_indexes = {}
    for hub_name, (hash_key, business_key, raw_file) in HUB_DEFS.items():
        seen = set()
        hub_rows = []
        index = {}
        for row in raw.get(raw_file, []):
            key_value = row.get(business_key, "")
            if not key_value or key_value in seen:
                continue
            seen.add(key_value)
            hk = _hash(key_value)
            index[key_value] = hk
            hub_rows.append({
                hash_key: hk,
                business_key: key_value,
                "load_date": load_date,
                "record_source": source,
            })
        hub_indexes[raw_file.removesuffix(".csv")] = index
        write_csv(silver_dir, hub_name, hub_rows, fieldnames=[hash_key, business_key, "load_date", "record_source"])

    for sat_name, (hash_key, raw_file, raw_key, attrs) in SAT_DEFS.items():
        index = hub_indexes[raw_file.removesuffix(".csv")]
        sat_rows = []
        for row in raw.get(raw_file, []):
            key_value = row.get(raw_key, "")
            if not key_value or key_value not in index:
                continue
            sat_row = {
                hash_key: index[key_value],
                "load_date": load_date,
                "record_source": source,
            }
            for attr in attrs:
                sat_row[attr] = row.get(attr, "")
            sat_rows.append(sat_row)
        write_csv(silver_dir, sat_name, sat_rows, fieldnames=[hash_key, "load_date", "record_source"] + attrs)

    for link_name, endpoints in LINK_DEFS.items():
        source_table = endpoints[0][1]
        link_key = link_name.removesuffix(".csv") + "_hash_key"
        link_rows = []
        seen = set()
        for row in raw.get(TABLE_FILE[source_table], []):
            endpoint_values = []
            link_row = {}
            missing = False
            for table_name, _, raw_col in endpoints:
                business_value = row.get(raw_col, "")
                index = hub_indexes.get(table_name, {})
                hash_col = TABLE_TO_HUB_HASH.get(table_name, f"{table_name}_hash_key")
                hash_value = index.get(business_value)
                if not business_value or not hash_value:
                    missing = True
                    break
                endpoint_values.append(hash_value)
                link_row[hash_col] = hash_value
            if missing:
                continue
            relationship_hash = _link_hash(endpoint_values)
            if relationship_hash in seen:
                continue
            seen.add(relationship_hash)
            link_rows.append({
                link_key: relationship_hash,
                **link_row,
                "load_date": load_date,
                "record_source": source,
            })
        fieldnames = [link_key]
        for table_name, _, _ in endpoints:
            hash_col = TABLE_TO_HUB_HASH.get(table_name)
            if hash_col and hash_col not in fieldnames:
                fieldnames.append(hash_col)
        fieldnames += ["load_date", "record_source"]
        write_csv(silver_dir, link_name, link_rows, fieldnames=fieldnames)

    return silver_dir


def build_claims_gold(bronze_dir, run_id):
    gold_dir = os.path.join(GOLD_CLAIMS_ROOT, run_id)
    os.makedirs(gold_dir, exist_ok=True)
    claims = _read_csv(os.path.join(bronze_dir, "claim.csv"))
    policies = {row["policy_identifier"]: row for row in _read_csv(os.path.join(bronze_dir, "policy.csv"))}
    participants = _read_csv(os.path.join(bronze_dir, "claim_participant.csv"))
    participant_count = {}
    for row in participants:
        claim_identifier = row["claim_identifier"]
        participant_count[claim_identifier] = participant_count.get(claim_identifier, 0) + 1

    fact_rows = []
    for row in claims:
        policy = policies.get(row.get("policy_identifier", ""), {})
        fact_rows.append({
            "claim_identifier": row.get("claim_identifier", ""),
            "policy_identifier": row.get("policy_identifier", ""),
            "claim_type": row.get("claim_type", ""),
            "claim_status": row.get("claim_status", ""),
            "policy_status_code": policy.get("policy_status_code", ""),
            "claim_open_date": row.get("claim_open_date", ""),
            "claim_close_date": row.get("claim_close_date", ""),
            "claim_requested_amount": row.get("claim_requested_amount", ""),
            "total_incurred": row.get("total_incurred", ""),
            "total_payment_amount": row.get("total_payment_amount", ""),
            "participant_count": participant_count.get(row.get("claim_identifier", ""), 0),
            "litigation_flag": row.get("litigation_flag", ""),
            "proven_claim_fraud_flag": row.get("proven_claim_fraud_flag", ""),
        })

    summary = {}
    for row in fact_rows:
        key = row["claim_status"]
        bucket = summary.setdefault(key, {"claim_status": key, "claim_count": 0, "requested_amount": 0.0, "paid_amount": 0.0})
        bucket["claim_count"] += 1
        bucket["requested_amount"] += float(row["claim_requested_amount"] or 0)
        bucket["paid_amount"] += float(row["total_payment_amount"] or 0)

    write_csv(gold_dir, "fact_claim.csv", fact_rows)
    write_csv(gold_dir, "claim_status_summary.csv", list(summary.values()))
    return gold_dir


def build_claims_product(raw_dir, run_id, load_date=None):
    bronze_dir = build_claims_bronze(raw_dir, run_id)
    silver_dir = build_claims_silver(bronze_dir, run_id, load_date=load_date)
    gold_dir = build_claims_gold(bronze_dir, run_id)
    return {
        "raw": raw_dir,
        "bronze": bronze_dir,
        "silver": silver_dir,
        "gold": gold_dir,
    }
