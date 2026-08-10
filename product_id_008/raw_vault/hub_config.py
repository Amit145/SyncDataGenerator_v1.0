# Databricks notebook source
# DBTITLE 1,Hub Configs - Product ID 8
# Databricks notebook source
# DBTITLE 1,Hub Configs - Product ID 8

hub_catastrophe_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_catastrophe_8",
  "object_type": "hub",
  "target_table": "hub_catastrophe",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, catastrophe_identifier)) AS catastrophe_hash_key,
  catastrophe_identifier AS catastrophe_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_catastrophe_event_register_csv
""",
  "source_table": "crm_catastrophe_event_register_csv"
}

hub_claim_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_claim_8",
  "object_type": "hub",
  "target_table": "hub_claim",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, claim_identifier)) AS claim_hash_key,
  claim_identifier AS claim_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_claim_register_csv
UNION ALL
SELECT DISTINCT
  md5(CONCAT(origin_sys, claim_ref_id)) AS claim_hash_key,
  claim_ref_id AS claim_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM sap_claim_db
""",
  "source_table": "crm_claim_register_csv,sap_claim_db"
}

hub_claim_event_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_claim_event_8",
  "object_type": "hub",
  "target_table": "hub_claim_event",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, claim_event_identifier)) AS claim_event_hash_key,
  claim_event_identifier AS claim_event_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_claim_event_log_csv
""",
  "source_table": "crm_claim_event_log_csv"
}

hub_claim_investigation_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_claim_investigation_8",
  "object_type": "hub",
  "target_table": "hub_claim_investigation",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, claim_investigation_identifier)) AS claim_investigation_hash_key,
  claim_investigation_identifier AS claim_investigation_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_claim_investigation_register_csv

UNION ALL

SELECT DISTINCT
  md5(CONCAT(origin_sys, case_event_id)) AS claim_investigation_hash_key,
  case_event_id AS claim_investigation_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM sap_claim_investigation_db
""",
  "source_table": "crm_claim_investigation_register_csv,sap_claim_investigation_db"
}

hub_claim_participant_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_claim_participant_8",
  "object_type": "hub",
  "target_table": "hub_claim_participant",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, claim_participant_identifier)) AS claim_participant_hash_key,
  claim_participant_identifier AS claim_participant_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_claim_participant_register_csv
""",
  "source_table": "crm_claim_participant_register_csv"
}

hub_coverage_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_coverage_8",
  "object_type": "hub",
  "target_table": "hub_coverage",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, coverage_identifier)) AS coverage_hash_key,
  coverage_identifier AS coverage_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_coverage_catalog_csv
""",
  "source_table": "crm_coverage_catalog_csv"
}

hub_diagnosis_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_diagnosis_8",
  "object_type": "hub",
  "target_table": "hub_diagnosis",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys,diagnosis_identifier)) AS diagnosis_hash_key,
  diagnosis_identifier AS diagnosis_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_diagnosis_register_csv
""",
  "source_table": "crm_diagnosis_register_csv"
}

hub_health_insurance_claim_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_health_insurance_claim_8",
  "object_type": "hub",
  "target_table": "hub_health_insurance_claim",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, health_insurance_claim_identifier)) AS health_insurance_claim_hash_key,
  health_insurance_claim_identifier AS health_insurance_claim_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_health_claim_register_csv
""",
  "source_table": "crm_health_claim_register_csv"
}

hub_injury_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_injury_8",
  "object_type": "hub",
  "target_table": "hub_injury",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, injury_identifier)) AS injury_hash_key,
  injury_identifier AS injury_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_injury_register_csv
""",
  "source_table": "crm_injury_register_csv"
}

hub_insured_entity_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_insured_entity_8",
  "object_type": "hub",
  "target_table": "hub_insured_entity",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, insured_entity_identifier)) AS insured_entity_hash_key,
  insured_entity_identifier AS insured_entity_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_insured_entity_register_csv
""",
  "source_table": "crm_insured_entity_register_csv"
}

hub_litigation_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_litigation_8",
  "object_type": "hub",
  "target_table": "hub_litigation",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, litigation_identifier )) AS litigation_hash_key,
  litigation_identifier AS litigation_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_litigation_register_csv
""",
  "source_table": "crm_litigation_register_csv"
}

hub_loss_event_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_loss_event_8",
  "object_type": "hub",
  "target_table": "hub_loss_event",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, loss_event_identifier)) AS loss_event_hash_key,
  loss_event_identifier AS loss_event_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_loss_event_register_csv
UNION ALL
SELECT DISTINCT
  md5(CONCAT(origin_sys, incident_id)) AS loss_event_hash_key,
  incident_id AS loss_event_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM sap_loss_event_db
""",
  "source_table": "crm_loss_event_register_csv,sap_loss_event_db"
}

hub_medical_assessment_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_medical_assessment_8",
  "object_type": "hub",
  "target_table": "hub_medical_assessment",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, medical_assessment_identifier)) AS medical_assessment_hash_key,
  medical_assessment_identifier AS medical_assessment_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_medical_assessment_register_csv
""",
  "source_table": "crm_medical_assessment_register_csv"
}

hub_medical_condition_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_medical_condition_8",
  "object_type": "hub",
  "target_table": "hub_medical_condition",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, medical_condition_identifier)) AS medical_condition_hash_key,
  medical_condition_identifier AS medical_condition_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_medical_condition_catalog_csv
""",
  "source_table": "crm_medical_condition_catalog_csv"
}

hub_medical_report_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_medical_report_8",
  "object_type": "hub",
  "target_table": "hub_medical_report",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, medical_report_identifier)) AS medical_report_hash_key,
  medical_report_identifier AS medical_report_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_medical_report_register_csv
""",
  "source_table": "crm_medical_report_register_csv"
}

hub_person_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_person_8",
  "object_type": "hub",
  "target_table": "hub_person",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, person_identifier)) AS person_hash_key,
  person_identifier AS person_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_party_master_csv
""",
  "source_table": "crm_party_master_csv"
}

hub_physical_place_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_physical_place_8",
  "object_type": "hub",
  "target_table": "hub_physical_place",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, physical_place_identifier)) AS physical_place_hash_key,
  physical_place_identifier AS physical_place_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_physical_place_register_csv
""",
  "source_table": "crm_physical_place_register_csv"
}

hub_police_report_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_police_report_8",
  "object_type": "hub",
  "target_table": "hub_police_report",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, police_report_identifier)) AS police_report_hash_key,
  police_report_identifier AS police_report_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_police_report_register_csv
""",
  "source_table": "crm_police_report_register_csv"
}

hub_policy_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_policy_8",
  "object_type": "hub",
  "target_table": "hub_policy",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, policy_identifier)) AS policy_hash_key,
  policy_identifier AS policy_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_policy_register_csv
""",
  "source_table": "crm_policy_register_csv"
}

hub_policy_coverage_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_policy_coverage_8",
  "object_type": "hub",
  "target_table": "hub_policy_coverage",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, policy_coverage_identifier)) AS policy_coverage_hash_key,
  policy_coverage_identifier AS policy_coverage_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_policy_coverage_register_csv
""",
  "source_table": "crm_policy_coverage_register_csv"
}

hub_repair_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_repair_8",
  "object_type": "hub",
  "target_table": "hub_repair",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, repair_identifier)) AS repair_hash_key,
  repair_identifier AS repair_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_repair_register_csv
""",
  "source_table": "crm_repair_register_csv"
}

hub_settlement_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_settlement_8",
  "object_type": "hub",
  "target_table": "hub_settlement",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, settlement_identifier)) AS settlement_hash_key,
  settlement_identifier AS settlement_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_settlement_register_csv
""",
  "source_table": "crm_settlement_register_csv"
}

hub_treatment_8 =  {
  "product_id": "8",
  "vault_object_id": "hub_treatment_8",
  "object_type": "hub",
  "target_table": "hub_treatment",
  "process_order": "1",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(origin_sys, treatment_identifier)) AS treatment_hash_key,
  treatment_identifier AS treatment_id,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM crm_treatment_register_csv
""",
  "source_table": "crm_treatment_register_csv"
}


# COMMAND ----------

# DBTITLE 1,Hub Details - Product ID 8
hub_details = {
    "process_order": "1",
    "status": {
        "hub_catastrophe_8": {"config": hub_catastrophe_8, "is_active": False},
        "hub_claim_8": {"config": hub_claim_8, "is_active": False},
        "hub_claim_event_8": {"config": hub_claim_event_8, "is_active": False},
        "hub_claim_investigation_8": {"config": hub_claim_investigation_8, "is_active": False},
        "hub_claim_participant_8": {"config": hub_claim_participant_8, "is_active": False},
        "hub_coverage_8": {"config": hub_coverage_8, "is_active": False},
        "hub_diagnosis_8": {"config": hub_diagnosis_8, "is_active": False},
        "hub_health_insurance_claim_8": {"config": hub_health_insurance_claim_8, "is_active": False},
        "hub_injury_8": {"config": hub_injury_8, "is_active": False},
        "hub_insured_entity_8": {"config": hub_insured_entity_8, "is_active": False},
        "hub_litigation_8": {"config": hub_litigation_8, "is_active": False},
        "hub_loss_event_8": {"config": hub_loss_event_8, "is_active": False},
        "hub_medical_assessment_8": {"config": hub_medical_assessment_8, "is_active": False},
        "hub_medical_condition_8": {"config": hub_medical_condition_8, "is_active": False},
        "hub_medical_report_8": {"config": hub_medical_report_8, "is_active": False},
        "hub_person_8": {"config": hub_person_8, "is_active": False},
        "hub_physical_place_8": {"config": hub_physical_place_8, "is_active": False},
        "hub_police_report_8": {"config": hub_police_report_8, "is_active": False},
        "hub_policy_8": {"config": hub_policy_8, "is_active": False},
        "hub_policy_coverage_8": {"config": hub_policy_coverage_8, "is_active": False},
        "hub_repair_8": {"config": hub_repair_8, "is_active": False},
        "hub_settlement_8": {"config": hub_settlement_8, "is_active": False},
        "hub_treatment_8": {"config": hub_treatment_8, "is_active": False}
    }
}


# COMMAND ----------

from collections import OrderedDict

active_configs = []
process_order = hub_details["process_order"]
# Use OrderedDict to preserve order as in hub_details["status"]
for k, v in OrderedDict(hub_details["status"]).items():
    if v["is_active"]:
        config = v["config"].copy()
        config["process_order"] = str(process_order)
        active_configs.append(config)
df = spark.createDataFrame(active_configs)
display(df)


# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Load into Tables
# MAGIC %skip
# MAGIC spark.sql("USE CATALOG dev_allianz_raw;")
# MAGIC spark.sql("  USE SCHEMA bronze_claims;")
# MAGIC
# MAGIC for item in df.collect():
# MAGIC   target_table = item['target_table']
# MAGIC   build_sql = item['build_sql']
# MAGIC   build_sql = f"""
# MAGIC   INSERT INTO dev_allianz_raw.silver_claims.{target_table}
# MAGIC   {build_sql}
# MAGIC   """
# MAGIC   spark.sql(build_sql)
# MAGIC   print(f"Inserted into {target_table}")