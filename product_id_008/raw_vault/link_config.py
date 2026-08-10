# Databricks notebook source
# DBTITLE 1,Link Configs - Product ID 8
# Databricks notebook source
# DBTITLE 1,Link Configs - Product ID 3

link_loss_event_catastrophe_8 =  {
  "product_id": "8",
  "vault_object_id": "link_loss_event_catastrophe_8",
  "object_type": "link",
  "target_table": "link_loss_event_catastrophe",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.loss_event_identifier)) AS loss_event_hash_key,
    md5(CONCAT(b.origin_sys, b.catastrophe_identifier)) AS catastrophe_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_loss_event_register_csv a
  INNER JOIN crm_catastrophe_event_register_csv b
    ON a.loss_event_identifier = b.loss_event_identifier

  UNION ALL

  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.incident_id)) AS loss_event_hash_key,
    md5(CONCAT(b.origin_sys, b.catastrophe_identifier)) AS catastrophe_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM sap_loss_event_db a
  INNER JOIN crm_catastrophe_event_register_csv b
    ON a.incident_id = b.loss_event_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, loss_event_hash_key, catastrophe_hash_key )) AS loss_event_catastrophe_hash_key,
  loss_event_hash_key,
  catastrophe_hash_key,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_loss_event_register_csv,sap_loss_event_db,crm_catastrophe_event_register_csv"
}

link_policy_claim_8 =  {
  "product_id": "8",
  "vault_object_id": "link_policy_claim_8",
  "object_type": "link",
  "target_table": "link_policy_claim",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.claim_identifier)) AS claim_hash_key,
    md5(CONCAT(a.origin_sys, b.policy_identifier)) AS policy_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_claim_register_csv a
  INNER JOIN crm_policy_register_csv b
    ON a.policy_identifier = b.policy_identifier

  UNION ALL

  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.claim_ref_id)) AS claim_hash_key,
    md5(CONCAT(b.origin_sys, b.policy_identifier)) AS policy_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM sap_claim_db a
  INNER JOIN crm_policy_register_csv b
    ON a.policy_reference = b.policy_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, claim_hash_key, policy_hash_key)) AS policy_claim_hash_key,
  policy_hash_key,
  claim_hash_key,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_policy_register_csv,crm_claim_register_csv,sap_claim_db"
}

link_claim_claim_event_8 =  {
  "product_id": "8",
  "vault_object_id": "link_claim_claim_event_8",
  "object_type": "link",
  "target_table": "link_claim_claim_event",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.claim_identifier)) AS claim_hash_key,
    md5(CONCAT(b.origin_sys, b.claim_event_identifier)) AS claim_event_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_claim_register_csv a
  INNER JOIN crm_claim_event_log_csv b
    ON a.claim_identifier = b.claim_identifier

  UNION ALL

  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.claim_ref_id)) AS claim_hash_key,
    md5(CONCAT(b.origin_sys, b.claim_event_identifier)) AS claim_event_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM sap_claim_db a
  INNER JOIN crm_claim_event_log_csv b
    ON a.claim_ref_id = b.claim_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, claim_hash_key, claim_event_hash_key)) AS claim_claim_event_hash_key,
  claim_hash_key,
  claim_event_hash_key,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_claim_register_csv,sap_claim_db,crm_claim_event_log_csv"
}

link_claim_event_claim_investigation_8 =  {
  "product_id": "8",
  "vault_object_id": "link_claim_event_claim_investigation_8",
  "object_type": "link",
  "target_table": "link_claim_event_claim_investigation",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.claim_event_identifier )) AS claim_event_hash_key,
    md5(CONCAT(b.origin_sys, b.claim_investigation_identifier)) AS claim_investigation_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_claim_event_log_csv a
  INNER JOIN crm_claim_investigation_register_csv b
    ON a.claim_event_identifier = b.claim_event_identifier

  UNION ALL

  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.claim_event_identifier)) AS claim_event_hash_key,
    md5(CONCAT(b.origin_sys, b.case_event_id)) AS claim_investigation_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_claim_event_log_csv a
  INNER JOIN sap_claim_investigation_db b
    ON a.claim_event_identifier = b.case_event_id
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, claim_event_hash_key, claim_investigation_hash_key)) AS claim_event_claim_investigation_hash_key,
  claim_event_hash_key,
  claim_investigation_hash_key,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_claim_investigation_register_csv,sap_claim_investigation_db,crm_claim_event_log_csv"
}

link_person_claim_participant_8 =  {
  "product_id": "8",
  "vault_object_id": "link_person_claim_participant_8",
  "object_type": "link",
  "target_table": "link_person_claim_participant",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.person_identifier)) AS person_hash_key,
    md5(CONCAT(b.origin_sys, b.claim_participant_identifier)) AS claim_participant_hash_key,
    CAST(a.load_ts AS timestamp) AS load_ts,
    a.origin_sys AS origin_sys
  FROM crm_party_master_csv a
  INNER JOIN crm_claim_participant_register_csv b
    ON a.person_identifier = b.person_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, person_hash_key, claim_participant_hash_key)) AS person_claim_participant_hash_key,
  person_hash_key,
  claim_participant_hash_key,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_party_master_csv,crm_claim_participant_register_csv"
}

link_claim_claim_participant_8 =  {
  "product_id": "8",
  "vault_object_id": "link_claim_claim_participant_8",
  "object_type": "link",
  "target_table": "link_claim_claim_participant",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys,a.claim_identifier)) AS claim_hash_key,
    md5(CONCAT(b.origin_sys,b.claim_participant_identifier)) AS claim_participant_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_claim_register_csv a
  INNER JOIN crm_claim_participant_register_csv b
    ON a.claim_identifier = b.claim_identifier

  UNION ALL

  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.claim_ref_id )) AS claim_hash_key,
    md5(CONCAT(b.origin_sys, b.claim_participant_identifier)) AS claim_participant_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM sap_claim_db a
  INNER JOIN crm_claim_participant_register_csv b
    ON a.claim_ref_id = b.claim_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, claim_hash_key, claim_participant_hash_key)) AS claim_claim_participant_hash_key,
  claim_hash_key,
  claim_participant_hash_key,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_claim_register_csv,sap_claim_db,crm_claim_participant_register_csv"
}

link_medical_condition_diagnosis_8 =  {
  "product_id": "8",
  "vault_object_id": "link_medical_condition_diagnosis_8",
  "object_type": "link",
  "target_table": "link_medical_condition_diagnosis",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.medical_condition_identifier)) AS medical_condition_hash_key,
    md5(CONCAT(b.origin_sys, b.diagnosis_identifier)) AS diagnosis_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_medical_condition_catalog_csv a
  INNER JOIN crm_diagnosis_register_csv b
    ON a.medical_condition_identifier = b.medical_condition_identifier
)
SELECT DISTINCT
  md5(CONCAT(medical_condition_hash_key, diagnosis_hash_key)) AS medical_condition_diagnosis_hash_key,
  medical_condition_hash_key,
  diagnosis_hash_key,
   CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_medical_condition_catalog_csv,crm_diagnosis_register_csv"
}

link_injury_diagnosis_8 =  {
  "product_id": "8",
  "vault_object_id": "link_injury_diagnosis_8",
  "object_type": "link",
  "target_table": "link_injury_diagnosis",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.injury_identifier)) AS injury_hash_key,
    md5(CONCAT(b.origin_sys, b.diagnosis_identifier)) AS diagnosis_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_injury_register_csv a
  INNER JOIN crm_diagnosis_register_csv b
    ON a.injury_identifier = b.injury_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, injury_hash_key, diagnosis_hash_key)) AS injury_diagnosis_hash_key,
  injury_hash_key,
  diagnosis_hash_key,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_injury_register_csv,crm_diagnosis_register_csv"
}

link_claim_health_insurance_claim_8 =  {
  "product_id": "8",
  "vault_object_id": "link_claim_health_insurance_claim_8",
  "object_type": "link",
  "target_table": "link_claim_health_insurance_claim",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.claim_identifier, b.health_insurance_claim_identifier)) AS claim_health_insurance_claim_hash_key,
    md5(CONCAT(a.origin_sys, a.claim_identifier)) AS claim_hash_key,
    md5(CONCAT( b.origin_sys, b.health_insurance_claim_identifier)) AS health_insurance_claim_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_claim_register_csv a
  INNER JOIN crm_health_claim_register_csv b
    ON a.claim_identifier = b.claim_identifier

  UNION ALL

  SELECT DISTINCT
    md5(CONCAT(a.claim_ref_id, b.health_insurance_claim_identifier)) AS claim_health_insurance_claim_hash_key,
    md5(CONCAT(a.origin_sys, a.claim_ref_id)) AS claim_hash_key,
    md5(CONCAT(b.origin_sys, b.health_insurance_claim_identifier)) AS health_insurance_claim_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM sap_claim_db a
  INNER JOIN crm_health_claim_register_csv b
    ON a.claim_ref_id = b.claim_identifier
)
SELECT DISTINCT
  md5(CONCAT(claim_hash_key, health_insurance_claim_hash_key, origin_sys)) AS claim_health_insurance_claim_hash_key,
  claim_hash_key,
  health_insurance_claim_hash_key,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_claim_register_csv,sap_claim_db,crm_health_claim_register_csv"
}

link_claim_event_injury_8 =  {
  "product_id": "8",
  "vault_object_id": "link_claim_event_injury_8",
  "object_type": "link",
  "target_table": "link_claim_event_injury",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.claim_event_identifier)) AS claim_event_hash_key,
    md5(CONCAT(b.origin_sys, b.injury_identifier)) AS injury_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_claim_event_log_csv a
  INNER JOIN crm_injury_register_csv b
    ON a.claim_event_identifier = b.claim_event_identifier
)
SELECT DISTINCT
  md5(CONCAT(claim_event_hash_key, injury_hash_key)) AS claim_event_injury_hash_key,
  claim_event_hash_key,
  injury_hash_key,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_claim_event_log_csv,crm_injury_register_csv"
}

link_claim_litigation_8 =  {
  "product_id": "8",
  "vault_object_id": "link_claim_litigation_8",
  "object_type": "link",
  "target_table": "link_claim_litigation",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.claim_identifier)) AS claim_hash_key,
    md5(CONCAT(b.origin_sys, b.litigation_identifier)) AS litigation_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_claim_register_csv a
  INNER JOIN crm_litigation_register_csv b
    ON a.claim_identifier = b.claim_identifier

  UNION ALL

  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.claim_ref_id)) AS claim_hash_key,
    md5(CONCAT(b.origin_sys, b.litigation_identifier)) AS litigation_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM sap_claim_db a
  INNER JOIN crm_litigation_register_csv b
    ON a.claim_ref_id = b.claim_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, claim_hash_key, litigation_hash_key)) AS claim_litigation_hash_key,
  claim_hash_key,
  litigation_hash_key,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_claim_register_csv,sap_claim_db,crm_litigation_register_csv"
}

link_claim_event_litigation_8 =  {
  "product_id": "8",
  "vault_object_id": "link_claim_event_litigation_8",
  "object_type": "link",
  "target_table": "link_claim_event_litigation",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.claim_event_identifier)) AS claim_event_hash_key,
    md5(CONCAT(b.origin_sys, b.litigation_identifier)) AS litigation_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_claim_event_log_csv a
  INNER JOIN crm_litigation_register_csv b
    ON a.claim_event_identifier = b.claim_event_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, claim_event_hash_key, litigation_hash_key)) AS claim_event_litigation_hash_key,
  claim_event_hash_key,
  litigation_hash_key,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_claim_event_log_csv,crm_litigation_register_csv"
}

link_claim_loss_event_8 =  {
  "product_id": "8",
  "vault_object_id": "link_claim_loss_event_8",
  "object_type": "link",
  "target_table": "link_claim_loss_event",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.claim_identifier)) AS claim_hash_key,
    md5(CONCAT(b.origin_sys,b.loss_event_identifier)) AS loss_event_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_claim_register_csv a
  INNER JOIN crm_loss_event_register_csv b
    ON a.claim_identifier = b.claim_identifier

  UNION ALL

  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.claim_identifier)) AS claim_hash_key,
    md5(CONCAT(b.origin_sys, b.incident_id)) AS loss_event_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_claim_register_csv a
  INNER JOIN sap_loss_event_db b
    ON a.claim_identifier = b.claim_reference

  UNION ALL

  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.claim_ref_id)) AS claim_hash_key,
    md5(CONCAT(b.origin_sys, b.loss_event_identifier)) AS loss_event_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM sap_claim_db a
  INNER JOIN crm_loss_event_register_csv b
    ON a.claim_ref_id = b.claim_identifier

  UNION ALL

  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.claim_ref_id)) AS claim_hash_key,
    md5(CONCAT(b.origin_sys, b.incident_id)) AS loss_event_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM sap_claim_db a
  INNER JOIN sap_loss_event_db b
    ON a.claim_ref_id = b.claim_reference
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, claim_hash_key, loss_event_hash_key)) AS claim_loss_event_hash_key,
  claim_hash_key,
  loss_event_hash_key,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_claim_register_csv,sap_claim_db,crm_loss_event_register_csv,sap_loss_event_db"
}

link_physical_place_loss_event_8 =  {
  "product_id": "8",
  "vault_object_id": "link_physical_place_loss_event_8",
  "object_type": "link",
  "target_table": "link_physical_place_loss_event",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.loss_event_identifier)) AS loss_event_hash_key,
    md5(CONCAT(b.origin_sys, b.physical_place_identifier)) AS physical_place_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_loss_event_register_csv a
  INNER JOIN crm_physical_place_register_csv b
    ON a.physical_place_identifier = b.physical_place_identifier

  UNION ALL

  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.incident_id)) AS loss_event_hash_key,
    md5(CONCAT(b.origin_sys, b.physical_place_identifier)) AS physical_place_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM sap_loss_event_db a
  INNER JOIN crm_physical_place_register_csv b
    ON a.location_reference = b.physical_place_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, loss_event_hash_key, physical_place_hash_key)) AS physical_place_loss_event_hash_key,
  physical_place_hash_key,
  loss_event_hash_key,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_loss_event_register_csv,sap_loss_event_db,crm_physical_place_register_csv"
}

link_claim_event_medical_assessment_8 =  {
  "product_id": "8",
  "vault_object_id": "link_claim_event_medical_assessment_8",
  "object_type": "link",
  "target_table": "link_claim_event_medical_assessment",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys,a.claim_event_identifier)) AS claim_event_hash_key,
    md5(CONCAT(b.origin_sys,b.medical_assessment_identifier)) AS medical_assessment_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_claim_event_log_csv a
  INNER JOIN crm_medical_assessment_register_csv b
    ON a.claim_event_identifier = b.claim_event_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, claim_event_hash_key, medical_assessment_hash_key)) AS claim_event_medical_assessment_hash_key,
  claim_event_hash_key,
  medical_assessment_hash_key,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_claim_event_log_csv,crm_medical_assessment_register_csv"
}

link_claim_medical_report_8 =  {
  "product_id": "8",
  "vault_object_id": "link_claim_medical_report_8",
  "object_type": "link",
  "target_table": "link_claim_medical_report",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys,a.claim_identifier)) AS claim_hash_key,
    md5(CONCAT(b.origin_sys,b.medical_report_identifier)) AS medical_report_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_claim_register_csv a
  INNER JOIN crm_medical_report_register_csv b
    ON a.claim_identifier = b.claim_identifier

  UNION ALL

  SELECT DISTINCT
    md5(CONCAT(a.origin_sys,a.claim_ref_id)) AS claim_hash_key,
    md5(CONCAT(b.origin_sys,b.medical_report_identifier)) AS medical_report_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM sap_claim_db a
  INNER JOIN crm_medical_report_register_csv b
    ON a.claim_ref_id = b.claim_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, claim_hash_key, medical_report_hash_key)) AS claim_medical_report_hash_key,
  claim_hash_key,
  medical_report_hash_key,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_claim_register_csv,sap_claim_db,crm_medical_report_register_csv"
}

link_claim_police_report_8 =  {
  "product_id": "8",
  "vault_object_id": "link_claim_police_report_8",
  "object_type": "link",
  "target_table": "link_claim_police_report",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.claim_identifier)) AS claim_hash_key,
    md5(CONCAT(b.origin_sys, b.police_report_identifier)) AS police_report_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_claim_register_csv a
  INNER JOIN crm_police_report_register_csv b
    ON a.claim_identifier = b.claim_identifier

  UNION ALL

  SELECT DISTINCT
    md5(CONCAT(a.origin_sys, a.claim_ref_id)) AS claim_hash_key,
    md5(CONCAT(b.origin_sys, b.police_report_identifier)) AS police_report_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM sap_claim_db a
  INNER JOIN crm_police_report_register_csv b
    ON a.claim_ref_id = b.claim_identifier
)
SELECT DISTINCT
  md5(CONCAT(claim_hash_key, police_report_hash_key)) AS claim_police_report_hash_key,
  claim_hash_key,
  police_report_hash_key,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_claim_register_csv,sap_claim_db,crm_police_report_register_csv"
}

link_coverage_policy_coverage_8 =  {
  "product_id": "8",
  "vault_object_id": "link_coverage_policy_coverage_8",
  "object_type": "link",
  "target_table": "link_coverage_policy_coverage",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys,a.coverage_identifier)) AS coverage_hash_key,
    md5(CONCAT(b.origin_sys,b.policy_coverage_identifier)) AS policy_coverage_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_coverage_catalog_csv a
  INNER JOIN crm_policy_coverage_register_csv b
    ON a.coverage_identifier = b.coverage_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, coverage_hash_key, policy_coverage_hash_key)) AS coverage_policy_coverage_hash_key,
  coverage_hash_key,
  policy_coverage_hash_key,
    CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_coverage_catalog_csv,crm_policy_coverage_register_csv"
}

link_insured_entity_policy_coverage_8 =  {
  "product_id": "8",
  "vault_object_id": "link_insured_entity_policy_coverage_8",
  "object_type": "link",
  "target_table": "link_insured_entity_policy_coverage",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys,a.insured_entity_identifier)) AS insured_entity_hash_key,
    md5(CONCAT(b.origin_sys,b.policy_coverage_identifier)) AS policy_coverage_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_insured_entity_register_csv a
  INNER JOIN crm_policy_coverage_register_csv b
    ON a.insured_entity_identifier = b.insured_entity_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, insured_entity_hash_key, policy_coverage_hash_key)) AS insured_entity_policy_coverage_hash_key,
  insured_entity_hash_key,
  policy_coverage_hash_key,
    CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_insured_entity_register_csv,crm_policy_coverage_register_csv"
}

link_policy_policy_coverage_8 =  {
  "product_id": "8",
  "vault_object_id": "link_policy_policy_coverage_8",
  "object_type": "link",
  "target_table": "link_policy_policy_coverage",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys,a.policy_identifier)) AS policy_hash_key,
    md5(CONCAT(b.origin_sys,b.policy_coverage_identifier)) AS policy_coverage_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_policy_register_csv a
  INNER JOIN crm_policy_coverage_register_csv b
    ON a.policy_coverage_identifier = b.policy_coverage_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, policy_hash_key, policy_coverage_hash_key)) AS policy_policy_coverage_hash_key,
  policy_hash_key,
  policy_coverage_hash_key,
    CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_policy_register_csv,crm_policy_coverage_register_csv"
}

link_claim_event_repair_8 =  {
  "product_id": "8",
  "vault_object_id": "link_claim_event_repair_8",
  "object_type": "link",
  "target_table": "link_claim_event_repair",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys,a.claim_event_identifier)) AS claim_event_hash_key,
    md5(CONCAT(b.origin_sys,b.repair_identifier)) AS repair_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_claim_event_log_csv a
  INNER JOIN crm_repair_register_csv b
    ON a.claim_event_identifier = b.claim_event_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, claim_event_hash_key, repair_hash_key)) AS claim_event_repair_hash_key,
  claim_event_hash_key,
  repair_hash_key,
    CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_claim_event_log_csv,crm_repair_register_csv"
}

link_claim_participant_repair_8 =  {
  "product_id": "8",
  "vault_object_id": "link_claim_participant_repair_8",
  "object_type": "link",
  "target_table": "link_claim_participant_repair",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys,a.claim_participant_identifier)) AS claim_participant_hash_key,
    md5(CONCAT(b.origin_sys,b.repair_identifier)) AS repair_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_claim_participant_register_csv a
  INNER JOIN crm_repair_register_csv b
    ON a.claim_participant_identifier = b.claim_participant_identifier
)
SELECT DISTINCT
  md5(CONCAT(claim_participant_hash_key, repair_hash_key)) AS claim_participant_repair_hash_key,
  claim_participant_hash_key,
  repair_hash_key,
    CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_claim_participant_register_csv,crm_repair_register_csv"
}

link_claim_event_settlement_8 =  {
  "product_id": "8",
  "vault_object_id": "link_claim_event_settlement_8",
  "object_type": "link",
  "target_table": "link_claim_event_settlement",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.claim_event_identifier, a.origin_sys)) AS claim_event_hash_key,
    md5(CONCAT(b.settlement_identifier, b.origin_sys)) AS settlement_hash_key,
    a.load_ts,
    COALESCE(a.origin_sys, a.origin_sys) AS origin_sys
  FROM crm_claim_event_log_csv a
  INNER JOIN crm_settlement_register_csv b
    ON a.claim_event_identifier = b.claim_event_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, claim_event_hash_key, settlement_hash_key)) AS claim_event_settlement_hash_key,
  claim_event_hash_key,
  settlement_hash_key,
    CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_claim_event_log_csv,crm_settlement_register_csv"
}

link_claim_event_treatment_8 =  {
  "product_id": "8",
  "vault_object_id": "link_claim_event_treatment_8",
  "object_type": "link",
  "target_table": "link_claim_event_treatment",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys,a.claim_event_identifier)) AS claim_event_hash_key,
    md5(CONCAT(b.origin_sys,b.treatment_identifier)) AS treatment_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_claim_event_log_csv a
  INNER JOIN crm_treatment_register_csv b
    ON a.claim_event_identifier = b.claim_event_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, claim_event_hash_key, treatment_hash_key)) AS claim_event_treatment_hash_key,
  claim_event_hash_key,
  treatment_hash_key,
    CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_claim_event_log_csv,crm_treatment_register_csv"
}

link_medical_condition_treatment_8 =  {
  "product_id": "8",
  "vault_object_id": "link_medical_condition_treatment_8",
  "object_type": "link",
  "target_table": "link_medical_condition_treatment",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys,a.medical_condition_identifier)) AS medical_condition_hash_key,
    md5(CONCAT(b.origin_sys,b.treatment_identifier)) AS treatment_hash_key,
    a.load_ts,
    COALESCE(a.origin_sys, b.origin_sys) AS origin_sys
  FROM crm_medical_condition_catalog_csv a
  INNER JOIN crm_treatment_register_csv b
    ON a.medical_condition_identifier = b.medical_condition_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, medical_condition_hash_key, treatment_hash_key)) AS medical_condition_treatment_hash_key,
  medical_condition_hash_key,
  treatment_hash_key,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_medical_condition_catalog_csv,crm_treatment_register_csv"
}

link_claim_participant_treatment_8 =  {
  "product_id": "8",
  "vault_object_id": "link_claim_participant_treatment_8",
  "object_type": "link",
  "target_table": "link_claim_participant_treatment",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT DISTINCT
    md5(CONCAT(a.origin_sys,a.claim_participant_identifier)) AS claim_participant_hash_key,
    md5(CONCAT(b.origin_sys,b.treatment_identifier)) AS treatment_hash_key,
    a.load_ts,
    a.origin_sys AS origin_sys
  FROM crm_claim_participant_register_csv a
  INNER JOIN crm_treatment_register_csv b
    ON a.claim_participant_identifier = b.claim_participant_identifier
)
SELECT DISTINCT
  md5(CONCAT(origin_sys, claim_participant_hash_key, treatment_hash_key)) AS claim_participant_treatment_hash_key,
  claim_participant_hash_key,
  treatment_hash_key,
  CAST(load_ts AS timestamp) AS load_ts,
  origin_sys AS record_source
FROM combined
""",
  "source_table": "crm_claim_participant_register_csv,crm_treatment_register_csv"
}


# COMMAND ----------

# DBTITLE 1,Link Details - Product ID 8
link_details = {
    "process_order": "2",
    "status": {
        "link_loss_event_catastrophe_8": {"config": link_loss_event_catastrophe_8, "is_active": False},
        "link_policy_claim_8": {"config": link_policy_claim_8, "is_active": False},
        "link_claim_claim_event_8": {"config": link_claim_claim_event_8, "is_active": False},
        "link_claim_event_claim_investigation_8": {"config": link_claim_event_claim_investigation_8, "is_active": False},
        "link_person_claim_participant_8": {"config": link_person_claim_participant_8, "is_active": False},
        "link_claim_claim_participant_8": {"config": link_claim_claim_participant_8, "is_active": False},
        "link_medical_condition_diagnosis_8": {"config": link_medical_condition_diagnosis_8, "is_active": False},
        "link_injury_diagnosis_8": {"config": link_injury_diagnosis_8, "is_active": False},
        "link_claim_health_insurance_claim_8": {"config": link_claim_health_insurance_claim_8, "is_active": False},
        "link_claim_event_injury_8": {"config": link_claim_event_injury_8, "is_active": False},
        "link_claim_litigation_8": {"config": link_claim_litigation_8, "is_active": False},
        "link_claim_loss_event_8": {"config": link_claim_loss_event_8, "is_active": False},
        "link_claim_event_litigation_8": {"config": link_claim_event_litigation_8, "is_active": False},
        "link_physical_place_loss_event_8": {"config": link_physical_place_loss_event_8, "is_active": False},
        "link_claim_event_medical_assessment_8": {"config": link_claim_event_medical_assessment_8, "is_active": False},
        "link_claim_medical_report_8": {"config": link_claim_medical_report_8, "is_active": False},
        "link_claim_police_report_8": {"config": link_claim_police_report_8, "is_active": False},
        "link_coverage_policy_coverage_8": {"config": link_coverage_policy_coverage_8, "is_active": False},
        "link_insured_entity_policy_coverage_8": {"config": link_insured_entity_policy_coverage_8, "is_active": False},
        "link_claim_event_settlement_8": {"config": link_claim_event_settlement_8, "is_active": False},
        "link_policy_policy_coverage_8": {"config": link_policy_policy_coverage_8, "is_active": False},
        "link_claim_event_repair_8": {"config": link_claim_event_repair_8, "is_active": False},
        "link_claim_event_treatment_8": {"config": link_claim_event_treatment_8, "is_active": False},
        "link_medical_condition_treatment_8": {"config": link_medical_condition_treatment_8, "is_active": False},
        "link_claim_participant_treatment_8": {"config": link_claim_participant_treatment_8, "is_active": False},
        "link_claim_participant_repair_8": {"config": link_claim_participant_repair_8, "is_active": False}
    }
}


# COMMAND ----------

from collections import OrderedDict

active_configs = []
process_order = link_details["process_order"]
# Use OrderedDict to preserve order as in link_details["status"]
for k, v in OrderedDict(link_details["status"]).items():
    if v["is_active"]:
        config = v["config"].copy()
        config["process_order"] = str(process_order)
        active_configs.append(config)
df = spark.createDataFrame(active_configs)
display(df)


# COMMAND ----------

# MAGIC %skip
# MAGIC display(df)

# COMMAND ----------

# DBTITLE 1,Load into Tables
# MAGIC %skip
# MAGIC spark.sql("USE CATALOG dev_allianz_raw;")
# MAGIC spark.sql("USE SCHEMA bronze_claims;")
# MAGIC
# MAGIC for item in df.collect():
# MAGIC   target_table = item['target_table']
# MAGIC   build_sql = item['build_sql']
# MAGIC   print(f"Building Table {target_table}")
# MAGIC   build_sql = f"""
# MAGIC   INSERT INTO dev_allianz_raw.silver_claims.{target_table}
# MAGIC   {build_sql}
# MAGIC   """
# MAGIC   spark.sql(build_sql)
# MAGIC   print(f"Inserted into {target_table}")