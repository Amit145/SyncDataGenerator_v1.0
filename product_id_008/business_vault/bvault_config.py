# Databricks notebook source
# DBTITLE 1,Variables for Catalog and Schema Widget Inputs
dbutils.widgets.text("data_catalog", "allianz_coe", "Data Catalog Name")
dbutils.widgets.text("target_schema", "bvault_silver_raw", "Source & Target Silver Schema")

catalog_name = dbutils.widgets.get("data_catalog")
silver_bv_schema = dbutils.widgets.get("target_schema")
silver_rv_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

# DBTITLE 1,BV_CLAIM_MASTER_CFG
BV_CLAIM_MASTER_CFG = {
    "modelling_pattern": "business_vault",
    "name": "bv_claim_master",
    "target_table": f"{catalog_name}.{silver_bv_schema}.bv_claim_master",
    "business_key_col": "global_claim_id",
    "scd_type": "2",
    "watermark_col": "load_ts",
    "effective_from_col": "effective_from",
    "current_flag_col": "current_flag",
    "hash_col": "master_claim_hash_key",
    "object_id": "bv_claim_master_8",

    "stage_sql": f"""
        WITH crm AS (
    SELECT
        hc.claim_id AS crm_claim_id,
        hc.claim_hash_key AS crm_claim_hash_key,
        scr.claim_number,
        scr.claim_type,
        scr.third_party_claim_number,
        scr.claim_status,
        scr.claim_state,
        scr.claim_status_date,
        scr.claim_open_date,
        scr.claim_close_date,
        scr.claim_reopen_date,
        scr.claim_reopen_reason,
        scr.claims_made_date,
        scr.movement_date,
        scr.claim_duration,
        scr.claim_type_code,
        scr.claim_process_method,
        scr.claim_specific_flag,
        scr.bodily_injury_indicator,
        scr.applicable_deductible_flag,
        scr.total_loss_flag,
        scr.litigation_flag,
        scr.proven_claim_fraud_flag,
        scr.cross_border_claim_indicator,
        scr.intercompany_agreement_flag,
        scr.no_claims_discount,
        scr.claim_requested_amount,
        scr.outstanding_subrogation_amount,
        scr.recovery_actual,
        scr.recovery_type,
        scr.claims_rejection_reason,
        scr.finalization_reason,
        scr.indemnity_logic,
        scr.coverage_verification_result,
        scr.instruction_closure_date,
        scr.subrogation_paid_date,
        scr.claims_history_lob,
        scr.fire_brigade_fees_and_levies,
        scr.total_incurred,
        scr.total_payment_amount,
        scr.reimbursable_claim_amount,
        scr.claim_approval_date,
        scr.claim_payment_date,
        scr.settlement_amount_pc,
        scr.claim_amounts_description,
        scr.${{watermark_col}} AS crm_watermark_col
    FROM {catalog_name}.{silver_rv_schema}.hub_claim hc
    INNER JOIN {catalog_name}.{silver_rv_schema}.sat_claim_crm scr
        ON hc.claim_hash_key = scr.claim_hash_key
    WHERE hc.record_source = 'CLAIMS'
),

sap AS (
    SELECT
        hc.claim_id AS sap_claim_id,
        hc.claim_hash_key AS sap_claim_hash_key,
        scp.ext_claim_number,
        scp.loss_category,
        scp.claim_stage,
        scp.case_state,
        scp.stage_update_date,
        scp.first_notice_date,
        scp.claim_settlement_date,
        scp.risk_level,
        scp.injury_flag,
        scp.write_off_flag,
        scp.legal_case_flag,
        scp.confirmed_fraud_flag,
        scp.hospitalization_flag,
        scp.last_activity_date,
        scp.case_age_days,
        scp.category_code,
        scp.handling_method,
        scp.expected_recovery_amount,
        scp.recovered_amount,
        scp.recovery_category,
        scp.denial_reason,
        scp.closure_reason,
        scp.recovery_payment_date,
        scp.work_status_prior_incident,
        scp.return_to_work_status,
        scp.total_estimated_cost,
        scp.total_paid_amount,
        scp.estimated_loss_amount,
        scp.eligible_reimbursement_amount,
        scp.medical_reimbursement_amount,
        scp.proposed_settlement_amount,
        scp.approval_date,
        scp.payment_release_date,
        scp.${{watermark_col}} AS sap_watermark_col
    FROM {catalog_name}.{silver_rv_schema}.hub_claim hc
    INNER JOIN {catalog_name}.{silver_rv_schema}.sat_claim_sap scp
        ON hc.claim_hash_key = scp.claim_hash_key
    WHERE hc.record_source = 'SAP'
),

policy_coverage AS (
    SELECT
        hc.claim_id AS pc_claim_id,
        hpc.policy_coverage_id,
        hp.policy_id,
        hi.insured_entity_id,
        hcov.coverage_id
    FROM {catalog_name}.{silver_rv_schema}.hub_claim hc
    INNER JOIN {catalog_name}.{silver_rv_schema}.link_policy_claim lpc
        ON hc.claim_hash_key = lpc.claim_hash_key
    INNER JOIN {catalog_name}.{silver_rv_schema}.hub_policy hp
        ON lpc.policy_hash_key = hp.policy_hash_key
    INNER JOIN {catalog_name}.{silver_rv_schema}.link_policy_policy_coverage lppc
        ON hp.policy_hash_key = lppc.policy_hash_key
    INNER JOIN {catalog_name}.{silver_rv_schema}.hub_policy_coverage hpc
        ON lppc.policy_coverage_hash_key = hpc.policy_coverage_hash_key
    LEFT JOIN {catalog_name}.{silver_rv_schema}.link_insured_entity_policy_coverage iepc
        ON hpc.policy_coverage_hash_key = iepc.policy_coverage_hash_key
    LEFT JOIN {catalog_name}.{silver_rv_schema}.hub_insured_entity hi
        ON iepc.insured_entity_hash_key = hi.insured_entity_hash_key
    LEFT JOIN {catalog_name}.{silver_rv_schema}.link_coverage_policy_coverage lcp
        ON hpc.policy_coverage_hash_key = lcp.policy_coverage_hash_key
    LEFT JOIN {catalog_name}.{silver_rv_schema}.hub_coverage hcov
        ON lcp.coverage_hash_key = hcov.coverage_hash_key
),

src AS (
    SELECT
        CONCAT(crm.crm_claim_id, '||', sap.sap_claim_id) AS global_claim_id,
        CONCAT(crm.crm_claim_hash_key, '||', sap.sap_claim_hash_key) AS master_claim_hash_key,

        COALESCE(crm.claim_type, sap.loss_category) AS claim_type,
        COALESCE(crm.claim_number, sap.ext_claim_number) AS claim_number,
        crm.third_party_claim_number,

        COALESCE(crm.claim_status, sap.claim_stage) AS claim_status,
        COALESCE(crm.claim_state, sap.case_state) AS claim_state,
        COALESCE(crm.claim_status_date, sap.stage_update_date) AS claim_status_date,
        COALESCE(crm.claim_open_date, sap.first_notice_date) AS claim_open_date,
        COALESCE(crm.claim_close_date, sap.claim_settlement_date) AS claim_close_date,

        crm.claim_reopen_date,
        crm.claim_reopen_reason,
        crm.claims_made_date,

        COALESCE(crm.movement_date, sap.last_activity_date) AS movement_date,
        COALESCE(crm.claim_duration, sap.case_age_days) AS claim_duration,
        COALESCE(crm.claim_type_code, sap.category_code) AS claim_type_code,
        COALESCE(crm.claim_process_method, sap.handling_method) AS claim_process_method,

        sap.risk_level AS claim_sensitivity,

        crm.claim_specific_flag,
        COALESCE(crm.bodily_injury_indicator, sap.injury_flag) AS bodily_injury_indicator,
        crm.applicable_deductible_flag,
        COALESCE(crm.total_loss_flag, sap.write_off_flag) AS total_loss_flag,
        COALESCE(crm.litigation_flag, sap.legal_case_flag) AS litigation_flag,
        COALESCE(crm.proven_claim_fraud_flag, sap.confirmed_fraud_flag) AS proven_claim_fraud_flag,

        sap.hospitalization_flag,

        crm.cross_border_claim_indicator,
        crm.intercompany_agreement_flag,
        crm.no_claims_discount,

        COALESCE(crm.claim_requested_amount, sap.estimated_loss_amount) AS claim_requested_amount,
        COALESCE(crm.outstanding_subrogation_amount, sap.expected_recovery_amount) AS outstanding_subrogation_amount,
        COALESCE(crm.recovery_actual, sap.recovered_amount) AS recovery_actual,
        COALESCE(crm.recovery_type, sap.recovery_category) AS recovery_type,

        COALESCE(crm.claims_rejection_reason, sap.denial_reason) AS claims_rejection_reason,
        COALESCE(crm.finalization_reason, sap.closure_reason) AS finalization_reason,

        crm.indemnity_logic,
        crm.coverage_verification_result,
        crm.instruction_closure_date,

        COALESCE(crm.subrogation_paid_date, sap.recovery_payment_date) AS subrogation_paid_date,

        crm.claims_history_lob,
        sap.work_status_prior_incident,
        sap.return_to_work_status,

        crm.fire_brigade_fees_and_levies,

        COALESCE(crm.total_incurred, sap.total_estimated_cost) AS total_incurred,
        COALESCE(crm.total_payment_amount, sap.total_paid_amount) AS total_payment_amount,
        COALESCE(crm.reimbursable_claim_amount, sap.eligible_reimbursement_amount) AS reimbursable_claim_amount,

        sap.medical_reimbursement_amount,

        COALESCE(crm.settlement_amount_pc, sap.proposed_settlement_amount) AS settlement_amount_pc,

        COALESCE(crm.claim_approval_date, sap.approval_date) AS claim_approval_date,
        COALESCE(crm.claim_payment_date, sap.payment_release_date) AS claim_payment_date,

        crm.claim_amounts_description,

        pcs.policy_coverage_id,
        pcs.policy_id,
        pcs.insured_entity_id,
        pcs.coverage_id,

        GREATEST(
            COALESCE(crm.crm_watermark_col, DATE('1900-01-01')),
            COALESCE(sap.sap_watermark_col, DATE('1900-01-01'))
        ) AS effective_from,

        GREATEST(
            COALESCE(crm.crm_watermark_col, DATE('1900-01-01')),
            COALESCE(sap.sap_watermark_col, DATE('1900-01-01'))
        ) AS watermark_col

    FROM crm
    INNER JOIN sap
        ON crm.claim_number = sap.ext_claim_number
    INNER JOIN policy_coverage pcs
        ON crm.crm_claim_id = pcs.pc_claim_id

    WHERE GREATEST(
        COALESCE(crm.crm_watermark_col, DATE('1900-01-01')),
        COALESCE(sap.sap_watermark_col, DATE('1900-01-01'))
    ) > ${{watermark_col}}
),

dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY global_claim_id
            ORDER BY watermark_col DESC
        ) AS rn
    FROM src
)

SELECT *
FROM dedup
WHERE rn = 1;
    """,

    "attribute_cols": [
        "global_claim_id",
        "master_claim_hash_key",
        "claim_type",
        "claim_number",
        "third_party_claim_number",
        "claim_status",
        "claim_state",
        "claim_status_date",
        "claim_open_date",
        "claim_close_date",
        "claim_reopen_date",
        "claim_reopen_reason",
        "claims_made_date",
        "movement_date",
        "claim_duration",
        "claim_type_code",
        "claim_process_method",
        "claim_sensitivity",
        "claim_specific_flag",
        "bodily_injury_indicator",
        "applicable_deductible_flag",
        "total_loss_flag",
        "litigation_flag",
        "proven_claim_fraud_flag",
        "hospitalization_flag",
        "cross_border_claim_indicator",
        "intercompany_agreement_flag",
        "no_claims_discount",
        "claim_requested_amount",
        "outstanding_subrogation_amount",
        "recovery_actual",
        "recovery_type",
        "claims_rejection_reason",
        "finalization_reason",
        "indemnity_logic",
        "coverage_verification_result",
        "instruction_closure_date",
        "subrogation_paid_date",
        "claims_history_lob",
        "work_status_prior_incident",
        "return_to_work_status",
        "fire_brigade_fees_and_levies",
        "total_incurred",
        "total_payment_amount",
        "reimbursable_claim_amount",
        "medical_reimbursement_amount",
        "settlement_amount_pc",
        "claim_approval_date",
        "claim_payment_date",
        "claim_amounts_description",
        "policy_coverage_id",
        "policy_id",
        "insured_entity_id",
        "coverage_id",
    ],

    "insert_cols": [
        "global_claim_id",
        "master_claim_hash_key",
        "claim_type",
        "claim_number",
        "third_party_claim_number",
        "claim_status",
        "claim_state",
        "claim_status_date",
        "claim_open_date",
        "claim_close_date",
        "claim_reopen_date",
        "claim_reopen_reason",
        "claims_made_date",
        "movement_date",
        "claim_duration",
        "claim_type_code",
        "claim_process_method",
        "claim_sensitivity",
        "claim_specific_flag",
        "bodily_injury_indicator",
        "applicable_deductible_flag",
        "total_loss_flag",
        "litigation_flag",
        "proven_claim_fraud_flag",
        "hospitalization_flag",
        "cross_border_claim_indicator",
        "intercompany_agreement_flag",
        "no_claims_discount",
        "claim_requested_amount",
        "outstanding_subrogation_amount",
        "recovery_actual",
        "recovery_type",
        "claims_rejection_reason",
        "finalization_reason",
        "indemnity_logic",
        "coverage_verification_result",
        "instruction_closure_date",
        "subrogation_paid_date",
        "claims_history_lob",
        "work_status_prior_incident",
        "return_to_work_status",
        "fire_brigade_fees_and_levies",
        "total_incurred",
        "total_payment_amount",
        "reimbursable_claim_amount",
        "medical_reimbursement_amount",
        "settlement_amount_pc",
        "claim_approval_date",
        "claim_payment_date",
        "claim_amounts_description",
        "policy_coverage_id",
        "policy_id",
        "insured_entity_id",
        "coverage_id",
        "effective_from",
        "effective_to",
        "current_flag",
    ]
}

# COMMAND ----------

# DBTITLE 1,BV_CLAIM_INVESTIGATION_MASTER_CFG
BV_CLAIM_INVESTIGATION_MASTER_CFG = {
    "modelling_pattern": "business_vault",
    "name": "bv_claim_investigation_master",
    "target_table": f"{catalog_name}.{silver_bv_schema}.bv_claim_investigation_master",
    "business_key_col": "global_claim_investigation_id",
    "scd_type": "2",
    "watermark_col": "load_ts",
    "effective_from_col": "effective_from",
    "current_flag_col": "current_flag",
    "hash_col": "master_claim_investigation_hash_key",
    "object_id": "bv_claim_investigation_master_8",

    "stage_sql": f"""
        WITH crm AS (
          SELECT
            hc.claim_investigation_id AS crm_claim_investigation_id,
            hc.claim_investigation_hash_key AS crm_claim_investigation_hash_key,
            sci.claim_handler_notes,
            sci.claim_investigation_start_date,
            sci.claim_investigation_end_date,
            sci.fraud_indicator,
            sci.investigator_flag,
            sci.claim_fraud_assessment,
            sci.claim_handler_identifier,
            sci.${{watermark_col}} AS crm_watermark_col
          FROM {catalog_name}.{silver_rv_schema}.hub_claim_investigation hc 
          INNER JOIN {catalog_name}.{silver_rv_schema}.sat_claim_investigation_crm sci   
          ON hc.claim_investigation_hash_key = sci.claim_investigation_hash_key
          WHERE hc.record_source = 'CLAIMS'
        ),
        sap AS (
          SELECT
            hc.claim_investigation_id AS sap_claim_investigation_id,
            hc.claim_investigation_hash_key AS sap_claim_investigation_hash_key,
            sis.investigation_notes,
            sis.case_open_date,
            sis.case_close_date,
            sis.fraud_confirmed_flag,
            sis.claim_review_flag,
            sis.fraud_risk_assessment,
            sis.claim_investigator_id,
            sis.${{watermark_col}} AS sap_watermark_col
          FROM {catalog_name}.{silver_rv_schema}.hub_claim_investigation hc 
          INNER JOIN {catalog_name}.{silver_rv_schema}.sat_claim_investigation_sap sis
          ON hc.claim_investigation_hash_key = sis.claim_investigation_hash_key
           WHERE hc.record_source = 'SAP'
        ),
        src AS (
          SELECT
            concat(crm.crm_claim_investigation_id, '||', sap.sap_claim_investigation_id) AS global_claim_investigation_id,
            concat(crm.crm_claim_investigation_hash_key, '||', sap.sap_claim_investigation_hash_key) AS master_claim_investigation_hash_key,
            CASE 
              WHEN crm.claim_handler_notes IS NOT NULL THEN crm.claim_handler_notes 
              ELSE sap.investigation_notes 
            END AS investigation_notes,
            CASE 
              WHEN crm.claim_investigation_start_date IS NOT NULL THEN crm.claim_investigation_start_date 
              ELSE sap.case_open_date 
            END AS investigation_start_date,
            CASE 
              WHEN crm.claim_investigation_end_date IS NOT NULL THEN crm.claim_investigation_end_date 
              ELSE sap.case_close_date 
            END AS investigation_end_date,
            CASE 
              WHEN crm.fraud_indicator IS NOT NULL THEN crm.fraud_indicator 
              ELSE sap.fraud_confirmed_flag 
            END AS fraud_flag,
            CASE 
              WHEN crm.investigator_flag IS NOT NULL THEN crm.investigator_flag 
              ELSE sap.claim_review_flag 
            END AS investigator_flag,
            CASE 
              WHEN crm.claim_fraud_assessment IS NOT NULL THEN crm.claim_fraud_assessment 
              ELSE sap.fraud_risk_assessment 
            END AS fraud_assessment,
            hce.claim_event_id AS claim_event_id,
            CASE 
              WHEN crm.claim_handler_identifier IS NOT NULL THEN crm.claim_handler_identifier 
              ELSE sap.claim_investigator_id 
            END AS claim_handler_id,
            greatest(
              COALESCE(crm.crm_watermark_col, DATE('1900-01-01')),
              COALESCE(sap.sap_watermark_col, DATE('1900-01-01'))
            ) AS effective_from,
            greatest(
              COALESCE(crm.crm_watermark_col, DATE('1900-01-01')),
              COALESCE(sap.sap_watermark_col, DATE('1900-01-01'))
            ) AS watermark_col
          FROM crm
          INNER JOIN sap 
            on crm.claim_handler_identifier = sap.claim_investigator_id
          INNER JOIN {catalog_name}.{silver_rv_schema}.link_claim_event_claim_investigation lcec
            ON crm.crm_claim_investigation_hash_key = lcec.claim_investigation_hash_key
          INNER JOIN {catalog_name}.{silver_rv_schema}.hub_claim_event hce
            ON lcec.claim_event_hash_key = hce.claim_event_hash_key
          WHERE
			      crm.claim_investigation_start_date = sap.case_open_date 
            AND crm.claim_investigation_end_date = sap.case_close_date
            AND greatest(
              COALESCE(crm.crm_watermark_col, DATE('1900-01-01')),
              COALESCE(sap.sap_watermark_col, DATE('1900-01-01'))
            ) > ${{watermark_col}}
          
        ),
        dedup AS (
          SELECT
            global_claim_investigation_id,
            master_claim_investigation_hash_key,
            investigation_notes,
            investigation_start_date,
            investigation_end_date,
            fraud_flag,
            investigator_flag,
            fraud_assessment,
            claim_event_id,
            claim_handler_id,
            effective_from,
            watermark_col,
            ROW_NUMBER() OVER (
              PARTITION BY global_claim_investigation_id
              ORDER BY watermark_col DESC
            ) AS rn
          FROM src)

        SELECT
          global_claim_investigation_id,
          master_claim_investigation_hash_key,
          investigation_notes,
          investigation_start_date,
          investigation_end_date,
          fraud_flag,
          investigator_flag,
          fraud_assessment,
          claim_event_id,
          claim_handler_id,
          watermark_col AS effective_from,
          TIMESTAMP '9999-12-31 00:00:00' AS effective_to
        FROM dedup
        WHERE rn = 1
    """,

    "attribute_cols": [
        "global_claim_investigation_id",
        "master_claim_investigation_hash_key",
        "investigation_notes",
        "investigation_start_date",
        "investigation_end_date",
        "fraud_flag",
        "investigator_flag",
        "fraud_assessment",
        "claim_event_id",
        "claim_handler_id",
    ],

    "insert_cols": [
        "global_claim_investigation_id",
        "master_claim_investigation_hash_key",
        "investigation_notes",
        "investigation_start_date",
        "investigation_end_date",
        "fraud_flag",
        "investigator_flag",
        "fraud_assessment",
        "claim_event_id",
        "claim_handler_id",
        "effective_from",
        "effective_to",
        "current_flag",
    ]
}

# COMMAND ----------

# DBTITLE 1,BV_LOSS_EVENT_MASTER_CFG
BV_LOSS_EVENT_MASTER_CFG = {
    "modelling_pattern": "business_vault",
    "name": "bv_loss_event_master",
    "target_table": f"{catalog_name}.{silver_bv_schema}.bv_loss_event_master",
    "business_key_col": "global_loss_event_id",
    "scd_type": "2",
    "watermark_col": "load_ts",
    "effective_from_col": "effective_from",
    "current_flag_col": "current_flag",
    "hash_col": "master_loss_event_hash_key",
    "object_id": "bv_loss_event_master_8",

    "stage_sql": f"""
        WITH crm_src AS (
          SELECT
            hc.record_source AS crm_record_source,
            hc.loss_event_hash_key AS crm_loss_event_hash_key,
            hc.loss_event_id AS crm_loss_event_id,
            hce.claim_id AS claim_id,
            sc.loss_event_name,
            sc.loss_event_description,
            sc.loss_date,
            sc.loss_time,
            sc.loss_event_start_date,
            sc.loss_event_end_date,
            sc.loss_event_status,
            sc.loss_type,
            sc.loss_category,
            sc.loss_cause,
            sc.property_liability_loss_cause,
            sc.notification_date,
            sc.notification_channel,
            sc.fatality_flag,
            sc.minimal_impact_flag,
            sc.number_of_injured_parties,
            sc.number_of_people_involved,
            sc.number_of_vehicles_involved,
            sc.drugs_alcohol_indicator,
            sc.contributory_negligence_flag,
            sc.damage_specification,
            lpc.physical_place_id,
            sc.effective_loss_date,
            sc.${{watermark_col}}
          FROM {catalog_name}.{silver_rv_schema}.hub_loss_event hc
          INNER JOIN {catalog_name}.{silver_rv_schema}.link_claim_loss_event lcl
            ON hc.loss_event_hash_key = lcl.loss_event_hash_key
          INNER JOIN {catalog_name}.{silver_rv_schema}.hub_claim hce
            ON lcl.claim_hash_key = hce.claim_hash_key
          INNER JOIN {catalog_name}.{silver_rv_schema}.sat_loss_event_crm sc
            ON sc.loss_event_hash_key = hc.loss_event_hash_key
          INNER JOIN {catalog_name}.{silver_rv_schema}.link_physical_place_loss_event lcec
            ON hc.loss_event_hash_key = lcec.loss_event_hash_key
          INNER JOIN {catalog_name}.{silver_rv_schema}.hub_physical_place lpc
            ON lcec.physical_place_hash_key = lpc.physical_place_hash_key
        ),
        sap_src AS (
          SELECT
            hc.record_source AS sap_record_source,
            hc.loss_event_hash_key AS sap_loss_event_hash_key,
            hc.loss_event_id AS sap_loss_event_id,
            hce.claim_id AS claim_id,
            ss.incident_name,
            ss.incident_description,
            ss.event_date,
            ss.event_time,
            ss.incident_start_date,
            ss.incident_end_date,
            ss.incident_status,
            ss.event_type,
            ss.incident_category,
            ss.root_cause,
            ss.liability_cause,
            ss.reported_date,
            ss.reporting_channel,
            ss.days_to_fnol,
            ss.casualty_indicator,
            ss.catastrophe_flag,
            ss.natcat_flag,
            ss.event_severity_low_flag,
            ss.casualty_count,
            ss.impacted_person_count,
            ss.impacted_vehicle_count,
            ss.impairment_flag,
            ss.shared_liability_flag,
            ss.impact_description,
            lpc.physical_place_id,
            ss.${{watermark_col}}
          FROM {catalog_name}.{silver_rv_schema}.hub_loss_event hc
          INNER JOIN {catalog_name}.{silver_rv_schema}.link_claim_loss_event lcl
            ON hc.loss_event_hash_key = lcl.loss_event_hash_key
          INNER JOIN {catalog_name}.{silver_rv_schema}.hub_claim hce
            ON lcl.claim_hash_key = hce.claim_hash_key
          INNER JOIN {catalog_name}.{silver_rv_schema}.sat_loss_event_sap ss
            ON ss.loss_event_hash_key = hc.loss_event_hash_key
          INNER JOIN {catalog_name}.{silver_rv_schema}.link_physical_place_loss_event lcec
            ON hc.loss_event_hash_key = lcec.loss_event_hash_key
          INNER JOIN {catalog_name}.{silver_rv_schema}.hub_physical_place lpc
            ON lcec.physical_place_hash_key = lpc.physical_place_hash_key
        ),
        src AS (
          SELECT
            concat(crm.crm_loss_event_id, '||', sap.sap_loss_event_id) AS global_loss_event_id,
            concat(crm.crm_loss_event_hash_key, '||', sap.sap_loss_event_hash_key) AS master_loss_event_hash_key,
            CASE WHEN crm.loss_event_name IS NOT NULL THEN crm.loss_event_name ELSE sap.incident_name END AS loss_event_name,
            CASE WHEN crm.loss_event_description IS NOT NULL THEN crm.loss_event_description ELSE sap.incident_description END AS loss_event_description,
            CASE WHEN crm.loss_date IS NOT NULL THEN crm.loss_date ELSE sap.event_date END AS loss_date,
            CASE WHEN crm.loss_time IS NOT NULL THEN crm.loss_time ELSE sap.event_time END AS loss_time,
            CASE WHEN crm.loss_event_start_date IS NOT NULL THEN crm.loss_event_start_date ELSE sap.incident_start_date END AS loss_event_start_date,
            CASE WHEN crm.loss_event_end_date IS NOT NULL THEN crm.loss_event_end_date ELSE sap.incident_end_date END AS loss_event_end_date,
            CASE WHEN crm.loss_event_status IS NOT NULL THEN crm.loss_event_status ELSE sap.incident_status END AS loss_event_status,
            CASE WHEN crm.loss_type IS NOT NULL THEN crm.loss_type ELSE sap.event_type END AS loss_type,
            CASE WHEN crm.loss_category IS NOT NULL THEN crm.loss_category ELSE sap.incident_category END AS loss_category,
            CASE WHEN crm.loss_cause IS NOT NULL THEN crm.loss_cause ELSE sap.root_cause END AS loss_cause,
            CASE WHEN crm.property_liability_loss_cause IS NOT NULL THEN crm.property_liability_loss_cause ELSE sap.liability_cause END AS property_liability_loss_cause,
            crm.effective_loss_date AS effective_loss_date,
            CASE WHEN crm.notification_date IS NOT NULL THEN crm.notification_date ELSE sap.reported_date END AS notification_date,
            CASE WHEN crm.notification_channel IS NOT NULL THEN crm.notification_channel ELSE sap.reporting_channel END AS notification_channel,
            sap.days_to_fnol AS days_to_fnol,
            CASE WHEN crm.fatality_flag IS NOT NULL THEN crm.fatality_flag ELSE sap.casualty_indicator END AS casualty_indicator,
            sap.catastrophe_flag AS catastrophe_flag,
            sap.natcat_flag AS natcat_flag,
            CASE WHEN crm.minimal_impact_flag IS NOT NULL THEN crm.minimal_impact_flag ELSE sap.event_severity_low_flag END AS minimal_impact_flag,
            CASE WHEN crm.number_of_injured_parties IS NOT NULL THEN crm.number_of_injured_parties ELSE sap.casualty_count END AS number_of_injured_parties,
            CASE WHEN crm.number_of_people_involved IS NOT NULL THEN crm.number_of_people_involved ELSE sap.impacted_person_count END AS number_of_people_involved,
            CASE WHEN crm.number_of_vehicles_involved IS NOT NULL THEN crm.number_of_vehicles_involved ELSE sap.impacted_vehicle_count END AS number_of_vehicles_involved,
            CASE WHEN crm.drugs_alcohol_indicator IS NOT NULL THEN crm.drugs_alcohol_indicator ELSE sap.impairment_flag END AS drugs_alcohol_indicator,
            CASE WHEN crm.contributory_negligence_flag IS NOT NULL THEN crm.contributory_negligence_flag ELSE sap.shared_liability_flag END AS contributory_negligence_flag,
            CASE WHEN crm.damage_specification IS NOT NULL THEN crm.damage_specification ELSE sap.impact_description END AS damage_specification,
            CASE WHEN crm.physical_place_id IS NOT NULL THEN crm.physical_place_id ELSE sap.physical_place_id END AS physical_place_id,
            crm.claim_id AS claim_id,
            greatest(
              COALESCE(crm.${{watermark_col}}, DATE('1900-01-01')),
              COALESCE(sap.${{watermark_col}}, DATE('1900-01-01'))
            ) AS effective_from,
            greatest(
              COALESCE(crm.${{watermark_col}}, DATE('1900-01-01')),
              COALESCE(sap.${{watermark_col}}, DATE('1900-01-01'))
            ) AS watermark_col
          FROM crm_src crm
          INNER JOIN sap_src sap
            ON concat(crm.claim_id, crm.loss_date, crm.loss_type) = concat(sap.claim_id, sap.event_date, sap.event_type)
            --ON crm.crm_loss_event_id = sap.sap_loss_event_id
          WHERE greatest(
                  COALESCE(crm.${{watermark_col}}, DATE('1900-01-01')),
                  COALESCE(sap.${{watermark_col}}, DATE('1900-01-01'))
                ) > ${{watermark_col}}
        ),
        dedup AS (
          SELECT
            global_loss_event_id,
            master_loss_event_hash_key,
            loss_event_name,
            loss_event_description,
            loss_date,
            loss_time,
            loss_event_start_date,
            loss_event_end_date,
            loss_event_status,
            loss_type,
            loss_category,
            loss_cause,
            property_liability_loss_cause,
            effective_loss_date,
            notification_date,
            notification_channel,
            days_to_fnol,
            casualty_indicator,
            catastrophe_flag,
            natcat_flag,
            minimal_impact_flag,
            number_of_injured_parties,
            number_of_people_involved,
            number_of_vehicles_involved,
            drugs_alcohol_indicator,
            contributory_negligence_flag,
            damage_specification,
            physical_place_id,
            claim_id,
            effective_from,
            watermark_col,
            ROW_NUMBER() OVER (
              PARTITION BY global_loss_event_id
              ORDER BY watermark_col DESC
            ) AS rn
          FROM src
        )
        SELECT
          global_loss_event_id,
          master_loss_event_hash_key,
          loss_event_name,
          loss_event_description,
          loss_date,
          loss_time,
          loss_event_start_date,
          loss_event_end_date,
          loss_event_status,
          loss_type,
          loss_category,
          loss_cause,
          property_liability_loss_cause,
          effective_loss_date,
          notification_date,
          notification_channel,
          days_to_fnol,
          casualty_indicator,
          catastrophe_flag,
          natcat_flag,
          minimal_impact_flag,
          number_of_injured_parties,
          number_of_people_involved,
          number_of_vehicles_involved,
          drugs_alcohol_indicator,
          contributory_negligence_flag,
          damage_specification,
          physical_place_id,
          claim_id,
          watermark_col AS effective_from,
          TIMESTAMP '9999-12-31 00:00:00' AS effective_to
        FROM dedup
        WHERE rn = 1
    """,

    "attribute_cols": [
        "global_loss_event_id",
        "master_loss_event_hash_key",
        "loss_event_name",
        "loss_event_description",
        "loss_date",
        "loss_time",
        "loss_event_start_date",
        "loss_event_end_date",
        "loss_event_status",
        "loss_type",
        "loss_category",
        "loss_cause",
        "property_liability_loss_cause",
        "effective_loss_date",
        "notification_date",
        "notification_channel",
        "days_to_fnol",
        "casualty_indicator",
        "catastrophe_flag",
        "natcat_flag",
        "minimal_impact_flag",
        "number_of_injured_parties",
        "number_of_people_involved",
        "number_of_vehicles_involved",
        "drugs_alcohol_indicator",
        "contributory_negligence_flag",
        "damage_specification",
        "physical_place_id",
        "claim_id",
    ],

    "insert_cols": [
        "global_loss_event_id",
        "master_loss_event_hash_key",
        "loss_event_name",
        "loss_event_description",
        "loss_date",
        "loss_time",
        "loss_event_start_date",
        "loss_event_end_date",
        "loss_event_status",
        "loss_type",
        "loss_category",
        "loss_cause",
        "property_liability_loss_cause",
        "effective_loss_date",
        "notification_date",
        "notification_channel",
        "days_to_fnol",
        "casualty_indicator",
        "catastrophe_flag",
        "natcat_flag",
        "minimal_impact_flag",
        "number_of_injured_parties",
        "number_of_people_involved",
        "number_of_vehicles_involved",
        "drugs_alcohol_indicator",
        "contributory_negligence_flag",
        "damage_specification",
        "physical_place_id",
        "claim_id",
        "effective_from",
        "effective_to",
        "current_flag",
    ]
}

# COMMAND ----------

# MAGIC %skip
# MAGIC BV_CLAIM_MASTER_XREF_CONFIG = {
# MAGIC     "modelling_pattern": "business_vault",
# MAGIC     "name": "bv_claim_master_xref",
# MAGIC     "target_table": f"{catalog_name}.{silver_bv_schema}.bv_claim_master_xref",
# MAGIC     "business_key_col": "global_claim_id",
# MAGIC     "watermark_col": "load_ts",
# MAGIC     "effective_from_col": "load_ts", 
# MAGIC     "scd_type": "1",
# MAGIC     "hash_col": "rawdv_hashkey",
# MAGIC     "object_id": "bv_claim_master_xref_8",
# MAGIC     "stage_sql":  f"""
# MAGIC           WITH crm AS (
# MAGIC             SELECT
# MAGIC               hc.record_source AS crm_record_source,
# MAGIC               hc.claim_hash_key AS crm_claim_hash_key,
# MAGIC               hc.claim_id AS crm_claim_id,
# MAGIC               sc.claim_number,
# MAGIC               sc.load_ts
# MAGIC             FROM {catalog_name}.{silver_rv_schema}.hub_claim hc
# MAGIC             INNER JOIN {catalog_name}.{silver_rv_schema}.sat_claim_crm sc
# MAGIC               ON sc.claim_hash_key = hc.claim_hash_key
# MAGIC           ),
# MAGIC           sap AS (
# MAGIC             SELECT
# MAGIC               hc.record_source AS sap_record_source,
# MAGIC               hc.claim_hash_key AS sap_claim_hash_key,
# MAGIC               hc.claim_id AS sap_claim_id,
# MAGIC               sc.ext_claim_number,
# MAGIC               sc.load_ts
# MAGIC             FROM {catalog_name}.{silver_rv_schema}.hub_claim hc
# MAGIC             INNER JOIN {catalog_name}.{silver_rv_schema}.sat_claim_sap sc
# MAGIC               ON sc.claim_hash_key = hc.claim_hash_key
# MAGIC           ),
# MAGIC           src AS (
# MAGIC             SELECT
# MAGIC               CONCAT(crm.crm_claim_id, '||', sap.sap_claim_id) AS global_claim_id,
# MAGIC               crm.crm_record_source AS rawdv_record_source,
# MAGIC               crm.crm_claim_id AS rawdv_source_business_key,
# MAGIC               crm.crm_claim_hash_key AS rawdv_hash_key,
# MAGIC               GREATEST(
# MAGIC                     COALESCE(crm.${{watermark_col}}, DATE('1900-01-01')),
# MAGIC                     COALESCE(sap.${{watermark_col}}, DATE('1900-01-01'))
# MAGIC                   ) AS watermark_col
# MAGIC             FROM crm
# MAGIC             INNER JOIN sap
# MAGIC               ON crm.claim_number = sap.ext_claim_number
# MAGIC             WHERE GREATEST(
# MAGIC                     COALESCE(crm.${{watermark_col}}, DATE('1900-01-01')),
# MAGIC                     COALESCE(sap.${{watermark_col}}, DATE('1900-01-01'))
# MAGIC                   ) > ${{watermark_col}}
# MAGIC           ),
# MAGIC           dedup AS (
# MAGIC             SELECT
# MAGIC               global_claim_id,
# MAGIC               rawdv_record_source,
# MAGIC               rawdv_source_business_key,
# MAGIC               rawdv_hash_key,
# MAGIC               watermark_col AS load_ts,
# MAGIC               ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY global_claim_id
# MAGIC                 ORDER BY watermark_col DESC
# MAGIC               ) AS rn
# MAGIC             FROM src
# MAGIC           )
# MAGIC           SELECT
# MAGIC             global_claim_id,
# MAGIC             rawdv_record_source,
# MAGIC             rawdv_source_business_key,
# MAGIC             rawdv_hash_key,
# MAGIC             load_ts
# MAGIC           FROM dedup
# MAGIC           WHERE rn = 1
# MAGIC           """,
# MAGIC
# MAGIC     "attribute_cols": ["global_claim_id", "rawdv_record_source", "rawdv_source_business_key", "rawdv_hash_key"],
# MAGIC
# MAGIC     "insert_cols": ["global_claim_id", "rawdv_record_source", "rawdv_source_business_key", "rawdv_hash_key", "load_ts"]
# MAGIC }

# COMMAND ----------

# DBTITLE 1,BV_CLAIM_MASTER_XREF_CONFIG
BV_CLAIM_MASTER_XREF_CONFIG = {
    "modelling_pattern": "business_vault",
    "name": "bv_claim_master_xref",
    "target_table": f"{catalog_name}.{silver_bv_schema}.bv_claim_master_xref",
    "business_key_col": "global_claim_id",
    "watermark_col": "load_ts",
    "effective_from_col": "load_ts", 
    "scd_type": "1",
    "hash_col": "rawdv_hashkey",
    "object_id": "bv_claim_master_xref_8",
    "stage_sql":  f"""
          WITH match AS (
  SELECT
            concat(hc.claim_id, '||', hsap.claim_id) AS global_claim_id,
            hc.record_source AS crm_source,
            hc.claim_id AS crm_business_key,
            hc.claim_hash_key AS crm_hash_key,
            hc.${{watermark_col}} AS crm_load_date,
            hsap.record_source AS sap_source,
            hsap.claim_id AS sap_business_key,
            hsap.claim_hash_key AS sap_hash_key,
            hsap.${{watermark_col}} AS sap_load_date

  FROM {catalog_name}.{silver_rv_schema}.hub_claim hc
   JOIN {catalog_name}.{silver_rv_schema}.sat_claim_crm scr ON hc.claim_hash_key = scr.claim_hash_key
   join {catalog_name}.{silver_rv_schema}.hub_claim hsap ON hsap.record_source = 'SAP'
   JOIN {catalog_name}.{silver_rv_schema}.sat_claim_sap sas ON hsap.claim_hash_key = sas.claim_hash_key
    WHERE hc.record_source = 'CLAIMS'
        AND scr.claim_number = sas.ext_claim_number
        AND GREATEST(
        COALESCE(hc.${{watermark_col}}, DATE('1900-01-01')),
        COALESCE(hsap.${{watermark_col}}, DATE('1900-01-01')) ) > ${{watermark_col}}
),

combined as (
    SELECT global_claim_id, crm_source AS rawdv_record_source, crm_business_key AS rawdv_source_business_key, crm_hash_key AS rawdv_hash_key, crm_load_date AS load_ts FROM match
    UNION ALL
    SELECT global_claim_id, sap_source AS rawdv_record_source, sap_business_key AS rawdv_source_business_key, sap_hash_key AS rawdv_hash_key, sap_load_date AS load_ts FROM match)

SELECT * FROM combined;
          """,

    "attribute_cols": ["global_claim_id", "rawdv_record_source", "rawdv_source_business_key", "rawdv_hash_key"],

    "insert_cols": ["global_claim_id", "rawdv_record_source", "rawdv_source_business_key", "rawdv_hash_key", "load_ts"]
}

# COMMAND ----------

# DBTITLE 1,BV_CLAIM_INVESTIGATION_MASTER_XREF_CONFIG
BV_CLAIM_INVESTIGATION_MASTER_XREF_CONFIG = {
    "modelling_pattern": "business_vault",
    "name": "bv_claim_investigation_master_xref",
    "target_table": f"{catalog_name}.{silver_bv_schema}.bv_claim_investigation_master_xref",
    "business_key_col": "global_claim_investigation_id",
    "watermark_col": "load_ts",
    "effective_from_col": "load_ts", 
    "scd_type": "1",
    "hash_col": "rawdv_hashkey",
    "object_id": "bv_claim_investigation_master_xref_8",
    "stage_sql":  f"""
          WITH match AS (
            SELECT
            concat(hc.claim_investigation_id, '||', hsap.claim_investigation_id) AS global_claim_investigation_id,
            hc.record_source AS crm_source,
            hc.claim_investigation_id AS crm_business_key,
            hc.claim_investigation_hash_key AS crm_hash_key,
            hc.${{watermark_col}} AS crm_load_date,
            hsap.record_source AS sap_source,
            hsap.claim_investigation_id AS sap_business_key,
            hsap.claim_investigation_hash_key AS sap_hash_key,
            hsap.${{watermark_col}} AS sap_load_date

  FROM {catalog_name}.{silver_rv_schema}.hub_claim_investigation hc
   JOIN {catalog_name}.{silver_rv_schema}.sat_claim_investigation_crm sci ON hc.claim_investigation_hash_key = sci.claim_investigation_hash_key
   join {catalog_name}.{silver_rv_schema}.hub_claim_investigation hsap ON hsap.record_source = 'SAP'
   JOIN {catalog_name}.{silver_rv_schema}.sat_claim_investigation_sap sas ON hsap.claim_investigation_hash_key = sas.claim_investigation_hash_key
    WHERE hc.record_source = 'CLAIMS'
        AND upper(trim(sci.claim_handler_identifier)) = upper(trim(sas.claim_investigator_id))
        AND sci.claim_investigation_start_date = sas.case_open_date
        AND sci.claim_investigation_end_date = sas.case_close_date
        AND GREATEST(
            COALESCE(hc.${{watermark_col}}, DATE('1900-01-01')),
            COALESCE(hsap.${{watermark_col}}, DATE('1900-01-01')) ) > ${{watermark_col}}
),

combined as (
    SELECT global_claim_investigation_id, crm_source AS rawdv_record_source, crm_business_key AS rawdv_source_business_key, crm_hash_key AS rawdv_hash_key, crm_load_date AS load_ts FROM match
    UNION ALL
    SELECT global_claim_investigation_id, sap_source AS rawdv_record_source, sap_business_key AS rawdv_source_business_key, sap_hash_key AS rawdv_hash_key, sap_load_date AS load_ts FROM match)

SELECT * FROM combined;
          """,

    "attribute_cols": ["global_claim_investigation_id", "rawdv_record_source", "rawdv_source_business_key", "rawdv_hash_key"],

    "insert_cols": ["global_claim_investigation_id", "rawdv_record_source", "rawdv_source_business_key", "rawdv_hash_key", "load_ts"]
}

# COMMAND ----------

# DBTITLE 1,BV_LOSS_EVENT_MASTER_XREF_CONFIG
BV_LOSS_EVENT_MASTER_XREF_CONFIG = {
    "modelling_pattern": "business_vault",
    "name": "bv_loss_event_master_xref",
    "target_table": f"{catalog_name}.{silver_bv_schema}.bv_loss_event_master_xref",
    "business_key_col": "global_loss_event_id",
    "watermark_col": "load_ts",
    "effective_from_col": "load_ts", 
    "scd_type": "1",
    "hash_col": "rawdv_hashkey",
    "object_id": "bv_loss_event_master_xref_8",
    "stage_sql":  f"""
          WITH match AS (
            SELECT
            concat(hc.loss_event_id, '||', hsap.loss_event_id) AS global_loss_event_id,
            hc.record_source AS crm_source,
            hc.loss_event_id AS crm_business_key,
            hc.loss_event_hash_key AS crm_hash_key,
            hc.${{watermark_col}} AS crm_load_date,
            hsap.record_source AS sap_source,
            hsap.loss_event_id AS sap_business_key,
            hsap.loss_event_hash_key AS sap_hash_key,
            hsap.${{watermark_col}} AS sap_load_date

  FROM {catalog_name}.{silver_rv_schema}.hub_loss_event hc
   JOIN {catalog_name}.{silver_rv_schema}.sat_loss_event_crm scr ON hc.loss_event_hash_key = scr.loss_event_hash_key
   join {catalog_name}.{silver_rv_schema}.hub_loss_event hsap ON hsap.record_source = 'SAP'
   JOIN {catalog_name}.{silver_rv_schema}.sat_loss_event_sap sas ON hsap.loss_event_hash_key = sas.loss_event_hash_key
    WHERE hc.record_source = 'CLAIMS' and
        hc.loss_event_id = hsap.loss_event_id
            AND GREATEST(
            COALESCE(hc.${{watermark_col}}, DATE('1900-01-01')),
            COALESCE(hsap.${{watermark_col}}, DATE('1900-01-01')) ) > ${{watermark_col}}
),

combined as (
    SELECT global_loss_event_id, crm_source AS rawdv_record_source, crm_business_key AS rawdv_source_business_key, crm_hash_key AS rawdv_hash_key, crm_load_date AS load_ts FROM match
    UNION ALL
    SELECT global_loss_event_id, sap_source AS rawdv_record_source, sap_business_key AS rawdv_source_business_key, sap_hash_key AS rawdv_hash_key, sap_load_date AS load_ts FROM match)

SELECT * FROM combined;
          """,

    "attribute_cols": ["global_loss_event_id", "rawdv_record_source", "rawdv_source_business_key", "rawdv_hash_key"],

    "insert_cols": ["global_loss_event_id", "rawdv_record_source", "rawdv_source_business_key", "rawdv_hash_key", "load_ts"]
}

# COMMAND ----------

ALL_DIM_CONFIGS = {
  "bv_claim_master": {"config": BV_CLAIM_MASTER_CFG, "is_active": True},
  "bv_claim_investigation_master": {"config": BV_CLAIM_INVESTIGATION_MASTER_CFG, "is_active": True},
  "bv_loss_event_master": {"config": BV_LOSS_EVENT_MASTER_CFG, "is_active": True},
  "bv_claim_master_xref": {"config": BV_CLAIM_MASTER_XREF_CONFIG, "is_active": True},
  "bv_claim_investigation_master_xref": {"config": BV_CLAIM_INVESTIGATION_MASTER_XREF_CONFIG, "is_active": True},
  "bv_loss_event_master_xref": {"config": BV_LOSS_EVENT_MASTER_XREF_CONFIG, "is_active": True},
}