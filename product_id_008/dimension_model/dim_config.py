# Databricks notebook source
# DBTITLE 1,Variables for Catalog and Schema Widget Inputs
dbutils.widgets.text("data_catalog", "allianz_coe", "Catalog Name")
dbutils.widgets.text("gold_schema_name", "gold", "Gold Schema Name")
dbutils.widgets.text("silver_schema_name", "silver", "Silver Schema Name")

catalog_name = dbutils.widgets.get("data_catalog")
gold_schema_name = dbutils.widgets.get("gold_schema_name")
vault_schema = dbutils.widgets.get("silver_schema_name")

# COMMAND ----------

# DBTITLE 1,DIM_LOSS_EVENT_CFG
DIM_LOSS_EVENT_CFG = {
    "name": "dim_loss_event",
    "target_table": f"{catalog_name}.{gold_schema_name}.dim_loss_event",
    "business_key_col": "loss_event_id",
    "scd_type": "2",
    "watermark_col": "load_ts",
    "effective_from_col": "effective_from_ts",
    "record_version_col": "record_version",

    "stage_sql": f"""
        WITH src AS (
          SELECT
            cim.global_loss_event_id AS loss_event_id,
            cim.loss_event_name AS loss_event_name,
            cim.loss_event_status AS loss_event_status,
            cim.loss_type AS loss_type,
            cim.loss_category AS loss_category,
            cim.loss_cause AS loss_cause,
            cim.property_liability_loss_cause AS property_liability_loss_cause,
            cim.loss_event_description AS loss_event_description,
            cim.loss_date AS loss_date,
            cim.loss_time AS loss_time,
            cim.loss_event_start_date AS loss_event_start_date,
            cim.loss_event_end_date AS loss_event_end_date,
            cim.effective_loss_date AS effective_loss_date,
            cim.notification_date AS notification_date,
            cim.notification_channel AS notification_channel,
            cim.days_to_fnol AS days_to_fnol,
            cim.casualty_indicator AS casualty_indicator,
            cim.catastrophe_flag AS catastrophe_flag,
            cim.natcat_flag AS natcat_flag,
            cim.minimal_impact_flag AS minimal_impact_flag,
            cim.number_of_injured_parties AS number_of_injured_parties,
            cim.number_of_people_involved AS number_of_people_involved,
            cim.number_of_vehicles_involved AS number_of_vehicles_involved,
            cim.drugs_alcohol_indicator AS drugs_alcohol_indicator,
            cim.contributory_negligence_flag AS contributory_negligence_flag,
            cim.damage_specification AS damage_specification,
            cim.physical_place_id AS physical_place_id,
            cim.claim_id AS claim_id,
            COALESCE(cim.effective_from, DATE('1900-01-01')) AS effective_from_ts,
            xci.${{watermark_col}} AS watermark_col,
            -1 AS geography_sk
          FROM {catalog_name}.{vault_schema}.bv_loss_event_master cim
          LEFT JOIN {catalog_name}.{vault_schema}.bv_loss_event_master_xref xci
            ON cim.global_loss_event_id = xci.global_loss_event_id
          LEFT JOIN {catalog_name}.{vault_schema}.link_physical_place_loss_event lpl
            ON xci.rawdv_hash_key = lpl.loss_event_hash_key
          LEFT JOIN {catalog_name}.{vault_schema}.hub_physical_place hip
            ON lpl.physical_place_hash_key = hip.physical_place_hash_key
          LEFT JOIN {catalog_name}.{vault_schema}.sat_physical_place spp
            ON hip.physical_place_hash_key = spp.physical_place_hash_key
          --LEFT JOIN {catalog_name}.{gold_schema_name}.dim_geography dg
            --ON UPPER(TRIM(spp.country)) = UPPER(TRIM(dg.country))
           --AND UPPER(TRIM(spp.state)) = UPPER(TRIM(dg.state))
           --AND UPPER(TRIM(spp.city)) = UPPER(TRIM(dg.city))
          WHERE xci.${{watermark_col}} > ${{watermark_col}}
        ),
        dedup AS (
          SELECT
            loss_event_id,
            loss_event_name,
            loss_event_status,
            loss_type,
            loss_category,
            loss_cause,
            property_liability_loss_cause,
            loss_event_description,
            loss_date,
            loss_time,
            loss_event_start_date,
            loss_event_end_date,
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
            geography_sk,
            effective_from_ts,
            watermark_col,
            ROW_NUMBER() OVER (
              PARTITION BY loss_event_id
              ORDER BY watermark_col DESC
            ) AS rn
          FROM src
        )
        SELECT
          geography_sk,
          loss_event_id,
          loss_event_name,
          loss_event_status,
          loss_type,
          loss_category,
          loss_cause,
          property_liability_loss_cause,
          loss_event_description,
          loss_date,
          loss_time,
          loss_event_start_date,
          loss_event_end_date,
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
          watermark_col AS effective_from_ts,
          TIMESTAMP '9999-12-31 00:00:00' AS effective_to_ts
        FROM dedup
        WHERE rn = 1
    """,

    "attribute_cols": [
        "geography_sk",
        "loss_event_id",
        "loss_event_name",
        "loss_event_status",
        "loss_type",
        "loss_category",
        "loss_cause",
        "property_liability_loss_cause",
        "loss_event_description",
        "loss_date",
        "loss_time",
        "loss_event_start_date",
        "loss_event_end_date",
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
        "geography_sk",
        "loss_event_id",
        "loss_event_name",
        "loss_event_status",
        "loss_type",
        "loss_category",
        "loss_cause",
        "property_liability_loss_cause",
        "loss_event_description",
        "loss_date",
        "loss_time",
        "loss_event_start_date",
        "loss_event_end_date",
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
        "attr_hash",
        "effective_from_ts",
        "effective_to_ts",
        "record_version",
        "created_by",
        "created_ts",
        "last_updated_by",
        "last_updated_ts",
    ]
}

# COMMAND ----------

# DBTITLE 1,DIM_COVERAGE_CFG
DIM_COVERAGE_CFG = {
    "name": "dim_coverage",
    "target_table": f"{catalog_name}.{gold_schema_name}.dim_coverage",
    "business_key_col": "coverage_id",
    "scd_type": "2",
    "watermark_col": "load_ts",
    "effective_from_col": "effective_from_ts",
    "record_version_col": "record_version",

    "stage_sql": f"""
        WITH src AS (
          SELECT
            hc.coverage_id AS coverage_id,
            sc.coverage_type AS coverage_type,
            sc.coverage_type_id AS coverage_type_id,
            sc.coverage_description AS coverage_description,
            sc.coverage_start_date AS coverage_start_date,
            sc.coverage_end_date AS coverage_end_date,
            sc.maximum_deductible AS maximum_deductible,
            COALESCE(sc.load_ts, DATE('1900-01-01')) AS effective_from_ts,
            sc.${{watermark_col}} AS watermark_col
          FROM {catalog_name}.{vault_schema}.hub_coverage hc
          LEFT JOIN {catalog_name}.{vault_schema}.sat_coverage sc
            ON hc.coverage_hash_key = sc.coverage_hash_key
          WHERE sc.${{watermark_col}} > ${{watermark_col}}
        ),
        dedup AS (
          SELECT
            coverage_id,
            coverage_type,
            coverage_type_id,
            coverage_description,
            coverage_start_date,
            coverage_end_date,
            maximum_deductible,
            effective_from_ts,
            watermark_col,
            ROW_NUMBER() OVER (
              PARTITION BY coverage_id
              ORDER BY watermark_col DESC
            ) AS rn
          FROM src
        )
        SELECT
          coverage_id,
          coverage_type,
          coverage_type_id,
          coverage_description,
          coverage_start_date,
          coverage_end_date,
          maximum_deductible,
          watermark_col AS effective_from_ts,
          TIMESTAMP '9999-12-31 00:00:00' AS effective_to_ts
        FROM dedup
        WHERE rn = 1
    """,

    "attribute_cols": [
        "coverage_id",
        "coverage_type",
        "coverage_type_id",
        "coverage_description",
        "coverage_start_date",
        "coverage_end_date",
        "maximum_deductible",
    ],

    "insert_cols": [
        "coverage_id",
        "coverage_type",
        "coverage_type_id",
        "coverage_description",
        "coverage_start_date",
        "coverage_end_date",
        "maximum_deductible",
        "attr_hash",
        "effective_from_ts",
        "effective_to_ts",
        "record_version",
        "created_by",
        "created_ts",
        "last_updated_by",
        "last_updated_ts",
    ]
}

# COMMAND ----------

# DBTITLE 1,DIM_MEDICAL_CFG
DIM_MEDICAL_CFG = {
    "name": "dim_medical",
    "target_table": f"{catalog_name}.{gold_schema_name}.dim_medical",
    "business_key_cols": ["treatment_id","diagnosis_id","medical_condition_id","injury_id","document_id","medical_report_id","health_insurance_claim_id","medical_assessment_id"],
    "scd_type": "2",
    "watermark_col": "load_ts",
    "effective_from_col": "effective_from_ts",
    "record_version_col": "record_version",

    "stage_sql": f"""
        WITH src AS (
          SELECT
            hd.diagnosis_id AS diagnosis_id,
            sd.diagnosis_code AS diagnosis_code,
            sd.diagnosis_description AS diagnosis_description,
            sd.diagnosis_priority AS diagnosis_priority,
            sd.diagnosis_standard AS diagnosis_standard,
            hi.injury_id AS injury_id,
            si.bodily_injury_date AS injury_date,
            si.injury_description AS injury_description,
            mc.medical_condition_id AS medical_condition_id,
            smc.disease_name AS disease_name,
            smc.disability_status AS disability_status,
            smc.pre_existing_condition AS pre_existing_condition,
            hma.medical_assessment_id AS medical_assessment_id,
            sma.person_hospital_status AS person_hospital_status,
            sma.psychological_factors AS psychological_factors,
            sma.medical_condition_description AS medical_condition_description,
            hmr.medical_report_id AS medical_report_id,
            smr.document_id AS document_id,
            smr.health_declaration AS health_declaration,
            ht.treatment_id AS treatment_id,
            st.treatment_code AS treatment_code,
            st.treatment_code_standard AS treatment_code_standard,
            st.treatment_description AS treatment_description,
            st.treatment_complexity_level AS treatment_complexity_level,
            st.medication_code AS medication_code,
            st.treatment_type AS treatment_type,
            hhic.health_insurance_claim_id AS health_insurance_claim_id,
            shic.health_data_consent AS health_data_consent,
            shic.date_of_first_medical_expert_report AS date_of_first_medical_expert_report,
            greatest(
              coalesce(sd.load_ts, date('1900-01-01')),
              coalesce(si.load_ts, date('1900-01-01')),
              coalesce(smc.load_ts, date('1900-01-01')),
              coalesce(sma.load_ts, date('1900-01-01')),
              coalesce(smr.load_ts, date('1900-01-01')),
              coalesce(st.load_ts, date('1900-01-01')),
              coalesce(shic.load_ts, date('1900-01-01'))
            ) AS effective_from_ts,
            greatest(
              coalesce(sd.${{watermark_col}}, date('1900-01-01')),
              coalesce(si.${{watermark_col}}, date('1900-01-01')),
              coalesce(smc.${{watermark_col}}, date('1900-01-01')),
              coalesce(sma.${{watermark_col}}, date('1900-01-01')),
              coalesce(smr.${{watermark_col}}, date('1900-01-01')),
              coalesce(st.${{watermark_col}}, date('1900-01-01')),
              coalesce(shic.${{watermark_col}}, date('1900-01-01'))
            ) AS watermark_col
          FROM {catalog_name}.{vault_schema}.hub_claim hc
          INNER JOIN {catalog_name}.{vault_schema}.link_claim_claim_event lc
            ON hc.claim_hash_key = lc.claim_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.link_claim_event_treatment lcet
            ON lc.claim_event_hash_key = lcet.claim_event_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.hub_treatment ht
            ON lcet.treatment_hash_key = ht.treatment_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.link_medical_condition_treatment lmct
            ON ht.treatment_hash_key = lmct.treatment_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.hub_medical_condition mc
            ON lmct.medical_condition_hash_key = mc.medical_condition_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.link_medical_condition_diagnosis lmcd
            ON lmct.medical_condition_hash_key = lmcd.medical_condition_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.hub_diagnosis hd
            ON lmcd.diagnosis_hash_key = hd.diagnosis_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.sat_diagnosis sd
            ON sd.diagnosis_hash_key = hd.diagnosis_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.link_claim_event_injury lcei
            ON lc.claim_event_hash_key = lcei.claim_event_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.hub_injury hi
            ON lcei.injury_hash_key = hi.injury_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.sat_injury si
            ON si.injury_hash_key = hi.injury_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.sat_medical_condition smc
            ON smc.medical_condition_hash_key = mc.medical_condition_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.link_claim_event_medical_assessment lcema
            ON lc.claim_event_hash_key = lcema.claim_event_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.hub_medical_assessment hma
            ON lcema.medical_assessment_hash_key = hma.medical_assessment_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.sat_medical_assessment sma
            ON sma.medical_assessment_hash_key = hma.medical_assessment_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.link_claim_medical_report lcmr
            ON hc.claim_hash_key = lcmr.claim_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.hub_medical_report hmr
            ON lcmr.medical_report_hash_key = hmr.medical_report_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.sat_medical_report smr
            ON smr.medical_report_hash_key = hmr.medical_report_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.sat_treatment st
            ON st.treatment_hash_key = ht.treatment_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.link_claim_health_insurance_claim lchic
            ON hc.claim_hash_key = lchic.claim_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.hub_health_insurance_claim hhic
            ON lchic.health_insurance_claim_hash_key = hhic.health_insurance_claim_hash_key
          INNER JOIN {catalog_name}.{vault_schema}.sat_health_insurance_claim shic
            ON shic.health_insurance_claim_hash_key = hhic.health_insurance_claim_hash_key
          WHERE greatest(
                  coalesce(sd.${{watermark_col}}, date('1900-01-01')),
                  coalesce(si.${{watermark_col}}, date('1900-01-01')),
                  coalesce(smc.${{watermark_col}}, date('1900-01-01')),
                  coalesce(sma.${{watermark_col}}, date('1900-01-01')),
                  coalesce(smr.${{watermark_col}}, date('1900-01-01')),
                  coalesce(st.${{watermark_col}}, date('1900-01-01')),
                  coalesce(shic.${{watermark_col}}, date('1900-01-01'))
                ) > ${{watermark_col}}
        ),
        dedup AS (
          SELECT
            diagnosis_id,
            diagnosis_code,
            diagnosis_description,
            diagnosis_priority,
            diagnosis_standard,
            injury_id,
            injury_date,
            injury_description,
            medical_condition_id,
            disease_name,
            disability_status,
            pre_existing_condition,
            medical_assessment_id,
            person_hospital_status,
            psychological_factors,
            medical_condition_description,
            medical_report_id,
            document_id,
            health_declaration,
            treatment_id,
            treatment_code,
            treatment_code_standard,
            treatment_description,
            treatment_complexity_level,
            medication_code,
            treatment_type,
            health_insurance_claim_id,
            health_data_consent,
            date_of_first_medical_expert_report,
            effective_from_ts,
            watermark_col,
            row_number() OVER (
              PARTITION BY treatment_id,
                           diagnosis_id,
                           medical_condition_id,
                           injury_id,
                           document_id,
                           medical_report_id,
                           health_insurance_claim_id,
                           medical_assessment_id
              ORDER BY watermark_col DESC
            ) AS rn
          FROM src
        )
        SELECT
          diagnosis_id,
          diagnosis_code,
          diagnosis_description,
          diagnosis_priority,
          diagnosis_standard,
          injury_id,
          injury_date,
          injury_description,
          medical_condition_id,
          disease_name,
          disability_status,
          pre_existing_condition,
          medical_assessment_id,
          person_hospital_status,
          psychological_factors,
          medical_condition_description,
          medical_report_id,
          document_id,
          health_declaration,
          treatment_id,
          treatment_code,
          treatment_code_standard,
          treatment_description,
          treatment_complexity_level,
          medication_code,
          treatment_type,
          health_insurance_claim_id,
          health_data_consent,
          date_of_first_medical_expert_report,
          watermark_col AS effective_from_ts,
          timestamp '9999-12-31 00:00:00' AS effective_to_ts
        FROM dedup
        WHERE rn = 1
    """,

    "attribute_cols": [
        "diagnosis_id",
        "diagnosis_code",
        "diagnosis_description",
        "diagnosis_priority",
        "diagnosis_standard",
        "injury_id",
        "injury_date",
        "injury_description",
        "medical_condition_id",
        "disease_name",
        "disability_status",
        "pre_existing_condition",
        "medical_assessment_id",
        "person_hospital_status",
        "psychological_factors",
        "medical_condition_description",
        "medical_report_id",
        "document_id",
        "health_declaration",
        "treatment_id",
        "treatment_code",
        "treatment_code_standard",
        "treatment_description",
        "treatment_complexity_level",
        "medication_code",
        "treatment_type",
        "health_data_consent",
        "health_insurance_claim_id",
        "date_of_first_medical_expert_report",
    ],

    "insert_cols": [
        "diagnosis_id",
        "diagnosis_code",
        "diagnosis_description",
        "diagnosis_priority",
        "diagnosis_standard",
        "injury_id",
        "injury_date",
        "injury_description",
        "medical_condition_id",
        "disease_name",
        "disability_status",
        "pre_existing_condition",
        "medical_assessment_id",
        "person_hospital_status",
        "psychological_factors",
        "medical_condition_description",
        "medical_report_id",
        "document_id",
        "health_declaration",
        "treatment_id",
        "treatment_code",
        "treatment_code_standard",
        "treatment_description",
        "treatment_complexity_level",
        "medication_code",
        "treatment_type",
        "health_data_consent",
        "health_insurance_claim_id",
        "date_of_first_medical_expert_report",
        "attr_hash",
        "effective_from_ts",
        "effective_to_ts",
        "record_version",
        "created_by",
        "created_ts",
        "last_updated_by",
        "last_updated_ts",
    ]
}

# COMMAND ----------

# DBTITLE 1,DIM Campaign Configuration and Latest Snapshot Query
DIM_CATASTROPHE_CFG = {
    "name": "dim_catastrophe",
    "target_table": f"{catalog_name}.{gold_schema_name}.dim_catastrophe",
    "business_key_col": "campaign_id",
    "watermark_col": "load_ts",
    "effective_from_col": "load_ts",
    "record_version_col": "record_version",
    "scd_type": "1",

    "stage_sql": f"""
WITH src AS (
  SELECT
    hc.catastrophe_id,
    sc.catastrophe_cause AS catastrophe_cause,
    sc.catastrophe_description AS catastrophe_description,
    sc.catastrophe_begin_date AS catastrophe_begin_date,
    sc.catastrophe_end_date AS catastrophe_end_date,
    sc.cresta_zone AS cresta_zone,
    sc.nat_cat_event_code AS nat_cat_event_code,
    sc.${{watermark_col}} AS watermark_col
  FROM {catalog_name}.{vault_schema}.hub_catastrophe hc
  LEFT JOIN {catalog_name}.{vault_schema}.sat_catastrophe sc
    ON sc.catastrophe_hash_key = hc.catastrophe_hash_key
  WHERE sc.${{watermark_col}} > ${{watermark_col}}
),
dedup AS (
  SELECT
    catastrophe_id,
    catastrophe_cause,
    catastrophe_description,
    catastrophe_begin_date,
    catastrophe_end_date,
    cresta_zone,
    nat_cat_event_code,
    ROW_NUMBER() OVER (
      PARTITION BY catastrophe_id
      ORDER BY watermark_col DESC
    ) AS rn
  FROM src
)
SELECT
  catastrophe_id,
  catastrophe_cause,
  catastrophe_description,
  catastrophe_begin_date,
  catastrophe_end_date,
  cresta_zone,
  nat_cat_event_code,
  SHA2(
    CONCAT_WS('||',
      COALESCE(CAST(catastrophe_id AS STRING), ''),
      COALESCE(CAST(catastrophe_cause AS STRING), ''),
      COALESCE(CAST(catastrophe_description AS STRING), ''),
      COALESCE(CAST(catastrophe_begin_date AS STRING), ''),
      COALESCE(CAST(catastrophe_end_date AS STRING), ''),
      COALESCE(CAST(cresta_zone AS STRING), ''),
      COALESCE(CAST(nat_cat_event_code AS STRING), '')
    ),
    256
  ) AS attr_hash
FROM dedup
WHERE rn = 1
""",

    "attribute_cols": ["campaign_id", "campaign_name", "campaign_type", "campaign_start_date", "campaign_end_date", "campaign_status", "campaign_budget", "campaign_target_audience", "campaign_marketing_source", "campaign_owner_department", "campaign_country", "campaign_conversion_goal", "number_of_clicks", "is_active", "number_of_visits", "number_of_policy_purchases", "number_of_emails_sent", "number_of_email_bounced", "number_of_emails_delivered", "number_of_emails_opened", "click_through_rate", "spend_amt", "incremental_revenue", "survey_wave", "total_number_of_respondents", "number_of_respondents_aware", "number_of_promoters", "number_of_passives", "number_of_detractors", "number_of_followers", "number_of_likes", "number_of_comments", "number_of_shares", "number_of_brand_mentions", "number_of_category_mentions", "number_of_impressions"
    ],

    "insert_cols": [
        "campaign_id", "campaign_name", "campaign_type", "campaign_start_date", "campaign_end_date", "campaign_status", "campaign_budget", "campaign_target_audience", "campaign_marketing_source", "campaign_owner_department", "campaign_country", "campaign_conversion_goal", "number_of_clicks", "is_active", "number_of_visits", "number_of_policy_purchases", "number_of_emails_sent", "number_of_email_bounced", "number_of_emails_delivered", "number_of_emails_opened", "click_through_rate", "spend_amt", "incremental_revenue", "survey_wave", "total_number_of_respondents", "number_of_respondents_aware", "number_of_promoters", "number_of_passives", "number_of_detractors", "number_of_followers", "number_of_likes", "number_of_comments", "number_of_shares", "number_of_brand_mentions", "number_of_category_mentions", "number_of_impressions",
        "attr_hash",
        "created_by",
        "created_ts",
        "last_updated_by",
        "last_updated_ts",
    ]
}

# COMMAND ----------

# DBTITLE 1,DIM Channel Configuration and Latest Snapshot Query
# MAGIC %skip
# MAGIC DIM_CHANNEL_CFG = {
# MAGIC     "name": "dim_channel",
# MAGIC     "target_table": f"{catalog_name}.{gold_schema_name}.dim_channel",
# MAGIC     "business_key_col": "channel_id",
# MAGIC     "watermark_col": "load_date",
# MAGIC     "effective_from_col": "effective_from_ts",
# MAGIC     "record_version_col": "record_version",
# MAGIC     "scd_type": "1",
# MAGIC
# MAGIC     "stage_sql": f"""
# MAGIC         WITH src AS (
# MAGIC   SELECT
# MAGIC     hu.channel_id AS channel_id,
# MAGIC     sa.channel_name AS channel_name,
# MAGIC     sa.channel_type AS channel_type,
# MAGIC     sa.load_date AS watermark_col
# MAGIC   FROM {catalog_name}.{vault_schema}.HUB_CHANNEL hu
# MAGIC   LEFT JOIN {catalog_name}.{vault_schema}.sat_channel_crm sa
# MAGIC     ON hu.channel_hash_key = sa.channel_hash_key
# MAGIC   WHERE sa.load_date > ${{watermark_col}}
# MAGIC ),
# MAGIC dedup AS (
# MAGIC   SELECT
# MAGIC     channel_id,
# MAGIC     channel_name,
# MAGIC     channel_type,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY channel_id
# MAGIC       ORDER BY watermark_col DESC
# MAGIC     ) AS rn,
# MAGIC     watermark_col as created_ts
# MAGIC   FROM src
# MAGIC )
# MAGIC SELECT
# MAGIC   channel_id,
# MAGIC   channel_name,
# MAGIC   channel_type,
# MAGIC   created_ts
# MAGIC FROM dedup
# MAGIC WHERE rn = 1
# MAGIC     """,
# MAGIC
# MAGIC     "attribute_cols": ["channel_id", "channel_name", "channel_type"
# MAGIC     ],
# MAGIC
# MAGIC     "insert_cols": [
# MAGIC         "channel_id", "channel_name", "channel_type",
# MAGIC         "attr_hash",
# MAGIC         "created_by",
# MAGIC         "created_ts",
# MAGIC         "last_updated_by",
# MAGIC         "last_updated_ts",
# MAGIC     ]
# MAGIC }

# COMMAND ----------

# DBTITLE 1,DIM Geography Configuration and Region Assignment Query
# MAGIC %skip
# MAGIC DIM_GEOGRAPHY_CFG = {
# MAGIC     "name": "dim_geography",
# MAGIC     "target_table": f"{catalog_name}.{gold_schema_name}.dim_geography",
# MAGIC     "business_key_cols": ["country","state","city"],
# MAGIC     "watermark_col": "load_timestamp",
# MAGIC     "effective_from_col": "effective_from_ts",
# MAGIC     "record_version_col": "record_version",
# MAGIC     "scd_type": "1",
# MAGIC
# MAGIC     "stage_sql": f"""
# MAGIC         with combined as (
# MAGIC 			SELECT
# MAGIC 				*,
# MAGIC 			row_number() over (partition by am.global_address_identifier order by sa.load_timestamp desc) as rn,
# MAGIC             sa.load_timestamp AS created_ts
# MAGIC 			FROM {catalog_name}.{vault_schema}.bv_address_master am
# MAGIC 			join {catalog_name}.{vault_schema}.bv_address_master_xref sa
# MAGIC 				on sa.global_address_identifier = am.global_address_identifier
# MAGIC 			)
# MAGIC
# MAGIC 			select distinct
# MAGIC 				city,
# MAGIC 				state,
# MAGIC 				country,
# MAGIC 				region,
# MAGIC 				created_ts
# MAGIC 			from combined where rn = 1 and created_ts > ${{watermark_col}};
# MAGIC     """,
# MAGIC
# MAGIC     "attribute_cols": ["city", "state", "country", "region"
# MAGIC     ],
# MAGIC
# MAGIC     "insert_cols": [
# MAGIC         "city", "state", "country", "region",
# MAGIC         "attr_hash",
# MAGIC         "created_by",
# MAGIC         "created_ts",
# MAGIC         "last_updated_by",
# MAGIC         "last_updated_ts",
# MAGIC     ]
# MAGIC }

# COMMAND ----------

# DBTITLE 1,DIM Insured Object Configuration and Deduplicated Query
# MAGIC %skip
# MAGIC DIM_INSURED_OBJECT_CFG = {
# MAGIC     "name": "dim_insured_object",
# MAGIC     "target_table": f"{catalog_name}.{gold_schema_name}.dim_insured_object",
# MAGIC     "business_key_col": "insured_object_id",
# MAGIC     "watermark_col": "load_date",
# MAGIC     "effective_from_col": "effective_from_ts",
# MAGIC     "record_version_col": "record_version",
# MAGIC     "scd_type": "1",
# MAGIC
# MAGIC     "stage_sql": f"""
# MAGIC         WITH src AS (
# MAGIC   SELECT
# MAGIC     hu.INSURED_OBJECT_ID AS insured_object_id,
# MAGIC     sa.INSURED_OBJECT_TYPE AS insured_object_type,
# MAGIC     sa.INSURED_OBJECT_SUB_TYPE AS insured_object_sub_type,
# MAGIC     sa.INSURED_OBJECT_DESCRIPTION AS insured_object_desc,
# MAGIC     sa.INSURED_VALUE AS insured_value,
# MAGIC     sa.CURRENCY_CODE AS currency_code,
# MAGIC     sa.INSURED_OBJECT_START_DATE AS insured_object_start_date,
# MAGIC     sa.INSURED_OBJECT_END_DATE AS insured_object_end_date,
# MAGIC     sa.INSURED_OBJECT_CURRENT_STATUS AS insured_object_current_status,
# MAGIC     sa.load_date AS watermark_col
# MAGIC   FROM {catalog_name}.{vault_schema}.HUB_INSURED_OBJECT hu
# MAGIC   LEFT JOIN {catalog_name}.{vault_schema}.sat_insured_object sa
# MAGIC     ON hu.insured_object_hash_key = sa.insured_object_hash_key
# MAGIC   WHERE sa.load_date > ${{watermark_col}}
# MAGIC ),
# MAGIC dedup AS (
# MAGIC   SELECT
# MAGIC     insured_object_id,
# MAGIC     insured_object_type,
# MAGIC     insured_object_sub_type,
# MAGIC     insured_object_desc,
# MAGIC     insured_value,
# MAGIC     currency_code,
# MAGIC     insured_object_start_date,
# MAGIC     insured_object_end_date,
# MAGIC     insured_object_current_status,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY insured_object_id
# MAGIC       ORDER BY watermark_col DESC
# MAGIC     ) AS rn,
# MAGIC     watermark_col as created_ts
# MAGIC   FROM src
# MAGIC )
# MAGIC SELECT
# MAGIC   insured_object_id,
# MAGIC   insured_object_type,
# MAGIC   insured_object_sub_type,
# MAGIC   insured_object_desc,
# MAGIC   insured_value,
# MAGIC   currency_code,
# MAGIC   insured_object_start_date,
# MAGIC   insured_object_end_date,
# MAGIC   insured_object_current_status,
# MAGIC   created_ts
# MAGIC FROM dedup
# MAGIC WHERE rn = 1
# MAGIC     """,
# MAGIC
# MAGIC     "attribute_cols": ["insured_object_id", "insured_object_type", "insured_object_sub_type", "insured_object_desc", "insured_value", "currency_code", "insured_object_start_date", "insured_object_end_date", "insured_object_current_status"
# MAGIC     ],
# MAGIC
# MAGIC     "insert_cols": [
# MAGIC         "insured_object_id", "insured_object_type", "insured_object_sub_type", "insured_object_desc", "insured_value", "currency_code", "insured_object_start_date", "insured_object_end_date", "insured_object_current_status",
# MAGIC         "attr_hash",
# MAGIC         "created_by",
# MAGIC         "created_ts",
# MAGIC         "last_updated_by",
# MAGIC         "last_updated_ts",
# MAGIC     ]
# MAGIC }

# COMMAND ----------

# DBTITLE 1,DIM Override Configuration and Latest Deduplicated Snap ...
# MAGIC %skip
# MAGIC DIM_OVERRIDE_CFG = {
# MAGIC     "name": "dim_override",
# MAGIC     "target_table": f"{catalog_name}.{gold_schema_name}.dim_override",
# MAGIC     "business_key_col": "override_id",
# MAGIC     "watermark_col": "load_date",
# MAGIC     "effective_from_col": "effective_from_ts",
# MAGIC     "record_version_col": "record_version",
# MAGIC     "scd_type": "1",
# MAGIC
# MAGIC     "stage_sql": f"""
# MAGIC         WITH src AS (
# MAGIC   SELECT
# MAGIC     hu.override_id AS override_id,
# MAGIC     sa.override_reason AS override_reason,
# MAGIC     sa.load_date AS watermark_col
# MAGIC   FROM {catalog_name}.{vault_schema}.HUB_OVERRIDE hu
# MAGIC   LEFT JOIN {catalog_name}.{vault_schema}.sat_override_crm sa
# MAGIC     ON sa.override_hash_key = hu.override_hash_key
# MAGIC   WHERE sa.load_date > ${{watermark_col}}
# MAGIC ),
# MAGIC dedup AS (
# MAGIC   SELECT
# MAGIC     override_id,
# MAGIC     override_reason,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY override_id
# MAGIC       ORDER BY watermark_col DESC
# MAGIC     ) AS rn,
# MAGIC     watermark_col as created_ts
# MAGIC   FROM src
# MAGIC )
# MAGIC SELECT
# MAGIC   override_id,
# MAGIC   override_reason,
# MAGIC   created_ts
# MAGIC FROM dedup
# MAGIC WHERE rn = 1
# MAGIC     """,
# MAGIC
# MAGIC     "attribute_cols": ["override_id", "override_reason"
# MAGIC     ],
# MAGIC
# MAGIC     "insert_cols": [
# MAGIC         "override_id", 
# MAGIC         "override_reason",
# MAGIC         "attr_hash",
# MAGIC         "created_by",
# MAGIC         "created_ts",
# MAGIC         "last_updated_by",
# MAGIC         "last_updated_ts",
# MAGIC     ]
# MAGIC }

# COMMAND ----------

# DBTITLE 1,DIM Regulation Configuration and Latest Deduplicated Qu ...
# MAGIC %skip
# MAGIC DIM_REGULATION_CFG = {
# MAGIC     "name": "dim_regulation",
# MAGIC     "target_table": f"{catalog_name}.{gold_schema_name}.dim_regulation",
# MAGIC     "business_key_col": "regulation_id",
# MAGIC     "watermark_col": "load_date",
# MAGIC     "effective_from_col": "effective_from_ts",
# MAGIC     "record_version_col": "record_version",
# MAGIC     "scd_type": "1",
# MAGIC
# MAGIC     "stage_sql": f"""
# MAGIC        WITH src AS (
# MAGIC   SELECT
# MAGIC     hu.REGULATION_ID AS regulation_id,
# MAGIC     sa.REGULATION_NUMBER AS regulation_number,
# MAGIC     sa.REGULATION_NAME AS regulation_name,
# MAGIC     sa.REGULATION_DEPARTMENT AS regulation_department,
# MAGIC     sa.REGULATION_REGION AS regulation_region,
# MAGIC     sa.REGULATION_RISK_LEVEL AS regulation_risk_level,
# MAGIC     sa.REGULATION_COMPLIANCE_STATUS AS regulation_compliance_status,
# MAGIC     sa.REGULATION_DATE_RAISED AS regulation_date_raised,
# MAGIC     sa.REGULATION_DATE_CLOSED AS regulation_date_closed,
# MAGIC     sa.REGULATION_OWNER AS regulation_owner,
# MAGIC     sa.REGULATION_DEADLINE_DATE AS regulation_deadline_date,
# MAGIC     sa.IS_REGULATION_ON_TIME AS is_regulation_on_time,
# MAGIC     sa.load_date AS watermark_col
# MAGIC   FROM {catalog_name}.{vault_schema}.HUB_REGULATION hu
# MAGIC   LEFT JOIN {catalog_name}.{vault_schema}.sat_regulation_crm sa
# MAGIC     ON hu.regulation_hash_key = sa.regulation_hash_key
# MAGIC   WHERE sa.load_date > ${{watermark_col}}
# MAGIC ),
# MAGIC dedup AS (
# MAGIC   SELECT
# MAGIC     regulation_id,
# MAGIC     regulation_number,
# MAGIC     regulation_name,
# MAGIC     regulation_department,
# MAGIC     regulation_region,
# MAGIC     regulation_risk_level,
# MAGIC     regulation_compliance_status,
# MAGIC     regulation_date_raised,
# MAGIC     regulation_date_closed,
# MAGIC     regulation_owner,
# MAGIC     regulation_deadline_date,
# MAGIC     is_regulation_on_time,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY regulation_id
# MAGIC       ORDER BY watermark_col DESC
# MAGIC     ) AS rn,
# MAGIC     watermark_col as created_ts
# MAGIC   FROM src
# MAGIC )
# MAGIC SELECT
# MAGIC   regulation_id,
# MAGIC   regulation_number,
# MAGIC   regulation_name,
# MAGIC   regulation_department,
# MAGIC   regulation_region,
# MAGIC   regulation_risk_level,
# MAGIC   regulation_compliance_status,
# MAGIC   regulation_date_raised,
# MAGIC   regulation_date_closed,
# MAGIC   regulation_owner,
# MAGIC   regulation_deadline_date,
# MAGIC   is_regulation_on_time,
# MAGIC   created_ts
# MAGIC FROM dedup
# MAGIC WHERE rn = 1
# MAGIC     """,
# MAGIC
# MAGIC     "attribute_cols": ["regulation_id", "regulation_number", "regulation_name", "regulation_department", "regulation_region", "regulation_risk_level", "regulation_compliance_status", "regulation_date_raised", "regulation_date_closed", "regulation_owner", "regulation_deadline_date", "is_regulation_on_time"
# MAGIC     ],
# MAGIC
# MAGIC     "insert_cols": [
# MAGIC         "regulation_id",
# MAGIC         "regulation_number", 
# MAGIC         "regulation_name", 
# MAGIC         "regulation_department", 
# MAGIC         "regulation_region", 
# MAGIC         "regulation_risk_level", 
# MAGIC         "regulation_compliance_status", 
# MAGIC         "regulation_date_raised", 
# MAGIC         "regulation_date_closed", 
# MAGIC         "regulation_owner", 
# MAGIC         "regulation_deadline_date", 
# MAGIC         "is_regulation_on_time",
# MAGIC         "attr_hash",
# MAGIC         "created_by",
# MAGIC         "created_ts",
# MAGIC         "last_updated_by",
# MAGIC         "last_updated_ts",
# MAGIC     ]
# MAGIC }

# COMMAND ----------

# DBTITLE 1,DIM Account Config and Latest Deduplicated Snapshot Que ...
# MAGIC %skip
# MAGIC DIM_ACCOUNT_CFG = {
# MAGIC     "name": "dim_account",
# MAGIC     "target_table": f"{catalog_name}.{gold_schema_name}.dim_account",
# MAGIC     "business_key_col": "account_id",
# MAGIC     "scd_type": "2",
# MAGIC     "watermark_col": "load_date",
# MAGIC     "effective_from_col": "effective_from_ts",
# MAGIC     "record_version_col": "record_version",
# MAGIC
# MAGIC     "stage_sql": f"""
# MAGIC         WITH src AS (
# MAGIC           SELECT
# MAGIC             ha.account_id AS account_id,
# MAGIC             sa.account_number AS account_number,
# MAGIC             sa.account_type AS account_type,
# MAGIC             sa.account_status AS account_status,
# MAGIC             sa.account_creation_type AS account_creation_type,
# MAGIC             sa.account_last_access AS account_last_access_ts,
# MAGIC             sa.account_last_change AS account_last_change_ts,
# MAGIC             COALESCE(sa.load_date, DATE('1900-01-01')) AS effective_from_ts,
# MAGIC             sa.${{watermark_col}} AS watermark_col
# MAGIC           FROM {catalog_name}.{vault_schema}.hub_account ha
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.sat_account_crm sa
# MAGIC             ON sa.account_hash_key = ha.account_hash_key
# MAGIC           WHERE sa.${{watermark_col}} > ${{watermark_col}}
# MAGIC         ),
# MAGIC         dedup AS (
# MAGIC           SELECT
# MAGIC             account_id,
# MAGIC             account_number,
# MAGIC             account_type,
# MAGIC             account_status,
# MAGIC             account_creation_type,
# MAGIC             account_last_access_ts,
# MAGIC             account_last_change_ts,
# MAGIC             effective_from_ts,
# MAGIC             watermark_col,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC               PARTITION BY account_id
# MAGIC               ORDER BY watermark_col DESC
# MAGIC             ) AS rn
# MAGIC           FROM src
# MAGIC         )
# MAGIC         SELECT
# MAGIC           account_id,
# MAGIC           account_number,
# MAGIC           account_type,
# MAGIC           account_status,
# MAGIC           account_creation_type,
# MAGIC           account_last_access_ts,
# MAGIC           account_last_change_ts,
# MAGIC           watermark_col AS effective_from_ts,
# MAGIC           TIMESTAMP '9999-12-31 00:00:00' AS effective_to_ts
# MAGIC         FROM dedup
# MAGIC         WHERE rn = 1
# MAGIC     """,
# MAGIC
# MAGIC     "attribute_cols": [
# MAGIC         "account_id",
# MAGIC         "account_number",
# MAGIC         "account_type",
# MAGIC         "account_status",
# MAGIC         "account_creation_type",
# MAGIC         "account_last_access_ts",
# MAGIC         "account_last_change_ts",
# MAGIC     ],
# MAGIC
# MAGIC     "insert_cols": [
# MAGIC         "account_id",
# MAGIC         "account_number",
# MAGIC         "account_type",
# MAGIC         "account_status",
# MAGIC         "account_creation_type",
# MAGIC         "account_last_access_ts",
# MAGIC         "account_last_change_ts",
# MAGIC         "attr_hash",
# MAGIC         "effective_from_ts",
# MAGIC         "effective_to_ts",
# MAGIC         "record_version",
# MAGIC         "created_by",
# MAGIC         "created_ts",
# MAGIC         "last_updated_by",
# MAGIC         "last_updated_ts",
# MAGIC     ]
# MAGIC }
# MAGIC

# COMMAND ----------

# DBTITLE 1,DIM Broker Configuration and Latest Effective Snapshot  ...
# MAGIC %skip
# MAGIC DIM_BROKER_CFG = {
# MAGIC     "name": "dim_broker",
# MAGIC     "target_table": f"{catalog_name}.{gold_schema_name}.dim_broker",
# MAGIC     "business_key_col": "agent_id",
# MAGIC     "scd_type": "2",
# MAGIC     "watermark_col": "load_date",
# MAGIC     "effective_from_col": "effective_from_ts",
# MAGIC     "record_version_col": "record_version",
# MAGIC
# MAGIC     "stage_sql": f"""
# MAGIC         WITH src AS (
# MAGIC           SELECT
# MAGIC             hb.agent_id AS agent_id,
# MAGIC             sb.agent_name AS agent_name,
# MAGIC             sb.agent_type AS agent_type,
# MAGIC             sb.agent_status AS agent_status,
# MAGIC             sb.agent_license_number AS agent_license_number,
# MAGIC             sb.agent_net_promoter_score AS agent_net_promoter_score,
# MAGIC             sb.agent_commission_percentage AS agent_commission_percentage,
# MAGIC             COALESCE(sb.load_date, DATE('1900-01-01')) AS effective_from_ts,
# MAGIC             sb.${{watermark_col}} AS watermark_col
# MAGIC           FROM {catalog_name}.{vault_schema}.hub_broker hb
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.sat_broker_crm sb
# MAGIC             ON hb.broker_hash_key = sb.broker_hash_key
# MAGIC           WHERE sb.${{watermark_col}} > ${{watermark_col}}
# MAGIC         ),
# MAGIC         dedup AS (
# MAGIC           SELECT
# MAGIC             agent_id,
# MAGIC             agent_name,
# MAGIC             agent_type,
# MAGIC             agent_status,
# MAGIC             agent_license_number,
# MAGIC             agent_net_promoter_score,
# MAGIC             agent_commission_percentage,
# MAGIC             effective_from_ts,
# MAGIC             watermark_col,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC               PARTITION BY agent_id
# MAGIC               ORDER BY watermark_col DESC
# MAGIC             ) AS rn
# MAGIC           FROM src
# MAGIC         )
# MAGIC         SELECT
# MAGIC           agent_id,
# MAGIC           agent_name,
# MAGIC           agent_type,
# MAGIC           agent_status,
# MAGIC           agent_license_number,
# MAGIC           agent_net_promoter_score,
# MAGIC           agent_commission_percentage,
# MAGIC           watermark_col AS effective_from_ts,
# MAGIC           TIMESTAMP '9999-12-31 00:00:00' AS effective_to_ts
# MAGIC         FROM dedup
# MAGIC         WHERE rn = 1
# MAGIC     """,
# MAGIC
# MAGIC     "attribute_cols": [
# MAGIC         "agent_id",
# MAGIC         "agent_name",
# MAGIC         "agent_type",
# MAGIC         "agent_status",
# MAGIC         "agent_license_number",
# MAGIC         "agent_net_promoter_score",
# MAGIC         "agent_commission_percentage",
# MAGIC     ],
# MAGIC
# MAGIC     "insert_cols": [
# MAGIC         "agent_id",
# MAGIC         "agent_name",
# MAGIC         "agent_type",
# MAGIC         "agent_status",
# MAGIC         "agent_license_number",
# MAGIC         "agent_net_promoter_score",
# MAGIC         "agent_commission_percentage",
# MAGIC         "attr_hash",
# MAGIC         "effective_from_ts",
# MAGIC         "effective_to_ts",
# MAGIC         "record_version",
# MAGIC         "created_by",
# MAGIC         "created_ts",
# MAGIC         "last_updated_by",
# MAGIC         "last_updated_ts",
# MAGIC     ]
# MAGIC }

# COMMAND ----------

# DBTITLE 1,DIM Claim Configuration and Latest Deduplicated Query
# MAGIC %skip
# MAGIC DIM_CLAIM_CFG = {
# MAGIC     "name": "dim_claim",
# MAGIC     "target_table": f"{catalog_name}.{gold_schema_name}.dim_claim",
# MAGIC     "business_key_col": "claim_id",
# MAGIC     "scd_type": "2",
# MAGIC     "watermark_col": "load_date",
# MAGIC     "effective_from_col": "effective_from_ts",
# MAGIC     "record_version_col": "record_version",
# MAGIC
# MAGIC     "stage_sql": f"""
# MAGIC         WITH src AS (
# MAGIC           SELECT
# MAGIC             hc.claim_id AS claim_id,
# MAGIC             sc.claim_number AS claim_number,
# MAGIC             sc.claim_type AS claim_type,
# MAGIC             sc.claim_status AS claim_status,
# MAGIC             sc.claim_reason AS claim_reason,
# MAGIC             sc.claim_channel AS claim_channel,
# MAGIC             sc.claim_handler AS claim_handler,
# MAGIC             sc.claim_reported_date AS claim_reported_date,
# MAGIC             sc.claim_settlement_date AS claim_settlement_date,
# MAGIC             sc.claim_product AS claim_product,
# MAGIC             sc.is_claim_suspicious AS is_claim_suspicious,
# MAGIC             sc.is_claim_fraud AS is_claim_fraud,
# MAGIC             sc.claim_fraud_status AS claim_fraud_status,
# MAGIC             sc.claim_fraud_type AS claim_fraud_type,
# MAGIC             sc.claim_fraud_detection_method AS claim_fraud_detection_method,
# MAGIC             sc.is_litigation AS is_litigation,
# MAGIC             sc.litigation_reason AS litigation_reason,
# MAGIC             sc.litigation_start_date AS litigation_start_date,
# MAGIC             sc.litigation_end_date AS litigation_end_date,
# MAGIC             sc.litigation_outcome AS litigation_outcome,
# MAGIC             sc.litigation_duration_days AS litigation_duration_days,
# MAGIC             sc.claim_fraud_detection_time_in_days AS claim_fraud_detection_time_in_days,
# MAGIC             sc.is_recovery_opportunity AS is_recovery_opportunity,
# MAGIC             sc.recovery_priority_score AS recovery_priority_score,
# MAGIC             sc.recovery_category AS recovery_category,
# MAGIC             sc.recovery_source AS recovery_source,
# MAGIC             sc.first_recovery_date AS first_recovery_date,
# MAGIC             sc.last_recovery_date AS last_recovery_date,
# MAGIC             sc.is_recovery_happened AS is_recovery_happened,
# MAGIC             sc.days_to_first_recovery AS days_to_first_recovery,
# MAGIC             sc.days_to_last_recovery AS days_to_last_recovery,
# MAGIC             sc.avg_days_to_close_claim AS avg_days_to_close_claim,
# MAGIC             sc.claim_fraud_outcome AS claim_fraud_outcome,
# MAGIC             sc.recovery_type AS recovery_type,
# MAGIC             sc.recovery_band AS recovery_band,
# MAGIC             sc.third_party_involved AS third_party_involved,
# MAGIC             sc.third_party_involved_overall_score AS third_party_involved_overall_score,
# MAGIC             sc.solicitor AS solicitor,
# MAGIC             sc.claim_amount AS claim_amt,
# MAGIC             sc.claims_paid AS claims_paid,
# MAGIC             sc.outstanding_reserve AS outstanding_reserve,
# MAGIC             sc.claims_expenses AS claims_expenses,
# MAGIC             sc.recovery_received AS recovery_received,
# MAGIC             sc.compensation_offered AS compensation_offered,
# MAGIC             sc.remediation_amount AS remediation_amt,
# MAGIC             sc.suspected_amount AS suspected_amt,
# MAGIC             sc.fraud_amount AS fraud_amt,
# MAGIC             sc.legal_expenses AS legal_expenses,
# MAGIC             sc.claim_band AS claim_band,
# MAGIC             sc.claim_band_sort AS claim_band_sort,
# MAGIC             sc.is_fault_claim AS is_fault_claim,
# MAGIC             sc.claim_satisfaction_score AS claim_satisfaction_score,
# MAGIC             sc.claims_feedback AS claims_feedback,
# MAGIC             COALESCE(sc.load_date, DATE('1900-01-01')) AS effective_from_ts,
# MAGIC             sc.${{watermark_col}} AS watermark_col
# MAGIC           FROM {catalog_name}.{vault_schema}.hub_claim hc
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.sat_claim_crm sc
# MAGIC             ON hc.claim_hash_key = sc.claim_hash_key
# MAGIC           WHERE sc.${{watermark_col}} > ${{watermark_col}}
# MAGIC         ),
# MAGIC         dedup AS (
# MAGIC           SELECT
# MAGIC             claim_id,
# MAGIC             claim_number,
# MAGIC             claim_type,
# MAGIC             claim_status,
# MAGIC             claim_reason,
# MAGIC             claim_channel,
# MAGIC             claim_handler,
# MAGIC             claim_reported_date,
# MAGIC             claim_settlement_date,
# MAGIC             claim_product,
# MAGIC             is_claim_suspicious,
# MAGIC             is_claim_fraud,
# MAGIC             claim_fraud_status,
# MAGIC             claim_fraud_type,
# MAGIC             claim_fraud_detection_method,
# MAGIC             is_litigation,
# MAGIC             litigation_reason,
# MAGIC             litigation_start_date,
# MAGIC             litigation_end_date,
# MAGIC             litigation_outcome,
# MAGIC             litigation_duration_days,
# MAGIC             claim_fraud_detection_time_in_days,
# MAGIC             is_recovery_opportunity,
# MAGIC             recovery_priority_score,
# MAGIC             recovery_category,
# MAGIC             recovery_source,
# MAGIC             first_recovery_date,
# MAGIC             last_recovery_date,
# MAGIC             is_recovery_happened,
# MAGIC             days_to_first_recovery,
# MAGIC             days_to_last_recovery,
# MAGIC             avg_days_to_close_claim,
# MAGIC             claim_fraud_outcome,
# MAGIC             recovery_type,
# MAGIC             recovery_band,
# MAGIC             third_party_involved,
# MAGIC             third_party_involved_overall_score,
# MAGIC             solicitor,
# MAGIC             claim_amt,
# MAGIC             claims_paid,
# MAGIC             outstanding_reserve,
# MAGIC             claims_expenses,
# MAGIC             recovery_received,
# MAGIC             compensation_offered,
# MAGIC             remediation_amt,
# MAGIC             suspected_amt,
# MAGIC             fraud_amt,
# MAGIC             legal_expenses,
# MAGIC             claim_band,
# MAGIC             claim_band_sort,
# MAGIC             is_fault_claim,
# MAGIC             claim_satisfaction_score,
# MAGIC             claims_feedback,
# MAGIC             effective_from_ts,
# MAGIC             watermark_col,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC               PARTITION BY claim_id
# MAGIC               ORDER BY watermark_col DESC
# MAGIC             ) AS rn
# MAGIC           FROM src
# MAGIC         )
# MAGIC         SELECT
# MAGIC           claim_id,
# MAGIC           claim_number,
# MAGIC           claim_type,
# MAGIC           claim_status,
# MAGIC           claim_reason,
# MAGIC           claim_channel,
# MAGIC           claim_handler,
# MAGIC           claim_reported_date,
# MAGIC           claim_settlement_date,
# MAGIC           claim_product,
# MAGIC           is_claim_suspicious,
# MAGIC           is_claim_fraud,
# MAGIC           claim_fraud_status,
# MAGIC           claim_fraud_type,
# MAGIC           claim_fraud_detection_method,
# MAGIC           is_litigation,
# MAGIC           litigation_reason,
# MAGIC           litigation_start_date,
# MAGIC           litigation_end_date,
# MAGIC           litigation_outcome,
# MAGIC           litigation_duration_days,
# MAGIC           claim_fraud_detection_time_in_days,
# MAGIC           is_recovery_opportunity,
# MAGIC           recovery_priority_score,
# MAGIC           recovery_category,
# MAGIC           recovery_source,
# MAGIC           first_recovery_date,
# MAGIC           last_recovery_date,
# MAGIC           is_recovery_happened,
# MAGIC           days_to_first_recovery,
# MAGIC           days_to_last_recovery,
# MAGIC           avg_days_to_close_claim,
# MAGIC           claim_fraud_outcome,
# MAGIC           recovery_type,
# MAGIC           recovery_band,
# MAGIC           third_party_involved,
# MAGIC           third_party_involved_overall_score,
# MAGIC           solicitor,
# MAGIC           claim_amt,
# MAGIC           claims_paid,
# MAGIC           outstanding_reserve,
# MAGIC           claims_expenses,
# MAGIC           recovery_received,
# MAGIC           compensation_offered,
# MAGIC           remediation_amt,
# MAGIC           suspected_amt,
# MAGIC           fraud_amt,
# MAGIC           legal_expenses,
# MAGIC           claim_band,
# MAGIC           claim_band_sort,
# MAGIC           is_fault_claim,
# MAGIC           claim_satisfaction_score,
# MAGIC           claims_feedback,
# MAGIC           watermark_col AS effective_from_ts,
# MAGIC           TIMESTAMP '9999-12-31 00:00:00' AS effective_to_ts
# MAGIC         FROM dedup
# MAGIC         WHERE rn = 1
# MAGIC     """,
# MAGIC
# MAGIC     "attribute_cols": [
# MAGIC         "claim_id",
# MAGIC         "claim_number",
# MAGIC         "claim_type",
# MAGIC         "claim_status",
# MAGIC         "claim_reason",
# MAGIC         "claim_channel",
# MAGIC         "claim_handler",
# MAGIC         "claim_reported_date",
# MAGIC         "claim_settlement_date",
# MAGIC         "claim_product",
# MAGIC         "is_claim_suspicious",
# MAGIC         "is_claim_fraud",
# MAGIC         "claim_fraud_status",
# MAGIC         "claim_fraud_type",
# MAGIC         "claim_fraud_detection_method",
# MAGIC         "is_litigation",
# MAGIC         "litigation_reason",
# MAGIC         "litigation_start_date",
# MAGIC         "litigation_end_date",
# MAGIC         "litigation_outcome",
# MAGIC         "litigation_duration_days",
# MAGIC         "claim_fraud_detection_time_in_days",
# MAGIC         "is_recovery_opportunity",
# MAGIC         "recovery_priority_score",
# MAGIC         "recovery_category",
# MAGIC         "recovery_source",
# MAGIC         "first_recovery_date",
# MAGIC         "last_recovery_date",
# MAGIC         "is_recovery_happened",
# MAGIC         "days_to_first_recovery",
# MAGIC         "days_to_last_recovery",
# MAGIC         "avg_days_to_close_claim",
# MAGIC         "claim_fraud_outcome",
# MAGIC         "recovery_type",
# MAGIC         "recovery_band",
# MAGIC         "third_party_involved",
# MAGIC         "third_party_involved_overall_score",
# MAGIC         "solicitor",
# MAGIC         "claim_amt",
# MAGIC         "claims_paid",
# MAGIC         "outstanding_reserve",
# MAGIC         "claims_expenses",
# MAGIC         "recovery_received",
# MAGIC         "compensation_offered",
# MAGIC         "remediation_amt",
# MAGIC         "suspected_amt",
# MAGIC         "fraud_amt",
# MAGIC         "legal_expenses",
# MAGIC         "claim_band",
# MAGIC         "claim_band_sort",
# MAGIC         "is_fault_claim",
# MAGIC         "claim_satisfaction_score",
# MAGIC         "claims_feedback",
# MAGIC     ],
# MAGIC
# MAGIC     "insert_cols": [
# MAGIC         "claim_id",
# MAGIC         "claim_number",
# MAGIC         "claim_type",
# MAGIC         "claim_status",
# MAGIC         "claim_reason",
# MAGIC         "claim_channel",
# MAGIC         "claim_handler",
# MAGIC         "claim_reported_date",
# MAGIC         "claim_settlement_date",
# MAGIC         "claim_product",
# MAGIC         "is_claim_suspicious",
# MAGIC         "is_claim_fraud",
# MAGIC         "claim_fraud_status",
# MAGIC         "claim_fraud_type",
# MAGIC         "claim_fraud_detection_method",
# MAGIC         "is_litigation",
# MAGIC         "litigation_reason",
# MAGIC         "litigation_start_date",
# MAGIC         "litigation_end_date",
# MAGIC         "litigation_outcome",
# MAGIC         "litigation_duration_days",
# MAGIC         "claim_fraud_detection_time_in_days",
# MAGIC         "is_recovery_opportunity",
# MAGIC         "recovery_priority_score",
# MAGIC         "recovery_category",
# MAGIC         "recovery_source",
# MAGIC         "first_recovery_date",
# MAGIC         "last_recovery_date",
# MAGIC         "is_recovery_happened",
# MAGIC         "days_to_first_recovery",
# MAGIC         "days_to_last_recovery",
# MAGIC         "avg_days_to_close_claim",
# MAGIC         "claim_fraud_outcome",
# MAGIC         "recovery_type",
# MAGIC         "recovery_band",
# MAGIC         "third_party_involved",
# MAGIC         "third_party_involved_overall_score",
# MAGIC         "solicitor",
# MAGIC         "claim_amt",
# MAGIC         "claims_paid",
# MAGIC         "outstanding_reserve",
# MAGIC         "claims_expenses",
# MAGIC         "recovery_received",
# MAGIC         "compensation_offered",
# MAGIC         "remediation_amt",
# MAGIC         "suspected_amt",
# MAGIC         "fraud_amt",
# MAGIC         "legal_expenses",
# MAGIC         "claim_band",
# MAGIC         "claim_band_sort",
# MAGIC         "is_fault_claim",
# MAGIC         "claim_satisfaction_score",
# MAGIC         "claims_feedback",
# MAGIC         "attr_hash",
# MAGIC         "effective_from_ts",
# MAGIC         "effective_to_ts",
# MAGIC         "record_version",
# MAGIC         "created_by",
# MAGIC         "created_ts",
# MAGIC         "last_updated_by",
# MAGIC         "last_updated_ts",
# MAGIC     ]
# MAGIC }

# COMMAND ----------

# DBTITLE 1,DIM Customer Configuration and Latest Snapshot Query
# MAGIC %skip
# MAGIC DIM_CUSTOMER_CFG = {
# MAGIC     "name": "dim_customer",
# MAGIC     "target_table": f"{catalog_name}.{gold_schema_name}.dim_customer",
# MAGIC     "business_key_col": "customer_id",
# MAGIC     "scd_type": "2",
# MAGIC     "watermark_col": "load_date",
# MAGIC     "effective_from_col": "effective_from_ts",
# MAGIC     "record_version_col": "record_version",
# MAGIC
# MAGIC     "stage_sql": f"""
# MAGIC         WITH src AS (
# MAGIC           SELECT
# MAGIC             hc.customer_id AS customer_id,
# MAGIC             sc.customer_number AS customer_number,
# MAGIC             sc.customer_rating AS customer_rating,
# MAGIC             sc.customer_segment AS customer_segment,
# MAGIC             sc.line_of_business AS line_of_business,
# MAGIC             sc.nps_score AS net_promoter_score,
# MAGIC             sc.customer_since AS customer_since_date,
# MAGIC             sc.customer_status AS customer_status_code,
# MAGIC             sc.customer_status_reason AS customer_status_reason,
# MAGIC             sc.income_band AS income_band,
# MAGIC             sc.customer_satisfaction AS customer_satisfaction,
# MAGIC             sc.customer_age_band AS customer_age_band,
# MAGIC             sc.net_promotor_code_segment AS net_promotor_code_segment,
# MAGIC 			      sc.customer_onboarding_satisfaction_score AS customer_onboarding_satisfaction_score,
# MAGIC             sc.customer_onboarding_feedback AS customer_onboarding_feedback,
# MAGIC             COALESCE(sc.load_date, DATE('1900-01-01')) AS effective_from_ts,
# MAGIC             sc.${{watermark_col}} AS watermark_col
# MAGIC           FROM {catalog_name}.{vault_schema}.hub_customer hc
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.sat_customer_crm sc
# MAGIC             ON hc.customer_hash_key = sc.customer_hash_key
# MAGIC           WHERE sc.${{watermark_col}} > ${{watermark_col}}
# MAGIC         ),
# MAGIC         dedup AS (
# MAGIC           SELECT
# MAGIC             customer_id,
# MAGIC             customer_number,
# MAGIC             customer_rating,
# MAGIC             customer_segment,
# MAGIC             line_of_business,
# MAGIC             net_promoter_score,
# MAGIC             customer_since_date,
# MAGIC             customer_status_code,
# MAGIC             customer_status_reason,
# MAGIC             income_band,
# MAGIC             customer_satisfaction,
# MAGIC             customer_age_band,
# MAGIC             net_promotor_code_segment,
# MAGIC 			      customer_onboarding_satisfaction_score,
# MAGIC             customer_onboarding_feedback,
# MAGIC             effective_from_ts,
# MAGIC             watermark_col,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC               PARTITION BY customer_id
# MAGIC               ORDER BY watermark_col DESC
# MAGIC             ) AS rn
# MAGIC           FROM src
# MAGIC         )
# MAGIC         SELECT
# MAGIC           customer_id,
# MAGIC           customer_number,
# MAGIC           customer_rating,
# MAGIC           customer_segment,
# MAGIC           line_of_business,
# MAGIC           net_promoter_score,
# MAGIC           customer_since_date,
# MAGIC           customer_status_code,
# MAGIC           customer_status_reason,
# MAGIC           income_band,
# MAGIC           customer_satisfaction,
# MAGIC           customer_age_band,
# MAGIC           net_promotor_code_segment,
# MAGIC 		      customer_onboarding_satisfaction_score,
# MAGIC           customer_onboarding_feedback,
# MAGIC           watermark_col AS effective_from_ts,
# MAGIC           TIMESTAMP '9999-12-31 00:00:00' AS effective_to_ts
# MAGIC         FROM dedup
# MAGIC         WHERE rn = 1
# MAGIC     """,
# MAGIC
# MAGIC     "attribute_cols": [
# MAGIC         "customer_id",
# MAGIC         "customer_number",
# MAGIC         "customer_rating",
# MAGIC         "customer_segment",
# MAGIC         "line_of_business",
# MAGIC         "net_promoter_score",
# MAGIC         "customer_since_date",
# MAGIC         "customer_status_code",
# MAGIC         "customer_status_reason",
# MAGIC         "income_band",
# MAGIC         "customer_satisfaction",
# MAGIC         "customer_age_band",
# MAGIC         "net_promotor_code_segment",
# MAGIC 		    "customer_onboarding_satisfaction_score",
# MAGIC         "customer_onboarding_feedback"
# MAGIC     ],
# MAGIC
# MAGIC     "insert_cols": [
# MAGIC         "customer_id",
# MAGIC         "customer_number",
# MAGIC         "customer_rating",
# MAGIC         "customer_segment",
# MAGIC         "line_of_business",
# MAGIC         "net_promoter_score",
# MAGIC         "customer_since_date",
# MAGIC         "customer_status_code",
# MAGIC         "customer_status_reason",
# MAGIC         "income_band",
# MAGIC         "customer_satisfaction",
# MAGIC         "customer_age_band",
# MAGIC         "net_promotor_code_segment",
# MAGIC 		    "customer_onboarding_satisfaction_score",
# MAGIC         "customer_onboarding_feedback",
# MAGIC         "attr_hash",
# MAGIC         "effective_from_ts",
# MAGIC         "effective_to_ts",
# MAGIC         "record_version",
# MAGIC         "created_by",
# MAGIC         "created_ts",
# MAGIC         "last_updated_by",
# MAGIC         "last_updated_ts",
# MAGIC     ]
# MAGIC }

# COMMAND ----------

# DBTITLE 1,DIM Home Configuration and Latest Effective Snapshot Qu ...
# MAGIC %skip
# MAGIC DIM_HOME_CFG = {
# MAGIC     "name": "dim_home",
# MAGIC     "target_table": f"{catalog_name}.{gold_schema_name}.dim_home",
# MAGIC     "business_key_col": "insured_object_home_id",
# MAGIC     "scd_type": "2",
# MAGIC     "watermark_col": "load_timestamp",
# MAGIC     "effective_from_col": "effective_from_ts",
# MAGIC     "record_version_col": "record_version",
# MAGIC     "stage_sql": f"""
# MAGIC         WITH src AS (
# MAGIC           SELECT
# MAGIC             dm.insured_object_sk AS insured_object_sk,
# MAGIC             bm.global_home_identifier AS insured_object_home_id,
# MAGIC             bm.is_existing_home_customer AS is_existing_home_customer,
# MAGIC             bm.home_location AS home_risk_address,
# MAGIC             bm.home_state AS home_state,
# MAGIC             bm.home_type AS home_type,
# MAGIC             bm.roof_construction_type AS roof_construction_material_type,
# MAGIC             bm.wall_construction_type AS wall_construction_material_type,
# MAGIC             COALESCE(xbm.load_timestamp, DATE('1900-01-01')) AS effective_from_ts,
# MAGIC             COALESCE(xbm.load_timestamp, DATE('1900-01-01')) AS watermark_col,
# MAGIC             TIMESTAMP '9999-12-31 00:00:00' AS effective_to_ts
# MAGIC           FROM {catalog_name}.{vault_schema}.bv_home_master bm
# MAGIC           INNER JOIN {catalog_name}.{vault_schema}.bv_home_master_xref xbm
# MAGIC             ON bm.global_home_identifier = xbm.global_home_identifier
# MAGIC           INNER JOIN {catalog_name}.{vault_schema}.hub_home hh
# MAGIC             ON xbm.rawdv_hashkey = hh.home_hash_key
# MAGIC           INNER JOIN {catalog_name}.{vault_schema}.link_insured_object_home rioh
# MAGIC             ON hh.home_hash_key = rioh.home_hash_key
# MAGIC           INNER JOIN {catalog_name}.{vault_schema}.hub_insured_object hio
# MAGIC             ON rioh.insured_object_hash_key = hio.insured_object_hash_key
# MAGIC           INNER JOIN {catalog_name}.{gold_schema_name}.dim_insured_object dm
# MAGIC             ON hio.insured_object_id = dm.insured_object_id   
# MAGIC           WHERE xbm.${{watermark_col}} > ${{watermark_col}}
# MAGIC         ),
# MAGIC         dedup AS (
# MAGIC           SELECT
# MAGIC             insured_object_sk,
# MAGIC             insured_object_home_id,
# MAGIC             is_existing_home_customer,
# MAGIC             home_risk_address,
# MAGIC             home_state,
# MAGIC             home_type,
# MAGIC             roof_construction_material_type,
# MAGIC             wall_construction_material_type,
# MAGIC             effective_from_ts,
# MAGIC             watermark_col,
# MAGIC             insured_object_home_id,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC               PARTITION BY insured_object_home_id
# MAGIC               ORDER BY watermark_col DESC
# MAGIC             ) AS rn
# MAGIC           FROM src
# MAGIC         )
# MAGIC         SELECT
# MAGIC           insured_object_sk,
# MAGIC           insured_object_home_id,
# MAGIC           is_existing_home_customer,
# MAGIC           home_risk_address,
# MAGIC           home_state,
# MAGIC           home_type,
# MAGIC           roof_construction_material_type,
# MAGIC           wall_construction_material_type,
# MAGIC           watermark_col AS effective_from_ts,
# MAGIC           TIMESTAMP '9999-12-31 00:00:00' AS effective_to_ts
# MAGIC         FROM dedup
# MAGIC         WHERE rn = 1
# MAGIC     """,
# MAGIC
# MAGIC     "attribute_cols": [
# MAGIC         "insured_object_sk",
# MAGIC         "insured_object_home_id",
# MAGIC         "is_existing_home_customer",
# MAGIC         "home_risk_address",
# MAGIC         "home_state",
# MAGIC         "home_type",
# MAGIC         "roof_construction_material_type",
# MAGIC         "wall_construction_material_type",
# MAGIC     ],
# MAGIC
# MAGIC     "insert_cols": [
# MAGIC         "insured_object_sk",
# MAGIC         "insured_object_home_id",
# MAGIC         "is_existing_home_customer",
# MAGIC         "home_risk_address",
# MAGIC         "home_state",
# MAGIC         "home_type",
# MAGIC         "roof_construction_material_type",
# MAGIC         "wall_construction_material_type",
# MAGIC         "attr_hash",
# MAGIC         "effective_from_ts",
# MAGIC         "effective_to_ts",
# MAGIC         "record_version",
# MAGIC         "created_by",
# MAGIC         "created_ts",
# MAGIC         "last_updated_by",
# MAGIC         "last_updated_ts",
# MAGIC     ]
# MAGIC }

# COMMAND ----------

# DBTITLE 1,DIM Identity Configuration and Latest Effective Snapsho ...
# MAGIC %skip
# MAGIC DIM_IDENTITY_CFG = {
# MAGIC     "name": "dim_identity",
# MAGIC     "target_table": f"{catalog_name}.{gold_schema_name}.dim_identity",
# MAGIC     "business_key_col": "identity_id",
# MAGIC     "scd_type": "2",
# MAGIC     "watermark_col": "load_date",
# MAGIC     "effective_from_col": "effective_from_ts",
# MAGIC     "record_version_col": "record_version",
# MAGIC
# MAGIC     "stage_sql": f"""
# MAGIC         WITH src AS (
# MAGIC           SELECT
# MAGIC             hi.identities_id AS identity_id,
# MAGIC             si.ecid AS experience_cloud_id,
# MAGIC             si.hashed_email AS email_address_hash_value,
# MAGIC             COALESCE(si.load_date, DATE('1900-01-01')) AS effective_from_ts,
# MAGIC             si.${{watermark_col}} AS watermark_col
# MAGIC           FROM {catalog_name}.{vault_schema}.hub_identities hi
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.sat_identities_crm si
# MAGIC             ON hi.identities_hash_key = si.identities_hash_key
# MAGIC           WHERE si.${{watermark_col}} > ${{watermark_col}}
# MAGIC         ),
# MAGIC         dedup AS (
# MAGIC           SELECT
# MAGIC             identity_id,
# MAGIC             experience_cloud_id,
# MAGIC             email_address_hash_value,
# MAGIC             effective_from_ts,
# MAGIC             watermark_col,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC               PARTITION BY identity_id
# MAGIC               ORDER BY watermark_col DESC
# MAGIC             ) AS rn
# MAGIC           FROM src
# MAGIC         )
# MAGIC         SELECT
# MAGIC           identity_id,
# MAGIC           experience_cloud_id,
# MAGIC           email_address_hash_value,
# MAGIC           watermark_col AS effective_from_ts,
# MAGIC           TIMESTAMP '9999-12-31 00:00:00' AS effective_to_ts
# MAGIC         FROM dedup
# MAGIC         WHERE rn = 1
# MAGIC     """,
# MAGIC
# MAGIC     "attribute_cols": [
# MAGIC         "identity_id",
# MAGIC         "experience_cloud_id",
# MAGIC         "email_address_hash_value",
# MAGIC     ],
# MAGIC
# MAGIC     "insert_cols": [
# MAGIC         "identity_id",
# MAGIC         "experience_cloud_id",
# MAGIC         "email_address_hash_value",
# MAGIC         "attr_hash",
# MAGIC         "effective_from_ts",
# MAGIC         "effective_to_ts",
# MAGIC         "record_version",
# MAGIC         "created_by",
# MAGIC         "created_ts",
# MAGIC         "last_updated_by",
# MAGIC         "last_updated_ts",
# MAGIC     ]
# MAGIC }

# COMMAND ----------

# DBTITLE 1,DIM Marketing Configuration and Latest Data Deduplicati ...
# MAGIC %skip
# MAGIC DIM_MARKETING_CFG = {
# MAGIC     "name": "dim_marketing",
# MAGIC     "target_table": f"{catalog_name}.{gold_schema_name}.dim_marketing",
# MAGIC     "business_key_cols": ["marketing_preference_id","marketing_engagement_id"],
# MAGIC     "scd_type": "2",
# MAGIC     "watermark_col": "load_date",
# MAGIC     "effective_from_col": "effective_from_ts",
# MAGIC     "record_version_col": "record_version",
# MAGIC
# MAGIC     "stage_sql": f"""
# MAGIC         WITH src AS (
# MAGIC           SELECT
# MAGIC             mp.marketing_preference_id AS marketing_preference_id,
# MAGIC             lp.any AS is_any_communication,
# MAGIC             ll.preferred_contact_method AS preferred_contact_method,
# MAGIC             lp.email_subscriptions AS is_email_subscriptions,
# MAGIC             lp.commercial_email AS is_commercial_email,
# MAGIC             lp.email AS is_personal_email,
# MAGIC             lp.call AS is_call,
# MAGIC             lp.sms AS is_sms,
# MAGIC             lp.postal_mail AS is_postal_mail,
# MAGIC             me.marketing_engagement_id AS marketing_engagement_id,
# MAGIC             le.opened_email AS is_opened_email,
# MAGIC             le.marketing_status AS marketing_status,
# MAGIC             le.promotion_code AS promotion_code,
# MAGIC             le.has_retention_team_interaction AS has_retention_team_interaction,
# MAGIC             le.customer_service_call_frequency AS customer_service_call_frequency,
# MAGIC             le.average_call_sentiment AS average_call_sentiment,
# MAGIC             le.engagement_score AS engagement_score,
# MAGIC 			      le.first_contact_resolution AS first_contact_resolution,
# MAGIC             greatest(
# MAGIC               COALESCE(lp.load_date, DATE('1900-01-01')),
# MAGIC               COALESCE(le.load_date, DATE('1900-01-01'))
# MAGIC             ) AS effective_from_ts,
# MAGIC             greatest(
# MAGIC               COALESCE(lp.${{watermark_col}}, DATE('1900-01-01')),
# MAGIC               COALESCE(le.${{watermark_col}}, DATE('1900-01-01'))
# MAGIC             ) AS watermark_col
# MAGIC           FROM {catalog_name}.{vault_schema}.hub_person hp
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.link_person_marketing_preference lpmp
# MAGIC             ON lpmp.person_hash_key = hp.person_hash_key
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.hub_marketing_preference mp
# MAGIC             ON mp.marketing_preference_hash_key = lpmp.marketing_preference_hash_key
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.sat_marketing_preference_crm lp
# MAGIC             ON lp.marketing_preference_hash_key = lpmp.marketing_preference_hash_key
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.link_person_lead lpl
# MAGIC             ON lpl.person_hash_key = hp.person_hash_key
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.sat_lead_crm ll
# MAGIC             ON ll.lead_hash_key = lpl.lead_hash_key
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.link_person_marketing_engagement lpme
# MAGIC             ON lpme.person_hash_key = hp.person_hash_key
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.hub_marketing_engagement me
# MAGIC             ON me.marketing_engagement_hash_key = lpme.marketing_engagement_hash_key
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.sat_marketing_engagement_crm le
# MAGIC             ON le.marketing_engagement_hash_key = lpme.marketing_engagement_hash_key
# MAGIC           WHERE greatest(
# MAGIC                   COALESCE(lp.${{watermark_col}}, DATE('1900-01-01')),
# MAGIC                   COALESCE(le.${{watermark_col}}, DATE('1900-01-01'))
# MAGIC                 ) > ${{watermark_col}}
# MAGIC         ),
# MAGIC         business_key_cte AS (
# MAGIC           SELECT
# MAGIC             marketing_preference_id,
# MAGIC             marketing_engagement_id,
# MAGIC             is_any_communication,
# MAGIC             preferred_contact_method,
# MAGIC             is_email_subscriptions,
# MAGIC             is_commercial_email,
# MAGIC             is_personal_email,
# MAGIC             is_call,
# MAGIC             is_sms,
# MAGIC             is_postal_mail,
# MAGIC             is_opened_email,
# MAGIC             marketing_status,
# MAGIC             promotion_code,
# MAGIC             has_retention_team_interaction,
# MAGIC             customer_service_call_frequency,
# MAGIC             average_call_sentiment,
# MAGIC 			      first_contact_resolution,
# MAGIC             engagement_score,
# MAGIC             effective_from_ts,
# MAGIC             watermark_col
# MAGIC           FROM src
# MAGIC         ),
# MAGIC         dedup AS (
# MAGIC           SELECT
# MAGIC             marketing_preference_id,
# MAGIC             marketing_engagement_id,
# MAGIC             is_any_communication,
# MAGIC             preferred_contact_method,
# MAGIC             is_email_subscriptions,
# MAGIC             is_commercial_email,
# MAGIC             is_personal_email,
# MAGIC             is_call,
# MAGIC             is_sms,
# MAGIC             is_postal_mail,
# MAGIC             is_opened_email,
# MAGIC             marketing_status,
# MAGIC             promotion_code,
# MAGIC             has_retention_team_interaction,
# MAGIC             customer_service_call_frequency,
# MAGIC             average_call_sentiment,
# MAGIC             engagement_score,
# MAGIC 			first_contact_resolution,
# MAGIC             effective_from_ts,
# MAGIC             watermark_col,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC               PARTITION BY marketing_preference_id, marketing_engagement_id
# MAGIC               ORDER BY watermark_col DESC
# MAGIC             ) AS rn
# MAGIC           FROM business_key_cte
# MAGIC         )
# MAGIC         SELECT
# MAGIC           marketing_preference_id,
# MAGIC           marketing_engagement_id,
# MAGIC           is_any_communication,
# MAGIC           preferred_contact_method,
# MAGIC           is_email_subscriptions,
# MAGIC           is_commercial_email,
# MAGIC           is_personal_email,
# MAGIC           is_call,
# MAGIC           is_sms,
# MAGIC           is_postal_mail,
# MAGIC           is_opened_email,
# MAGIC           marketing_status,
# MAGIC           promotion_code,
# MAGIC           has_retention_team_interaction,
# MAGIC           customer_service_call_frequency,
# MAGIC           average_call_sentiment,
# MAGIC           engagement_score,
# MAGIC 		      first_contact_resolution,
# MAGIC           watermark_col AS effective_from_ts,
# MAGIC           TIMESTAMP '9999-12-31 00:00:00' AS effective_to_ts
# MAGIC         FROM dedup
# MAGIC         WHERE rn = 1
# MAGIC     """,
# MAGIC
# MAGIC     "attribute_cols": [
# MAGIC         "marketing_preference_id",
# MAGIC         "is_any_communication",
# MAGIC         "preferred_contact_method",
# MAGIC         "is_email_subscriptions",
# MAGIC         "is_commercial_email",
# MAGIC         "is_personal_email",
# MAGIC         "is_call",
# MAGIC         "is_sms",
# MAGIC         "is_postal_mail",
# MAGIC         "marketing_engagement_id",
# MAGIC         "is_opened_email",
# MAGIC         "marketing_status",
# MAGIC         "promotion_code",
# MAGIC         "has_retention_team_interaction",
# MAGIC         "customer_service_call_frequency",
# MAGIC         "average_call_sentiment",
# MAGIC         "engagement_score",
# MAGIC 		    "first_contact_resolution"
# MAGIC     ],
# MAGIC
# MAGIC     "insert_cols": [
# MAGIC         "marketing_preference_id",
# MAGIC         "is_any_communication",
# MAGIC         "preferred_contact_method",
# MAGIC         "is_email_subscriptions",
# MAGIC         "is_commercial_email",
# MAGIC         "is_personal_email",
# MAGIC         "is_call",
# MAGIC         "is_sms",
# MAGIC         "is_postal_mail",
# MAGIC         "marketing_engagement_id",
# MAGIC         "is_opened_email",
# MAGIC         "marketing_status",
# MAGIC         "promotion_code",
# MAGIC         "has_retention_team_interaction",
# MAGIC         "customer_service_call_frequency",
# MAGIC         "average_call_sentiment",
# MAGIC         "engagement_score",
# MAGIC 		    "first_contact_resolution",
# MAGIC         "attr_hash",
# MAGIC         "effective_from_ts",
# MAGIC         "effective_to_ts",
# MAGIC         "record_version",
# MAGIC         "created_by",
# MAGIC         "created_ts",
# MAGIC         "last_updated_by",
# MAGIC         "last_updated_ts",
# MAGIC     ]
# MAGIC }

# COMMAND ----------

# DBTITLE 1,DIM Motor Configuration and Latest Deduplicated Query
# MAGIC %skip
# MAGIC DIM_MOTOR_CFG = {
# MAGIC     "name": "dim_motor",
# MAGIC     "target_table": f"{catalog_name}.{gold_schema_name}.dim_motor",
# MAGIC     "business_key_col": "insured_object_motor_id",
# MAGIC     "scd_type": "2",
# MAGIC     "watermark_col": "load_timestamp",
# MAGIC     "effective_from_col": "effective_from_ts",
# MAGIC     "record_version_col": "record_version",
# MAGIC
# MAGIC     "stage_sql": f"""
# MAGIC         WITH src AS (
# MAGIC           SELECT
# MAGIC             dm.insured_object_sk AS insured_object_sk,
# MAGIC             bm.global_motor_identifier AS insured_object_motor_id,
# MAGIC             sm.auto_decline_vehicle AS is_auto_decline_vehicle,
# MAGIC             bm.is_existing_motor_customer AS is_existing_motor_customer,
# MAGIC             bm.motor_lapsed_policies AS motor_lapsed_policies,
# MAGIC             bm.motor_sum_insured AS vehicle_sum_insured_amt,
# MAGIC             bm.motor_risk_class_code AS vehicle_risk_class_code,
# MAGIC             bm.motor_parked_location AS vehicle_risk_address,
# MAGIC             bm.body_type AS vehicle_body_type,
# MAGIC             bm.fuel_type AS vehicle_fuel_type,
# MAGIC             bm.motor_variant AS vehicle_variant,
# MAGIC             bm.motor_age AS vehicle_age,
# MAGIC             bm.motor_class AS vehicle_class,
# MAGIC             bm.motor_model AS vehicle_model,
# MAGIC             bm.motor_owner_type AS vehicle_owner_type,
# MAGIC             bm.motor_registration_state AS vehicle_reg_state,
# MAGIC             bm.motor_type AS vehicle_type,
# MAGIC             CAST(YEAR(bm.motor_manufacturing_date) AS INT) AS vehicle_year,
# MAGIC             bm.license_status AS driver_license_status,
# MAGIC             bm.driver_experience_years AS driver_experience_years,
# MAGIC             COALESCE(xbm.load_timestamp, DATE('1900-01-01')) AS effective_from_ts,
# MAGIC             xbm.${{watermark_col}} AS watermark_col,
# MAGIC             TIMESTAMP '9999-12-31 00:00:00' AS effective_to_ts
# MAGIC           FROM {catalog_name}.{vault_schema}.bv_motor_master bm
# MAGIC           INNER JOIN {catalog_name}.{vault_schema}.bv_motor_master_xref xbm
# MAGIC             ON bm.global_motor_identifier = xbm.global_motor_identifier
# MAGIC           INNER JOIN {catalog_name}.{vault_schema}.hub_motor hm
# MAGIC             ON xbm.rawdv_hashkey = hm.motor_hash_key
# MAGIC           INNER JOIN {catalog_name}.{vault_schema}.sat_motor_crm sm
# MAGIC             ON hm.motor_hash_key = sm.motor_hash_key
# MAGIC           INNER JOIN {catalog_name}.{vault_schema}.link_insured_object_motor riom
# MAGIC             ON riom.motor_hash_key = hm.motor_hash_key
# MAGIC           INNER JOIN {catalog_name}.{vault_schema}.hub_insured_object hio
# MAGIC             ON hio.insured_object_hash_key = riom.insured_object_hash_key
# MAGIC           INNER JOIN {catalog_name}.{gold_schema_name}.dim_insured_object dm
# MAGIC             ON dm.insured_object_id = hio.insured_object_id
# MAGIC           WHERE xbm.${{watermark_col}} > ${{watermark_col}}
# MAGIC         ),
# MAGIC         dedup AS (
# MAGIC           SELECT
# MAGIC             insured_object_sk,
# MAGIC             insured_object_motor_id,
# MAGIC             is_auto_decline_vehicle,
# MAGIC             is_existing_motor_customer,
# MAGIC             motor_lapsed_policies,
# MAGIC             vehicle_sum_insured_amt,
# MAGIC             vehicle_risk_class_code,
# MAGIC             vehicle_risk_address,
# MAGIC             vehicle_body_type,
# MAGIC             vehicle_fuel_type,
# MAGIC             vehicle_variant,
# MAGIC             vehicle_age,
# MAGIC             vehicle_class,
# MAGIC             vehicle_model,
# MAGIC             vehicle_owner_type,
# MAGIC             vehicle_reg_state,
# MAGIC             vehicle_type,
# MAGIC             vehicle_year,
# MAGIC             driver_license_status,
# MAGIC             driver_experience_years,
# MAGIC             effective_from_ts,
# MAGIC             watermark_col,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC               PARTITION BY insured_object_motor_id
# MAGIC               ORDER BY watermark_col DESC
# MAGIC             ) AS rn
# MAGIC           FROM src
# MAGIC         )
# MAGIC         SELECT
# MAGIC           insured_object_sk,
# MAGIC           insured_object_motor_id,
# MAGIC           is_auto_decline_vehicle,
# MAGIC           is_existing_motor_customer,
# MAGIC           motor_lapsed_policies,
# MAGIC           vehicle_sum_insured_amt,
# MAGIC           vehicle_risk_class_code,
# MAGIC           vehicle_risk_address,
# MAGIC           vehicle_body_type,
# MAGIC           vehicle_fuel_type,
# MAGIC           vehicle_variant,
# MAGIC           vehicle_age,
# MAGIC           vehicle_class,
# MAGIC           vehicle_model,
# MAGIC           vehicle_owner_type,
# MAGIC           vehicle_reg_state,
# MAGIC           vehicle_type,
# MAGIC           vehicle_year,
# MAGIC           driver_license_status,
# MAGIC           driver_experience_years,
# MAGIC           watermark_col AS effective_from_ts,
# MAGIC           TIMESTAMP '9999-12-31 00:00:00' AS effective_to_ts
# MAGIC         FROM dedup
# MAGIC         WHERE rn = 1
# MAGIC     """,
# MAGIC
# MAGIC     "attribute_cols": [
# MAGIC         "insured_object_sk",
# MAGIC         "insured_object_motor_id",
# MAGIC         "is_auto_decline_vehicle",
# MAGIC         "is_existing_motor_customer",
# MAGIC         "motor_lapsed_policies",
# MAGIC         "vehicle_sum_insured_amt",
# MAGIC         "vehicle_risk_class_code",
# MAGIC         "vehicle_risk_address",
# MAGIC         "vehicle_body_type",
# MAGIC         "vehicle_fuel_type",
# MAGIC         "vehicle_variant",
# MAGIC         "vehicle_age",
# MAGIC         "vehicle_class",
# MAGIC         "vehicle_model",
# MAGIC         "vehicle_owner_type",
# MAGIC         "vehicle_reg_state",
# MAGIC         "vehicle_type",
# MAGIC         "vehicle_year",
# MAGIC         "driver_license_status",
# MAGIC         "driver_experience_years",
# MAGIC     ],
# MAGIC
# MAGIC     "insert_cols": [
# MAGIC         "insured_object_sk",
# MAGIC         "insured_object_motor_id",
# MAGIC         "is_auto_decline_vehicle",
# MAGIC         "is_existing_motor_customer",
# MAGIC         "motor_lapsed_policies",
# MAGIC         "vehicle_sum_insured_amt",
# MAGIC         "vehicle_risk_class_code",
# MAGIC         "vehicle_risk_address",
# MAGIC         "vehicle_body_type",
# MAGIC         "vehicle_fuel_type",
# MAGIC         "vehicle_variant",
# MAGIC         "vehicle_age",
# MAGIC         "vehicle_class",
# MAGIC         "vehicle_model",
# MAGIC         "vehicle_owner_type",
# MAGIC         "vehicle_reg_state",
# MAGIC         "vehicle_type",
# MAGIC         "vehicle_year",
# MAGIC         "driver_license_status",
# MAGIC         "driver_experience_years",
# MAGIC         "attr_hash",
# MAGIC         "effective_from_ts",
# MAGIC         "effective_to_ts",
# MAGIC         "record_version",
# MAGIC         "created_by",
# MAGIC         "created_ts",
# MAGIC         "last_updated_by",
# MAGIC         "last_updated_ts",
# MAGIC     ]
# MAGIC }

# COMMAND ----------

# DBTITLE 1,DIM Person Configuration and Latest Snapshot Query
# MAGIC %skip
# MAGIC DIM_PERSON_CFG = {
# MAGIC     "name": "dim_person",
# MAGIC     "target_table": f"{catalog_name}.{gold_schema_name}.dim_person",
# MAGIC     "business_key_col": "person_id",
# MAGIC     "scd_type": "2",
# MAGIC     "watermark_col": "load_date",
# MAGIC     "effective_from_col": "effective_from_ts",
# MAGIC     "record_version_col": "record_version",
# MAGIC
# MAGIC     "stage_sql": f"""
# MAGIC WITH natural_person AS (
# MAGIC     SELECT
# MAGIC         np.global_person_identifier AS person_id,
# MAGIC         dg.geography_sk,
# MAGIC         di.identity_sk,
# MAGIC         np.courtesy_title,
# MAGIC         np.first_name,
# MAGIC         np.last_name,
# MAGIC         np.full_name,
# MAGIC         np.person_type,
# MAGIC         np.date_of_birth AS birth_date,
# MAGIC         np.gender,
# MAGIC         np.nationality,
# MAGIC         np.marital_status,
# MAGIC         np.occupation,
# MAGIC         adr.global_address_identifier AS address_id,
# MAGIC         adr.address_type,
# MAGIC         adr.address_line_2 AS street_address,
# MAGIC         adr.postal_code AS postcode,
# MAGIC         hc.contact_id,
# MAGIC         np.home_phone AS home_phone_number,
# MAGIC         np.work_phone AS work_phone_number,
# MAGIC         np.personal_email,
# MAGIC         np.work_email,
# MAGIC         np.job_title,
# MAGIC         np.role,
# MAGIC         CAST(NULL AS STRING) AS company_name,
# MAGIC         CAST(NULL AS DATE) AS date_of_constitution,
# MAGIC         np.is_lead,
# MAGIC         np.preferred_language,
# MAGIC         np.source_identifier AS source_id,
# MAGIC         np.source_type,
# MAGIC         np.tenant_identifier AS tenant_id,
# MAGIC         np.assesed_disability_degree AS assessed_disability_degree,
# MAGIC         np.operational_paperless_consent AS is_operational_paperless_consent,
# MAGIC         hco.consent_id,
# MAGIC         sc.opt_in_legitimate_interest AS is_opt_in_legitimate_interest,
# MAGIC         sc.opt_in_validated AS is_opt_in_validated,
# MAGIC         CAST(np.effective_from AS TIMESTAMP) AS effective_from_ts,
# MAGIC         CAST(np.effective_to AS TIMESTAMP) AS effective_to_ts,
# MAGIC         CAST(1 AS INT) AS record_version,
# MAGIC         GREATEST(   
# MAGIC             COALESCE(lpi.${{watermark_col}}, TIMESTAMP('1900-01-01')),   
# MAGIC             COALESCE(lpc.${{watermark_col}}, TIMESTAMP('1900-01-01')),
# MAGIC             COALESCE(lpco.${{watermark_col}}, TIMESTAMP('1900-01-01')),
# MAGIC             COALESCE(sc.${{watermark_col}}, TIMESTAMP('1900-01-01'))) AS watermark_col,
# MAGIC         GREATEST(   
# MAGIC             COALESCE(lpi.${{watermark_col}}, TIMESTAMP('1900-01-01')),   
# MAGIC             COALESCE(lpc.${{watermark_col}}, TIMESTAMP('1900-01-01')),
# MAGIC             COALESCE(lpco.${{watermark_col}}, TIMESTAMP('1900-01-01')),
# MAGIC             COALESCE(sc.${{watermark_col}}, TIMESTAMP('1900-01-01'))) AS effective_from_ts
# MAGIC     FROM {catalog_name}.{vault_schema}.bv_natural_person_master np
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.hub_person hp_crm ON hp_crm.person_id = trim(split(np.global_person_identifier, '\\\\|\\\\|')[0])
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.hub_person hp_sap ON hp_sap.person_id = trim(split(np.global_person_identifier, '\\\\|\\\\|')[1])
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.bv_address_master adr ON adr.global_person_identifier = np.global_person_identifier
# MAGIC     LEFT JOIN {catalog_name}.{gold_schema_name}.dim_geography dg
# MAGIC         ON coalesce(dg.city, '') = coalesce(adr.city, '')
# MAGIC        AND coalesce(dg.state, '') = coalesce(adr.state, '')
# MAGIC        AND coalesce(dg.country, '') = coalesce(adr.country, '')
# MAGIC        AND coalesce(dg.region, '') = coalesce(adr.region, '')
# MAGIC     
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.link_person_identities lpi on lpi.person_hash_key = hp_crm.person_hash_key
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.hub_identities hi ON hi.identities_hash_key = lpi.identities_hash_key
# MAGIC     LEFT JOIN {catalog_name}.{gold_schema_name}.dim_identity di ON di.identity_id = hi.identities_id
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.link_person_contact lpc ON lpc.person_hash_key = hp_crm.person_hash_key
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.hub_contact hc ON hc.contact_hash_key = lpc.contact_hash_key
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.link_person_consent lpco ON lpco.person_hash_key = hp_crm.person_hash_key
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.hub_consent hco ON hco.consent_hash_key = lpco.consent_hash_key
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.sat_consent_crm sc ON sc.consent_hash_key = hco.consent_hash_key
# MAGIC ),
# MAGIC
# MAGIC legal_person AS (
# MAGIC     SELECT
# MAGIC         lp.global_person_identifier AS person_id,
# MAGIC         dg.geography_sk,
# MAGIC         di.identity_sk,
# MAGIC         CAST(NULL AS STRING) AS courtesy_title,
# MAGIC         CAST(NULL AS STRING) AS first_name,
# MAGIC         CAST(NULL AS STRING) AS last_name,
# MAGIC         lp.organization AS full_name,
# MAGIC         lp.person_type,
# MAGIC         CAST(NULL AS DATE) AS birth_date,
# MAGIC         CAST(NULL AS STRING) AS gender,
# MAGIC         CAST(NULL AS STRING) AS nationality,
# MAGIC         CAST(NULL AS STRING) AS marital_status,
# MAGIC         CAST(NULL AS STRING) AS occupation,
# MAGIC         adr.global_address_identifier AS home_address_id,
# MAGIC         adr.address_type,
# MAGIC         adr.address_line_2 AS street_address,
# MAGIC         adr.postal_code AS postcode,
# MAGIC         hc.contact_id,
# MAGIC         CAST(NULL AS STRING) AS home_phone_number,
# MAGIC         lp.phone_number AS work_phone_number,
# MAGIC         CAST(NULL AS STRING) AS personal_email,
# MAGIC         lp.email_address AS work_email,
# MAGIC         CAST(NULL AS STRING) AS job_title,
# MAGIC         CAST(NULL AS STRING) AS role,
# MAGIC         lp.organization AS company_name,
# MAGIC         lp.org_establishment_date AS date_of_constitution,
# MAGIC         lp.is_lead,
# MAGIC         CAST(NULL AS STRING) AS preferred_language,
# MAGIC         lp.source_identifier AS source_id,
# MAGIC         lp.source_type,
# MAGIC         lp.tenant_identifier AS tenant_id,
# MAGIC         CAST(NULL AS STRING) AS assessed_disability_degree,
# MAGIC         lp.operational_paperless_consent AS is_operational_paperless_consent,
# MAGIC         hco.consent_id,
# MAGIC         sc.opt_in_legitimate_interest AS is_opt_in_legitimate_interest,
# MAGIC         sc.opt_in_validated AS is_opt_in_validated,
# MAGIC         CAST(lp.effective_from AS TIMESTAMP) AS effective_from_ts,
# MAGIC         CAST(lp.effective_to AS TIMESTAMP) AS effective_to_ts,
# MAGIC         CAST(1 AS INT) AS record_version,
# MAGIC         GREATEST(   
# MAGIC             COALESCE(lpi.${{watermark_col}}, TIMESTAMP('1900-01-01')),   
# MAGIC             COALESCE(lpc.${{watermark_col}}, TIMESTAMP('1900-01-01')),
# MAGIC             COALESCE(lpco.${{watermark_col}}, TIMESTAMP('1900-01-01')),
# MAGIC             COALESCE(sc.${{watermark_col}}, TIMESTAMP('1900-01-01'))) AS watermark_col,
# MAGIC         GREATEST(   
# MAGIC             COALESCE(lpi.${{watermark_col}}, TIMESTAMP('1900-01-01')),   
# MAGIC             COALESCE(lpc.${{watermark_col}}, TIMESTAMP('1900-01-01')),
# MAGIC             COALESCE(lpco.${{watermark_col}}, TIMESTAMP('1900-01-01')),
# MAGIC             COALESCE(sc.${{watermark_col}}, TIMESTAMP('1900-01-01'))) AS effective_from_ts
# MAGIC     FROM {catalog_name}.{vault_schema}.bv_legal_person_master lp
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.hub_person hp_crm ON hp_crm.person_id = trim(split(lp.global_person_identifier, '\\\\|\\\\|')[0])
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.hub_person hp_sap ON hp_sap.person_id = trim(split(lp.global_person_identifier, '\\\\|\\\\|')[1])
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.bv_address_master adr ON adr.global_person_identifier = lp.global_person_identifier
# MAGIC     LEFT JOIN {catalog_name}.{gold_schema_name}.dim_geography dg
# MAGIC         ON coalesce(dg.city, '') = coalesce(adr.city, '')
# MAGIC        AND coalesce(dg.state, '') = coalesce(adr.state, '')
# MAGIC        AND coalesce(dg.country, '') = coalesce(adr.country, '')
# MAGIC        AND coalesce(dg.region, '') = coalesce(adr.region, '')
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.link_person_identities lpi on lpi.person_hash_key = hp_crm.person_hash_key
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.hub_identities hi ON hi.identities_hash_key = lpi.identities_hash_key
# MAGIC     LEFT JOIN {catalog_name}.{gold_schema_name}.dim_identity di ON di.identity_id = hi.identities_id
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.link_person_contact lpc ON lpc.person_hash_key = hp_crm.person_hash_key
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.hub_contact hc ON hc.contact_hash_key = lpc.contact_hash_key
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.link_person_consent lpco ON lpco.person_hash_key = hp_crm.person_hash_key
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.hub_consent hco ON hco.consent_hash_key = lpco.consent_hash_key
# MAGIC     LEFT JOIN {catalog_name}.{vault_schema}.sat_consent_crm sc ON sc.consent_hash_key = hco.consent_hash_key
# MAGIC ),
# MAGIC combined as (
# MAGIC     SELECT * FROM natural_person
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM legal_person
# MAGIC ),
# MAGIC deduped AS (
# MAGIC         SELECT
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY person_id
# MAGIC                 ORDER BY watermark_col DESC
# MAGIC             ) AS rn
# MAGIC         FROM combined
# MAGIC     )
# MAGIC SELECT 
# MAGIC     geography_sk,
# MAGIC     identity_sk,
# MAGIC     person_id,
# MAGIC     courtesy_title,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     full_name,
# MAGIC     person_type,
# MAGIC     birth_date,
# MAGIC     gender,
# MAGIC     nationality,
# MAGIC     marital_status,
# MAGIC     occupation,
# MAGIC     address_id,
# MAGIC     address_type,
# MAGIC     street_address,
# MAGIC     postcode,
# MAGIC     contact_id,
# MAGIC     home_phone_number,
# MAGIC     work_phone_number,
# MAGIC     personal_email,
# MAGIC     work_email,
# MAGIC     job_title,
# MAGIC     role,
# MAGIC     company_name,
# MAGIC     date_of_constitution,
# MAGIC     is_lead,
# MAGIC     preferred_language,
# MAGIC     source_id,
# MAGIC     source_type,
# MAGIC     tenant_id,
# MAGIC     assessed_disability_degree,
# MAGIC     is_operational_paperless_consent,
# MAGIC     consent_id,
# MAGIC     is_opt_in_legitimate_interest,
# MAGIC     is_opt_in_validated,
# MAGIC     watermark_col AS effective_from_ts,
# MAGIC     TIMESTAMP '9999-12-31 00:00:00' AS effective_to_ts
# MAGIC     from deduped
# MAGIC     WHERE rn = 1 and watermark_col > ${{watermark_col}};
# MAGIC     """,
# MAGIC
# MAGIC     "attribute_cols": [
# MAGIC         "geography_sk",
# MAGIC         "identity_sk",
# MAGIC         "person_id",
# MAGIC         "courtesy_title",
# MAGIC         "first_name",
# MAGIC         "last_name",
# MAGIC         "full_name",
# MAGIC         "person_type",
# MAGIC         "birth_date",
# MAGIC         "gender",
# MAGIC         "nationality",
# MAGIC         "marital_status",
# MAGIC         "occupation",
# MAGIC         "address_id",
# MAGIC         "address_type",
# MAGIC         "street_address",
# MAGIC         "postcode",
# MAGIC         "contact_id",
# MAGIC         "home_phone_number",
# MAGIC         "work_phone_number",
# MAGIC         "personal_email",
# MAGIC         "work_email",
# MAGIC         "job_title",
# MAGIC         "role",
# MAGIC         "company_name",
# MAGIC         "date_of_constitution",
# MAGIC         "preferred_language",
# MAGIC         "assessed_disability_degree",
# MAGIC         "consent_id",
# MAGIC         "source_id",
# MAGIC 		"source_type",
# MAGIC         "tenant_id",
# MAGIC         "is_lead",
# MAGIC         "is_operational_paperless_consent",
# MAGIC         "is_opt_in_legitimate_interest",
# MAGIC         "is_opt_in_validated"  
# MAGIC     ],
# MAGIC
# MAGIC     "insert_cols": [
# MAGIC         "geography_sk",
# MAGIC         "identity_sk",
# MAGIC         "person_id",
# MAGIC         "courtesy_title",
# MAGIC         "first_name",
# MAGIC         "last_name",
# MAGIC         "full_name",
# MAGIC         "person_type",
# MAGIC         "birth_date",
# MAGIC         "gender",
# MAGIC         "nationality",
# MAGIC         "marital_status",
# MAGIC         "occupation",
# MAGIC         "address_id",
# MAGIC         "address_type",
# MAGIC         "street_address",
# MAGIC         "postcode",
# MAGIC         "contact_id",
# MAGIC         "home_phone_number",
# MAGIC         "work_phone_number",
# MAGIC         "personal_email",
# MAGIC         "work_email",
# MAGIC         "job_title",
# MAGIC         "role",
# MAGIC         "company_name",
# MAGIC         "date_of_constitution",
# MAGIC         "preferred_language",
# MAGIC         "assessed_disability_degree",
# MAGIC         "consent_id",
# MAGIC         "source_id",
# MAGIC 		"source_type",
# MAGIC         "tenant_id",
# MAGIC         "is_lead",
# MAGIC         "is_operational_paperless_consent",
# MAGIC         "is_opt_in_legitimate_interest",
# MAGIC         "is_opt_in_validated",
# MAGIC         "attr_hash",
# MAGIC         "effective_from_ts",
# MAGIC         "effective_to_ts",
# MAGIC         "record_version",
# MAGIC         "created_by",
# MAGIC         "created_ts",
# MAGIC         "last_updated_by",
# MAGIC         "last_updated_ts",
# MAGIC     ]
# MAGIC }
# MAGIC

# COMMAND ----------

# DBTITLE 1,DIM Policy Configuration and Latest Snapshot Analysis
# MAGIC %skip
# MAGIC DIM_POLICY_CFG = {
# MAGIC     "name": "dim_policy",
# MAGIC     "target_table": f"{catalog_name}.{gold_schema_name}.dim_policy",
# MAGIC     "business_key_col": "policy_id",
# MAGIC     "scd_type": "2",
# MAGIC     "watermark_col": "load_date",
# MAGIC     "effective_from_col": "effective_from_ts",
# MAGIC     "record_version_col": "record_version",
# MAGIC
# MAGIC     "stage_sql": f"""
# MAGIC         WITH src AS (
# MAGIC           SELECT
# MAGIC             hp.policy_id AS policy_id,
# MAGIC             sp.policy_number AS policy_number,
# MAGIC             sp.policy_start_date AS policy_start_ts,
# MAGIC             sp.policy_end_date AS policy_end_ts,
# MAGIC             sp.policy_length AS policy_tenure,
# MAGIC             sp.policy_cycle AS policy_cycle,
# MAGIC             sp.renewal_date AS renewal_date,
# MAGIC             sp.policy_status AS policy_status,
# MAGIC             sp.cover_option AS policy_cover_option,
# MAGIC             sp.sales_channel AS policy_sales_channel,
# MAGIC             sp.fraud_flag AS is_fraud,
# MAGIC             hq.quote_id AS quote_id,
# MAGIC             sp.policy_type AS policy_type,
# MAGIC             sp.policy_issue_date AS policy_issue_date,
# MAGIC             sp.is_policy_renewal AS is_policy_renewal,
# MAGIC             sp.policy_cancellation_reason AS policy_cancellation_reason,
# MAGIC             sp.policy_sum_insured AS policy_sum_insured,
# MAGIC             sp.policy_retention_limit AS policy_retention_limit,
# MAGIC             sp.policy_risk_score AS policy_risk_score,
# MAGIC             sp.policy_risk_band AS policy_risk_band,
# MAGIC             sp.is_auto_renew_enabled AS is_auto_renew_enabled,
# MAGIC             sp.no_claims_discount_years AS no_claims_discount_years,
# MAGIC             sp.payment_method AS payment_method,
# MAGIC             sp.is_direct_debit_cancellation AS is_direct_debit_cancellation,
# MAGIC             sp.missed_payment_count AS missed_payment_count,
# MAGIC             sp.loyalty_discount_usage AS loyalty_discount_usage,
# MAGIC             sp.is_installment_default AS is_installment_default,
# MAGIC             dc.channel_sk AS channel_sk,
# MAGIC             dp.product_sk AS product_sk,
# MAGIC             doi.insured_object_sk AS insured_object_sk,
# MAGIC             COALESCE(sp.load_date, DATE('1900-01-01')) AS effective_from_ts,
# MAGIC             sp.${{watermark_col}} AS watermark_col
# MAGIC           FROM {catalog_name}.{vault_schema}.hub_policy hp
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.sat_policy_crm sp
# MAGIC             ON hp.policy_hash_key = sp.policy_hash_key
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.link_policy_channel rpc
# MAGIC             ON rpc.policy_hash_key = hp.policy_hash_key
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.hub_channel hc
# MAGIC             ON hc.channel_hash_key = rpc.channel_hash_key
# MAGIC           LEFT JOIN {catalog_name}.{gold_schema_name}.dim_channel dc
# MAGIC             ON dc.channel_id = hc.channel_id
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.link_policy_product lpp
# MAGIC             ON lpp.policy_hash_key = hp.policy_hash_key
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.hub_product hpr
# MAGIC             ON hpr.product_hash_key = lpp.product_hash_key
# MAGIC
# MAGIC           LEFT JOIN dev_allianz_raw.bvault_gold.dim_product dp
# MAGIC             on hpr.product_id = trim(split(dp.product_id, '\\\\|\\\\|')[0]) 
# MAGIC             or hpr.product_id = trim(split(dp.product_id, '\\\\|\\\\|')[1])
# MAGIC
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.link_policy_insured_object lio
# MAGIC             ON lio.policy_hash_key = hp.policy_hash_key
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.hub_insured_object hi
# MAGIC             ON hi.insured_object_hash_key = lio.insured_object_hash_key
# MAGIC           LEFT JOIN {catalog_name}.{gold_schema_name}.dim_insured_object doi
# MAGIC             ON doi.insured_object_id = hi.insured_object_id
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.link_policy_quote lq
# MAGIC             ON hp.policy_hash_key = lq.policy_hash_key
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.hub_quote hq
# MAGIC             ON hq.quote_hash_key = lq.quote_hash_key
# MAGIC           WHERE sp.${{watermark_col}} > ${{watermark_col}}
# MAGIC         ),
# MAGIC         dedup AS (
# MAGIC           SELECT
# MAGIC             policy_id,
# MAGIC             policy_number,
# MAGIC             policy_start_ts,
# MAGIC             policy_end_ts,
# MAGIC             policy_tenure,
# MAGIC             policy_cycle,
# MAGIC             renewal_date,
# MAGIC             policy_status,
# MAGIC             policy_cover_option,
# MAGIC             policy_sales_channel,
# MAGIC             is_fraud,
# MAGIC             quote_id,
# MAGIC             policy_type,
# MAGIC             policy_issue_date,
# MAGIC             is_policy_renewal,
# MAGIC             policy_cancellation_reason,
# MAGIC             policy_sum_insured,
# MAGIC             policy_retention_limit,
# MAGIC             policy_risk_score,
# MAGIC             policy_risk_band,
# MAGIC             is_auto_renew_enabled,
# MAGIC             no_claims_discount_years,
# MAGIC             payment_method,
# MAGIC             is_direct_debit_cancellation,
# MAGIC             missed_payment_count,
# MAGIC             loyalty_discount_usage,
# MAGIC             is_installment_default,
# MAGIC             channel_sk,
# MAGIC             product_sk,
# MAGIC             COALESCE(insured_object_sk,-1) AS insured_object_sk,
# MAGIC             effective_from_ts,
# MAGIC             watermark_col,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC               PARTITION BY policy_id
# MAGIC               ORDER BY watermark_col DESC
# MAGIC             ) AS rn
# MAGIC           FROM src
# MAGIC         )
# MAGIC         SELECT
# MAGIC           policy_id,
# MAGIC           policy_number,
# MAGIC           policy_start_ts,
# MAGIC           policy_end_ts,
# MAGIC           policy_tenure,
# MAGIC           policy_cycle,
# MAGIC           renewal_date,
# MAGIC           policy_status,
# MAGIC           policy_cover_option,
# MAGIC           policy_sales_channel,
# MAGIC           is_fraud,
# MAGIC           quote_id,
# MAGIC           policy_type,
# MAGIC           policy_issue_date,
# MAGIC           is_policy_renewal,
# MAGIC           policy_cancellation_reason,
# MAGIC           policy_sum_insured,
# MAGIC           policy_retention_limit,
# MAGIC           policy_risk_score,
# MAGIC           policy_risk_band,
# MAGIC           is_auto_renew_enabled,
# MAGIC           no_claims_discount_years,
# MAGIC           payment_method,
# MAGIC           is_direct_debit_cancellation,
# MAGIC           missed_payment_count,
# MAGIC           loyalty_discount_usage,
# MAGIC           is_installment_default,
# MAGIC           channel_sk,
# MAGIC           product_sk,
# MAGIC           coalesce(insured_object_sk,-1) AS insured_object_sk,
# MAGIC           watermark_col AS effective_from_ts,
# MAGIC           TIMESTAMP '9999-12-31 00:00:00' AS effective_to_ts
# MAGIC         FROM dedup
# MAGIC         WHERE rn = 1
# MAGIC     """,
# MAGIC
# MAGIC     "attribute_cols": [
# MAGIC         "channel_sk",
# MAGIC         "product_sk",
# MAGIC         "insured_object_sk",
# MAGIC         "policy_id",
# MAGIC         "policy_number",
# MAGIC         "policy_start_ts",
# MAGIC         "policy_end_ts",
# MAGIC         "policy_tenure",
# MAGIC         "policy_cycle",
# MAGIC         "renewal_date",
# MAGIC         "policy_status",
# MAGIC         "policy_cover_option",
# MAGIC         "policy_sales_channel",
# MAGIC         "is_fraud",
# MAGIC         "quote_id",
# MAGIC         "policy_type",
# MAGIC         "policy_issue_date",
# MAGIC         "is_policy_renewal",
# MAGIC         "policy_cancellation_reason",
# MAGIC         "policy_sum_insured",
# MAGIC         "policy_retention_limit",
# MAGIC         "policy_risk_score",
# MAGIC         "policy_risk_band",
# MAGIC         "is_auto_renew_enabled",
# MAGIC         "no_claims_discount_years",
# MAGIC         "payment_method",
# MAGIC         "is_direct_debit_cancellation",
# MAGIC         "missed_payment_count",
# MAGIC         "loyalty_discount_usage",
# MAGIC         "is_installment_default",
# MAGIC     ],
# MAGIC
# MAGIC     "insert_cols": [
# MAGIC         "channel_sk",
# MAGIC         "product_sk",
# MAGIC         "insured_object_sk",
# MAGIC         "policy_id",
# MAGIC         "policy_number",
# MAGIC         "policy_start_ts",
# MAGIC         "policy_end_ts",
# MAGIC         "policy_tenure",
# MAGIC         "policy_cycle",
# MAGIC         "renewal_date",
# MAGIC         "policy_status",
# MAGIC         "policy_cover_option",
# MAGIC         "policy_sales_channel",
# MAGIC         "is_fraud",
# MAGIC         "quote_id",
# MAGIC         "policy_type",
# MAGIC         "policy_issue_date",
# MAGIC         "is_policy_renewal",
# MAGIC         "policy_cancellation_reason",
# MAGIC         "policy_sum_insured",
# MAGIC         "policy_retention_limit",
# MAGIC         "policy_risk_score",
# MAGIC         "policy_risk_band",
# MAGIC         "is_auto_renew_enabled",
# MAGIC         "no_claims_discount_years",
# MAGIC         "payment_method",
# MAGIC         "is_direct_debit_cancellation",
# MAGIC         "missed_payment_count",
# MAGIC         "loyalty_discount_usage",
# MAGIC         "is_installment_default",
# MAGIC         "attr_hash",
# MAGIC         "effective_from_ts",
# MAGIC         "effective_to_ts",
# MAGIC         "record_version",
# MAGIC         "created_by",
# MAGIC         "created_ts",
# MAGIC         "last_updated_by",
# MAGIC         "last_updated_ts",
# MAGIC     ]
# MAGIC }

# COMMAND ----------

# DBTITLE 1,DIM Product Configuration and Latest Deduplicated Query
# MAGIC %skip
# MAGIC DIM_PRODUCT_CFG = {
# MAGIC     "name": "dim_product",
# MAGIC     "target_table": f"{catalog_name}.{gold_schema_name}.dim_product",
# MAGIC     "business_key_col": "product_id",
# MAGIC     "scd_type": "2",
# MAGIC     "watermark_col": "load_timestamp",
# MAGIC     "effective_from_col": "effective_from_ts",
# MAGIC     "record_version_col": "record_version",
# MAGIC
# MAGIC     "stage_sql": f"""
# MAGIC     WITH src AS (
# MAGIC           SELECT
# MAGIC             bpm.global_product_identifier AS product_id,
# MAGIC             bpm.product_type AS product_type,
# MAGIC             bpm.product_sub_type AS product_variant,
# MAGIC             bpm.product_name AS product_name,
# MAGIC             bpm.product_launch_date AS product_launch_date,
# MAGIC             bpm.product_status AS product_status,
# MAGIC             bpm.product_line_of_business AS product_line_of_business_code,
# MAGIC             bpm.underwriting_group AS underwriting_group,
# MAGIC             bpm.regulatory_approval_code AS regulatory_approval_code,
# MAGIC             COALESCE(bpm.effective_from, DATE('1900-01-01')) AS effective_from_ts,
# MAGIC             x.${{watermark_col}} AS watermark_col
# MAGIC           FROM {catalog_name}.{vault_schema}.bv_product_master bpm
# MAGIC           LEFT JOIN {catalog_name}.{vault_schema}.bv_product_master_xref x
# MAGIC             ON x.global_product_identifier = bpm.global_product_identifier
# MAGIC           WHERE x.${{watermark_col}} > ${{watermark_col}}
# MAGIC         ),
# MAGIC         dedup AS (
# MAGIC           SELECT
# MAGIC             product_id,
# MAGIC             product_type,
# MAGIC             product_variant,
# MAGIC             product_name,
# MAGIC             product_launch_date,
# MAGIC             product_status,
# MAGIC             product_line_of_business_code,
# MAGIC             underwriting_group,
# MAGIC             regulatory_approval_code,
# MAGIC             effective_from_ts,
# MAGIC             watermark_col,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC               PARTITION BY product_id
# MAGIC               ORDER BY watermark_col DESC
# MAGIC             ) AS rn
# MAGIC           FROM src
# MAGIC         )
# MAGIC         SELECT
# MAGIC           product_id,
# MAGIC           product_type,
# MAGIC           product_variant,
# MAGIC           product_name,
# MAGIC           product_launch_date,
# MAGIC           product_status,
# MAGIC           product_line_of_business_code,
# MAGIC           underwriting_group,
# MAGIC           regulatory_approval_code,
# MAGIC           watermark_col AS effective_from_ts,
# MAGIC           TIMESTAMP '9999-12-31 00:00:00' AS effective_to_ts
# MAGIC         FROM dedup
# MAGIC         WHERE rn = 1
# MAGIC     """,
# MAGIC
# MAGIC     "attribute_cols": [
# MAGIC         "product_id",
# MAGIC         "product_type",
# MAGIC         "product_variant",
# MAGIC         "product_name",
# MAGIC         "product_launch_date",
# MAGIC         "product_status",
# MAGIC         "product_line_of_business_code",
# MAGIC         "underwriting_group",
# MAGIC         "regulatory_approval_code",
# MAGIC     ],
# MAGIC
# MAGIC     "insert_cols": [
# MAGIC         "product_id",
# MAGIC         "product_type",
# MAGIC         "product_variant",
# MAGIC         "product_name",
# MAGIC         "product_launch_date",
# MAGIC         "product_status",
# MAGIC         "product_line_of_business_code",
# MAGIC         "underwriting_group",
# MAGIC         "regulatory_approval_code",
# MAGIC         "attr_hash",
# MAGIC         "effective_from_ts",
# MAGIC         "effective_to_ts",
# MAGIC         "record_version",
# MAGIC         "created_by",
# MAGIC         "created_ts",
# MAGIC         "last_updated_by",
# MAGIC         "last_updated_ts",
# MAGIC     ]
# MAGIC }

# COMMAND ----------

# DBTITLE 1,ALL_DIM_CONFIGS
ALL_DIM_CONFIGS = {
  "dim_loss_event" : DIM_LOSS_EVENT_CFG,
  "dim_coverage" : DIM_COVERAGE_CFG,
  "dim_medical" : DIM_MEDICAL_CFG
}