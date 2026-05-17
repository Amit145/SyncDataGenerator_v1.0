-- Build newly added enhanced dimensional entities from enhanced Data Vault tables.
-- Source mapping: dims/Enhanced_Customer360_S2T_Mapping_DV_to_Dimensional_Model.xlsx
--
-- Scope:
--   Dimensions: dim_broker, dim_campaign, dim_channel, dim_claim,
--               dim_insured_object, dim_override, dim_regulation
--   Fact:       fact_complaint
--
-- Assumptions:
--   1. Target dim/fact tables already exist.
--   2. Source enhanced Data Vault tables are available in the current schema.
--   3. Satellite rows are current-snapshot rows. If multiple satellite versions exist,
--      the latest row by load_date is selected.
--   4. Output column order follows the S2T physical order.
--   5. A few source aliases are corrected to the active enhanced DDL:
--      - sat_campaign.number_of_policy_purchases -> dim_campaign.number_of_policy_purchases
--      - sat_campaign.spend_amount -> dim_campaign.spend_amt
--      - sat_claim.claim_amount -> dim_claim.claim_amt
--      - sat_claim.remediation_amount -> dim_claim.remediation_amt
--      - sat_claim.fraud_amount -> dim_claim.fraud_amt
--      - sat_insured_object.insured_object_description -> dim_insured_object.insured_object_desc
--      - sat_complaint.compensation_amount -> fact_complaint.compensation_amt
--      - sat_complaint.complaint_status -> fact_complaint.compaint_status

INSERT OVERWRITE TABLE dim_broker
WITH latest_sat_broker AS (
    SELECT *
    FROM (
        SELECT
            sb.*,
            ROW_NUMBER() OVER (PARTITION BY sb.broker_hash_key ORDER BY sb.load_date DESC) AS rn
        FROM sat_broker sb
    ) x
    WHERE rn = 1
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY hb.agent_id) AS BIGINT) AS broker_sk,
    hb.agent_id AS agent_id,
    sb.agent_name AS agent_name,
    sb.agent_type AS agent_type,
    sb.agent_status AS agent_status,
    sb.agent_license_number AS agent_license_number,
    CAST(sb.agent_net_promoter_score AS DECIMAL(3,1)) AS agent_net_promoter_score,
    CAST(sb.agent_commission_percentage AS DECIMAL(4,2)) AS agent_commission_percentage,
    CAST(sb.load_date AS TIMESTAMP) AS effective_from_ts,
    CAST('9999-12-31 00:00:00' AS TIMESTAMP) AS effective_to_ts,
    CAST(1 AS INTEGER) AS record_version,
    'ETL_SYSTEM' AS created_by,
    current_timestamp() AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp() AS last_updated_ts,
    md5(concat_ws('|',
        coalesce(cast(hb.agent_id AS STRING), ''),
        coalesce(cast(sb.agent_name AS STRING), ''),
        coalesce(cast(sb.agent_type AS STRING), ''),
        coalesce(cast(sb.agent_status AS STRING), ''),
        coalesce(cast(sb.agent_license_number AS STRING), ''),
        coalesce(cast(sb.agent_net_promoter_score AS STRING), ''),
        coalesce(cast(sb.agent_commission_percentage AS STRING), '')
    )) AS attr_hash
FROM hub_broker hb
LEFT JOIN latest_sat_broker sb
    ON sb.broker_hash_key = hb.broker_hash_key;

INSERT OVERWRITE TABLE dim_campaign
WITH latest_sat_campaign AS (
    SELECT *
    FROM (
        SELECT
            sc.*,
            ROW_NUMBER() OVER (PARTITION BY sc.campaign_hash_key ORDER BY sc.load_date DESC) AS rn
        FROM sat_campaign sc
    ) x
    WHERE rn = 1
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY hc.campaign_id) AS BIGINT) AS campaign_sk,
    hc.campaign_id AS campaign_id,
    sc.campaign_name AS campaign_name,
    sc.campaign_type AS campaign_type,
    CAST(sc.campaign_start_date AS DATE) AS campaign_start_date,
    CAST(sc.campaign_end_date AS DATE) AS campaign_end_date,
    sc.campaign_status AS campaign_status,
    CAST(sc.campaign_budget AS INTEGER) AS campaign_budget,
    sc.campaign_target_audience AS campaign_target_audience,
    sc.campaign_marketing_source AS campaign_marketing_source,
    sc.campaign_owner_department AS campaign_owner_department,
    sc.campaign_country AS campaign_country,
    sc.campaign_conversion_goal AS campaign_conversion_goal,
    CAST(sc.number_of_clicks AS INTEGER) AS number_of_clicks,
    CAST(sc.is_active AS CHAR(1)) AS is_active,
    CAST(sc.number_of_visits AS INTEGER) AS number_of_visits,
    CAST(sc.number_of_policy_purchases AS INTEGER) AS number_of_policy_purchases,
    CAST(sc.number_of_emails_sent AS INTEGER) AS number_of_emails_sent,
    CAST(sc.number_of_email_bounced AS INTEGER) AS number_of_email_bounced,
    CAST(sc.number_of_emails_delivered AS INTEGER) AS number_of_emails_delivered,
    CAST(sc.number_of_emails_opened AS INTEGER) AS number_of_emails_opened,
    CAST(sc.click_through_rate AS DECIMAL(10,9)) AS click_through_rate,
    CAST(sc.spend_amount AS DECIMAL(12,4)) AS spend_amt,
    CAST(sc.incremental_revenue AS DECIMAL(18,4)) AS incremental_revenue,
    sc.survey_wave AS survey_wave,
    CAST(sc.total_number_of_respondents AS INTEGER) AS total_number_of_respondents,
    CAST(sc.number_of_respondents_aware AS INTEGER) AS number_of_respondents_aware,
    CAST(sc.number_of_promoters AS INTEGER) AS number_of_promoters,
    CAST(sc.number_of_passives AS INTEGER) AS number_of_passives,
    CAST(sc.number_of_detractors AS INTEGER) AS number_of_detractors,
    CAST(sc.number_of_followers AS INTEGER) AS number_of_followers,
    CAST(sc.number_of_likes AS INTEGER) AS number_of_likes,
    CAST(sc.number_of_comments AS INTEGER) AS number_of_comments,
    CAST(sc.number_of_shares AS INTEGER) AS number_of_shares,
    CAST(sc.number_of_brand_mentions AS INTEGER) AS number_of_brand_mentions,
    CAST(sc.number_of_category_mentions AS INTEGER) AS number_of_category_mentions,
    CAST(sc.number_of_impressions AS INTEGER) AS number_of_impressions,
    'ETL_SYSTEM' AS created_by,
    current_timestamp() AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp() AS last_updated_ts,
    md5(concat_ws('|',
        coalesce(cast(hc.campaign_id AS STRING), ''),
        coalesce(cast(sc.campaign_name AS STRING), ''),
        coalesce(cast(sc.campaign_type AS STRING), ''),
        coalesce(cast(sc.campaign_status AS STRING), ''),
        coalesce(cast(sc.campaign_budget AS STRING), ''),
        coalesce(cast(sc.is_active AS STRING), '')
    )) AS attr_hash
FROM hub_campaign hc
LEFT JOIN latest_sat_campaign sc
    ON sc.campaign_hash_key = hc.campaign_hash_key;

INSERT OVERWRITE TABLE dim_channel
WITH latest_sat_channel AS (
    SELECT *
    FROM (
        SELECT
            sc.*,
            ROW_NUMBER() OVER (PARTITION BY sc.channel_hash_key ORDER BY sc.load_date DESC) AS rn
        FROM sat_channel sc
    ) x
    WHERE rn = 1
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY hc.channel_id) AS BIGINT) AS channel_sk,
    hc.channel_id AS channel_id,
    sc.channel_name AS channel_name,
    sc.channel_type AS channel_type,
    'ETL_SYSTEM' AS created_by,
    current_timestamp() AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp() AS last_updated_ts,
    md5(concat_ws('|',
        coalesce(cast(hc.channel_id AS STRING), ''),
        coalesce(cast(sc.channel_name AS STRING), ''),
        coalesce(cast(sc.channel_type AS STRING), '')
    )) AS attr_hash
FROM hub_channel hc
LEFT JOIN latest_sat_channel sc
    ON sc.channel_hash_key = hc.channel_hash_key;

INSERT OVERWRITE TABLE dim_claim
WITH latest_sat_claim AS (
    SELECT *
    FROM (
        SELECT
            sc.*,
            ROW_NUMBER() OVER (PARTITION BY sc.claim_hash_key ORDER BY sc.load_date DESC) AS rn
        FROM sat_claim sc
    ) x
    WHERE rn = 1
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY hc.claim_id) AS BIGINT) AS claim_sk,
    hc.claim_id AS claim_id,
    sc.claim_number AS claim_number,
    sc.claim_type AS claim_type,
    sc.claim_status AS claim_status,
    sc.claim_reason AS claim_reason,
    sc.claim_channel AS claim_channel,
    sc.claim_handler AS claim_handler,
    CAST(sc.claim_reported_date AS DATE) AS claim_reported_date,
    CAST(sc.claim_settlement_date AS DATE) AS claim_settlement_date,
    sc.claim_product AS claim_product,
    CAST(sc.is_claim_suspicious AS CHAR(1)) AS is_claim_suspicious,
    CAST(sc.is_claim_fraud AS CHAR(1)) AS is_claim_fraud,
    sc.claim_fraud_status AS claim_fraud_status,
    sc.claim_fraud_type AS claim_fraud_type,
    sc.claim_fraud_detection_method AS claim_fraud_detection_method,
    CAST(sc.is_litigation AS CHAR(1)) AS is_litigation,
    sc.litigation_reason AS litigation_reason,
    CAST(sc.litigation_start_date AS DATE) AS litigation_start_date,
    CAST(sc.litigation_end_date AS DATE) AS litigation_end_date,
    sc.litigation_outcome AS litigation_outcome,
    CAST(sc.litigation_duration_days AS INTEGER) AS litigation_duration_days,
    CAST(sc.claim_fraud_detection_time_in_days AS INTEGER) AS claim_fraud_detection_time_in_days,
    CAST(sc.is_recovery_opportunity AS CHAR(1)) AS is_recovery_opportunity,
    CAST(sc.recovery_priority_score AS INTEGER) AS recovery_priority_score,
    sc.recovery_category AS recovery_category,
    sc.recovery_source AS recovery_source,
    CAST(sc.first_recovery_date AS DATE) AS first_recovery_date,
    CAST(sc.last_recovery_date AS DATE) AS last_recovery_date,
    CAST(sc.is_recovery_happened AS CHAR(1)) AS is_recovery_happened,
    CAST(sc.days_to_first_recovery AS INTEGER) AS days_to_first_recovery,
    CAST(sc.days_to_last_recovery AS INTEGER) AS days_to_last_recovery,
    CAST(sc.avg_days_to_close_claim AS INTEGER) AS avg_days_to_close_claim,
    sc.claim_fraud_outcome AS claim_fraud_outcome,
    sc.recovery_type AS recovery_type,
    sc.recovery_band AS recovery_band,
    sc.third_party_involved AS third_party_involved,
    CAST(sc.third_party_involved_overall_score AS DECIMAL(10,9)) AS third_party_involved_overall_score,
    sc.solicitor AS solicitor,
    CAST(sc.claim_amount AS DECIMAL(18,4)) AS claim_amt,
    CAST(sc.claims_paid AS DECIMAL(18,4)) AS claims_paid,
    CAST(sc.outstanding_reserve AS DECIMAL(18,4)) AS outstanding_reserve,
    CAST(sc.claims_expenses AS DECIMAL(18,4)) AS claims_expenses,
    CAST(sc.recovery_received AS DECIMAL(18,4)) AS recovery_received,
    CAST(sc.compensation_offered AS DECIMAL(18,4)) AS compensation_offered,
    CAST(sc.remediation_amount AS DECIMAL(18,4)) AS remediation_amt,
    CAST(sc.suspectd_amount AS DECIMAL(18,4)) AS suspected_amt,
    CAST(sc.fraud_amount AS DECIMAL(18,4)) AS fraud_amt,
    CAST(sc.legal_expenses AS DECIMAL(18,4)) AS legal_expenses,
    sc.claim_band AS claim_band,
    CAST(sc.claim_band_sort AS INTEGER) AS claim_band_sort,
    CAST(sc.load_date AS TIMESTAMP) AS effective_from_ts,
    CAST('9999-12-31 00:00:00' AS TIMESTAMP) AS effective_to_ts,
    CAST(1 AS INTEGER) AS record_version,
    'ETL_SYSTEM' AS created_by,
    current_timestamp() AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp() AS last_updated_ts,
    md5(concat_ws('|',
        coalesce(cast(hc.claim_id AS STRING), ''),
        coalesce(cast(sc.claim_number AS STRING), ''),
        coalesce(cast(sc.claim_status AS STRING), ''),
        coalesce(cast(sc.claim_channel AS STRING), ''),
        coalesce(cast(sc.claim_product AS STRING), ''),
        coalesce(cast(sc.claim_amount AS STRING), ''),
        coalesce(cast(sc.claims_paid AS STRING), '')
    )) AS attr_hash
FROM hub_claim hc
LEFT JOIN latest_sat_claim sc
    ON sc.claim_hash_key = hc.claim_hash_key;

INSERT OVERWRITE TABLE dim_insured_object
WITH latest_sat_insured_object AS (
    SELECT *
    FROM (
        SELECT
            sio.*,
            ROW_NUMBER() OVER (PARTITION BY sio.insured_object_hash_key ORDER BY sio.load_date DESC) AS rn
        FROM sat_insured_object sio
    ) x
    WHERE rn = 1
)
SELECT
    CAST(substr(md5(cast(hio.insured_object_id AS STRING)), 1, 18) AS CHAR(18)) AS insured_object_sk,
    hio.insured_object_id AS insured_object_id,
    sio.insured_object_type AS insured_object_type,
    sio.insured_object_sub_type AS insured_object_sub_type,
    sio.insured_object_description AS insured_object_desc,
    CAST(sio.insured_value AS FLOAT) AS insured_value,
    sio.currency_code AS currency_code,
    CAST(sio.insured_object_start_date AS DATE) AS insured_object_start_date,
    CAST(sio.insured_object_end_date AS DATE) AS insured_object_end_date,
    sio.insured_object_current_status AS insured_object_current_status,
    'ETL_SYSTEM' AS created_by,
    current_timestamp() AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp() AS last_updated_ts,
    md5(concat_ws('|',
        coalesce(cast(hio.insured_object_id AS STRING), ''),
        coalesce(cast(sio.insured_object_type AS STRING), ''),
        coalesce(cast(sio.insured_object_sub_type AS STRING), ''),
        coalesce(cast(sio.insured_value AS STRING), ''),
        coalesce(cast(sio.insured_object_current_status AS STRING), '')
    )) AS attr_hash
FROM hub_insured_object hio
LEFT JOIN latest_sat_insured_object sio
    ON sio.insured_object_hash_key = hio.insured_object_hash_key;

INSERT OVERWRITE TABLE dim_override
WITH latest_sat_override AS (
    SELECT *
    FROM (
        SELECT
            so.*,
            ROW_NUMBER() OVER (PARTITION BY so.override_hash_key ORDER BY so.load_date DESC) AS rn
        FROM sat_override so
    ) x
    WHERE rn = 1
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY ho.override_id) AS BIGINT) AS override_sk,
    ho.override_id AS override_id,
    so.override_reason AS override_reason,
    'ETL_SYSTEM' AS created_by,
    current_timestamp() AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp() AS last_updated_ts,
    md5(concat_ws('|',
        coalesce(cast(ho.override_id AS STRING), ''),
        coalesce(cast(so.override_reason AS STRING), '')
    )) AS attr_hash
FROM hub_override ho
LEFT JOIN latest_sat_override so
    ON so.override_hash_key = ho.override_hash_key;

INSERT OVERWRITE TABLE dim_regulation
WITH latest_sat_regulation AS (
    SELECT *
    FROM (
        SELECT
            sr.*,
            ROW_NUMBER() OVER (PARTITION BY sr.regulation_hash_key ORDER BY sr.load_date DESC) AS rn
        FROM sat_regulation sr
    ) x
    WHERE rn = 1
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY hr.regulation_id) AS BIGINT) AS regulation_sk,
    hr.regulation_id AS regulation_id,
    sr.regulation_number AS regulation_number,
    sr.regulation_name AS regulation_name,
    sr.regulation_department AS regulation_department,
    sr.regulation_region AS regulation_region,
    sr.regulation_risk_level AS regulation_risk_level,
    sr.regulation_compliance_status AS regulation_compliance_status,
    CAST(sr.regulation_date_raised AS DATE) AS regulation_date_raised,
    CAST(sr.regulation_date_closed AS DATE) AS regulation_date_closed,
    sr.regulation_owner AS regulation_owner,
    CAST(sr.regulation_deadline_date AS DATE) AS regulation_deadline_date,
    CAST(sr.is_regulation_on_time AS CHAR(1)) AS is_regulation_on_time,
    'ETL_SYSTEM' AS created_by,
    current_timestamp() AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp() AS last_updated_ts,
    md5(concat_ws('|',
        coalesce(cast(hr.regulation_id AS STRING), ''),
        coalesce(cast(sr.regulation_number AS STRING), ''),
        coalesce(cast(sr.regulation_name AS STRING), ''),
        coalesce(cast(sr.regulation_compliance_status AS STRING), ''),
        coalesce(cast(sr.is_regulation_on_time AS STRING), '')
    )) AS attr_hash
FROM hub_regulation hr
LEFT JOIN latest_sat_regulation sr
    ON sr.regulation_hash_key = hr.regulation_hash_key;

INSERT OVERWRITE TABLE fact_complaint
WITH latest_sat_complaint AS (
    SELECT *
    FROM (
        SELECT
            sc.*,
            ROW_NUMBER() OVER (PARTITION BY sc.complaint_hash_key ORDER BY sc.load_date DESC) AS rn
        FROM sat_complaint sc
    ) x
    WHERE rn = 1
),
complaint_policy AS (
    SELECT
        lcp.complaint_hash_key,
        lcp.policy_hash_key,
        lpc.customer_hash_key,
        lcpol.channel_hash_key,
        lpio.insured_object_hash_key
    FROM link_complaint_policy lcp
    LEFT JOIN link_policy_customer lpc
        ON lpc.policy_hash_key = lcp.policy_hash_key
    LEFT JOIN link_policy_channel lcpol
        ON lcpol.policy_hash_key = lcp.policy_hash_key
    LEFT JOIN link_policy_insured_object lpio
        ON lpio.policy_hash_key = lcp.policy_hash_key
),
complaint_person AS (
    SELECT
        cp.*,
        lcpp.person_hash_key
    FROM complaint_policy cp
    LEFT JOIN link_customer_person lcpp
        ON lcpp.customer_hash_key = cp.customer_hash_key
),
complaint_regulation AS (
    SELECT
        lcr.complaint_hash_key,
        lcr.regulation_hash_key
    FROM link_complaint_regulation lcr
)
SELECT
    dp.person_sk AS person_sk,
    dc.customer_sk AS customer_sk,
    dr.regulation_sk AS regulation_sk,
    dch.channel_sk AS channel_sk,
    CAST(date_format(CAST(sc.complaint_date AS DATE), 'yyyyMMdd') AS BIGINT) AS date_sk,
    dio.insured_object_sk AS insured_object_sk,
    dp.person_id AS person_id,
    hc.complaint_id AS complaint_id,
    CAST(sc.complaint_date AS DATE) AS complaint_date,
    CAST(sc.complaint_acknowledgement_date AS DATE) AS complaint_acknowledgement_date,
    CAST(sc.complaint_resolved_date AS DATE) AS complaint_resolved_date,
    sc.complaint_upheld_status AS complaint_upheld_status,
    CAST(sc.is_financial_ombudsman_service_referral AS CHAR(1)) AS is_financial_ombudsman_service_referral,
    sc.complaint_driver AS complaint_driver,
    sc.complaint_channel AS complaint_channel,
    CAST(sc.compensation_amount AS DECIMAL(18,4)) AS compensation_amt,
    sc.complaint_status AS compaint_status,
    sc.insurance_category AS insurance_category,
    'ETL_SYSTEM' AS created_by,
    current_timestamp() AS created_ts,
    current_timestamp() AS load_ts
FROM hub_complaint hc
LEFT JOIN latest_sat_complaint sc
    ON sc.complaint_hash_key = hc.complaint_hash_key
LEFT JOIN complaint_person cp
    ON cp.complaint_hash_key = hc.complaint_hash_key
LEFT JOIN hub_person hp
    ON hp.person_hash_key = cp.person_hash_key
LEFT JOIN hub_customer hcu
    ON hcu.customer_hash_key = cp.customer_hash_key
LEFT JOIN hub_channel hch
    ON hch.channel_hash_key = cp.channel_hash_key
LEFT JOIN hub_insured_object hio
    ON hio.insured_object_hash_key = cp.insured_object_hash_key
LEFT JOIN complaint_regulation cr
    ON cr.complaint_hash_key = hc.complaint_hash_key
LEFT JOIN hub_regulation hr
    ON hr.regulation_hash_key = cr.regulation_hash_key
LEFT JOIN dim_person dp
    ON dp.person_id = hp.person_id
LEFT JOIN dim_customer dc
    ON dc.customer_id = hcu.customer_id
LEFT JOIN dim_channel dch
    ON dch.channel_id = hch.channel_id
LEFT JOIN dim_insured_object dio
    ON dio.insured_object_id = hio.insured_object_id
LEFT JOIN dim_regulation dr
    ON dr.regulation_id = hr.regulation_id;

