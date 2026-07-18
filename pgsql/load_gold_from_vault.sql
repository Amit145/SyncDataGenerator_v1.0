-- Build enhanced dimensional tables from the current PostgreSQL Raw Vault and Business Vault.
--
-- Source schemas:
--   raw_vault
--   business_vault
--
-- Target schema:
--   gold
--
-- Scope:
--   18 dimensions + 4 facts from the enhanced dimensional model.
--
-- Notes:
--   1. BV-aware dimensions use business_vault master tables where available.
--   2. Non-BV dimensions and fact_complaint use raw_vault directly.
--   3. The BV S2T separates home and motor, so this script creates dim_home.home_sk
--      and dim_motor.motor_sk, and facts carry home_sk/motor_sk where applicable.
--   4. Helper raw hash columns are retained in dimensions to make fact population
--      transparent and auditable.

DROP SCHEMA IF EXISTS gold CASCADE;
CREATE SCHEMA gold;
SET search_path TO gold, business_vault, raw_vault, public;

CREATE TABLE dim_date AS
WITH src_dates AS (
    SELECT NULLIF(policy_start_date, '')::date AS d FROM raw_vault.sat_policy_crm WHERE NULLIF(policy_start_date, '') IS NOT NULL
    UNION SELECT NULLIF(policy_end_date, '')::date FROM raw_vault.sat_policy_crm WHERE NULLIF(policy_end_date, '') IS NOT NULL
    UNION SELECT NULLIF(policy_issue_date, '')::date FROM raw_vault.sat_policy_crm WHERE NULLIF(policy_issue_date, '') IS NOT NULL
    UNION SELECT NULLIF(renewal_date, '')::date FROM raw_vault.sat_policy_crm WHERE NULLIF(renewal_date, '') IS NOT NULL
    UNION SELECT NULLIF(quote_date, '')::date FROM raw_vault.sat_quote_crm WHERE NULLIF(quote_date, '') IS NOT NULL
    UNION SELECT NULLIF(converted_date, '')::date FROM raw_vault.sat_lead_crm WHERE NULLIF(converted_date, '') IS NOT NULL
    UNION SELECT NULLIF(complaint_date, '')::date FROM raw_vault.sat_complaint_crm WHERE NULLIF(complaint_date, '') IS NOT NULL
),
bounds AS (
    SELECT coalesce(min(d), current_date) AS min_d, coalesce(max(d), current_date) AS max_d
    FROM src_dates
)
SELECT
    to_char(dt, 'YYYYMMDD')::bigint AS date_sk,
    to_char(dt, 'FMDay') AS day_name,
    extract(day from dt)::integer AS day_of_month_number,
    to_char(dt, 'FMMonth') AS month_name,
    extract(year from dt)::integer AS year_number,
    dt::date AS full_date,
    CASE WHEN extract(isodow from dt) IN (6, 7) THEN 'Y' ELSE 'N' END AS is_weekend,
    extract(month from dt)::integer AS month_number,
    extract(quarter from dt)::integer AS quarter_number,
    extract(week from dt)::integer AS week_of_year_number,
    to_char(dt, 'Mon') AS month_short_name,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts
FROM bounds
CROSS JOIN generate_series(min_d, max_d, interval '1 day') AS g(dt);

ALTER TABLE dim_date ADD PRIMARY KEY (date_sk);

CREATE TABLE dim_product AS
SELECT
    row_number() OVER (ORDER BY bpm.global_product_identifier)::bigint AS product_sk,
    bpm.global_product_identifier AS product_id,
    bpm.product_type,
    bpm.product_sub_type AS product_variant,
    bpm.product_name,
    bpm.product_launch_date,
    bpm.product_status,
    bpm.product_line_of_business AS product_line_of_business_code,
    bpm.underwriting_group,
    bpm.regulatory_approval_code,
    bpm.effective_from::timestamp AS effective_from_ts,
    bpm.effective_to::timestamp AS effective_to_ts,
    1::integer AS record_version,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp AS last_updated_ts,
    md5(concat_ws('|', bpm.global_product_identifier, bpm.product_type, bpm.product_sub_type, bpm.product_name)) AS attr_hash,
    max(CASE WHEN x.rawdv_source_name = 'CRM' THEN x.rawdv_hashkey END) AS crm_product_hash_key,
    max(CASE WHEN x.rawdv_source_name = 'SAP' THEN x.rawdv_hashkey END) AS sap_product_hash_key
FROM business_vault.bv_product_master bpm
LEFT JOIN business_vault.bv_product_master_xref x
    ON x.global_product_identifier = bpm.global_product_identifier
GROUP BY
    bpm.global_product_identifier, bpm.product_type, bpm.product_sub_type, bpm.product_name,
    bpm.product_launch_date, bpm.product_status, bpm.product_line_of_business,
    bpm.underwriting_group, bpm.regulatory_approval_code, bpm.effective_from, bpm.effective_to;

ALTER TABLE dim_product ADD PRIMARY KEY (product_sk);

CREATE TABLE dim_geography AS
SELECT
    row_number() OVER (ORDER BY city, state, country, region)::bigint AS geography_sk,
    city,
    state,
    country,
    region,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp AS last_updated_ts,
    md5(concat_ws('|', city, state, country, region)) AS attr_hash
FROM (
    SELECT DISTINCT
        nullif(city, '') AS city,
        nullif(state, '') AS state,
        nullif(country, '') AS country,
        nullif(region, '') AS region
    FROM business_vault.bv_address_master
) g;

ALTER TABLE dim_geography ADD PRIMARY KEY (geography_sk);

CREATE TABLE dim_identity AS
WITH latest_sat AS (
    SELECT *
    FROM (
        SELECT si.*, row_number() OVER (PARTITION BY identities_hash_key ORDER BY load_date DESC) AS rn
        FROM raw_vault.sat_identities_crm si
    ) s
    WHERE rn = 1
)
SELECT
    row_number() OVER (ORDER BY hi.identities_id)::bigint AS identity_sk,
    hi.identities_id AS identity_id,
    si.ecid AS experience_cloud_id,
    si.hashed_email AS email_address_hash_value,
    hi.load_date AS effective_from_ts,
    timestamp '9999-12-31 00:00:00' AS effective_to_ts,
    1::integer AS record_version,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp AS last_updated_ts,
    md5(concat_ws('|', hi.identities_id, si.ecid, si.hashed_email)) AS attr_hash,
    hi.identities_hash_key
FROM raw_vault.hub_identities hi
LEFT JOIN latest_sat si
    ON si.identities_hash_key = hi.identities_hash_key;

ALTER TABLE dim_identity ADD PRIMARY KEY (identity_sk);

CREATE TABLE dim_person AS
WITH natural_person AS (
    SELECT
        np.global_person_identifier AS person_id,
        split_part(np.global_person_identifier, '||', 1) AS crm_person_id,
        split_part(np.global_person_identifier, '||', 2) AS sap_person_id,
        hp_crm.person_hash_key AS crm_person_hash_key,
        hp_sap.person_hash_key AS sap_person_hash_key,
        dg.geography_sk,
        di.identity_sk,
        np.courtesy_title,
        np.first_name,
        np.last_name,
        np.full_name,
        np.person_type,
        np.date_of_birth AS birth_date,
        np.gender,
        np.nationality,
        np.marital_status,
        np.occupation,
        adr.global_address_identifier AS home_address_id,
        adr.address_type,
        adr.address_line_2 AS street_address,
        adr.postal_code AS postcode,
        hc.contact_id,
        np.home_phone AS home_phone_number,
        np.work_phone AS work_phone_number,
        np.personal_email,
        np.work_email,
        np.job_title,
        np.role,
        NULL::text AS company_name,
        NULL::date AS date_of_constitution,
        np.is_lead,
        np.preferred_language,
        np.source_identifier AS source_id,
        np.source_type,
        np.tenant_identifier AS tenant_id,
        np.assesed_disability_degree AS assessed_disability_degree,
        np.operational_paperless_consent AS is_operational_paperless_consent,
        hco.consent_id,
        sc.opt_in_legitimate_interest AS is_opt_in_legitimate_interest,
        sc.opt_in_validated AS is_opt_in_validated,
        np.effective_from::timestamp AS effective_from_ts,
        np.effective_to::timestamp AS effective_to_ts,
        1::integer AS record_version
    FROM business_vault.bv_natural_person_master np
    LEFT JOIN raw_vault.hub_person hp_crm ON hp_crm.person_id = split_part(np.global_person_identifier, '||', 1)
    LEFT JOIN raw_vault.hub_person hp_sap ON hp_sap.person_id = split_part(np.global_person_identifier, '||', 2)
    LEFT JOIN business_vault.bv_address_master adr ON adr.global_person_identifier = np.global_person_identifier
    LEFT JOIN gold.dim_geography dg
        ON coalesce(dg.city, '') = coalesce(adr.city, '')
       AND coalesce(dg.state, '') = coalesce(adr.state, '')
       AND coalesce(dg.country, '') = coalesce(adr.country, '')
       AND coalesce(dg.region, '') = coalesce(adr.region, '')
    LEFT JOIN raw_vault.link_person_identities lpi ON lpi.person_hash_key = hp_crm.person_hash_key
    LEFT JOIN gold.dim_identity di ON di.identities_hash_key = lpi.identities_hash_key
    LEFT JOIN raw_vault.link_person_contact lpc ON lpc.person_hash_key = hp_crm.person_hash_key
    LEFT JOIN raw_vault.hub_contact hc ON hc.contact_hash_key = lpc.contact_hash_key
    LEFT JOIN raw_vault.link_person_consent lpco ON lpco.person_hash_key = hp_crm.person_hash_key
    LEFT JOIN raw_vault.hub_consent hco ON hco.consent_hash_key = lpco.consent_hash_key
    LEFT JOIN raw_vault.sat_consent_crm sc ON sc.consent_hash_key = hco.consent_hash_key
),
legal_person AS (
    SELECT
        lp.global_person_identifier AS person_id,
        split_part(lp.global_person_identifier, '||', 1) AS crm_person_id,
        split_part(lp.global_person_identifier, '||', 2) AS sap_person_id,
        hp_crm.person_hash_key AS crm_person_hash_key,
        hp_sap.person_hash_key AS sap_person_hash_key,
        dg.geography_sk,
        di.identity_sk,
        NULL::text AS courtesy_title,
        NULL::text AS first_name,
        NULL::text AS last_name,
        lp.organization AS full_name,
        lp.person_type,
        NULL::date AS birth_date,
        NULL::text AS gender,
        NULL::text AS nationality,
        NULL::text AS marital_status,
        NULL::text AS occupation,
        adr.global_address_identifier AS home_address_id,
        adr.address_type,
        adr.address_line_2 AS street_address,
        adr.postal_code AS postcode,
        hc.contact_id,
        NULL::text AS home_phone_number,
        lp.phone_number AS work_phone_number,
        NULL::text AS personal_email,
        lp.email_address AS work_email,
        NULL::text AS job_title,
        NULL::text AS role,
        lp.organization AS company_name,
        lp.org_establishment_date AS date_of_constitution,
        lp.is_lead,
        NULL::text AS preferred_language,
        lp.source_identifier AS source_id,
        lp.source_type,
        lp.tenant_identifier AS tenant_id,
        NULL::text AS assessed_disability_degree,
        lp.operational_paperless_consent AS is_operational_paperless_consent,
        hco.consent_id,
        sc.opt_in_legitimate_interest AS is_opt_in_legitimate_interest,
        sc.opt_in_validated AS is_opt_in_validated,
        lp.effective_from::timestamp AS effective_from_ts,
        lp.effective_to::timestamp AS effective_to_ts,
        1::integer AS record_version
    FROM business_vault.bv_legal_person_master lp
    LEFT JOIN raw_vault.hub_person hp_crm ON hp_crm.person_id = split_part(lp.global_person_identifier, '||', 1)
    LEFT JOIN raw_vault.hub_person hp_sap ON hp_sap.person_id = split_part(lp.global_person_identifier, '||', 2)
    LEFT JOIN business_vault.bv_address_master adr ON adr.global_person_identifier = lp.global_person_identifier
    LEFT JOIN gold.dim_geography dg
        ON coalesce(dg.city, '') = coalesce(adr.city, '')
       AND coalesce(dg.state, '') = coalesce(adr.state, '')
       AND coalesce(dg.country, '') = coalesce(adr.country, '')
       AND coalesce(dg.region, '') = coalesce(adr.region, '')
    LEFT JOIN raw_vault.link_person_identities lpi ON lpi.person_hash_key = hp_crm.person_hash_key
    LEFT JOIN gold.dim_identity di ON di.identities_hash_key = lpi.identities_hash_key
    LEFT JOIN raw_vault.link_person_contact lpc ON lpc.person_hash_key = hp_crm.person_hash_key
    LEFT JOIN raw_vault.hub_contact hc ON hc.contact_hash_key = lpc.contact_hash_key
    LEFT JOIN raw_vault.link_person_consent lpco ON lpco.person_hash_key = hp_crm.person_hash_key
    LEFT JOIN raw_vault.hub_consent hco ON hco.consent_hash_key = lpco.consent_hash_key
    LEFT JOIN raw_vault.sat_consent_crm sc ON sc.consent_hash_key = hco.consent_hash_key
)
SELECT
    row_number() OVER (ORDER BY person_id)::bigint AS person_sk,
    geography_sk,
    identity_sk,
    person_id,
    crm_person_id,
    sap_person_id,
    crm_person_hash_key,
    sap_person_hash_key,
    courtesy_title,
    first_name,
    last_name,
    full_name,
    person_type,
    birth_date,
    gender,
    nationality,
    marital_status,
    occupation,
    home_address_id,
    address_type,
    street_address,
    postcode,
    contact_id,
    home_phone_number,
    work_phone_number,
    personal_email,
    work_email,
    job_title,
    role,
    company_name,
    date_of_constitution,
    is_lead,
    preferred_language,
    source_id,
    source_type,
    tenant_id,
    assessed_disability_degree,
    is_operational_paperless_consent,
    consent_id,
    is_opt_in_legitimate_interest,
    is_opt_in_validated,
    effective_from_ts,
    effective_to_ts,
    record_version,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp AS last_updated_ts,
    md5(concat_ws('|', person_id, first_name, last_name, company_name, birth_date::text, date_of_constitution::text)) AS attr_hash
FROM (
    SELECT * FROM natural_person
    UNION ALL
    SELECT * FROM legal_person
) p;

ALTER TABLE dim_person ADD PRIMARY KEY (person_sk);

CREATE TABLE dim_home AS
SELECT
    row_number() OVER (ORDER BY hm.global_home_identifier)::bigint AS home_sk,
    dp.product_sk,
    hm.global_home_identifier AS home_id,
    hm.policy_identifier,
    hm.is_existing_home_customer,
    hm.home_location AS home_risk_address,
    hm.home_state,
    hm.home_type,
    hm.roof_construction_type AS roof_construction_material_type,
    hm.wall_construction_type AS wall_construction_material_type,
    hm.effective_from::timestamp AS effective_from_ts,
    hm.effective_to::timestamp AS effective_to_ts,
    1::integer AS record_version,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp AS last_updated_ts,
    md5(concat_ws('|', hm.global_home_identifier, hm.home_type, hm.home_location, hm.home_state)) AS attr_hash,
    max(CASE WHEN x.rawdv_source_name = 'CRM' THEN x.rawdv_hashkey END) AS crm_home_hash_key,
    max(CASE WHEN x.rawdv_source_name = 'SAP' THEN x.rawdv_hashkey END) AS sap_home_hash_key
FROM business_vault.bv_home_master hm
LEFT JOIN gold.dim_product dp
    ON dp.product_id = hm.global_product_identifier
LEFT JOIN business_vault.bv_home_master_xref x
    ON x.global_home_identifier = hm.global_home_identifier
GROUP BY
    hm.global_home_identifier, dp.product_sk, hm.policy_identifier, hm.is_existing_home_customer,
    hm.home_location, hm.home_state, hm.home_type, hm.roof_construction_type,
    hm.wall_construction_type, hm.effective_from, hm.effective_to;

ALTER TABLE dim_home ADD PRIMARY KEY (home_sk);

CREATE TABLE dim_motor AS
SELECT
    row_number() OVER (ORDER BY mm.global_motor_identifier)::bigint AS motor_sk,
    dp.product_sk,
    mm.global_motor_identifier AS motor_id,
    mm.policy_identifier,
    NULL::text AS is_auto_decline_vehicle,
    mm.is_existing_motor_customer,
    NULLIF(mm.motor_lapsed_policies, '')::integer AS motor_lapsed_policies,
    mm.motor_sum_insured::numeric(18,4) AS vehicle_sum_insured_amt,
    mm.motor_risk_class_code AS vehicle_risk_class_code,
    mm.motor_parked_location AS vehicle_risk_address,
    mm.body_type AS vehicle_body_type,
    mm.fuel_type AS vehicle_fuel_type,
    mm.motor_variant AS vehicle_variant,
    mm.motor_age AS vehicle_age,
    mm.motor_class AS vehicle_class,
    mm.motor_model AS vehicle_model,
    mm.motor_owner_type AS vehicle_owner_type,
    mm.motor_registration_state AS vehicle_reg_state,
    mm.motor_type AS vehicle_type,
    extract(year from mm.motor_manufacturing_date)::integer AS vehicle_year,
    mm.license_status AS driver_license_status,
    mm.driver_experience_years,
    mm.effective_from::timestamp AS effective_from_ts,
    mm.effective_to::timestamp AS effective_to_ts,
    1::integer AS record_version,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp AS last_updated_ts,
    md5(concat_ws('|', mm.global_motor_identifier, mm.motor_class, mm.motor_model, mm.motor_type, mm.fuel_type)) AS attr_hash,
    max(CASE WHEN x.rawdv_source_name = 'CRM' THEN x.rawdv_hashkey END) AS crm_motor_hash_key,
    max(CASE WHEN x.rawdv_source_name = 'SAP' THEN x.rawdv_hashkey END) AS sap_motor_hash_key
FROM business_vault.bv_motor_master mm
LEFT JOIN gold.dim_product dp
    ON dp.product_id = mm.global_product_identifier
LEFT JOIN business_vault.bv_motor_master_xref x
    ON x.global_motor_identifier = mm.global_motor_identifier
GROUP BY
    mm.global_motor_identifier, dp.product_sk, mm.policy_identifier, mm.is_existing_motor_customer,
    mm.motor_lapsed_policies, mm.motor_sum_insured, mm.motor_risk_class_code, mm.motor_parked_location,
    mm.body_type, mm.fuel_type, mm.motor_variant, mm.motor_age, mm.motor_class, mm.motor_model,
    mm.motor_owner_type, mm.motor_registration_state, mm.motor_type, mm.motor_manufacturing_date,
    mm.license_status, mm.driver_experience_years, mm.effective_from, mm.effective_to;

ALTER TABLE dim_motor ADD PRIMARY KEY (motor_sk);

CREATE TABLE dim_account AS
WITH latest_sat AS (
    SELECT *
    FROM (
        SELECT sa.*, row_number() OVER (PARTITION BY account_hash_key ORDER BY load_date DESC) AS rn
        FROM raw_vault.sat_account_crm sa
    ) s
    WHERE rn = 1
)
SELECT
    row_number() OVER (ORDER BY ha.account_id)::bigint AS account_sk,
    ha.account_id,
    sa.account_number,
    sa.account_type,
    sa.account_status,
    sa.account_creation_type,
    NULLIF(sa.account_last_access, '')::timestamp AS account_last_access_ts,
    NULLIF(sa.account_last_change, '')::timestamp AS account_last_change_ts,
    ha.load_date AS effective_from_ts,
    timestamp '9999-12-31 00:00:00' AS effective_to_ts,
    1::integer AS record_version,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp AS last_updated_ts,
    md5(concat_ws('|', ha.account_id, sa.account_number, sa.account_type, sa.account_status)) AS attr_hash,
    ha.account_hash_key
FROM raw_vault.hub_account ha
LEFT JOIN latest_sat sa ON sa.account_hash_key = ha.account_hash_key;

ALTER TABLE dim_account ADD PRIMARY KEY (account_sk);

CREATE TABLE dim_customer AS
WITH latest_sat AS (
    SELECT *
    FROM (
        SELECT sc.*, row_number() OVER (PARTITION BY customer_hash_key ORDER BY load_date DESC) AS rn
        FROM raw_vault.sat_customer_crm sc
    ) s
    WHERE rn = 1
)
SELECT
    row_number() OVER (ORDER BY hc.customer_id)::bigint AS customer_sk,
    hc.customer_id,
    NULLIF(sc.customer_number, '')::integer AS customer_number,
    NULLIF(sc.customer_rating, '')::integer AS customer_rating,
    sc.customer_segment,
    sc.line_of_business,
    NULLIF(sc.nps_score, '')::double precision AS net_promoter_score,
    NULLIF(sc.customer_since, '')::date AS customer_since_date,
    sc.customer_status AS customer_status_code,
    sc.customer_status_reason,
    sc.income_band,
    sc.customer_satisfaction,
    sc.customer_age_band,
    sc.net_promotor_code_segment,
    NULLIF(sc.customer_onboarding_satisfaction_score, '')::integer AS customer_onboarding_satisfaction_score,
    sc.customer_onboarding_feedback,
    hc.load_date AS effective_from_ts,
    timestamp '9999-12-31 00:00:00' AS effective_to_ts,
    1::integer AS record_version,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp AS last_updated_ts,
    md5(concat_ws('|', hc.customer_id, sc.customer_number, sc.customer_status, sc.customer_segment)) AS attr_hash,
    hc.customer_hash_key
FROM raw_vault.hub_customer hc
LEFT JOIN latest_sat sc ON sc.customer_hash_key = hc.customer_hash_key;

ALTER TABLE dim_customer ADD PRIMARY KEY (customer_sk);

CREATE TABLE dim_broker AS
WITH latest_sat AS (
    SELECT *
    FROM (
        SELECT sb.*, row_number() OVER (PARTITION BY broker_hash_key ORDER BY load_date DESC) AS rn
        FROM raw_vault.sat_broker_crm sb
    ) s
    WHERE rn = 1
)
SELECT
    row_number() OVER (ORDER BY hb.agent_id)::bigint AS broker_sk,
    hb.agent_id,
    sb.agent_name,
    sb.agent_type,
    sb.agent_status,
    sb.agent_license_number,
    NULLIF(sb.agent_net_promoter_score, '')::numeric(3,1) AS agent_net_promoter_score,
    NULLIF(sb.agent_commission_percentage, '')::numeric(4,2) AS agent_commission_percentage,
    hb.load_date AS effective_from_ts,
    timestamp '9999-12-31 00:00:00' AS effective_to_ts,
    1::integer AS record_version,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp AS last_updated_ts,
    md5(concat_ws('|', hb.agent_id, sb.agent_name, sb.agent_type, sb.agent_status)) AS attr_hash,
    hb.broker_hash_key
FROM raw_vault.hub_broker hb
LEFT JOIN latest_sat sb ON sb.broker_hash_key = hb.broker_hash_key;

ALTER TABLE dim_broker ADD PRIMARY KEY (broker_sk);

CREATE TABLE dim_campaign AS
WITH latest_sat AS (
    SELECT *
    FROM (
        SELECT sc.*, row_number() OVER (PARTITION BY campaign_hash_key ORDER BY load_date DESC) AS rn
        FROM raw_vault.sat_campaign_crm sc
    ) s
    WHERE rn = 1
)
SELECT
    row_number() OVER (ORDER BY hc.campaign_id)::bigint AS campaign_sk,
    hc.campaign_id,
    sc.campaign_name,
    sc.campaign_type,
    NULLIF(sc.campaign_start_date, '')::date AS campaign_start_date,
    NULLIF(sc.campaign_end_date, '')::date AS campaign_end_date,
    sc.campaign_status,
    NULLIF(sc.campaign_budget, '')::integer AS campaign_budget,
    sc.campaign_target_audience,
    sc.campaign_marketing_source,
    sc.campaign_owner_department,
    sc.campaign_country,
    sc.campaign_conversion_goal,
    NULLIF(sc.number_of_clicks, '')::integer AS number_of_clicks,
    sc.is_active,
    NULLIF(sc.number_of_visits, '')::integer AS number_of_visits,
    NULLIF(sc.number_of_policy_purchases, '')::integer AS number_of_policy_purchases,
    NULLIF(sc.number_of_emails_sent, '')::integer AS number_of_emails_sent,
    NULLIF(sc.number_of_email_bounced, '')::integer AS number_of_email_bounced,
    NULLIF(sc.number_of_emails_delivered, '')::integer AS number_of_emails_delivered,
    NULLIF(sc.number_of_emails_opened, '')::integer AS number_of_emails_opened,
    NULLIF(sc.click_through_rate, '')::numeric(10,9) AS click_through_rate,
    NULLIF(sc.spend_amount, '')::numeric(12,4) AS spend_amt,
    NULLIF(sc.incremental_revenue, '')::numeric(18,4) AS incremental_revenue,
    sc.survey_wave,
    NULLIF(sc.total_number_of_respondents, '')::integer AS total_number_of_respondents,
    NULLIF(sc.number_of_respondents_aware, '')::integer AS number_of_respondents_aware,
    NULLIF(sc.number_of_promoters, '')::integer AS number_of_promoters,
    NULLIF(sc.number_of_passives, '')::integer AS number_of_passives,
    NULLIF(sc.number_of_detractors, '')::integer AS number_of_detractors,
    NULLIF(sc.number_of_followers, '')::integer AS number_of_followers,
    NULLIF(sc.number_of_likes, '')::integer AS number_of_likes,
    NULLIF(sc.number_of_comments, '')::integer AS number_of_comments,
    NULLIF(sc.number_of_shares, '')::integer AS number_of_shares,
    NULLIF(sc.number_of_brand_mentions, '')::integer AS number_of_brand_mentions,
    NULLIF(sc.number_of_category_mentions, '')::integer AS number_of_category_mentions,
    NULLIF(sc.number_of_impressions, '')::integer AS number_of_impressions,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp AS last_updated_ts,
    md5(concat_ws('|', hc.campaign_id, sc.campaign_name, sc.campaign_type, sc.campaign_status)) AS attr_hash,
    hc.campaign_hash_key
FROM raw_vault.hub_campaign hc
LEFT JOIN latest_sat sc ON sc.campaign_hash_key = hc.campaign_hash_key;

ALTER TABLE dim_campaign ADD PRIMARY KEY (campaign_sk);

CREATE TABLE dim_channel AS
WITH latest_sat AS (
    SELECT *
    FROM (
        SELECT sc.*, row_number() OVER (PARTITION BY channel_hash_key ORDER BY load_date DESC) AS rn
        FROM raw_vault.sat_channel_crm sc
    ) s
    WHERE rn = 1
)
SELECT
    row_number() OVER (ORDER BY hc.channel_id)::bigint AS channel_sk,
    hc.channel_id,
    sc.channel_name,
    sc.channel_type,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp AS last_updated_ts,
    md5(concat_ws('|', hc.channel_id, sc.channel_name, sc.channel_type)) AS attr_hash,
    hc.channel_hash_key
FROM raw_vault.hub_channel hc
LEFT JOIN latest_sat sc ON sc.channel_hash_key = hc.channel_hash_key;

ALTER TABLE dim_channel ADD PRIMARY KEY (channel_sk);

CREATE TABLE dim_claim AS
WITH latest_sat AS (
    SELECT *
    FROM (
        SELECT sc.*, row_number() OVER (PARTITION BY claim_hash_key ORDER BY load_date DESC) AS rn
        FROM raw_vault.sat_claim_crm sc
    ) s
    WHERE rn = 1
)
SELECT
    row_number() OVER (ORDER BY hc.claim_id)::bigint AS claim_sk,
    hc.claim_id,
    sc.claim_number,
    sc.claim_type,
    sc.claim_status,
    sc.claim_reason,
    sc.claim_channel,
    sc.claim_handler,
    NULLIF(sc.claim_reported_date, '')::date AS claim_reported_date,
    NULLIF(sc.claim_settlement_date, '')::date AS claim_settlement_date,
    sc.claim_product,
    sc.is_claim_suspicious,
    sc.is_claim_fraud,
    sc.claim_fraud_status,
    sc.claim_fraud_type,
    sc.claim_fraud_detection_method,
    sc.is_litigation,
    sc.litigation_reason,
    NULLIF(sc.litigation_start_date, '')::date AS litigation_start_date,
    NULLIF(sc.litigation_end_date, '')::date AS litigation_end_date,
    sc.litigation_outcome,
    NULLIF(sc.litigation_duration_days, '')::integer AS litigation_duration_days,
    NULLIF(sc.claim_fraud_detection_time_in_days, '')::integer AS claim_fraud_detection_time_in_days,
    sc.is_recovery_opportunity,
    NULLIF(sc.recovery_priority_score, '')::integer AS recovery_priority_score,
    sc.recovery_category,
    sc.recovery_source,
    NULLIF(sc.first_recovery_date, '')::date AS first_recovery_date,
    NULLIF(sc.last_recovery_date, '')::date AS last_recovery_date,
    sc.is_recovery_happened,
    NULLIF(sc.days_to_first_recovery, '')::integer AS days_to_first_recovery,
    NULLIF(sc.days_to_last_recovery, '')::integer AS days_to_last_recovery,
    NULLIF(sc.avg_days_to_close_claim, '')::integer AS avg_days_to_close_claim,
    sc.claim_fraud_outcome,
    sc.recovery_type,
    sc.recovery_band,
    sc.third_party_involved,
    NULLIF(sc.third_party_involved_overall_score, '')::numeric(10,9) AS third_party_involved_overall_score,
    sc.solicitor,
    NULLIF(sc.claim_amount, '')::numeric(18,4) AS claim_amt,
    NULLIF(sc.claims_paid, '')::numeric(18,4) AS claims_paid,
    NULLIF(sc.outstanding_reserve, '')::numeric(18,4) AS outstanding_reserve,
    NULLIF(sc.claims_expenses, '')::numeric(18,4) AS claims_expenses,
    NULLIF(sc.recovery_received, '')::numeric(18,4) AS recovery_received,
    NULLIF(sc.compensation_offered, '')::numeric(18,4) AS compensation_offered,
    NULLIF(sc.remediation_amount, '')::numeric(18,4) AS remediation_amt,
    NULLIF(sc.suspected_amount, '')::numeric(18,4) AS suspected_amt,
    NULLIF(sc.fraud_amount, '')::numeric(18,4) AS fraud_amt,
    NULLIF(sc.legal_expenses, '')::numeric(18,4) AS legal_expenses,
    sc.claim_band,
    NULLIF(sc.claim_band_sort, '')::integer AS claim_band_sort,
    sc.is_fault_claim,
    NULLIF(sc.claim_satisfaction_score, '')::integer AS claim_satisfaction_score,
    sc.claims_feedback,
    sc.is_claim_complaint_raised,
    hc.load_date AS effective_from_ts,
    timestamp '9999-12-31 00:00:00' AS effective_to_ts,
    1::integer AS record_version,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp AS last_updated_ts,
    md5(concat_ws('|', hc.claim_id, sc.claim_number, sc.claim_status, sc.claim_product)) AS attr_hash,
    hc.claim_hash_key
FROM raw_vault.hub_claim hc
LEFT JOIN latest_sat sc ON sc.claim_hash_key = hc.claim_hash_key;

ALTER TABLE dim_claim ADD PRIMARY KEY (claim_sk);

CREATE TABLE dim_insured_object AS
WITH latest_sat AS (
    SELECT *
    FROM (
        SELECT sio.*, row_number() OVER (PARTITION BY insured_object_hash_key ORDER BY load_date DESC) AS rn
        FROM raw_vault.sat_insured_object sio
    ) s
    WHERE rn = 1
)
SELECT
    row_number() OVER (ORDER BY hio.insured_object_id)::bigint AS insured_object_sk,
    hio.insured_object_id,
    sio.insured_object_type,
    sio.insured_object_sub_type,
    sio.insured_object_description AS insured_object_desc,
    NULLIF(sio.insured_value, '')::double precision AS insured_value,
    sio.currency_code,
    NULLIF(sio.insured_object_start_date, '')::date AS insured_object_start_date,
    NULLIF(sio.insured_object_end_date, '')::date AS insured_object_end_date,
    sio.insured_object_current_status,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp AS last_updated_ts,
    md5(concat_ws('|', hio.insured_object_id, sio.insured_object_type, sio.insured_object_sub_type)) AS attr_hash,
    hio.insured_object_hash_key
FROM raw_vault.hub_insured_object hio
LEFT JOIN latest_sat sio ON sio.insured_object_hash_key = hio.insured_object_hash_key;

ALTER TABLE dim_insured_object ADD PRIMARY KEY (insured_object_sk);

CREATE TABLE dim_marketing AS
WITH latest_pref AS (
    SELECT *
    FROM (
        SELECT smp.*, row_number() OVER (PARTITION BY marketing_preference_hash_key ORDER BY load_date DESC) AS rn
        FROM raw_vault.sat_marketing_preference_crm smp
    ) s
    WHERE rn = 1
),
latest_eng AS (
    SELECT *
    FROM (
        SELECT sme.*, row_number() OVER (PARTITION BY marketing_engagement_hash_key ORDER BY load_date DESC) AS rn
        FROM raw_vault.sat_marketing_engagement_crm sme
    ) s
    WHERE rn = 1
),
pref_person AS (
    SELECT l.person_hash_key, hmp.marketing_preference_id, hmp.marketing_preference_hash_key
    FROM raw_vault.link_person_marketing_preference l
    JOIN raw_vault.hub_marketing_preference hmp ON hmp.marketing_preference_hash_key = l.marketing_preference_hash_key
),
eng_person AS (
    SELECT l.person_hash_key, hme.marketing_engagement_id, hme.marketing_engagement_hash_key
    FROM raw_vault.link_person_marketing_engagement l
    JOIN raw_vault.hub_marketing_engagement hme ON hme.marketing_engagement_hash_key = l.marketing_engagement_hash_key
)
SELECT
    row_number() OVER (ORDER BY coalesce(pp.marketing_preference_id, ''), coalesce(ep.marketing_engagement_id, ''))::bigint AS marketing_sk,
    pp.marketing_preference_id,
    lp.any AS is_any_communication,
    NULL::text AS preferred_contact_method,
    lp.email_subscriptions AS is_email_subscriptions,
    lp.commercial_email AS is_commercial_email,
    lp.email AS is_personal_email,
    lp.call AS is_call,
    lp.sms AS is_sms,
    lp.postal_mail AS is_postal_mail,
    ep.marketing_engagement_id,
    le.opened_email AS is_opened_email,
    le.marketing_status,
    le.promotion_code,
    le.has_retention_team_interaction,
    NULLIF(le.customer_service_call_frequency, '')::integer AS customer_service_call_frequency,
    le.average_call_sentiment,
    NULLIF(le.engagement_score, '')::integer AS engagement_score,
    le.first_contact_resolution,
    coalesce(lp.load_date, le.load_date) AS effective_from_ts,
    timestamp '9999-12-31 00:00:00' AS effective_to_ts,
    1::integer AS record_version,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp AS last_updated_ts,
    md5(concat_ws('|', pp.marketing_preference_id, ep.marketing_engagement_id, lp.any, le.marketing_status)) AS attr_hash,
    coalesce(pp.person_hash_key, ep.person_hash_key) AS person_hash_key,
    pp.marketing_preference_hash_key,
    ep.marketing_engagement_hash_key
FROM pref_person pp
FULL OUTER JOIN eng_person ep
    ON ep.person_hash_key = pp.person_hash_key
LEFT JOIN latest_pref lp ON lp.marketing_preference_hash_key = pp.marketing_preference_hash_key
LEFT JOIN latest_eng le ON le.marketing_engagement_hash_key = ep.marketing_engagement_hash_key;

ALTER TABLE dim_marketing ADD PRIMARY KEY (marketing_sk);

CREATE TABLE dim_override AS
WITH latest_sat AS (
    SELECT *
    FROM (
        SELECT so.*, row_number() OVER (PARTITION BY override_hash_key ORDER BY load_date DESC) AS rn
        FROM raw_vault.sat_override_crm so
    ) s
    WHERE rn = 1
)
SELECT
    row_number() OVER (ORDER BY ho.override_id)::bigint AS override_sk,
    ho.override_id,
    so.override_reason,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp AS last_updated_ts,
    md5(concat_ws('|', ho.override_id, so.override_reason)) AS attr_hash,
    ho.override_hash_key
FROM raw_vault.hub_override ho
LEFT JOIN latest_sat so ON so.override_hash_key = ho.override_hash_key;

ALTER TABLE dim_override ADD PRIMARY KEY (override_sk);

CREATE TABLE dim_regulation AS
WITH latest_sat AS (
    SELECT *
    FROM (
        SELECT sr.*, row_number() OVER (PARTITION BY regulation_hash_key ORDER BY load_date DESC) AS rn
        FROM raw_vault.sat_regulation_crm sr
    ) s
    WHERE rn = 1
)
SELECT
    row_number() OVER (ORDER BY hr.regulation_id)::bigint AS regulation_sk,
    hr.regulation_id,
    sr.regulation_number,
    sr.regulation_name,
    sr.regulation_department,
    sr.regulation_region,
    sr.regulation_risk_level,
    sr.regulation_compliance_status,
    NULLIF(sr.regulation_date_raised, '')::date AS regulation_date_raised,
    NULLIF(sr.regulation_date_closed, '')::date AS regulation_date_closed,
    sr.regulation_owner,
    NULLIF(sr.regulation_deadline_date, '')::date AS regulation_deadline_date,
    sr.is_regulation_on_time,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp AS last_updated_ts,
    md5(concat_ws('|', hr.regulation_id, sr.regulation_number, sr.regulation_name, sr.regulation_compliance_status)) AS attr_hash,
    hr.regulation_hash_key
FROM raw_vault.hub_regulation hr
LEFT JOIN latest_sat sr ON sr.regulation_hash_key = hr.regulation_hash_key;

ALTER TABLE dim_regulation ADD PRIMARY KEY (regulation_sk);

CREATE TABLE dim_policy AS
WITH latest_sat AS (
    SELECT *
    FROM (
        SELECT sp.*, row_number() OVER (PARTITION BY policy_hash_key ORDER BY load_date DESC) AS rn
        FROM raw_vault.sat_policy_crm sp
    ) s
    WHERE rn = 1
)
SELECT
    row_number() OVER (ORDER BY hp.policy_id)::bigint AS policy_sk,
    dch.channel_sk,
    dp.product_sk,
    dio.insured_object_sk,
    dh.home_sk,
    dm.motor_sk,
    hp.policy_id,
    sp.policy_number,
    NULLIF(sp.policy_start_date, '')::timestamp AS policy_start_ts,
    NULLIF(sp.policy_end_date, '')::timestamp AS policy_end_ts,
    NULLIF(sp.policy_length, '')::integer AS policy_tenure,
    sp.policy_cycle,
    NULLIF(sp.renewal_date, '')::date AS renewal_date,
    sp.policy_status,
    sp.cover_option AS policy_cover_option,
    sp.sales_channel AS policy_sales_channel,
    sp.fraud_flag AS is_fraud,
    sp.quote_id,
    sp.policy_type,
    NULLIF(sp.policy_issue_date, '')::date AS policy_issue_date,
    sp.is_policy_renewal,
    sp.policy_cancellation_reason,
    NULLIF(sp.policy_sum_insured, '')::numeric(18,4) AS policy_sum_insured,
    NULLIF(sp.policy_retention_limit, '')::numeric(18,4) AS policy_retention_limit,
    NULLIF(sp.policy_risk_score, '')::integer AS policy_risk_score,
    sp.policy_risk_band,
    sp.is_auto_renew_enabled,
    NULLIF(sp.no_claims_discount_years, '')::integer AS no_claims_discount_years,
    sp.payment_method,
    sp.is_direct_debit_cancellation,
    NULLIF(sp.missed_payment_count, '')::integer AS missed_payment_count,
    sp.loyalty_discount_usage,
    sp.is_installment_default,
    NULLIF(sp.policy_renewal_satisfaction_score, '')::integer AS policy_renewal_satisfaction_score,
    sp.policy_renewal_feedback,
    sp.is_renewal_escalation,
    hp.load_date AS effective_from_ts,
    timestamp '9999-12-31 00:00:00' AS effective_to_ts,
    1::integer AS record_version,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    'ETL_SYSTEM' AS last_updated_by,
    current_timestamp AS last_updated_ts,
    md5(concat_ws('|', hp.policy_id, sp.policy_number, sp.policy_status, sp.policy_type, dp.product_id)) AS attr_hash,
    hp.policy_hash_key,
    dp.product_id,
    dp.product_type
FROM raw_vault.hub_policy hp
LEFT JOIN latest_sat sp ON sp.policy_hash_key = hp.policy_hash_key
LEFT JOIN raw_vault.link_policy_channel lpc ON lpc.policy_hash_key = hp.policy_hash_key
LEFT JOIN gold.dim_channel dch ON dch.channel_hash_key = lpc.channel_hash_key
LEFT JOIN raw_vault.link_policy_product lpp ON lpp.policy_hash_key = hp.policy_hash_key
LEFT JOIN gold.dim_product dp
    ON dp.crm_product_hash_key = lpp.product_hash_key OR dp.sap_product_hash_key = lpp.product_hash_key
LEFT JOIN raw_vault.link_policy_insured_object lpio ON lpio.policy_hash_key = hp.policy_hash_key
LEFT JOIN gold.dim_insured_object dio ON dio.insured_object_hash_key = lpio.insured_object_hash_key
LEFT JOIN raw_vault.link_insured_object_home lioh ON lioh.insured_object_hash_key = lpio.insured_object_hash_key
LEFT JOIN gold.dim_home dh
    ON dh.crm_home_hash_key = lioh.home_hash_key OR dh.sap_home_hash_key = lioh.home_hash_key
LEFT JOIN raw_vault.link_insured_object_motor liom ON liom.insured_object_hash_key = lpio.insured_object_hash_key
LEFT JOIN gold.dim_motor dm
    ON dm.crm_motor_hash_key = liom.motor_hash_key OR dm.sap_motor_hash_key = liom.motor_hash_key;

ALTER TABLE dim_policy ADD PRIMARY KEY (policy_sk);

CREATE TABLE fact_lead AS
WITH latest_sat AS (
    SELECT *
    FROM (
        SELECT sl.*, row_number() OVER (PARTITION BY lead_hash_key ORDER BY load_date DESC) AS rn
        FROM raw_vault.sat_lead_crm sl
    ) s
    WHERE rn = 1
),
lead_person_one AS (
    SELECT DISTINCT ON (lead_hash_key) lead_hash_key, person_hash_key
    FROM raw_vault.link_person_lead
    ORDER BY lead_hash_key, load_date DESC
),
person_marketing_one AS (
    SELECT DISTINCT ON (person_hash_key) person_hash_key, marketing_sk
    FROM gold.dim_marketing
    WHERE person_hash_key IS NOT NULL
    ORDER BY person_hash_key, marketing_sk
)
SELECT
    dp.person_sk,
    dd.date_sk,
    pm.marketing_sk,
    hl.lead_id,
    dp.person_id,
    sl.interested_level,
    NULLIF(sl.person_score, '')::integer AS person_score,
    sl.person_status,
    NULLIF(sl.converted_date, '')::timestamp AS lead_creation_ts,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    current_timestamp AS load_ts
FROM raw_vault.hub_lead hl
LEFT JOIN latest_sat sl ON sl.lead_hash_key = hl.lead_hash_key
LEFT JOIN lead_person_one lpl ON lpl.lead_hash_key = hl.lead_hash_key
LEFT JOIN gold.dim_person dp
    ON dp.crm_person_hash_key = lpl.person_hash_key OR dp.sap_person_hash_key = lpl.person_hash_key
LEFT JOIN gold.dim_date dd ON dd.full_date = NULLIF(sl.converted_date, '')::date
LEFT JOIN person_marketing_one pm ON pm.person_hash_key = lpl.person_hash_key;

CREATE TABLE fact_policy AS
WITH latest_sat AS (
    SELECT *
    FROM (
        SELECT sp.*, row_number() OVER (PARTITION BY policy_hash_key ORDER BY load_date DESC) AS rn
        FROM raw_vault.sat_policy_crm sp
    ) s
    WHERE rn = 1
),
policy_customer_one AS (
    SELECT DISTINCT ON (policy_hash_key) policy_hash_key, customer_hash_key
    FROM raw_vault.link_policy_customer
    ORDER BY policy_hash_key, load_date DESC
),
customer_person_one AS (
    SELECT DISTINCT ON (customer_hash_key) customer_hash_key, person_hash_key
    FROM raw_vault.link_customer_person
    ORDER BY customer_hash_key, load_date DESC
),
person_account_one AS (
    SELECT DISTINCT ON (person_hash_key) person_hash_key, account_hash_key
    FROM raw_vault.link_person_account
    ORDER BY person_hash_key, load_date DESC
),
person_marketing_one AS (
    SELECT DISTINCT ON (person_hash_key) person_hash_key, marketing_sk
    FROM gold.dim_marketing
    WHERE person_hash_key IS NOT NULL
    ORDER BY person_hash_key, marketing_sk
),
policy_channel_one AS (
    SELECT DISTINCT ON (policy_hash_key) policy_hash_key, channel_hash_key
    FROM raw_vault.link_policy_channel
    ORDER BY policy_hash_key, load_date DESC
),
policy_broker_one AS (
    SELECT DISTINCT ON (policy_hash_key) policy_hash_key, broker_hash_key
    FROM raw_vault.link_policy_broker
    ORDER BY policy_hash_key, load_date DESC
),
policy_claim_one AS (
    SELECT DISTINCT ON (policy_hash_key) policy_hash_key, claim_hash_key
    FROM raw_vault.link_claim_policy
    ORDER BY policy_hash_key, load_date DESC
),
policy_override_one AS (
    SELECT DISTINCT ON (policy_hash_key) policy_hash_key, override_hash_key
    FROM raw_vault.link_policy_override
    ORDER BY policy_hash_key, load_date DESC
),
policy_object_one AS (
    SELECT DISTINCT ON (policy_hash_key) policy_hash_key, insured_object_hash_key
    FROM raw_vault.link_policy_insured_object
    ORDER BY policy_hash_key, load_date DESC
)
SELECT
    dpol.policy_sk,
    dp.person_sk,
    dc.customer_sk,
    da.account_sk,
    dd.date_sk,
    pm.marketing_sk,
    dch.channel_sk,
    db.broker_sk,
    dcl.claim_sk,
    dov.override_sk,
    dio.insured_object_sk,
    dh.home_sk,
    dmo.motor_sk,
    dp.person_id,
    NULLIF(sp.number_of_active_claim, '')::integer AS active_claims_number,
    NULLIF(sp.number_of_previous_claim, '')::integer AS previous_claims_number,
    NULLIF(sp.declined_claims, '')::integer AS declined_claims_number,
    NULLIF(sp.gross_revenue, '')::numeric(18,4) AS policy_gross_revenue_amt,
    NULLIF(sp.net_revenue, '')::numeric(18,4) AS policy_net_revenue_amt,
    NULLIF(sp.renewal_amount_current_period, '')::numeric(18,4) AS policy_renewal_current_period_amt,
    NULLIF(sp.renewal_amount_next_period, '')::numeric(18,4) AS policy_renewal_next_period_amt,
    NULLIF(sp.policy_base_premium, '')::numeric(18,4) AS policy_base_premium,
    NULLIF(sp.gross_written_premium, '')::numeric(18,4) AS gross_written_premium,
    NULLIF(sp.earned_premium, '')::numeric(18,4) AS earned_premium,
    NULLIF(sp.incurred_but_not_reported, '')::numeric(18,4) AS incurred_but_not_reported,
    NULLIF(sp.operating_expenses, '')::numeric(18,4) AS operating_expenses,
    NULLIF(sp.administrative_expenses, '')::numeric(18,4) AS administrative_expenses,
    NULLIF(sp.profit_margin, '')::numeric(18,4) AS profit_margin,
    NULLIF(sp.taxes_and_levies, '')::numeric(18,4) AS taxes_and_levies,
    NULLIF(sp.amount_approved, '')::numeric(18,4) AS amt_approved,
    NULLIF(sp.ceded_premium, '')::numeric(18,4) AS ceded_premium,
    NULLIF(sp.commission_paid, '')::numeric(18,4) AS commission_paid,
    NULLIF(sp.ceded_commission, '')::numeric(18,4) AS ceded_commission,
    NULLIF(sp.exposure_amount, '')::numeric(18,4) AS exposure_amt,
    NULLIF(sp.investment_income, '')::numeric(18,4) AS investment_income,
    NULLIF(sp.underwriting_cycle_time_in_days, '')::integer AS underwriting_cycle_time_in_days,
    NULLIF(sp.underwriting_expenses, '')::numeric(18,4) AS underwriting_expenses,
    NULLIF(sp.transaction_date, '')::date AS transaction_date,
    sp.record_type,
    NULLIF(sp.discount, '')::numeric(18,4) AS discount,
    NULLIF(sp.override_commission, '')::numeric(18,4) AS override_commission,
    NULLIF(sp.partial_recovery_percentage, '')::numeric(18,4) AS partial_recovery_percentage,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    current_timestamp AS load_ts
FROM raw_vault.hub_policy hp
JOIN policy_customer_one lpcu ON lpcu.policy_hash_key = hp.policy_hash_key
LEFT JOIN latest_sat sp ON sp.policy_hash_key = hp.policy_hash_key
LEFT JOIN gold.dim_policy dpol ON dpol.policy_hash_key = hp.policy_hash_key
LEFT JOIN gold.dim_customer dc ON dc.customer_hash_key = lpcu.customer_hash_key
LEFT JOIN customer_person_one lcp ON lcp.customer_hash_key = lpcu.customer_hash_key
LEFT JOIN gold.dim_person dp
    ON dp.crm_person_hash_key = lcp.person_hash_key OR dp.sap_person_hash_key = lcp.person_hash_key
LEFT JOIN person_account_one lpa ON lpa.person_hash_key = lcp.person_hash_key
LEFT JOIN gold.dim_account da ON da.account_hash_key = lpa.account_hash_key
LEFT JOIN gold.dim_date dd ON dd.full_date = coalesce(NULLIF(sp.transaction_date, '')::date, NULLIF(sp.policy_start_date, '')::date)
LEFT JOIN person_marketing_one pm ON pm.person_hash_key = lcp.person_hash_key
LEFT JOIN policy_channel_one lpch ON lpch.policy_hash_key = hp.policy_hash_key
LEFT JOIN gold.dim_channel dch ON dch.channel_hash_key = lpch.channel_hash_key
LEFT JOIN policy_broker_one lpb ON lpb.policy_hash_key = hp.policy_hash_key
LEFT JOIN gold.dim_broker db ON db.broker_hash_key = lpb.broker_hash_key
LEFT JOIN policy_claim_one lclp ON lclp.policy_hash_key = hp.policy_hash_key
LEFT JOIN gold.dim_claim dcl ON dcl.claim_hash_key = lclp.claim_hash_key
LEFT JOIN policy_override_one lpo ON lpo.policy_hash_key = hp.policy_hash_key
LEFT JOIN gold.dim_override dov ON dov.override_hash_key = lpo.override_hash_key
LEFT JOIN policy_object_one po ON po.policy_hash_key = hp.policy_hash_key
LEFT JOIN gold.dim_insured_object dio ON dio.insured_object_hash_key = po.insured_object_hash_key
LEFT JOIN gold.dim_home dh
    ON split_part(dh.policy_identifier, '||', 1) = hp.policy_id
LEFT JOIN gold.dim_motor dmo
    ON split_part(dmo.policy_identifier, '||', 1) = hp.policy_id;

CREATE TABLE fact_quote AS
WITH latest_sat AS (
    SELECT *
    FROM (
        SELECT sq.*, row_number() OVER (PARTITION BY quote_hash_key ORDER BY load_date DESC) AS rn
        FROM raw_vault.sat_quote_crm sq
    ) s
    WHERE rn = 1
),
quote_person_one AS (
    SELECT DISTINCT ON (quote_hash_key) quote_hash_key, person_hash_key
    FROM raw_vault.link_quote_person
    ORDER BY quote_hash_key, load_date DESC
),
person_customer_one AS (
    SELECT DISTINCT ON (person_hash_key) person_hash_key, customer_hash_key
    FROM raw_vault.link_customer_person
    ORDER BY person_hash_key, load_date DESC
),
person_account_one AS (
    SELECT DISTINCT ON (person_hash_key) person_hash_key, account_hash_key
    FROM raw_vault.link_person_account
    ORDER BY person_hash_key, load_date DESC
),
person_marketing_one AS (
    SELECT DISTINCT ON (person_hash_key) person_hash_key, marketing_sk
    FROM gold.dim_marketing
    WHERE person_hash_key IS NOT NULL
    ORDER BY person_hash_key, marketing_sk
),
person_campaign_one AS (
    SELECT DISTINCT ON (person_hash_key) person_hash_key, campaign_hash_key
    FROM raw_vault.link_person_campaign
    ORDER BY person_hash_key, load_date DESC
),
quote_channel_one AS (
    SELECT DISTINCT ON (quote_hash_key) quote_hash_key, channel_hash_key
    FROM raw_vault.link_quote_channel
    ORDER BY quote_hash_key, load_date DESC
),
quote_broker_one AS (
    SELECT DISTINCT ON (quote_hash_key) quote_hash_key, broker_hash_key
    FROM raw_vault.link_quote_broker
    ORDER BY quote_hash_key, load_date DESC
),
quote_policy_one AS (
    SELECT DISTINCT ON (quote_hash_key) quote_hash_key, policy_hash_key
    FROM raw_vault.link_policy_quote
    ORDER BY quote_hash_key, load_date DESC
),
policy_object_one AS (
    SELECT DISTINCT ON (policy_hash_key) policy_hash_key, insured_object_hash_key
    FROM raw_vault.link_policy_insured_object
    ORDER BY policy_hash_key, load_date DESC
)
SELECT
    dp.person_sk,
    dc.customer_sk,
    da.account_sk,
    pm.marketing_sk,
    dd.date_sk,
    dcam.campaign_sk,
    dch.channel_sk,
    db.broker_sk,
    dio.insured_object_sk,
    dh.home_sk,
    dmo.motor_sk,
    hq.quote_id,
    dp.person_id,
    sq.quote_number,
    sq.quote_status,
    NULLIF(sq.gross_revenue, '')::numeric(18,4) AS quote_gross_revenue_amt,
    NULLIF(sq.net_revenue, '')::numeric(18,4) AS quote_net_revenue_amt,
    NULLIF(sq.renewal_amt_current_period, '')::numeric(18,4) AS quote_renewal_current_period_amt,
    NULLIF(sq.renewal_amt_next_period, '')::numeric(18,4) AS quote_renewal_next_period_amt,
    NULLIF(sq.quoted_premium, '')::numeric(18,4) AS quoted_premium,
    NULLIF(sq.quote_date, '')::date AS quote_date,
    sq.quote_month_name,
    NULLIF(sq.risk_score, '')::integer AS risk_score,
    sq.policy_complexity,
    sq.uw_approval_type,
    sq.rejection_reason,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    current_timestamp AS load_ts
FROM raw_vault.hub_quote hq
LEFT JOIN latest_sat sq ON sq.quote_hash_key = hq.quote_hash_key
LEFT JOIN quote_person_one lqp ON lqp.quote_hash_key = hq.quote_hash_key
LEFT JOIN gold.dim_person dp
    ON dp.crm_person_hash_key = lqp.person_hash_key OR dp.sap_person_hash_key = lqp.person_hash_key
LEFT JOIN person_customer_one pc ON pc.person_hash_key = lqp.person_hash_key
LEFT JOIN gold.dim_customer dc ON dc.customer_hash_key = pc.customer_hash_key
LEFT JOIN person_account_one pa ON pa.person_hash_key = lqp.person_hash_key
LEFT JOIN gold.dim_account da ON da.account_hash_key = pa.account_hash_key
LEFT JOIN person_marketing_one pm ON pm.person_hash_key = lqp.person_hash_key
LEFT JOIN gold.dim_date dd ON dd.full_date = NULLIF(sq.quote_date, '')::date
LEFT JOIN person_campaign_one pcam ON pcam.person_hash_key = lqp.person_hash_key
LEFT JOIN gold.dim_campaign dcam ON dcam.campaign_hash_key = pcam.campaign_hash_key
LEFT JOIN quote_channel_one lqch ON lqch.quote_hash_key = hq.quote_hash_key
LEFT JOIN gold.dim_channel dch ON dch.channel_hash_key = lqch.channel_hash_key
LEFT JOIN quote_broker_one lqb ON lqb.quote_hash_key = hq.quote_hash_key
LEFT JOIN gold.dim_broker db ON db.broker_hash_key = lqb.broker_hash_key
LEFT JOIN quote_policy_one qpo ON qpo.quote_hash_key = hq.quote_hash_key
LEFT JOIN raw_vault.hub_policy hp ON hp.policy_hash_key = qpo.policy_hash_key
LEFT JOIN policy_object_one po ON po.policy_hash_key = hp.policy_hash_key
LEFT JOIN gold.dim_insured_object dio ON dio.insured_object_hash_key = po.insured_object_hash_key
LEFT JOIN gold.dim_home dh
    ON split_part(dh.policy_identifier, '||', 1) = hp.policy_id
LEFT JOIN gold.dim_motor dmo
    ON split_part(dmo.policy_identifier, '||', 1) = hp.policy_id;

CREATE TABLE fact_complaint AS
WITH latest_sat AS (
    SELECT *
    FROM (
        SELECT sc.*, row_number() OVER (PARTITION BY complaint_hash_key ORDER BY load_date DESC) AS rn
        FROM raw_vault.sat_complaint_crm sc
    ) s
    WHERE rn = 1
),
complaint_policy_one AS (
    SELECT DISTINCT ON (complaint_hash_key) complaint_hash_key, policy_hash_key
    FROM raw_vault.link_complaint_policy
    ORDER BY complaint_hash_key, load_date DESC
),
policy_customer_one AS (
    SELECT DISTINCT ON (policy_hash_key) policy_hash_key, customer_hash_key
    FROM raw_vault.link_policy_customer
    ORDER BY policy_hash_key, load_date DESC
),
customer_person_one AS (
    SELECT DISTINCT ON (customer_hash_key) customer_hash_key, person_hash_key
    FROM raw_vault.link_customer_person
    ORDER BY customer_hash_key, load_date DESC
),
complaint_regulation_one AS (
    SELECT DISTINCT ON (complaint_hash_key) complaint_hash_key, regulation_hash_key
    FROM raw_vault.link_complaint_regulation
    ORDER BY complaint_hash_key, load_date DESC
),
policy_object_one AS (
    SELECT DISTINCT ON (policy_hash_key) policy_hash_key, insured_object_hash_key
    FROM raw_vault.link_policy_insured_object
    ORDER BY policy_hash_key, load_date DESC
)
SELECT
    dp.person_sk,
    dc.customer_sk,
    dr.regulation_sk,
    dch.channel_sk,
    dd.date_sk,
    dio.insured_object_sk,
    dh.home_sk,
    dmo.motor_sk,
    dp.person_id,
    hc.complaint_id,
    NULLIF(sc.complaint_date, '')::date AS complaint_date,
    NULLIF(sc.complaint_acknowledgement_date, '')::date AS complaint_acknowledgement_date,
    NULLIF(sc.complaint_resolved_date, '')::date AS complaint_resolved_date,
    sc.complaint_upheld_status,
    sc.is_financial_ombudsman_service_referral,
    sc.complaint_driver,
    sc.complaint_channel,
    NULLIF(sc.compensation_amount, '')::numeric(18,4) AS compensation_amt,
    sc.complaint_status,
    sc.insurance_category,
    'ETL_SYSTEM' AS created_by,
    current_timestamp AS created_ts,
    current_timestamp AS load_ts
FROM raw_vault.hub_complaint hc
LEFT JOIN latest_sat sc ON sc.complaint_hash_key = hc.complaint_hash_key
LEFT JOIN complaint_policy_one cpol ON cpol.complaint_hash_key = hc.complaint_hash_key
LEFT JOIN raw_vault.hub_policy hp ON hp.policy_hash_key = cpol.policy_hash_key
LEFT JOIN policy_customer_one pcust ON pcust.policy_hash_key = cpol.policy_hash_key
LEFT JOIN gold.dim_customer dc ON dc.customer_hash_key = pcust.customer_hash_key
LEFT JOIN customer_person_one cper ON cper.customer_hash_key = pcust.customer_hash_key
LEFT JOIN gold.dim_person dp
    ON dp.crm_person_hash_key = cper.person_hash_key OR dp.sap_person_hash_key = cper.person_hash_key
LEFT JOIN complaint_regulation_one cr ON cr.complaint_hash_key = hc.complaint_hash_key
LEFT JOIN gold.dim_regulation dr ON dr.regulation_hash_key = cr.regulation_hash_key
LEFT JOIN gold.dim_channel dch ON lower(dch.channel_name) = lower(sc.complaint_channel)
LEFT JOIN gold.dim_date dd ON dd.full_date = NULLIF(sc.complaint_date, '')::date
LEFT JOIN policy_object_one po ON po.policy_hash_key = hp.policy_hash_key
LEFT JOIN gold.dim_insured_object dio ON dio.insured_object_hash_key = po.insured_object_hash_key
LEFT JOIN gold.dim_home dh
    ON split_part(dh.policy_identifier, '||', 1) = hp.policy_id
LEFT JOIN gold.dim_motor dmo
    ON split_part(dmo.policy_identifier, '||', 1) = hp.policy_id;

-- Quick row count check.
SELECT 'dim_account' AS table_name, count(*) FROM dim_account UNION ALL
SELECT 'dim_broker', count(*) FROM dim_broker UNION ALL
SELECT 'dim_campaign', count(*) FROM dim_campaign UNION ALL
SELECT 'dim_channel', count(*) FROM dim_channel UNION ALL
SELECT 'dim_claim', count(*) FROM dim_claim UNION ALL
SELECT 'dim_customer', count(*) FROM dim_customer UNION ALL
SELECT 'dim_date', count(*) FROM dim_date UNION ALL
SELECT 'dim_geography', count(*) FROM dim_geography UNION ALL
SELECT 'dim_home', count(*) FROM dim_home UNION ALL
SELECT 'dim_identity', count(*) FROM dim_identity UNION ALL
SELECT 'dim_insured_object', count(*) FROM dim_insured_object UNION ALL
SELECT 'dim_marketing', count(*) FROM dim_marketing UNION ALL
SELECT 'dim_motor', count(*) FROM dim_motor UNION ALL
SELECT 'dim_override', count(*) FROM dim_override UNION ALL
SELECT 'dim_person', count(*) FROM dim_person UNION ALL
SELECT 'dim_policy', count(*) FROM dim_policy UNION ALL
SELECT 'dim_product', count(*) FROM dim_product UNION ALL
SELECT 'dim_regulation', count(*) FROM dim_regulation UNION ALL
SELECT 'fact_complaint', count(*) FROM fact_complaint UNION ALL
SELECT 'fact_lead', count(*) FROM fact_lead UNION ALL
SELECT 'fact_policy', count(*) FROM fact_policy UNION ALL
SELECT 'fact_quote', count(*) FROM fact_quote
ORDER BY table_name;

