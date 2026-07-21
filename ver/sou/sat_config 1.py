# Databricks notebook source
# DBTITLE 1,Sat Configs - Product ID 3

sat_account_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_account_crm_3",
  "object_type": "sat",
  "target_table": "sat_account_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(account_ref, origin_sys)) AS account_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  account_no AS account_number,
  account_type_txt AS account_type,
  last_access_dt AS account_last_access,
  last_change_dt AS account_last_change,
  account_create_type_txt AS account_creation_type,
  account_status_txt AS account_status
FROM crm_account_book_csv
""",
  "source_table": "crm_account_book_csv"
}

sat_consent_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_consent_crm_3",
  "object_type": "sat",
  "target_table": "sat_consent_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(concat(consent_ref, origin_sys)) AS consent_hash_key,
  CAST(pull_ts AS TIMESTAMP) AS load_date,
  opt_in_valid_ind AS opt_in_validated,
  opt_in_legit_ind AS opt_in_legitimate_interest
FROM crm_consent_snapshot_csv
""",
  "source_table": "crm_consent_snapshot_csv"
}

sat_contact_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_contact_crm_3",
  "object_type": "sat",
  "target_table": "sat_contact_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(contact_ref, origin_sys)) AS contact_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  email_home_txt AS personal_email,
  email_work_txt AS work_email,
  phone_work_txt AS work_phone,
  phone_home_txt AS home_phone
FROM crm_contact_point_csv
""",
  "source_table": "crm_contact_point_csv"
}

sat_customer_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_customer_crm_3",
  "object_type": "sat",
  "target_table": "sat_customer_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  customer_no AS customer_number,
  customer_status_txt AS customer_status,
  customer_status_reason_txt AS customer_status_reason,
  customer_since_dt AS customer_since,
  customer_rating_no AS customer_rating,
  customer_segment_txt AS customer_segment,
  lob_txt AS line_of_business,
  nps_score_no AS nps_score,
  NULL AS income_band,
  NULL AS customer_satisfaction,
  NULL AS customer_age_band,
  NULL AS net_promotor_code_segment,
  customer_onboarding_satisfaction_score AS customer_onboarding_satisfaction_score,
  customer_onboarding_feedback AS customer_onboarding_feedback
FROM crm_customer_portfolio_csv
UNION ALL
SELECT DISTINCT
  md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  NULL AS customer_number,
  NULL AS customer_status,
  NULL AS customer_status_reason,
  NULL AS customer_since,
  NULL AS customer_rating,
  NULL AS customer_segment,
  NULL AS line_of_business,
  NULL AS nps_score,
  src_income_band AS income_band,
  src_customer_satisfaction AS customer_satisfaction,
  src_customer_age_band AS customer_age_band,
  src_net_promotor_code_segment AS net_promotor_code_segment,
  NULL AS customer_onboarding_satisfaction_score,
  NULL AS customer_onboarding_feedback
FROM crm_enhanced_enrichments_csv
""",
  "source_table": "crm_customer_portfolio_csv,crm_enhanced_enrichments_csv"
}

sat_home_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_home_crm_3",
  "object_type": "sat",
  "target_table": "sat_home_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(property_ref, origin_sys)) AS home_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  wall_material_txt AS wall_construction,
  risk_address_txt AS home_risk_address,
  roof_material_txt AS roof_construction,
  property_type_txt AS home_type,
  property_state_cd AS home_state,
  existing_home_ind AS is_existing_home_customer
FROM crm_property_asset_csv
""",
  "source_table": "crm_property_asset_csv"
}

sat_address_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_address_crm_3",
  "object_type": "sat",
  "target_table": "sat_address_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(CAST(address_ref AS STRING), CAST(origin_sys AS STRING))) AS address_hash_key,
  CAST(pull_ts AS TIMESTAMP) AS load_date,
  street_txt AS street,
  postal_cd AS postcode,
  city_nm AS city,
  state_cd AS state,
  country_cd AS country,
  address_type_txt AS type,
  region_txt AS region
FROM crm_address_book_csv
""",
  "source_table": "crm_address_book_csv"
}

sat_identities_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_identities_crm_3",
  "object_type": "sat",
  "target_table": "sat_identities_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(identity_ref, origin_sys)) AS identities_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  ecid_txt AS ecid,
  hashed_email_txt AS hashed_email
FROM crm_identity_registry_csv
""",
  "source_table": "crm_identity_registry_csv"
}

sat_lead_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_lead_crm_3",
  "object_type": "sat",
  "target_table": "sat_lead_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(lead_ref, origin_sys)) AS lead_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  interest_bucket AS interested_level,
  contact_pref AS preferred_contact_method,
  person_score_no AS person_score,
  person_status_txt AS person_status,
  converted_dt AS converted_date
FROM crm_lead_register_csv
""",
  "source_table": "crm_lead_register_csv"
}

sat_legal_person_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_legal_person_crm_3",
  "object_type": "sat",
  "target_table": "sat_legal_person_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(concat(legal_ref, origin_sys)) AS legal_person_hash_key,
  to_timestamp(pull_ts) AS load_date,
  legal_job_title_txt AS job_title,
  lead_conv_dt AS converted_date,
  legal_status_txt AS person_status,
  legal_score_no AS person_score,
  legal_name AS company_name,
  legal_src_type AS source_type,
  legal_src_ref AS source_id,
  constitution_dt AS date_of_constitution
FROM crm_party_master_csv
WHERE Party_kind = 'Legal'
""",
  "source_table": "crm_party_master_csv"
}

sat_marketing_engagement_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_marketing_engagement_crm_3",
  "object_type": "sat",
  "target_table": "sat_marketing_engagement_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(src_marketing_engagement_ref, origin_sys)) AS marketing_engagement_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  src_promotion_code AS promotion_code,
  src_opened_email AS opened_email,
  src_marketing_status AS marketing_status,
  has_retention_team_interaction,
  customer_service_call_frequency,
  average_call_sentiment,
  engagement_score,
  first_contact_resolution
FROM crm_enhanced_enrichments_csv
UNION ALL
SELECT DISTINCT
  md5(CONCAT(src_marketing_engagement_ref, origin_sys)) AS marketing_engagement_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  src_promotion_code AS promotion_code,
  src_opened_email AS opened_email,
  src_marketing_status AS marketing_status,
  has_retention_team_interaction,
  customer_service_call_frequency,
  average_call_sentiment,
  engagement_score,
  first_contact_resolution
FROM crm_campaign_touch_csv
""",
  "source_table": "crm_enhanced_enrichments_csv,crm_campaign_touch_csv"
}

sat_marketing_preference_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_marketing_preference_crm_3",
  "object_type": "sat",
  "target_table": "sat_marketing_preference_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(preference_ref, origin_sys)) AS marketing_preference_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  sms_ind AS sms,
  email_ind AS email,
  email_sub_ind AS email_subscriptions,
  call_ind AS call,
  any_ind AS any,
  commercial_email_ind AS commercial_email,
  postal_mail_ind AS postal_mail
FROM crm_comm_preference_csv
""",
  "source_table": "crm_comm_preference_csv"
}

sat_motor_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_motor_crm_3",
  "object_type": "sat",
  "target_table": "sat_motor_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(concat(vehicle_ref, origin_sys)) AS motor_hash_key,
  to_timestamp(pull_ts) AS load_date,
  auto_decline_ind AS auto_decline_vehicle,
  body_style_txt AS body_type,
  fuel_type_txt AS fuel_type,
  license_status_txt AS license_status,
  existing_motor_ind AS is_existing_motor_customer,
  motor_lapse_cnt AS motor_lapsed_policies,
  garage_address_txt AS motor_risk_address,
  risk_class_cd AS risk_class_code,
  variant_nm AS variant,
  owner_type_txt AS vehicle_owner_type,
  registration_state_cd AS vehicle_regstate,
  vehicle_class_txt AS vehicle_class,
  model_nm AS vehicle_model,
  vehicle_type_txt AS vehicle_type,
  insured_value_amt AS motor_sum_insrd,
  vehicle_age_yrs AS vehicle_year,
  CAST(NULL AS INT) AS vehicle_age,
  driver_experience_years AS driver_experience_years
FROM crm_vehicle_asset_csv
UNION ALL
SELECT DISTINCT
  md5(concat(vehicle_ref, origin_sys)) AS motor_hash_key,
  to_timestamp(pull_ts) AS load_date,
  CAST(NULL AS STRING) AS auto_decline_vehicle,
  CAST(NULL AS STRING) AS body_type,
  CAST(NULL AS STRING) AS fuel_type,
  CAST(NULL AS STRING) AS license_status,
  CAST(NULL AS STRING) AS is_existing_motor_customer,
  CAST(NULL AS INT) AS motor_lapsed_policies,
  CAST(NULL AS STRING) AS motor_risk_address,
  CAST(NULL AS STRING) AS risk_class_code,
  CAST(NULL AS STRING) AS variant,
  CAST(NULL AS STRING) AS vehicle_owner_type,
  CAST(NULL AS STRING) AS vehicle_regstate,
  CAST(NULL AS STRING) AS vehicle_class,
  CAST(NULL AS STRING) AS vehicle_model,
  CAST(NULL AS STRING) AS vehicle_type,
  CAST(NULL AS DOUBLE) AS motor_sum_insrd,
  CAST(NULL AS INT) AS vehicle_year,
  src_vehicle_age AS vehicle_age,
  CAST(NULL AS INT) AS driver_experience_years
FROM crm_enhanced_enrichments_csv
""",
  "source_table": "crm_vehicle_asset_csv,crm_enhanced_enrichments_csv"
}

sat_natural_person_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_natural_person_crm_3",
  "object_type": "sat",
  "target_table": "sat_natural_person_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(natural_ref, origin_sys)) AS natural_person_hash_key,
  to_timestamp(pull_ts) AS load_date,
  given_nm AS first_name,
  family_nm AS last_name,
  display_nm AS full_name,
  title_txt AS courtesy_title,
  role_txt AS role,
  occupation_txt AS occupation,
  dob AS birth_date,
  birth_yr AS birth_year,
  nationality_txt AS nationality,
  gender_txt AS gender,
  marital_txt AS marital_status,
  disability_degree AS assesed_disability_degree,
  language_pref AS preferred_language,
  job_title_txt AS job_title
FROM crm_party_master_csv
WHERE party_kind = 'Natural'
""",
  "source_table": "crm_party_master_csv"
}

sat_person_CRM_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_person_CRM_3",
  "object_type": "sat",
  "target_table": "sat_person_CRM",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  tenant_cd AS tenant_id,
  lead_ind AS is_lead,
  party_kind AS type,
  paperless_ind AS operational_paperless_consent,
  src_party_ref AS source_id,
  src_party_type AS source_type
FROM crm_party_master_csv
""",
  "source_table": "crm_party_master_csv"
}

sat_policy_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_policy_crm_3",
  "object_type": "sat",
  "target_table": "sat_policy_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(concat(policy_ref, origin_sys)) AS policy_hash_key,
  CAST(pull_ts AS TIMESTAMP) AS load_date,
  cover_option_txt AS cover_option,
  declined_claim_cnt AS declined_claims,
  fraud_ind AS fraud_flag,
  gross_amt AS gross_revenue,
  net_amt AS net_revenue,
  active_claim_cnt AS number_of_active_claim,
  previous_claim_cnt AS number_of_previous_claim,
  policy_cycle_no AS policy_cycle,
  policy_end_dt AS policy_end_date,
  policy_term_months AS policy_length,
  policy_no AS policy_number,
  policy_start_dt AS policy_start_date,
  policy_status_txt AS policy_status,
  renewal_premium_curr AS renewal_amount_current_period,
  renewal_premium_next AS renewal_amount_next_period,
  renewal_dt AS renewal_date,
  sales_channel_txt AS sales_channel,
  NULL AS quote_id,
  NULL AS policy_type,
  NULL AS policy_issue_date,
  NULL AS is_policy_renewal,
  NULL AS policy_cancellation_reason,
  NULL AS policy_sum_insured,
  NULL AS policy_retention_limit,
  NULL AS policy_risk_score,
  NULL AS policy_risk_band,
  NULL AS policy_base_premium,
  NULL AS gross_written_premium,
  NULL AS earned_premium,
  NULL AS incurred_but_not_reported,
  NULL AS operating_expenses,
  NULL AS administrative_expenses,
  NULL AS profit_margin,
  NULL AS taxes_and_levies,
  NULL AS amount_approved,
  NULL AS ceded_premium,
  NULL AS commission_paid,
  NULL AS ceded_commission,
  NULL AS exposure_amount,
  NULL AS investment_income,
  NULL AS underwriting_cycle_time_in_days,
  NULL AS underwriting_expenses,
  NULL AS transaction_date,
  NULL AS record_type,
  NULL AS discount,
  NULL AS override_commission,
  NULL AS partial_recovery_percentage,
  is_auto_renew_enabled AS is_auto_renew_enabled,
  no_claims_discount_years AS no_claims_discount_years,
  payment_method AS payment_method,
  is_direct_debit_cancellation AS is_direct_debit_cancellation,
  missed_payment_count AS missed_payment_count,
  loyalty_discount_usage AS loyalty_discount_usage,
  is_installment_default AS is_installment_default,
  policy_renewal_satisfaction_score AS policy_renewal_satisfaction_score,
  crm_policy_register_csv AS policy_renewal_feedback,
  crm_policy_register_csv AS is_renewal_escalation
FROM crm_policy_register_csv

UNION ALL

SELECT DISTINCT
  md5(concat(src_quote_id, origin_sys)) AS policy_hash_key,
  CAST(pull_ts AS TIMESTAMP) AS load_date,
  NULL AS cover_option,
  NULL AS declined_claims,
  NULL AS fraud_flag,
  NULL AS gross_revenue,
  NULL AS net_revenue,
  NULL AS number_of_active_claim,
  NULL AS number_of_previous_claim,
  NULL AS policy_cycle,
  NULL AS policy_end_date,
  NULL AS policy_length,
  NULL AS policy_number,
  NULL AS policy_start_date,
  NULL AS policy_status,
  NULL AS renewal_amount_current_period,
  NULL AS renewal_amount_next_period,
  NULL AS renewal_date,
  NULL AS sales_channel,
  src_quote_id AS quote_id,
  src_policy_type AS policy_type,
  src_policy_issue_date AS policy_issue_date,
  src_is_policy_renewal AS is_policy_renewal,
  src_policy_cancellation_reason AS policy_cancellation_reason,
  src_policy_sum_insured AS policy_sum_insured,
  src_policy_retention_limit AS policy_retention_limit,
  src_policy_risk_score AS policy_risk_score,
  src_policy_risk_band AS policy_risk_band,
  src_policy_base_premium AS policy_base_premium,
  src_gross_written_premium AS gross_written_premium,
  src_operating_expenses AS earned_premium,
  src_incurred_but_not_reported AS incurred_but_not_reported,
  src_operating_expenses AS operating_expenses,
  src_administrative_expenses AS administrative_expenses,
  src_profit_margin AS profit_margin,
  src_taxes_and_levies AS taxes_and_levies,
  src_amount_approved AS amount_approved,
  src_ceded_premium AS ceded_premium,
  src_commission_paid AS commission_paid,
  src_ceded_commission AS ceded_commission,
  src_exposure_amount AS exposure_amount,
  src_investment_income AS investment_income,
  src_underwriting_cycle_time_in_days AS underwriting_cycle_time_in_days,
  src_underwriting_expenses AS underwriting_expenses,
  src_transaction_date AS transaction_date,
  src_record_type AS record_type,
  src_discount AS discount,
  src_override_commission AS override_commission,
  src_partial_recovery_percentage AS partial_recovery_percentage,
  NULL AS is_auto_renew_enabled,
  NULL AS no_claims_discount_years,
  NULL AS payment_method,
  NULL AS is_direct_debit_cancellation,
  NULL AS missed_payment_count,
  NULL AS loyalty_discount_usage,
  NULL AS is_installment_default,
  NULL AS policy_renewal_satisfaction_score,
  NULL AS policy_renewal_feedback,
  NULL AS is_renewal_escalation
FROM crm_enhanced_enrichments_csv
""",
  "source_table": "crm_policy_register_csv,crm_enhanced_enrichments_csv"
}

sat_product_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_product_crm_3",
  "object_type": "sat",
  "target_table": "sat_product_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(concat(product_ref, origin_sys)) AS product_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  product_type_txt AS type,
  product_variant AS product_variant,
  product_cd AS product_name,
  product_launch_dt AS product_launch_date,
  product_status_txt AS product_status,
  product_lob_cd AS product_line_of_business_code,
  underwriting_group_txt AS underwriting_group,
  regulatory_approval_cd AS regulatory_approval_code
FROM crm_product_catelog_csv
UNION ALL
SELECT DISTINCT
  md5(concat(product_ref, origin_sys)) AS product_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  product_type_txt AS type,
  product_variant AS product_variant,
  product_cd AS product_name,
  product_launch_dt AS product_launch_date,
  product_status_txt AS product_status,
  product_lob_cd AS product_line_of_business_code,
  underwriting_group_txt AS underwriting_group,
  regulatory_approval_cd AS regulatory_approval_code
FROM crm_product_catalog_csv
""",
  "source_table": "crm_product_catelog_csv,crm_product_catalog_csv"
}

sat_quote_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_quote_crm_3",
  "object_type": "sat",
  "target_table": "sat_quote_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(concat(quote_ref, origin_sys)) AS quote_hash_key,
  to_timestamp(pull_ts) AS load_date,
  gross_amt AS gross_revenue,
  net_amt AS net_revenue,
  quote_no AS quote_number,
  quote_status_txt AS quote_status,
  renewal_amt_curr AS renewal_amt_current_period,
  renewal_amt_next AS renewal_amt_next_period,
  quoted_premium AS quoted_premium,
  quoted_date AS quote_date,
  quote_month_name AS quote_month_name,
  risk_score AS risk_score,
  policy_complexity AS policy_complexity,
  uw_approval_type AS uw_approval_type,
  rejection_reason AS rejection_reason
FROM crm_quote_register_csv
""",
  "source_table": "crm_quote_register_csv"
}

sat_broker_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_broker_crm_3",
  "object_type": "sat",
  "target_table": "sat_broker_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(concat(src_agent_id, src_system)) AS broker_hash_key,
  CAST(pull_ts AS TIMESTAMP) AS load_date,
  src_agent_name AS agent_name,
  src_agent_type AS agent_type,
  src_agent_status AS agent_status,
  src_agent_license_number AS agent_license_number,
  src_agent_net_promoter_score AS agent_net_promoter_score,
  src_agent_commission_percentage AS agent_commission_percentage
FROM crm_broker_book_csv
""",
  "source_table": "crm_broker_book_csv"
}

sat_channel_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_channel_crm_3",
  "object_type": "sat",
  "target_table": "sat_channel_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(src_channel_id, src_system)) AS channel_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  src_channel_name AS channel_name,
  src_channel_type AS channel_type
FROM crm_channel_catalog_csv
""",
  "source_table": "crm_channel_catalog_csv"
}

sat_regulation_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_regulation_crm_3",
  "object_type": "sat",
  "target_table": "sat_regulation_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(src_regulation_id, src_system)) AS regulation_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  src_regulation_number AS regulation_number,
  src_regulation_name AS regulation_name,
  src_regulation_department AS regulation_department,
  src_regulation_region AS regulation_region,
  src_regulation_risk_level AS regulation_risk_level,
  src_regulation_compliance_status AS regulation_compliance_status,
  src_regulation_date_raised AS regulation_date_raised,
  src_regulation_date_closed AS regulation_date_closed,
  src_regulation_owner AS regulation_owner,
  src_regulation_deadline_date AS regulation_deadline_date,
  src_is_regulation_on_time AS is_regulation_on_time
FROM crm_regulation_register_csv
""",
  "source_table": "crm_regulation_register_csv"
}

sat_campaign_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_campaign_crm_3",
  "object_type": "sat",
  "target_table": "sat_campaign_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(concat(src_campaign_id, src_system)) AS campaign_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  src_campaign_name AS campaign_name,
  src_campaign_type AS campaign_type,
  src_campaign_start_date AS campaign_start_date,
  src_campaign_end_date AS campaign_end_date,
  src_campaign_status AS campaign_status,
  src_campaign_budget AS campaign_budget,
  src_campaign_target_audience AS campaign_target_audience,
  src_campaign_marketing_source AS campaign_marketing_source,
  src_campaign_owner_department AS campaign_owner_department,
  src_campaign_country AS campaign_country,
  src_campaign_conversion_goal AS campaign_conversion_goal,
  src_number_of_impressions AS number_of_impressions,
  src_number_of_clicks AS number_of_clicks,
  src_is_active AS is_active,
  src_number_of_visits AS number_of_visits,
  src_number_of_policy_purchases AS number_of_policy_purchases,
  src_number_of_emails_sent AS number_of_emails_sent,
  src_number_of_email_bounced AS number_of_email_bounced,
  src_number_of_emails_delivered AS number_of_emails_delivered,
  src_number_of_emails_opened AS number_of_emails_opened,
  src_click_through_rate AS click_through_rate,
  src_spend_amount AS spend_amount,
  src_incremental_revenue AS incremental_revenue,
  src_survey_wave AS survey_wave,
  src_total_number_of_respondents AS total_number_of_respondents,
  src_number_of_respondents_aware AS number_of_respondents_aware,
  src_number_of_promoters AS number_of_promoters,
  src_number_of_passives AS number_of_passives,
  src_number_of_detractors AS number_of_detractors,
  src_number_of_followers AS number_of_followers,
  src_number_of_likes AS number_of_likes,
  src_number_of_comments AS number_of_comments,
  src_number_of_shares AS number_of_shares,
  src_number_of_brand_mentions AS number_of_brand_mentions,
  src_number_of_category_mentions AS number_of_category_mentions
FROM crm_campaign_register_csv
""",
  "source_table": "crm_campaign_register_csv"
}

sat_override_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_override_crm_3",
  "object_type": "sat",
  "target_table": "sat_override_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(src_override_id, src_system)) AS override_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  src_override_reason AS override_reason
FROM crm_override_register_csv
""",
  "source_table": "crm_override_register_csv"
}

sat_claim_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_claim_crm_3",
  "object_type": "sat",
  "target_table": "sat_claim_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(src_claim_id, src_system)) AS claim_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  src_claim_number AS claim_number,
  src_claim_type AS claim_type,
  src_claim_status AS claim_status,
  src_claim_reason AS claim_reason,
  src_claim_channel AS claim_channel,
  src_claim_handler AS claim_handler,
  src_claim_reported_date AS claim_reported_date,
  src_claim_settlement_date AS claim_settlement_date,
  src_claim_product AS claim_product,
  src_is_claim_suspicious AS is_claim_suspicious,
  src_is_claim_fraud AS is_claim_fraud,
  src_claim_fraud_status AS claim_fraud_status,
  src_claim_fraud_type AS claim_fraud_type,
  src_claim_fraud_detection_method AS claim_fraud_detection_method,
  src_is_litigation AS is_litigation,
  src_litigation_reason AS litigation_reason,
  src_litigation_start_date AS litigation_start_date,
  src_litigation_end_date AS litigation_end_date,
  src_litigation_outcome AS litigation_outcome,
  src_litigation_duration_days AS litigation_duration_days,
  src_claim_fraud_detection_time_in_days AS claim_fraud_detection_time_in_days,
  src_is_recovery_opportunity AS is_recovery_opportunity,
  src_recovery_priority_score AS recovery_priority_score,
  src_recovery_category AS recovery_category,
  src_recovery_source AS recovery_source,
  src_first_recovery_date AS first_recovery_date,
  src_last_recovery_date AS last_recovery_date,
  src_is_recovery_happened AS is_recovery_happened,
  src_days_to_first_recovery AS days_to_first_recovery,
  src_days_to_last_recovery AS days_to_last_recovery,
  src_avg_days_to_close_claim AS avg_days_to_close_claim,
  src_claim_fraud_outcome AS claim_fraud_outcome,
  src_recovery_type AS recovery_type,
  src_recovery_band AS recovery_band,
  src_third_party_involved AS third_party_involved,
  src_third_party_involved_overall_score AS third_party_involved_overall_score,
  src_solicitor AS solicitor,
  src_claim_amount AS claim_amount,
  src_claims_paid AS claims_paid,
  src_outstanding_reserve AS outstanding_reserve,
  src_claims_expenses AS claims_expenses,
  src_recovery_received AS recovery_received,
  src_compensation_offered AS compensation_offered,
  src_remediation_amount AS remediation_amount,
  src_suspectd_amount AS suspected_amount,
  src_fraud_amount AS fraud_amount,
  src_legal_expenses AS legal_expenses,
  src_claim_band AS claim_band,
  src_claim_band_sort AS claim_band_sort,
  src_is_fault_claim AS is_fault_claim,
  src_claim_satisfaction_score AS claim_satisfaction_score,
  src_claims_feedback AS claims_feedback,
  src_is_claim_complaint_raised AS is_claim_complaint_raised
FROM crm_claim_register_csv
""",
  "source_table": "crm_claim_register_csv"
}

sat_complaint_crm_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_complaint_crm_3",
  "object_type": "sat",
  "target_table": "sat_complaint_crm",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(src_complaint_id, src_system)) AS complaint_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  src_complaint_date AS complaint_date,
  src_complaint_acknowledgement_date AS complaint_acknowledgement_date,
  src_complaint_resolved_date AS complaint_resolved_date,
  src_complaint_upheld_status AS complaint_upheld_status,
  src_is_financial_ombudsman_service_referral AS is_financial_ombudsman_service_referral,
  src_complaint_driver AS complaint_driver,
  src_complaint_channel AS complaint_channel,
  src_compensation_amount AS compensation_amount,
  src_insurance_category AS insurance_category,
  src_complaint_status AS complaint_status,
  src_complaint_feedback AS complaint_feedback,
  src_customer_complaint_satisfaction_score AS customer_complaint_satisfaction_score
FROM crm_complaint_register_csv
""",
  "source_table": "crm_complaint_register_csv"
}

sat_insured_object_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_insured_object_3",
  "object_type": "sat",
  "target_table": "sat_insured_object",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(concat(insured_object_id, origin_sys)) AS insured_object_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  insured_object_type,
  insured_object_sub_type,
  insured_object_description,
  insured_value,
  currency_code,
  insured_object_start_date,
  insured_object_end_date,
  insured_object_current_status
FROM crm_property_asset_csv

UNION ALL

SELECT DISTINCT
  md5(concat(insured_object_id, origin_sys)) AS insured_object_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  insured_object_type,
  insured_object_sub_type,
  insured_object_description,
  insured_value,
  currency_code,
  insured_object_start_date,
  insured_object_end_date,
  insured_object_current_status
FROM crm_vehicle_asset_csv
""",
  "source_table": "crm_property_asset_csv,crm_vehicle_asset_csv"
}

sat_person_SAP_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_person_SAP_3",
  "object_type": "sat",
  "target_table": "sat_person_SAP",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(person_id, origin_sys)) AS person_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  person_type AS person_type,
  email_address AS email_address,
  phone_number AS phone_number
FROM sap_person_db
""",
  "source_table": "sap_person_db"
}

sat_natural_person_sap_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_natural_person_sap_3",
  "object_type": "sat",
  "target_table": "sat_natural_person_sap",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(person_id, origin_sys)) AS natural_person_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  first_name AS first_name,
  middle_name AS middle_name,
  last_name AS last_name,
  date_of_birth AS date_of_birth,
  gender AS gender,
  occupation AS occupation
FROM sap_person_db
WHERE person_type = 'Natural'
""",
  "source_table": "sap_person_db"
}

sat_legal_person_sap_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_legal_person_sap_3",
  "object_type": "sat",
  "target_table": "sat_legal_person_sap",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(person_id, origin_sys)) AS legal_person_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  organization AS organization,
  org_establishment_date AS org_establishment_date
FROM sap_person_db
WHERE person_type = 'Legal'
""",
  "source_table": "sap_person_db"
}

sat_address_sap_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_address_sap_3",
  "object_type": "sat",
  "target_table": "sat_address_sap",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(address_id, origin_sys)) AS address_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  address_line_1 AS address_line_1,
  address_line_2 AS address_line_2,
  city AS city,
  state AS state,
  country AS country,
  zipcode AS zipcode,
  person_id AS person_id
FROM sap_address_db
""",
  "source_table": "sap_address_db"
}

sat_motor_sap_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_motor_sap_3",
  "object_type": "sat",
  "target_table": "sat_motor_sap",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(motor_id, origin_sys)) AS motor_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  motor_class,
  motor_model,
  motor_type,
  fuel_type,
  motor_parked_location,
  manufacturing_date,
  gear_type,
  body_colour,
  policy_id,
  product_id
FROM sap_motor_db
""",
  "source_table": "sap_motor_db"
}

sat_home_sap_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_home_sap_3",
  "object_type": "sat",
  "target_table": "sat_home_sap",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(home_id, origin_sys)) AS home_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  home_type,
  home_location,
  roof_material,
  wall_type,
  NULL AS policy_id,
  NULL AS product_id
FROM sap_home_db
UNION ALL
SELECT DISTINCT
  md5(CONCAT(policy_id, origin_sys)) AS home_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  NULL AS home_type,
  NULL AS home_location,
  NULL AS roof_material,
  NULL AS wall_type,
  policy_id,
  product_id
FROM sap_motor_db
""",
  "source_table": "sap_home_db,sap_motor_db"
}

sat_product_sap_3 =  {
  "product_id": "3",
  "vault_object_id": "sat_product_sap_3",
  "object_type": "sat",
  "target_table": "sat_product_sap",
  "process_order": "3",
  "build_sql": f"""
SELECT DISTINCT
  md5(CONCAT(product_id, origin_sys)) AS product_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  product_type AS product_type,
  product_sub_type AS product_sub_type,
  product_name AS product_name,
  product_start_date AS product_start_date,
  line_of_business AS line_of_business
FROM sap_product_db
""",
  "source_table": "sap_product_db"
}
