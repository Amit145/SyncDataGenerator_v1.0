-- Raw PRD2 to enhanced/MLOps Data Vault deltas.
-- Dialect: MySQL 8 compatible.
-- These queries assume base vault tables are already loaded from PRD1.
-- PRD2 raw columns use src_* names. Business references are hashed into vault hash keys.

-- -------------------------
-- Added entity hubs/satellites
-- -------------------------

INSERT INTO hub_address (address_hash_key, load_date, record_source, address_id)
SELECT DISTINCT MD5(src_address_ref), src_extract_ts, src_system, src_address_id
FROM raw_prd2_address_book
WHERE src_address_ref <> '';

INSERT INTO sat_address
SELECT MD5(src_address_ref) AS address_hash_key, src_extract_ts AS load_date,
       src_street, src_postcode, src_city, src_state, src_country, src_type, src_region
FROM raw_prd2_address_book;

INSERT INTO hub_broker (broker_hash_key, load_date, record_source, agent_id)
SELECT DISTINCT MD5(src_broker_ref), src_extract_ts, src_system, COALESCE(src_agent_id, src_broker_id)
FROM raw_prd2_broker_book
WHERE src_broker_ref <> '';

INSERT INTO sat_broker
SELECT MD5(src_broker_ref) AS broker_hash_key, src_extract_ts AS load_date,
       src_agent_name, src_agent_type, src_agent_status, src_agent_license_number,
       src_agent_net_promoter_score, src_agent_commission_percentage
FROM raw_prd2_broker_book;

INSERT INTO hub_campaign (campaign_hash_key, load_date, record_source, campaign_id)
SELECT DISTINCT MD5(src_campaign_ref), src_extract_ts, src_system, src_campaign_id
FROM raw_prd2_campaign_register
WHERE src_campaign_ref <> '';

INSERT INTO sat_campaign
SELECT MD5(src_campaign_ref) AS campaign_hash_key, src_extract_ts AS load_date,
       src_campaign_name, src_campaign_type, src_campaign_start_date, src_campaign_end_date,
       src_campaign_status, src_campaign_budget, src_campaign_target_audience,
       src_campaign_marketing_source, src_campaign_owner_department, src_campaign_country,
       src_campaign_conversion_goal, src_number_of_impressions, src_number_of_clicks,
       src_is_active, src_number_of_visits, src_number_of_policy_purchases,
       src_number_of_emails_sent, src_number_of_email_bounced, src_number_of_emails_delivered,
       src_number_of_emails_opened, src_click_through_rate, src_spend_amount,
       src_incremental_revenue, src_survey_wave, src_total_number_of_respondents,
       src_number_of_respondents_aware, src_number_of_promoters, src_number_of_passives,
       src_number_of_detractors, src_number_of_followers, src_number_of_likes,
       src_number_of_comments, src_number_of_shares, src_number_of_brand_mentions,
       src_number_of_category_mentions
FROM raw_prd2_campaign_register;

INSERT INTO hub_channel (channel_hash_key, load_date, record_source, channel_id)
SELECT DISTINCT MD5(src_channel_ref), src_extract_ts, src_system, src_channel_id
FROM raw_prd2_channel_catalog
WHERE src_channel_ref <> '';

INSERT INTO sat_channel
SELECT MD5(src_channel_ref) AS channel_hash_key, src_extract_ts AS load_date,
       src_channel_name, src_channel_type
FROM raw_prd2_channel_catalog;

INSERT INTO hub_claim (claim_hash_key, load_date, record_source, claim_id)
SELECT DISTINCT MD5(src_claim_ref), src_extract_ts, src_system, src_claim_id
FROM raw_prd2_claim_register
WHERE src_claim_ref <> '';

INSERT INTO sat_claim
SELECT MD5(src_claim_ref) AS claim_hash_key, src_extract_ts AS load_date,
       src_claim_number, src_claim_type, src_claim_status, src_claim_reason,
       src_claim_channel, src_claim_handler, src_claim_reported_date,
       src_claim_settlement_date, src_claim_product, src_is_claim_suspicious,
       src_is_claim_fraud, src_claim_fraud_status, src_claim_fraud_type,
       src_claim_fraud_detection_method, src_is_litigation, src_litigation_reason,
       src_litigation_start_date, src_litigation_end_date, src_litigation_outcome,
       src_litigation_duration_days, src_claim_fraud_detection_time_in_days,
       src_is_recovery_opportunity, src_recovery_priority_score, src_recovery_category,
       src_recovery_source, src_first_recovery_date, src_last_recovery_date,
       src_is_recovery_happened, src_days_to_first_recovery, src_days_to_last_recovery,
       src_avg_days_to_close_claim, src_claim_fraud_outcome, src_recovery_type,
       src_recovery_band, src_third_party_involved, src_third_party_involved_overall_score,
       src_solicitor, src_claim_amount, src_claims_paid, src_outstanding_reserve,
       src_claims_expenses, src_recovery_received, src_compensation_offered,
       src_remediation_amount, src_suspected_amount, src_fraud_amount,
       src_legal_expenses, src_claim_band, src_claim_band_sort, src_is_fault_claim,
       src_claim_satisfaction_score
FROM raw_prd2_claim_register;

INSERT INTO hub_complaint (complaint_hash_key, load_date, record_source, complaint_id)
SELECT DISTINCT MD5(src_complaint_ref), src_extract_ts, src_system, src_complaint_id
FROM raw_prd2_complaint_register
WHERE src_complaint_ref <> '';

INSERT INTO sat_complaint
SELECT MD5(src_complaint_ref) AS complaint_hash_key, src_extract_ts AS load_date,
       src_complaint_date, src_complaint_acknowledgement_date, src_complaint_resolved_date,
       src_complaint_upheld_status, src_is_financial_ombudsman_service_referral,
       src_complaint_driver, src_complaint_channel, src_compensation_amount,
       src_insurance_category, src_complaint_status
FROM raw_prd2_complaint_register;

INSERT INTO hub_insured_object (insured_object_hash_key, load_date, record_source, insured_object_id)
SELECT DISTINCT MD5(src_insured_object_ref), src_extract_ts, src_system, src_insured_object_id
FROM raw_prd2_insured_object_register
WHERE src_insured_object_ref <> '';

INSERT INTO sat_insured_object
SELECT MD5(src_insured_object_ref) AS insured_object_hash_key, src_extract_ts AS load_date,
       src_insured_object_type, src_insured_object_sub_type, src_insured_object_description,
       src_insured_value, src_currency_code, src_insured_object_start_date,
       src_insured_object_end_date, src_insured_object_current_status
FROM raw_prd2_insured_object_register;

INSERT INTO hub_override (override_hash_key, load_date, record_source, override_id)
SELECT DISTINCT MD5(src_override_ref), src_extract_ts, src_system, src_override_id
FROM raw_prd2_override_register
WHERE src_override_ref <> '';

INSERT INTO sat_override
SELECT MD5(src_override_ref) AS override_hash_key, src_extract_ts AS load_date,
       src_override_reason
FROM raw_prd2_override_register;

INSERT INTO hub_regulation (regulation_hash_key, load_date, record_source, regulation_id)
SELECT DISTINCT MD5(src_regulation_ref), src_extract_ts, src_system, src_regulation_id
FROM raw_prd2_regulation_register
WHERE src_regulation_ref <> '';

INSERT INTO sat_regulation
SELECT MD5(src_regulation_ref) AS regulation_hash_key, src_extract_ts AS load_date,
       src_regulation_number, src_regulation_name, src_regulation_department,
       src_regulation_region, src_regulation_risk_level, src_regulation_compliance_status,
       src_regulation_date_raised, src_regulation_date_closed, src_regulation_owner,
       src_regulation_deadline_date, src_is_regulation_on_time
FROM raw_prd2_regulation_register;

-- -------------------------
-- Added links
-- Hash order follows enhanced_synthetic_generator.py, not alphabetical order.
-- -------------------------

INSERT INTO link_broker_person (broker_person_hash_key, load_date, record_source, broker_hash_key, person_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(src_broker_ref), '|', MD5(src_person_ref))),
       src_extract_ts, src_system, MD5(src_broker_ref), MD5(src_person_ref)
FROM raw_prd2_broker_person_bridge
WHERE src_broker_ref <> '' AND src_person_ref <> '';

INSERT INTO link_policy_broker (policy_broker_hash_key, load_date, record_source, broker_hash_key, policy_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(src_broker_ref), '|', MD5(src_policy_ref))),
       src_extract_ts, src_system, MD5(src_broker_ref), MD5(src_policy_ref)
FROM raw_prd2_policy_broker_bridge
WHERE src_broker_ref <> '' AND src_policy_ref <> '';

INSERT INTO link_policy_channel (policy_channel_hash_key, load_date, record_source, channel_hash_key, policy_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(src_channel_ref), '|', MD5(src_policy_ref))),
       src_extract_ts, src_system, MD5(src_channel_ref), MD5(src_policy_ref)
FROM raw_prd2_policy_channel_bridge
WHERE src_channel_ref <> '' AND src_policy_ref <> '';

INSERT INTO link_policy_quote (policy_quote_hash_key, load_date, record_source, quote_hash_key, policy_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(src_quote_ref), '|', MD5(src_policy_ref))),
       src_extract_ts, src_system, MD5(src_quote_ref), MD5(src_policy_ref)
FROM raw_prd2_policy_quote_bridge
WHERE src_quote_ref <> '' AND src_policy_ref <> '';

INSERT INTO link_quote_broker (quote_broker_hash_key, load_date, record_source, quote_hash_key, broker_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(src_quote_ref), '|', MD5(src_broker_ref))),
       src_extract_ts, src_system, MD5(src_quote_ref), MD5(src_broker_ref)
FROM raw_prd2_quote_broker_bridge
WHERE src_quote_ref <> '' AND src_broker_ref <> '';

INSERT INTO link_quote_channel (quote_channel_hash_key, load_date, record_source, quote_hash_key, channel_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(src_quote_ref), '|', MD5(src_channel_ref))),
       src_extract_ts, src_system, MD5(src_quote_ref), MD5(src_channel_ref)
FROM raw_prd2_quote_channel_bridge
WHERE src_quote_ref <> '' AND src_channel_ref <> '';

INSERT INTO link_claim_policy (claim_policy_hash_key, load_date, record_source, policy_hash_key, claim_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(src_policy_ref), '|', MD5(src_claim_ref))),
       src_extract_ts, src_system, MD5(src_policy_ref), MD5(src_claim_ref)
FROM raw_prd2_claim_policy_bridge
WHERE src_policy_ref <> '' AND src_claim_ref <> '';

INSERT INTO link_complaint_policy (complaint_policy_hash_key, load_date, record_source, complaint_hash_key, policy_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(src_complaint_ref), '|', MD5(src_policy_ref))),
       src_extract_ts, src_system, MD5(src_complaint_ref), MD5(src_policy_ref)
FROM raw_prd2_complaint_policy_bridge
WHERE src_complaint_ref <> '' AND src_policy_ref <> '';

INSERT INTO link_complaint_regulation (complaint_regulation_hash_key, load_date, record_source, regulation_hash_key, complaint_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(src_regulation_ref), '|', MD5(src_complaint_ref))),
       src_extract_ts, src_system, MD5(src_regulation_ref), MD5(src_complaint_ref)
FROM raw_prd2_complaint_regulation_bridge
WHERE src_regulation_ref <> '' AND src_complaint_ref <> '';

INSERT INTO link_person_address (person_address_hash_key, load_date, record_source, person_hash_key, address_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(src_person_ref), '|', MD5(src_address_ref))),
       src_extract_ts, src_system, MD5(src_person_ref), MD5(src_address_ref)
FROM raw_prd2_person_address_bridge
WHERE src_person_ref <> '' AND src_address_ref <> '';

INSERT INTO link_person_campaign (person_campaign_hash_key, load_date, record_source, campaign_hash_key, person_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(src_campaign_ref), '|', MD5(src_person_ref))),
       src_extract_ts, src_system, MD5(src_campaign_ref), MD5(src_person_ref)
FROM raw_prd2_person_campaign_bridge
WHERE src_campaign_ref <> '' AND src_person_ref <> '';

INSERT INTO link_policy_insured_object (policy_insured_object_hash_key, load_date, record_source, policy_hash_key, insured_object_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(src_policy_ref), '|', MD5(src_insured_object_ref))),
       src_extract_ts, src_system, MD5(src_policy_ref), MD5(src_insured_object_ref)
FROM raw_prd2_policy_insured_object_bridge
WHERE src_policy_ref <> '' AND src_insured_object_ref <> '';

INSERT INTO link_policy_override (policy_override_hash_key, load_date, record_source, policy_hash_key, override_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(src_policy_ref), '|', MD5(src_override_ref))),
       src_extract_ts, src_system, MD5(src_policy_ref), MD5(src_override_ref)
FROM raw_prd2_policy_override_bridge
WHERE src_policy_ref <> '' AND src_override_ref <> '';

INSERT INTO link_insured_object_home (insured_object_home_hash_key, load_date, record_source, insured_object_hash_key, home_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(src_insured_object_ref), '|', MD5(src_home_ref))),
       src_extract_ts, src_system, MD5(src_insured_object_ref), MD5(src_home_ref)
FROM raw_prd2_insured_object_home_bridge
WHERE src_insured_object_ref <> '' AND src_home_ref <> '';

INSERT INTO link_insured_object_motor (insured_object_motor_hash_key, load_date, record_source, insured_object_hash_key, motor_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(src_insured_object_ref), '|', MD5(src_motor_ref))),
       src_extract_ts, src_system, MD5(src_insured_object_ref), MD5(src_motor_ref)
FROM raw_prd2_insured_object_motor_bridge
WHERE src_insured_object_ref <> '' AND src_motor_ref <> '';

-- -------------------------
-- Enrichment merges
-- Use UPDATE for an existing base satellite row. Use INSERT if your vault stores
-- enrichment as a new satellite version instead of widening current rows.
-- -------------------------

UPDATE sat_policy sp
JOIN raw_prd2_policy_enrichment r
  ON sp.policy_hash_key = MD5(r.src_policy_ref)
SET
  sp.quote_id = r.src_quote_id,
  sp.policy_type = r.src_policy_type,
  sp.policy_issue_date = r.src_policy_issue_date,
  sp.is_policy_renewal = r.src_is_policy_renewal,
  sp.policy_cancellation_reason = r.src_policy_cancellation_reason,
  sp.policy_sum_insured = r.src_policy_sum_insured,
  sp.policy_retention_limit = r.src_policy_retention_limit,
  sp.policy_risk_score = r.src_policy_risk_score,
  sp.policy_risk_band = r.src_policy_risk_band,
  sp.policy_base_premium = r.src_policy_base_premium,
  sp.gross_written_premium = r.src_gross_written_premium,
  sp.earned_premium = r.src_earned_premium,
  sp.incurred_but_not_reported = r.src_incurred_but_not_reported,
  sp.operating_expenses = r.src_operating_expenses,
  sp.administrative_expenses = r.src_administrative_expenses,
  sp.profit_margin = r.src_profit_margin,
  sp.taxes_and_levies = r.src_taxes_and_levies,
  sp.amount_approved = r.src_amount_approved,
  sp.ceded_premium = r.src_ceded_premium,
  sp.commission_paid = r.src_commission_paid,
  sp.ceded_commission = r.src_ceded_commission,
  sp.exposure_amount = r.src_exposure_amount,
  sp.investment_income = r.src_investment_income,
  sp.underwriting_cycle_time_in_days = r.src_underwriting_cycle_time_in_days,
  sp.underwriting_expenses = r.src_underwriting_expenses,
  sp.transaction_date = r.src_transaction_date,
  sp.record_type = r.src_record_type,
  sp.discount = r.src_discount,
  sp.override_commission = r.src_override_commission,
  sp.partial_recovery_percentage = r.src_partial_recovery_percentage,
  sp.is_auto_renew_enabled = r.src_is_auto_renew_enabled,
  sp.no_claims_discount_years = r.src_no_claims_discount_years,
  sp.payment_method = r.src_payment_method,
  sp.is_direct_debit_cancellation = r.src_is_direct_debit_cancellation,
  sp.missed_payment_count = r.src_missed_payment_count,
  sp.loyalty_discount_usage = r.src_loyalty_discount_usage,
  sp.is_installment_default = r.src_is_installment_default;

UPDATE sat_customer sc
JOIN raw_prd2_customer_enrichment r
  ON sc.customer_hash_key = MD5(r.src_customer_ref)
SET
  sc.customer_number = r.src_customer_number,
  sc.customer_status = r.src_customer_status,
  sc.customer_status_reason = r.src_customer_status_reason,
  sc.customer_since = r.src_customer_since,
  sc.customer_rating = r.src_customer_rating,
  sc.customer_segment = r.src_customer_segment,
  sc.line_of_business = r.src_line_of_business,
  sc.nps_score = r.src_nps_score,
  sc.income_band = r.src_income_band,
  sc.customer_satisfaction = r.src_customer_satisfaction,
  sc.customer_age_band = r.src_customer_age_band,
  sc.net_promotor_code_segment = r.src_net_promotor_code_segment;

UPDATE sat_marketing_engagement sme
JOIN raw_prd2_marketing_engagement_enrichment r
  ON sme.marketing_engagement_hash_key = MD5(r.src_marketing_engagement_ref)
SET
  sme.promotion_code = r.src_promotion_code,
  sme.opened_email = r.src_opened_email,
  sme.marketing_status = r.src_marketing_status,
  sme.has_retention_team_interaction = r.src_has_retention_team_interaction,
  sme.customer_service_call_frequency = r.src_customer_service_call_frequency,
  sme.average_call_sentiment = r.src_average_call_sentiment,
  sme.engagement_score = r.src_engagement_score;

UPDATE sat_motor sm
JOIN raw_prd2_motor_enrichment r
  ON sm.motor_hash_key = MD5(r.src_motor_ref)
SET
  sm.auto_decline_vehicle = r.src_auto_decline_vehicle,
  sm.body_type = r.src_body_type,
  sm.fuel_type = r.src_fuel_type,
  sm.license_status = r.src_license_status,
  sm.is_existing_motor_customer = r.src_is_existing_motor_customer,
  sm.motor_lapsed_policies = r.src_motor_lapsed_policies,
  sm.motor_risk_address = r.src_motor_risk_address,
  sm.risk_class_code = r.src_risk_class_code,
  sm.vehicle_owner_type = r.src_vehicle_owner_type,
  sm.vehicle_regstate = r.src_vehicle_regstate,
  sm.vehicle_class = r.src_vehicle_class,
  sm.vehicle_model = r.src_vehicle_model,
  sm.vehicle_type = r.src_vehicle_type,
  sm.motor_sum_insrd = r.src_motor_sum_insrd,
  sm.vehicle_year = r.src_vehicle_year,
  sm.vehicle_age = r.src_vehicle_age,
  sm.driver_experience_years = r.src_driver_experience_years;
