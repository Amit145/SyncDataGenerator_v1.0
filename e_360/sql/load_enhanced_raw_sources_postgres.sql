-- PostgreSQL source table loader for enhanced PRD1/PRD2 raw.

-- run_id: 20260715193343

BEGIN;

DROP TABLE IF EXISTS "crm_account_book_csv";
CREATE TABLE "crm_account_book_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "party_ref" TEXT,
    "account_ref" TEXT,
    "account_no" TEXT,
    "account_type_txt" TEXT,
    "last_access_dt" TEXT,
    "last_change_dt" TEXT,
    "account_create_type_txt" TEXT,
    "account_status_txt" TEXT
);

\copy "crm_account_book_csv" ("batch_ref", "pull_ts", "origin_sys", "party_ref", "account_ref", "account_no", "account_type_txt", "last_access_dt", "last_change_dt", "account_create_type_txt", "account_status_txt") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/account_book.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_address_book_csv";
CREATE TABLE "crm_address_book_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "party_ref" TEXT,
    "address_ref" TEXT,
    "street_txt" TEXT,
    "postal_cd" TEXT,
    "city_nm" TEXT,
    "state_cd" TEXT,
    "country_cd" TEXT,
    "address_type_txt" TEXT,
    "region_txt" TEXT
);

\copy "crm_address_book_csv" ("batch_ref", "pull_ts", "origin_sys", "party_ref", "address_ref", "street_txt", "postal_cd", "city_nm", "state_cd", "country_cd", "address_type_txt", "region_txt") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/address_book.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_broker_book_csv";
CREATE TABLE "crm_broker_book_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "src_broker_ref" TEXT,
    "src_extract_ts" TEXT,
    "src_system" TEXT,
    "src_agent_id" TEXT,
    "src_agent_name" TEXT,
    "src_agent_type" TEXT,
    "src_agent_status" TEXT,
    "src_agent_license_number" TEXT,
    "src_agent_net_promoter_score" TEXT,
    "src_agent_commission_percentage" TEXT,
    "src_person_id" TEXT
);

\copy "crm_broker_book_csv" ("batch_ref", "pull_ts", "origin_sys", "src_broker_ref", "src_extract_ts", "src_system", "src_agent_id", "src_agent_name", "src_agent_type", "src_agent_status", "src_agent_license_number", "src_agent_net_promoter_score", "src_agent_commission_percentage", "src_person_id") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/broker_book.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_campaign_register_csv";
CREATE TABLE "crm_campaign_register_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "src_campaign_ref" TEXT,
    "src_extract_ts" TEXT,
    "src_system" TEXT,
    "src_campaign_id" TEXT,
    "src_campaign_name" TEXT,
    "src_campaign_type" TEXT,
    "src_campaign_start_date" TEXT,
    "src_campaign_end_date" TEXT,
    "src_campaign_status" TEXT,
    "src_campaign_budget" TEXT,
    "src_campaign_target_audience" TEXT,
    "src_campaign_marketing_source" TEXT,
    "src_campaign_owner_department" TEXT,
    "src_campaign_country" TEXT,
    "src_campaign_conversion_goal" TEXT,
    "src_number_of_impressions" TEXT,
    "src_number_of_clicks" TEXT,
    "src_is_active" TEXT,
    "src_number_of_visits" TEXT,
    "src_number_of_policy_purchases" TEXT,
    "src_number_of_emails_sent" TEXT,
    "src_number_of_email_bounced" TEXT,
    "src_number_of_emails_delivered" TEXT,
    "src_number_of_emails_opened" TEXT,
    "src_click_through_rate" TEXT,
    "src_spend_amount" TEXT,
    "src_incremental_revenue" TEXT,
    "src_survey_wave" TEXT,
    "src_total_number_of_respondents" TEXT,
    "src_number_of_respondents_aware" TEXT,
    "src_number_of_promoters" TEXT,
    "src_number_of_passives" TEXT,
    "src_number_of_detractors" TEXT,
    "src_number_of_followers" TEXT,
    "src_number_of_likes" TEXT,
    "src_number_of_comments" TEXT,
    "src_number_of_shares" TEXT,
    "src_number_of_brand_mentions" TEXT,
    "src_number_of_category_mentions" TEXT,
    "src_person_id" TEXT
);

\copy "crm_campaign_register_csv" ("batch_ref", "pull_ts", "origin_sys", "src_campaign_ref", "src_extract_ts", "src_system", "src_campaign_id", "src_campaign_name", "src_campaign_type", "src_campaign_start_date", "src_campaign_end_date", "src_campaign_status", "src_campaign_budget", "src_campaign_target_audience", "src_campaign_marketing_source", "src_campaign_owner_department", "src_campaign_country", "src_campaign_conversion_goal", "src_number_of_impressions", "src_number_of_clicks", "src_is_active", "src_number_of_visits", "src_number_of_policy_purchases", "src_number_of_emails_sent", "src_number_of_email_bounced", "src_number_of_emails_delivered", "src_number_of_emails_opened", "src_click_through_rate", "src_spend_amount", "src_incremental_revenue", "src_survey_wave", "src_total_number_of_respondents", "src_number_of_respondents_aware", "src_number_of_promoters", "src_number_of_passives", "src_number_of_detractors", "src_number_of_followers", "src_number_of_likes", "src_number_of_comments", "src_number_of_shares", "src_number_of_brand_mentions", "src_number_of_category_mentions", "src_person_id") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/campaign_register.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_campaign_touch_csv";
CREATE TABLE "crm_campaign_touch_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "party_ref" TEXT,
    "engagement_ref" TEXT,
    "promo_cd" TEXT,
    "email_opened_ind" TEXT,
    "campaign_status_txt" TEXT,
    "has_retention_team_interaction" TEXT,
    "customer_service_call_frequency" TEXT,
    "average_call_sentiment" TEXT,
    "engagement_score" TEXT,
    "first_contact_resolution" TEXT
);

\copy "crm_campaign_touch_csv" ("batch_ref", "pull_ts", "origin_sys", "party_ref", "engagement_ref", "promo_cd", "email_opened_ind", "campaign_status_txt", "has_retention_team_interaction", "customer_service_call_frequency", "average_call_sentiment", "engagement_score", "first_contact_resolution") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/campaign_touch.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_channel_catalog_csv";
CREATE TABLE "crm_channel_catalog_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "src_channel_ref" TEXT,
    "src_extract_ts" TEXT,
    "src_system" TEXT,
    "src_channel_id" TEXT,
    "src_channel_name" TEXT,
    "src_channel_type" TEXT
);

\copy "crm_channel_catalog_csv" ("batch_ref", "pull_ts", "origin_sys", "src_channel_ref", "src_extract_ts", "src_system", "src_channel_id", "src_channel_name", "src_channel_type") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/channel_catalog.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_claim_register_csv";
CREATE TABLE "crm_claim_register_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "src_claim_ref" TEXT,
    "src_extract_ts" TEXT,
    "src_system" TEXT,
    "src_claim_id" TEXT,
    "src_claim_number" TEXT,
    "src_claim_type" TEXT,
    "src_claim_status" TEXT,
    "src_claim_reason" TEXT,
    "src_claim_channel" TEXT,
    "src_claim_handler" TEXT,
    "src_claim_reported_date" TEXT,
    "src_claim_settlement_date" TEXT,
    "src_claim_product" TEXT,
    "src_is_claim_suspicious" TEXT,
    "src_is_claim_fraud" TEXT,
    "src_claim_fraud_status" TEXT,
    "src_claim_fraud_type" TEXT,
    "src_claim_fraud_detection_method" TEXT,
    "src_is_litigation" TEXT,
    "src_litigation_reason" TEXT,
    "src_litigation_start_date" TEXT,
    "src_litigation_end_date" TEXT,
    "src_litigation_outcome" TEXT,
    "src_litigation_duration_days" TEXT,
    "src_claim_fraud_detection_time_in_days" TEXT,
    "src_is_recovery_opportunity" TEXT,
    "src_recovery_priority_score" TEXT,
    "src_recovery_category" TEXT,
    "src_recovery_source" TEXT,
    "src_first_recovery_date" TEXT,
    "src_last_recovery_date" TEXT,
    "src_is_recovery_happened" TEXT,
    "src_days_to_first_recovery" TEXT,
    "src_days_to_last_recovery" TEXT,
    "src_avg_days_to_close_claim" TEXT,
    "src_claim_fraud_outcome" TEXT,
    "src_recovery_type" TEXT,
    "src_recovery_band" TEXT,
    "src_third_party_involved" TEXT,
    "src_third_party_involved_overall_score" TEXT,
    "src_solicitor" TEXT,
    "src_claim_amount" TEXT,
    "src_claims_paid" TEXT,
    "src_outstanding_reserve" TEXT,
    "src_claims_expenses" TEXT,
    "src_recovery_received" TEXT,
    "src_compensation_offered" TEXT,
    "src_remediation_amount" TEXT,
    "src_suspectd_amount" TEXT,
    "src_fraud_amount" TEXT,
    "src_legal_expenses" TEXT,
    "src_claim_band" TEXT,
    "src_claim_band_sort" TEXT,
    "src_policy_id" TEXT,
    "src_is_fault_claim" TEXT,
    "src_claim_satisfaction_score" TEXT,
    "src_claims_feedback" TEXT,
    "src_is_claim_complaint_raised" TEXT
);

\copy "crm_claim_register_csv" ("batch_ref", "pull_ts", "origin_sys", "src_claim_ref", "src_extract_ts", "src_system", "src_claim_id", "src_claim_number", "src_claim_type", "src_claim_status", "src_claim_reason", "src_claim_channel", "src_claim_handler", "src_claim_reported_date", "src_claim_settlement_date", "src_claim_product", "src_is_claim_suspicious", "src_is_claim_fraud", "src_claim_fraud_status", "src_claim_fraud_type", "src_claim_fraud_detection_method", "src_is_litigation", "src_litigation_reason", "src_litigation_start_date", "src_litigation_end_date", "src_litigation_outcome", "src_litigation_duration_days", "src_claim_fraud_detection_time_in_days", "src_is_recovery_opportunity", "src_recovery_priority_score", "src_recovery_category", "src_recovery_source", "src_first_recovery_date", "src_last_recovery_date", "src_is_recovery_happened", "src_days_to_first_recovery", "src_days_to_last_recovery", "src_avg_days_to_close_claim", "src_claim_fraud_outcome", "src_recovery_type", "src_recovery_band", "src_third_party_involved", "src_third_party_involved_overall_score", "src_solicitor", "src_claim_amount", "src_claims_paid", "src_outstanding_reserve", "src_claims_expenses", "src_recovery_received", "src_compensation_offered", "src_remediation_amount", "src_suspectd_amount", "src_fraud_amount", "src_legal_expenses", "src_claim_band", "src_claim_band_sort", "src_policy_id", "src_is_fault_claim", "src_claim_satisfaction_score", "src_claims_feedback", "src_is_claim_complaint_raised") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/claim_register.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_comm_preference_csv";
CREATE TABLE "crm_comm_preference_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "party_ref" TEXT,
    "preference_ref" TEXT,
    "sms_ind" TEXT,
    "email_ind" TEXT,
    "email_sub_ind" TEXT,
    "call_ind" TEXT,
    "any_ind" TEXT,
    "commercial_email_ind" TEXT,
    "postal_mail_ind" TEXT
);

\copy "crm_comm_preference_csv" ("batch_ref", "pull_ts", "origin_sys", "party_ref", "preference_ref", "sms_ind", "email_ind", "email_sub_ind", "call_ind", "any_ind", "commercial_email_ind", "postal_mail_ind") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/comm_preference.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_complaint_register_csv";
CREATE TABLE "crm_complaint_register_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "src_complaint_ref" TEXT,
    "src_extract_ts" TEXT,
    "src_system" TEXT,
    "src_complaint_id" TEXT,
    "src_complaint_date" TEXT,
    "src_complaint_acknowledgement_date" TEXT,
    "src_complaint_resolved_date" TEXT,
    "src_complaint_upheld_status" TEXT,
    "src_is_financial_ombudsman_service_referral" TEXT,
    "src_complaint_driver" TEXT,
    "src_complaint_channel" TEXT,
    "src_compensation_amount" TEXT,
    "src_insurance_category" TEXT,
    "src_complaint_status" TEXT,
    "src_policy_id" TEXT,
    "src_regulation_id" TEXT,
    "src_customer_complaint_satisfaction_score" TEXT,
    "src_complaint_feedback" TEXT
);

\copy "crm_complaint_register_csv" ("batch_ref", "pull_ts", "origin_sys", "src_complaint_ref", "src_extract_ts", "src_system", "src_complaint_id", "src_complaint_date", "src_complaint_acknowledgement_date", "src_complaint_resolved_date", "src_complaint_upheld_status", "src_is_financial_ombudsman_service_referral", "src_complaint_driver", "src_complaint_channel", "src_compensation_amount", "src_insurance_category", "src_complaint_status", "src_policy_id", "src_regulation_id", "src_customer_complaint_satisfaction_score", "src_complaint_feedback") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/complaint_register.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_consent_snapshot_csv";
CREATE TABLE "crm_consent_snapshot_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "party_ref" TEXT,
    "consent_ref" TEXT,
    "opt_in_valid_ind" TEXT,
    "opt_in_legit_ind" TEXT
);

\copy "crm_consent_snapshot_csv" ("batch_ref", "pull_ts", "origin_sys", "party_ref", "consent_ref", "opt_in_valid_ind", "opt_in_legit_ind") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/consent_snapshot.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_contact_point_csv";
CREATE TABLE "crm_contact_point_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "party_ref" TEXT,
    "contact_ref" TEXT,
    "email_home_txt" TEXT,
    "email_work_txt" TEXT,
    "phone_work_txt" TEXT,
    "phone_home_txt" TEXT
);

\copy "crm_contact_point_csv" ("batch_ref", "pull_ts", "origin_sys", "party_ref", "contact_ref", "email_home_txt", "email_work_txt", "phone_work_txt", "phone_home_txt") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/contact_point.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_customer_lead_bridge_csv";
CREATE TABLE "crm_customer_lead_bridge_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "customer_ref" TEXT,
    "lead_ref" TEXT
);

\copy "crm_customer_lead_bridge_csv" ("batch_ref", "pull_ts", "origin_sys", "customer_ref", "lead_ref") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/customer_lead_bridge.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_customer_portfolio_csv";
CREATE TABLE "crm_customer_portfolio_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "party_ref" TEXT,
    "customer_ref" TEXT,
    "customer_no" TEXT,
    "customer_status_txt" TEXT,
    "customer_status_reason_txt" TEXT,
    "customer_since_dt" TEXT,
    "customer_rating_no" TEXT,
    "customer_segment_txt" TEXT,
    "lob_txt" TEXT,
    "nps_score_no" TEXT,
    "customer_onboarding_satisfaction_score" TEXT,
    "customer_onboarding_feedback" TEXT
);

\copy "crm_customer_portfolio_csv" ("batch_ref", "pull_ts", "origin_sys", "party_ref", "customer_ref", "customer_no", "customer_status_txt", "customer_status_reason_txt", "customer_since_dt", "customer_rating_no", "customer_segment_txt", "lob_txt", "nps_score_no", "customer_onboarding_satisfaction_score", "customer_onboarding_feedback") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/customer_portfolio.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_enhanced_address_book_csv";
CREATE TABLE "crm_enhanced_address_book_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "src_address_ref" TEXT,
    "src_extract_ts" TEXT,
    "src_system" TEXT,
    "src_address_id" TEXT,
    "src_street" TEXT,
    "src_postcode" TEXT,
    "src_city" TEXT,
    "src_state" TEXT,
    "src_country" TEXT,
    "src_type" TEXT,
    "src_person_id" TEXT
);

\copy "crm_enhanced_address_book_csv" ("batch_ref", "pull_ts", "origin_sys", "src_address_ref", "src_extract_ts", "src_system", "src_address_id", "src_street", "src_postcode", "src_city", "src_state", "src_country", "src_type", "src_person_id") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/enhanced_address_book.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_enhanced_enrichments_csv";
CREATE TABLE "crm_enhanced_enrichments_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "source_extract" TEXT,
    "src_customer_ref" TEXT,
    "src_extract_ts" TEXT,
    "src_customer_number" TEXT,
    "src_customer_status" TEXT,
    "src_customer_status_reason" TEXT,
    "src_customer_since" TEXT,
    "src_customer_rating" TEXT,
    "src_customer_segment" TEXT,
    "src_line_of_business" TEXT,
    "src_nps_score" TEXT,
    "src_income_band" TEXT,
    "src_customer_satisfaction" TEXT,
    "src_customer_age_band" TEXT,
    "src_net_promotor_code_segment" TEXT,
    "src_marketing_engagement_ref" TEXT,
    "src_promotion_code" TEXT,
    "src_opened_email" TEXT,
    "src_marketing_status" TEXT,
    "src_motor_ref" TEXT,
    "src_auto_decline_vehicle" TEXT,
    "src_body_type" TEXT,
    "src_fuel_type" TEXT,
    "src_license_status" TEXT,
    "src_is_existing_motor_customer" TEXT,
    "src_motor_lapsed_policies" TEXT,
    "src_motor_risk_address" TEXT,
    "src_risk_class_code" TEXT,
    "src_vehicle_owner_type" TEXT,
    "src_vehicle_regstate" TEXT,
    "src_vehicle_class" TEXT,
    "src_vehicle_model" TEXT,
    "src_vehicle_type" TEXT,
    "src_motor_sum_insrd" TEXT,
    "src_vehicle_year" TEXT,
    "src_vehicle_age" TEXT,
    "src_policy_ref" TEXT,
    "src_cover_option" TEXT,
    "src_declined_claims" TEXT,
    "src_fraud_flag" TEXT,
    "src_gross_revenue" TEXT,
    "src_net_revenue" TEXT,
    "src_number_of_active_claim" TEXT,
    "src_number_of_previous_claim" TEXT,
    "src_policy_cycle" TEXT,
    "src_policy_end_date" TEXT,
    "src_policy_length" TEXT,
    "src_policy_number" TEXT,
    "src_policy_start_date" TEXT,
    "src_policy_status" TEXT,
    "src_renewal_amount_current_period" TEXT,
    "src_renewal_amount_next_period" TEXT,
    "src_renewal_date" TEXT,
    "src_sales_channel" TEXT,
    "src_quote_id" TEXT,
    "src_policy_type" TEXT,
    "src_policy_issue_date" TEXT,
    "src_is_policy_renewal" TEXT,
    "src_policy_cancellation_reason" TEXT,
    "src_policy_sum_insured" TEXT,
    "src_policy_retention_limit" TEXT,
    "src_policy_risk_score" TEXT,
    "src_policy_risk_band" TEXT,
    "src_policy_base_premium" TEXT,
    "src_gross_written_premium" TEXT,
    "src_earned_premium" TEXT,
    "src_incurred_but_not_reported" TEXT,
    "src_operating_expenses" TEXT,
    "src_administrative_expenses" TEXT,
    "src_profit_margin" TEXT,
    "src_taxes_and_levies" TEXT,
    "src_amount_approved" TEXT,
    "src_ceded_premium" TEXT,
    "src_commission_paid" TEXT,
    "src_ceded_commission" TEXT,
    "src_exposure_amount" TEXT,
    "src_investment_income" TEXT,
    "src_underwriting_cycle_time_in_days" TEXT,
    "src_underwriting_expenses" TEXT,
    "src_transaction_date" TEXT,
    "src_record_type" TEXT,
    "src_discount" TEXT,
    "src_override_commission" TEXT,
    "src_partial_recovery_percentage" TEXT
);

\copy "crm_enhanced_enrichments_csv" ("batch_ref", "pull_ts", "origin_sys", "source_extract", "src_customer_ref", "src_extract_ts", "src_customer_number", "src_customer_status", "src_customer_status_reason", "src_customer_since", "src_customer_rating", "src_customer_segment", "src_line_of_business", "src_nps_score", "src_income_band", "src_customer_satisfaction", "src_customer_age_band", "src_net_promotor_code_segment", "src_marketing_engagement_ref", "src_promotion_code", "src_opened_email", "src_marketing_status", "src_motor_ref", "src_auto_decline_vehicle", "src_body_type", "src_fuel_type", "src_license_status", "src_is_existing_motor_customer", "src_motor_lapsed_policies", "src_motor_risk_address", "src_risk_class_code", "src_vehicle_owner_type", "src_vehicle_regstate", "src_vehicle_class", "src_vehicle_model", "src_vehicle_type", "src_motor_sum_insrd", "src_vehicle_year", "src_vehicle_age", "src_policy_ref", "src_cover_option", "src_declined_claims", "src_fraud_flag", "src_gross_revenue", "src_net_revenue", "src_number_of_active_claim", "src_number_of_previous_claim", "src_policy_cycle", "src_policy_end_date", "src_policy_length", "src_policy_number", "src_policy_start_date", "src_policy_status", "src_renewal_amount_current_period", "src_renewal_amount_next_period", "src_renewal_date", "src_sales_channel", "src_quote_id", "src_policy_type", "src_policy_issue_date", "src_is_policy_renewal", "src_policy_cancellation_reason", "src_policy_sum_insured", "src_policy_retention_limit", "src_policy_risk_score", "src_policy_risk_band", "src_policy_base_premium", "src_gross_written_premium", "src_earned_premium", "src_incurred_but_not_reported", "src_operating_expenses", "src_administrative_expenses", "src_profit_margin", "src_taxes_and_levies", "src_amount_approved", "src_ceded_premium", "src_commission_paid", "src_ceded_commission", "src_exposure_amount", "src_investment_income", "src_underwriting_cycle_time_in_days", "src_underwriting_expenses", "src_transaction_date", "src_record_type", "src_discount", "src_override_commission", "src_partial_recovery_percentage") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/enhanced_enrichments.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_enhanced_person_relationships_csv";
CREATE TABLE "crm_enhanced_person_relationships_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "source_extract" TEXT,
    "src_broker_person_ref" TEXT,
    "src_extract_ts" TEXT,
    "src_system" TEXT,
    "src_broker_ref" TEXT,
    "src_person_ref" TEXT,
    "src_agent_id" TEXT,
    "src_person_id" TEXT,
    "src_person_address_ref" TEXT,
    "src_address_ref" TEXT,
    "src_address_id" TEXT,
    "src_person_campaign_ref" TEXT,
    "src_campaign_ref" TEXT,
    "src_campaign_id" TEXT
);

\copy "crm_enhanced_person_relationships_csv" ("batch_ref", "pull_ts", "origin_sys", "source_extract", "src_broker_person_ref", "src_extract_ts", "src_system", "src_broker_ref", "src_person_ref", "src_agent_id", "src_person_id", "src_person_address_ref", "src_address_ref", "src_address_id", "src_person_campaign_ref", "src_campaign_ref", "src_campaign_id") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/enhanced_person_relationships.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_enhanced_policy_relationships_csv";
CREATE TABLE "crm_enhanced_policy_relationships_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "source_extract" TEXT,
    "src_claim_policy_ref" TEXT,
    "src_extract_ts" TEXT,
    "src_system" TEXT,
    "src_claim_ref" TEXT,
    "src_policy_ref" TEXT,
    "src_claim_id" TEXT,
    "src_policy_id" TEXT,
    "src_complaint_policy_ref" TEXT,
    "src_complaint_ref" TEXT,
    "src_complaint_id" TEXT,
    "src_complaint_regulation_ref" TEXT,
    "src_regulation_ref" TEXT,
    "src_regulation_id" TEXT,
    "src_insured_object_home_ref" TEXT,
    "src_insured_object_ref" TEXT,
    "src_home_ref" TEXT,
    "src_insured_object_id" TEXT,
    "src_home_id" TEXT,
    "src_insured_object_motor_ref" TEXT,
    "src_motor_ref" TEXT,
    "src_motor_id" TEXT,
    "src_policy_broker_ref" TEXT,
    "src_broker_ref" TEXT,
    "src_agent_id" TEXT,
    "src_policy_channel_ref" TEXT,
    "src_channel_ref" TEXT,
    "src_channel_id" TEXT,
    "src_policy_insured_object_ref" TEXT,
    "src_policy_override_ref" TEXT,
    "src_override_ref" TEXT,
    "src_override_id" TEXT,
    "src_policy_quote_ref" TEXT,
    "src_quote_ref" TEXT,
    "src_quote_id" TEXT,
    "src_quote_broker_ref" TEXT,
    "src_quote_channel_ref" TEXT
);

\copy "crm_enhanced_policy_relationships_csv" ("batch_ref", "pull_ts", "origin_sys", "source_extract", "src_claim_policy_ref", "src_extract_ts", "src_system", "src_claim_ref", "src_policy_ref", "src_claim_id", "src_policy_id", "src_complaint_policy_ref", "src_complaint_ref", "src_complaint_id", "src_complaint_regulation_ref", "src_regulation_ref", "src_regulation_id", "src_insured_object_home_ref", "src_insured_object_ref", "src_home_ref", "src_insured_object_id", "src_home_id", "src_insured_object_motor_ref", "src_motor_ref", "src_motor_id", "src_policy_broker_ref", "src_broker_ref", "src_agent_id", "src_policy_channel_ref", "src_channel_ref", "src_channel_id", "src_policy_insured_object_ref", "src_policy_override_ref", "src_override_ref", "src_override_id", "src_policy_quote_ref", "src_quote_ref", "src_quote_id", "src_quote_broker_ref", "src_quote_channel_ref") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/enhanced_policy_relationships.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_identity_registry_csv";
CREATE TABLE "crm_identity_registry_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "party_ref" TEXT,
    "identity_ref" TEXT,
    "ecid_txt" TEXT,
    "hashed_email_txt" TEXT
);

\copy "crm_identity_registry_csv" ("batch_ref", "pull_ts", "origin_sys", "party_ref", "identity_ref", "ecid_txt", "hashed_email_txt") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/identity_registry.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_lead_register_csv";
CREATE TABLE "crm_lead_register_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "party_ref" TEXT,
    "lead_ref" TEXT,
    "interest_bucket" TEXT,
    "contact_pref" TEXT,
    "person_score_no" TEXT,
    "person_status_txt" TEXT,
    "converted_dt" TEXT
);

\copy "crm_lead_register_csv" ("batch_ref", "pull_ts", "origin_sys", "party_ref", "lead_ref", "interest_bucket", "contact_pref", "person_score_no", "person_status_txt", "converted_dt") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/lead_register.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_override_register_csv";
CREATE TABLE "crm_override_register_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "src_override_ref" TEXT,
    "src_extract_ts" TEXT,
    "src_system" TEXT,
    "src_override_id" TEXT,
    "src_override_reason" TEXT,
    "src_policy_id" TEXT
);

\copy "crm_override_register_csv" ("batch_ref", "pull_ts", "origin_sys", "src_override_ref", "src_extract_ts", "src_system", "src_override_id", "src_override_reason", "src_policy_id") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/override_register.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_party_master_csv";
CREATE TABLE "crm_party_master_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "party_ref" TEXT,
    "party_kind" TEXT,
    "tenant_cd" TEXT,
    "lead_ind" TEXT,
    "paperless_ind" TEXT,
    "src_party_ref" TEXT,
    "src_party_type" TEXT,
    "natural_ref" TEXT,
    "given_nm" TEXT,
    "family_nm" TEXT,
    "display_nm" TEXT,
    "title_txt" TEXT,
    "occupation_txt" TEXT,
    "dob" TEXT,
    "birth_yr" TEXT,
    "nationality_txt" TEXT,
    "gender_txt" TEXT,
    "marital_txt" TEXT,
    "disability_degree" TEXT,
    "language_pref" TEXT,
    "role_txt" TEXT,
    "job_title_txt" TEXT,
    "legal_ref" TEXT,
    "legal_name" TEXT,
    "legal_score_no" TEXT,
    "legal_status_txt" TEXT,
    "legal_job_title_txt" TEXT,
    "legal_src_ref" TEXT,
    "legal_src_type" TEXT,
    "constitution_dt" TEXT,
    "lead_conv_dt" TEXT
);

\copy "crm_party_master_csv" ("batch_ref", "pull_ts", "origin_sys", "party_ref", "party_kind", "tenant_cd", "lead_ind", "paperless_ind", "src_party_ref", "src_party_type", "natural_ref", "given_nm", "family_nm", "display_nm", "title_txt", "occupation_txt", "dob", "birth_yr", "nationality_txt", "gender_txt", "marital_txt", "disability_degree", "language_pref", "role_txt", "job_title_txt", "legal_ref", "legal_name", "legal_score_no", "legal_status_txt", "legal_job_title_txt", "legal_src_ref", "legal_src_type", "constitution_dt", "lead_conv_dt") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/party_master.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_policy_register_csv";
CREATE TABLE "crm_policy_register_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "party_ref" TEXT,
    "customer_ref" TEXT,
    "policy_ref" TEXT,
    "quote_ref" TEXT,
    "product_ref" TEXT,
    "product_cd" TEXT,
    "cover_option_txt" TEXT,
    "declined_claim_cnt" TEXT,
    "fraud_ind" TEXT,
    "gross_amt" TEXT,
    "net_amt" TEXT,
    "active_claim_cnt" TEXT,
    "previous_claim_cnt" TEXT,
    "policy_cycle_no" TEXT,
    "policy_end_dt" TEXT,
    "policy_term_months" TEXT,
    "policy_no" TEXT,
    "policy_start_dt" TEXT,
    "policy_status_txt" TEXT,
    "renewal_premium_curr" TEXT,
    "renewal_premium_next" TEXT,
    "renewal_dt" TEXT,
    "sales_channel_txt" TEXT,
    "is_auto_renew_enabled" TEXT,
    "no_claims_discount_years" TEXT,
    "payment_method" TEXT,
    "is_direct_debit_cancellation" TEXT,
    "missed_payment_count" TEXT,
    "loyalty_discount_usage" TEXT,
    "is_installment_default" TEXT,
    "policy_renewal_satisfaction_score" TEXT,
    "policy_renewal_feedback" TEXT,
    "is_renewal_escalation" TEXT
);

\copy "crm_policy_register_csv" ("batch_ref", "pull_ts", "origin_sys", "party_ref", "customer_ref", "policy_ref", "quote_ref", "product_ref", "product_cd", "cover_option_txt", "declined_claim_cnt", "fraud_ind", "gross_amt", "net_amt", "active_claim_cnt", "previous_claim_cnt", "policy_cycle_no", "policy_end_dt", "policy_term_months", "policy_no", "policy_start_dt", "policy_status_txt", "renewal_premium_curr", "renewal_premium_next", "renewal_dt", "sales_channel_txt", "is_auto_renew_enabled", "no_claims_discount_years", "payment_method", "is_direct_debit_cancellation", "missed_payment_count", "loyalty_discount_usage", "is_installment_default", "policy_renewal_satisfaction_score", "policy_renewal_feedback", "is_renewal_escalation") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/policy_register.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_product_catalog_csv";
CREATE TABLE "crm_product_catalog_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "product_ref" TEXT,
    "product_cd" TEXT,
    "product_line" TEXT,
    "product_type_txt" TEXT,
    "underwriting_group_txt" TEXT,
    "regulatory_approval_cd" TEXT,
    "product_status_txt" TEXT,
    "product_lob_cd" TEXT,
    "product_launch_dt" TEXT,
    "product_variant" TEXT
);

\copy "crm_product_catalog_csv" ("batch_ref", "pull_ts", "origin_sys", "product_ref", "product_cd", "product_line", "product_type_txt", "underwriting_group_txt", "regulatory_approval_cd", "product_status_txt", "product_lob_cd", "product_launch_dt", "product_variant") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/product_catalog.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_property_asset_csv";
CREATE TABLE "crm_property_asset_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "policy_ref" TEXT,
    "product_ref" TEXT,
    "product_cd" TEXT,
    "property_ref" TEXT,
    "wall_material_txt" TEXT,
    "risk_address_txt" TEXT,
    "roof_material_txt" TEXT,
    "property_type_txt" TEXT,
    "property_state_cd" TEXT,
    "existing_home_ind" TEXT,
    "street_txt" TEXT,
    "postal_cd" TEXT,
    "city_nm" TEXT,
    "state_cd" TEXT,
    "country_cd" TEXT,
    "insured_object_id" TEXT,
    "insured_object_type" TEXT,
    "insured_object_sub_type" TEXT,
    "insured_object_description" TEXT,
    "insured_value" TEXT,
    "currency_code" TEXT,
    "insured_object_start_date" TEXT,
    "insured_object_end_date" TEXT,
    "insured_object_current_status" TEXT
);

\copy "crm_property_asset_csv" ("batch_ref", "pull_ts", "origin_sys", "policy_ref", "product_ref", "product_cd", "property_ref", "wall_material_txt", "risk_address_txt", "roof_material_txt", "property_type_txt", "property_state_cd", "existing_home_ind", "street_txt", "postal_cd", "city_nm", "state_cd", "country_cd", "insured_object_id", "insured_object_type", "insured_object_sub_type", "insured_object_description", "insured_value", "currency_code", "insured_object_start_date", "insured_object_end_date", "insured_object_current_status") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/property_asset.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_quote_register_csv";
CREATE TABLE "crm_quote_register_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "party_ref" TEXT,
    "quote_ref" TEXT,
    "product_ref" TEXT,
    "product_cd" TEXT,
    "gross_amt" TEXT,
    "net_amt" TEXT,
    "quote_no" TEXT,
    "quote_status_txt" TEXT,
    "renewal_amt_curr" TEXT,
    "renewal_amt_next" TEXT,
    "quoted_premium" TEXT,
    "quoted_date" TEXT,
    "quote_month_name" TEXT,
    "risk_score" TEXT,
    "policy_complexity" TEXT,
    "uw_approval_type" TEXT,
    "rejection_reason" TEXT
);

\copy "crm_quote_register_csv" ("batch_ref", "pull_ts", "origin_sys", "party_ref", "quote_ref", "product_ref", "product_cd", "gross_amt", "net_amt", "quote_no", "quote_status_txt", "renewal_amt_curr", "renewal_amt_next", "quoted_premium", "quoted_date", "quote_month_name", "risk_score", "policy_complexity", "uw_approval_type", "rejection_reason") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/quote_register.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_regulation_register_csv";
CREATE TABLE "crm_regulation_register_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "src_regulation_ref" TEXT,
    "src_extract_ts" TEXT,
    "src_system" TEXT,
    "src_regulation_id" TEXT,
    "src_regulation_number" TEXT,
    "src_regulation_name" TEXT,
    "src_regulation_department" TEXT,
    "src_regulation_region" TEXT,
    "src_regulation_risk_level" TEXT,
    "src_regulation_compliance_status" TEXT,
    "src_regulation_date_raised" TEXT,
    "src_regulation_date_closed" TEXT,
    "src_regulation_owner" TEXT,
    "src_regulation_deadline_date" TEXT,
    "src_is_regulation_on_time" TEXT
);

\copy "crm_regulation_register_csv" ("batch_ref", "pull_ts", "origin_sys", "src_regulation_ref", "src_extract_ts", "src_system", "src_regulation_id", "src_regulation_number", "src_regulation_name", "src_regulation_department", "src_regulation_region", "src_regulation_risk_level", "src_regulation_compliance_status", "src_regulation_date_raised", "src_regulation_date_closed", "src_regulation_owner", "src_regulation_deadline_date", "src_is_regulation_on_time") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/regulation_register.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "crm_vehicle_asset_csv";
CREATE TABLE "crm_vehicle_asset_csv" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "policy_ref" TEXT,
    "product_ref" TEXT,
    "product_cd" TEXT,
    "vehicle_ref" TEXT,
    "auto_decline_ind" TEXT,
    "body_style_txt" TEXT,
    "fuel_type_txt" TEXT,
    "license_status_txt" TEXT,
    "existing_motor_ind" TEXT,
    "motor_lapse_cnt" TEXT,
    "garage_address_txt" TEXT,
    "risk_class_cd" TEXT,
    "variant_nm" TEXT,
    "owner_type_txt" TEXT,
    "registration_state_cd" TEXT,
    "vehicle_class_txt" TEXT,
    "model_nm" TEXT,
    "vehicle_type_txt" TEXT,
    "insured_value_amt" TEXT,
    "manufacture_yr" TEXT,
    "vehicle_age_yrs" TEXT,
    "driver_experience_years" TEXT,
    "insured_object_id" TEXT,
    "insured_object_type" TEXT,
    "insured_object_sub_type" TEXT,
    "insured_object_description" TEXT,
    "insured_value" TEXT,
    "currency_code" TEXT,
    "insured_object_start_date" TEXT,
    "insured_object_end_date" TEXT,
    "insured_object_current_status" TEXT
);

\copy "crm_vehicle_asset_csv" ("batch_ref", "pull_ts", "origin_sys", "policy_ref", "product_ref", "product_cd", "vehicle_ref", "auto_decline_ind", "body_style_txt", "fuel_type_txt", "license_status_txt", "existing_motor_ind", "motor_lapse_cnt", "garage_address_txt", "risk_class_cd", "variant_nm", "owner_type_txt", "registration_state_cd", "vehicle_class_txt", "model_nm", "vehicle_type_txt", "insured_value_amt", "manufacture_yr", "vehicle_age_yrs", "driver_experience_years", "insured_object_id", "insured_object_type", "insured_object_sub_type", "insured_object_description", "insured_value", "currency_code", "insured_object_start_date", "insured_object_end_date", "insured_object_current_status") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260715193343/vault_ready_28/vehicle_asset.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "sap_address_db";
CREATE TABLE "sap_address_db" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "address_id" TEXT,
    "person_id" TEXT,
    "address_line_1" TEXT,
    "address_line_2" TEXT,
    "city" TEXT,
    "state" TEXT,
    "country" TEXT,
    "zipcode" TEXT
);

\copy "sap_address_db" ("batch_ref", "pull_ts", "origin_sys", "address_id", "person_id", "address_line_1", "address_line_2", "city", "state", "country", "zipcode") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_02/20260715193343/address.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "sap_home_db";
CREATE TABLE "sap_home_db" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "home_id" TEXT,
    "policy_id" TEXT,
    "product_id" TEXT,
    "home_type" TEXT,
    "home_location" TEXT,
    "wall_type" TEXT,
    "roof_material" TEXT
);

\copy "sap_home_db" ("batch_ref", "pull_ts", "origin_sys", "home_id", "policy_id", "product_id", "home_type", "home_location", "wall_type", "roof_material") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_02/20260715193343/home.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "sap_motor_db";
CREATE TABLE "sap_motor_db" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "motor_id" TEXT,
    "policy_id" TEXT,
    "product_id" TEXT,
    "motor_class" TEXT,
    "motor_model" TEXT,
    "motor_type" TEXT,
    "manufacturing_date" TEXT,
    "body_colour" TEXT,
    "fuel_type" TEXT,
    "gear_type" TEXT,
    "motor_parked_location" TEXT
);

\copy "sap_motor_db" ("batch_ref", "pull_ts", "origin_sys", "motor_id", "policy_id", "product_id", "motor_class", "motor_model", "motor_type", "manufacturing_date", "body_colour", "fuel_type", "gear_type", "motor_parked_location") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_02/20260715193343/motor.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "sap_person_db";
CREATE TABLE "sap_person_db" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "person_id" TEXT,
    "person_type" TEXT,
    "organization" TEXT,
    "org_establishment_date" TEXT,
    "first_name" TEXT,
    "middle_name" TEXT,
    "last_name" TEXT,
    "date_of_birth" TEXT,
    "gender" TEXT,
    "occupation" TEXT,
    "email_address" TEXT,
    "phone_number" TEXT
);

\copy "sap_person_db" ("batch_ref", "pull_ts", "origin_sys", "person_id", "person_type", "organization", "org_establishment_date", "first_name", "middle_name", "last_name", "date_of_birth", "gender", "occupation", "email_address", "phone_number") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_02/20260715193343/person.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DROP TABLE IF EXISTS "sap_product_db";
CREATE TABLE "sap_product_db" (
    "batch_ref" TEXT,
    "pull_ts" TEXT,
    "origin_sys" TEXT,
    "product_id" TEXT,
    "product_type" TEXT,
    "product_sub_type" TEXT,
    "product_name" TEXT,
    "product_start_date" TEXT,
    "line_of_business" TEXT
);

\copy "sap_product_db" ("batch_ref", "pull_ts", "origin_sys", "product_id", "product_type", "product_sub_type", "product_name", "product_start_date", "line_of_business") FROM 'F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_02/20260715193343/product.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

COMMIT;
