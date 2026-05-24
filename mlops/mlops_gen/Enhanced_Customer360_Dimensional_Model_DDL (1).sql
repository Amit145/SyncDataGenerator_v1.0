
CREATE OR REPLACE TABLE dim_account
(
	account_sk BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY ,
	account_id STRING,
	account_number STRING,
	account_type STRING,
	account_status STRING,
	account_creation_type STRING,
	account_last_access_ts TIMESTAMP,
	account_last_change_ts TIMESTAMP,
	effective_from_ts TIMESTAMP,
	effective_to_ts TIMESTAMP,
	record_version INTEGER,
	created_by STRING,
	created_ts TIMESTAMP,
	last_updated_by STRING,
	last_updated_ts TIMESTAMP,
	attr_hash STRING
)
PARTITIONED BY (account_number, effective_from_ts);

ALTER TABLE dim_account
	ADD CONSTRAINT XPKDIM_ACCOUNT PRIMARY KEY (account_sk);

CREATE OR REPLACE TABLE dim_broker
(
	broker_sk BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY ,
	agent_id STRING,
	agent_name STRING,
	agent_type STRING,
	agent_status STRING,
	agent_license_number STRING,
	agent_net_promoter_score DECIMAL(3,1),
	agent_commission_percentage DECIMAL(4,2),
	effective_from_ts TIMESTAMP,
	effective_to_ts TIMESTAMP,
	record_version INTEGER,
	created_by STRING,
	created_ts TIMESTAMP,
	last_updated_by STRING,
	last_updated_ts TIMESTAMP,
	attr_hash STRING
)
PARTITIONED BY (agent_id, effective_from_ts);

ALTER TABLE dim_broker
	ADD CONSTRAINT XPKDIM_BROKER PRIMARY KEY (broker_sk);

CREATE OR REPLACE TABLE dim_campaign
(
	campaign_sk BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY ,
	campaign_id STRING,
	campaign_name STRING,
	campaign_type STRING,
	campaign_start_date DATE,
	campaign_end_date DATE,
	campaign_status STRING,
	campaign_budget INTEGER,
	campaign_target_audience STRING,
	campaign_marketing_source STRING,
	campaign_owner_department STRING,
	campaign_country STRING,
	campaign_conversion_goal STRING,
	number_of_clicks INTEGER,
	is_active CHAR(1),
	number_of_visits INTEGER,
	number_of_policy_purchases INTEGER,
	number_of_emails_sent INTEGER,
	number_of_email_bounced INTEGER,
	number_of_emails_delivered INTEGER,
	number_of_emails_opened INTEGER,
	click_through_rate DECIMAL(10,9),
	spend_amt DECIMAL(12,4),
	incremental_revenue DECIMAL(18,4),
	survey_wave STRING,
	total_number_of_respondents INTEGER,
	number_of_respondents_aware INTEGER,
	number_of_promoters INTEGER,
	number_of_passives INTEGER,
	number_of_detractors INTEGER,
	number_of_followers INTEGER,
	number_of_likes INTEGER,
	number_of_comments INTEGER,
	number_of_shares INTEGER,
	number_of_brand_mentions INTEGER,
	number_of_category_mentions INTEGER,
	number_of_impressions INTEGER,
	created_by STRING,
	created_ts TIMESTAMP,
	last_updated_by STRING,
	last_updated_ts TIMESTAMP,
	attr_hash STRING
)
PARTITIONED BY (campaign_id);

ALTER TABLE dim_campaign
	ADD CONSTRAINT XPKDIM_CAMPAIGN PRIMARY KEY (campaign_sk);

CREATE OR REPLACE TABLE dim_channel
(
	channel_sk BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY ,
	channel_id STRING,
	channel_name STRING,
	channel_type STRING,
	created_by STRING,
	created_ts TIMESTAMP,
	last_updated_by STRING,
	last_updated_ts TIMESTAMP,
	attr_hash STRING
)
PARTITIONED BY (channel_id);

ALTER TABLE dim_channel
	ADD CONSTRAINT XPKDIM_CHANNEL PRIMARY KEY (channel_sk);

CREATE OR REPLACE TABLE dim_claim
(
	claim_sk BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY ,
	claim_id STRING,
	claim_number STRING,
	claim_type STRING,
	claim_status STRING,
	claim_reason STRING,
	claim_channel STRING,
	claim_handler STRING,
	claim_reported_date DATE,
	claim_settlement_date DATE,
	claim_product STRING,
	is_claim_suspicious CHAR(1),
	is_claim_fraud CHAR(1),
	claim_fraud_status STRING,
	claim_fraud_type STRING,
	claim_fraud_detection_method STRING,
	is_litigation CHAR(1),
	litigation_reason STRING,
	litigation_start_date DATE,
	litigation_end_date DATE,
	litigation_outcome STRING,
	litigation_duration_days INTEGER,
	claim_fraud_detection_time_in_days INTEGER,
	is_recovery_opportunity CHAR(1),
	recovery_priority_score INTEGER,
	recovery_category STRING,
	recovery_source STRING,
	first_recovery_date DATE,
	last_recovery_date DATE,
	is_recovery_happened CHAR(1),
	days_to_first_recovery INTEGER,
	days_to_last_recovery INTEGER,
	avg_days_to_close_claim INTEGER,
	claim_fraud_outcome STRING,
	recovery_type STRING,
	recovery_band STRING,
	third_party_involved STRING,
	third_party_involved_overall_score DECIMAL(10,9),
	solicitor STRING,
	claim_amt DECIMAL(18,4),
	claims_paid DECIMAL(18,4),
	outstanding_reserve DECIMAL(18,4),
	claims_expenses DECIMAL(18,4),
	recovery_received DECIMAL(18,4),
	compensation_offered DECIMAL(18,4),
	remediation_amt DECIMAL(18,4),
	suspected_amt DECIMAL(18,4),
	fraud_amt DECIMAL(18,4),
	legal_expenses DECIMAL(18,4),
	claim_band STRING,
	claim_band_sort INTEGER,
	is_fault_claim CHAR(1),
	claim_satisfaction_score STRING,
	effective_from_ts TIMESTAMP,
	effective_to_ts TIMESTAMP,
	record_version INTEGER,
	created_by STRING,
	created_ts TIMESTAMP,
	last_updated_by STRING,
	last_updated_ts TIMESTAMP,
	attr_hash STRING
)
PARTITIONED BY (claim_number, effective_from_ts);

ALTER TABLE dim_claim
	ADD CONSTRAINT XPKDIM_CLAIM PRIMARY KEY (claim_sk);

CREATE OR REPLACE TABLE dim_customer
(
	customer_sk BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY ,
	customer_id STRING,
	customer_number INTEGER,
	customer_rating INTEGER,
	customer_segment STRING,
	line_of_business STRING,
	net_promoter_score FLOAT,
	customer_since_date DATE,
	customer_status_code STRING,
	customer_status_reason STRING,
	income_band STRING,
	customer_satisfaction STRING,
	customer_age_band STRING,
	net_promotor_code_segment STRING,
	effective_from_ts TIMESTAMP,
	effective_to_ts TIMESTAMP,
	record_version INTEGER,
	created_by STRING,
	created_ts TIMESTAMP,
	last_updated_by STRING,
	last_updated_ts TIMESTAMP,
	attr_hash STRING
)
PARTITIONED BY (customer_number, effective_from_ts);

ALTER TABLE dim_customer
	ADD CONSTRAINT XPKDIM_CUSTOMER PRIMARY KEY (customer_sk);

CREATE OR REPLACE TABLE dim_date
(
	date_sk BIGINT NOT NULL,
	day_name STRING,
	day_of_month_number INTEGER,
	month_name STRING,
	year_number INTEGER,
	full_date DATE,
	is_weekend CHAR(1),
	month_number INTEGER,
	quarter_number INTEGER,
	week_of_year_number INTEGER,
	month_short_name STRING,
	created_by STRING,
	created_ts TIMESTAMP
);

ALTER TABLE dim_date
	ADD CONSTRAINT XPKDIM_DATE PRIMARY KEY (date_sk);

CREATE OR REPLACE TABLE dim_geography
(
	geography_sk BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY ,
	city STRING,
	state STRING,
	country STRING,
	region STRING,
	created_by STRING,
	created_ts TIMESTAMP,
	last_updated_by STRING,
	last_updated_ts TIMESTAMP,
	attr_hash STRING
)
PARTITIONED BY (country, created_ts);

ALTER TABLE dim_geography
	ADD CONSTRAINT XPKDIM_GEOGRAPHY PRIMARY KEY (geography_sk);

CREATE OR REPLACE TABLE dim_home
(
	insured_object_sk BIGINT NOT NULL,
	insured_object_home_id STRING,
	is_existing_home_customer CHAR(1),
	home_risk_address STRING,
	home_state STRING,
	home_type STRING,
	roof_construction_material_type STRING,
	wall_construction_material_type STRING,
	effective_from_ts TIMESTAMP,
	effective_to_ts TIMESTAMP,
	record_version INTEGER,
	created_by STRING,
	created_ts TIMESTAMP,
	last_updated_by STRING,
	last_updated_ts TIMESTAMP,
	attr_hash STRING
)
PARTITIONED BY (insured_object_home_id, effective_from_ts);

ALTER TABLE dim_home
	ADD CONSTRAINT XPKDIM_HOME PRIMARY KEY (insured_object_sk);

CREATE OR REPLACE TABLE dim_identity
(
	identity_sk BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY ,
	identity_id STRING,
	experience_cloud_id STRING,
	email_address_hash_value STRING,
	effective_from_ts TIMESTAMP,
	effective_to_ts TIMESTAMP,
	record_version INTEGER,
	created_by STRING,
	created_ts TIMESTAMP,
	last_updated_by STRING,
	last_updated_ts TIMESTAMP,
	attr_hash STRING
)
PARTITIONED BY (experience_cloud_id, effective_from_ts);

ALTER TABLE dim_identity
	ADD CONSTRAINT XPKDIM_IDENTITY PRIMARY KEY (identity_sk);

CREATE OR REPLACE TABLE dim_insured_object
(
	insured_object_sk BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY ,
	insured_object_id STRING,
	insured_object_type STRING,
	insured_object_sub_type STRING,
	insured_object_desc STRING,
	insured_value FLOAT,
	currency_code STRING,
	insured_object_start_date DATE,
	insured_object_end_date DATE,
	insured_object_current_status STRING,
	created_by STRING,
	created_ts TIMESTAMP,
	last_updated_by STRING,
	last_updated_ts TIMESTAMP,
	attr_hash STRING
);

ALTER TABLE dim_insured_object
	ADD CONSTRAINT XPKDIM_INSURED_OBJECT PRIMARY KEY (insured_object_sk);

CREATE OR REPLACE TABLE dim_marketing
(
	marketing_sk BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY ,
	marketing_preference_id STRING,
	is_any_communication CHAR(1),
	preferred_contact_method STRING,
	is_email_subscriptions CHAR(1),
	is_commercial_email CHAR(1),
	is_personal_email CHAR(1),
	is_call CHAR(1),
	is_sms CHAR(1),
	is_postal_mail CHAR(1),
	marketing_engagement_id STRING,
	is_opened_email CHAR(1),
	marketing_status STRING,
	promotion_code STRING,
	has_retention_team_interaction CHAR(1),
	customer_service_call_frequency INTEGER,
	average_call_sentiment STRING,
	engagement_score STRING,
	effective_from_ts TIMESTAMP,
	effective_to_ts TIMESTAMP,
	record_version INTEGER,
	created_by STRING,
	created_ts TIMESTAMP,
	last_updated_by STRING,
	last_updated_ts TIMESTAMP,
	attr_hash STRING
)
PARTITIONED BY (marketing_preference_id, marketing_engagement_id, effective_from_ts);

ALTER TABLE dim_marketing
	ADD CONSTRAINT XPKDIM_MARKETING PRIMARY KEY (marketing_sk);

CREATE OR REPLACE TABLE dim_motor
(
	insured_object_sk BIGINT NOT NULL,
	insured_object_motor_id STRING,
	is_auto_decline_vehicle CHAR(1),
	is_existing_motor_customer CHAR(1),
	motor_lapsed_policies INTEGER,
	vehicle_sum_insured_amt DECIMAL(18,4),
	vehicle_risk_class_code STRING,
	vehicle_risk_address STRING,
	vehicle_body_type STRING,
	vehicle_fuel_type STRING,
	vehicle_variant STRING,
	vehicle_age INTEGER,
	vehicle_class STRING,
	vehicle_model STRING,
	vehicle_owner_type STRING,
	vehicle_reg_state STRING,
	vehicle_type STRING,
	vehicle_year INTEGER,
	driver_license_status STRING,
	driver_experience_years INTEGER,
	effective_from_ts TIMESTAMP,
	effective_to_ts TIMESTAMP,
	record_version INTEGER,
	created_by STRING,
	created_ts TIMESTAMP,
	last_updated_by STRING,
	last_updated_ts TIMESTAMP,
	attr_hash STRING
)
PARTITIONED BY (insured_object_motor_id, effective_from_ts);

ALTER TABLE dim_motor
	ADD CONSTRAINT XPKDIM_MOTOR PRIMARY KEY (insured_object_sk);

CREATE OR REPLACE TABLE dim_override
(
	override_sk BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY ,
	override_id STRING,
	override_reason STRING,
	created_by STRING,
	created_ts TIMESTAMP,
	last_updated_by STRING,
	last_updated_ts TIMESTAMP,
	attr_hash STRING
)
PARTITIONED BY (override_id);

ALTER TABLE dim_override
	ADD CONSTRAINT XPKDIM_OVERRIDE PRIMARY KEY (override_sk);

CREATE OR REPLACE TABLE dim_person
(
	person_sk BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY ,
	geography_sk BIGINT NOT NULL,
	identity_sk BIGINT,
	person_id STRING,
	courtesy_title STRING,
	first_name STRING,
	last_name STRING,
	full_name STRING,
	person_type STRING,
	birth_date DATE,
	gender STRING,
	nationality STRING,
	marital_status STRING,
	occupation STRING,
	address_id STRING,
	address_type CHAR(18),
	street_address STRING,
	postcode STRING,
	contact_id STRING,
	home_phone_number STRING,
	work_phone_number STRING,
	personal_email STRING,
	work_email STRING,
	job_title STRING,
	role STRING,
	company_name STRING,
	date_of_constitution DATE,
	is_lead CHAR(1),
	preferred_language STRING,
	source_id STRING,
	source_type STRING,
	tenant_id STRING,
	assesed_disability_degree STRING,
	is_operational_paperless_consent CHAR(1),
	consent_id STRING,
	is_opt_in_legitimate_interest CHAR(1),
	is_opt_in_validated CHAR(1),
	effective_from_ts TIMESTAMP,
	effective_to_ts TIMESTAMP,
	record_version INTEGER,
	created_by STRING,
	created_ts TIMESTAMP,
	last_updated_by STRING,
	last_updated_ts TIMESTAMP,
	attr_hash STRING
)
PARTITIONED BY (person_id, effective_from_ts);

ALTER TABLE dim_person
	ADD CONSTRAINT XPKDIM_PERSON PRIMARY KEY (person_sk);

CREATE OR REPLACE TABLE dim_policy
(
	policy_sk BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY ,
	channel_sk BIGINT NOT NULL,
	product_sk BIGINT NOT NULL,
	insured_object_sk BIGINT NOT NULL,
	policy_id STRING,
	policy_number STRING,
	policy_start_ts TIMESTAMP,
	policy_end_ts TIMESTAMP,
	policy_tenure INTEGER,
	policy_cycle INTEGER,
	renewal_date DATE,
	policy_status STRING,
	policy_cover_option STRING,
	policy_sales_channel STRING,
	is_fraud CHAR(1),
	quote_id STRING,
	policy_type STRING,
	policy_issue_date DATE,
	is_policy_renewal CHAR(1),
	policy_cancellation_reason STRING,
	policy_sum_insured DECIMAL(18,4),
	policy_retention_limit DECIMAL(18,4),
	policy_risk_score DECIMAL(4,3),
	policy_risk_band STRING,
	is_auto_renew_enabled CHAR(1),
	no_claims_discount_years INTEGER,
	payment_method STRING,
	is_direct_debit_cancellation CHAR(1),
	missed_payment_count INTEGER,
	loyalty_discount_usage STRING,
	is_installment_default CHAR(1),
	effective_from_ts TIMESTAMP,
	effective_to_ts TIMESTAMP,
	record_version INTEGER,
	created_by STRING,
	created_ts TIMESTAMP,
	last_updated_by STRING,
	last_updated_ts TIMESTAMP,
	attr_hash STRING
)
PARTITIONED BY (policy_number, effective_from_ts);

ALTER TABLE dim_policy
	ADD CONSTRAINT XPKDIM_POLICY PRIMARY KEY (policy_sk);

CREATE OR REPLACE TABLE dim_product
(
	product_sk BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY ,
	product_id STRING,
	product_type STRING,
	product_variant STRING,
	product_name STRING,
	product_launch_date DATE,
	product_status STRING,
	product_line_of_business_code STRING,
	underwriting_group STRING,
	regulatory_approval_code STRING,
	effective_from_ts TIMESTAMP,
	effective_to_ts TIMESTAMP,
	record_version INTEGER,
	created_by STRING,
	created_ts TIMESTAMP,
	last_updated_by STRING,
	last_updated_ts TIMESTAMP,
	attr_hash STRING
)
PARTITIONED BY (product_id, effective_from_ts);

ALTER TABLE dim_product
	ADD CONSTRAINT XPKDIM_PRODUCT PRIMARY KEY (product_sk);

CREATE OR REPLACE TABLE dim_regulation
(
	regulation_sk BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY ,
	regulation_id STRING,
	regulation_number STRING,
	regulation_name STRING,
	regulation_department STRING,
	regulation_region STRING,
	regulation_risk_level STRING,
	regulation_compliance_status STRING,
	regulation_date_raised DATE,
	regulation_date_closed DATE,
	regulation_owner STRING,
	regulation_deadline_date DATE,
	is_regulation_on_time CHAR(1),
	created_by STRING,
	created_ts TIMESTAMP,
	last_updated_by STRING,
	last_updated_ts TIMESTAMP,
	attr_hash STRING
)
PARTITIONED BY (regulation_number);

ALTER TABLE dim_regulation
	ADD CONSTRAINT XPKDIM_REGULATION PRIMARY KEY (regulation_sk);

CREATE OR REPLACE TABLE fact_complaint
(
	person_sk BIGINT NOT NULL,
	customer_sk BIGINT NOT NULL,
	regulation_sk BIGINT NOT NULL,
	channel_sk BIGINT NOT NULL,
	date_sk BIGINT NOT NULL,
	insured_object_sk BIGINT NOT NULL,
	person_id STRING,
	complaint_id STRING,
	complaint_date DATE,
	complaint_acknowledgement_date DATE,
	complaint_resolved_date DATE,
	complaint_upheld_status STRING,
	is_financial_ombudsman_service_referral CHAR(1),
	complaint_driver STRING,
	complaint_channel STRING,
	compensation_amt DECIMAL(18,4),
	complaint_status STRING,
	insurance_category STRING,
	created_by STRING,
	created_ts TIMESTAMP,
	load_ts TIMESTAMP
)
PARTITIONED BY (created_ts);

CREATE OR REPLACE TABLE fact_lead
(
	person_sk BIGINT NOT NULL,
	date_sk BIGINT NOT NULL,
	marketing_sk BIGINT NOT NULL,
	lead_id STRING,
	person_id STRING,
	interested_level STRING,
	person_score FLOAT,
	person_status STRING,
	lead_creation_ts TIMESTAMP,
	created_by STRING,
	created_ts TIMESTAMP,
	load_ts TIMESTAMP
)
PARTITIONED BY (created_ts);

CREATE OR REPLACE TABLE fact_policy
(
	policy_sk BIGINT NOT NULL,
	person_sk BIGINT NOT NULL,
	customer_sk BIGINT NOT NULL,
	account_sk BIGINT,
	date_sk BIGINT NOT NULL,
	marketing_sk BIGINT,
	channel_sk BIGINT NOT NULL,
	broker_sk BIGINT NOT NULL,
	claim_sk BIGINT NOT NULL,
	override_sk BIGINT NOT NULL,
	insured_object_sk BIGINT NOT NULL,
	person_id STRING,
	active_claims_number INTEGER,
	previous_claims_number INTEGER,
	declined_claims_number INTEGER,
	policy_gross_revenue_amt DECIMAL(18,4),
	policy_net_revenue_amt DECIMAL(18,4),
	policy_renewal_current_period_amt DECIMAL(18,4),
	policy_renewal_next_period_amt DECIMAL(18,4),
	policy_base_premium DECIMAL(18,4),
	gross_written_premium DECIMAL(18,4),
	earned_premium DECIMAL(18,4),
	incurred_but_not_reported DECIMAL(18,4),
	operating_expenses DECIMAL(18,4),
	administrative_expenses DECIMAL(18,4),
	profit_margin DECIMAL(18,4),
	taxes_and_levies DECIMAL(18,4),
	amt_approved DECIMAL(18,4),
	ceded_premium DECIMAL(18,4),
	commission_paid DECIMAL(18,4),
	ceded_commission DECIMAL(18,4),
	exposure_amt DECIMAL(18,4),
	investment_income DECIMAL(18,4),
	underwriting_cycle_time_in_days INTEGER,
	underwriting_expenses DECIMAL(18,4),
	transaction_date DATE,
	record_type STRING,
	discount DECIMAL(18,4),
	override_commission DECIMAL(18,4),
	partial_recovery_percentage DECIMAL(18,4),
	created_by STRING,
	created_ts TIMESTAMP,
	load_ts TIMESTAMP
)
PARTITIONED BY (created_ts);

CREATE OR REPLACE TABLE fact_quote
(
	person_sk BIGINT NOT NULL,
	customer_sk BIGINT NOT NULL,
	account_sk BIGINT,
	marketing_sk BIGINT,
	date_sk BIGINT NOT NULL,
	campaign_sk BIGINT NOT NULL,
	channel_sk BIGINT NOT NULL,
	broker_sk BIGINT NOT NULL,
	insured_object_sk BIGINT NOT NULL,
	quote_id STRING,
	person_id STRING,
	quote_number STRING,
	quote_status STRING,
	quote_gross_revenue_amt DECIMAL(18,4),
	quote_net_revenue_amt DECIMAL(18,4),
	quote_renewal_current_period_amt DECIMAL(18,4),
	quote_renewal_next_period_amt DECIMAL(18,4),
	quoted_premium DECIMAL(18,4),
	quote_date DATE,
	quote_month_name STRING,
	risk_score INTEGER,
	policy_complexity STRING,
	uw_approval_type STRING,
	rejection_reason STRING,
	created_by STRING,
	created_ts TIMESTAMP,
	load_ts TIMESTAMP
)
PARTITIONED BY (created_ts);

ALTER TABLE dim_home
	ADD CONSTRAINT R_71 FOREIGN KEY (insured_object_sk) REFERENCES dim_insured_object (insured_object_sk);

ALTER TABLE dim_motor
	ADD CONSTRAINT R_72 FOREIGN KEY (insured_object_sk) REFERENCES dim_insured_object (insured_object_sk);

ALTER TABLE dim_person
	ADD CONSTRAINT R_24 FOREIGN KEY (identity_sk) REFERENCES dim_identity (identity_sk);

ALTER TABLE dim_person
	ADD CONSTRAINT R_25 FOREIGN KEY (geography_sk) REFERENCES dim_geography (geography_sk);

ALTER TABLE dim_policy
	ADD CONSTRAINT R_65 FOREIGN KEY (channel_sk) REFERENCES dim_channel (channel_sk);

ALTER TABLE dim_policy
	ADD CONSTRAINT R_73 FOREIGN KEY (product_sk) REFERENCES dim_product (product_sk);

ALTER TABLE dim_policy
	ADD CONSTRAINT R_70 FOREIGN KEY (insured_object_sk) REFERENCES dim_insured_object (insured_object_sk);

ALTER TABLE fact_complaint
	ADD CONSTRAINT R_53 FOREIGN KEY (customer_sk) REFERENCES dim_customer (customer_sk);

ALTER TABLE fact_complaint
	ADD CONSTRAINT R_52 FOREIGN KEY (regulation_sk) REFERENCES dim_regulation (regulation_sk);

ALTER TABLE fact_complaint
	ADD CONSTRAINT R_56 FOREIGN KEY (channel_sk) REFERENCES dim_channel (channel_sk);

ALTER TABLE fact_complaint
	ADD CONSTRAINT R_68 FOREIGN KEY (person_sk) REFERENCES dim_person (person_sk);

ALTER TABLE fact_complaint
	ADD CONSTRAINT R_74 FOREIGN KEY (date_sk) REFERENCES dim_date (date_sk);

ALTER TABLE fact_complaint
	ADD CONSTRAINT R_77 FOREIGN KEY (insured_object_sk) REFERENCES dim_insured_object (insured_object_sk);

ALTER TABLE fact_lead
	ADD CONSTRAINT FK_FACT_LEAD__DIM_DATE FOREIGN KEY (date_sk) REFERENCES dim_date (date_sk);

ALTER TABLE fact_lead
	ADD CONSTRAINT FK_FACT_LEAD__DIM_MARKETING FOREIGN KEY (marketing_sk) REFERENCES dim_marketing (marketing_sk);

ALTER TABLE fact_lead
	ADD CONSTRAINT FK_FACT_LEAD__DIM_PERSON FOREIGN KEY (person_sk) REFERENCES dim_person (person_sk);

ALTER TABLE fact_policy
	ADD CONSTRAINT R_26 FOREIGN KEY (customer_sk) REFERENCES dim_customer (customer_sk);

ALTER TABLE fact_policy
	ADD CONSTRAINT R_59 FOREIGN KEY (claim_sk) REFERENCES dim_claim (claim_sk);

ALTER TABLE fact_policy
	ADD CONSTRAINT R_58 FOREIGN KEY (broker_sk) REFERENCES dim_broker (broker_sk);

ALTER TABLE fact_policy
	ADD CONSTRAINT R_57 FOREIGN KEY (channel_sk) REFERENCES dim_channel (channel_sk);

ALTER TABLE fact_policy
	ADD CONSTRAINT R_60 FOREIGN KEY (override_sk) REFERENCES dim_override (override_sk);

ALTER TABLE fact_policy
	ADD CONSTRAINT R_76 FOREIGN KEY (insured_object_sk) REFERENCES dim_insured_object (insured_object_sk);

ALTER TABLE fact_policy
	ADD CONSTRAINT FK_FACT_POLICY__DIM_ACCOUNT FOREIGN KEY (account_sk) REFERENCES dim_account (account_sk);

ALTER TABLE fact_policy
	ADD CONSTRAINT FK_FACT_POLICY__DIM_DATE FOREIGN KEY (date_sk) REFERENCES dim_date (date_sk);

ALTER TABLE fact_policy
	ADD CONSTRAINT FK_FACT_POLICY__DIM_MARKETING FOREIGN KEY (marketing_sk) REFERENCES dim_marketing (marketing_sk);

ALTER TABLE fact_policy
	ADD CONSTRAINT FK_FACT_POLICY__DIM_PERSON FOREIGN KEY (person_sk) REFERENCES dim_person (person_sk);

ALTER TABLE fact_policy
	ADD CONSTRAINT FK_FACT_POLICY__DIM_POLICY FOREIGN KEY (policy_sk) REFERENCES dim_policy (policy_sk);

ALTER TABLE fact_quote
	ADD CONSTRAINT R_61 FOREIGN KEY (campaign_sk) REFERENCES dim_campaign (campaign_sk);

ALTER TABLE fact_quote
	ADD CONSTRAINT R_63 FOREIGN KEY (channel_sk) REFERENCES dim_channel (channel_sk);

ALTER TABLE fact_quote
	ADD CONSTRAINT R_75 FOREIGN KEY (broker_sk) REFERENCES dim_broker (broker_sk);

ALTER TABLE fact_quote
	ADD CONSTRAINT R_78 FOREIGN KEY (insured_object_sk) REFERENCES dim_insured_object (insured_object_sk);

ALTER TABLE fact_quote
	ADD CONSTRAINT FK_FACT_QUOTES__DIM_ACCOUNT FOREIGN KEY (account_sk) REFERENCES dim_account (account_sk);

ALTER TABLE fact_quote
	ADD CONSTRAINT FK_FACT_QUOTES__DIM_CUSTOMER FOREIGN KEY (customer_sk) REFERENCES dim_customer (customer_sk);

ALTER TABLE fact_quote
	ADD CONSTRAINT FK_FACT_QUOTES__DIM_DATE FOREIGN KEY (date_sk) REFERENCES dim_date (date_sk);

ALTER TABLE fact_quote
	ADD CONSTRAINT FK_FACT_QUOTES__DIM_MARKETING FOREIGN KEY (marketing_sk) REFERENCES dim_marketing (marketing_sk);

ALTER TABLE fact_quote
	ADD CONSTRAINT FK_FACT_QUOTES__DIM_PERSON FOREIGN KEY (person_sk) REFERENCES dim_person (person_sk);