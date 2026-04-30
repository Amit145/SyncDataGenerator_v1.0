

CREATE TABLE hub_account
(
	account_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	account_id STRING
);

ALTER TABLE hub_account
	ADD CONSTRAINT xpkhub_account PRIMARY KEY (account_hash_key);

CREATE TABLE hub_broker
(
	broker_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	agent_id STRING
);

ALTER TABLE hub_broker
	ADD CONSTRAINT xpkhub_lead PRIMARY KEY (broker_hash_key);

CREATE TABLE hub_campaign
(
	campaign_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	campaign_id STRING
);

ALTER TABLE hub_campaign
	ADD CONSTRAINT xpkhub_lead PRIMARY KEY (campaign_hash_key);

CREATE TABLE hub_channel
(
	channel_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	channel_id STRING
);

ALTER TABLE hub_channel
	ADD CONSTRAINT xpkhub_lead PRIMARY KEY (channel_hash_key);

CREATE TABLE hub_claim
(
	claim_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	claim_id STRING
);

ALTER TABLE hub_claim
	ADD CONSTRAINT xpkhub_lead PRIMARY KEY (claim_hash_key);

CREATE TABLE hub_complaints
(
	complaints_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	complaint_id STRING
);

ALTER TABLE hub_complaints
	ADD CONSTRAINT xpkhub_lead PRIMARY KEY (complaints_hash_key);

CREATE TABLE hub_consent
(
	consent_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	consent_id STRING
);

ALTER TABLE hub_consent
	ADD CONSTRAINT xpkhub_consent PRIMARY KEY (consent_hash_key);

CREATE TABLE hub_contact
(
	contact_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	contact_id STRING
);

ALTER TABLE hub_contact
	ADD CONSTRAINT xpkhub_contact PRIMARY KEY (contact_hash_key);

CREATE TABLE hub_customer
(
	customer_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	customer_id STRING
);

ALTER TABLE hub_customer
	ADD CONSTRAINT xpkhub_customer PRIMARY KEY (customer_hash_key);

CREATE TABLE hub_home
(
	home_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	home_id STRING
);

ALTER TABLE hub_home
	ADD CONSTRAINT xpkhub_home PRIMARY KEY (home_hash_key);

CREATE TABLE hub_home_address
(
	home_address_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	home_address_id STRING
);

ALTER TABLE hub_home_address
	ADD CONSTRAINT xpkhub_home_address PRIMARY KEY (home_address_hash_key);

CREATE TABLE hub_identities
(
	identities_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	identities_id STRING
);

ALTER TABLE hub_identities
	ADD CONSTRAINT xpkhub_identities PRIMARY KEY (identities_hash_key);

CREATE TABLE hub_lead
(
	lead_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	lead_id STRING
);

ALTER TABLE hub_lead
	ADD CONSTRAINT xpkhub_lead PRIMARY KEY (lead_hash_key);

CREATE TABLE hub_legal_person
(
	legal_person_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	legal_person_id STRING
);

ALTER TABLE hub_legal_person
	ADD CONSTRAINT xpkhub_legal_person PRIMARY KEY (legal_person_hash_key);

CREATE TABLE hub_marketing_engagement
(
	marketing_engagement_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	marketing_engagement_id STRING
);

ALTER TABLE hub_marketing_engagement
	ADD CONSTRAINT xpkhub_marketing_engagement PRIMARY KEY (marketing_engagement_hash_key);

CREATE TABLE hub_marketing_preference
(
	marketing_preference_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	marketing_preference_id STRING
);

ALTER TABLE hub_marketing_preference
	ADD CONSTRAINT xpkhub_marketing_preference PRIMARY KEY (marketing_preference_hash_key);

CREATE TABLE hub_motor
(
	motor_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	motor_id STRING
);

ALTER TABLE hub_motor
	ADD CONSTRAINT xpkhub_motor PRIMARY KEY (motor_hash_key);

CREATE TABLE hub_natural_person
(
	natural_person_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	natural_person_id STRING
);

ALTER TABLE hub_natural_person
	ADD CONSTRAINT xpkhub_natural_person PRIMARY KEY (natural_person_hash_key);

CREATE TABLE hub_override
(
	override_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	override_id STRING
);

ALTER TABLE hub_override
	ADD CONSTRAINT xpkhub_lead PRIMARY KEY (override_hash_key);

CREATE TABLE hub_person
(
	person_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	person_id STRING
);

ALTER TABLE hub_person
	ADD CONSTRAINT xpkhub_person PRIMARY KEY (person_hash_key);

CREATE TABLE hub_policy
(
	policy_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	policy_id STRING
);

ALTER TABLE hub_policy
	ADD CONSTRAINT xpkhub_policy PRIMARY KEY (policy_hash_key);

CREATE TABLE hub_product
(
	product_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	product_id STRING
);

ALTER TABLE hub_product
	ADD CONSTRAINT xpkhub_product PRIMARY KEY (product_hash_key);

CREATE TABLE hub_quote
(
	quote_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	quote_id STRING
);

ALTER TABLE hub_quote
	ADD CONSTRAINT xpkhub_quote PRIMARY KEY (quote_hash_key);

CREATE TABLE hub_regulations
(
	regulations_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	regulation_id STRING
);

ALTER TABLE hub_regulations
	ADD CONSTRAINT xpkhub_lead PRIMARY KEY (regulations_hash_key);

CREATE TABLE link_complaints_customer
(
	complaints_customer_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	complaints_hash_key STRING NOT NULL,
	customer_hash_key STRING NOT NULL
);

ALTER TABLE link_complaints_customer
	ADD CONSTRAINT xpklink_person_contact PRIMARY KEY (complaints_customer_hash_key);

CREATE TABLE link_customer_lead
(
	customer_lead_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	lead_hash_key STRING,
	customer_hash_key STRING
);

ALTER TABLE link_customer_lead
	ADD CONSTRAINT xpklink_customer_lead PRIMARY KEY (customer_lead_hash_key);

CREATE TABLE link_customer_person
(
	customer_person_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	customer_hash_key STRING,
	person_hash_key STRING
);

ALTER TABLE link_customer_person
	ADD CONSTRAINT xpklink_customer_person PRIMARY KEY (customer_person_hash_key);

CREATE TABLE link_override_policy
(
	override_policy_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	override_hash_key STRING NOT NULL,
	policy_hash_key STRING NOT NULL
);

ALTER TABLE link_override_policy
	ADD CONSTRAINT xpklink_person_contact PRIMARY KEY (override_policy_hash_key);

CREATE TABLE link_person_account
(
	person_account_hash_key STRING NOT NULL,
	record_source STRING,
	account_hash_key STRING,
	load_date TIMESTAMP,
	person_hash_key STRING
);

ALTER TABLE link_person_account
	ADD CONSTRAINT xpklink_person_account PRIMARY KEY (person_account_hash_key);

CREATE TABLE link_person_broker
(
	person_broker_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	person_hash_key STRING NOT NULL,
	broker_hash_key STRING NOT NULL
);

ALTER TABLE link_person_broker
	ADD CONSTRAINT xpklink_person_contact PRIMARY KEY (person_broker_hash_key);

CREATE TABLE link_person_campaign
(
	person_campaign_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	campaign_hash_key STRING NOT NULL,
	person_hash_key STRING NOT NULL
);

ALTER TABLE link_person_campaign
	ADD CONSTRAINT xpklink_person_contact PRIMARY KEY (person_campaign_hash_key);

CREATE TABLE link_person_consent
(
	person_consent_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	person_hash_key STRING,
	consent_hash_key STRING
);

ALTER TABLE link_person_consent
	ADD CONSTRAINT xpklink_person_consent PRIMARY KEY (person_consent_hash_key);

CREATE TABLE link_person_contact
(
	person_contact_hash_key STRING NOT NULL,
	person_hash_key STRING,
	contact_hash_key STRING,
	load_date TIMESTAMP,
	record_source STRING
);

ALTER TABLE link_person_contact
	ADD CONSTRAINT xpklink_person_contact PRIMARY KEY (person_contact_hash_key);

CREATE TABLE link_person_home_address
(
	person_home_address_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	person_hash_key STRING,
	home_address_hash_key STRING
);

ALTER TABLE link_person_home_address
	ADD CONSTRAINT xpklink_person_home_address PRIMARY KEY (person_home_address_hash_key);

CREATE TABLE link_person_identities
(
	person_identities_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	person_hash_key STRING,
	identities_hash_key STRING
);

ALTER TABLE link_person_identities
	ADD CONSTRAINT xpklink_person_identities PRIMARY KEY (person_identities_hash_key);

CREATE TABLE link_person_lead
(
	person_lead_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	person_hash_key STRING,
	lead_hash_key STRING
);

ALTER TABLE link_person_lead
	ADD CONSTRAINT xpklink_person_lead PRIMARY KEY (person_lead_hash_key);

CREATE TABLE link_person_legal_person
(
	person_legal_person_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	person_hash_key STRING,
	legal_person_hash_key STRING
);

ALTER TABLE link_person_legal_person
	ADD CONSTRAINT xpklink_person_legal_person PRIMARY KEY (person_legal_person_hash_key);

CREATE TABLE link_person_marketing_engagement
(
	person_marketing_engagement_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	person_hash_key STRING,
	marketing_engagement_hash_key STRING
);

ALTER TABLE link_person_marketing_engagement
	ADD CONSTRAINT xpklink_person_marketing_engagement PRIMARY KEY (person_marketing_engagement_hash_key);

CREATE TABLE link_person_marketing_preference
(
	person_marketing_preference_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	person_hash_key STRING,
	marketing_preference_hash_key STRING
);

ALTER TABLE link_person_marketing_preference
	ADD CONSTRAINT xpklink_person_marketing_preference PRIMARY KEY (person_marketing_preference_hash_key);

CREATE TABLE link_person_natural_person
(
	person_natural_person_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	person_hash_key STRING,
	natural_person_hash_key STRING
);

ALTER TABLE link_person_natural_person
	ADD CONSTRAINT xpklink_person_natural_person PRIMARY KEY (person_natural_person_hash_key);

CREATE TABLE link_policy_channel
(
	policy_channel_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	channel_hash_key STRING NOT NULL,
	policy_hash_key STRING NOT NULL
);

ALTER TABLE link_policy_channel
	ADD CONSTRAINT xpklink_person_contact PRIMARY KEY (policy_channel_hash_key);

CREATE TABLE link_policy_claim
(
	policy_claim_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	policy_hash_key STRING NOT NULL,
	claim_hash_key STRING NOT NULL
);

ALTER TABLE link_policy_claim
	ADD CONSTRAINT xpklink_person_contact PRIMARY KEY (policy_claim_hash_key);

CREATE TABLE link_policy_customer
(
	policy_customer_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	customer_hash_key STRING,
	policy_hash_key STRING
);

ALTER TABLE link_policy_customer
	ADD CONSTRAINT xpklink_policy_customer PRIMARY KEY (policy_customer_hash_key);

CREATE TABLE link_policy_product
(
	policy_customer_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	policy_hash_key STRING,
	product_hash_key STRING
);

ALTER TABLE link_policy_product
	ADD CONSTRAINT xpklink_policy_product PRIMARY KEY (policy_customer_hash_key);

CREATE TABLE link_product_home
(
	product_home_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	product_hash_key STRING,
	home_hash_key STRING
);

ALTER TABLE link_product_home
	ADD CONSTRAINT xpklink_product_home PRIMARY KEY (product_home_hash_key);

CREATE TABLE link_product_motor
(
	product_motor_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	product_hash_key STRING,
	motor_hash_key STRING
);

ALTER TABLE link_product_motor
	ADD CONSTRAINT xpklink_product_motor PRIMARY KEY (product_motor_hash_key);

CREATE TABLE link_quote_person
(
	quote_person_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	quote_hash_key STRING,
	person_hash_key STRING
);

ALTER TABLE link_quote_person
	ADD CONSTRAINT xpklink_quote_person PRIMARY KEY (quote_person_hash_key);

CREATE TABLE link_quote_product
(
	quote_product_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	quote_hash_key STRING,
	product_hash_key STRING
);

ALTER TABLE link_quote_product
	ADD CONSTRAINT xpklink_quote_product PRIMARY KEY (quote_product_hash_key);

CREATE TABLE link_regulations_product
(
	regulations_product_hash_key STRING NOT NULL,
	load_date TIMESTAMP,
	record_source STRING,
	regulations_hash_key STRING NOT NULL,
	product_hash_key STRING NOT NULL
);

ALTER TABLE link_regulations_product
	ADD CONSTRAINT xpklink_person_contact PRIMARY KEY (regulations_product_hash_key);

CREATE TABLE sat_account
(
	account_hash_key STRING NOT NULL,
	load_date TIMESTAMP NOT NULL,
	account_number INT,
	account_type STRING,
	account_last_access TIMESTAMP,
	account_last_change TIMESTAMP,
	account_creation_type STRING,
	account_status STRING
);

ALTER TABLE sat_account
	ADD CONSTRAINT xpksat_account PRIMARY KEY (account_hash_key, load_date);

CREATE TABLE sat_broker
(
	load_date TIMESTAMP NOT NULL,
	broker_hash_key STRING NOT NULL,
	agent_name STRING,
	agent_type STRING,
	agent_license_number STRING,
	agent_net_promoter_score DOUBLE,
	agent_commission_percentage DOUBLE
);

ALTER TABLE sat_broker
	ADD CONSTRAINT xpksat_lead PRIMARY KEY (broker_hash_key, load_date);

CREATE TABLE sat_campaign
(
	load_date TIMESTAMP NOT NULL,
	campaign_hash_key STRING NOT NULL,
	campaign_name STRING,
	campaign_type STRING,
	campaign_start_date DATE,
	campaign_end_date DATE,
	campaign_status STRING,
	campaign_budget INT,
	campaign_target_audience STRING,
	campaign_marketing_source STRING,
	campaign_owner_department STRING,
	campaign_country STRING,
	campaign_conversion_goal STRING
);

ALTER TABLE sat_campaign
	ADD CONSTRAINT xpksat_lead PRIMARY KEY (campaign_hash_key, load_date);

CREATE TABLE sat_channel
(
	load_date TIMESTAMP NOT NULL,
	channel_hash_key STRING NOT NULL,
	channel_name STRING,
	channel_type STRING
);

ALTER TABLE sat_channel
	ADD CONSTRAINT xpksat_lead PRIMARY KEY (channel_hash_key, load_date);

CREATE TABLE sat_claim
(
	load_date TIMESTAMP NOT NULL,
	claim_hash_key STRING NOT NULL,
	claim_number STRING,
	claim_type STRING,
	claim_status STRING,
	claim_reason STRING,
	claim_channel STRING,
	claim_handler STRING,
	claim_reported_date DATE,
	claim_settlement_date DATE,
	claim_product STRING,
	is_claim_suspicious STRING,
	is_claim_fraud STRING,
	claim_fraud_status STRING,
	claim_fraud_type STRING,
	claim_fraud_detection_method STRING,
	is_litigation STRING,
	litigation_reason STRING,
	litigation_start_date DATE,
	litigation_end_date DATE,
	litigation_outcome STRING,
	litigation_duration_days INT,
	claim_fraud_detection_time_in_days INT,
	is_recovery_opportunity STRING,
	recovery_priority_score INT,
	recovery_category STRING,
	recovery_source STRING,
	first_recovery_date DATE,
	last_recovery_date DATE,
	is_recovery_happened STRING,
	days_to_first_recovery INT,
	days_to_last_recovery INT,
	avg_days_to_close_claim INT,
	claim_fraud_outcome STRING,
	recovery_type STRING,
	recovery_band STRING,
	third_party_involved STRING,
	third_party_involved_overall_score DOUBLE,
	solicitor STRING
);

ALTER TABLE sat_claim
	ADD CONSTRAINT xpksat_lead PRIMARY KEY (claim_hash_key, load_date);

CREATE TABLE sat_complaints
(
	load_date TIMESTAMP NOT NULL,
	complaints_hash_key STRING NOT NULL,
	complaint_date DATE,
	complaint_acknowledgement_date DATE,
	complaint_resolved_date DATE,
	complaint_upheld_status STRING,
	is_financial_ombudsman_service_referral STRING,
	complaint_driver STRING,
	complaint_channel STRING,
	compensation_amount INT,
	insurance_category STRING,
	complaint_status STRING
);

ALTER TABLE sat_complaints
	ADD CONSTRAINT xpksat_lead PRIMARY KEY (complaints_hash_key, load_date);

CREATE TABLE sat_consent
(
	consent_hash_key STRING NOT NULL,
	load_date TIMESTAMP NOT NULL,
	opt_in_validated STRING,
	opt_in_legitimate_interest STRING
);

ALTER TABLE sat_consent
	ADD CONSTRAINT xpksat_consent PRIMARY KEY (consent_hash_key, load_date);

CREATE TABLE sat_contact
(
	load_date TIMESTAMP NOT NULL,
	contact_hash_key STRING NOT NULL,
	personal_email STRING,
	work_email STRING,
	work_phone STRING,
	home_phone STRING
);

ALTER TABLE sat_contact
	ADD CONSTRAINT xpksat_contact PRIMARY KEY (contact_hash_key, load_date);

CREATE TABLE sat_customer
(
	load_date TIMESTAMP NOT NULL,
	customer_hash_key STRING NOT NULL,
	customer_number INT,
	customer_status STRING,
	customer_status_reason STRING,
	customer_since TIMESTAMP,
	customer_rating INT,
	customer_segment STRING,
	line_of_business STRING,
	nps_score INT,
	income_band STRING,
	customer_satisfaction STRING,
	customer_age_band STRING,
	net_promotor_code_segment STRING
);

ALTER TABLE sat_customer
	ADD CONSTRAINT xpksat_customer PRIMARY KEY (customer_hash_key, load_date);

CREATE TABLE sat_home
(
	home_hash_key STRING NOT NULL,
	load_date TIMESTAMP NOT NULL,
	wall_construction STRING,
	home_risk_address STRING,
	roof_construction STRING,
	home_type STRING,
	home_state STRING,
	is_existing_home_customer STRING
);

ALTER TABLE sat_home
	ADD CONSTRAINT xpksat_home PRIMARY KEY (home_hash_key, load_date);

CREATE TABLE sat_home_address
(
	home_address_hash_key STRING NOT NULL,
	load_date TIMESTAMP NOT NULL,
	street STRING,
	postcode STRING,
	city STRING,
	state STRING,
	country STRING
);

ALTER TABLE sat_home_address
	ADD CONSTRAINT xpksat_home_address PRIMARY KEY (home_address_hash_key, load_date);

CREATE TABLE sat_identities
(
	identities_hash_key STRING NOT NULL,
	load_date TIMESTAMP NOT NULL,
	ecid STRING,
	hashed_email STRING
);

ALTER TABLE sat_identities
	ADD CONSTRAINT xpksat_identities PRIMARY KEY (identities_hash_key, load_date);

CREATE TABLE sat_lead
(
	lead_hash_key STRING NOT NULL,
	load_date TIMESTAMP NOT NULL,
	interested_level STRING,
	preferred_contact_method STRING,
	person_score INT,
	person_status STRING,
	converted_date TIMESTAMP
);

ALTER TABLE sat_lead
	ADD CONSTRAINT xpksat_lead PRIMARY KEY (lead_hash_key, load_date);

CREATE TABLE sat_legal_person
(
	legal_person_hash_key STRING NOT NULL,
	load_date TIMESTAMP NOT NULL,
	job_title STRING,
	converted_date TIMESTAMP,
	person_status STRING,
	person_score INT,
	company_name STRING,
	source_type STRING,
	source_id STRING,
	date_of_constitution TIMESTAMP
);

ALTER TABLE sat_legal_person
	ADD CONSTRAINT xpksat_legal_person PRIMARY KEY (legal_person_hash_key, load_date);

CREATE TABLE sat_marketing_engagement
(
	marketing_engagement_hash_key STRING NOT NULL,
	load_date TIMESTAMP NOT NULL,
	promotion_code STRING,
	opened_email STRING,
	marketing_status STRING
);

ALTER TABLE sat_marketing_engagement
	ADD CONSTRAINT xpksat_marketing_engagement PRIMARY KEY (marketing_engagement_hash_key, load_date);

CREATE TABLE sat_marketing_preference
(
	marketing_preference_hash_key STRING NOT NULL,
	load_date TIMESTAMP NOT NULL,
	sms STRING,
	email STRING,
	email_subscriptions STRING,
	commercial_email STRING,
	postal_mail STRING,
	call VARCHAR(20),
	any VARCHAR(20)
);

ALTER TABLE sat_marketing_preference
	ADD CONSTRAINT xpksat_marketing_preference PRIMARY KEY (marketing_preference_hash_key, load_date);

CREATE TABLE sat_motor
(
	motor_hash_key STRING NOT NULL,
	load_date TIMESTAMP NOT NULL,
	auto_decline_vehicle STRING,
	body_type STRING,
	fuel_type STRING,
	license_status STRING,
	is_existing_motor_customer STRING,
	motor_lapsed_policies INT,
	motor_risk_address STRING,
	risk_class_code STRING,
	variant STRING,
	vehicle_owner_type STRING,
	vehicle_regstate STRING,
	vehicle_class STRING,
	vehicle_model STRING,
	vehicle_type STRING,
	motor_sum_insrd DOUBLE,
	vehicle_year INT,
	vehicle_age INT
);

ALTER TABLE sat_motor
	ADD CONSTRAINT xpksat_motor PRIMARY KEY (motor_hash_key, load_date);

CREATE TABLE sat_natural_person
(
	load_date TIMESTAMP NOT NULL,
	natural_person_hash_key STRING NOT NULL,
	first_name STRING,
	last_name STRING,
	full_name STRING,
	courtesy_title STRING,
	occupation STRING,
	birth_date date,
	birth_year INT,
	nationality STRING,
	gender STRING,
	marital_status STRING,
	assesed_disability_degree STRING,
	preferred_language STRING,
	job_title STRING,
	role STRING
);

ALTER TABLE sat_natural_person
	ADD CONSTRAINT xpksat_natural_person PRIMARY KEY (natural_person_hash_key, load_date);

CREATE TABLE sat_override
(
	load_date TIMESTAMP NOT NULL,
	override_hash_key STRING NOT NULL,
	override_reason STRING
);

ALTER TABLE sat_override
	ADD CONSTRAINT xpksat_lead PRIMARY KEY (override_hash_key, load_date);

CREATE TABLE sat_person
(
	person_hash_key STRING NOT NULL,
	load_date TIMESTAMP NOT NULL,
	tenant_id STRING,
	is_lead STRING,
	type STRING,
	operational_paperless_consent STRING,
	source_id STRING,
	source_type STRING
);

ALTER TABLE sat_person
	ADD CONSTRAINT xpksat_person PRIMARY KEY (person_hash_key, load_date);

CREATE TABLE sat_policy
(
	policy_hash_key STRING NOT NULL,
	load_date TIMESTAMP NOT NULL,
	cover_option STRING,
	declined_claims INT,
	fraud_flag STRING,
	gross_revenue DOUBLE,
	net_revenue DOUBLE,
	number_of_active_claim INT,
	number_of_previous_claim INT,
	policy_cicle INT,
	policy_end_date TIMESTAMP,
	policy_length INT,
	policy_number INT,
	policy_start_date TIMESTAMP,
	policy_status STRING,
	renewal_amount_current_period DOUBLE,
	renewal_amount_next_period DOUBLE,
	renewal_date TIMESTAMP,
	sales_channel STRING,
	quote_id STRING,
	policy_type STRING,
	is_policy_renewal STRING,
	policy_cancellation_reason STRING,
	policy_sum_insured INT,
	policy_retention_limit INT,
	policy_risk_score DOUBLE,
	policy_risk_band STRING,
	policy_base_premium INT,
	gross_written_premium INT,
	earned_premium INT,
	incurred_but_not_reported INT,
	operating_expenses INT,
	administrative_expenses INT,
	profit_margin INT,
	taxes_and_levies INT,
	amount_approved INT,
	ceded_premium INT,
	commission_paid INT,
	ceded_commission INT,
	exposure_amount INT,
	investment_income INT,
	underwriting_cycle_time_in_days INT,
	underwriting_expenses INT,
	claim_amount INT,
	claims_paid INT,
	outstanding_reserve INT,
	claims_expenses INT,
	recovery_received INT,
	compensation_offered INT,
	remediation_amount INT,
	suspectd_amount INT,
	fraud_amount INT,
	legal_expenses INT,
	transaction_date DATE,
	record_type STRING,
	discount INT,
	override_commission INT,
	partial_recovery_percentage INT,
	claim_band STRING,
	claim_band_sort INT
);

ALTER TABLE sat_policy
	ADD CONSTRAINT xpksat_policy PRIMARY KEY (policy_hash_key, load_date);

CREATE TABLE sat_product
(
	product_hash_key STRING NOT NULL,
	load_date TIMESTAMP NOT NULL,
	type STRING,
	product_variant STRING,
	product_name STRING,
	product_launch_date DATE,
	product_status STRING,
	line_of_business_code STRING,
	underwriting_group STRING,
	regulatory_approval_code STRING
);

ALTER TABLE sat_product
	ADD CONSTRAINT xpksat_product PRIMARY KEY (load_date, product_hash_key);

CREATE TABLE sat_quote
(
	quote_hash_key STRING NOT NULL,
	load_date TIMESTAMP NOT NULL,
	gross_revenue DOUBLE,
	net_revenue DOUBLE,
	quote_number INT,
	quote_status STRING,
	renewal_amt_current_period DOUBLE,
	renewal_amt_next_period DOUBLE,
	quoted_premium INT,
	quote_date DATE,
	quote_month_name STRING,
	clicks INT,
	impressions INT,
	is_active CHAR(18),
	visits INT,
	has_policy_purchases CHAR(18),
	emails_sent CHAR(18),
	email_bounced CHAR(18),
	emails_delivered CHAR(18),
	emails_opened CHAR(18),
	ctr DOUBLE,
	spend DOUBLE,
	incremental_revenue INT,
	survey_wave STRING,
	total_number_of_respondents INT,
	number_of_respondents_aware INT,
	number_of_promoters INT,
	number_of_passives INT,
	number_of_detractors INT,
	number_of_followers INT,
	number_of_likes INT,
	number_of_comments INT,
	number_of_shares INT,
	number_of_brand_mentions INT,
	number_of_category_mentions INT,
	risk_score INT,
	policy_complexity STRING,
	uw_approval_type STRING,
	rejection_reason STRING
);

ALTER TABLE sat_quote
	ADD CONSTRAINT xpksat_quote PRIMARY KEY (quote_hash_key, load_date);

CREATE TABLE sat_regulations
(
	load_date TIMESTAMP NOT NULL,
	regulations_hash_key STRING NOT NULL,
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
	is_regulation_on_time STRING
);

ALTER TABLE sat_regulations
	ADD CONSTRAINT xpksat_lead PRIMARY KEY (regulations_hash_key, load_date);

ALTER TABLE link_complaints_customer
	ADD CONSTRAINT R_68 FOREIGN KEY (complaints_hash_key) REFERENCES hub_complaints (complaints_hash_key);

ALTER TABLE link_complaints_customer
	ADD CONSTRAINT R_76 FOREIGN KEY (customer_hash_key) REFERENCES hub_customer (customer_hash_key);

ALTER TABLE link_customer_lead
	ADD CONSTRAINT xif1_link_customer_lead FOREIGN KEY (customer_hash_key) REFERENCES hub_customer (customer_hash_key);

ALTER TABLE link_customer_lead
	ADD CONSTRAINT xif2_link_customer_lead FOREIGN KEY (lead_hash_key) REFERENCES hub_lead (lead_hash_key);

ALTER TABLE link_customer_person
	ADD CONSTRAINT xif1_link_customer_person FOREIGN KEY (customer_hash_key) REFERENCES hub_customer (customer_hash_key);

ALTER TABLE link_customer_person
	ADD CONSTRAINT xif2_link_customer_person FOREIGN KEY (person_hash_key) REFERENCES hub_person (person_hash_key);

ALTER TABLE link_override_policy
	ADD CONSTRAINT R_65 FOREIGN KEY (override_hash_key) REFERENCES hub_override (override_hash_key);

ALTER TABLE link_override_policy
	ADD CONSTRAINT R_66 FOREIGN KEY (policy_hash_key) REFERENCES hub_policy (policy_hash_key);

ALTER TABLE link_person_account
	ADD CONSTRAINT xif2_link_person_account FOREIGN KEY (account_hash_key) REFERENCES hub_account (account_hash_key);

ALTER TABLE link_person_account
	ADD CONSTRAINT xif3_link_person_account FOREIGN KEY (person_hash_key) REFERENCES hub_person (person_hash_key);

ALTER TABLE link_person_broker
	ADD CONSTRAINT R_63 FOREIGN KEY (person_hash_key) REFERENCES hub_person (person_hash_key);

ALTER TABLE link_person_broker
	ADD CONSTRAINT R_64 FOREIGN KEY (broker_hash_key) REFERENCES hub_broker (broker_hash_key);

ALTER TABLE link_person_campaign
	ADD CONSTRAINT R_67 FOREIGN KEY (campaign_hash_key) REFERENCES hub_campaign (campaign_hash_key);

ALTER TABLE link_person_campaign
	ADD CONSTRAINT R_75 FOREIGN KEY (person_hash_key) REFERENCES hub_person (person_hash_key);

ALTER TABLE link_person_consent
	ADD CONSTRAINT xif1_link_person_consent FOREIGN KEY (person_hash_key) REFERENCES hub_person (person_hash_key);

ALTER TABLE link_person_consent
	ADD CONSTRAINT xif2_link_person_consent FOREIGN KEY (consent_hash_key) REFERENCES hub_consent (consent_hash_key);

ALTER TABLE link_person_contact
	ADD CONSTRAINT xif1_link_person_contact FOREIGN KEY (person_hash_key) REFERENCES hub_person (person_hash_key);

ALTER TABLE link_person_contact
	ADD CONSTRAINT xif2_link_person_contact FOREIGN KEY (contact_hash_key) REFERENCES hub_contact (contact_hash_key);

ALTER TABLE link_person_home_address
	ADD CONSTRAINT xif1_link_person_home_address FOREIGN KEY (person_hash_key) REFERENCES hub_person (person_hash_key);

ALTER TABLE link_person_home_address
	ADD CONSTRAINT xif2_link_person_home_address FOREIGN KEY (home_address_hash_key) REFERENCES hub_home_address (home_address_hash_key);

ALTER TABLE link_person_identities
	ADD CONSTRAINT xif1_link_person_identities FOREIGN KEY (person_hash_key) REFERENCES hub_person (person_hash_key);

ALTER TABLE link_person_identities
	ADD CONSTRAINT xif2_link_person_identities FOREIGN KEY (identities_hash_key) REFERENCES hub_identities (identities_hash_key);

ALTER TABLE link_person_lead
	ADD CONSTRAINT xif1_link_person_lead FOREIGN KEY (person_hash_key) REFERENCES hub_person (person_hash_key);

ALTER TABLE link_person_lead
	ADD CONSTRAINT xif2_link_person_lead FOREIGN KEY (lead_hash_key) REFERENCES hub_lead (lead_hash_key);

ALTER TABLE link_person_legal_person
	ADD CONSTRAINT xif1_link_person_legal_person FOREIGN KEY (person_hash_key) REFERENCES hub_person (person_hash_key);

ALTER TABLE link_person_legal_person
	ADD CONSTRAINT xif2_link_person_legal_person FOREIGN KEY (legal_person_hash_key) REFERENCES hub_legal_person (legal_person_hash_key);

ALTER TABLE link_person_marketing_engagement
	ADD CONSTRAINT xif1_link_person_marketing_engagement FOREIGN KEY (person_hash_key) REFERENCES hub_person (person_hash_key);

ALTER TABLE link_person_marketing_engagement
	ADD CONSTRAINT xif2_link_person_marketing_engagement FOREIGN KEY (marketing_engagement_hash_key) REFERENCES hub_marketing_engagement (marketing_engagement_hash_key);

ALTER TABLE link_person_marketing_preference
	ADD CONSTRAINT xif1_link_person_marketing_preference FOREIGN KEY (person_hash_key) REFERENCES hub_person (person_hash_key);

ALTER TABLE link_person_marketing_preference
	ADD CONSTRAINT xif2_link_person_marketing_preference FOREIGN KEY (marketing_preference_hash_key) REFERENCES hub_marketing_preference (marketing_preference_hash_key);

ALTER TABLE link_person_natural_person
	ADD CONSTRAINT xif1_link_person_natural_person FOREIGN KEY (person_hash_key) REFERENCES hub_person (person_hash_key);

ALTER TABLE link_person_natural_person
	ADD CONSTRAINT xif2_link_person_natural_person FOREIGN KEY (natural_person_hash_key) REFERENCES hub_natural_person (natural_person_hash_key);

ALTER TABLE link_policy_channel
	ADD CONSTRAINT R_69 FOREIGN KEY (channel_hash_key) REFERENCES hub_channel (channel_hash_key);

ALTER TABLE link_policy_channel
	ADD CONSTRAINT R_71 FOREIGN KEY (policy_hash_key) REFERENCES hub_policy (policy_hash_key);

ALTER TABLE link_policy_claim
	ADD CONSTRAINT R_73 FOREIGN KEY (policy_hash_key) REFERENCES hub_policy (policy_hash_key);

ALTER TABLE link_policy_claim
	ADD CONSTRAINT R_74 FOREIGN KEY (claim_hash_key) REFERENCES hub_claim (claim_hash_key);

ALTER TABLE link_policy_customer
	ADD CONSTRAINT xif1_link_policy_customer FOREIGN KEY (policy_hash_key) REFERENCES hub_policy (policy_hash_key);

ALTER TABLE link_policy_customer
	ADD CONSTRAINT xif2_link_policy_customer FOREIGN KEY (customer_hash_key) REFERENCES hub_customer (customer_hash_key);

ALTER TABLE link_policy_product
	ADD CONSTRAINT xif1_link_policy_product FOREIGN KEY (policy_hash_key) REFERENCES hub_policy (policy_hash_key);

ALTER TABLE link_policy_product
	ADD CONSTRAINT xif2_link_policy_product FOREIGN KEY (product_hash_key) REFERENCES hub_product (product_hash_key);

ALTER TABLE link_product_home
	ADD CONSTRAINT xif1_link_product_home FOREIGN KEY (product_hash_key) REFERENCES hub_product (product_hash_key);

ALTER TABLE link_product_home
	ADD CONSTRAINT xif2_link_product_home FOREIGN KEY (home_hash_key) REFERENCES hub_home (home_hash_key);

ALTER TABLE link_product_motor
	ADD CONSTRAINT xif1_link_product_motor FOREIGN KEY (product_hash_key) REFERENCES hub_product (product_hash_key);

ALTER TABLE link_product_motor
	ADD CONSTRAINT xif2_link_product_motor FOREIGN KEY (motor_hash_key) REFERENCES hub_motor (motor_hash_key);

ALTER TABLE link_quote_person
	ADD CONSTRAINT xif1_link_quote_person FOREIGN KEY (quote_hash_key) REFERENCES hub_quote (quote_hash_key);

ALTER TABLE link_quote_person
	ADD CONSTRAINT xif2_link_quote_person FOREIGN KEY (person_hash_key) REFERENCES hub_person (person_hash_key);

ALTER TABLE link_quote_product
	ADD CONSTRAINT xif1_link_quote_product FOREIGN KEY (quote_hash_key) REFERENCES hub_quote (quote_hash_key);

ALTER TABLE link_quote_product
	ADD CONSTRAINT xif2_link_quote_product FOREIGN KEY (product_hash_key) REFERENCES hub_product (product_hash_key);

ALTER TABLE link_regulations_product
	ADD CONSTRAINT R_70 FOREIGN KEY (regulations_hash_key) REFERENCES hub_regulations (regulations_hash_key);

ALTER TABLE link_regulations_product
	ADD CONSTRAINT R_72 FOREIGN KEY (product_hash_key) REFERENCES hub_product (product_hash_key);

ALTER TABLE sat_account
	ADD CONSTRAINT xif1_sat_account FOREIGN KEY (account_hash_key) REFERENCES hub_account (account_hash_key);

ALTER TABLE sat_broker
	ADD CONSTRAINT R_55 FOREIGN KEY (broker_hash_key) REFERENCES hub_broker (broker_hash_key);

ALTER TABLE sat_campaign
	ADD CONSTRAINT R_58 FOREIGN KEY (campaign_hash_key) REFERENCES hub_campaign (campaign_hash_key);

ALTER TABLE sat_channel
	ADD CONSTRAINT R_59 FOREIGN KEY (channel_hash_key) REFERENCES hub_channel (channel_hash_key);

ALTER TABLE sat_claim
	ADD CONSTRAINT R_60 FOREIGN KEY (claim_hash_key) REFERENCES hub_claim (claim_hash_key);

ALTER TABLE sat_complaints
	ADD CONSTRAINT R_62 FOREIGN KEY (complaints_hash_key) REFERENCES hub_complaints (complaints_hash_key);

ALTER TABLE sat_consent
	ADD CONSTRAINT xif1_sat_consent FOREIGN KEY (consent_hash_key) REFERENCES hub_consent (consent_hash_key);

ALTER TABLE sat_contact
	ADD CONSTRAINT xif1_sat_contact FOREIGN KEY (contact_hash_key) REFERENCES hub_contact (contact_hash_key);

ALTER TABLE sat_customer
	ADD CONSTRAINT xif1_sat_customer FOREIGN KEY (customer_hash_key) REFERENCES hub_customer (customer_hash_key);

ALTER TABLE sat_home
	ADD CONSTRAINT xif1_sat_home FOREIGN KEY (home_hash_key) REFERENCES hub_home (home_hash_key);

ALTER TABLE sat_home_address
	ADD CONSTRAINT xif1_sat_home_address FOREIGN KEY (home_address_hash_key) REFERENCES hub_home_address (home_address_hash_key);

ALTER TABLE sat_identities
	ADD CONSTRAINT xif1_sat_identities FOREIGN KEY (identities_hash_key) REFERENCES hub_identities (identities_hash_key);

ALTER TABLE sat_lead
	ADD CONSTRAINT xif1_sat_lead FOREIGN KEY (lead_hash_key) REFERENCES hub_lead (lead_hash_key);

ALTER TABLE sat_legal_person
	ADD CONSTRAINT xif1_sat_legal_person FOREIGN KEY (legal_person_hash_key) REFERENCES hub_legal_person (legal_person_hash_key);

ALTER TABLE sat_marketing_engagement
	ADD CONSTRAINT xif1_sat_marketing_engagement FOREIGN KEY (marketing_engagement_hash_key) REFERENCES hub_marketing_engagement (marketing_engagement_hash_key);

ALTER TABLE sat_marketing_preference
	ADD CONSTRAINT xif1_sat_marketing_preference FOREIGN KEY (marketing_preference_hash_key) REFERENCES hub_marketing_preference (marketing_preference_hash_key);

ALTER TABLE sat_motor
	ADD CONSTRAINT xif1_sat_motor FOREIGN KEY (motor_hash_key) REFERENCES hub_motor (motor_hash_key);

ALTER TABLE sat_natural_person
	ADD CONSTRAINT xif1_sat_natural_person FOREIGN KEY (natural_person_hash_key) REFERENCES hub_natural_person (natural_person_hash_key);

ALTER TABLE sat_override
	ADD CONSTRAINT R_61 FOREIGN KEY (override_hash_key) REFERENCES hub_override (override_hash_key);

ALTER TABLE sat_person
	ADD CONSTRAINT xif8_sat_person FOREIGN KEY (person_hash_key) REFERENCES hub_person (person_hash_key);

ALTER TABLE sat_policy
	ADD CONSTRAINT xif5_sat_policy FOREIGN KEY (policy_hash_key) REFERENCES hub_policy (policy_hash_key);

ALTER TABLE sat_product
	ADD CONSTRAINT xif1_sat_product FOREIGN KEY (product_hash_key) REFERENCES hub_product (product_hash_key);

ALTER TABLE sat_quote
	ADD CONSTRAINT xif3_sat_quote FOREIGN KEY (quote_hash_key) REFERENCES hub_quote (quote_hash_key);

ALTER TABLE sat_regulations
	ADD CONSTRAINT R_57 FOREIGN KEY (regulations_hash_key) REFERENCES hub_regulations (regulations_hash_key);
