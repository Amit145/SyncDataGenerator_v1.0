-- Dimensional DDL for enhanced entity dimensions.
-- Source: dims/Enhanced_Customer360_S2T_Mapping_DV_to_Dimensional_Model.xlsx
-- Scope: seven enhanced entity dimensions only.
-- Tables: dim_broker, dim_campaign, dim_channel, dim_claim,
--         dim_insured_object, dim_override, dim_regulation.

CREATE TABLE dim_broker
(
    broker_sk BIGINT NOT NULL,
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
);

ALTER TABLE dim_broker
    ADD CONSTRAINT xpk_dim_broker PRIMARY KEY (broker_sk);

CREATE TABLE dim_campaign
(
    campaign_sk BIGINT NOT NULL,
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
);

ALTER TABLE dim_campaign
    ADD CONSTRAINT xpk_dim_campaign PRIMARY KEY (campaign_sk);

CREATE TABLE dim_channel
(
    channel_sk BIGINT NOT NULL,
    channel_id STRING,
    channel_name STRING,
    channel_type STRING,
    created_by STRING,
    created_ts TIMESTAMP,
    last_updated_by STRING,
    last_updated_ts TIMESTAMP,
    attr_hash STRING
);

ALTER TABLE dim_channel
    ADD CONSTRAINT xpk_dim_channel PRIMARY KEY (channel_sk);

CREATE TABLE dim_claim
(
    claim_sk BIGINT NOT NULL,
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
    effective_from_ts TIMESTAMP,
    effective_to_ts TIMESTAMP,
    record_version INTEGER,
    created_by STRING,
    created_ts TIMESTAMP,
    last_updated_by STRING,
    last_updated_ts TIMESTAMP,
    attr_hash STRING
);

ALTER TABLE dim_claim
    ADD CONSTRAINT xpk_dim_claim PRIMARY KEY (claim_sk);

CREATE TABLE dim_insured_object
(
    insured_object_sk CHAR(18) NOT NULL,
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
    ADD CONSTRAINT xpk_dim_insured_object PRIMARY KEY (insured_object_sk);

CREATE TABLE dim_override
(
    override_sk BIGINT NOT NULL,
    override_id STRING,
    override_reason STRING,
    created_by STRING,
    created_ts TIMESTAMP,
    last_updated_by STRING,
    last_updated_ts TIMESTAMP,
    attr_hash STRING
);

ALTER TABLE dim_override
    ADD CONSTRAINT xpk_dim_override PRIMARY KEY (override_sk);

CREATE TABLE dim_regulation
(
    regulation_sk BIGINT NOT NULL,
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
);

ALTER TABLE dim_regulation
    ADD CONSTRAINT xpk_dim_regulation PRIMARY KEY (regulation_sk);

