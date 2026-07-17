-- Enhanced raw vault source-split load SQL.
-- Source assumptions:
--   PRD1/CRM raw tables are named crm_<raw_file_stem>_csv, e.g. crm_party_master_csv.
--   PRD2/SAP raw tables are named sap_<raw_file_stem>_db, e.g. sap_person_db.
-- Design:
--   25 union hubs, 30 union links, 30 source-split satellites.
--   SAP has five source entities: person, address, product, home, motor.
--   SAP natural/legal attributes are kept in sat_person_sap for the 30-satellite design.
--   If you want sat_natural_person_sap and sat_legal_person_sap as separate raw-vault sats,
--   expand this model to 32 satellites / 87 total tables.

-- ============================================================
-- HUBS
-- ============================================================

CREATE TABLE IF NOT EXISTS hub_person (
    person_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    person_id TEXT
);

TRUNCATE TABLE hub_person;
INSERT INTO hub_person (person_hash_key, load_date, record_source, person_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))) AS person_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(party_ref AS CHAR)), '') AS person_id
FROM crm_party_master_csv
WHERE NULLIF(TRIM(CAST(party_ref AS CHAR)), '') IS NOT NULL
UNION ALL
SELECT DISTINCT
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(person_id AS CHAR)), ''))) AS person_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    NULLIF(TRIM(CAST(person_id AS CHAR)), '') AS person_id
FROM sap_person_db
WHERE NULLIF(TRIM(CAST(person_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_natural_person (
    natural_person_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    natural_person_id TEXT
);

TRUNCATE TABLE hub_natural_person;
INSERT INTO hub_natural_person (natural_person_hash_key, load_date, record_source, natural_person_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(natural_ref AS CHAR)), ''))) AS natural_person_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(natural_ref AS CHAR)), '') AS natural_person_id
FROM crm_party_master_csv
WHERE NULLIF(TRIM(CAST(natural_ref AS CHAR)), '') IS NOT NULL
UNION ALL
SELECT DISTINCT
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(person_id AS CHAR)), ''))) AS natural_person_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    NULLIF(TRIM(CAST(person_id AS CHAR)), '') AS natural_person_id
FROM sap_person_db
WHERE NULLIF(TRIM(CAST(person_id AS CHAR)), '') IS NOT NULL AND UPPER(person_type) = 'NATURAL';

CREATE TABLE IF NOT EXISTS hub_legal_person (
    legal_person_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    legal_person_id TEXT
);

TRUNCATE TABLE hub_legal_person;
INSERT INTO hub_legal_person (legal_person_hash_key, load_date, record_source, legal_person_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(legal_ref AS CHAR)), ''))) AS legal_person_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(legal_ref AS CHAR)), '') AS legal_person_id
FROM crm_party_master_csv
WHERE NULLIF(TRIM(CAST(legal_ref AS CHAR)), '') IS NOT NULL
UNION ALL
SELECT DISTINCT
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(person_id AS CHAR)), ''))) AS legal_person_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    NULLIF(TRIM(CAST(person_id AS CHAR)), '') AS legal_person_id
FROM sap_person_db
WHERE NULLIF(TRIM(CAST(person_id AS CHAR)), '') IS NOT NULL AND UPPER(person_type) = 'LEGAL';

CREATE TABLE IF NOT EXISTS hub_contact (
    contact_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    contact_id TEXT
);

TRUNCATE TABLE hub_contact;
INSERT INTO hub_contact (contact_hash_key, load_date, record_source, contact_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(contact_ref AS CHAR)), ''))) AS contact_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(contact_ref AS CHAR)), '') AS contact_id
FROM crm_contact_point_csv
WHERE NULLIF(TRIM(CAST(contact_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_identities (
    identities_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    identities_id TEXT
);

TRUNCATE TABLE hub_identities;
INSERT INTO hub_identities (identities_hash_key, load_date, record_source, identities_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(identity_ref AS CHAR)), ''))) AS identities_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(identity_ref AS CHAR)), '') AS identities_id
FROM crm_identity_registry_csv
WHERE NULLIF(TRIM(CAST(identity_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_address (
    address_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    address_id TEXT
);

TRUNCATE TABLE hub_address;
INSERT INTO hub_address (address_hash_key, load_date, record_source, address_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(address_ref AS CHAR)), ''))) AS address_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(address_ref AS CHAR)), '') AS address_id
FROM crm_address_book_csv
WHERE NULLIF(TRIM(CAST(address_ref AS CHAR)), '') IS NOT NULL
UNION ALL
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_address_id AS CHAR)), ''))) AS address_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(src_address_id AS CHAR)), '') AS address_id
FROM crm_enhanced_address_book_csv
WHERE NULLIF(TRIM(CAST(src_address_id AS CHAR)), '') IS NOT NULL
UNION ALL
SELECT DISTINCT
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(address_id AS CHAR)), ''))) AS address_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    NULLIF(TRIM(CAST(address_id AS CHAR)), '') AS address_id
FROM sap_address_db
WHERE NULLIF(TRIM(CAST(address_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_consent (
    consent_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    consent_id TEXT
);

TRUNCATE TABLE hub_consent;
INSERT INTO hub_consent (consent_hash_key, load_date, record_source, consent_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(consent_ref AS CHAR)), ''))) AS consent_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(consent_ref AS CHAR)), '') AS consent_id
FROM crm_consent_snapshot_csv
WHERE NULLIF(TRIM(CAST(consent_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_marketing_preference (
    marketing_preference_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    marketing_preference_id TEXT
);

TRUNCATE TABLE hub_marketing_preference;
INSERT INTO hub_marketing_preference (marketing_preference_hash_key, load_date, record_source, marketing_preference_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(preference_ref AS CHAR)), ''))) AS marketing_preference_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(preference_ref AS CHAR)), '') AS marketing_preference_id
FROM crm_comm_preference_csv
WHERE NULLIF(TRIM(CAST(preference_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_marketing_engagement (
    marketing_engagement_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    marketing_engagement_id TEXT
);

TRUNCATE TABLE hub_marketing_engagement;
INSERT INTO hub_marketing_engagement (marketing_engagement_hash_key, load_date, record_source, marketing_engagement_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(engagement_ref AS CHAR)), ''))) AS marketing_engagement_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(engagement_ref AS CHAR)), '') AS marketing_engagement_id
FROM crm_campaign_touch_csv
WHERE NULLIF(TRIM(CAST(engagement_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_lead (
    lead_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    lead_id TEXT
);

TRUNCATE TABLE hub_lead;
INSERT INTO hub_lead (lead_hash_key, load_date, record_source, lead_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(lead_ref AS CHAR)), ''))) AS lead_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(lead_ref AS CHAR)), '') AS lead_id
FROM crm_lead_register_csv
WHERE NULLIF(TRIM(CAST(lead_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_quote (
    quote_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    quote_id TEXT
);

TRUNCATE TABLE hub_quote;
INSERT INTO hub_quote (quote_hash_key, load_date, record_source, quote_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(quote_ref AS CHAR)), ''))) AS quote_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(quote_ref AS CHAR)), '') AS quote_id
FROM crm_quote_register_csv
WHERE NULLIF(TRIM(CAST(quote_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_policy (
    policy_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    policy_id TEXT
);

TRUNCATE TABLE hub_policy;
INSERT INTO hub_policy (policy_hash_key, load_date, record_source, policy_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(policy_ref AS CHAR)), ''))) AS policy_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(policy_ref AS CHAR)), '') AS policy_id
FROM crm_policy_register_csv
WHERE NULLIF(TRIM(CAST(policy_ref AS CHAR)), '') IS NOT NULL
UNION ALL
SELECT DISTINCT
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(policy_id AS CHAR)), ''))) AS policy_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    NULLIF(TRIM(CAST(policy_id AS CHAR)), '') AS policy_id
FROM sap_home_db
WHERE NULLIF(TRIM(CAST(policy_id AS CHAR)), '') IS NOT NULL
UNION ALL
SELECT DISTINCT
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(policy_id AS CHAR)), ''))) AS policy_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    NULLIF(TRIM(CAST(policy_id AS CHAR)), '') AS policy_id
FROM sap_motor_db
WHERE NULLIF(TRIM(CAST(policy_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_account (
    account_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    account_id TEXT
);

TRUNCATE TABLE hub_account;
INSERT INTO hub_account (account_hash_key, load_date, record_source, account_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(account_ref AS CHAR)), ''))) AS account_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(account_ref AS CHAR)), '') AS account_id
FROM crm_account_book_csv
WHERE NULLIF(TRIM(CAST(account_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_customer (
    customer_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    customer_id TEXT
);

TRUNCATE TABLE hub_customer;
INSERT INTO hub_customer (customer_hash_key, load_date, record_source, customer_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(customer_ref AS CHAR)), ''))) AS customer_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(customer_ref AS CHAR)), '') AS customer_id
FROM crm_customer_portfolio_csv
WHERE NULLIF(TRIM(CAST(customer_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_product (
    product_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    product_id TEXT
);

TRUNCATE TABLE hub_product;
INSERT INTO hub_product (product_hash_key, load_date, record_source, product_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(product_ref AS CHAR)), ''))) AS product_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(product_ref AS CHAR)), '') AS product_id
FROM crm_product_catalog_csv
WHERE NULLIF(TRIM(CAST(product_ref AS CHAR)), '') IS NOT NULL
UNION ALL
SELECT DISTINCT
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(product_id AS CHAR)), ''))) AS product_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    NULLIF(TRIM(CAST(product_id AS CHAR)), '') AS product_id
FROM sap_product_db
WHERE NULLIF(TRIM(CAST(product_id AS CHAR)), '') IS NOT NULL
UNION ALL
SELECT DISTINCT
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(product_id AS CHAR)), ''))) AS product_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    NULLIF(TRIM(CAST(product_id AS CHAR)), '') AS product_id
FROM sap_home_db
WHERE NULLIF(TRIM(CAST(product_id AS CHAR)), '') IS NOT NULL
UNION ALL
SELECT DISTINCT
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(product_id AS CHAR)), ''))) AS product_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    NULLIF(TRIM(CAST(product_id AS CHAR)), '') AS product_id
FROM sap_motor_db
WHERE NULLIF(TRIM(CAST(product_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_motor (
    motor_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    motor_id TEXT
);

TRUNCATE TABLE hub_motor;
INSERT INTO hub_motor (motor_hash_key, load_date, record_source, motor_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(vehicle_ref AS CHAR)), ''))) AS motor_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(vehicle_ref AS CHAR)), '') AS motor_id
FROM crm_vehicle_asset_csv
WHERE NULLIF(TRIM(CAST(vehicle_ref AS CHAR)), '') IS NOT NULL
UNION ALL
SELECT DISTINCT
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(motor_id AS CHAR)), ''))) AS motor_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    NULLIF(TRIM(CAST(motor_id AS CHAR)), '') AS motor_id
FROM sap_motor_db
WHERE NULLIF(TRIM(CAST(motor_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_home (
    home_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    home_id TEXT
);

TRUNCATE TABLE hub_home;
INSERT INTO hub_home (home_hash_key, load_date, record_source, home_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(property_ref AS CHAR)), ''))) AS home_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(property_ref AS CHAR)), '') AS home_id
FROM crm_property_asset_csv
WHERE NULLIF(TRIM(CAST(property_ref AS CHAR)), '') IS NOT NULL
UNION ALL
SELECT DISTINCT
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(home_id AS CHAR)), ''))) AS home_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    NULLIF(TRIM(CAST(home_id AS CHAR)), '') AS home_id
FROM sap_home_db
WHERE NULLIF(TRIM(CAST(home_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_broker (
    broker_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    agent_id TEXT
);

TRUNCATE TABLE hub_broker;
INSERT INTO hub_broker (broker_hash_key, load_date, record_source, agent_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_agent_id AS CHAR)), ''))) AS broker_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(src_agent_id AS CHAR)), '') AS agent_id
FROM crm_broker_book_csv
WHERE NULLIF(TRIM(CAST(src_agent_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_campaign (
    campaign_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    campaign_id TEXT
);

TRUNCATE TABLE hub_campaign;
INSERT INTO hub_campaign (campaign_hash_key, load_date, record_source, campaign_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_campaign_id AS CHAR)), ''))) AS campaign_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(src_campaign_id AS CHAR)), '') AS campaign_id
FROM crm_campaign_register_csv
WHERE NULLIF(TRIM(CAST(src_campaign_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_channel (
    channel_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    channel_id TEXT
);

TRUNCATE TABLE hub_channel;
INSERT INTO hub_channel (channel_hash_key, load_date, record_source, channel_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_channel_id AS CHAR)), ''))) AS channel_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(src_channel_id AS CHAR)), '') AS channel_id
FROM crm_channel_catalog_csv
WHERE NULLIF(TRIM(CAST(src_channel_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_claim (
    claim_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    claim_id TEXT
);

TRUNCATE TABLE hub_claim;
INSERT INTO hub_claim (claim_hash_key, load_date, record_source, claim_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_claim_id AS CHAR)), ''))) AS claim_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(src_claim_id AS CHAR)), '') AS claim_id
FROM crm_claim_register_csv
WHERE NULLIF(TRIM(CAST(src_claim_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_complaint (
    complaint_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    complaint_id TEXT
);

TRUNCATE TABLE hub_complaint;
INSERT INTO hub_complaint (complaint_hash_key, load_date, record_source, complaint_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_complaint_id AS CHAR)), ''))) AS complaint_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(src_complaint_id AS CHAR)), '') AS complaint_id
FROM crm_complaint_register_csv
WHERE NULLIF(TRIM(CAST(src_complaint_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_insured_object (
    insured_object_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    insured_object_id TEXT
);

TRUNCATE TABLE hub_insured_object;
INSERT INTO hub_insured_object (insured_object_hash_key, load_date, record_source, insured_object_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(insured_object_id AS CHAR)), ''))) AS insured_object_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(insured_object_id AS CHAR)), '') AS insured_object_id
FROM crm_property_asset_csv
WHERE NULLIF(TRIM(CAST(insured_object_id AS CHAR)), '') IS NOT NULL
UNION ALL
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(insured_object_id AS CHAR)), ''))) AS insured_object_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(insured_object_id AS CHAR)), '') AS insured_object_id
FROM crm_vehicle_asset_csv
WHERE NULLIF(TRIM(CAST(insured_object_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_override (
    override_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    override_id TEXT
);

TRUNCATE TABLE hub_override;
INSERT INTO hub_override (override_hash_key, load_date, record_source, override_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_override_id AS CHAR)), ''))) AS override_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(src_override_id AS CHAR)), '') AS override_id
FROM crm_override_register_csv
WHERE NULLIF(TRIM(CAST(src_override_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS hub_regulation (
    regulation_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    regulation_id TEXT
);

TRUNCATE TABLE hub_regulation;
INSERT INTO hub_regulation (regulation_hash_key, load_date, record_source, regulation_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_regulation_id AS CHAR)), ''))) AS regulation_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    NULLIF(TRIM(CAST(src_regulation_id AS CHAR)), '') AS regulation_id
FROM crm_regulation_register_csv
WHERE NULLIF(TRIM(CAST(src_regulation_id AS CHAR)), '') IS NOT NULL;

-- ============================================================
-- LINKS
-- ============================================================

CREATE TABLE IF NOT EXISTS link_person_natural_person (
    person_natural_person_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    person_hash_key TEXT,
    natural_person_hash_key TEXT
);

TRUNCATE TABLE link_person_natural_person;
INSERT INTO link_person_natural_person (person_natural_person_hash_key, load_date, record_source, person_hash_key, natural_person_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(natural_ref AS CHAR)), ''))))) AS person_natural_person_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))) AS person_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(natural_ref AS CHAR)), ''))) AS natural_person_hash_key
FROM crm_party_master_csv
WHERE NULLIF(TRIM(CAST(party_ref AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(natural_ref AS CHAR)), '') IS NOT NULL AND 1=1
UNION ALL
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(person_id AS CHAR)), ''))), MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(person_id AS CHAR)), ''))))) AS person_natural_person_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(person_id AS CHAR)), ''))) AS person_hash_key,
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(person_id AS CHAR)), ''))) AS natural_person_hash_key
FROM sap_person_db
WHERE NULLIF(TRIM(CAST(person_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(person_id AS CHAR)), '') IS NOT NULL AND UPPER(person_type) = 'NATURAL';

CREATE TABLE IF NOT EXISTS link_person_legal_person (
    person_legal_person_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    person_hash_key TEXT,
    legal_person_hash_key TEXT
);

TRUNCATE TABLE link_person_legal_person;
INSERT INTO link_person_legal_person (person_legal_person_hash_key, load_date, record_source, person_hash_key, legal_person_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(legal_ref AS CHAR)), ''))))) AS person_legal_person_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))) AS person_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(legal_ref AS CHAR)), ''))) AS legal_person_hash_key
FROM crm_party_master_csv
WHERE NULLIF(TRIM(CAST(party_ref AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(legal_ref AS CHAR)), '') IS NOT NULL AND 1=1
UNION ALL
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(person_id AS CHAR)), ''))), MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(person_id AS CHAR)), ''))))) AS person_legal_person_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(person_id AS CHAR)), ''))) AS person_hash_key,
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(person_id AS CHAR)), ''))) AS legal_person_hash_key
FROM sap_person_db
WHERE NULLIF(TRIM(CAST(person_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(person_id AS CHAR)), '') IS NOT NULL AND UPPER(person_type) = 'LEGAL';

CREATE TABLE IF NOT EXISTS link_person_contact (
    person_contact_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    person_hash_key TEXT,
    contact_hash_key TEXT
);

TRUNCATE TABLE link_person_contact;
INSERT INTO link_person_contact (person_contact_hash_key, load_date, record_source, person_hash_key, contact_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(contact_ref AS CHAR)), ''))))) AS person_contact_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))) AS person_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(contact_ref AS CHAR)), ''))) AS contact_hash_key
FROM crm_contact_point_csv
WHERE NULLIF(TRIM(CAST(party_ref AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(contact_ref AS CHAR)), '') IS NOT NULL AND 1=1;

CREATE TABLE IF NOT EXISTS link_person_identities (
    person_identities_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    person_hash_key TEXT,
    identities_hash_key TEXT
);

TRUNCATE TABLE link_person_identities;
INSERT INTO link_person_identities (person_identities_hash_key, load_date, record_source, person_hash_key, identities_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(identity_ref AS CHAR)), ''))))) AS person_identities_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))) AS person_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(identity_ref AS CHAR)), ''))) AS identities_hash_key
FROM crm_identity_registry_csv
WHERE NULLIF(TRIM(CAST(party_ref AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(identity_ref AS CHAR)), '') IS NOT NULL AND 1=1;

CREATE TABLE IF NOT EXISTS link_person_address (
    person_address_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    person_hash_key TEXT,
    address_hash_key TEXT
);

TRUNCATE TABLE link_person_address;
INSERT INTO link_person_address (person_address_hash_key, load_date, record_source, person_hash_key, address_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(address_ref AS CHAR)), ''))))) AS person_address_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))) AS person_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(address_ref AS CHAR)), ''))) AS address_hash_key
FROM crm_address_book_csv
WHERE NULLIF(TRIM(CAST(party_ref AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(address_ref AS CHAR)), '') IS NOT NULL AND 1=1
UNION ALL
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_person_id AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_address_id AS CHAR)), ''))))) AS person_address_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_person_id AS CHAR)), ''))) AS person_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_address_id AS CHAR)), ''))) AS address_hash_key
FROM crm_enhanced_person_relationships_csv
WHERE NULLIF(TRIM(CAST(src_person_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(src_address_id AS CHAR)), '') IS NOT NULL AND source_extract = 'person_address_bridge.csv'
UNION ALL
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(person_id AS CHAR)), ''))), MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(address_id AS CHAR)), ''))))) AS person_address_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(person_id AS CHAR)), ''))) AS person_hash_key,
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(address_id AS CHAR)), ''))) AS address_hash_key
FROM sap_address_db
WHERE NULLIF(TRIM(CAST(person_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(address_id AS CHAR)), '') IS NOT NULL AND 1=1;

CREATE TABLE IF NOT EXISTS link_person_consent (
    person_consent_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    person_hash_key TEXT,
    consent_hash_key TEXT
);

TRUNCATE TABLE link_person_consent;
INSERT INTO link_person_consent (person_consent_hash_key, load_date, record_source, person_hash_key, consent_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(consent_ref AS CHAR)), ''))))) AS person_consent_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))) AS person_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(consent_ref AS CHAR)), ''))) AS consent_hash_key
FROM crm_consent_snapshot_csv
WHERE NULLIF(TRIM(CAST(party_ref AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(consent_ref AS CHAR)), '') IS NOT NULL AND 1=1;

CREATE TABLE IF NOT EXISTS link_person_marketing_preference (
    person_marketing_preference_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    person_hash_key TEXT,
    marketing_preference_hash_key TEXT
);

TRUNCATE TABLE link_person_marketing_preference;
INSERT INTO link_person_marketing_preference (person_marketing_preference_hash_key, load_date, record_source, person_hash_key, marketing_preference_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(preference_ref AS CHAR)), ''))))) AS person_marketing_preference_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))) AS person_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(preference_ref AS CHAR)), ''))) AS marketing_preference_hash_key
FROM crm_comm_preference_csv
WHERE NULLIF(TRIM(CAST(party_ref AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(preference_ref AS CHAR)), '') IS NOT NULL AND 1=1;

CREATE TABLE IF NOT EXISTS link_person_marketing_engagement (
    person_marketing_engagement_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    person_hash_key TEXT,
    marketing_engagement_hash_key TEXT
);

TRUNCATE TABLE link_person_marketing_engagement;
INSERT INTO link_person_marketing_engagement (person_marketing_engagement_hash_key, load_date, record_source, person_hash_key, marketing_engagement_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(engagement_ref AS CHAR)), ''))))) AS person_marketing_engagement_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))) AS person_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(engagement_ref AS CHAR)), ''))) AS marketing_engagement_hash_key
FROM crm_campaign_touch_csv
WHERE NULLIF(TRIM(CAST(party_ref AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(engagement_ref AS CHAR)), '') IS NOT NULL AND 1=1;

CREATE TABLE IF NOT EXISTS link_person_lead (
    person_lead_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    person_hash_key TEXT,
    lead_hash_key TEXT
);

TRUNCATE TABLE link_person_lead;
INSERT INTO link_person_lead (person_lead_hash_key, load_date, record_source, person_hash_key, lead_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(lead_ref AS CHAR)), ''))))) AS person_lead_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))) AS person_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(lead_ref AS CHAR)), ''))) AS lead_hash_key
FROM crm_lead_register_csv
WHERE NULLIF(TRIM(CAST(party_ref AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(lead_ref AS CHAR)), '') IS NOT NULL AND 1=1;

CREATE TABLE IF NOT EXISTS link_person_account (
    person_account_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    person_hash_key TEXT,
    account_hash_key TEXT
);

TRUNCATE TABLE link_person_account;
INSERT INTO link_person_account (person_account_hash_key, load_date, record_source, person_hash_key, account_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(account_ref AS CHAR)), ''))))) AS person_account_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))) AS person_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(account_ref AS CHAR)), ''))) AS account_hash_key
FROM crm_account_book_csv
WHERE NULLIF(TRIM(CAST(party_ref AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(account_ref AS CHAR)), '') IS NOT NULL AND 1=1;

CREATE TABLE IF NOT EXISTS link_customer_person (
    customer_person_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    customer_hash_key TEXT,
    person_hash_key TEXT
);

TRUNCATE TABLE link_customer_person;
INSERT INTO link_customer_person (customer_person_hash_key, load_date, record_source, customer_hash_key, person_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(customer_ref AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))))) AS customer_person_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(customer_ref AS CHAR)), ''))) AS customer_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))) AS person_hash_key
FROM crm_customer_portfolio_csv
WHERE NULLIF(TRIM(CAST(customer_ref AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(party_ref AS CHAR)), '') IS NOT NULL AND 1=1;

CREATE TABLE IF NOT EXISTS link_customer_lead (
    customer_lead_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    customer_hash_key TEXT,
    lead_hash_key TEXT
);

TRUNCATE TABLE link_customer_lead;
INSERT INTO link_customer_lead (customer_lead_hash_key, load_date, record_source, customer_hash_key, lead_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(customer_ref AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(lead_ref AS CHAR)), ''))))) AS customer_lead_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(customer_ref AS CHAR)), ''))) AS customer_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(lead_ref AS CHAR)), ''))) AS lead_hash_key
FROM crm_customer_lead_bridge_csv
WHERE NULLIF(TRIM(CAST(customer_ref AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(lead_ref AS CHAR)), '') IS NOT NULL AND 1=1;

CREATE TABLE IF NOT EXISTS link_quote_person (
    quote_person_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    quote_hash_key TEXT,
    person_hash_key TEXT
);

TRUNCATE TABLE link_quote_person;
INSERT INTO link_quote_person (quote_person_hash_key, load_date, record_source, quote_hash_key, person_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(quote_ref AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))))) AS quote_person_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(quote_ref AS CHAR)), ''))) AS quote_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))) AS person_hash_key
FROM crm_quote_register_csv
WHERE NULLIF(TRIM(CAST(quote_ref AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(party_ref AS CHAR)), '') IS NOT NULL AND 1=1;

CREATE TABLE IF NOT EXISTS link_quote_product (
    quote_product_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    quote_hash_key TEXT,
    product_hash_key TEXT
);

TRUNCATE TABLE link_quote_product;
INSERT INTO link_quote_product (quote_product_hash_key, load_date, record_source, quote_hash_key, product_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(quote_ref AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(product_ref AS CHAR)), ''))))) AS quote_product_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(quote_ref AS CHAR)), ''))) AS quote_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(product_ref AS CHAR)), ''))) AS product_hash_key
FROM crm_quote_register_csv
WHERE NULLIF(TRIM(CAST(quote_ref AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(product_ref AS CHAR)), '') IS NOT NULL AND 1=1;

CREATE TABLE IF NOT EXISTS link_policy_customer (
    policy_customer_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    policy_hash_key TEXT,
    customer_hash_key TEXT
);

TRUNCATE TABLE link_policy_customer;
INSERT INTO link_policy_customer (policy_customer_hash_key, load_date, record_source, policy_hash_key, customer_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(policy_ref AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(customer_ref AS CHAR)), ''))))) AS policy_customer_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(policy_ref AS CHAR)), ''))) AS policy_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(customer_ref AS CHAR)), ''))) AS customer_hash_key
FROM crm_policy_register_csv
WHERE NULLIF(TRIM(CAST(policy_ref AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(customer_ref AS CHAR)), '') IS NOT NULL AND 1=1;

CREATE TABLE IF NOT EXISTS link_policy_product (
    policy_product_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    policy_hash_key TEXT,
    product_hash_key TEXT
);

TRUNCATE TABLE link_policy_product;
INSERT INTO link_policy_product (policy_product_hash_key, load_date, record_source, policy_hash_key, product_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(policy_ref AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(product_ref AS CHAR)), ''))))) AS policy_product_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(policy_ref AS CHAR)), ''))) AS policy_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(product_ref AS CHAR)), ''))) AS product_hash_key
FROM crm_policy_register_csv
WHERE NULLIF(TRIM(CAST(policy_ref AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(product_ref AS CHAR)), '') IS NOT NULL AND 1=1
UNION ALL
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(policy_id AS CHAR)), ''))), MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(product_id AS CHAR)), ''))))) AS policy_product_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(policy_id AS CHAR)), ''))) AS policy_hash_key,
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(product_id AS CHAR)), ''))) AS product_hash_key
FROM sap_home_db
WHERE NULLIF(TRIM(CAST(policy_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(product_id AS CHAR)), '') IS NOT NULL AND 1=1
UNION ALL
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(policy_id AS CHAR)), ''))), MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(product_id AS CHAR)), ''))))) AS policy_product_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(policy_id AS CHAR)), ''))) AS policy_hash_key,
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(product_id AS CHAR)), ''))) AS product_hash_key
FROM sap_motor_db
WHERE NULLIF(TRIM(CAST(policy_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(product_id AS CHAR)), '') IS NOT NULL AND 1=1;

CREATE TABLE IF NOT EXISTS link_policy_quote (
    policy_quote_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    policy_hash_key TEXT,
    quote_hash_key TEXT
);

TRUNCATE TABLE link_policy_quote;
INSERT INTO link_policy_quote (policy_quote_hash_key, load_date, record_source, policy_hash_key, quote_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(policy_ref AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(quote_ref AS CHAR)), ''))))) AS policy_quote_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(policy_ref AS CHAR)), ''))) AS policy_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(quote_ref AS CHAR)), ''))) AS quote_hash_key
FROM crm_policy_register_csv
WHERE NULLIF(TRIM(CAST(policy_ref AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(quote_ref AS CHAR)), '') IS NOT NULL AND 1=1
UNION ALL
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_policy_id AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_quote_id AS CHAR)), ''))))) AS policy_quote_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_policy_id AS CHAR)), ''))) AS policy_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_quote_id AS CHAR)), ''))) AS quote_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE NULLIF(TRIM(CAST(src_policy_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(src_quote_id AS CHAR)), '') IS NOT NULL AND source_extract = 'policy_quote_bridge.csv';

CREATE TABLE IF NOT EXISTS link_broker_person (
    broker_person_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    broker_hash_key TEXT,
    person_hash_key TEXT
);

TRUNCATE TABLE link_broker_person;
INSERT INTO link_broker_person (broker_person_hash_key, load_date, record_source, broker_hash_key, person_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_agent_id AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_person_id AS CHAR)), ''))))) AS broker_person_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_agent_id AS CHAR)), ''))) AS broker_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_person_id AS CHAR)), ''))) AS person_hash_key
FROM crm_enhanced_person_relationships_csv
WHERE NULLIF(TRIM(CAST(src_agent_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(src_person_id AS CHAR)), '') IS NOT NULL AND source_extract = 'broker_person_bridge.csv';

CREATE TABLE IF NOT EXISTS link_person_campaign (
    person_campaign_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    person_hash_key TEXT,
    campaign_hash_key TEXT
);

TRUNCATE TABLE link_person_campaign;
INSERT INTO link_person_campaign (person_campaign_hash_key, load_date, record_source, person_hash_key, campaign_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_person_id AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_campaign_id AS CHAR)), ''))))) AS person_campaign_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_person_id AS CHAR)), ''))) AS person_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_campaign_id AS CHAR)), ''))) AS campaign_hash_key
FROM crm_enhanced_person_relationships_csv
WHERE NULLIF(TRIM(CAST(src_person_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(src_campaign_id AS CHAR)), '') IS NOT NULL AND source_extract = 'person_campaign_bridge.csv';

CREATE TABLE IF NOT EXISTS link_claim_policy (
    claim_policy_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    claim_hash_key TEXT,
    policy_hash_key TEXT
);

TRUNCATE TABLE link_claim_policy;
INSERT INTO link_claim_policy (claim_policy_hash_key, load_date, record_source, claim_hash_key, policy_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_claim_id AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_policy_id AS CHAR)), ''))))) AS claim_policy_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_claim_id AS CHAR)), ''))) AS claim_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_policy_id AS CHAR)), ''))) AS policy_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE NULLIF(TRIM(CAST(src_claim_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(src_policy_id AS CHAR)), '') IS NOT NULL AND source_extract = 'claim_policy_bridge.csv';

CREATE TABLE IF NOT EXISTS link_complaint_policy (
    complaint_policy_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    complaint_hash_key TEXT,
    policy_hash_key TEXT
);

TRUNCATE TABLE link_complaint_policy;
INSERT INTO link_complaint_policy (complaint_policy_hash_key, load_date, record_source, complaint_hash_key, policy_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_complaint_id AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_policy_id AS CHAR)), ''))))) AS complaint_policy_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_complaint_id AS CHAR)), ''))) AS complaint_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_policy_id AS CHAR)), ''))) AS policy_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE NULLIF(TRIM(CAST(src_complaint_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(src_policy_id AS CHAR)), '') IS NOT NULL AND source_extract = 'complaint_policy_bridge.csv';

CREATE TABLE IF NOT EXISTS link_complaint_regulation (
    complaint_regulation_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    complaint_hash_key TEXT,
    regulation_hash_key TEXT
);

TRUNCATE TABLE link_complaint_regulation;
INSERT INTO link_complaint_regulation (complaint_regulation_hash_key, load_date, record_source, complaint_hash_key, regulation_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_complaint_id AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_regulation_id AS CHAR)), ''))))) AS complaint_regulation_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_complaint_id AS CHAR)), ''))) AS complaint_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_regulation_id AS CHAR)), ''))) AS regulation_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE NULLIF(TRIM(CAST(src_complaint_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(src_regulation_id AS CHAR)), '') IS NOT NULL AND source_extract = 'complaint_regulation_bridge.csv';

CREATE TABLE IF NOT EXISTS link_insured_object_home (
    insured_object_home_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    insured_object_hash_key TEXT,
    home_hash_key TEXT
);

TRUNCATE TABLE link_insured_object_home;
INSERT INTO link_insured_object_home (insured_object_home_hash_key, load_date, record_source, insured_object_hash_key, home_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_insured_object_id AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_home_id AS CHAR)), ''))))) AS insured_object_home_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_insured_object_id AS CHAR)), ''))) AS insured_object_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_home_id AS CHAR)), ''))) AS home_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE NULLIF(TRIM(CAST(src_insured_object_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(src_home_id AS CHAR)), '') IS NOT NULL AND source_extract = 'insured_object_home_bridge.csv';

CREATE TABLE IF NOT EXISTS link_insured_object_motor (
    insured_object_motor_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    insured_object_hash_key TEXT,
    motor_hash_key TEXT
);

TRUNCATE TABLE link_insured_object_motor;
INSERT INTO link_insured_object_motor (insured_object_motor_hash_key, load_date, record_source, insured_object_hash_key, motor_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_insured_object_id AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_motor_id AS CHAR)), ''))))) AS insured_object_motor_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_insured_object_id AS CHAR)), ''))) AS insured_object_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_motor_id AS CHAR)), ''))) AS motor_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE NULLIF(TRIM(CAST(src_insured_object_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(src_motor_id AS CHAR)), '') IS NOT NULL AND source_extract = 'insured_object_motor_bridge.csv';

CREATE TABLE IF NOT EXISTS link_policy_broker (
    policy_broker_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    policy_hash_key TEXT,
    broker_hash_key TEXT
);

TRUNCATE TABLE link_policy_broker;
INSERT INTO link_policy_broker (policy_broker_hash_key, load_date, record_source, policy_hash_key, broker_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_policy_id AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_agent_id AS CHAR)), ''))))) AS policy_broker_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_policy_id AS CHAR)), ''))) AS policy_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_agent_id AS CHAR)), ''))) AS broker_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE NULLIF(TRIM(CAST(src_policy_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(src_agent_id AS CHAR)), '') IS NOT NULL AND source_extract = 'policy_broker_bridge.csv';

CREATE TABLE IF NOT EXISTS link_policy_channel (
    policy_channel_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    policy_hash_key TEXT,
    channel_hash_key TEXT
);

TRUNCATE TABLE link_policy_channel;
INSERT INTO link_policy_channel (policy_channel_hash_key, load_date, record_source, policy_hash_key, channel_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_policy_id AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_channel_id AS CHAR)), ''))))) AS policy_channel_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_policy_id AS CHAR)), ''))) AS policy_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_channel_id AS CHAR)), ''))) AS channel_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE NULLIF(TRIM(CAST(src_policy_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(src_channel_id AS CHAR)), '') IS NOT NULL AND source_extract = 'policy_channel_bridge.csv';

CREATE TABLE IF NOT EXISTS link_policy_insured_object (
    policy_insured_object_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    policy_hash_key TEXT,
    insured_object_hash_key TEXT
);

TRUNCATE TABLE link_policy_insured_object;
INSERT INTO link_policy_insured_object (policy_insured_object_hash_key, load_date, record_source, policy_hash_key, insured_object_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_policy_id AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_insured_object_id AS CHAR)), ''))))) AS policy_insured_object_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_policy_id AS CHAR)), ''))) AS policy_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_insured_object_id AS CHAR)), ''))) AS insured_object_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE NULLIF(TRIM(CAST(src_policy_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(src_insured_object_id AS CHAR)), '') IS NOT NULL AND source_extract = 'policy_insured_object_bridge.csv';

CREATE TABLE IF NOT EXISTS link_policy_override (
    policy_override_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    policy_hash_key TEXT,
    override_hash_key TEXT
);

TRUNCATE TABLE link_policy_override;
INSERT INTO link_policy_override (policy_override_hash_key, load_date, record_source, policy_hash_key, override_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_policy_id AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_override_id AS CHAR)), ''))))) AS policy_override_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_policy_id AS CHAR)), ''))) AS policy_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_override_id AS CHAR)), ''))) AS override_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE NULLIF(TRIM(CAST(src_policy_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(src_override_id AS CHAR)), '') IS NOT NULL AND source_extract = 'policy_override_bridge.csv';

CREATE TABLE IF NOT EXISTS link_quote_broker (
    quote_broker_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    quote_hash_key TEXT,
    broker_hash_key TEXT
);

TRUNCATE TABLE link_quote_broker;
INSERT INTO link_quote_broker (quote_broker_hash_key, load_date, record_source, quote_hash_key, broker_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_quote_id AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_agent_id AS CHAR)), ''))))) AS quote_broker_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_quote_id AS CHAR)), ''))) AS quote_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_agent_id AS CHAR)), ''))) AS broker_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE NULLIF(TRIM(CAST(src_quote_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(src_agent_id AS CHAR)), '') IS NOT NULL AND source_extract = 'quote_broker_bridge.csv';

CREATE TABLE IF NOT EXISTS link_quote_channel (
    quote_channel_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    quote_hash_key TEXT,
    channel_hash_key TEXT
);

TRUNCATE TABLE link_quote_channel;
INSERT INTO link_quote_channel (quote_channel_hash_key, load_date, record_source, quote_hash_key, channel_hash_key)
SELECT DISTINCT
    MD5(CONCAT_WS('|', MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_quote_id AS CHAR)), ''))), MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_channel_id AS CHAR)), ''))))) AS quote_channel_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_quote_id AS CHAR)), ''))) AS quote_hash_key,
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_channel_id AS CHAR)), ''))) AS channel_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE NULLIF(TRIM(CAST(src_quote_id AS CHAR)), '') IS NOT NULL AND NULLIF(TRIM(CAST(src_channel_id AS CHAR)), '') IS NOT NULL AND source_extract = 'quote_channel_bridge.csv';

-- ============================================================
-- SATELLITES
-- ============================================================

CREATE TABLE IF NOT EXISTS sat_person_crm (
    person_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    party_kind TEXT,
    tenant_cd TEXT,
    lead_ind TEXT,
    paperless_ind TEXT,
    src_party_ref TEXT,
    src_party_type TEXT,
    natural_ref TEXT,
    given_nm TEXT,
    family_nm TEXT,
    display_nm TEXT,
    title_txt TEXT,
    occupation_txt TEXT,
    dob TEXT,
    birth_yr TEXT,
    nationality_txt TEXT,
    gender_txt TEXT,
    marital_txt TEXT,
    disability_degree TEXT,
    language_pref TEXT,
    role_txt TEXT,
    job_title_txt TEXT,
    legal_ref TEXT,
    legal_name TEXT,
    legal_score_no TEXT,
    legal_status_txt TEXT,
    legal_job_title_txt TEXT,
    legal_src_ref TEXT,
    legal_src_type TEXT,
    constitution_dt TEXT,
    lead_conv_dt TEXT
);

TRUNCATE TABLE sat_person_crm;
INSERT INTO sat_person_crm (person_hash_key, load_date, record_source, party_kind, tenant_cd, lead_ind, paperless_ind, src_party_ref, src_party_type, natural_ref, given_nm, family_nm, display_nm, title_txt, occupation_txt, dob, birth_yr, nationality_txt, gender_txt, marital_txt, disability_degree, language_pref, role_txt, job_title_txt, legal_ref, legal_name, legal_score_no, legal_status_txt, legal_job_title_txt, legal_src_ref, legal_src_type, constitution_dt, lead_conv_dt)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(party_ref AS CHAR)), ''))) AS person_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    party_kind AS party_kind,
    tenant_cd AS tenant_cd,
    lead_ind AS lead_ind,
    paperless_ind AS paperless_ind,
    src_party_ref AS src_party_ref,
    src_party_type AS src_party_type,
    natural_ref AS natural_ref,
    given_nm AS given_nm,
    family_nm AS family_nm,
    display_nm AS display_nm,
    title_txt AS title_txt,
    occupation_txt AS occupation_txt,
    dob AS dob,
    birth_yr AS birth_yr,
    nationality_txt AS nationality_txt,
    gender_txt AS gender_txt,
    marital_txt AS marital_txt,
    disability_degree AS disability_degree,
    language_pref AS language_pref,
    role_txt AS role_txt,
    job_title_txt AS job_title_txt,
    legal_ref AS legal_ref,
    legal_name AS legal_name,
    legal_score_no AS legal_score_no,
    legal_status_txt AS legal_status_txt,
    legal_job_title_txt AS legal_job_title_txt,
    legal_src_ref AS legal_src_ref,
    legal_src_type AS legal_src_type,
    constitution_dt AS constitution_dt,
    lead_conv_dt AS lead_conv_dt
FROM crm_party_master_csv
WHERE NULLIF(TRIM(CAST(party_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_person_sap (
    person_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    person_type TEXT,
    organization TEXT,
    org_establishment_date TEXT,
    first_name TEXT,
    middle_name TEXT,
    last_name TEXT,
    date_of_birth TEXT,
    gender TEXT,
    occupation TEXT,
    email_address TEXT,
    phone_number TEXT
);

TRUNCATE TABLE sat_person_sap;
INSERT INTO sat_person_sap (person_hash_key, load_date, record_source, person_type, organization, org_establishment_date, first_name, middle_name, last_name, date_of_birth, gender, occupation, email_address, phone_number)
SELECT DISTINCT
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(person_id AS CHAR)), ''))) AS person_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    person_type AS person_type,
    organization AS organization,
    org_establishment_date AS org_establishment_date,
    first_name AS first_name,
    middle_name AS middle_name,
    last_name AS last_name,
    date_of_birth AS date_of_birth,
    gender AS gender,
    occupation AS occupation,
    email_address AS email_address,
    phone_number AS phone_number
FROM sap_person_db
WHERE NULLIF(TRIM(CAST(person_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_address_crm (
    address_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    party_ref TEXT,
    street_txt TEXT,
    postal_cd TEXT,
    city_nm TEXT,
    state_cd TEXT,
    country_cd TEXT,
    address_type_txt TEXT,
    region_txt TEXT
);

TRUNCATE TABLE sat_address_crm;
INSERT INTO sat_address_crm (address_hash_key, load_date, record_source, party_ref, street_txt, postal_cd, city_nm, state_cd, country_cd, address_type_txt, region_txt)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(address_ref AS CHAR)), ''))) AS address_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    party_ref AS party_ref,
    street_txt AS street_txt,
    postal_cd AS postal_cd,
    city_nm AS city_nm,
    state_cd AS state_cd,
    country_cd AS country_cd,
    address_type_txt AS address_type_txt,
    region_txt AS region_txt
FROM crm_address_book_csv
WHERE NULLIF(TRIM(CAST(address_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_address_sap (
    address_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    person_id TEXT,
    address_line_1 TEXT,
    address_line_2 TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    zipcode TEXT
);

TRUNCATE TABLE sat_address_sap;
INSERT INTO sat_address_sap (address_hash_key, load_date, record_source, person_id, address_line_1, address_line_2, city, state, country, zipcode)
SELECT DISTINCT
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(address_id AS CHAR)), ''))) AS address_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    person_id AS person_id,
    address_line_1 AS address_line_1,
    address_line_2 AS address_line_2,
    city AS city,
    state AS state,
    country AS country,
    zipcode AS zipcode
FROM sap_address_db
WHERE NULLIF(TRIM(CAST(address_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_product_crm (
    product_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    product_cd TEXT,
    product_line TEXT,
    product_type_txt TEXT,
    underwriting_group_txt TEXT,
    regulatory_approval_cd TEXT,
    product_status_txt TEXT,
    product_lob_cd TEXT,
    product_launch_dt TEXT,
    product_variant TEXT
);

TRUNCATE TABLE sat_product_crm;
INSERT INTO sat_product_crm (product_hash_key, load_date, record_source, product_cd, product_line, product_type_txt, underwriting_group_txt, regulatory_approval_cd, product_status_txt, product_lob_cd, product_launch_dt, product_variant)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(product_ref AS CHAR)), ''))) AS product_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    product_cd AS product_cd,
    product_line AS product_line,
    product_type_txt AS product_type_txt,
    underwriting_group_txt AS underwriting_group_txt,
    regulatory_approval_cd AS regulatory_approval_cd,
    product_status_txt AS product_status_txt,
    product_lob_cd AS product_lob_cd,
    product_launch_dt AS product_launch_dt,
    product_variant AS product_variant
FROM crm_product_catalog_csv
WHERE NULLIF(TRIM(CAST(product_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_product_sap (
    product_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    product_type TEXT,
    product_sub_type TEXT,
    product_name TEXT,
    product_start_date TEXT,
    line_of_business TEXT
);

TRUNCATE TABLE sat_product_sap;
INSERT INTO sat_product_sap (product_hash_key, load_date, record_source, product_type, product_sub_type, product_name, product_start_date, line_of_business)
SELECT DISTINCT
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(product_id AS CHAR)), ''))) AS product_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    product_type AS product_type,
    product_sub_type AS product_sub_type,
    product_name AS product_name,
    product_start_date AS product_start_date,
    line_of_business AS line_of_business
FROM sap_product_db
WHERE NULLIF(TRIM(CAST(product_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_home_crm (
    home_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    policy_ref TEXT,
    product_ref TEXT,
    product_cd TEXT,
    wall_material_txt TEXT,
    risk_address_txt TEXT,
    roof_material_txt TEXT,
    property_type_txt TEXT,
    property_state_cd TEXT,
    existing_home_ind TEXT,
    street_txt TEXT,
    postal_cd TEXT,
    city_nm TEXT,
    state_cd TEXT,
    country_cd TEXT,
    insured_object_id TEXT,
    insured_object_type TEXT,
    insured_object_sub_type TEXT,
    insured_object_description TEXT,
    insured_value TEXT,
    currency_code TEXT,
    insured_object_start_date TEXT,
    insured_object_end_date TEXT,
    insured_object_current_status TEXT
);

TRUNCATE TABLE sat_home_crm;
INSERT INTO sat_home_crm (home_hash_key, load_date, record_source, policy_ref, product_ref, product_cd, wall_material_txt, risk_address_txt, roof_material_txt, property_type_txt, property_state_cd, existing_home_ind, street_txt, postal_cd, city_nm, state_cd, country_cd, insured_object_id, insured_object_type, insured_object_sub_type, insured_object_description, insured_value, currency_code, insured_object_start_date, insured_object_end_date, insured_object_current_status)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(property_ref AS CHAR)), ''))) AS home_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    policy_ref AS policy_ref,
    product_ref AS product_ref,
    product_cd AS product_cd,
    wall_material_txt AS wall_material_txt,
    risk_address_txt AS risk_address_txt,
    roof_material_txt AS roof_material_txt,
    property_type_txt AS property_type_txt,
    property_state_cd AS property_state_cd,
    existing_home_ind AS existing_home_ind,
    street_txt AS street_txt,
    postal_cd AS postal_cd,
    city_nm AS city_nm,
    state_cd AS state_cd,
    country_cd AS country_cd,
    insured_object_id AS insured_object_id,
    insured_object_type AS insured_object_type,
    insured_object_sub_type AS insured_object_sub_type,
    insured_object_description AS insured_object_description,
    insured_value AS insured_value,
    currency_code AS currency_code,
    insured_object_start_date AS insured_object_start_date,
    insured_object_end_date AS insured_object_end_date,
    insured_object_current_status AS insured_object_current_status
FROM crm_property_asset_csv
WHERE NULLIF(TRIM(CAST(property_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_home_sap (
    home_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    policy_id TEXT,
    product_id TEXT,
    home_type TEXT,
    home_location TEXT,
    wall_type TEXT,
    roof_material TEXT
);

TRUNCATE TABLE sat_home_sap;
INSERT INTO sat_home_sap (home_hash_key, load_date, record_source, policy_id, product_id, home_type, home_location, wall_type, roof_material)
SELECT DISTINCT
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(home_id AS CHAR)), ''))) AS home_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    policy_id AS policy_id,
    product_id AS product_id,
    home_type AS home_type,
    home_location AS home_location,
    wall_type AS wall_type,
    roof_material AS roof_material
FROM sap_home_db
WHERE NULLIF(TRIM(CAST(home_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_motor_crm (
    motor_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    policy_ref TEXT,
    product_ref TEXT,
    product_cd TEXT,
    auto_decline_ind TEXT,
    body_style_txt TEXT,
    fuel_type_txt TEXT,
    license_status_txt TEXT,
    existing_motor_ind TEXT,
    motor_lapse_cnt TEXT,
    garage_address_txt TEXT,
    risk_class_cd TEXT,
    variant_nm TEXT,
    owner_type_txt TEXT,
    registration_state_cd TEXT,
    vehicle_class_txt TEXT,
    model_nm TEXT,
    vehicle_type_txt TEXT,
    insured_value_amt TEXT,
    manufacture_yr TEXT,
    vehicle_age_yrs TEXT,
    driver_experience_years TEXT,
    insured_object_id TEXT,
    insured_object_type TEXT,
    insured_object_sub_type TEXT,
    insured_object_description TEXT,
    insured_value TEXT,
    currency_code TEXT,
    insured_object_start_date TEXT,
    insured_object_end_date TEXT,
    insured_object_current_status TEXT
);

TRUNCATE TABLE sat_motor_crm;
INSERT INTO sat_motor_crm (motor_hash_key, load_date, record_source, policy_ref, product_ref, product_cd, auto_decline_ind, body_style_txt, fuel_type_txt, license_status_txt, existing_motor_ind, motor_lapse_cnt, garage_address_txt, risk_class_cd, variant_nm, owner_type_txt, registration_state_cd, vehicle_class_txt, model_nm, vehicle_type_txt, insured_value_amt, manufacture_yr, vehicle_age_yrs, driver_experience_years, insured_object_id, insured_object_type, insured_object_sub_type, insured_object_description, insured_value, currency_code, insured_object_start_date, insured_object_end_date, insured_object_current_status)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(vehicle_ref AS CHAR)), ''))) AS motor_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    policy_ref AS policy_ref,
    product_ref AS product_ref,
    product_cd AS product_cd,
    auto_decline_ind AS auto_decline_ind,
    body_style_txt AS body_style_txt,
    fuel_type_txt AS fuel_type_txt,
    license_status_txt AS license_status_txt,
    existing_motor_ind AS existing_motor_ind,
    motor_lapse_cnt AS motor_lapse_cnt,
    garage_address_txt AS garage_address_txt,
    risk_class_cd AS risk_class_cd,
    variant_nm AS variant_nm,
    owner_type_txt AS owner_type_txt,
    registration_state_cd AS registration_state_cd,
    vehicle_class_txt AS vehicle_class_txt,
    model_nm AS model_nm,
    vehicle_type_txt AS vehicle_type_txt,
    insured_value_amt AS insured_value_amt,
    manufacture_yr AS manufacture_yr,
    vehicle_age_yrs AS vehicle_age_yrs,
    driver_experience_years AS driver_experience_years,
    insured_object_id AS insured_object_id,
    insured_object_type AS insured_object_type,
    insured_object_sub_type AS insured_object_sub_type,
    insured_object_description AS insured_object_description,
    insured_value AS insured_value,
    currency_code AS currency_code,
    insured_object_start_date AS insured_object_start_date,
    insured_object_end_date AS insured_object_end_date,
    insured_object_current_status AS insured_object_current_status
FROM crm_vehicle_asset_csv
WHERE NULLIF(TRIM(CAST(vehicle_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_motor_sap (
    motor_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    policy_id TEXT,
    product_id TEXT,
    motor_class TEXT,
    motor_model TEXT,
    motor_type TEXT,
    manufacturing_date TEXT,
    body_colour TEXT,
    fuel_type TEXT,
    gear_type TEXT,
    motor_parked_location TEXT
);

TRUNCATE TABLE sat_motor_sap;
INSERT INTO sat_motor_sap (motor_hash_key, load_date, record_source, policy_id, product_id, motor_class, motor_model, motor_type, manufacturing_date, body_colour, fuel_type, gear_type, motor_parked_location)
SELECT DISTINCT
    MD5(CONCAT('SAP|', NULLIF(TRIM(CAST(motor_id AS CHAR)), ''))) AS motor_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'SAP' AS record_source,
    policy_id AS policy_id,
    product_id AS product_id,
    motor_class AS motor_class,
    motor_model AS motor_model,
    motor_type AS motor_type,
    manufacturing_date AS manufacturing_date,
    body_colour AS body_colour,
    fuel_type AS fuel_type,
    gear_type AS gear_type,
    motor_parked_location AS motor_parked_location
FROM sap_motor_db
WHERE NULLIF(TRIM(CAST(motor_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_contact (
    contact_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    party_ref TEXT,
    email_home_txt TEXT,
    email_work_txt TEXT,
    phone_work_txt TEXT,
    phone_home_txt TEXT
);

TRUNCATE TABLE sat_contact;
INSERT INTO sat_contact (contact_hash_key, load_date, record_source, party_ref, email_home_txt, email_work_txt, phone_work_txt, phone_home_txt)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(contact_ref AS CHAR)), ''))) AS contact_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    party_ref AS party_ref,
    email_home_txt AS email_home_txt,
    email_work_txt AS email_work_txt,
    phone_work_txt AS phone_work_txt,
    phone_home_txt AS phone_home_txt
FROM crm_contact_point_csv
WHERE NULLIF(TRIM(CAST(contact_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_identities (
    identities_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    party_ref TEXT,
    ecid_txt TEXT,
    hashed_email_txt TEXT
);

TRUNCATE TABLE sat_identities;
INSERT INTO sat_identities (identities_hash_key, load_date, record_source, party_ref, ecid_txt, hashed_email_txt)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(identity_ref AS CHAR)), ''))) AS identities_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    party_ref AS party_ref,
    ecid_txt AS ecid_txt,
    hashed_email_txt AS hashed_email_txt
FROM crm_identity_registry_csv
WHERE NULLIF(TRIM(CAST(identity_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_consent (
    consent_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    party_ref TEXT,
    opt_in_valid_ind TEXT,
    opt_in_legit_ind TEXT
);

TRUNCATE TABLE sat_consent;
INSERT INTO sat_consent (consent_hash_key, load_date, record_source, party_ref, opt_in_valid_ind, opt_in_legit_ind)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(consent_ref AS CHAR)), ''))) AS consent_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    party_ref AS party_ref,
    opt_in_valid_ind AS opt_in_valid_ind,
    opt_in_legit_ind AS opt_in_legit_ind
FROM crm_consent_snapshot_csv
WHERE NULLIF(TRIM(CAST(consent_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_marketing_preference (
    marketing_preference_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    party_ref TEXT,
    sms_ind TEXT,
    email_ind TEXT,
    email_sub_ind TEXT,
    call_ind TEXT,
    any_ind TEXT,
    commercial_email_ind TEXT,
    postal_mail_ind TEXT
);

TRUNCATE TABLE sat_marketing_preference;
INSERT INTO sat_marketing_preference (marketing_preference_hash_key, load_date, record_source, party_ref, sms_ind, email_ind, email_sub_ind, call_ind, any_ind, commercial_email_ind, postal_mail_ind)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(preference_ref AS CHAR)), ''))) AS marketing_preference_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    party_ref AS party_ref,
    sms_ind AS sms_ind,
    email_ind AS email_ind,
    email_sub_ind AS email_sub_ind,
    call_ind AS call_ind,
    any_ind AS any_ind,
    commercial_email_ind AS commercial_email_ind,
    postal_mail_ind AS postal_mail_ind
FROM crm_comm_preference_csv
WHERE NULLIF(TRIM(CAST(preference_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_marketing_engagement (
    marketing_engagement_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    party_ref TEXT,
    promo_cd TEXT,
    email_opened_ind TEXT,
    campaign_status_txt TEXT,
    has_retention_team_interaction TEXT,
    customer_service_call_frequency TEXT,
    average_call_sentiment TEXT,
    engagement_score TEXT,
    first_contact_resolution TEXT
);

TRUNCATE TABLE sat_marketing_engagement;
INSERT INTO sat_marketing_engagement (marketing_engagement_hash_key, load_date, record_source, party_ref, promo_cd, email_opened_ind, campaign_status_txt, has_retention_team_interaction, customer_service_call_frequency, average_call_sentiment, engagement_score, first_contact_resolution)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(engagement_ref AS CHAR)), ''))) AS marketing_engagement_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    party_ref AS party_ref,
    promo_cd AS promo_cd,
    email_opened_ind AS email_opened_ind,
    campaign_status_txt AS campaign_status_txt,
    has_retention_team_interaction AS has_retention_team_interaction,
    customer_service_call_frequency AS customer_service_call_frequency,
    average_call_sentiment AS average_call_sentiment,
    engagement_score AS engagement_score,
    first_contact_resolution AS first_contact_resolution
FROM crm_campaign_touch_csv
WHERE NULLIF(TRIM(CAST(engagement_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_lead (
    lead_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    party_ref TEXT,
    interest_bucket TEXT,
    contact_pref TEXT,
    person_score_no TEXT,
    person_status_txt TEXT,
    converted_dt TEXT
);

TRUNCATE TABLE sat_lead;
INSERT INTO sat_lead (lead_hash_key, load_date, record_source, party_ref, interest_bucket, contact_pref, person_score_no, person_status_txt, converted_dt)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(lead_ref AS CHAR)), ''))) AS lead_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    party_ref AS party_ref,
    interest_bucket AS interest_bucket,
    contact_pref AS contact_pref,
    person_score_no AS person_score_no,
    person_status_txt AS person_status_txt,
    converted_dt AS converted_dt
FROM crm_lead_register_csv
WHERE NULLIF(TRIM(CAST(lead_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_quote (
    quote_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    party_ref TEXT,
    product_ref TEXT,
    product_cd TEXT,
    gross_amt TEXT,
    net_amt TEXT,
    quote_no TEXT,
    quote_status_txt TEXT,
    renewal_amt_curr TEXT,
    renewal_amt_next TEXT,
    quoted_premium TEXT,
    quoted_date TEXT,
    quote_month_name TEXT,
    risk_score TEXT,
    policy_complexity TEXT,
    uw_approval_type TEXT,
    rejection_reason TEXT
);

TRUNCATE TABLE sat_quote;
INSERT INTO sat_quote (quote_hash_key, load_date, record_source, party_ref, product_ref, product_cd, gross_amt, net_amt, quote_no, quote_status_txt, renewal_amt_curr, renewal_amt_next, quoted_premium, quoted_date, quote_month_name, risk_score, policy_complexity, uw_approval_type, rejection_reason)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(quote_ref AS CHAR)), ''))) AS quote_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    party_ref AS party_ref,
    product_ref AS product_ref,
    product_cd AS product_cd,
    gross_amt AS gross_amt,
    net_amt AS net_amt,
    quote_no AS quote_no,
    quote_status_txt AS quote_status_txt,
    renewal_amt_curr AS renewal_amt_curr,
    renewal_amt_next AS renewal_amt_next,
    quoted_premium AS quoted_premium,
    quoted_date AS quoted_date,
    quote_month_name AS quote_month_name,
    risk_score AS risk_score,
    policy_complexity AS policy_complexity,
    uw_approval_type AS uw_approval_type,
    rejection_reason AS rejection_reason
FROM crm_quote_register_csv
WHERE NULLIF(TRIM(CAST(quote_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_policy (
    policy_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    party_ref TEXT,
    customer_ref TEXT,
    quote_ref TEXT,
    product_ref TEXT,
    product_cd TEXT,
    cover_option_txt TEXT,
    declined_claim_cnt TEXT,
    fraud_ind TEXT,
    gross_amt TEXT,
    net_amt TEXT,
    active_claim_cnt TEXT,
    previous_claim_cnt TEXT,
    policy_cycle_no TEXT,
    policy_end_dt TEXT,
    policy_term_months TEXT,
    policy_no TEXT,
    policy_start_dt TEXT,
    policy_status_txt TEXT,
    renewal_premium_curr TEXT,
    renewal_premium_next TEXT,
    renewal_dt TEXT,
    sales_channel_txt TEXT,
    is_auto_renew_enabled TEXT,
    no_claims_discount_years TEXT,
    payment_method TEXT,
    is_direct_debit_cancellation TEXT,
    missed_payment_count TEXT,
    loyalty_discount_usage TEXT,
    is_installment_default TEXT,
    policy_renewal_satisfaction_score TEXT,
    policy_renewal_feedback TEXT,
    is_renewal_escalation TEXT
);

TRUNCATE TABLE sat_policy;
INSERT INTO sat_policy (policy_hash_key, load_date, record_source, party_ref, customer_ref, quote_ref, product_ref, product_cd, cover_option_txt, declined_claim_cnt, fraud_ind, gross_amt, net_amt, active_claim_cnt, previous_claim_cnt, policy_cycle_no, policy_end_dt, policy_term_months, policy_no, policy_start_dt, policy_status_txt, renewal_premium_curr, renewal_premium_next, renewal_dt, sales_channel_txt, is_auto_renew_enabled, no_claims_discount_years, payment_method, is_direct_debit_cancellation, missed_payment_count, loyalty_discount_usage, is_installment_default, policy_renewal_satisfaction_score, policy_renewal_feedback, is_renewal_escalation)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(policy_ref AS CHAR)), ''))) AS policy_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    party_ref AS party_ref,
    customer_ref AS customer_ref,
    quote_ref AS quote_ref,
    product_ref AS product_ref,
    product_cd AS product_cd,
    cover_option_txt AS cover_option_txt,
    declined_claim_cnt AS declined_claim_cnt,
    fraud_ind AS fraud_ind,
    gross_amt AS gross_amt,
    net_amt AS net_amt,
    active_claim_cnt AS active_claim_cnt,
    previous_claim_cnt AS previous_claim_cnt,
    policy_cycle_no AS policy_cycle_no,
    policy_end_dt AS policy_end_dt,
    policy_term_months AS policy_term_months,
    policy_no AS policy_no,
    policy_start_dt AS policy_start_dt,
    policy_status_txt AS policy_status_txt,
    renewal_premium_curr AS renewal_premium_curr,
    renewal_premium_next AS renewal_premium_next,
    renewal_dt AS renewal_dt,
    sales_channel_txt AS sales_channel_txt,
    is_auto_renew_enabled AS is_auto_renew_enabled,
    no_claims_discount_years AS no_claims_discount_years,
    payment_method AS payment_method,
    is_direct_debit_cancellation AS is_direct_debit_cancellation,
    missed_payment_count AS missed_payment_count,
    loyalty_discount_usage AS loyalty_discount_usage,
    is_installment_default AS is_installment_default,
    policy_renewal_satisfaction_score AS policy_renewal_satisfaction_score,
    policy_renewal_feedback AS policy_renewal_feedback,
    is_renewal_escalation AS is_renewal_escalation
FROM crm_policy_register_csv
WHERE NULLIF(TRIM(CAST(policy_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_account (
    account_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    party_ref TEXT,
    account_no TEXT,
    account_type_txt TEXT,
    last_access_dt TEXT,
    last_change_dt TEXT,
    account_create_type_txt TEXT,
    account_status_txt TEXT
);

TRUNCATE TABLE sat_account;
INSERT INTO sat_account (account_hash_key, load_date, record_source, party_ref, account_no, account_type_txt, last_access_dt, last_change_dt, account_create_type_txt, account_status_txt)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(account_ref AS CHAR)), ''))) AS account_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    party_ref AS party_ref,
    account_no AS account_no,
    account_type_txt AS account_type_txt,
    last_access_dt AS last_access_dt,
    last_change_dt AS last_change_dt,
    account_create_type_txt AS account_create_type_txt,
    account_status_txt AS account_status_txt
FROM crm_account_book_csv
WHERE NULLIF(TRIM(CAST(account_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_customer (
    customer_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    party_ref TEXT,
    customer_no TEXT,
    customer_status_txt TEXT,
    customer_status_reason_txt TEXT,
    customer_since_dt TEXT,
    customer_rating_no TEXT,
    customer_segment_txt TEXT,
    lob_txt TEXT,
    nps_score_no TEXT,
    customer_onboarding_satisfaction_score TEXT,
    customer_onboarding_feedback TEXT
);

TRUNCATE TABLE sat_customer;
INSERT INTO sat_customer (customer_hash_key, load_date, record_source, party_ref, customer_no, customer_status_txt, customer_status_reason_txt, customer_since_dt, customer_rating_no, customer_segment_txt, lob_txt, nps_score_no, customer_onboarding_satisfaction_score, customer_onboarding_feedback)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(customer_ref AS CHAR)), ''))) AS customer_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    party_ref AS party_ref,
    customer_no AS customer_no,
    customer_status_txt AS customer_status_txt,
    customer_status_reason_txt AS customer_status_reason_txt,
    customer_since_dt AS customer_since_dt,
    customer_rating_no AS customer_rating_no,
    customer_segment_txt AS customer_segment_txt,
    lob_txt AS lob_txt,
    nps_score_no AS nps_score_no,
    customer_onboarding_satisfaction_score AS customer_onboarding_satisfaction_score,
    customer_onboarding_feedback AS customer_onboarding_feedback
FROM crm_customer_portfolio_csv
WHERE NULLIF(TRIM(CAST(customer_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_broker (
    broker_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    src_broker_ref TEXT,
    src_extract_ts TEXT,
    src_system TEXT,
    src_agent_name TEXT,
    src_agent_type TEXT,
    src_agent_status TEXT,
    src_agent_license_number TEXT,
    src_agent_net_promoter_score TEXT,
    src_agent_commission_percentage TEXT,
    src_person_id TEXT
);

TRUNCATE TABLE sat_broker;
INSERT INTO sat_broker (broker_hash_key, load_date, record_source, src_broker_ref, src_extract_ts, src_system, src_agent_name, src_agent_type, src_agent_status, src_agent_license_number, src_agent_net_promoter_score, src_agent_commission_percentage, src_person_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_agent_id AS CHAR)), ''))) AS broker_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    src_broker_ref AS src_broker_ref,
    src_extract_ts AS src_extract_ts,
    src_system AS src_system,
    src_agent_name AS src_agent_name,
    src_agent_type AS src_agent_type,
    src_agent_status AS src_agent_status,
    src_agent_license_number AS src_agent_license_number,
    src_agent_net_promoter_score AS src_agent_net_promoter_score,
    src_agent_commission_percentage AS src_agent_commission_percentage,
    src_person_id AS src_person_id
FROM crm_broker_book_csv
WHERE NULLIF(TRIM(CAST(src_agent_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_campaign (
    campaign_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    src_campaign_ref TEXT,
    src_extract_ts TEXT,
    src_system TEXT,
    src_campaign_name TEXT,
    src_campaign_type TEXT,
    src_campaign_start_date TEXT,
    src_campaign_end_date TEXT,
    src_campaign_status TEXT,
    src_campaign_budget TEXT,
    src_campaign_target_audience TEXT,
    src_campaign_marketing_source TEXT,
    src_campaign_owner_department TEXT,
    src_campaign_country TEXT,
    src_campaign_conversion_goal TEXT,
    src_number_of_impressions TEXT,
    src_number_of_clicks TEXT,
    src_is_active TEXT,
    src_number_of_visits TEXT,
    src_number_of_policy_purchases TEXT,
    src_number_of_emails_sent TEXT,
    src_number_of_email_bounced TEXT,
    src_number_of_emails_delivered TEXT,
    src_number_of_emails_opened TEXT,
    src_click_through_rate TEXT,
    src_spend_amount TEXT,
    src_incremental_revenue TEXT,
    src_survey_wave TEXT,
    src_total_number_of_respondents TEXT,
    src_number_of_respondents_aware TEXT,
    src_number_of_promoters TEXT,
    src_number_of_passives TEXT,
    src_number_of_detractors TEXT,
    src_number_of_followers TEXT,
    src_number_of_likes TEXT,
    src_number_of_comments TEXT,
    src_number_of_shares TEXT,
    src_number_of_brand_mentions TEXT,
    src_number_of_category_mentions TEXT,
    src_person_id TEXT
);

TRUNCATE TABLE sat_campaign;
INSERT INTO sat_campaign (campaign_hash_key, load_date, record_source, src_campaign_ref, src_extract_ts, src_system, src_campaign_name, src_campaign_type, src_campaign_start_date, src_campaign_end_date, src_campaign_status, src_campaign_budget, src_campaign_target_audience, src_campaign_marketing_source, src_campaign_owner_department, src_campaign_country, src_campaign_conversion_goal, src_number_of_impressions, src_number_of_clicks, src_is_active, src_number_of_visits, src_number_of_policy_purchases, src_number_of_emails_sent, src_number_of_email_bounced, src_number_of_emails_delivered, src_number_of_emails_opened, src_click_through_rate, src_spend_amount, src_incremental_revenue, src_survey_wave, src_total_number_of_respondents, src_number_of_respondents_aware, src_number_of_promoters, src_number_of_passives, src_number_of_detractors, src_number_of_followers, src_number_of_likes, src_number_of_comments, src_number_of_shares, src_number_of_brand_mentions, src_number_of_category_mentions, src_person_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_campaign_id AS CHAR)), ''))) AS campaign_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    src_campaign_ref AS src_campaign_ref,
    src_extract_ts AS src_extract_ts,
    src_system AS src_system,
    src_campaign_name AS src_campaign_name,
    src_campaign_type AS src_campaign_type,
    src_campaign_start_date AS src_campaign_start_date,
    src_campaign_end_date AS src_campaign_end_date,
    src_campaign_status AS src_campaign_status,
    src_campaign_budget AS src_campaign_budget,
    src_campaign_target_audience AS src_campaign_target_audience,
    src_campaign_marketing_source AS src_campaign_marketing_source,
    src_campaign_owner_department AS src_campaign_owner_department,
    src_campaign_country AS src_campaign_country,
    src_campaign_conversion_goal AS src_campaign_conversion_goal,
    src_number_of_impressions AS src_number_of_impressions,
    src_number_of_clicks AS src_number_of_clicks,
    src_is_active AS src_is_active,
    src_number_of_visits AS src_number_of_visits,
    src_number_of_policy_purchases AS src_number_of_policy_purchases,
    src_number_of_emails_sent AS src_number_of_emails_sent,
    src_number_of_email_bounced AS src_number_of_email_bounced,
    src_number_of_emails_delivered AS src_number_of_emails_delivered,
    src_number_of_emails_opened AS src_number_of_emails_opened,
    src_click_through_rate AS src_click_through_rate,
    src_spend_amount AS src_spend_amount,
    src_incremental_revenue AS src_incremental_revenue,
    src_survey_wave AS src_survey_wave,
    src_total_number_of_respondents AS src_total_number_of_respondents,
    src_number_of_respondents_aware AS src_number_of_respondents_aware,
    src_number_of_promoters AS src_number_of_promoters,
    src_number_of_passives AS src_number_of_passives,
    src_number_of_detractors AS src_number_of_detractors,
    src_number_of_followers AS src_number_of_followers,
    src_number_of_likes AS src_number_of_likes,
    src_number_of_comments AS src_number_of_comments,
    src_number_of_shares AS src_number_of_shares,
    src_number_of_brand_mentions AS src_number_of_brand_mentions,
    src_number_of_category_mentions AS src_number_of_category_mentions,
    src_person_id AS src_person_id
FROM crm_campaign_register_csv
WHERE NULLIF(TRIM(CAST(src_campaign_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_channel (
    channel_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    src_channel_ref TEXT,
    src_extract_ts TEXT,
    src_system TEXT,
    src_channel_name TEXT,
    src_channel_type TEXT
);

TRUNCATE TABLE sat_channel;
INSERT INTO sat_channel (channel_hash_key, load_date, record_source, src_channel_ref, src_extract_ts, src_system, src_channel_name, src_channel_type)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_channel_id AS CHAR)), ''))) AS channel_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    src_channel_ref AS src_channel_ref,
    src_extract_ts AS src_extract_ts,
    src_system AS src_system,
    src_channel_name AS src_channel_name,
    src_channel_type AS src_channel_type
FROM crm_channel_catalog_csv
WHERE NULLIF(TRIM(CAST(src_channel_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_claim (
    claim_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    src_claim_ref TEXT,
    src_extract_ts TEXT,
    src_system TEXT,
    src_claim_number TEXT,
    src_claim_type TEXT,
    src_claim_status TEXT,
    src_claim_reason TEXT,
    src_claim_channel TEXT,
    src_claim_handler TEXT,
    src_claim_reported_date TEXT,
    src_claim_settlement_date TEXT,
    src_claim_product TEXT,
    src_is_claim_suspicious TEXT,
    src_is_claim_fraud TEXT,
    src_claim_fraud_status TEXT,
    src_claim_fraud_type TEXT,
    src_claim_fraud_detection_method TEXT,
    src_is_litigation TEXT,
    src_litigation_reason TEXT,
    src_litigation_start_date TEXT,
    src_litigation_end_date TEXT,
    src_litigation_outcome TEXT,
    src_litigation_duration_days TEXT,
    src_claim_fraud_detection_time_in_days TEXT,
    src_is_recovery_opportunity TEXT,
    src_recovery_priority_score TEXT,
    src_recovery_category TEXT,
    src_recovery_source TEXT,
    src_first_recovery_date TEXT,
    src_last_recovery_date TEXT,
    src_is_recovery_happened TEXT,
    src_days_to_first_recovery TEXT,
    src_days_to_last_recovery TEXT,
    src_avg_days_to_close_claim TEXT,
    src_claim_fraud_outcome TEXT,
    src_recovery_type TEXT,
    src_recovery_band TEXT,
    src_third_party_involved TEXT,
    src_third_party_involved_overall_score TEXT,
    src_solicitor TEXT,
    src_claim_amount TEXT,
    src_claims_paid TEXT,
    src_outstanding_reserve TEXT,
    src_claims_expenses TEXT,
    src_recovery_received TEXT,
    src_compensation_offered TEXT,
    src_remediation_amount TEXT,
    src_suspectd_amount TEXT,
    src_fraud_amount TEXT,
    src_legal_expenses TEXT,
    src_claim_band TEXT,
    src_claim_band_sort TEXT,
    src_policy_id TEXT,
    src_is_fault_claim TEXT,
    src_claim_satisfaction_score TEXT,
    src_claims_feedback TEXT,
    src_is_claim_complaint_raised TEXT
);

TRUNCATE TABLE sat_claim;
INSERT INTO sat_claim (claim_hash_key, load_date, record_source, src_claim_ref, src_extract_ts, src_system, src_claim_number, src_claim_type, src_claim_status, src_claim_reason, src_claim_channel, src_claim_handler, src_claim_reported_date, src_claim_settlement_date, src_claim_product, src_is_claim_suspicious, src_is_claim_fraud, src_claim_fraud_status, src_claim_fraud_type, src_claim_fraud_detection_method, src_is_litigation, src_litigation_reason, src_litigation_start_date, src_litigation_end_date, src_litigation_outcome, src_litigation_duration_days, src_claim_fraud_detection_time_in_days, src_is_recovery_opportunity, src_recovery_priority_score, src_recovery_category, src_recovery_source, src_first_recovery_date, src_last_recovery_date, src_is_recovery_happened, src_days_to_first_recovery, src_days_to_last_recovery, src_avg_days_to_close_claim, src_claim_fraud_outcome, src_recovery_type, src_recovery_band, src_third_party_involved, src_third_party_involved_overall_score, src_solicitor, src_claim_amount, src_claims_paid, src_outstanding_reserve, src_claims_expenses, src_recovery_received, src_compensation_offered, src_remediation_amount, src_suspectd_amount, src_fraud_amount, src_legal_expenses, src_claim_band, src_claim_band_sort, src_policy_id, src_is_fault_claim, src_claim_satisfaction_score, src_claims_feedback, src_is_claim_complaint_raised)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_claim_id AS CHAR)), ''))) AS claim_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    src_claim_ref AS src_claim_ref,
    src_extract_ts AS src_extract_ts,
    src_system AS src_system,
    src_claim_number AS src_claim_number,
    src_claim_type AS src_claim_type,
    src_claim_status AS src_claim_status,
    src_claim_reason AS src_claim_reason,
    src_claim_channel AS src_claim_channel,
    src_claim_handler AS src_claim_handler,
    src_claim_reported_date AS src_claim_reported_date,
    src_claim_settlement_date AS src_claim_settlement_date,
    src_claim_product AS src_claim_product,
    src_is_claim_suspicious AS src_is_claim_suspicious,
    src_is_claim_fraud AS src_is_claim_fraud,
    src_claim_fraud_status AS src_claim_fraud_status,
    src_claim_fraud_type AS src_claim_fraud_type,
    src_claim_fraud_detection_method AS src_claim_fraud_detection_method,
    src_is_litigation AS src_is_litigation,
    src_litigation_reason AS src_litigation_reason,
    src_litigation_start_date AS src_litigation_start_date,
    src_litigation_end_date AS src_litigation_end_date,
    src_litigation_outcome AS src_litigation_outcome,
    src_litigation_duration_days AS src_litigation_duration_days,
    src_claim_fraud_detection_time_in_days AS src_claim_fraud_detection_time_in_days,
    src_is_recovery_opportunity AS src_is_recovery_opportunity,
    src_recovery_priority_score AS src_recovery_priority_score,
    src_recovery_category AS src_recovery_category,
    src_recovery_source AS src_recovery_source,
    src_first_recovery_date AS src_first_recovery_date,
    src_last_recovery_date AS src_last_recovery_date,
    src_is_recovery_happened AS src_is_recovery_happened,
    src_days_to_first_recovery AS src_days_to_first_recovery,
    src_days_to_last_recovery AS src_days_to_last_recovery,
    src_avg_days_to_close_claim AS src_avg_days_to_close_claim,
    src_claim_fraud_outcome AS src_claim_fraud_outcome,
    src_recovery_type AS src_recovery_type,
    src_recovery_band AS src_recovery_band,
    src_third_party_involved AS src_third_party_involved,
    src_third_party_involved_overall_score AS src_third_party_involved_overall_score,
    src_solicitor AS src_solicitor,
    src_claim_amount AS src_claim_amount,
    src_claims_paid AS src_claims_paid,
    src_outstanding_reserve AS src_outstanding_reserve,
    src_claims_expenses AS src_claims_expenses,
    src_recovery_received AS src_recovery_received,
    src_compensation_offered AS src_compensation_offered,
    src_remediation_amount AS src_remediation_amount,
    src_suspectd_amount AS src_suspectd_amount,
    src_fraud_amount AS src_fraud_amount,
    src_legal_expenses AS src_legal_expenses,
    src_claim_band AS src_claim_band,
    src_claim_band_sort AS src_claim_band_sort,
    src_policy_id AS src_policy_id,
    src_is_fault_claim AS src_is_fault_claim,
    src_claim_satisfaction_score AS src_claim_satisfaction_score,
    src_claims_feedback AS src_claims_feedback,
    src_is_claim_complaint_raised AS src_is_claim_complaint_raised
FROM crm_claim_register_csv
WHERE NULLIF(TRIM(CAST(src_claim_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_complaint (
    complaint_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    src_complaint_ref TEXT,
    src_extract_ts TEXT,
    src_system TEXT,
    src_complaint_date TEXT,
    src_complaint_acknowledgement_date TEXT,
    src_complaint_resolved_date TEXT,
    src_complaint_upheld_status TEXT,
    src_is_financial_ombudsman_service_referral TEXT,
    src_complaint_driver TEXT,
    src_complaint_channel TEXT,
    src_compensation_amount TEXT,
    src_insurance_category TEXT,
    src_complaint_status TEXT,
    src_policy_id TEXT,
    src_regulation_id TEXT,
    src_customer_complaint_satisfaction_score TEXT,
    src_complaint_feedback TEXT
);

TRUNCATE TABLE sat_complaint;
INSERT INTO sat_complaint (complaint_hash_key, load_date, record_source, src_complaint_ref, src_extract_ts, src_system, src_complaint_date, src_complaint_acknowledgement_date, src_complaint_resolved_date, src_complaint_upheld_status, src_is_financial_ombudsman_service_referral, src_complaint_driver, src_complaint_channel, src_compensation_amount, src_insurance_category, src_complaint_status, src_policy_id, src_regulation_id, src_customer_complaint_satisfaction_score, src_complaint_feedback)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_complaint_id AS CHAR)), ''))) AS complaint_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    src_complaint_ref AS src_complaint_ref,
    src_extract_ts AS src_extract_ts,
    src_system AS src_system,
    src_complaint_date AS src_complaint_date,
    src_complaint_acknowledgement_date AS src_complaint_acknowledgement_date,
    src_complaint_resolved_date AS src_complaint_resolved_date,
    src_complaint_upheld_status AS src_complaint_upheld_status,
    src_is_financial_ombudsman_service_referral AS src_is_financial_ombudsman_service_referral,
    src_complaint_driver AS src_complaint_driver,
    src_complaint_channel AS src_complaint_channel,
    src_compensation_amount AS src_compensation_amount,
    src_insurance_category AS src_insurance_category,
    src_complaint_status AS src_complaint_status,
    src_policy_id AS src_policy_id,
    src_regulation_id AS src_regulation_id,
    src_customer_complaint_satisfaction_score AS src_customer_complaint_satisfaction_score,
    src_complaint_feedback AS src_complaint_feedback
FROM crm_complaint_register_csv
WHERE NULLIF(TRIM(CAST(src_complaint_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_insured_object (
    insured_object_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    policy_ref TEXT,
    product_ref TEXT,
    product_cd TEXT,
    property_ref TEXT,
    wall_material_txt TEXT,
    risk_address_txt TEXT,
    roof_material_txt TEXT,
    property_type_txt TEXT,
    property_state_cd TEXT,
    existing_home_ind TEXT,
    street_txt TEXT,
    postal_cd TEXT,
    city_nm TEXT,
    state_cd TEXT,
    country_cd TEXT,
    insured_object_type TEXT,
    insured_object_sub_type TEXT,
    insured_object_description TEXT,
    insured_value TEXT,
    currency_code TEXT,
    insured_object_start_date TEXT,
    insured_object_end_date TEXT,
    insured_object_current_status TEXT
);

TRUNCATE TABLE sat_insured_object;
INSERT INTO sat_insured_object (insured_object_hash_key, load_date, record_source, policy_ref, product_ref, product_cd, property_ref, wall_material_txt, risk_address_txt, roof_material_txt, property_type_txt, property_state_cd, existing_home_ind, street_txt, postal_cd, city_nm, state_cd, country_cd, insured_object_type, insured_object_sub_type, insured_object_description, insured_value, currency_code, insured_object_start_date, insured_object_end_date, insured_object_current_status)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(insured_object_id AS CHAR)), ''))) AS insured_object_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    policy_ref AS policy_ref,
    product_ref AS product_ref,
    product_cd AS product_cd,
    property_ref AS property_ref,
    wall_material_txt AS wall_material_txt,
    risk_address_txt AS risk_address_txt,
    roof_material_txt AS roof_material_txt,
    property_type_txt AS property_type_txt,
    property_state_cd AS property_state_cd,
    existing_home_ind AS existing_home_ind,
    street_txt AS street_txt,
    postal_cd AS postal_cd,
    city_nm AS city_nm,
    state_cd AS state_cd,
    country_cd AS country_cd,
    insured_object_type AS insured_object_type,
    insured_object_sub_type AS insured_object_sub_type,
    insured_object_description AS insured_object_description,
    insured_value AS insured_value,
    currency_code AS currency_code,
    insured_object_start_date AS insured_object_start_date,
    insured_object_end_date AS insured_object_end_date,
    insured_object_current_status AS insured_object_current_status
FROM crm_property_asset_csv
WHERE NULLIF(TRIM(CAST(insured_object_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_override (
    override_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    src_override_ref TEXT,
    src_extract_ts TEXT,
    src_system TEXT,
    src_override_reason TEXT,
    src_policy_id TEXT
);

TRUNCATE TABLE sat_override;
INSERT INTO sat_override (override_hash_key, load_date, record_source, src_override_ref, src_extract_ts, src_system, src_override_reason, src_policy_id)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_override_id AS CHAR)), ''))) AS override_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    src_override_ref AS src_override_ref,
    src_extract_ts AS src_extract_ts,
    src_system AS src_system,
    src_override_reason AS src_override_reason,
    src_policy_id AS src_policy_id
FROM crm_override_register_csv
WHERE NULLIF(TRIM(CAST(src_override_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_regulation (
    regulation_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    src_regulation_ref TEXT,
    src_extract_ts TEXT,
    src_system TEXT,
    src_regulation_number TEXT,
    src_regulation_name TEXT,
    src_regulation_department TEXT,
    src_regulation_region TEXT,
    src_regulation_risk_level TEXT,
    src_regulation_compliance_status TEXT,
    src_regulation_date_raised TEXT,
    src_regulation_date_closed TEXT,
    src_regulation_owner TEXT,
    src_regulation_deadline_date TEXT,
    src_is_regulation_on_time TEXT
);

TRUNCATE TABLE sat_regulation;
INSERT INTO sat_regulation (regulation_hash_key, load_date, record_source, src_regulation_ref, src_extract_ts, src_system, src_regulation_number, src_regulation_name, src_regulation_department, src_regulation_region, src_regulation_risk_level, src_regulation_compliance_status, src_regulation_date_raised, src_regulation_date_closed, src_regulation_owner, src_regulation_deadline_date, src_is_regulation_on_time)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(src_regulation_id AS CHAR)), ''))) AS regulation_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    src_regulation_ref AS src_regulation_ref,
    src_extract_ts AS src_extract_ts,
    src_system AS src_system,
    src_regulation_number AS src_regulation_number,
    src_regulation_name AS src_regulation_name,
    src_regulation_department AS src_regulation_department,
    src_regulation_region AS src_regulation_region,
    src_regulation_risk_level AS src_regulation_risk_level,
    src_regulation_compliance_status AS src_regulation_compliance_status,
    src_regulation_date_raised AS src_regulation_date_raised,
    src_regulation_date_closed AS src_regulation_date_closed,
    src_regulation_owner AS src_regulation_owner,
    src_regulation_deadline_date AS src_regulation_deadline_date,
    src_is_regulation_on_time AS src_is_regulation_on_time
FROM crm_regulation_register_csv
WHERE NULLIF(TRIM(CAST(src_regulation_id AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_natural_person (
    natural_person_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    party_ref TEXT,
    party_kind TEXT,
    tenant_cd TEXT,
    lead_ind TEXT,
    paperless_ind TEXT,
    src_party_ref TEXT,
    src_party_type TEXT,
    given_nm TEXT,
    family_nm TEXT,
    display_nm TEXT,
    title_txt TEXT,
    occupation_txt TEXT,
    dob TEXT,
    birth_yr TEXT,
    nationality_txt TEXT,
    gender_txt TEXT,
    marital_txt TEXT,
    disability_degree TEXT,
    language_pref TEXT,
    role_txt TEXT,
    job_title_txt TEXT,
    legal_ref TEXT,
    legal_name TEXT,
    legal_score_no TEXT,
    legal_status_txt TEXT,
    legal_job_title_txt TEXT,
    legal_src_ref TEXT,
    legal_src_type TEXT,
    constitution_dt TEXT,
    lead_conv_dt TEXT
);

TRUNCATE TABLE sat_natural_person;
INSERT INTO sat_natural_person (natural_person_hash_key, load_date, record_source, party_ref, party_kind, tenant_cd, lead_ind, paperless_ind, src_party_ref, src_party_type, given_nm, family_nm, display_nm, title_txt, occupation_txt, dob, birth_yr, nationality_txt, gender_txt, marital_txt, disability_degree, language_pref, role_txt, job_title_txt, legal_ref, legal_name, legal_score_no, legal_status_txt, legal_job_title_txt, legal_src_ref, legal_src_type, constitution_dt, lead_conv_dt)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(natural_ref AS CHAR)), ''))) AS natural_person_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    party_ref AS party_ref,
    party_kind AS party_kind,
    tenant_cd AS tenant_cd,
    lead_ind AS lead_ind,
    paperless_ind AS paperless_ind,
    src_party_ref AS src_party_ref,
    src_party_type AS src_party_type,
    given_nm AS given_nm,
    family_nm AS family_nm,
    display_nm AS display_nm,
    title_txt AS title_txt,
    occupation_txt AS occupation_txt,
    dob AS dob,
    birth_yr AS birth_yr,
    nationality_txt AS nationality_txt,
    gender_txt AS gender_txt,
    marital_txt AS marital_txt,
    disability_degree AS disability_degree,
    language_pref AS language_pref,
    role_txt AS role_txt,
    job_title_txt AS job_title_txt,
    legal_ref AS legal_ref,
    legal_name AS legal_name,
    legal_score_no AS legal_score_no,
    legal_status_txt AS legal_status_txt,
    legal_job_title_txt AS legal_job_title_txt,
    legal_src_ref AS legal_src_ref,
    legal_src_type AS legal_src_type,
    constitution_dt AS constitution_dt,
    lead_conv_dt AS lead_conv_dt
FROM crm_party_master_csv
WHERE NULLIF(TRIM(CAST(natural_ref AS CHAR)), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS sat_legal_person (
    legal_person_hash_key TEXT,
    load_date TEXT,
    record_source TEXT,
    party_ref TEXT,
    party_kind TEXT,
    tenant_cd TEXT,
    lead_ind TEXT,
    paperless_ind TEXT,
    src_party_ref TEXT,
    src_party_type TEXT,
    natural_ref TEXT,
    given_nm TEXT,
    family_nm TEXT,
    display_nm TEXT,
    title_txt TEXT,
    occupation_txt TEXT,
    dob TEXT,
    birth_yr TEXT,
    nationality_txt TEXT,
    gender_txt TEXT,
    marital_txt TEXT,
    disability_degree TEXT,
    language_pref TEXT,
    role_txt TEXT,
    job_title_txt TEXT,
    legal_name TEXT,
    legal_score_no TEXT,
    legal_status_txt TEXT,
    legal_job_title_txt TEXT,
    legal_src_ref TEXT,
    legal_src_type TEXT,
    constitution_dt TEXT,
    lead_conv_dt TEXT
);

TRUNCATE TABLE sat_legal_person;
INSERT INTO sat_legal_person (legal_person_hash_key, load_date, record_source, party_ref, party_kind, tenant_cd, lead_ind, paperless_ind, src_party_ref, src_party_type, natural_ref, given_nm, family_nm, display_nm, title_txt, occupation_txt, dob, birth_yr, nationality_txt, gender_txt, marital_txt, disability_degree, language_pref, role_txt, job_title_txt, legal_name, legal_score_no, legal_status_txt, legal_job_title_txt, legal_src_ref, legal_src_type, constitution_dt, lead_conv_dt)
SELECT DISTINCT
    MD5(CONCAT('CRM|', NULLIF(TRIM(CAST(legal_ref AS CHAR)), ''))) AS legal_person_hash_key,
    COALESCE(NULLIF(TRIM(CAST(pull_ts AS CHAR)), ''), CURRENT_TIMESTAMP) AS load_date,
    'CRM' AS record_source,
    party_ref AS party_ref,
    party_kind AS party_kind,
    tenant_cd AS tenant_cd,
    lead_ind AS lead_ind,
    paperless_ind AS paperless_ind,
    src_party_ref AS src_party_ref,
    src_party_type AS src_party_type,
    natural_ref AS natural_ref,
    given_nm AS given_nm,
    family_nm AS family_nm,
    display_nm AS display_nm,
    title_txt AS title_txt,
    occupation_txt AS occupation_txt,
    dob AS dob,
    birth_yr AS birth_yr,
    nationality_txt AS nationality_txt,
    gender_txt AS gender_txt,
    marital_txt AS marital_txt,
    disability_degree AS disability_degree,
    language_pref AS language_pref,
    role_txt AS role_txt,
    job_title_txt AS job_title_txt,
    legal_name AS legal_name,
    legal_score_no AS legal_score_no,
    legal_status_txt AS legal_status_txt,
    legal_job_title_txt AS legal_job_title_txt,
    legal_src_ref AS legal_src_ref,
    legal_src_type AS legal_src_type,
    constitution_dt AS constitution_dt,
    lead_conv_dt AS lead_conv_dt
FROM crm_party_master_csv
WHERE NULLIF(TRIM(CAST(legal_ref AS CHAR)), '') IS NOT NULL;
