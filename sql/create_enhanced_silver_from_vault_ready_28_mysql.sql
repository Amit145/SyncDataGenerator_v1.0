-- MySQL 8.x SQL to build the 80-table enhanced silver vault from the 28 flat vault_ready_28 bronze tables.
-- Assumptions:
--   1. The 28 bronze CSVs are loaded as tables using their file stems as table names.
--      Example: party_master.csv -> table `party_master`; enhanced_policy_relationships.csv -> table `enhanced_policy_relationships`.
--   2. Run this in the target silver schema/database, or add schema prefixes as needed.
--   3. MySQL MD5() is used to match the Python generator hash logic.
--   4. Adjust the three load-date variables if your run uses different vault load timestamps.

SET @hub_load_date  = '2025-06-01T10:51:38';
SET @link_load_date = '2025-06-06T10:51:38';
SET @sat_load_date  = '2025-06-11T10:51:38';
SET @record_source  = 'CRM';


DROP TABLE IF EXISTS `hub_person`;
CREATE TABLE `hub_person` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(p.`party_ref` AS CHAR), '')) AS `person_hash_key`,
    @hub_load_date AS `load_date`,
    COALESCE(NULLIF(p.`origin_sys`, ''), @record_source) AS `record_source`,
    p.`party_ref` AS `person_id`
FROM `party_master` p
WHERE COALESCE(CAST(p.`party_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_natural_person`;
CREATE TABLE `hub_natural_person` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(p.`natural_ref` AS CHAR), '')) AS `natural_person_hash_key`,
    @hub_load_date AS `load_date`,
    COALESCE(NULLIF(p.`origin_sys`, ''), @record_source) AS `record_source`,
    p.`natural_ref` AS `natural_person_id`
FROM `party_master` p
WHERE COALESCE(CAST(p.`natural_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_legal_person`;
CREATE TABLE `hub_legal_person` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(p.`legal_ref` AS CHAR), '')) AS `legal_person_hash_key`,
    @hub_load_date AS `load_date`,
    COALESCE(NULLIF(p.`origin_sys`, ''), @record_source) AS `record_source`,
    p.`legal_ref` AS `legal_person_id`
FROM `party_master` p
WHERE COALESCE(CAST(p.`legal_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_contact`;
CREATE TABLE `hub_contact` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(c.`contact_ref` AS CHAR), '')) AS `contact_hash_key`,
    @hub_load_date AS `load_date`,
    COALESCE(NULLIF(c.`origin_sys`, ''), @record_source) AS `record_source`,
    c.`contact_ref` AS `contact_id`
FROM `contact_point` c
WHERE COALESCE(CAST(c.`contact_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_identities`;
CREATE TABLE `hub_identities` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(i.`identity_ref` AS CHAR), '')) AS `identities_hash_key`,
    @hub_load_date AS `load_date`,
    COALESCE(NULLIF(i.`origin_sys`, ''), @record_source) AS `record_source`,
    i.`identity_ref` AS `identities_id`
FROM `identity_registry` i
WHERE COALESCE(CAST(i.`identity_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_lead`;
CREATE TABLE `hub_lead` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(l.`lead_ref` AS CHAR), '')) AS `lead_hash_key`,
    @hub_load_date AS `load_date`,
    COALESCE(NULLIF(l.`origin_sys`, ''), @record_source) AS `record_source`,
    l.`lead_ref` AS `lead_id`
FROM `lead_register` l
WHERE COALESCE(CAST(l.`lead_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_customer`;
CREATE TABLE `hub_customer` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(c.`customer_ref` AS CHAR), '')) AS `customer_hash_key`,
    @hub_load_date AS `load_date`,
    COALESCE(NULLIF(c.`origin_sys`, ''), @record_source) AS `record_source`,
    c.`customer_ref` AS `customer_id`
FROM `customer_portfolio` c
WHERE COALESCE(CAST(c.`customer_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_consent`;
CREATE TABLE `hub_consent` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(c.`consent_ref` AS CHAR), '')) AS `consent_hash_key`,
    @hub_load_date AS `load_date`,
    COALESCE(NULLIF(c.`origin_sys`, ''), @record_source) AS `record_source`,
    c.`consent_ref` AS `consent_id`
FROM `consent_snapshot` c
WHERE COALESCE(CAST(c.`consent_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_marketing_preference`;
CREATE TABLE `hub_marketing_preference` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(m.`preference_ref` AS CHAR), '')) AS `marketing_preference_hash_key`,
    @hub_load_date AS `load_date`,
    COALESCE(NULLIF(m.`origin_sys`, ''), @record_source) AS `record_source`,
    m.`preference_ref` AS `marketing_preference_id`
FROM `comm_preference` m
WHERE COALESCE(CAST(m.`preference_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_marketing_engagement`;
CREATE TABLE `hub_marketing_engagement` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(m.`engagement_ref` AS CHAR), '')) AS `marketing_engagement_hash_key`,
    @hub_load_date AS `load_date`,
    COALESCE(NULLIF(m.`origin_sys`, ''), @record_source) AS `record_source`,
    m.`engagement_ref` AS `marketing_engagement_id`
FROM `campaign_touch` m
WHERE COALESCE(CAST(m.`engagement_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_account`;
CREATE TABLE `hub_account` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(a.`account_ref` AS CHAR), '')) AS `account_hash_key`,
    @hub_load_date AS `load_date`,
    COALESCE(NULLIF(a.`origin_sys`, ''), @record_source) AS `record_source`,
    a.`account_ref` AS `account_id`
FROM `account_book` a
WHERE COALESCE(CAST(a.`account_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_product`;
CREATE TABLE `hub_product` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(p.`product_ref` AS CHAR), '')) AS `product_hash_key`,
    @hub_load_date AS `load_date`,
    COALESCE(NULLIF(p.`origin_sys`, ''), @record_source) AS `record_source`,
    p.`product_ref` AS `product_id`
FROM `product_catalog` p
WHERE COALESCE(CAST(p.`product_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_quote`;
CREATE TABLE `hub_quote` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(q.`quote_ref` AS CHAR), '')) AS `quote_hash_key`,
    @hub_load_date AS `load_date`,
    COALESCE(NULLIF(q.`origin_sys`, ''), @record_source) AS `record_source`,
    q.`quote_ref` AS `quote_id`
FROM `quote_register` q
WHERE COALESCE(CAST(q.`quote_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_policy`;
CREATE TABLE `hub_policy` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(p.`policy_ref` AS CHAR), '')) AS `policy_hash_key`,
    @hub_load_date AS `load_date`,
    COALESCE(NULLIF(p.`origin_sys`, ''), @record_source) AS `record_source`,
    p.`policy_ref` AS `policy_id`
FROM `policy_register` p
WHERE COALESCE(CAST(p.`policy_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_home`;
CREATE TABLE `hub_home` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(h.`property_ref` AS CHAR), '')) AS `home_hash_key`,
    @hub_load_date AS `load_date`,
    COALESCE(NULLIF(h.`origin_sys`, ''), @record_source) AS `record_source`,
    CAST(NULL AS CHAR) AS `insured_object_home_id`
FROM `property_asset` h
WHERE COALESCE(CAST(h.`property_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_motor`;
CREATE TABLE `hub_motor` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(m.`vehicle_ref` AS CHAR), '')) AS `motor_hash_key`,
    @hub_load_date AS `load_date`,
    COALESCE(NULLIF(m.`origin_sys`, ''), @record_source) AS `record_source`,
    CAST(NULL AS CHAR) AS `insured_object_motor_id`
FROM `vehicle_asset` m
WHERE COALESCE(CAST(m.`vehicle_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_person`;
CREATE TABLE `sat_person` AS
SELECT 
    MD5(COALESCE(CAST(p.`party_ref` AS CHAR), '')) AS `person_hash_key`,
    @sat_load_date AS `load_date`,
    p.`tenant_cd` AS `tenant_id`,
    p.`lead_ind` AS `is_lead`,
    p.`party_kind` AS `type`,
    p.`paperless_ind` AS `operational_paperless_consent`,
    p.`src_party_ref` AS `source_id`,
    p.`src_party_type` AS `source_type`
FROM `party_master` p
WHERE COALESCE(CAST(p.`party_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_natural_person`;
CREATE TABLE `sat_natural_person` AS
SELECT 
    MD5(COALESCE(CAST(p.`natural_ref` AS CHAR), '')) AS `natural_person_hash_key`,
    @sat_load_date AS `load_date`,
    p.`given_nm` AS `first_name`,
    p.`family_nm` AS `last_name`,
    p.`display_nm` AS `full_name`,
    p.`title_txt` AS `courtesy_title`,
    p.`role_txt` AS `role`,
    p.`occupation_txt` AS `occupation`,
    p.`dob` AS `birth_date`,
    p.`birth_yr` AS `birth_year`,
    p.`nationality_txt` AS `nationality`,
    p.`gender_txt` AS `gender`,
    p.`marital_txt` AS `marital_status`,
    p.`disability_degree` AS `assesed_disability_degree`,
    p.`language_pref` AS `preferred_language`,
    p.`job_title_txt` AS `job_title`
FROM `party_master` p
WHERE COALESCE(CAST(p.`natural_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_legal_person`;
CREATE TABLE `sat_legal_person` AS
SELECT 
    MD5(COALESCE(CAST(p.`legal_ref` AS CHAR), '')) AS `legal_person_hash_key`,
    @sat_load_date AS `load_date`,
    p.`legal_job_title_txt` AS `job_title`,
    p.`lead_conv_dt` AS `converted_date`,
    p.`legal_status_txt` AS `person_status`,
    p.`legal_score_no` AS `person_score`,
    p.`legal_name` AS `company_name`,
    p.`legal_src_type` AS `source_type`,
    p.`legal_src_ref` AS `source_id`,
    CASE WHEN p.`constitution_dt` IS NULL OR p.`constitution_dt` = '' THEN NULL WHEN p.`constitution_dt` LIKE '% %' OR p.`constitution_dt` LIKE '%T%' THEN p.`constitution_dt` ELSE CONCAT(p.`constitution_dt`, ' 00:00:00') END AS `date_of_constitution`
FROM `party_master` p
WHERE COALESCE(CAST(p.`legal_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_person_natural_person`;
CREATE TABLE `link_person_natural_person` AS
SELECT DISTINCT 
    MD5(CONCAT(MD5(COALESCE(CAST(p.`party_ref` AS CHAR), '')), '|', MD5(COALESCE(CAST(p.`natural_ref` AS CHAR), '')))) AS `person_natural_person_hash_key`,
    @link_load_date AS `load_date`,
    COALESCE(NULLIF(p.`origin_sys`, ''), @record_source) AS `record_source`,
    MD5(COALESCE(CAST(p.`party_ref` AS CHAR), '')) AS `person_hash_key`,
    MD5(COALESCE(CAST(p.`natural_ref` AS CHAR), '')) AS `natural_person_hash_key`
FROM `party_master` p
WHERE COALESCE(CAST(p.`party_ref` AS CHAR), '') <> '' AND COALESCE(CAST(p.`natural_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_person_legal_person`;
CREATE TABLE `link_person_legal_person` AS
SELECT DISTINCT 
    MD5(CONCAT(MD5(COALESCE(CAST(p.`party_ref` AS CHAR), '')), '|', MD5(COALESCE(CAST(p.`legal_ref` AS CHAR), '')))) AS `person_legal_person_hash_key`,
    @link_load_date AS `load_date`,
    COALESCE(NULLIF(p.`origin_sys`, ''), @record_source) AS `record_source`,
    MD5(COALESCE(CAST(p.`party_ref` AS CHAR), '')) AS `person_hash_key`,
    MD5(COALESCE(CAST(p.`legal_ref` AS CHAR), '')) AS `legal_person_hash_key`
FROM `party_master` p
WHERE COALESCE(CAST(p.`party_ref` AS CHAR), '') <> '' AND COALESCE(CAST(p.`legal_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_person_contact`;
CREATE TABLE `link_person_contact` AS
SELECT DISTINCT 
    MD5(CONCAT(MD5(COALESCE(CAST(c.`party_ref` AS CHAR), '')), '|', MD5(COALESCE(CAST(c.`contact_ref` AS CHAR), '')))) AS `person_contact_hash_key`,
    MD5(COALESCE(CAST(c.`party_ref` AS CHAR), '')) AS `person_hash_key`,
    MD5(COALESCE(CAST(c.`contact_ref` AS CHAR), '')) AS `contact_hash_key`,
    @link_load_date AS `load_date`,
    COALESCE(NULLIF(c.`origin_sys`, ''), @record_source) AS `record_source`
FROM `contact_point` c
WHERE COALESCE(CAST(c.`contact_ref` AS CHAR), '') <> '' AND COALESCE(CAST(c.`party_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_contact`;
CREATE TABLE `sat_contact` AS
SELECT 
    MD5(COALESCE(CAST(c.`contact_ref` AS CHAR), '')) AS `contact_hash_key`,
    @sat_load_date AS `load_date`,
    c.`email_home_txt` AS `personal_email`,
    c.`email_work_txt` AS `work_email`,
    c.`phone_work_txt` AS `work_phone`,
    c.`phone_home_txt` AS `home_phone`
FROM `contact_point` c
WHERE COALESCE(CAST(c.`contact_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_person_identities`;
CREATE TABLE `link_person_identities` AS
SELECT DISTINCT 
    MD5(CONCAT(MD5(COALESCE(CAST(i.`party_ref` AS CHAR), '')), '|', MD5(COALESCE(CAST(i.`identity_ref` AS CHAR), '')))) AS `person_identities_hash_key`,
    @link_load_date AS `load_date`,
    COALESCE(NULLIF(i.`origin_sys`, ''), @record_source) AS `record_source`,
    MD5(COALESCE(CAST(i.`party_ref` AS CHAR), '')) AS `person_hash_key`,
    MD5(COALESCE(CAST(i.`identity_ref` AS CHAR), '')) AS `identities_hash_key`
FROM `identity_registry` i
WHERE COALESCE(CAST(i.`identity_ref` AS CHAR), '') <> '' AND COALESCE(CAST(i.`party_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_identities`;
CREATE TABLE `sat_identities` AS
SELECT 
    MD5(COALESCE(CAST(i.`identity_ref` AS CHAR), '')) AS `identities_hash_key`,
    @sat_load_date AS `load_date`,
    i.`ecid_txt` AS `ecid`,
    i.`hashed_email_txt` AS `hashed_email`
FROM `identity_registry` i
WHERE COALESCE(CAST(i.`identity_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_person_lead`;
CREATE TABLE `link_person_lead` AS
SELECT DISTINCT 
    MD5(CONCAT(MD5(COALESCE(CAST(l.`party_ref` AS CHAR), '')), '|', MD5(COALESCE(CAST(l.`lead_ref` AS CHAR), '')))) AS `person_lead_hash_key`,
    @link_load_date AS `load_date`,
    COALESCE(NULLIF(l.`origin_sys`, ''), @record_source) AS `record_source`,
    MD5(COALESCE(CAST(l.`party_ref` AS CHAR), '')) AS `person_hash_key`,
    MD5(COALESCE(CAST(l.`lead_ref` AS CHAR), '')) AS `lead_hash_key`
FROM `lead_register` l
WHERE COALESCE(CAST(l.`lead_ref` AS CHAR), '') <> '' AND COALESCE(CAST(l.`party_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_lead`;
CREATE TABLE `sat_lead` AS
SELECT 
    MD5(COALESCE(CAST(l.`lead_ref` AS CHAR), '')) AS `lead_hash_key`,
    @sat_load_date AS `load_date`,
    l.`interest_bucket` AS `interested_level`,
    l.`contact_pref` AS `preferred_contact_method`,
    l.`person_score_no` AS `person_score`,
    l.`person_status_txt` AS `person_status`,
    l.`converted_dt` AS `converted_date`
FROM `lead_register` l
WHERE COALESCE(CAST(l.`lead_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_person_consent`;
CREATE TABLE `link_person_consent` AS
SELECT DISTINCT 
    MD5(CONCAT(MD5(COALESCE(CAST(c.`party_ref` AS CHAR), '')), '|', MD5(COALESCE(CAST(c.`consent_ref` AS CHAR), '')))) AS `person_consent_hash_key`,
    @link_load_date AS `load_date`,
    COALESCE(NULLIF(c.`origin_sys`, ''), @record_source) AS `record_source`,
    MD5(COALESCE(CAST(c.`party_ref` AS CHAR), '')) AS `person_hash_key`,
    MD5(COALESCE(CAST(c.`consent_ref` AS CHAR), '')) AS `consent_hash_key`
FROM `consent_snapshot` c
WHERE COALESCE(CAST(c.`consent_ref` AS CHAR), '') <> '' AND COALESCE(CAST(c.`party_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_consent`;
CREATE TABLE `sat_consent` AS
SELECT 
    MD5(COALESCE(CAST(c.`consent_ref` AS CHAR), '')) AS `consent_hash_key`,
    @sat_load_date AS `load_date`,
    c.`opt_in_valid_ind` AS `opt_in_validated`,
    c.`opt_in_legit_ind` AS `opt_in_legitimate_interest`
FROM `consent_snapshot` c
WHERE COALESCE(CAST(c.`consent_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_person_marketing_preference`;
CREATE TABLE `link_person_marketing_preference` AS
SELECT DISTINCT 
    MD5(CONCAT(MD5(COALESCE(CAST(m.`party_ref` AS CHAR), '')), '|', MD5(COALESCE(CAST(m.`preference_ref` AS CHAR), '')))) AS `person_marketing_preference_hash_key`,
    @link_load_date AS `load_date`,
    COALESCE(NULLIF(m.`origin_sys`, ''), @record_source) AS `record_source`,
    MD5(COALESCE(CAST(m.`party_ref` AS CHAR), '')) AS `person_hash_key`,
    MD5(COALESCE(CAST(m.`preference_ref` AS CHAR), '')) AS `marketing_preference_hash_key`
FROM `comm_preference` m
WHERE COALESCE(CAST(m.`preference_ref` AS CHAR), '') <> '' AND COALESCE(CAST(m.`party_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_marketing_preference`;
CREATE TABLE `sat_marketing_preference` AS
SELECT 
    MD5(COALESCE(CAST(m.`preference_ref` AS CHAR), '')) AS `marketing_preference_hash_key`,
    @sat_load_date AS `load_date`,
    m.`sms_ind` AS `sms`,
    m.`email_ind` AS `email`,
    m.`email_sub_ind` AS `email_subscriptions`,
    m.`call_ind` AS `call`,
    m.`any_ind` AS `any`,
    m.`commercial_email_ind` AS `commercial_email`,
    m.`postal_mail_ind` AS `postal_mail`
FROM `comm_preference` m
WHERE COALESCE(CAST(m.`preference_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_person_marketing_engagement`;
CREATE TABLE `link_person_marketing_engagement` AS
SELECT DISTINCT 
    MD5(CONCAT(MD5(COALESCE(CAST(m.`party_ref` AS CHAR), '')), '|', MD5(COALESCE(CAST(m.`engagement_ref` AS CHAR), '')))) AS `person_marketing_engagement_hash_key`,
    @link_load_date AS `load_date`,
    COALESCE(NULLIF(m.`origin_sys`, ''), @record_source) AS `record_source`,
    MD5(COALESCE(CAST(m.`party_ref` AS CHAR), '')) AS `person_hash_key`,
    MD5(COALESCE(CAST(m.`engagement_ref` AS CHAR), '')) AS `marketing_engagement_hash_key`
FROM `campaign_touch` m
WHERE COALESCE(CAST(m.`engagement_ref` AS CHAR), '') <> '' AND COALESCE(CAST(m.`party_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_marketing_engagement`;
CREATE TABLE `sat_marketing_engagement` AS
SELECT 
    MD5(COALESCE(CAST(m.`engagement_ref` AS CHAR), '')) AS `marketing_engagement_hash_key`,
    @sat_load_date AS `load_date`,
    m.`promo_cd` AS `promotion_code`,
    m.`email_opened_ind` AS `opened_email`,
    m.`campaign_status_txt` AS `marketing_status`
FROM `campaign_touch` m
WHERE COALESCE(CAST(m.`engagement_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_person_account`;
CREATE TABLE `link_person_account` AS
SELECT DISTINCT 
    MD5(CONCAT(MD5(COALESCE(CAST(a.`party_ref` AS CHAR), '')), '|', MD5(COALESCE(CAST(a.`account_ref` AS CHAR), '')))) AS `person_account_hash_key`,
    COALESCE(NULLIF(a.`origin_sys`, ''), @record_source) AS `record_source`,
    @link_load_date AS `load_date`,
    MD5(COALESCE(CAST(a.`party_ref` AS CHAR), '')) AS `person_hash_key`,
    MD5(COALESCE(CAST(a.`account_ref` AS CHAR), '')) AS `account_hash_key`
FROM `account_book` a
WHERE COALESCE(CAST(a.`account_ref` AS CHAR), '') <> '' AND COALESCE(CAST(a.`party_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_account`;
CREATE TABLE `sat_account` AS
SELECT 
    MD5(COALESCE(CAST(a.`account_ref` AS CHAR), '')) AS `account_hash_key`,
    @sat_load_date AS `load_date`,
    a.`account_no` AS `account_number`,
    a.`account_type_txt` AS `account_type`,
    a.`last_access_dt` AS `account_last_access`,
    a.`last_change_dt` AS `account_last_change`,
    a.`account_create_type_txt` AS `account_creation_type`,
    a.`account_status_txt` AS `account_status`
FROM `account_book` a
WHERE COALESCE(CAST(a.`account_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_customer_person`;
CREATE TABLE `link_customer_person` AS
SELECT DISTINCT 
    MD5(CONCAT(MD5(COALESCE(CAST(c.`customer_ref` AS CHAR), '')), '|', MD5(COALESCE(CAST(c.`party_ref` AS CHAR), '')))) AS `customer_person_hash_key`,
    @link_load_date AS `load_date`,
    COALESCE(NULLIF(c.`origin_sys`, ''), @record_source) AS `record_source`,
    MD5(COALESCE(CAST(c.`customer_ref` AS CHAR), '')) AS `customer_hash_key`,
    MD5(COALESCE(CAST(c.`party_ref` AS CHAR), '')) AS `person_hash_key`
FROM `customer_portfolio` c
WHERE COALESCE(CAST(c.`customer_ref` AS CHAR), '') <> '' AND COALESCE(CAST(c.`party_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_customer`;
CREATE TABLE `sat_customer` AS
SELECT 
    MD5(COALESCE(CAST(c.`customer_ref` AS CHAR), '')) AS `customer_hash_key`,
    @sat_load_date AS `load_date`,
    COALESCE(NULLIF(e.`src_customer_number`,''), c.`customer_no`) AS `customer_number`,
    COALESCE(NULLIF(e.`src_customer_status`,''), c.`customer_status_txt`) AS `customer_status`,
    COALESCE(NULLIF(e.`src_customer_status_reason`,''), c.`customer_status_reason_txt`) AS `customer_status_reason`,
    COALESCE(NULLIF(e.`src_customer_since`,''), c.`customer_since_dt`) AS `customer_since`,
    COALESCE(NULLIF(e.`src_customer_rating`,''), c.`customer_rating_no`) AS `customer_rating`,
    COALESCE(NULLIF(e.`src_customer_segment`,''), c.`customer_segment_txt`) AS `customer_segment`,
    COALESCE(NULLIF(e.`src_line_of_business`,''), c.`lob_txt`) AS `line_of_business`,
    COALESCE(NULLIF(e.`src_nps_score`,''), c.`nps_score_no`) AS `nps_score`,
    e.`src_income_band` AS `income_band`,
    e.`src_customer_satisfaction` AS `customer_satisfaction`,
    e.`src_customer_age_band` AS `customer_age_band`,
    e.`src_net_promotor_code_segment` AS `net_promotor_code_segment`
FROM `customer_portfolio` c
LEFT JOIN `enhanced_enrichments` e ON e.`source_extract` = 'customer_enrichment.csv' AND MD5(COALESCE(CAST(e.`src_customer_ref` AS CHAR), '')) = MD5(COALESCE(CAST(c.`customer_ref` AS CHAR), ''))
WHERE COALESCE(CAST(c.`customer_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_customer_lead`;
CREATE TABLE `link_customer_lead` AS
SELECT DISTINCT 
    MD5(CONCAT(MD5(COALESCE(CAST(cl.`customer_ref` AS CHAR), '')), '|', MD5(COALESCE(CAST(cl.`lead_ref` AS CHAR), '')))) AS `customer_lead_hash_key`,
    @link_load_date AS `load_date`,
    COALESCE(NULLIF(cl.`origin_sys`, ''), @record_source) AS `record_source`,
    MD5(COALESCE(CAST(cl.`lead_ref` AS CHAR), '')) AS `lead_hash_key`,
    MD5(COALESCE(CAST(cl.`customer_ref` AS CHAR), '')) AS `customer_hash_key`
FROM `customer_lead_bridge` cl
WHERE COALESCE(CAST(cl.`customer_ref` AS CHAR), '') <> '' AND COALESCE(CAST(cl.`lead_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_product`;
CREATE TABLE `sat_product` AS
SELECT 
    @sat_load_date AS `load_date`,
    MD5(COALESCE(CAST(p.`product_ref` AS CHAR), '')) AS `product_hash_key`,
    p.`product_line` AS `type`,
    CAST(NULL AS CHAR) AS `product_variant`,
    CAST(NULL AS CHAR) AS `product_name`,
    CAST(NULL AS CHAR) AS `product_launch_date`,
    CAST(NULL AS CHAR) AS `product_status`,
    CAST(NULL AS CHAR) AS `product_line_of_business_code`,
    CAST(NULL AS CHAR) AS `underwriting_group`,
    CAST(NULL AS CHAR) AS `regulatory_approval_code`
FROM `product_catalog` p
WHERE COALESCE(CAST(p.`product_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_quote`;
CREATE TABLE `sat_quote` AS
SELECT 
    MD5(COALESCE(CAST(q.`quote_ref` AS CHAR), '')) AS `quote_hash_key`,
    @sat_load_date AS `load_date`,
    q.`gross_amt` AS `gross_revenue`,
    q.`net_amt` AS `net_revenue`,
    q.`quote_no` AS `quote_number`,
    q.`quote_status_txt` AS `quote_status`,
    q.`renewal_amt_curr` AS `renewal_amt_current_period`,
    q.`renewal_amt_next` AS `renewal_amt_next_period`,
    CAST(NULL AS CHAR) AS `quoted_premium`,
    CAST(NULL AS CHAR) AS `quote_date`,
    CAST(NULL AS CHAR) AS `quote_month_name`,
    CAST(NULL AS CHAR) AS `risk_score`,
    CAST(NULL AS CHAR) AS `policy_complexity`,
    CAST(NULL AS CHAR) AS `uw_approval_type`,
    CAST(NULL AS CHAR) AS `rejection_reason`
FROM `quote_register` q
WHERE COALESCE(CAST(q.`quote_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_quote_person`;
CREATE TABLE `link_quote_person` AS
SELECT DISTINCT 
    MD5(CONCAT(MD5(COALESCE(CAST(q.`quote_ref` AS CHAR), '')), '|', MD5(COALESCE(CAST(q.`party_ref` AS CHAR), '')))) AS `quote_person_hash_key`,
    @link_load_date AS `load_date`,
    COALESCE(NULLIF(q.`origin_sys`, ''), @record_source) AS `record_source`,
    MD5(COALESCE(CAST(q.`quote_ref` AS CHAR), '')) AS `quote_hash_key`,
    MD5(COALESCE(CAST(q.`party_ref` AS CHAR), '')) AS `person_hash_key`
FROM `quote_register` q
WHERE COALESCE(CAST(q.`quote_ref` AS CHAR), '') <> '' AND COALESCE(CAST(q.`party_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_quote_product`;
CREATE TABLE `link_quote_product` AS
SELECT DISTINCT 
    MD5(CONCAT(MD5(COALESCE(CAST(q.`quote_ref` AS CHAR), '')), '|', MD5(COALESCE(CAST(q.`product_ref` AS CHAR), '')))) AS `quote_product_hash_key`,
    @link_load_date AS `load_date`,
    COALESCE(NULLIF(q.`origin_sys`, ''), @record_source) AS `record_source`,
    MD5(COALESCE(CAST(q.`quote_ref` AS CHAR), '')) AS `quote_hash_key`,
    MD5(COALESCE(CAST(q.`product_ref` AS CHAR), '')) AS `product_hash_key`
FROM `quote_register` q
WHERE COALESCE(CAST(q.`quote_ref` AS CHAR), '') <> '' AND COALESCE(CAST(q.`product_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_policy`;
CREATE TABLE `sat_policy` AS
SELECT 
    MD5(COALESCE(CAST(p.`policy_ref` AS CHAR), '')) AS `policy_hash_key`,
    @sat_load_date AS `load_date`,
    COALESCE(NULLIF(e.`src_cover_option`, ''), p.`cover_option_txt`) AS `cover_option`,
    COALESCE(NULLIF(e.`src_declined_claims`, ''), p.`declined_claim_cnt`) AS `declined_claims`,
    COALESCE(NULLIF(e.`src_fraud_flag`, ''), p.`fraud_ind`) AS `fraud_flag`,
    COALESCE(NULLIF(e.`src_gross_revenue`, ''), p.`gross_amt`) AS `gross_revenue`,
    COALESCE(NULLIF(e.`src_net_revenue`, ''), p.`net_amt`) AS `net_revenue`,
    COALESCE(NULLIF(e.`src_number_of_active_claim`, ''), p.`active_claim_cnt`) AS `number_of_active_claim`,
    COALESCE(NULLIF(e.`src_number_of_previous_claim`, ''), p.`previous_claim_cnt`) AS `number_of_previous_claim`,
    COALESCE(NULLIF(e.`src_policy_cycle`, ''), p.`policy_cycle_no`) AS `policy_cycle`,
    COALESCE(NULLIF(e.`src_policy_end_date`, ''), p.`policy_end_dt`) AS `policy_end_date`,
    COALESCE(NULLIF(e.`src_policy_length`, ''), p.`policy_term_months`) AS `policy_length`,
    COALESCE(NULLIF(e.`src_policy_number`, ''), p.`policy_no`) AS `policy_number`,
    COALESCE(NULLIF(e.`src_policy_start_date`, ''), p.`policy_start_dt`) AS `policy_start_date`,
    COALESCE(NULLIF(e.`src_policy_status`, ''), p.`policy_status_txt`) AS `policy_status`,
    COALESCE(NULLIF(e.`src_renewal_amount_current_period`, ''), p.`renewal_premium_curr`) AS `renewal_amount_current_period`,
    COALESCE(NULLIF(e.`src_renewal_amount_next_period`, ''), p.`renewal_premium_next`) AS `renewal_amount_next_period`,
    COALESCE(NULLIF(e.`src_renewal_date`, ''), p.`renewal_dt`) AS `renewal_date`,
    COALESCE(NULLIF(e.`src_sales_channel`, ''), p.`sales_channel_txt`) AS `sales_channel`,
    e.`src_quote_id` AS `quote_id`,
    e.`src_policy_type` AS `policy_type`,
    e.`src_policy_issue_date` AS `policy_issue_date`,
    e.`src_is_policy_renewal` AS `is_policy_renewal`,
    e.`src_policy_cancellation_reason` AS `policy_cancellation_reason`,
    e.`src_policy_sum_insured` AS `policy_sum_insured`,
    e.`src_policy_retention_limit` AS `policy_retention_limit`,
    e.`src_policy_risk_score` AS `policy_risk_score`,
    e.`src_policy_risk_band` AS `policy_risk_band`,
    e.`src_policy_base_premium` AS `policy_base_premium`,
    e.`src_gross_written_premium` AS `gross_written_premium`,
    e.`src_earned_premium` AS `earned_premium`,
    e.`src_incurred_but_not_reported` AS `incurred_but_not_reported`,
    e.`src_operating_expenses` AS `operating_expenses`,
    e.`src_administrative_expenses` AS `administrative_expenses`,
    e.`src_profit_margin` AS `profit_margin`,
    e.`src_taxes_and_levies` AS `taxes_and_levies`,
    e.`src_amount_approved` AS `amount_approved`,
    e.`src_ceded_premium` AS `ceded_premium`,
    e.`src_commission_paid` AS `commission_paid`,
    e.`src_ceded_commission` AS `ceded_commission`,
    e.`src_exposure_amount` AS `exposure_amount`,
    e.`src_investment_income` AS `investment_income`,
    e.`src_underwriting_cycle_time_in_days` AS `underwriting_cycle_time_in_days`,
    e.`src_underwriting_expenses` AS `underwriting_expenses`,
    e.`src_transaction_date` AS `transaction_date`,
    e.`src_record_type` AS `record_type`,
    e.`src_discount` AS `discount`,
    e.`src_override_commission` AS `override_commission`,
    e.`src_partial_recovery_percentage` AS `partial_recovery_percentage`
FROM `policy_register` p
LEFT JOIN `enhanced_enrichments` e ON e.`source_extract` = 'policy_enrichment.csv' AND MD5(COALESCE(CAST(e.`src_policy_ref` AS CHAR), '')) = MD5(COALESCE(CAST(p.`policy_ref` AS CHAR), ''))
WHERE COALESCE(CAST(p.`policy_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_policy_customer`;
CREATE TABLE `link_policy_customer` AS
SELECT DISTINCT 
    MD5(CONCAT(MD5(COALESCE(CAST(p.`policy_ref` AS CHAR), '')), '|', MD5(COALESCE(CAST(p.`customer_ref` AS CHAR), '')))) AS `policy_customer_hash_key`,
    @link_load_date AS `load_date`,
    COALESCE(NULLIF(p.`origin_sys`, ''), @record_source) AS `record_source`,
    MD5(COALESCE(CAST(p.`customer_ref` AS CHAR), '')) AS `customer_hash_key`,
    MD5(COALESCE(CAST(p.`policy_ref` AS CHAR), '')) AS `policy_hash_key`
FROM `policy_register` p
WHERE COALESCE(CAST(p.`policy_ref` AS CHAR), '') <> '' AND COALESCE(CAST(p.`customer_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_policy_product`;
CREATE TABLE `link_policy_product` AS
SELECT DISTINCT 
    MD5(CONCAT(MD5(COALESCE(CAST(p.`policy_ref` AS CHAR), '')), '|', MD5(COALESCE(CAST(p.`product_ref` AS CHAR), '')))) AS `policy_customer_hash_key`,
    @link_load_date AS `load_date`,
    COALESCE(NULLIF(p.`origin_sys`, ''), @record_source) AS `record_source`,
    MD5(COALESCE(CAST(p.`policy_ref` AS CHAR), '')) AS `policy_hash_key`,
    MD5(COALESCE(CAST(p.`product_ref` AS CHAR), '')) AS `product_hash_key`
FROM `policy_register` p
WHERE COALESCE(CAST(p.`policy_ref` AS CHAR), '') <> '' AND COALESCE(CAST(p.`product_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_home`;
CREATE TABLE `sat_home` AS
SELECT 
    MD5(COALESCE(CAST(h.`property_ref` AS CHAR), '')) AS `home_hash_key`,
    @sat_load_date AS `load_date`,
    h.`wall_material_txt` AS `wall_construction`,
    h.`risk_address_txt` AS `home_risk_address`,
    h.`roof_material_txt` AS `roof_construction`,
    h.`property_type_txt` AS `home_type`,
    h.`property_state_cd` AS `home_state`,
    h.`existing_home_ind` AS `is_existing_home_customer`
FROM `property_asset` h
WHERE COALESCE(CAST(h.`property_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_motor`;
CREATE TABLE `sat_motor` AS
SELECT 
    MD5(COALESCE(CAST(m.`vehicle_ref` AS CHAR), '')) AS `motor_hash_key`,
    @sat_load_date AS `load_date`,
    COALESCE(NULLIF(e.`src_auto_decline_vehicle`, ''), m.`auto_decline_ind`) AS `auto_decline_vehicle`,
    COALESCE(NULLIF(e.`src_body_type`, ''), m.`body_style_txt`) AS `body_type`,
    COALESCE(NULLIF(e.`src_fuel_type`, ''), m.`fuel_type_txt`) AS `fuel_type`,
    COALESCE(NULLIF(e.`src_license_status`, ''), m.`license_status_txt`) AS `license_status`,
    COALESCE(NULLIF(e.`src_is_existing_motor_customer`, ''), m.`existing_motor_ind`) AS `is_existing_motor_customer`,
    COALESCE(NULLIF(e.`src_motor_lapsed_policies`, ''), m.`motor_lapse_cnt`) AS `motor_lapsed_policies`,
    COALESCE(NULLIF(e.`src_motor_risk_address`, ''), m.`garage_address_txt`) AS `motor_risk_address`,
    COALESCE(NULLIF(e.`src_risk_class_code`, ''), m.`risk_class_cd`) AS `risk_class_code`,
    m.`variant_nm` AS `variant`,
    COALESCE(NULLIF(e.`src_vehicle_owner_type`, ''), m.`owner_type_txt`) AS `vehicle_owner_type`,
    COALESCE(NULLIF(e.`src_vehicle_regstate`, ''), m.`registration_state_cd`) AS `vehicle_regstate`,
    COALESCE(NULLIF(e.`src_vehicle_class`, ''), m.`vehicle_class_txt`) AS `vehicle_class`,
    COALESCE(NULLIF(e.`src_vehicle_model`, ''), m.`model_nm`) AS `vehicle_model`,
    COALESCE(NULLIF(e.`src_vehicle_type`, ''), m.`vehicle_type_txt`) AS `vehicle_type`,
    COALESCE(NULLIF(e.`src_motor_sum_insrd`, ''), m.`insured_value_amt`) AS `motor_sum_insrd`,
    COALESCE(NULLIF(e.`src_vehicle_year`, ''), m.`manufacture_yr`) AS `vehicle_year`,
    COALESCE(NULLIF(e.`src_vehicle_age`, ''), m.`vehicle_age_yrs`) AS `vehicle_age`
FROM `vehicle_asset` m
LEFT JOIN `enhanced_enrichments` e ON e.`source_extract` = 'motor_enrichment.csv' AND MD5(COALESCE(CAST(e.`src_motor_ref` AS CHAR), '')) = MD5(COALESCE(CAST(m.`vehicle_ref` AS CHAR), ''))
WHERE COALESCE(CAST(m.`vehicle_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_address`;
CREATE TABLE `hub_address` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(s.`src_address_ref` AS CHAR), '')) AS `address_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    s.`src_address_id` AS `address_id`
FROM `enhanced_address_book` s
WHERE COALESCE(CAST(s.`src_address_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_address`;
CREATE TABLE `sat_address` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(s.`src_address_ref` AS CHAR), '')) AS `address_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_street` AS `street`,
    s.`src_postcode` AS `postcode`,
    s.`src_city` AS `city`,
    s.`src_state` AS `state`,
    s.`src_country` AS `country`,
    s.`src_type` AS `type`
FROM `enhanced_address_book` s
WHERE COALESCE(CAST(s.`src_address_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_broker`;
CREATE TABLE `hub_broker` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(s.`src_broker_ref` AS CHAR), '')) AS `broker_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    s.`src_agent_id` AS `agent_id`
FROM `broker_book` s
WHERE COALESCE(CAST(s.`src_broker_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_broker`;
CREATE TABLE `sat_broker` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(s.`src_broker_ref` AS CHAR), '')) AS `broker_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_agent_name` AS `agent_name`,
    s.`src_agent_type` AS `agent_type`,
    s.`src_agent_status` AS `agent_status`,
    s.`src_agent_license_number` AS `agent_license_number`,
    s.`src_agent_net_promoter_score` AS `agent_net_promoter_score`,
    s.`src_agent_commission_percentage` AS `agent_commission_percentage`
FROM `broker_book` s
WHERE COALESCE(CAST(s.`src_broker_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_campaign`;
CREATE TABLE `hub_campaign` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(s.`src_campaign_ref` AS CHAR), '')) AS `campaign_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    s.`src_campaign_id` AS `campaign_id`
FROM `campaign_register` s
WHERE COALESCE(CAST(s.`src_campaign_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_campaign`;
CREATE TABLE `sat_campaign` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(s.`src_campaign_ref` AS CHAR), '')) AS `campaign_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_campaign_name` AS `campaign_name`,
    s.`src_campaign_type` AS `campaign_type`,
    s.`src_campaign_start_date` AS `campaign_start_date`,
    s.`src_campaign_end_date` AS `campaign_end_date`,
    s.`src_campaign_status` AS `campaign_status`,
    s.`src_campaign_budget` AS `campaign_budget`,
    s.`src_campaign_target_audience` AS `campaign_target_audience`,
    s.`src_campaign_marketing_source` AS `campaign_marketing_source`,
    s.`src_campaign_owner_department` AS `campaign_owner_department`,
    s.`src_campaign_country` AS `campaign_country`,
    s.`src_campaign_conversion_goal` AS `campaign_conversion_goal`,
    s.`src_number_of_impressions` AS `number_of_impressions`,
    s.`src_number_of_clicks` AS `number_of_clicks`,
    s.`src_is_active` AS `is_active`,
    s.`src_number_of_visits` AS `number_of_visits`,
    s.`src_number_of_policy_purchases` AS `number_of_policy_purchases`,
    s.`src_number_of_emails_sent` AS `number_of_emails_sent`,
    s.`src_number_of_email_bounced` AS `number_of_email_bounced`,
    s.`src_number_of_emails_delivered` AS `number_of_emails_delivered`,
    s.`src_number_of_emails_opened` AS `number_of_emails_opened`,
    s.`src_click_through_rate` AS `click_through_rate`,
    s.`src_spend_amount` AS `spend_amount`,
    s.`src_incremental_revenue` AS `incremental_revenue`,
    s.`src_survey_wave` AS `survey_wave`,
    s.`src_total_number_of_respondents` AS `total_number_of_respondents`,
    s.`src_number_of_respondents_aware` AS `number_of_respondents_aware`,
    s.`src_number_of_promoters` AS `number_of_promoters`,
    s.`src_number_of_passives` AS `number_of_passives`,
    s.`src_number_of_detractors` AS `number_of_detractors`,
    s.`src_number_of_followers` AS `number_of_followers`,
    s.`src_number_of_likes` AS `number_of_likes`,
    s.`src_number_of_comments` AS `number_of_comments`,
    s.`src_number_of_shares` AS `number_of_shares`,
    s.`src_number_of_brand_mentions` AS `number_of_brand_mentions`,
    s.`src_number_of_category_mentions` AS `number_of_category_mentions`
FROM `campaign_register` s
WHERE COALESCE(CAST(s.`src_campaign_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_channel`;
CREATE TABLE `hub_channel` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(s.`src_channel_ref` AS CHAR), '')) AS `channel_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    s.`src_channel_id` AS `channel_id`
FROM `channel_catalog` s
WHERE COALESCE(CAST(s.`src_channel_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_channel`;
CREATE TABLE `sat_channel` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(s.`src_channel_ref` AS CHAR), '')) AS `channel_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_channel_name` AS `channel_name`,
    s.`src_channel_type` AS `channel_type`
FROM `channel_catalog` s
WHERE COALESCE(CAST(s.`src_channel_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_claim`;
CREATE TABLE `hub_claim` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(s.`src_claim_ref` AS CHAR), '')) AS `claim_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    s.`src_claim_id` AS `claim_id`
FROM `claim_register` s
WHERE COALESCE(CAST(s.`src_claim_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_claim`;
CREATE TABLE `sat_claim` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(s.`src_claim_ref` AS CHAR), '')) AS `claim_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_claim_number` AS `claim_number`,
    s.`src_claim_type` AS `claim_type`,
    s.`src_claim_status` AS `claim_status`,
    s.`src_claim_reason` AS `claim_reason`,
    s.`src_claim_channel` AS `claim_channel`,
    s.`src_claim_handler` AS `claim_handler`,
    s.`src_claim_reported_date` AS `claim_reported_date`,
    s.`src_claim_settlement_date` AS `claim_settlement_date`,
    s.`src_claim_product` AS `claim_product`,
    s.`src_is_claim_suspicious` AS `is_claim_suspicious`,
    s.`src_is_claim_fraud` AS `is_claim_fraud`,
    s.`src_claim_fraud_status` AS `claim_fraud_status`,
    s.`src_claim_fraud_type` AS `claim_fraud_type`,
    s.`src_claim_fraud_detection_method` AS `claim_fraud_detection_method`,
    s.`src_is_litigation` AS `is_litigation`,
    s.`src_litigation_reason` AS `litigation_reason`,
    s.`src_litigation_start_date` AS `litigation_start_date`,
    s.`src_litigation_end_date` AS `litigation_end_date`,
    s.`src_litigation_outcome` AS `litigation_outcome`,
    s.`src_litigation_duration_days` AS `litigation_duration_days`,
    s.`src_claim_fraud_detection_time_in_days` AS `claim_fraud_detection_time_in_days`,
    s.`src_is_recovery_opportunity` AS `is_recovery_opportunity`,
    s.`src_recovery_priority_score` AS `recovery_priority_score`,
    s.`src_recovery_category` AS `recovery_category`,
    s.`src_recovery_source` AS `recovery_source`,
    s.`src_first_recovery_date` AS `first_recovery_date`,
    s.`src_last_recovery_date` AS `last_recovery_date`,
    s.`src_is_recovery_happened` AS `is_recovery_happened`,
    s.`src_days_to_first_recovery` AS `days_to_first_recovery`,
    s.`src_days_to_last_recovery` AS `days_to_last_recovery`,
    s.`src_avg_days_to_close_claim` AS `avg_days_to_close_claim`,
    s.`src_claim_fraud_outcome` AS `claim_fraud_outcome`,
    s.`src_recovery_type` AS `recovery_type`,
    s.`src_recovery_band` AS `recovery_band`,
    s.`src_third_party_involved` AS `third_party_involved`,
    s.`src_third_party_involved_overall_score` AS `third_party_involved_overall_score`,
    s.`src_solicitor` AS `solicitor`,
    s.`src_claim_amount` AS `claim_amount`,
    s.`src_claims_paid` AS `claims_paid`,
    s.`src_outstanding_reserve` AS `outstanding_reserve`,
    s.`src_claims_expenses` AS `claims_expenses`,
    s.`src_recovery_received` AS `recovery_received`,
    s.`src_compensation_offered` AS `compensation_offered`,
    s.`src_remediation_amount` AS `remediation_amount`,
    s.`src_suspectd_amount` AS `suspectd_amount`,
    s.`src_fraud_amount` AS `fraud_amount`,
    s.`src_legal_expenses` AS `legal_expenses`,
    s.`src_claim_band` AS `claim_band`,
    s.`src_claim_band_sort` AS `claim_band_sort`
FROM `claim_register` s
WHERE COALESCE(CAST(s.`src_claim_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_complaint`;
CREATE TABLE `hub_complaint` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(s.`src_complaint_ref` AS CHAR), '')) AS `complaint_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    s.`src_complaint_id` AS `complaint_id`
FROM `complaint_register` s
WHERE COALESCE(CAST(s.`src_complaint_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_complaint`;
CREATE TABLE `sat_complaint` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(s.`src_complaint_ref` AS CHAR), '')) AS `complaint_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_complaint_date` AS `complaint_date`,
    s.`src_complaint_acknowledgement_date` AS `complaint_acknowledgement_date`,
    s.`src_complaint_resolved_date` AS `complaint_resolved_date`,
    s.`src_complaint_upheld_status` AS `complaint_upheld_status`,
    s.`src_is_financial_ombudsman_service_referral` AS `is_financial_ombudsman_service_referral`,
    s.`src_complaint_driver` AS `complaint_driver`,
    s.`src_complaint_channel` AS `complaint_channel`,
    s.`src_compensation_amount` AS `compensation_amount`,
    s.`src_insurance_category` AS `insurance_category`,
    s.`src_complaint_status` AS `complaint_status`
FROM `complaint_register` s
WHERE COALESCE(CAST(s.`src_complaint_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_insured_object`;
CREATE TABLE `hub_insured_object` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(s.`src_insured_object_ref` AS CHAR), '')) AS `insured_object_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    s.`src_insured_object_id` AS `insured_object_id`
FROM `insured_object_register` s
WHERE COALESCE(CAST(s.`src_insured_object_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_insured_object`;
CREATE TABLE `sat_insured_object` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(s.`src_insured_object_ref` AS CHAR), '')) AS `insured_object_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_insured_object_type` AS `insured_object_type`,
    s.`src_insured_object_sub_type` AS `insured_object_sub_type`,
    s.`src_insured_object_description` AS `insured_object_description`,
    s.`src_insured_value` AS `insured_value`,
    s.`src_currency_code` AS `currency_code`,
    s.`src_insured_object_start_date` AS `insured_object_start_date`,
    s.`src_insured_object_end_date` AS `insured_object_end_date`,
    s.`src_insured_object_current_status` AS `insured_object_current_status`
FROM `insured_object_register` s
WHERE COALESCE(CAST(s.`src_insured_object_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_override`;
CREATE TABLE `hub_override` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(s.`src_override_ref` AS CHAR), '')) AS `override_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    s.`src_override_id` AS `override_id`
FROM `override_register` s
WHERE COALESCE(CAST(s.`src_override_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_override`;
CREATE TABLE `sat_override` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(s.`src_override_ref` AS CHAR), '')) AS `override_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_override_reason` AS `override_reason`
FROM `override_register` s
WHERE COALESCE(CAST(s.`src_override_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `hub_regulation`;
CREATE TABLE `hub_regulation` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(s.`src_regulation_ref` AS CHAR), '')) AS `regulation_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    s.`src_regulation_id` AS `regulation_id`
FROM `regulation_register` s
WHERE COALESCE(CAST(s.`src_regulation_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `sat_regulation`;
CREATE TABLE `sat_regulation` AS
SELECT DISTINCT 
    MD5(COALESCE(CAST(s.`src_regulation_ref` AS CHAR), '')) AS `regulation_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_regulation_number` AS `regulation_number`,
    s.`src_regulation_name` AS `regulation_name`,
    s.`src_regulation_department` AS `regulation_department`,
    s.`src_regulation_region` AS `regulation_region`,
    s.`src_regulation_risk_level` AS `regulation_risk_level`,
    s.`src_regulation_compliance_status` AS `regulation_compliance_status`,
    s.`src_regulation_date_raised` AS `regulation_date_raised`,
    s.`src_regulation_date_closed` AS `regulation_date_closed`,
    s.`src_regulation_owner` AS `regulation_owner`,
    s.`src_regulation_deadline_date` AS `regulation_deadline_date`,
    s.`src_is_regulation_on_time` AS `is_regulation_on_time`
FROM `regulation_register` s
WHERE COALESCE(CAST(s.`src_regulation_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_claim_policy`;
CREATE TABLE `link_claim_policy` AS
SELECT DISTINCT 
    MD5(CONCAT_WS('|', MD5(COALESCE(CAST(s.`src_claim_ref` AS CHAR), '')), MD5(COALESCE(CAST(s.`src_policy_ref` AS CHAR), '')))) AS `claim_policy_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    MD5(COALESCE(CAST(s.`src_claim_ref` AS CHAR), '')) AS `claim_hash_key`,
    MD5(COALESCE(CAST(s.`src_policy_ref` AS CHAR), '')) AS `policy_hash_key`
FROM `enhanced_policy_relationships` s
WHERE s.`source_extract` = 'claim_policy_bridge.csv' AND COALESCE(CAST(s.`src_claim_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_broker_person`;
CREATE TABLE `link_broker_person` AS
SELECT DISTINCT 
    MD5(CONCAT_WS('|', MD5(COALESCE(CAST(s.`src_broker_ref` AS CHAR), '')), MD5(COALESCE(CAST(s.`src_person_ref` AS CHAR), '')))) AS `broker_person_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    MD5(COALESCE(CAST(s.`src_broker_ref` AS CHAR), '')) AS `broker_hash_key`,
    MD5(COALESCE(CAST(s.`src_person_ref` AS CHAR), '')) AS `person_hash_key`
FROM `enhanced_person_relationships` s
WHERE s.`source_extract` = 'broker_person_bridge.csv' AND COALESCE(CAST(s.`src_broker_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_complaint_policy`;
CREATE TABLE `link_complaint_policy` AS
SELECT DISTINCT 
    MD5(CONCAT_WS('|', MD5(COALESCE(CAST(s.`src_complaint_ref` AS CHAR), '')), MD5(COALESCE(CAST(s.`src_policy_ref` AS CHAR), '')))) AS `complaint_policy_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    MD5(COALESCE(CAST(s.`src_complaint_ref` AS CHAR), '')) AS `complaint_hash_key`,
    MD5(COALESCE(CAST(s.`src_policy_ref` AS CHAR), '')) AS `policy_hash_key`
FROM `enhanced_policy_relationships` s
WHERE s.`source_extract` = 'complaint_policy_bridge.csv' AND COALESCE(CAST(s.`src_complaint_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_complaint_regulation`;
CREATE TABLE `link_complaint_regulation` AS
SELECT DISTINCT 
    MD5(CONCAT_WS('|', MD5(COALESCE(CAST(s.`src_regulation_ref` AS CHAR), '')), MD5(COALESCE(CAST(s.`src_complaint_ref` AS CHAR), '')))) AS `complaint_regulation_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    MD5(COALESCE(CAST(s.`src_regulation_ref` AS CHAR), '')) AS `regulation_hash_key`,
    MD5(COALESCE(CAST(s.`src_complaint_ref` AS CHAR), '')) AS `complaint_hash_key`
FROM `enhanced_policy_relationships` s
WHERE s.`source_extract` = 'complaint_regulation_bridge.csv' AND COALESCE(CAST(s.`src_regulation_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_insured_object_home`;
CREATE TABLE `link_insured_object_home` AS
SELECT DISTINCT 
    MD5(CONCAT_WS('|', MD5(COALESCE(CAST(s.`src_insured_object_ref` AS CHAR), '')), MD5(COALESCE(CAST(s.`src_home_ref` AS CHAR), '')))) AS `insured_object_home_hash_key`,
    s.`src_system` AS `record_source`,
    s.`src_extract_ts` AS `load_date`,
    MD5(COALESCE(CAST(s.`src_insured_object_ref` AS CHAR), '')) AS `insured_object_hash_key`,
    MD5(COALESCE(CAST(s.`src_home_ref` AS CHAR), '')) AS `home_hash_key`
FROM `enhanced_policy_relationships` s
WHERE s.`source_extract` = 'insured_object_home_bridge.csv' AND COALESCE(CAST(s.`src_insured_object_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_insured_object_motor`;
CREATE TABLE `link_insured_object_motor` AS
SELECT DISTINCT 
    MD5(CONCAT_WS('|', MD5(COALESCE(CAST(s.`src_insured_object_ref` AS CHAR), '')), MD5(COALESCE(CAST(s.`src_motor_ref` AS CHAR), '')))) AS `insured_object_motor_hash_key`,
    s.`src_system` AS `record_source`,
    s.`src_extract_ts` AS `load_date`,
    MD5(COALESCE(CAST(s.`src_insured_object_ref` AS CHAR), '')) AS `insured_object_hash_key`,
    MD5(COALESCE(CAST(s.`src_motor_ref` AS CHAR), '')) AS `motor_hash_key`
FROM `enhanced_policy_relationships` s
WHERE s.`source_extract` = 'insured_object_motor_bridge.csv' AND COALESCE(CAST(s.`src_insured_object_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_person_address`;
CREATE TABLE `link_person_address` AS
SELECT DISTINCT 
    MD5(CONCAT_WS('|', MD5(COALESCE(CAST(s.`src_person_ref` AS CHAR), '')), MD5(COALESCE(CAST(s.`src_address_ref` AS CHAR), '')))) AS `person_address_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    MD5(COALESCE(CAST(s.`src_person_ref` AS CHAR), '')) AS `person_hash_key`,
    MD5(COALESCE(CAST(s.`src_address_ref` AS CHAR), '')) AS `address_hash_key`
FROM `enhanced_person_relationships` s
WHERE s.`source_extract` = 'person_address_bridge.csv' AND COALESCE(CAST(s.`src_person_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_person_campaign`;
CREATE TABLE `link_person_campaign` AS
SELECT DISTINCT 
    MD5(CONCAT_WS('|', MD5(COALESCE(CAST(s.`src_person_ref` AS CHAR), '')), MD5(COALESCE(CAST(s.`src_campaign_ref` AS CHAR), '')))) AS `person_campaign_hash_key`,
    MD5(COALESCE(CAST(s.`src_person_ref` AS CHAR), '')) AS `person_hash_key`,
    MD5(COALESCE(CAST(s.`src_campaign_ref` AS CHAR), '')) AS `campaign_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`
FROM `enhanced_person_relationships` s
WHERE s.`source_extract` = 'person_campaign_bridge.csv' AND COALESCE(CAST(s.`src_person_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_policy_broker`;
CREATE TABLE `link_policy_broker` AS
SELECT DISTINCT 
    MD5(CONCAT_WS('|', MD5(COALESCE(CAST(s.`src_policy_ref` AS CHAR), '')), MD5(COALESCE(CAST(s.`src_broker_ref` AS CHAR), '')))) AS `policy_broker_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    MD5(COALESCE(CAST(s.`src_policy_ref` AS CHAR), '')) AS `policy_hash_key`,
    MD5(COALESCE(CAST(s.`src_broker_ref` AS CHAR), '')) AS `broker_hash_key`
FROM `enhanced_policy_relationships` s
WHERE s.`source_extract` = 'policy_broker_bridge.csv' AND COALESCE(CAST(s.`src_policy_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_policy_channel`;
CREATE TABLE `link_policy_channel` AS
SELECT DISTINCT 
    MD5(CONCAT_WS('|', MD5(COALESCE(CAST(s.`src_policy_ref` AS CHAR), '')), MD5(COALESCE(CAST(s.`src_channel_ref` AS CHAR), '')))) AS `policy_channel_hash_key`,
    MD5(COALESCE(CAST(s.`src_policy_ref` AS CHAR), '')) AS `policy_hash_key`,
    MD5(COALESCE(CAST(s.`src_channel_ref` AS CHAR), '')) AS `channel_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`
FROM `enhanced_policy_relationships` s
WHERE s.`source_extract` = 'policy_channel_bridge.csv' AND COALESCE(CAST(s.`src_policy_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_policy_insured_object`;
CREATE TABLE `link_policy_insured_object` AS
SELECT DISTINCT 
    MD5(CONCAT_WS('|', MD5(COALESCE(CAST(s.`src_policy_ref` AS CHAR), '')), MD5(COALESCE(CAST(s.`src_insured_object_ref` AS CHAR), '')))) AS `policy_insured_object_hash_key`,
    s.`src_system` AS `record_source`,
    s.`src_extract_ts` AS `load_date`,
    MD5(COALESCE(CAST(s.`src_policy_ref` AS CHAR), '')) AS `policy_hash_key`,
    MD5(COALESCE(CAST(s.`src_insured_object_ref` AS CHAR), '')) AS `insured_object_hash_key`
FROM `enhanced_policy_relationships` s
WHERE s.`source_extract` = 'policy_insured_object_bridge.csv' AND COALESCE(CAST(s.`src_policy_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_policy_override`;
CREATE TABLE `link_policy_override` AS
SELECT DISTINCT 
    MD5(CONCAT_WS('|', MD5(COALESCE(CAST(s.`src_policy_ref` AS CHAR), '')), MD5(COALESCE(CAST(s.`src_override_ref` AS CHAR), '')))) AS `policy_override_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    MD5(COALESCE(CAST(s.`src_policy_ref` AS CHAR), '')) AS `policy_hash_key`,
    MD5(COALESCE(CAST(s.`src_override_ref` AS CHAR), '')) AS `override_hash_key`
FROM `enhanced_policy_relationships` s
WHERE s.`source_extract` = 'policy_override_bridge.csv' AND COALESCE(CAST(s.`src_policy_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_policy_quote`;
CREATE TABLE `link_policy_quote` AS
SELECT DISTINCT 
    MD5(CONCAT_WS('|', MD5(COALESCE(CAST(s.`src_policy_ref` AS CHAR), '')), MD5(COALESCE(CAST(s.`src_quote_ref` AS CHAR), '')))) AS `policy_quote_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    MD5(COALESCE(CAST(s.`src_policy_ref` AS CHAR), '')) AS `policy_hash_key`,
    MD5(COALESCE(CAST(s.`src_quote_ref` AS CHAR), '')) AS `quote_hash_key`
FROM `enhanced_policy_relationships` s
WHERE s.`source_extract` = 'policy_quote_bridge.csv' AND COALESCE(CAST(s.`src_policy_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_quote_broker`;
CREATE TABLE `link_quote_broker` AS
SELECT DISTINCT 
    MD5(CONCAT_WS('|', MD5(COALESCE(CAST(s.`src_quote_ref` AS CHAR), '')), MD5(COALESCE(CAST(s.`src_broker_ref` AS CHAR), '')))) AS `quote_broker_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    MD5(COALESCE(CAST(s.`src_quote_ref` AS CHAR), '')) AS `quote_hash_key`,
    MD5(COALESCE(CAST(s.`src_broker_ref` AS CHAR), '')) AS `broker_hash_key`
FROM `enhanced_policy_relationships` s
WHERE s.`source_extract` = 'quote_broker_bridge.csv' AND COALESCE(CAST(s.`src_quote_ref` AS CHAR), '') <> '';

DROP TABLE IF EXISTS `link_quote_channel`;
CREATE TABLE `link_quote_channel` AS
SELECT DISTINCT 
    MD5(CONCAT_WS('|', MD5(COALESCE(CAST(s.`src_quote_ref` AS CHAR), '')), MD5(COALESCE(CAST(s.`src_channel_ref` AS CHAR), '')))) AS `quote_channel_hash_key`,
    s.`src_extract_ts` AS `load_date`,
    s.`src_system` AS `record_source`,
    MD5(COALESCE(CAST(s.`src_quote_ref` AS CHAR), '')) AS `quote_hash_key`,
    MD5(COALESCE(CAST(s.`src_channel_ref` AS CHAR), '')) AS `channel_hash_key`
FROM `enhanced_policy_relationships` s
WHERE s.`source_extract` = 'quote_channel_bridge.csv' AND COALESCE(CAST(s.`src_quote_ref` AS CHAR), '') <> '';

-- Optional primary key/index suggestions. Add these after reviewing existing DB naming standards.
-- ALTER TABLE `hub_person` ADD PRIMARY KEY (`person_hash_key`);
-- Repeat for each hub/link/satellite hash key if your database requires enforced keys.

