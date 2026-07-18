# SQL Table Comparison

Generated on 2026-07-17 15:59:52.

This report ignores `product_id` and `vault_object_id`. It compares `build_sql` from Source 1 Python against Source 2 notebook for each `target_table`.

## Hub

<table>
<thead><tr><th>Table</th><th>Source 1: Python</th><th>Source 2: Notebook</th><th>Difference Compared With Notebook</th></tr></thead>
<tbody>
<tr><td><code>hub_account</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(account_ref, origin_sys)) AS account_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  account_ref AS account_id,
  &#x27;hub_account&#x27; AS source_tbl_name
FROM crm_account_book_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    MD5(CONCAT(origin_sys, account_ref)) AS account_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    account_ref AS account_id
FROM crm_account_book_csv;</code></pre></details></td><td>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `account_hash_key`: Python `md5(concat(account_ref, origin_sys))`; Notebook `md5(concat(origin_sys, account_ref))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>hub_address</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(address_ref, origin_sys)) AS address_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  address_ref AS address_id,
  &#x27;hub_address&#x27; AS source_tbl_name
FROM crm_address_book_csv

UNION ALL

SELECT
  md5(CONCAT(address_id, origin_sys)) AS address_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  address_id AS address_id,
  &#x27;hub_address&#x27; AS source_tbl_name
FROM sap_address_db</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>with combined as (
SELECT DISTINCT
    MD5(CONCAT(origin_sys, address_ref)) AS address_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    address_ref AS address_id
FROM crm_address_book_csv
UNION ALL
SELECT DISTINCT
    MD5(CONCAT(origin_sys, src_address_id)) AS address_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    src_address_id AS address_id
FROM crm_enhanced_address_book_csv
UNION ALL
SELECT DISTINCT
    MD5(CONCAT(origin_sys, address_id)) AS address_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    address_id AS address_id
FROM sap_address_db)
select distinct * from combined;</code></pre></details></td><td>FROM differs. Python: crm_address_book_csv, sap_address_db. Notebook: combined, crm_address_book_csv, crm_enhanced_address_book_csv, sap_address_db.<br>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `address_hash_key`: Python `md5(concat(address_id, origin_sys))`; Notebook `md5(concat(origin_sys, address_id))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>hub_broker</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(src_agent_id, src_system)) AS broker_hash_key,
  current_timestamp() AS load_date,
  src_system AS record_source,
  src_agent_id AS agent_id,
  &#x27;hub_broker&#x27; AS source_tbl_name
FROM crm_broker_book_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    MD5(CONCAT(origin_sys, src_agent_id)) AS broker_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    src_agent_id AS agent_id
FROM crm_broker_book_csv;</code></pre></details></td><td>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `broker_hash_key`: Python `md5(concat(src_agent_id, src_system))`; Notebook `md5(concat(origin_sys, src_agent_id))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `record_source`: Python `src_system`; Notebook `origin_sys`.</td></tr>
<tr><td><code>hub_campaign</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(src_campaign_id, src_system)) AS campaign_hash_key,
  current_timestamp() AS load_date,
  src_system AS record_source,
  src_campaign_id AS campaign_id,
  &#x27;hub_campaign&#x27; AS source_tbl_name
FROM crm_campaign_register_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    MD5(CONCAT(origin_sys, src_campaign_id)) AS campaign_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    src_campaign_id AS campaign_id
FROM crm_campaign_register_csv;</code></pre></details></td><td>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `campaign_hash_key`: Python `md5(concat(src_campaign_id, src_system))`; Notebook `md5(concat(origin_sys, src_campaign_id))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `record_source`: Python `src_system`; Notebook `origin_sys`.</td></tr>
<tr><td><code>hub_channel</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(src_channel_id, src_system)) AS channel_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  src_system AS record_source,
  src_channel_id AS channel_id,
  &#x27;hub_channel&#x27; AS source_tbl_name
FROM crm_channel_catalog_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    MD5(CONCAT(origin_sys, src_channel_id)) AS channel_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    src_channel_id AS channel_id
FROM crm_channel_catalog_csv;</code></pre></details></td><td>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `channel_hash_key`: Python `md5(concat(src_channel_id, src_system))`; Notebook `md5(concat(origin_sys, src_channel_id))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `record_source`: Python `src_system`; Notebook `origin_sys`.</td></tr>
<tr><td><code>hub_claim</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(src_claim_id, src_system)) AS claim_hash_key,
  current_timestamp() AS load_date,
  src_system AS record_source,
  src_claim_id AS claim_id,
  &#x27;hub_claim&#x27; AS source_tbl_name
FROM crm_claim_register_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    MD5(CONCAT(origin_sys, src_claim_id)) AS claim_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    src_claim_id AS claim_id
FROM crm_claim_register_csv;</code></pre></details></td><td>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `claim_hash_key`: Python `md5(concat(src_claim_id, src_system))`; Notebook `md5(concat(origin_sys, src_claim_id))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `record_source`: Python `src_system`; Notebook `origin_sys`.</td></tr>
<tr><td><code>hub_complaint</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(src_complaint_id, src_system)) AS complaint_hash_key,
  current_timestamp() AS load_date,
  src_system AS record_source,
  src_complaint_id AS complaint_id,
  &#x27;hub_complaint&#x27; AS source_tbl_name
FROM crm_complaint_register_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    MD5(CONCAT(origin_sys, src_complaint_id)) AS complaint_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    src_complaint_id AS complaint_id
FROM crm_complaint_register_csv;</code></pre></details></td><td>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `complaint_hash_key`: Python `md5(concat(src_complaint_id, src_system))`; Notebook `md5(concat(origin_sys, src_complaint_id))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `record_source`: Python `src_system`; Notebook `origin_sys`.</td></tr>
<tr><td><code>hub_consent</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(consent_ref, origin_sys)) AS consent_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  consent_ref AS consent_id,
  &#x27;hub_consent&#x27; AS source_tbl_name
FROM crm_consent_snapshot_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    MD5(CONCAT(origin_sys, consent_ref)) AS consent_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    consent_ref AS consent_id
FROM crm_consent_snapshot_csv;</code></pre></details></td><td>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `consent_hash_key`: Python `md5(concat(consent_ref, origin_sys))`; Notebook `md5(concat(origin_sys, consent_ref))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>hub_contact</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(contact_ref, origin_sys)) AS contact_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  contact_ref AS contact_id,
  &#x27;hub_contact&#x27; AS source_tbl_name
FROM crm_contact_point_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    MD5(CONCAT(origin_sys, contact_ref)) AS contact_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    contact_ref AS contact_id
FROM crm_contact_point_csv;</code></pre></details></td><td>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `contact_hash_key`: Python `md5(concat(contact_ref, origin_sys))`; Notebook `md5(concat(origin_sys, contact_ref))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>hub_customer</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  customer_ref AS customer_id,
  &#x27;hub_customer&#x27; AS source_tbl_name
FROM crm_customer_portfolio_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    MD5(CONCAT(origin_sys, customer_ref)) AS customer_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    customer_ref AS customer_id
FROM crm_customer_portfolio_csv;</code></pre></details></td><td>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `customer_hash_key`: Python `md5(concat(customer_ref, origin_sys))`; Notebook `md5(concat(origin_sys, customer_ref))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>hub_home</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(property_ref, origin_sys)) AS home_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  property_ref AS insured_object_home_id,
  &#x27;hub_home&#x27; AS source_tbl_name
FROM crm_property_asset_csv
UNION ALL
SELECT
  md5(CONCAT(home_id, origin_sys)) AS home_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  home_id AS insured_object_home_id,
  &#x27;hub_home&#x27; AS source_tbl_name
FROM sap_home_db</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>with combined as (
SELECT DISTINCT
    MD5(CONCAT(origin_sys, property_ref)) AS home_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    property_ref AS insured_object_home_id
FROM crm_property_asset_csv
UNION ALL
SELECT DISTINCT
    MD5(CONCAT(origin_sys, home_id)) AS home_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    home_id AS insured_object_home_id
FROM sap_home_db)
select distinct * from combined;</code></pre></details></td><td>FROM differs. Python: crm_property_asset_csv, sap_home_db. Notebook: combined, crm_property_asset_csv, sap_home_db.<br>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `home_hash_key`: Python `md5(concat(home_id, origin_sys))`; Notebook `md5(concat(origin_sys, home_id))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>hub_identities</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(identity_ref, origin_sys)) AS identities_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  identity_ref AS identities_id,
  &#x27;hub_identities&#x27; AS source_tbl_name
FROM crm_identity_registry_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    MD5(CONCAT(origin_sys, identity_ref)) AS identities_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    identity_ref AS identities_id
FROM crm_identity_registry_csv;</code></pre></details></td><td>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `identities_hash_key`: Python `md5(concat(identity_ref, origin_sys))`; Notebook `md5(concat(origin_sys, identity_ref))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>hub_insured_object</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  insured_object_id AS insured_object_id,
  &#x27;hub_insured_object&#x27; AS source_tbl_name
FROM crm_property_asset_csv

UNION ALL

SELECT
  md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  insured_object_id AS insured_object_id,
  &#x27;hub_insured_object&#x27; AS source_tbl_name
FROM crm_vehicle_asset_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>with combined as (
SELECT DISTINCT
    MD5(CONCAT(origin_sys, insured_object_id)) AS insured_object_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    insured_object_id AS insured_object_id
FROM crm_property_asset_csv
UNION ALL
SELECT DISTINCT
    MD5(CONCAT(origin_sys, insured_object_id)) AS insured_object_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    insured_object_id AS insured_object_id
FROM crm_vehicle_asset_csv)
select distinct * from combined;</code></pre></details></td><td>FROM differs. Python: crm_property_asset_csv, crm_vehicle_asset_csv. Notebook: combined, crm_property_asset_csv, crm_vehicle_asset_csv.<br>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `insured_object_hash_key`: Python `md5(concat(insured_object_id, origin_sys))`; Notebook `md5(concat(origin_sys, insured_object_id))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>hub_lead</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(lead_ref, origin_sys)) AS lead_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  lead_ref AS lead_id,
  &#x27;hub_lead&#x27; AS source_tbl_name
FROM crm_lead_register_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    MD5(CONCAT(origin_sys, lead_ref)) AS lead_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    lead_ref AS lead_id
FROM crm_lead_register_csv;</code></pre></details></td><td>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `lead_hash_key`: Python `md5(concat(lead_ref, origin_sys))`; Notebook `md5(concat(origin_sys, lead_ref))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>hub_legal_person</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(legal_ref, origin_sys)) AS legal_person_hash_key,
  CAST(current_timestamp() AS timestamp) AS load_date,
  origin_sys AS record_source,
  legal_ref AS legal_person_id,
  &#x27;hub_legal_person&#x27; AS source_tbl_name
FROM crm_party_master_csv
UNION ALL
SELECT
  md5(CONCAT(person_id, origin_sys)) AS legal_person_hash_key,
  CAST(current_timestamp() AS timestamp) AS load_date,
  origin_sys AS record_source,
  person_id AS legal_person_id,
  &#x27;hub_legal_person&#x27; AS source_tbl_name
FROM sap_person_db</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>with combined as (
SELECT DISTINCT
    MD5(CONCAT(origin_sys, legal_ref)) AS legal_person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    natural_ref AS legal_person_id
FROM crm_party_master_csv
WHERE party_kind = &#x27;LEGAL&#x27;
UNION ALL
SELECT DISTINCT
    MD5(CONCAT(origin_sys, person_id)) AS legal_person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    person_id AS legal_person_id
FROM sap_person_db
WHERE person_type = &#x27;LEGAL&#x27;)

select distinct * from combined;</code></pre></details></td><td>FROM differs. Python: crm_party_master_csv, sap_person_db. Notebook: combined, crm_party_master_csv, sap_person_db.<br>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `legal_person_hash_key`: Python `md5(concat(person_id, origin_sys))`; Notebook `md5(concat(origin_sys, person_id))`; `load_date`: Python `cast(current_timestamp() as timestamp)`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>hub_marketing_engagement</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(src_marketing_engagement_ref, origin_sys)) AS marketing_engagement_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  src_marketing_engagement_ref AS marketing_engagement_id,
  &#x27;hub_marketing_engagement&#x27; AS source_tbl_name
FROM crm_enhanced_enrichments_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    MD5(CONCAT(origin_sys, engagement_ref)) AS marketing_engagement_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    engagement_ref AS marketing_engagement_id
FROM crm_campaign_touch_csv;</code></pre></details></td><td>FROM differs. Python: crm_enhanced_enrichments_csv. Notebook: crm_campaign_touch_csv.<br>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `marketing_engagement_hash_key`: Python `md5(concat(src_marketing_engagement_ref, origin_sys))`; Notebook `md5(concat(origin_sys, engagement_ref))`; `marketing_engagement_id`: Python `src_marketing_engagement_ref`; Notebook `engagement_ref`.</td></tr>
<tr><td><code>hub_marketing_preference</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(preference_ref, origin_sys)) AS marketing_preference_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  preference_ref AS marketing_preference_id,
  &#x27;hub_marketing_preference&#x27; AS source_tbl_name
FROM crm_comm_preference_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    MD5(CONCAT(origin_sys, preference_ref)) AS marketing_preference_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    preference_ref AS marketing_preference_id
FROM crm_comm_preference_csv;</code></pre></details></td><td>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `marketing_preference_hash_key`: Python `md5(concat(preference_ref, origin_sys))`; Notebook `md5(concat(origin_sys, preference_ref))`.</td></tr>
<tr><td><code>hub_motor</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(vehicle_ref, origin_sys)) AS motor_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  vehicle_ref AS insured_object_motor_id,
  &#x27;hub_motor&#x27; AS source_tbl_name
FROM crm_vehicle_asset_csv
UNION ALL
SELECT
  md5(CONCAT(motor_id, origin_sys)) AS motor_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  motor_id AS insured_object_motor_id,
  &#x27;hub_motor&#x27; AS source_tbl_name
FROM sap_motor_db</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>with combined as (
SELECT DISTINCT
    MD5(CONCAT(origin_sys, vehicle_ref)) AS motor_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    vehicle_ref AS insured_object_motor_id
FROM crm_vehicle_asset_csv
UNION ALL
SELECT DISTINCT
    MD5(CONCAT(origin_sys, motor_id)) AS motor_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    motor_id AS insured_object_motor_id
FROM sap_motor_db)
select distinct * from combined;</code></pre></details></td><td>FROM differs. Python: crm_vehicle_asset_csv, sap_motor_db. Notebook: combined, crm_vehicle_asset_csv, sap_motor_db.<br>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `motor_hash_key`: Python `md5(concat(motor_id, origin_sys))`; Notebook `md5(concat(origin_sys, motor_id))`.</td></tr>
<tr><td><code>hub_natural_person</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(natural_ref, origin_sys)) AS natural_person_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  natural_ref AS natural_person_id,
  &#x27;hub_natural_person&#x27; AS source_tbl_name
FROM crm_party_master_csv
UNION ALL
SELECT
  md5(CONCAT(person_id, origin_sys)) AS natural_person_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  person_id AS natural_person_id,
  &#x27;hub_natural_person&#x27; AS source_tbl_name
FROM sap_person_db</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>with combined as (
SELECT DISTINCT
    MD5(CONCAT(origin_sys, natural_ref)) AS natural_person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    natural_ref AS natural_person_id
FROM crm_party_master_csv
WHERE party_kind = &#x27;NATURAL&#x27;
UNION ALL
SELECT DISTINCT
    MD5(CONCAT(origin_sys, person_id)) AS natural_person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    person_id AS natural_person_id
FROM sap_person_db
WHERE person_type = &#x27;NATURAL&#x27;)

select distinct * from combined;</code></pre></details></td><td>FROM differs. Python: crm_party_master_csv, sap_person_db. Notebook: combined, crm_party_master_csv, sap_person_db.<br>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `natural_person_hash_key`: Python `md5(concat(person_id, origin_sys))`; Notebook `md5(concat(origin_sys, person_id))`.</td></tr>
<tr><td><code>hub_override</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(src_override_id, src_system)) AS override_hash_key,
  current_timestamp() AS load_date,
  src_system AS record_source,
  src_override_id AS override_id,
  &#x27;hub_override&#x27; AS source_tbl_name
FROM crm_override_register_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    MD5(CONCAT(origin_sys, src_override_id)) AS override_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    src_override_id AS override_id
FROM crm_override_register_csv;</code></pre></details></td><td>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `override_hash_key`: Python `md5(concat(src_override_id, src_system))`; Notebook `md5(concat(origin_sys, src_override_id))`; `record_source`: Python `src_system`; Notebook `origin_sys`.</td></tr>
<tr><td><code>hub_person</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  party_ref AS person_id,
  &#x27;hub_person&#x27; AS source_tbl_name
FROM crm_party_master_csv
UNION ALL
SELECT
  md5(CONCAT(person_id, origin_sys)) AS person_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  person_id AS person_id,
  &#x27;hub_person&#x27; AS source_tbl_name
FROM sap_person_db</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>with combined as (
SELECT DISTINCT
    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    party_ref AS person_id
FROM crm_party_master_csv
UNION ALL
SELECT DISTINCT
    MD5(CONCAT(origin_sys, person_id)) AS person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    person_id AS person_id
FROM sap_person_db)
select distinct * from combined;</code></pre></details></td><td>FROM differs. Python: crm_party_master_csv, sap_person_db. Notebook: combined, crm_party_master_csv, sap_person_db.<br>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `person_hash_key`: Python `md5(concat(person_id, origin_sys))`; Notebook `md5(concat(origin_sys, person_id))`.</td></tr>
<tr><td><code>hub_policy</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  policy_ref AS policy_id,
  &#x27;hub_policy&#x27; AS source_tbl_name
FROM crm_policy_register_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>with combined as (
SELECT DISTINCT
    MD5(CONCAT(origin_sys, policy_ref)) AS policy_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    policy_ref AS policy_id
FROM crm_policy_register_csv
UNION ALL
SELECT DISTINCT
    MD5(CONCAT(origin_sys, policy_id)) AS policy_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    policy_id AS policy_id
FROM sap_home_db
UNION ALL
SELECT DISTINCT
    MD5(CONCAT(origin_sys, policy_id)) AS policy_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    policy_id AS policy_id
FROM sap_motor_db)
select distinct * from combined;</code></pre></details></td><td>FROM differs. Python: crm_policy_register_csv. Notebook: combined, crm_policy_register_csv, sap_home_db, sap_motor_db.<br>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `policy_hash_key`: Python `md5(concat(policy_ref, origin_sys))`; Notebook `md5(concat(origin_sys, policy_id))`; `policy_id`: Python `policy_ref`; Notebook `policy_id`.</td></tr>
<tr><td><code>hub_product</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(product_ref, origin_sys)) AS product_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  product_ref AS product_id,
  &#x27;hub_product&#x27; AS source_tbl_name
FROM crm_product_catelog_csv

UNION ALL

SELECT
  md5(CONCAT(product_id, origin_sys)) AS product_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  product_id AS product_id,
  &#x27;hub_product&#x27; AS source_tbl_name
FROM sap_product_db</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>with combined as (
SELECT DISTINCT
    MD5(CONCAT(origin_sys, product_ref)) AS product_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    product_ref AS product_id
FROM crm_product_catalog_csv
UNION ALL
SELECT DISTINCT
    MD5(CONCAT(origin_sys, product_id)) AS product_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    product_id AS product_id
FROM sap_product_db
UNION ALL
SELECT DISTINCT
    MD5(CONCAT(origin_sys, product_id)) AS product_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    product_id AS product_id
FROM sap_home_db
UNION ALL
SELECT DISTINCT
    MD5(CONCAT(origin_sys, product_id)) AS product_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    product_id AS product_id
FROM sap_motor_db)
select distinct * from combined;</code></pre></details></td><td>FROM differs. Python: crm_product_catelog_csv, sap_product_db. Notebook: combined, crm_product_catalog_csv, sap_home_db, sap_motor_db, sap_product_db.<br>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `product_hash_key`: Python `md5(concat(product_id, origin_sys))`; Notebook `md5(concat(origin_sys, product_id))`.</td></tr>
<tr><td><code>hub_quote</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  quote_ref AS quote_id,
  &#x27;hub_quote&#x27; AS source_tbl_name
FROM crm_quote_register_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    MD5(CONCAT(quote_ref, quote_ref)) AS quote_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    quote_ref AS quote_id
FROM crm_quote_register_csv;</code></pre></details></td><td>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `quote_hash_key`: Python `md5(concat(quote_ref, origin_sys))`; Notebook `md5(concat(quote_ref, quote_ref))`.</td></tr>
<tr><td><code>hub_regulation</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT
  md5(CONCAT(src_regulation_id, src_system)) AS regulation_hash_key,
  current_timestamp() AS load_date,
  src_system AS record_source,
  src_regulation_id AS regulation_id,
  &#x27;hub_regulation&#x27; AS source_tbl_name
FROM crm_regulation_register_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    MD5(CONCAT(origin_sys, src_regulation_id)) AS regulation_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    src_regulation_id AS regulation_id
FROM crm_regulation_register_csv;</code></pre></details></td><td>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `record_source`: Python `src_system`; Notebook `origin_sys`; `regulation_hash_key`: Python `md5(concat(src_regulation_id, src_system))`; Notebook `md5(concat(origin_sys, src_regulation_id))`.</td></tr>
</tbody>
</table>

## Link

<table>
<thead><tr><th>Table</th><th>Source 1: Python</th><th>Source 2: Notebook</th><th>Difference Compared With Notebook</th></tr></thead>
<tbody>
<tr><td><code>link_broker_person</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(src_agent_id, src_system)) AS broker_hash_key,
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    current_timestamp() AS load_ts,
    COALESCE(src_system, origin_sys) AS record_source
  FROM crm_broker_book_csv
  CROSS JOIN crm_party_master_csv
)
SELECT
  md5(CONCAT(broker_hash_key, person_hash_key)) AS broker_person_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  record_source,
  broker_hash_key,
  person_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, src_agent_id)),MD5(CONCAT(origin_sys, src_person_id))))AS broker_person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, src_agent_id)) AS broker_hash_key,
    MD5(CONCAT(origin_sys, src_person_id)) AS person_hash_key
FROM crm_enhanced_person_relationships_csv
WHERE source_extract = &#x27;broker_person_bridge.csv&#x27;;</code></pre></details></td><td>FROM differs. Python: combined, crm_broker_book_csv. Notebook: crm_enhanced_person_relationships_csv.<br>Notebook removes columns: `broker_person_hash_key`, `load_ts`.<br>Changed expressions: `broker_hash_key`: Python `md5(concat(src_agent_id, src_system))`; Notebook `md5(concat(origin_sys, src_agent_id))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `person_hash_key`: Python `md5(concat(party_ref, origin_sys))`; Notebook `md5(concat(origin_sys, src_person_id))`; `record_source`: Python `coalesce(src_system, origin_sys)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_claim_policy</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(src_claim_id, src_system)) AS claim_hash_key,
    CAST(NULL AS STRING) AS policy_hash_key,
    current_timestamp() AS etl_load_timestamp,
    COALESCE(src_system, origin_sys) AS record_source
  FROM crm_claim_register_csv

  UNION ALL

  SELECT
    CAST(NULL AS STRING) AS claim_hash_key,
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    current_timestamp() AS etl_load_timestamp,
    COALESCE(src_system, origin_sys) AS record_source
  FROM crm_policy_register_csv
)
SELECT
  md5(CONCAT(claim_hash_key, policy_hash_key)) AS claim_policy_hash_key,
  CAST(etl_load_timestamp AS timestamp) AS load_date,
  record_source AS record_source,
  claim_hash_key,
  policy_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, src_claim_id)),MD5(CONCAT(origin_sys, src_policy_id))))AS claim_policy_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, src_claim_id)) AS claim_hash_key,
    MD5(CONCAT(origin_sys, src_policy_id)) AS policy_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE source_extract = &#x27;claim_policy_bridge.csv&#x27;;</code></pre></details></td><td>FROM differs. Python: combined, crm_claim_register_csv, crm_policy_register_csv. Notebook: crm_enhanced_policy_relationships_csv.<br>Notebook removes columns: `claim_policy_hash_key`, `etl_load_timestamp`.<br>Changed expressions: `claim_hash_key`: Python `cast(null as string)`; Notebook `md5(concat(origin_sys, src_claim_id))`; `load_date`: Python `cast(etl_load_timestamp as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `policy_hash_key`: Python `md5(concat(policy_ref, origin_sys))`; Notebook `md5(concat(origin_sys, src_policy_id))`; `record_source`: Python `record_source`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_complaint_policy</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(src_complaint_id, src_system)) AS complaint_hash_key,
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    current_timestamp() AS load_date,
    COALESCE(src_system, origin_sys) AS record_source
  FROM crm_complaint_register_csv
  CROSS JOIN crm_policy_register_csv
)
SELECT
  md5(CONCAT(complaint_hash_key, policy_hash_key)) AS complaint_policy_hash_key,
  CAST(load_date AS TIMESTAMP) AS load_date,
  record_source AS record_source,
  complaint_hash_key,
  policy_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, src_complaint_id)),MD5(CONCAT(origin_sys, src_policy_id))))AS complaint_policy_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, src_complaint_id)) AS complaint_hash_key,
    MD5(CONCAT(origin_sys, src_policy_id)) AS policy_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE source_extract = &#x27;complaint_policy_bridge.csv&#x27;;</code></pre></details></td><td>FROM differs. Python: combined, crm_complaint_register_csv. Notebook: crm_enhanced_policy_relationships_csv.<br>Notebook removes columns: `complaint_policy_hash_key`.<br>Changed expressions: `complaint_hash_key`: Python `md5(concat(src_complaint_id, src_system))`; Notebook `md5(concat(origin_sys, src_complaint_id))`; `load_date`: Python `cast(load_date as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `policy_hash_key`: Python `md5(concat(policy_ref, origin_sys))`; Notebook `md5(concat(origin_sys, src_policy_id))`; `record_source`: Python `record_source`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_complaint_regulation</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(src_complaint_id, src_system)) AS complaint_hash_key,
    md5(CONCAT(src_regulation_id, src_system)) AS regulation_hash_key,
    current_timestamp() AS load_ts,
    src_system AS origin_sys,
    &#x27;crm_complaint_register_csv&#x27; AS source_tbl_name
  FROM crm_complaint_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(src_complaint_id, src_system)) AS complaint_hash_key,
    md5(CONCAT(src_regulation_id, src_system)) AS regulation_hash_key,
    current_timestamp() AS load_ts,
    src_system AS origin_sys,
    &#x27;crm_regulation_register_csv&#x27; AS source_tbl_name
  FROM crm_regulation_register_csv
)
SELECT
  md5(CONCAT(src_complaint_id, src_regulation_id)) AS complaint_regulation_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  complaint_hash_key,
  regulation_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, src_complaint_id)),MD5(CONCAT(origin_sys, src_regulation_id))))AS complaint_regulation_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, src_complaint_id)) AS complaint_hash_key,
    MD5(CONCAT(origin_sys, src_regulation_id)) AS regulation_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE source_extract = &#x27;complaint_regulation_bridge.csv&#x27;;</code></pre></details></td><td>FROM differs. Python: combined, crm_complaint_register_csv, crm_regulation_register_csv. Notebook: crm_enhanced_policy_relationships_csv.<br>Notebook removes columns: `complaint_regulation_hash_key`, `load_ts`, `origin_sys`, `source_tbl_name`.<br>Changed expressions: `complaint_hash_key`: Python `md5(concat(src_complaint_id, src_system))`; Notebook `md5(concat(origin_sys, src_complaint_id))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `record_source`: Python `coalesce(origin_sys, origin_sys)`; Notebook `origin_sys`; `regulation_hash_key`: Python `md5(concat(src_regulation_id, src_system))`; Notebook `md5(concat(origin_sys, src_regulation_id))`.</td></tr>
<tr><td><code>link_customer_lead</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
    md5(CONCAT(lead_ref, origin_sys)) AS lead_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    &#x27;link_customer_lead&#x27; AS source_tbl_name
  FROM crm_customer_portfolio_csv
  UNION ALL
  SELECT
    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
    md5(CONCAT(lead_ref, origin_sys)) AS lead_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    &#x27;link_customer_lead&#x27; AS source_tbl_name
  FROM crm_lead_register_csv
)
SELECT
  md5(CONCAT(customer_ref, lead_ref)) AS customer_lead_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  customer_hash_key,
  lead_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, lead_ref)),MD5(CONCAT(origin_sys, customer_ref))))AS customer_lead_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, lead_ref)) AS lead_hash_key,
    MD5(CONCAT(origin_sys,customer_ref)) AS customer_hash_key
FROM crm_customer_lead_bridge_csv;</code></pre></details></td><td>FROM differs. Python: combined, crm_customer_portfolio_csv, crm_lead_register_csv. Notebook: crm_customer_lead_bridge_csv.<br>Notebook removes columns: `customer_lead_hash_key`, `load_ts`, `source_tbl_name`.<br>Changed expressions: `customer_hash_key`: Python `md5(concat(customer_ref, origin_sys))`; Notebook `md5(concat(origin_sys,customer_ref))`; `lead_hash_key`: Python `md5(concat(lead_ref, origin_sys))`; Notebook `md5(concat(origin_sys, lead_ref))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `record_source`: Python `coalesce(origin_sys, origin_sys)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_customer_person</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    &#x27;crm_customer_portfolio_csv&#x27; AS source_tbl_name
  FROM crm_customer_portfolio_csv
  UNION ALL
  SELECT
    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    &#x27;crm_party_master_csv&#x27; AS source_tbl_name
  FROM crm_party_master_csv
)
SELECT
  md5(CONCAT(customer_hash_key, person_hash_key)) AS customer_person_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  customer_hash_key,
  person_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, customer_ref))))AS customer_person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
    MD5(CONCAT(origin_sys,customer_ref)) AS customer_hash_key
FROM crm_customer_portfolio_csv;</code></pre></details></td><td>FROM differs. Python: combined, crm_customer_portfolio_csv, crm_party_master_csv. Notebook: crm_customer_portfolio_csv.<br>Notebook removes columns: `customer_person_hash_key`, `load_ts`, `source_tbl_name`.<br>Changed expressions: `customer_hash_key`: Python `md5(concat(customer_ref, origin_sys))`; Notebook `md5(concat(origin_sys,customer_ref))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `person_hash_key`: Python `md5(concat(party_ref, origin_sys))`; Notebook `md5(concat(origin_sys, party_ref))`; `record_source`: Python `coalesce(origin_sys, origin_sys)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_insured_object_home</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(src_insured_object_id, property_ref)) AS insured_object_home_hash_key,
    md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
    md5(CONCAT(property_ref, origin_sys)) AS home_hash_key,
    current_timestamp() AS load_timestamp,
    origin_sys,
    &#x27;crm_property_asset_csv&#x27; AS source_tbl_name
  FROM crm_property_asset_csv
)
SELECT
  md5(CONCAT(insured_object_hash_key, home_hash_key, origin_sys)) AS insured_object_home_hash_key,
  CAST(load_timestamp AS timestamp) AS load_date,
  origin_sys AS record_source,
  insured_object_hash_key,
  home_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, src_insured_object_id)),MD5(CONCAT(origin_sys, src_home_id))))AS insured_object_home_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, src_insured_object_id)) AS insured_object_hash_key,
    MD5(CONCAT(origin_sys, src_home_id)) AS home_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE source_extract = &#x27;insured_object_home_bridge.csv&#x27;;</code></pre></details></td><td>FROM differs. Python: combined, crm_property_asset_csv. Notebook: crm_enhanced_policy_relationships_csv.<br>Notebook removes columns: `insured_object_home_hash_key`, `load_timestamp`, `source_tbl_name`.<br>Changed expressions: `home_hash_key`: Python `md5(concat(property_ref, origin_sys))`; Notebook `md5(concat(origin_sys, src_home_id))`; `insured_object_hash_key`: Python `md5(concat(insured_object_id, origin_sys))`; Notebook `md5(concat(origin_sys, src_insured_object_id))`; `load_date`: Python `cast(load_timestamp as timestamp)`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>link_insured_object_motor</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
    md5(CONCAT(vehicle_ref, origin_sys)) AS motor_hash_key,
    current_timestamp() AS load_timestamp,
    origin_sys,
    &#x27;crm_vehicle_asset_csv&#x27; AS source_tbl_name
  FROM crm_vehicle_asset_csv
)
SELECT
  md5(CONCAT(insured_object_hash_key, motor_hash_key, origin_sys)) AS insured_object_motor_hash_key,
  CAST(load_timestamp AS TIMESTAMP) AS load_date,
  origin_sys AS record_source,
  insured_object_hash_key,
  motor_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, src_insured_object_id)),MD5(CONCAT(origin_sys, src_motor_id))))AS insured_object_motor_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, src_insured_object_id)) AS insured_object_hash_key,
    MD5(CONCAT(origin_sys, src_motor_id)) AS motor_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE source_extract = &#x27;insured_object_motor_bridge.csv&#x27;;</code></pre></details></td><td>FROM differs. Python: combined, crm_vehicle_asset_csv. Notebook: crm_enhanced_policy_relationships_csv.<br>Notebook removes columns: `insured_object_motor_hash_key`, `load_timestamp`, `source_tbl_name`.<br>Changed expressions: `insured_object_hash_key`: Python `md5(concat(insured_object_id, origin_sys))`; Notebook `md5(concat(origin_sys, src_insured_object_id))`; `load_date`: Python `cast(load_timestamp as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `motor_hash_key`: Python `md5(concat(vehicle_ref, origin_sys))`; Notebook `md5(concat(origin_sys, src_motor_id))`.</td></tr>
<tr><td><code>link_person_account</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    SHA2(CONCAT_WS(&#x27;||&#x27;, CAST(party_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS person_hash_key,
    SHA2(CONCAT_WS(&#x27;||&#x27;, CAST(account_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS account_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    &#x27;link_person_account&#x27; AS source_tbl_name
  FROM crm_party_master_csv
  JOIN crm_account_book_csv
    ON crm_party_master_csv.party_ref = crm_account_book_csv.party_ref
)
SELECT
  SHA2(CONCAT_WS(&#x27;||&#x27;, CAST(person_hash_key AS STRING), CAST(account_hash_key AS STRING), CAST(origin_sys AS STRING)), 256) AS person_account_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  person_hash_key,
  account_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, account_ref))))AS person_account_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
    MD5(CONCAT(origin_sys,account_ref)) AS account_hash_key
FROM crm_account_book_csv;</code></pre></details></td><td>FROM differs. Python: combined, crm_party_master_csv. Notebook: crm_account_book_csv.<br>Notebook removes columns: `load_ts`, `person_account_hash_key`, `source_tbl_name`.<br>Changed expressions: `account_hash_key`: Python `sha2(concat_ws('||', cast(account_ref as string), cast(origin_sys as string)), 256)`; Notebook `md5(concat(origin_sys,account_ref))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `person_hash_key`: Python `sha2(concat_ws('||', cast(party_ref as string), cast(origin_sys as string)), 256)`; Notebook `md5(concat(origin_sys, party_ref))`; `record_source`: Python `coalesce(origin_sys, origin_sys)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_person_address</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(address_ref, origin_sys)) AS address_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    &#x27;link_person_address&#x27; AS source_tbl_name
  FROM crm_party_master_csv
  CROSS JOIN crm_address_book_csv
)
SELECT
  md5(CONCAT(person_hash_key, address_hash_key)) AS person_address_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  person_hash_key,
  address_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>with combined as (
SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, address_ref))))AS person_address_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
    MD5(CONCAT(origin_sys,address_ref)) AS address_hash_key
FROM crm_address_book_csv
UNION ALL
SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, src_person_id)),MD5(CONCAT(origin_sys, src_address_id))))AS person_address_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, src_person_id)) AS person_hash_key,
    MD5(CONCAT(origin_sys,src_address_id)) AS address_hash_key
FROM crm_enhanced_person_relationships_csv
WHERE source_extract = &#x27;person_address_bridge.csv&#x27;
UNION ALL
SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, person_id)),MD5(CONCAT(origin_sys, address_id))))AS person_address_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, person_id)) AS person_hash_key,
    MD5(CONCAT(origin_sys,address_id)) AS address_hash_key
FROM sap_address_db)
select distinct * from combined;</code></pre></details></td><td>FROM differs. Python: combined, crm_party_master_csv. Notebook: combined, crm_address_book_csv, crm_enhanced_person_relationships_csv, sap_address_db.<br>Notebook removes columns: `load_ts`, `person_address_hash_key`, `source_tbl_name`.<br>Changed expressions: `address_hash_key`: Python `md5(concat(address_ref, origin_sys))`; Notebook `md5(concat(origin_sys,address_id))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `person_hash_key`: Python `md5(concat(party_ref, origin_sys))`; Notebook `md5(concat(origin_sys, person_id))`; `record_source`: Python `coalesce(origin_sys, origin_sys)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_person_campaign</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(party_ref, src_campaign_id)) AS person_campaign_hash_key,
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(src_campaign_id, src_system)) AS campaign_hash_key,
    current_timestamp() AS load_timestamp,
    coalesce(origin_sys, src_system) AS record_source
  FROM crm_party_master_csv
  CROSS JOIN crm_campaign_register_csv
)
SELECT
  person_campaign_hash_key,
  CAST(load_timestamp AS timestamp) AS load_date,
  record_source,
  person_hash_key,
  campaign_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, src_person_id)),MD5(CONCAT(origin_sys, src_campaign_id))))AS person_campaign_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, src_person_id)) AS person_hash_key,
    MD5(CONCAT(origin_sys, src_campaign_id)) AS campaign_hash_key
FROM crm_enhanced_person_relationships_csv
WHERE source_extract = &#x27;person_campaign_bridge.csv&#x27;;</code></pre></details></td><td>FROM differs. Python: combined, crm_party_master_csv. Notebook: crm_enhanced_person_relationships_csv.<br>Notebook removes columns: `load_timestamp`, `person_campaign_hash_key`.<br>Changed expressions: `campaign_hash_key`: Python `md5(concat(src_campaign_id, src_system))`; Notebook `md5(concat(origin_sys, src_campaign_id))`; `load_date`: Python `cast(load_timestamp as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `person_hash_key`: Python `md5(concat(party_ref, origin_sys))`; Notebook `md5(concat(origin_sys, src_person_id))`; `record_source`: Python `coalesce(origin_sys, src_system)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_person_consent</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(consent_ref, origin_sys)) AS consent_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    &#x27;link_person_consent&#x27; AS source_tbl_name
  FROM crm_party_master_csv
  CROSS JOIN crm_consent_snapshot_csv
)
SELECT
  md5(CONCAT(person_hash_key, consent_hash_key)) AS person_consent_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  person_hash_key,
  consent_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, consent_ref))))AS person_consent_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
    MD5(CONCAT(origin_sys,consent_ref)) AS consent_hash_key
FROM crm_consent_snapshot_csv;</code></pre></details></td><td>FROM differs. Python: combined, crm_party_master_csv. Notebook: crm_consent_snapshot_csv.<br>Notebook removes columns: `load_ts`, `person_consent_hash_key`, `source_tbl_name`.<br>Changed expressions: `consent_hash_key`: Python `md5(concat(consent_ref, origin_sys))`; Notebook `md5(concat(origin_sys,consent_ref))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `person_hash_key`: Python `md5(concat(party_ref, origin_sys))`; Notebook `md5(concat(origin_sys, party_ref))`; `record_source`: Python `coalesce(origin_sys, origin_sys)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_person_contact</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(contact_ref, origin_sys)) AS contact_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    &#x27;link_person_contact&#x27; AS source_tbl_name
  FROM crm_party_master_csv
  UNION ALL
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(contact_ref, origin_sys)) AS contact_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    &#x27;link_person_contact&#x27; AS source_tbl_name
  FROM crm_contact_point_csv
)
SELECT
  md5(CONCAT(person_hash_key, contact_hash_key)) AS person_contact_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  person_hash_key,
  contact_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, contact_ref))))AS person_contact_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
    MD5(CONCAT(origin_sys,contact_ref)) AS contact_hash_key
FROM crm_contact_point_csv;</code></pre></details></td><td>FROM differs. Python: combined, crm_contact_point_csv, crm_party_master_csv. Notebook: crm_contact_point_csv.<br>Notebook removes columns: `load_ts`, `person_contact_hash_key`, `source_tbl_name`.<br>Changed expressions: `contact_hash_key`: Python `md5(concat(contact_ref, origin_sys))`; Notebook `md5(concat(origin_sys,contact_ref))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `person_hash_key`: Python `md5(concat(party_ref, origin_sys))`; Notebook `md5(concat(origin_sys, party_ref))`; `record_source`: Python `coalesce(origin_sys, origin_sys)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_person_identities</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(identity_ref, origin_sys)) AS identities_hash_key,
    load_timestamp AS source_load_timestamp_col,
    origin_sys,
    &#x27;crm_party_master_csv&#x27; AS source_tbl_name
  FROM crm_party_master_csv
  UNION ALL
  SELECT
    md5(CONCAT(person_id, origin_sys)) AS person_hash_key,
    md5(CONCAT(identity_ref, origin_sys)) AS identities_hash_key,
    load_timestamp AS source_load_timestamp_col,
    origin_sys,
    &#x27;crm_identity_registry_csv&#x27; AS source_tbl_name
  FROM crm_identity_registry_csv
)
SELECT
  md5(CONCAT(person_hash_key, identities_hash_key)) AS person_identities_hash_key,
  CAST(source_load_timestamp_col AS timestamp) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  person_hash_key,
  identities_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, identity_ref))))AS person_identities_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
    MD5(CONCAT(origin_sys,identity_ref)) AS identities_hash_key
FROM crm_identity_registry_csv;</code></pre></details></td><td>FROM differs. Python: combined, crm_identity_registry_csv, crm_party_master_csv. Notebook: crm_identity_registry_csv.<br>Notebook removes columns: `person_identities_hash_key`, `source_load_timestamp_col`, `source_tbl_name`.<br>Changed expressions: `identities_hash_key`: Python `md5(concat(identity_ref, origin_sys))`; Notebook `md5(concat(origin_sys,identity_ref))`; `load_date`: Python `cast(source_load_timestamp_col as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `person_hash_key`: Python `md5(concat(person_id, origin_sys))`; Notebook `md5(concat(origin_sys, party_ref))`; `record_source`: Python `coalesce(origin_sys, origin_sys)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_person_lead</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(lead_ref, origin_sys)) AS lead_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    &#x27;link_person_lead&#x27; AS source_tbl_name
  FROM crm_party_master_csv
  CROSS JOIN crm_lead_register_csv
)
SELECT
  md5(CONCAT(person_hash_key, lead_hash_key)) AS person_lead_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  origin_sys AS record_source,
  person_hash_key,
  lead_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, lead_ref))))AS person_lead_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
    MD5(CONCAT(origin_sys,lead_ref)) AS lead_hash_key
FROM crm_lead_register_csv;</code></pre></details></td><td>FROM differs. Python: combined, crm_party_master_csv. Notebook: crm_lead_register_csv.<br>Notebook removes columns: `load_ts`, `person_lead_hash_key`, `source_tbl_name`.<br>Changed expressions: `lead_hash_key`: Python `md5(concat(lead_ref, origin_sys))`; Notebook `md5(concat(origin_sys,lead_ref))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `person_hash_key`: Python `md5(concat(party_ref, origin_sys))`; Notebook `md5(concat(origin_sys, party_ref))`.</td></tr>
<tr><td><code>link_person_legal_person</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(legal_ref, origin_sys)) AS legal_person_hash_key,
    origin_sys,
    current_timestamp() AS etl_load_timestamp,
    &#x27;crm_party_master_csv&#x27; AS source_tbl_name
  FROM crm_party_master_csv

  UNION ALL

  SELECT
    md5(CONCAT(person_id, origin_sys)) AS person_hash_key,
    md5(CONCAT(person_id, origin_sys)) AS legal_person_hash_key,
    origin_sys,
    current_timestamp() AS etl_load_timestamp,
    &#x27;sap_person_db&#x27; AS source_tbl_name
  FROM sap_person_db
)
SELECT
  md5(CONCAT(person_hash_key, legal_person_hash_key)) AS person_legal_person_hash_key,
  CAST(etl_load_timestamp AS timestamp) AS load_date,
  origin_sys AS record_source,
  person_hash_key,
  legal_person_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>with combined as (
SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, legal_ref))))AS person_legal_person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
    MD5(CONCAT(origin_sys, legal_ref)) AS legal_person_hash_key
FROM crm_party_master_csv
WHERE where party_kind = &#x27;LEGAL&#x27;
UNION ALL
SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, person_id)),MD5(CONCAT(origin_sys, person_id))))AS person_legal_person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, person_id)) AS person_hash_key,
    MD5(CONCAT(origin_sys, person_id)) AS legal_person_hash_key
FROM sap_person_db 
WHERE person_type = &#x27;LEGAL&#x27;)
select distinct * from combined;</code></pre></details></td><td>Notebook removes columns: `etl_load_timestamp`, `person_legal_person_hash_key`, `source_tbl_name`.<br>Changed expressions: `legal_person_hash_key`: Python `md5(concat(person_id, origin_sys))`; Notebook `md5(concat(origin_sys, person_id))`; `load_date`: Python `cast(etl_load_timestamp as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `person_hash_key`: Python `md5(concat(person_id, origin_sys))`; Notebook `md5(concat(origin_sys, person_id))`.</td></tr>
<tr><td><code>link_person_marketing_engagement</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(src_marketing_engagement_ref, origin_sys)) AS marketing_engagement_hash_key,
    current_timestamp() AS etl_load_timestamp,
    origin_sys,
    &#x27;crm_party_master_csv&#x27; AS source_tbl_name
  FROM crm_party_master_csv
  UNION ALL
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(src_marketing_engagement_ref, origin_sys)) AS marketing_engagement_hash_key,
    current_timestamp() AS etl_load_timestamp,
    origin_sys,
    &#x27;crm_enhanced_enrichments_csv&#x27; AS source_tbl_name
  FROM crm_enhanced_enrichments_csv
)
SELECT
  md5(CONCAT(person_hash_key, marketing_engagement_hash_key)) AS person_marketing_engagement_hash_key,
  CAST(etl_load_timestamp AS timestamp) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  person_hash_key,
  marketing_engagement_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, engagement_ref))))AS person_marketing_engagement_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
    MD5(CONCAT(origin_sys,engagement_ref)) AS marketing_engagement_hash_key
FROM crm_campaign_touch_csv;</code></pre></details></td><td>FROM differs. Python: combined, crm_enhanced_enrichments_csv, crm_party_master_csv. Notebook: crm_campaign_touch_csv.<br>Notebook removes columns: `etl_load_timestamp`, `person_marketing_engagement_hash_key`, `source_tbl_name`.<br>Changed expressions: `load_date`: Python `cast(etl_load_timestamp as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `marketing_engagement_hash_key`: Python `md5(concat(src_marketing_engagement_ref, origin_sys))`; Notebook `md5(concat(origin_sys,engagement_ref))`; `person_hash_key`: Python `md5(concat(party_ref, origin_sys))`; Notebook `md5(concat(origin_sys, party_ref))`; `record_source`: Python `coalesce(origin_sys, origin_sys)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_person_marketing_preference</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(preference_ref, origin_sys)) AS marketing_preference_hash_key,
    current_timestamp() AS etl_load_timestamp,
    origin_sys,
    &#x27;link_person_marketing_preference&#x27; AS source_tbl_name
  FROM crm_party_master_csv
  JOIN crm_comm_preference_csv
    ON crm_party_master_csv.party_ref = crm_comm_preference_csv.party_ref
)
SELECT
  md5(CONCAT(person_hash_key, marketing_preference_hash_key)) AS person_marketing_preference_hash_key,
  CAST(etl_load_timestamp AS timestamp) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  person_hash_key,
  marketing_preference_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, preference_ref))))AS person_marketing_preference_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
    MD5(CONCAT(origin_sys,preference_ref)) AS marketing_preference_hash_key
FROM crm_comm_preference_csv;</code></pre></details></td><td>FROM differs. Python: combined, crm_party_master_csv. Notebook: crm_comm_preference_csv.<br>Notebook removes columns: `etl_load_timestamp`, `person_marketing_preference_hash_key`, `source_tbl_name`.<br>Changed expressions: `load_date`: Python `cast(etl_load_timestamp as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `marketing_preference_hash_key`: Python `md5(concat(preference_ref, origin_sys))`; Notebook `md5(concat(origin_sys,preference_ref))`; `person_hash_key`: Python `md5(concat(party_ref, origin_sys))`; Notebook `md5(concat(origin_sys, party_ref))`; `record_source`: Python `coalesce(origin_sys, origin_sys)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_person_natural_person</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    SHA2(CONCAT_WS(&#x27;||&#x27;, CAST(party_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS person_hash_key,
    SHA2(CONCAT_WS(&#x27;||&#x27;, CAST(natural_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS natural_person_hash_key,
    source_load_timestamp AS source_load_timestamp_col,
    origin_sys,
    &#x27;crm_party_master_csv&#x27; AS source_tbl_name
  FROM crm_party_master_csv

  UNION ALL

  SELECT
    SHA2(CONCAT_WS(&#x27;||&#x27;, CAST(person_id AS STRING), CAST(origin_sys AS STRING)), 256) AS person_hash_key,
    SHA2(CONCAT_WS(&#x27;||&#x27;, CAST(person_id AS STRING), CAST(origin_sys AS STRING)), 256) AS natural_person_hash_key,
    source_load_timestamp AS source_load_timestamp_col,
    origin_sys,
    &#x27;sap_person_db&#x27; AS source_tbl_name
  FROM sap_person_db
)
SELECT
  SHA2(CONCAT_WS(&#x27;||&#x27;, CAST(person_hash_key AS STRING), CAST(natural_person_hash_key AS STRING), CAST(origin_sys AS STRING)), 256) AS person_natural_person_hash_key,
  CAST(source_load_timestamp_col AS TIMESTAMP) AS load_date,
  origin_sys AS record_source,
  person_hash_key,
  natural_person_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>with combined as (
SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, natural_ref))))AS person_natural_person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
    MD5(CONCAT(origin_sys, natural_ref)) AS natural_person_hash_key
FROM crm_party_master_csv
WHERE where party_kind = &#x27;NATURAL&#x27;
UNION ALL
SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, person_id)),MD5(CONCAT(origin_sys, person_id))))AS person_natural_person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, person_id)) AS person_hash_key,
    MD5(CONCAT(origin_sys, person_id)) AS natural_person_hash_key
FROM sap_person_db 
WHERE person_type = &#x27;NATURAL&#x27;)
select distinct * from combined;</code></pre></details></td><td>Notebook removes columns: `person_natural_person_hash_key`, `source_load_timestamp_col`, `source_tbl_name`.<br>Changed expressions: `load_date`: Python `cast(source_load_timestamp_col as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `natural_person_hash_key`: Python `sha2(concat_ws('||', cast(person_id as string), cast(origin_sys as string)), 256)`; Notebook `md5(concat(origin_sys, person_id))`; `person_hash_key`: Python `sha2(concat_ws('||', cast(person_id as string), cast(origin_sys as string)), 256)`; Notebook `md5(concat(origin_sys, person_id))`.</td></tr>
<tr><td><code>link_policy_broker</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    SHA2(CONCAT_WS(&#x27;||&#x27;, CAST(policy_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS policy_hash_key,
    SHA2(CONCAT_WS(&#x27;||&#x27;, CAST(src_agent_id AS STRING), CAST(src_system AS STRING)), 256) AS broker_hash_key,
    current_timestamp() AS load_ts,
    COALESCE(origin_sys, src_system) AS record_source
  FROM crm_policy_register_csv
  UNION ALL
  SELECT
    SHA2(CONCAT_WS(&#x27;||&#x27;, CAST(policy_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS policy_hash_key,
    SHA2(CONCAT_WS(&#x27;||&#x27;, CAST(src_agent_id AS STRING), CAST(src_system AS STRING)), 256) AS broker_hash_key,
    current_timestamp() AS load_ts,
    COALESCE(origin_sys, src_system) AS record_source
  FROM crm_broker_book_csv
)
SELECT
  SHA2(CONCAT_WS(&#x27;||&#x27;, CAST(policy_hash_key AS STRING), CAST(broker_hash_key AS STRING)), 256) AS policy_broker_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  record_source,
  policy_hash_key,
  broker_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, src_policy_id)),MD5(CONCAT(origin_sys, src_agent_id))))AS policy_broker_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, src_policy_id)) AS policy_hash_key,
    MD5(CONCAT(origin_sys, src_agent_id)) AS broker_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE source_extract = &#x27;policy_broker_bridge.csv&#x27;;</code></pre></details></td><td>FROM differs. Python: combined, crm_broker_book_csv, crm_policy_register_csv. Notebook: crm_enhanced_policy_relationships_csv.<br>Notebook removes columns: `load_ts`, `policy_broker_hash_key`.<br>Changed expressions: `broker_hash_key`: Python `sha2(concat_ws('||', cast(src_agent_id as string), cast(src_system as string)), 256)`; Notebook `md5(concat(origin_sys, src_agent_id))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `policy_hash_key`: Python `sha2(concat_ws('||', cast(policy_ref as string), cast(origin_sys as string)), 256)`; Notebook `md5(concat(origin_sys, src_policy_id))`; `record_source`: Python `coalesce(origin_sys, src_system)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_policy_channel</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(policy_ref, src_channel_id)) AS policy_channel_hash_key,
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(src_channel_id, src_system)) AS channel_hash_key,
    current_timestamp() AS load_ts,
    coalesce(origin_sys, src_system) AS record_source
  FROM crm_policy_register_csv
  CROSS JOIN crm_channel_catalog_csv
)
SELECT
  policy_channel_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  record_source,
  policy_hash_key,
  channel_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, src_policy_id)),MD5(CONCAT(origin_sys, src_channel_id))))AS policy_channel_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, src_policy_id)) AS policy_hash_key,
    MD5(CONCAT(origin_sys, src_channel_id)) AS channel_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE source_extract = &#x27;policy_channel_bridge.csv&#x27;;</code></pre></details></td><td>FROM differs. Python: combined, crm_policy_register_csv. Notebook: crm_enhanced_policy_relationships_csv.<br>Notebook removes columns: `load_ts`, `policy_channel_hash_key`.<br>Changed expressions: `channel_hash_key`: Python `md5(concat(src_channel_id, src_system))`; Notebook `md5(concat(origin_sys, src_channel_id))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `policy_hash_key`: Python `md5(concat(policy_ref, origin_sys))`; Notebook `md5(concat(origin_sys, src_policy_id))`; `record_source`: Python `coalesce(origin_sys, src_system)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_policy_customer</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    &#x27;crm_policy_register_csv&#x27; AS source_tbl_name
  FROM crm_policy_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    &#x27;crm_customer_portfolio_csv&#x27; AS source_tbl_name
  FROM crm_customer_portfolio_csv
)
SELECT
  md5(CONCAT(policy_hash_key, customer_hash_key)) AS policy_customer_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  customer_hash_key,
  policy_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, policy_ref)),MD5(CONCAT(origin_sys, customer_ref))))AS policy_customer_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, policy_ref)) AS policy_hash_key,
    MD5(CONCAT(origin_sys,customer_ref)) AS customer_hash_key
FROM crm_policy_register_csv;</code></pre></details></td><td>FROM differs. Python: combined, crm_customer_portfolio_csv, crm_policy_register_csv. Notebook: crm_policy_register_csv.<br>Notebook removes columns: `load_ts`, `policy_customer_hash_key`, `source_tbl_name`.<br>Changed expressions: `customer_hash_key`: Python `md5(concat(customer_ref, origin_sys))`; Notebook `md5(concat(origin_sys,customer_ref))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `policy_hash_key`: Python `md5(concat(policy_ref, origin_sys))`; Notebook `md5(concat(origin_sys, policy_ref))`; `record_source`: Python `coalesce(origin_sys, origin_sys)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_policy_insured_object</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
    source_load_timestamp AS source_load_timestamp,
    origin_sys,
    &#x27;crm_policy_register_csv&#x27; AS source_tbl_name
  FROM crm_policy_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
    source_load_timestamp AS source_load_timestamp,
    origin_sys,
    &#x27;crm_vehicle_asset_csv&#x27; AS source_tbl_name
  FROM crm_vehicle_asset_csv
  UNION ALL
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
    source_load_timestamp AS source_load_timestamp,
    origin_sys,
    &#x27;crm_property_asset_csv&#x27; AS source_tbl_name
  FROM crm_property_asset_csv
)
SELECT
  md5(CONCAT(policy_hash_key, insured_object_hash_key)) AS policy_insured_object_hash_key,
  CAST(source_load_timestamp AS TIMESTAMP) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  policy_hash_key,
  insured_object_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, src_policy_id)),MD5(CONCAT(origin_sys, src_insured_object_id))))AS policy_insured_object_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, src_policy_id)) AS policy_hash_key,
    MD5(CONCAT(origin_sys, src_insured_object_id)) AS insured_object_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE source_extract = &#x27;policy_insured_object_bridge.csv&#x27;;</code></pre></details></td><td>FROM differs. Python: combined, crm_policy_register_csv, crm_property_asset_csv, crm_vehicle_asset_csv. Notebook: crm_enhanced_policy_relationships_csv.<br>Notebook removes columns: `policy_insured_object_hash_key`, `source_load_timestamp`, `source_tbl_name`.<br>Changed expressions: `insured_object_hash_key`: Python `md5(concat(insured_object_id, origin_sys))`; Notebook `md5(concat(origin_sys, src_insured_object_id))`; `load_date`: Python `cast(source_load_timestamp as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `policy_hash_key`: Python `md5(concat(policy_ref, origin_sys))`; Notebook `md5(concat(origin_sys, src_policy_id))`; `record_source`: Python `coalesce(origin_sys, origin_sys)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_policy_override</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(policy_ref, src_override_id)) AS policy_override_hash_key,
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(src_override_id, src_system)) AS override_hash_key,
    etl_load_timestamp AS source_load_timestamp,
    COALESCE(origin_sys, src_system) AS record_source
  FROM crm_policy_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(policy_ref, src_override_id)) AS policy_override_hash_key,
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(src_override_id, src_system)) AS override_hash_key,
    etl_load_timestamp AS source_load_timestamp,
    COALESCE(origin_sys, src_system) AS record_source
  FROM crm_override_register_csv
)
SELECT
  policy_override_hash_key,
  CAST(source_load_timestamp AS TIMESTAMP) AS load_date,
  record_source,
  policy_hash_key,
  override_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, src_policy_id)),MD5(CONCAT(origin_sys, src_override_id))))AS policy_override_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, src_policy_id)) AS policy_hash_key,
    MD5(CONCAT(origin_sys, src_override_id)) AS override_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE source_extract = &#x27;policy_override_bridge.csv&#x27;;</code></pre></details></td><td>FROM differs. Python: combined, crm_override_register_csv, crm_policy_register_csv. Notebook: crm_enhanced_policy_relationships_csv.<br>Notebook removes columns: `policy_override_hash_key`, `source_load_timestamp`.<br>Changed expressions: `load_date`: Python `cast(source_load_timestamp as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `override_hash_key`: Python `md5(concat(src_override_id, src_system))`; Notebook `md5(concat(origin_sys, src_override_id))`; `policy_hash_key`: Python `md5(concat(policy_ref, origin_sys))`; Notebook `md5(concat(origin_sys, src_policy_id))`; `record_source`: Python `coalesce(origin_sys, src_system)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_policy_product</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(product_ref, origin_sys)) AS product_hash_key,
    current_timestamp() AS etl_load_timestamp,
    origin_sys,
    &#x27;link_policy_product&#x27; AS source_tbl_name
  FROM crm_policy_register_csv
  JOIN crm_product_catelog_csv
    ON crm_policy_register_csv.product_ref = crm_product_catelog_csv.product_ref
)
SELECT
  md5(CONCAT(policy_ref, product_ref)) AS policy_product_hash_key,
  CAST(etl_load_timestamp AS timestamp) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  policy_hash_key,
  product_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>with combined as (
SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, policy_ref)),MD5(CONCAT(origin_sys, product_ref))))AS policy_product_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, policy_ref)) AS policy_hash_key,
    MD5(CONCAT(origin_sys, product_ref)) AS product_hash_key
FROM crm_policy_register_csv
UNION ALL
SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, policy_id)),MD5(CONCAT(origin_sys, product_id))))AS policy_product_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, policy_id)) AS policy_hash_key,
    MD5(CONCAT(origin_sys, product_id)) AS product_hash_key
FROM sap_home_db
UNION ALL
SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, policy_id)),MD5(CONCAT(origin_sys, product_id))))AS policy_product_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, policy_id)) AS policy_hash_key,
    MD5(CONCAT(origin_sys, product_id)) AS product_hash_key
FROM sap_motor_db)
select distinct * from combined;</code></pre></details></td><td>FROM differs. Python: combined, crm_policy_register_csv. Notebook: combined, crm_policy_register_csv, sap_home_db, sap_motor_db.<br>Notebook removes columns: `etl_load_timestamp`, `policy_product_hash_key`, `source_tbl_name`.<br>Changed expressions: `load_date`: Python `cast(etl_load_timestamp as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `policy_hash_key`: Python `md5(concat(policy_ref, origin_sys))`; Notebook `md5(concat(origin_sys, policy_id))`; `product_hash_key`: Python `md5(concat(product_ref, origin_sys))`; Notebook `md5(concat(origin_sys, product_id))`; `record_source`: Python `coalesce(origin_sys, origin_sys)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_policy_quote</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
    load_timestamp AS source_load_timestamp_col,
    origin_sys,
    &#x27;crm_policy_register_csv&#x27; AS source_tbl_name
  FROM crm_policy_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
    load_timestamp AS source_load_timestamp_col,
    origin_sys,
    &#x27;crm_quote_register_csv&#x27; AS source_tbl_name
  FROM crm_quote_register_csv
)
SELECT
  md5(CONCAT(policy_ref, quote_ref)) AS policy_quote_hash_key,
  CAST(source_load_timestamp_col AS TIMESTAMP) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  policy_hash_key,
  quote_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>with combined as (
SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, policy_ref)),MD5(CONCAT(origin_sys, quote_ref))))AS policy_quote_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, policy_ref)) AS policy_hash_key,
    MD5(CONCAT(origin_sys, quote_ref)) AS quote_hash_key
FROM crm_policy_register_csv
UNION ALL
SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, src_policy_id)),MD5(CONCAT(origin_sys, src_quote_id))))AS policy_quote_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, src_policy_id)) AS policy_hash_key,
    MD5(CONCAT(origin_sys, src_quote_id)) AS quote_hash_key
FROM crm_enhanced_policy_relationships_csv where source_extract = &#x27;policy_quote_bridge.csv&#x27;)
select distinct * from combined;</code></pre></details></td><td>FROM differs. Python: combined, crm_policy_register_csv, crm_quote_register_csv. Notebook: combined, crm_enhanced_policy_relationships_csv, crm_policy_register_csv.<br>Notebook removes columns: `policy_quote_hash_key`, `source_load_timestamp_col`, `source_tbl_name`.<br>Changed expressions: `load_date`: Python `cast(source_load_timestamp_col as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `policy_hash_key`: Python `md5(concat(policy_ref, origin_sys))`; Notebook `md5(concat(origin_sys, src_policy_id))`; `quote_hash_key`: Python `md5(concat(quote_ref, origin_sys))`; Notebook `md5(concat(origin_sys, src_quote_id))`; `record_source`: Python `coalesce(origin_sys, origin_sys)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_quote_broker</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
    md5(CONCAT(src_agent_id, src_system)) AS broker_hash_key,
    current_timestamp() AS load_ts,
    COALESCE(origin_sys, src_system) AS record_source
  FROM crm_quote_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
    md5(CONCAT(src_agent_id, src_system)) AS broker_hash_key,
    current_timestamp() AS load_ts,
    COALESCE(origin_sys, src_system) AS record_source
  FROM crm_broker_book_csv
)
SELECT
  md5(CONCAT(quote_hash_key, broker_hash_key)) AS quote_broker_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  record_source,
  quote_hash_key,
  broker_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, src_quote_id)),MD5(CONCAT(origin_sys, src_agent_id))))AS quote_broker_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, src_quote_id)) AS quote_hash_key,
    MD5(CONCAT(origin_sys, src_agent_id)) AS broker_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE source_extract = &#x27;quote_broker_bridge.csv&#x27;;</code></pre></details></td><td>FROM differs. Python: combined, crm_broker_book_csv, crm_quote_register_csv. Notebook: crm_enhanced_policy_relationships_csv.<br>Notebook removes columns: `load_ts`, `quote_broker_hash_key`.<br>Changed expressions: `broker_hash_key`: Python `md5(concat(src_agent_id, src_system))`; Notebook `md5(concat(origin_sys, src_agent_id))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `quote_hash_key`: Python `md5(concat(quote_ref, origin_sys))`; Notebook `md5(concat(origin_sys, src_quote_id))`; `record_source`: Python `coalesce(origin_sys, src_system)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_quote_channel</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    SHA2(CONCAT_WS(&#x27;||&#x27;, CAST(quote_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS quote_hash_key,
    SHA2(CONCAT_WS(&#x27;||&#x27;, CAST(src_channel_id AS STRING), CAST(src_system AS STRING)), 256) AS channel_hash_key,
    load_timestamp AS source_load_timestamp_col,
    COALESCE(origin_sys, src_system) AS record_source
  FROM crm_quote_register_csv
  JOIN crm_channel_catalog_csv
    ON crm_quote_register_csv.src_channel_id = crm_channel_catalog_csv.src_channel_id
)
SELECT
  SHA2(CONCAT_WS(&#x27;||&#x27;, CAST(quote_hash_key AS STRING), CAST(channel_hash_key AS STRING)), 256) AS quote_channel_hash_key,
  CAST(source_load_timestamp_col AS TIMESTAMP) AS load_date,
  record_source,
  quote_hash_key,
  channel_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, src_quote_id)),MD5(CONCAT(origin_sys, src_channel_id))))AS quote_channel_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, src_quote_id)) AS quote_hash_key,
    MD5(CONCAT(origin_sys, src_channel_id)) AS channel_hash_key
FROM crm_enhanced_policy_relationships_csv
WHERE  source_extract = &#x27;quote_channel_bridge.csv&#x27;;</code></pre></details></td><td>FROM differs. Python: combined, crm_quote_register_csv. Notebook: crm_enhanced_policy_relationships_csv.<br>Notebook removes columns: `quote_channel_hash_key`, `source_load_timestamp_col`.<br>Changed expressions: `channel_hash_key`: Python `sha2(concat_ws('||', cast(src_channel_id as string), cast(src_system as string)), 256)`; Notebook `md5(concat(origin_sys, src_channel_id))`; `load_date`: Python `cast(source_load_timestamp_col as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `quote_hash_key`: Python `sha2(concat_ws('||', cast(quote_ref as string), cast(origin_sys as string)), 256)`; Notebook `md5(concat(origin_sys, src_quote_id))`; `record_source`: Python `coalesce(origin_sys, src_system)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_quote_person</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    &#x27;crm_quote_register_csv&#x27; AS source_tbl_name
  FROM crm_quote_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    &#x27;crm_party_master_csv&#x27; AS source_tbl_name
  FROM crm_party_master_csv
)
SELECT
  md5(CONCAT(quote_hash_key, person_hash_key)) AS quote_person_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  quote_hash_key,
  person_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, quote_ref))))AS quote_person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
    MD5(CONCAT(origin_sys,quote_ref)) AS quote_hash_key
FROM crm_quote_register_csv;</code></pre></details></td><td>FROM differs. Python: combined, crm_party_master_csv, crm_quote_register_csv. Notebook: crm_quote_register_csv.<br>Notebook removes columns: `load_ts`, `quote_person_hash_key`, `source_tbl_name`.<br>Changed expressions: `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `person_hash_key`: Python `md5(concat(party_ref, origin_sys))`; Notebook `md5(concat(origin_sys, party_ref))`; `quote_hash_key`: Python `md5(concat(quote_ref, origin_sys))`; Notebook `md5(concat(origin_sys,quote_ref))`; `record_source`: Python `coalesce(origin_sys, origin_sys)`; Notebook `origin_sys`.</td></tr>
<tr><td><code>link_quote_product</code></td><td><details><summary>Python build_sql</summary><pre><code>WITH combined AS (
  SELECT
    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
    md5(CONCAT(product_ref, origin_sys)) AS product_hash_key,
    current_timestamp() AS etl_load_timestamp,
    origin_sys,
    &#x27;crm_quote_register_csv&#x27; AS source_tbl_name
  FROM crm_quote_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
    md5(CONCAT(product_ref, origin_sys)) AS product_hash_key,
    current_timestamp() AS etl_load_timestamp,
    origin_sys,
    &#x27;crm_product_catelog_csv&#x27; AS source_tbl_name
  FROM crm_product_catelog_csv
)
SELECT
  md5(CONCAT(quote_hash_key, product_hash_key)) AS quote_product_hash_key,
  CAST(etl_load_timestamp AS timestamp) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  quote_hash_key,
  product_hash_key
FROM combined</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat( MD5(CONCAT(origin_sys, product_ref)),MD5(CONCAT(origin_sys, quote_ref))))AS quote_product_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    origin_sys AS record_source,
    MD5(CONCAT(origin_sys, product_ref)) AS product_hash_key,
    MD5(CONCAT(origin_sys,quote_ref)) AS quote_hash_key
FROM crm_quote_register_csv;</code></pre></details></td><td>FROM differs. Python: combined, crm_product_catelog_csv, crm_quote_register_csv. Notebook: crm_quote_register_csv.<br>Notebook removes columns: `etl_load_timestamp`, `quote_product_hash_key`, `source_tbl_name`.<br>Changed expressions: `load_date`: Python `cast(etl_load_timestamp as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `product_hash_key`: Python `md5(concat(product_ref, origin_sys))`; Notebook `md5(concat(origin_sys, product_ref))`; `quote_hash_key`: Python `md5(concat(quote_ref, origin_sys))`; Notebook `md5(concat(origin_sys,quote_ref))`; `record_source`: Python `coalesce(origin_sys, origin_sys)`; Notebook `origin_sys`.</td></tr>
</tbody>
</table>

## Satellite

<table>
<thead><tr><th>Table</th><th>Source 1: Python</th><th>Source 2: Notebook</th><th>Difference Compared With Notebook</th></tr></thead>
<tbody>
<tr><td><code>sat_account_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(account_ref, origin_sys)) AS account_hash_key,
  current_timestamp() AS load_date,
  account_no AS account_number,
  account_type_txt AS account_type,
  last_access_dt AS account_last_access,
  last_change_dt AS account_last_change,
  account_create_type_txt AS account_creation_type,
  account_status_txt AS account_status,
  &#x27;sat_account_crm&#x27; AS source_tbl_name,
  origin_sys AS record_source
FROM crm_account_book_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, account_ref)) AS account_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    account_no AS account_number,
    account_type_txt AS account_type,
    last_access_dt AS account_last_access,
    last_change_dt AS account_last_change,
    account_create_type_txt AS account_creation_type,
    account_status_txt AS account_status
FROM crm_account_book_csv;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `account_hash_key`: Python `md5(concat(account_ref, origin_sys))`; Notebook `md5(concat(origin_sys, account_ref))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>sat_address_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(address_ref, origin_sys)) AS address_hash_key,
  current_timestamp() AS load_date,
  street_txt AS street,
  postal_cd AS postcode,
  city_nm AS city,
  state_cd AS state,
  country_cd AS country,
  address_type_txt AS type,
  region_txt AS region,
  origin_sys AS record_source,
  &#x27;sat_address_crm&#x27; AS source_tbl_name
FROM crm_address_book_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, address_ref)) AS address_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    street_txt AS street,
    postal_cd AS postcode,
    city_nm AS city,
    state_cd AS state,
    country_cd AS country,
    address_type_txt AS type,
    region_txt AS region
FROM crm_address_book_csv;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `address_hash_key`: Python `md5(concat(address_ref, origin_sys))`; Notebook `md5(concat(origin_sys, address_ref))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>sat_address_sap</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(address_id, origin_sys)) AS address_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  address_line_1 AS address_line_1,
  address_line_2 AS address_line_2,
  city AS city,
  state AS state,
  country AS country,
  zipcode AS zipcode,
  person_id AS person_id,
  origin_sys AS record_source,
  &#x27;sat_address_sap&#x27; AS source_tbl_name
FROM sap_address_db</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, address_id)) AS address_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    person_id AS person_id,
    address_line_1 AS address_line_1,
    address_line_2 AS address_line_2,
    city AS city,
    state AS state,
    country AS country,
    zipcode AS zipcode
FROM sap_address_db;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `address_hash_key`: Python `md5(concat(address_id, origin_sys))`; Notebook `md5(concat(origin_sys, address_id))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>sat_broker_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(src_agent_id, src_system)) AS broker_hash_key,
  current_timestamp() AS load_date,
  src_agent_name AS agent_name,
  src_agent_type AS agent_type,
  src_agent_status AS agent_status,
  src_agent_license_number AS agent_license_number,
  src_agent_net_promoter_score AS agent_net_promoter_score,
  src_agent_commission_percentage AS agent_commission_percentage,
  src_system AS record_source,
  &#x27;sat_broker_crm&#x27; AS source_tbl_name
FROM crm_broker_book_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT 
    md5(concat(origin_sys, src_agent_id)) AS broker_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    src_agent_name AS agent_name,
    src_agent_type AS agent_type,
    src_agent_status AS agent_status,
    src_agent_license_number AS agent_license_number,
    src_agent_net_promoter_score AS agent_net_promoter_score,
    src_agent_commission_percentage AS agent_commission_percentage
FROM crm_broker_book_csv;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `broker_hash_key`: Python `md5(concat(src_agent_id, src_system))`; Notebook `md5(concat(origin_sys, src_agent_id))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>sat_campaign_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(src_campaign_id, src_system)) AS campaign_hash_key,
  current_timestamp() AS load_date,
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
  src_number_of_category_mentions AS number_of_category_mentions,
  src_system AS record_source,
  &#x27;sat_campaign_crm&#x27; AS source_tbl_name
FROM crm_campaign_register_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT 
    md5(concat(origin_sys, src_campaign_id)) AS campaign_hash_key,
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
FROM crm_campaign_register_csv;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `campaign_hash_key`: Python `md5(concat(src_campaign_id, src_system))`; Notebook `md5(concat(origin_sys, src_campaign_id))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>sat_channel_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(src_channel_id, src_system)) AS channel_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  src_channel_name AS channel_name,
  src_channel_type AS channel_type,
  src_system AS record_source,
  &#x27;sat_channel_crm&#x27; AS source_tbl_name
FROM crm_channel_catalog_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT 
    md5(concat(origin_sys, src_channel_id)) AS channel_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    src_channel_name AS channel_name,
    src_channel_type AS channel_type
FROM crm_channel_catalog_csv;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `channel_hash_key`: Python `md5(concat(src_channel_id, src_system))`; Notebook `md5(concat(origin_sys, src_channel_id))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>sat_claim_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(src_claim_id, src_system)) AS claim_hash_key,
  current_timestamp() AS load_date,
  src_claim_number AS claim_number,
  src_claim_type AS claim_type,
  src_claim_status AS claim_status,
  src_claim_reason AS claim_reason,
  src_claim_channel AS claim_channel,
  src_claim_handler AS claim_handler,
  to_timestamp(src_claim_reported_date) AS claim_reported_date,
  to_timestamp(src_claim_settlement_date) AS claim_settlement_date,
  src_claim_product AS claim_product,
  src_is_claim_suspicious AS is_claim_suspicious,
  src_is_claim_fraud AS is_claim_fraud,
  src_claim_fraud_status AS claim_fraud_status,
  src_claim_fraud_type AS claim_fraud_type,
  src_claim_fraud_detection_method AS claim_fraud_detection_method,
  src_is_litigation AS is_litigation,
  src_litigation_reason AS litigation_reason,
  to_timestamp(src_litigation_start_date) AS litigation_start_date,
  to_timestamp(src_litigation_end_date) AS litigation_end_date,
  src_litigation_outcome AS litigation_outcome,
  src_litigation_duration_days AS litigation_duration_days,
  src_claim_fraud_detection_time_in_days AS claim_fraud_detection_time_in_days,
  src_is_recovery_opportunity AS is_recovery_opportunity,
  src_recovery_priority_score AS recovery_priority_score,
  src_recovery_category AS recovery_category,
  src_recovery_source AS recovery_source,
  to_timestamp(src_first_recovery_date) AS first_recovery_date,
  to_timestamp(src_last_recovery_date) AS last_recovery_date,
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
  src_is_claim_complaint_raised AS is_claim_complaint_raised,
  src_system AS record_source,
  &#x27;sat_claim_crm&#x27; AS source_tbl_name
FROM crm_claim_register_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT 
    md5(concat(origin_sys, src_claim_id)) AS claim_hash_key,
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
FROM crm_claim_register_csv;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `claim_hash_key`: Python `md5(concat(src_claim_id, src_system))`; Notebook `md5(concat(origin_sys, src_claim_id))`; `claim_reported_date`: Python `to_timestamp(src_claim_reported_date)`; Notebook `src_claim_reported_date`; `claim_settlement_date`: Python `to_timestamp(src_claim_settlement_date)`; Notebook `src_claim_settlement_date`; `first_recovery_date`: Python `to_timestamp(src_first_recovery_date)`; Notebook `src_first_recovery_date`; `last_recovery_date`: Python `to_timestamp(src_last_recovery_date)`; Notebook `src_last_recovery_date`; `litigation_end_date`: Python `to_timestamp(src_litigation_end_date)`; Notebook `src_litigation_end_date`; plus 2 more changed expressions.</td></tr>
<tr><td><code>sat_complaint_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(src_complaint_id, src_system)) AS complaint_hash_key,
  current_timestamp() AS load_date,
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
  src_customer_complaint_satisfaction_score AS customer_complaint_satisfaction_score,
  &#x27;sat_complaint_crm&#x27; AS source_tbl_name
FROM crm_complaint_register_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT 
    md5(concat(origin_sys, src_complaint_id)) AS complaint_hash_key,
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
    src_customer_complaint_satisfaction_score AS customer_complaint_satisfaction_score,
    src_complaint_feedback AS complaint_feedback
FROM crm_complaint_register_csv;</code></pre></details></td><td>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `complaint_hash_key`: Python `md5(concat(src_complaint_id, src_system))`; Notebook `md5(concat(origin_sys, src_complaint_id))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>sat_consent_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(consent_ref, origin_sys)) AS consent_hash_key,
  current_timestamp() AS load_date,
  opt_in_valid_ind AS opt_in_validated,
  opt_in_legit_ind AS opt_in_legitimate_interest,
  origin_sys AS record_source,
  &#x27;sat_consent_crm&#x27; AS source_tbl_name
FROM crm_consent_snapshot_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, consent_ref)) AS consent_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    opt_in_valid_ind AS opt_in_validated,
    opt_in_legit_ind AS opt_in_legitimate_interest
FROM crm_consent_snapshot_csv;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `consent_hash_key`: Python `md5(concat(consent_ref, origin_sys))`; Notebook `md5(concat(origin_sys, consent_ref))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>sat_contact_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(contact_ref, origin_sys)) AS contact_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  email_home_txt AS personal_email,
  email_work_txt AS work_email,
  phone_work_txt AS work_phone,
  phone_home_txt AS home_phone,
  origin_sys AS record_source,
  &#x27;sat_contact_crm&#x27; AS source_tbl_name
FROM crm_contact_point_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, contact_ref)) AS contact_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    email_home_txt AS personal_email,
    email_work_txt AS work_email,
    phone_work_txt AS work_phone,
    phone_home_txt AS home_phone
FROM crm_contact_point_csv;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `contact_hash_key`: Python `md5(concat(contact_ref, origin_sys))`; Notebook `md5(concat(origin_sys, contact_ref))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>sat_customer_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
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
  customer_onboarding_feedback AS customer_onboarding_feedback,
  origin_sys AS record_source,
  &#x27;crm_customer_portfolio_csv&#x27; AS source_tbl_name
FROM crm_customer_portfolio_csv

UNION ALL

SELECT DISTINCT
  md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
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
  NULL AS customer_onboarding_feedback,
  origin_sys AS record_source,
  &#x27;crm_enhanced_enrichments_csv&#x27; AS source_tbl_name
FROM crm_enhanced_enrichments_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>with combined as (
    SELECT DISTINCT 
    md5(concat(s.origin_sys, s.customer_ref)) AS customer_hash_key,
    CAST(s.pull_ts AS timestamp) AS load_date,
    s.customer_no AS customer_number,
    s.customer_status_txt AS customer_status,
    s.customer_status_reason_txt AS customer_status_reason,
    s.customer_since_dt AS customer_since,
    s.customer_rating_no AS customer_rating,
    s.customer_segment_txt AS customer_segment,
    s.lob_txt AS line_of_business,
    s.nps_score_no AS nps_score,
    p.src_income_band as income_band,
    p.src_customer_satisfaction as customer_satisfaction,
    p.src_customer_age_band as customer_age_band,
    p.src_net_promotor_code_segment as net_promotor_code_segment,
    s.customer_onboarding_satisfaction_score AS customer_onboarding_satisfaction_score,
    s.customer_onboarding_feedback AS customer_onboarding_feedback 
    FROM crm_customer_portfolio_csv s 
    left join crm_enhanced_enrichments_csv p
    on s.customer_ref = p.src_customer_ref
)
SELECT DISTINCT * FROM combined;</code></pre></details></td><td>FROM differs. Python: crm_customer_portfolio_csv, crm_enhanced_enrichments_csv. Notebook: combined, crm_customer_portfolio_csv.<br>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `customer_age_band`: Python `src_customer_age_band`; Notebook `p.src_customer_age_band`; `customer_hash_key`: Python `md5(concat(customer_ref, origin_sys))`; Notebook `md5(concat(s.origin_sys, s.customer_ref))`; `customer_number`: Python `null`; Notebook `s.customer_no`; `customer_onboarding_feedback`: Python `null`; Notebook `s.customer_onboarding_feedback`; `customer_onboarding_satisfaction_score`: Python `null`; Notebook `s.customer_onboarding_satisfaction_score`; `customer_rating`: Python `null`; Notebook `s.customer_rating_no`; plus 10 more changed expressions.</td></tr>
<tr><td><code>sat_home_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(property_ref, origin_sys)) AS home_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  wall_material_txt AS wall_construction,
  risk_address_txt AS home_risk_address,
  roof_material_txt AS roof_construction,
  property_type_txt AS home_type,
  property_state_cd AS home_state,
  existing_home_ind AS is_existing_home_customer,
  &#x27;sat_home_crm&#x27; AS source_tbl_name
FROM crm_property_asset_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, property_ref)) AS home_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    wall_material_txt AS wall_construction,
    risk_address_txt AS home_risk_address,
    roof_material_txt AS roof_construction,
    property_type_txt AS home_type,
    property_state_cd AS home_state,
    existing_home_ind AS is_existing_home_customer
FROM crm_property_asset_csv;</code></pre></details></td><td>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `home_hash_key`: Python `md5(concat(property_ref, origin_sys))`; Notebook `md5(concat(origin_sys, property_ref))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>sat_home_sap</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(home_id, origin_sys)) AS home_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  home_type,
  home_location,
  roof_material,
  wall_type,
  NULL AS policy_id,
  NULL AS product_id,
  origin_sys AS record_source,
  &#x27;sat_home_sap&#x27; AS source_tbl_name
FROM sap_home_db
UNION ALL
SELECT DISTINCT
  md5(CONCAT(policy_id, origin_sys)) AS home_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  NULL AS home_type,
  NULL AS home_location,
  NULL AS roof_material,
  NULL AS wall_type,
  policy_id,
  product_id,
  origin_sys AS record_source,
  &#x27;sat_home_sap&#x27; AS source_tbl_name
FROM sap_motor_db</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, home_id)) AS home_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    policy_id AS policy_id,
    product_id AS product_id,
    home_type AS home_type,
    home_location AS home_location,
    wall_type AS wall_type,
    roof_material AS roof_material
FROM sap_home_db;</code></pre></details></td><td>FROM differs. Python: sap_home_db, sap_motor_db. Notebook: sap_home_db.<br>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `home_hash_key`: Python `md5(concat(policy_id, origin_sys))`; Notebook `md5(concat(origin_sys, home_id))`; `home_location`: Python `null`; Notebook `home_location`; `home_type`: Python `null`; Notebook `home_type`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `policy_id`: Python `null`; Notebook `policy_id`; `product_id`: Python `null`; Notebook `product_id`; plus 2 more changed expressions.</td></tr>
<tr><td><code>sat_identities_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(identity_ref, origin_sys)) AS identities_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  ecid_txt AS ecid,
  hashed_email_txt AS hashed_email,
  origin_sys AS record_source,
  &#x27;sat_identities_crm&#x27; AS source_tbl_name
FROM crm_identity_registry_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, identity_ref)) AS identities_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    ecid_txt AS ecid,
    hashed_email_txt AS hashed_email
FROM crm_identity_registry_csv;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `identities_hash_key`: Python `md5(concat(identity_ref, origin_sys))`; Notebook `md5(concat(origin_sys, identity_ref))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>sat_insured_object</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
  current_timestamp() AS load_date,
  insured_object_type,
  insured_object_sub_type,
  insured_object_description,
  insured_value,
  currency_code,
  insured_object_start_date,
  insured_object_end_date,
  insured_object_current_status,
  origin_sys AS record_source,
  &#x27;sat_insured_object&#x27; AS source_tbl_name
FROM crm_property_asset_csv

UNION ALL

SELECT DISTINCT
  md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
  current_timestamp() AS load_date,
  insured_object_type,
  insured_object_sub_type,
  insured_object_description,
  insured_value,
  currency_code,
  insured_object_start_date,
  insured_object_end_date,
  insured_object_current_status,
  origin_sys AS record_source,
  &#x27;sat_insured_object&#x27; AS source_tbl_name
FROM crm_vehicle_asset_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>with combined as (
SELECT DISTINCT
    md5(concat(origin_sys, insured_object_id)) AS insured_object_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    insured_object_type AS insured_object_type,
    insured_object_sub_type AS insured_object_sub_type,
    insured_object_description AS insured_object_description,
    insured_value AS insured_value,
    currency_code AS currency_code,
    insured_object_start_date AS insured_object_start_date,
    insured_object_end_date AS insured_object_end_date,
    insured_object_current_status AS insured_object_current_status
FROM crm_property_asset_csv
UNION ALL
SELECT DISTINCT
    md5(concat(origin_sys, insured_object_id)) AS insured_object_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    insured_object_type AS insured_object_type,
    insured_object_sub_type AS insured_object_sub_type,
    insured_object_description AS insured_object_description,
    insured_value AS insured_value,
    currency_code AS currency_code,
    insured_object_start_date AS insured_object_start_date,
    insured_object_end_date AS insured_object_end_date,
    insured_object_current_status AS insured_object_current_status
FROM crm_vehicle_asset_csv)
select * from combined;</code></pre></details></td><td>FROM differs. Python: crm_property_asset_csv, crm_vehicle_asset_csv. Notebook: combined, crm_property_asset_csv, crm_vehicle_asset_csv.<br>Notebook adds columns: `currency_code`, `insured_object_current_status`, `insured_object_description`, `insured_object_end_date`, `insured_object_start_date`, `insured_object_sub_type`, `insured_object_type`, `insured_value`.<br>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `insured_object_hash_key`: Python `md5(concat(insured_object_id, origin_sys))`; Notebook `md5(concat(origin_sys, insured_object_id))`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>sat_lead_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(lead_ref, origin_sys)) AS lead_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  interest_bucket AS interested_level,
  contact_pref AS preferred_contact_method,
  person_score_no AS person_score,
  person_status_txt AS person_status,
  converted_dt AS converted_date,
  origin_sys AS record_source,
  &#x27;sat_lead_crm&#x27; AS source_tbl_name
FROM crm_lead_register_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, lead_ref)) AS lead_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    interest_bucket AS interested_level,
    contact_pref AS preferred_contact_method,
    person_score_no AS person_score,
    person_status_txt AS person_status,
    converted_dt AS converted_date
FROM crm_lead_register_csv;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `lead_hash_key`: Python `md5(concat(lead_ref, origin_sys))`; Notebook `md5(concat(origin_sys, lead_ref))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>sat_legal_person_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(legal_ref, origin_sys)) AS legal_person_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  legal_job_title_txt AS job_title,
  lead_conv_dt AS converted_date,
  legal_status_txt AS person_status,
  legal_score_no AS person_score,
  legal_name AS company_name,
  legal_src_type AS source_type,
  legal_src_ref AS source_id,
  constitution_dt AS date_of_constitution,
  origin_sys AS record_source,
  &#x27;sat_legal_person_crm&#x27; AS source_tbl_name
FROM crm_party_master_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, legal_ref)) AS legal_person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    legal_job_title_txt AS job_title,
    CAST(lead_conv_dt AS timestamp) AS converted_date,
    legal_status_txt AS person_status,
    legal_score_no AS person_score,
    legal_name AS company_name,
    legal_src_type AS source_type,
    legal_src_ref AS source_id,
    to_date(constitution_dt,&#x27;y-M-d&#x27;) AS date_of_constitution
From crm_party_master_csv where party_kind = &#x27;LEGAL&#x27;;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `converted_date`: Python `lead_conv_dt`; Notebook `cast(lead_conv_dt as timestamp)`; `date_of_constitution`: Python `constitution_dt`; Notebook `to_date(constitution_dt,'y-m-d')`; `legal_person_hash_key`: Python `md5(concat(legal_ref, origin_sys))`; Notebook `md5(concat(origin_sys, legal_ref))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`.</td></tr>
<tr><td><code>sat_legal_person_sap</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(person_id, origin_sys)) AS legal_person_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  organization AS organization,
  org_establishment_date AS org_establishment_date,
  origin_sys AS record_source,
  &#x27;sat_legal_person_sap&#x27; AS source_tbl_name
FROM sap_person_db</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, person_id)) AS legal_person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    organization as organization,
    to_date(org_establishment_date, &#x27;y-M-d&#x27;) as org_establishment_date
From sap_person_db where person_type = &#x27;LEGAL&#x27;;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `legal_person_hash_key`: Python `md5(concat(person_id, origin_sys))`; Notebook `md5(concat(origin_sys, person_id))`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `org_establishment_date`: Python `org_establishment_date`; Notebook `to_date(org_establishment_date, 'y-m-d')`.</td></tr>
<tr><td><code>sat_marketing_engagement_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(src_marketing_engagement_ref, origin_sys)) AS marketing_engagement_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  src_promotion_code AS promotion_code,
  src_opened_email AS opened_email,
  src_marketing_status AS marketing_status,
  NULL AS has_retention_team_interaction,
  NULL AS customer_service_call_frequency,
  NULL AS average_call_sentiment,
  NULL AS engagement_score,
  NULL AS first_contact_resolution,
  origin_sys AS record_source,
  &#x27;sat_marketing_engagement_crm&#x27; AS source_tbl_name
FROM crm_enhanced_enrichments_csv

UNION ALL

SELECT DISTINCT
  md5(CONCAT(src_marketing_engagement_ref, origin_sys)) AS marketing_engagement_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  NULL AS promotion_code,
  NULL AS opened_email,
  NULL AS marketing_status,
  has_retention_team_interaction AS has_retention_team_interaction,
  customer_service_call_frequency AS customer_service_call_frequency,
  average_call_sentiment AS average_call_sentiment,
  engagement_score AS engagement_score,
  first_contact_resolution AS first_contact_resolution,
  origin_sys AS record_source,
  &#x27;sat_marketing_engagement_crm&#x27; AS source_tbl_name
FROM crm_campaign_touch_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, engagement_ref)) AS marketing_engagement_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    promo_cd AS promotion_code,
    email_opened_ind AS opened_email,
    campaign_status_txt AS marketing_status,
    has_retention_team_interaction AS has_retention_team_interaction,
    customer_service_call_frequency AS customer_service_call_frequency,
    average_call_sentiment AS average_call_sentiment,
    engagement_score AS engagement_score,
    first_contact_resolution AS first_contact_resolution
FROM crm_campaign_touch_csv;</code></pre></details></td><td>FROM differs. Python: crm_campaign_touch_csv, crm_enhanced_enrichments_csv. Notebook: crm_campaign_touch_csv.<br>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `marketing_engagement_hash_key`: Python `md5(concat(src_marketing_engagement_ref, origin_sys))`; Notebook `md5(concat(origin_sys, engagement_ref))`; `marketing_status`: Python `null`; Notebook `campaign_status_txt`; `opened_email`: Python `null`; Notebook `email_opened_ind`; `promotion_code`: Python `null`; Notebook `promo_cd`.</td></tr>
<tr><td><code>sat_marketing_preference_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(preference_ref, origin_sys)) AS marketing_preference_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  sms_ind AS sms,
  email_ind AS email,
  email_sub_ind AS email_subscriptions,
  call_ind AS call,
  any_ind AS any,
  commercial_email_ind AS commercial_email,
  postal_mail_ind AS postal_mail,
  origin_sys AS record_source,
  &#x27;sat_marketing_preference_crm&#x27; AS source_tbl_name
FROM crm_comm_preference_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, preference_ref)) AS marketing_preference_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    sms_ind AS sms,
    email_ind AS email,
    email_sub_ind AS email_subscriptions,
    call_ind AS call,
    any_ind AS any,
    commercial_email_ind AS commercial_email,
    postal_mail_ind AS postal_mail
FROM crm_comm_preference_csv;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `marketing_preference_hash_key`: Python `md5(concat(preference_ref, origin_sys))`; Notebook `md5(concat(origin_sys, preference_ref))`.</td></tr>
<tr><td><code>sat_motor_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(CAST(vehicle_ref AS STRING), CAST(origin_sys AS STRING))) AS motor_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
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
  driver_experience_years AS driver_experience_years,
  origin_sys AS record_source,
  &#x27;sat_motor_crm&#x27; AS source_tbl_name
FROM crm_vehicle_asset_csv
UNION ALL
SELECT DISTINCT
  md5(CONCAT(CAST(vehicle_ref AS STRING), CAST(origin_sys AS STRING))) AS motor_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  CAST(NULL AS STRING) AS auto_decline_vehicle,
  CAST(NULL AS STRING) AS body_type,
  CAST(NULL AS STRING) AS fuel_type,
  CAST(NULL AS STRING) AS license_status,
  CAST(NULL AS STRING) AS is_existing_motor_customer,
  CAST(NULL AS BIGINT) AS motor_lapsed_policies,
  CAST(NULL AS STRING) AS motor_risk_address,
  CAST(NULL AS STRING) AS risk_class_code,
  CAST(NULL AS STRING) AS variant,
  CAST(NULL AS STRING) AS vehicle_owner_type,
  CAST(NULL AS STRING) AS vehicle_regstate,
  CAST(NULL AS STRING) AS vehicle_class,
  CAST(NULL AS STRING) AS vehicle_model,
  CAST(NULL AS STRING) AS vehicle_type,
  CAST(NULL AS DECIMAL(18,2)) AS motor_sum_insrd,
  CAST(NULL AS BIGINT) AS vehicle_year,
  src_vehicle_age AS vehicle_age,
  CAST(NULL AS BIGINT) AS driver_experience_years,
  origin_sys AS record_source,
  &#x27;sat_motor_crm&#x27; AS source_tbl_name
FROM crm_enhanced_enrichments_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, vehicle_ref)) AS motor_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
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
    manufacture_yr AS vehicle_year,
    vehicle_age_yrs AS vehicle_age,
    driver_experience_years AS driver_experience_years
FROM crm_vehicle_asset_csv;</code></pre></details></td><td>FROM differs. Python: crm_enhanced_enrichments_csv, crm_vehicle_asset_csv. Notebook: crm_vehicle_asset_csv.<br>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `auto_decline_vehicle`: Python `cast(null as string)`; Notebook `auto_decline_ind`; `body_type`: Python `cast(null as string)`; Notebook `body_style_txt`; `driver_experience_years`: Python `cast(null as bigint)`; Notebook `driver_experience_years`; `fuel_type`: Python `cast(null as string)`; Notebook `fuel_type_txt`; `is_existing_motor_customer`: Python `cast(null as string)`; Notebook `existing_motor_ind`; `license_status`: Python `cast(null as string)`; Notebook `license_status_txt`; plus 14 more changed expressions.</td></tr>
<tr><td><code>sat_motor_sap</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(motor_id, origin_sys)) AS motor_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  motor_class,
  motor_model,
  motor_type,
  fuel_type,
  motor_parked_location,
  manufacturing_date,
  gear_type,
  body_colour,
  policy_id,
  product_id,
  origin_sys AS record_source,
  &#x27;sat_motor_sap&#x27; AS source_tbl_name
FROM sap_motor_db</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, motor_id)) AS motor_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
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
FROM sap_motor_db;</code></pre></details></td><td>Notebook adds columns: `body_colour`, `fuel_type`, `gear_type`, `manufacturing_date`, `motor_class`, `motor_model`, `motor_parked_location`, `motor_type`, `policy_id`, `product_id`.<br>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `motor_hash_key`: Python `md5(concat(motor_id, origin_sys))`; Notebook `md5(concat(origin_sys, motor_id))`.</td></tr>
<tr><td><code>sat_natural_person_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(natural_ref, origin_sys)) AS natural_person_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
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
  job_title_txt AS job_title,
  origin_sys AS record_source,
  &#x27;sat_natural_person_crm&#x27; AS source_tbl_name
FROM crm_party_master_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, natural_ref)) AS natural_person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    given_nm AS first_name,
    family_nm AS last_name,
    display_nm AS full_name,
    title_txt AS courtesy_title,
    role_txt AS role,
    occupation_txt AS occupation,
    to_date(dob, &#x27;y-M-d&#x27;) AS birth_date,
    cast(birth_yr AS int) AS birth_year,
    nationality_txt AS nationality,
    gender_txt AS gender,
    marital_txt AS marital_status,
    disability_degree AS assesed_disability_degree,
    language_pref AS preferred_language,
    job_title_txt AS job_title
FROM crm_party_master_csv
where party_kind = &#x27;NATURAL&#x27;;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `birth_date`: Python `dob`; Notebook `to_date(dob, 'y-m-d')`; `birth_year`: Python `birth_yr`; Notebook `cast(birth_yr as int)`; `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `natural_person_hash_key`: Python `md5(concat(natural_ref, origin_sys))`; Notebook `md5(concat(origin_sys, natural_ref))`.</td></tr>
<tr><td><code>sat_natural_person_sap</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(person_id, origin_sys)) AS natural_person_hash_key,
  current_timestamp() AS load_date,
  first_name AS first_name,
  middle_name AS middle_name,
  last_name AS last_name,
  date_of_birth AS date_of_birth,
  gender AS gender,
  occupation AS occupation,
  origin_sys AS record_source,
  &#x27;sat_natural_person_sap&#x27; AS source_tbl_name
FROM sap_person_db</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, person_id)) AS natural_person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    first_name AS first_name,
    middle_name AS middle_name,
    last_name AS last_name,
    to_date(date_of_birth, &#x27;y-M-d&#x27;) AS date_of_birth,
    gender AS gender,
    occupation AS occupation
FROM sap_person_db
where person_type = &#x27;NATURAL&#x27;;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `date_of_birth`: Python `date_of_birth`; Notebook `to_date(date_of_birth, 'y-m-d')`; `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `natural_person_hash_key`: Python `md5(concat(person_id, origin_sys))`; Notebook `md5(concat(origin_sys, person_id))`.</td></tr>
<tr><td><code>sat_override_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(src_override_id, src_system)) AS override_hash_key,
  current_timestamp() AS load_date,
  src_override_reason AS override_reason,
  src_system AS record_source,
  &#x27;sat_override_crm&#x27; AS source_tbl_name
FROM crm_override_register_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, src_override_id)) AS override_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    src_override_reason AS override_reason
FROM crm_override_register_csv;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `override_hash_key`: Python `md5(concat(src_override_id, src_system))`; Notebook `md5(concat(origin_sys, src_override_id))`.</td></tr>
<tr><td><code>sat_person_CRM</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
  current_timestamp() AS load_date,
  tenant_cd AS tenant_id,
  lead_ind AS is_lead,
  party_kind AS type,
  paperless_ind AS operational_paperless_consent,
  src_party_ref AS source_id,
  src_party_type AS source_type,
  origin_sys AS record_source,
  &#x27;sat_person_CRM&#x27; AS source_tbl_name
FROM crm_party_master_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, party_ref)) AS person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    party_kind AS type,
    tenant_cd AS tenant_id,
    lead_ind AS is_lead,
    paperless_ind AS operational_paperless_consent,
    src_party_ref AS source_id,
    src_party_type AS source_type
FROM crm_party_master_csv;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `person_hash_key`: Python `md5(concat(party_ref, origin_sys))`; Notebook `md5(concat(origin_sys, party_ref))`.</td></tr>
<tr><td><code>sat_person_SAP</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(person_id, origin_sys)) AS person_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  person_type,
  email_address,
  phone_number,
  origin_sys AS record_source,
  &#x27;sat_person_SAP&#x27; AS source_tbl_name
FROM sap_person_db</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, person_id)) AS person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    person_type AS person_type,
    email_address AS email_address,
    phone_number AS phone_number
FROM sap_person_db;</code></pre></details></td><td>Notebook adds columns: `email_address`, `person_type`, `phone_number`.<br>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `person_hash_key`: Python `md5(concat(person_id, origin_sys))`; Notebook `md5(concat(origin_sys, person_id))`.</td></tr>
<tr><td><code>sat_policy_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
  current_timestamp() AS load_date,
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
  crm_policy_register_csv AS is_renewal_escalation,
  origin_sys AS record_source,
  &#x27;sat_policy_crm&#x27; AS source_tbl_name
FROM crm_policy_register_csv

UNION ALL

SELECT DISTINCT
  md5(CONCAT(src_quote_id, origin_sys)) AS policy_hash_key,
  current_timestamp() AS load_date,
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
  NULL AS is_renewal_escalation,
  origin_sys AS record_source,
  &#x27;sat_policy_crm&#x27; AS source_tbl_name
FROM crm_enhanced_enrichments_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>WITH combined AS (
    SELECT DISTINCT
        md5(concat(s.origin_sys, s.policy_ref)) AS policy_hash_key,
        CAST(s.pull_ts AS timestamp) AS load_date,
        s.cover_option_txt AS cover_option,
        s.declined_claim_cnt AS declined_claims,
        s.fraud_ind AS fraud_flag,
        s.gross_amt AS gross_revenue,
        s.net_amt AS net_revenue,
        s.active_claim_cnt AS number_of_active_claim,
        s.previous_claim_cnt AS number_of_previous_claim,
        s.policy_cycle_no AS policy_cycle,
        s.policy_end_dt AS policy_end_date,
        s.policy_term_months AS policy_length,
        s.policy_no AS policy_number,
        s.policy_start_dt AS policy_start_date,
        s.policy_status_txt AS policy_status,
        s.renewal_premium_curr AS renewal_amount_current_period,
        s.renewal_premium_next AS renewal_amount_next_period,
        s.renewal_dt AS renewal_date,
        s.sales_channel_txt AS sales_channel,
        p.src_quote_id AS quote_id,
        p.src_policy_type AS policy_type,
        p.src_policy_issue_date AS policy_issue_date,
        p.src_is_policy_renewal AS is_policy_renewal,
        p.src_policy_cancellation_reason AS policy_cancellation_reason,
        p.src_policy_sum_insured AS policy_sum_insured,
        p.src_policy_retention_limit AS policy_retention_limit,
        p.src_policy_risk_score AS policy_risk_score,
        p.src_policy_risk_band AS policy_risk_band,
        p.src_policy_base_premium AS policy_base_premium,
        p.src_gross_written_premium AS gross_written_premium,
        p.src_operating_expenses AS earned_premium,
        p.src_incurred_but_not_reported AS incurred_but_not_reported,
        p.src_operating_expenses AS operating_expenses,
        p.src_administrative_expenses AS administrative_expenses,
        p.src_profit_margin AS profit_margin,
        p.src_taxes_and_levies AS taxes_and_levies,
        p.src_amount_approved AS amount_approved,
        p.src_ceded_premium AS ceded_premium,
        p.src_commission_paid AS commission_paid,
        p.src_ceded_commission AS ceded_commission,
        p.src_exposure_amount AS exposure_amount,
        p.src_investment_income AS investment_income,
        p.src_underwriting_cycle_time_in_days AS underwriting_cycle_time_in_days,
        p.src_underwriting_expenses AS underwriting_expenses,
        p.src_transaction_date AS transaction_date,
        p.src_record_type AS record_type,
        p.src_discount AS discount,
        p.src_override_commission AS override_commission,
        p.src_partial_recovery_percentage AS partial_recovery_percentage,
        s.is_auto_renew_enabled AS is_auto_renew_enabled,
        s.no_claims_discount_years AS no_claims_discount_years,
        s.payment_method AS payment_method,
        s.is_direct_debit_cancellation AS is_direct_debit_cancellation,
        s.missed_payment_count AS missed_payment_count,
        s.loyalty_discount_usage AS loyalty_discount_usage,
        s.is_installment_default AS is_installment_default,
        s.policy_renewal_satisfaction_score AS policy_renewal_satisfaction_score,
        s.policy_renewal_feedback AS policy_renewal_feedback,
        s.is_renewal_escalation AS is_renewal_escalation

    FROM crm_policy_register_csv s
    LEFT JOIN crm_enhanced_enrichments_csv p
        ON s.customer_ref = p.src_customer_ref
)
SELECT DISTINCT *
FROM combined;</code></pre></details></td><td>FROM differs. Python: crm_enhanced_enrichments_csv, crm_policy_register_csv. Notebook: combined, crm_policy_register_csv.<br>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `administrative_expenses`: Python `src_administrative_expenses`; Notebook `p.src_administrative_expenses`; `amount_approved`: Python `src_amount_approved`; Notebook `p.src_amount_approved`; `ceded_commission`: Python `src_ceded_commission`; Notebook `p.src_ceded_commission`; `ceded_premium`: Python `src_ceded_premium`; Notebook `p.src_ceded_premium`; `commission_paid`: Python `src_commission_paid`; Notebook `p.src_commission_paid`; `cover_option`: Python `null`; Notebook `s.cover_option_txt`; plus 53 more changed expressions.</td></tr>
<tr><td><code>sat_product_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(product_ref, origin_sys)) AS product_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  NULL AS type,
  NULL AS product_variant,
  product_cd AS product_name,
  product_launch_dt AS product_launch_date,
  product_status_txt AS product_status,
  product_lob_cd AS product_line_of_business_code,
  underwriting_group_txt AS underwriting_group,
  regulatory_approval_cd AS regulatory_approval_code,
  &#x27;sat_product_crm&#x27; AS source_tbl_name
FROM crm_product_catelog_csv
UNION ALL
SELECT DISTINCT
  md5(CONCAT(product_ref, origin_sys)) AS product_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  product_type_txt AS type,
  product_variant AS product_variant,
  product_cd AS product_name,
  product_launch_dt AS product_launch_date,
  product_status_txt AS product_status,
  product_lob_cd AS product_line_of_business_code,
  underwriting_group_txt AS underwriting_group,
  regulatory_approval_cd AS regulatory_approval_code,
  &#x27;sat_product_crm&#x27; AS source_tbl_name
FROM crm_product_catalog_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, product_ref)) AS product_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    product_type_txt AS type,
    product_variant AS product_variant,
    product_cd AS product_name,
    product_launch_dt AS product_launch_date,
    product_status_txt AS product_status,
    product_lob_cd as product_line_of_business_code,
    underwriting_group_txt as underwriting_group,
    regulatory_approval_cd as regulatory_approval_code
FROM crm_product_catalog_csv;</code></pre></details></td><td>FROM differs. Python: crm_product_catalog_csv, crm_product_catelog_csv. Notebook: crm_product_catalog_csv.<br>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `product_hash_key`: Python `md5(concat(product_ref, origin_sys))`; Notebook `md5(concat(origin_sys, product_ref))`.</td></tr>
<tr><td><code>sat_product_sap</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(CAST(product_id AS STRING), CAST(origin_sys AS STRING))) AS product_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  product_type,
  product_sub_type,
  product_name,
  product_start_date,
  line_of_business,
  origin_sys AS record_source,
  &#x27;sat_product_sap&#x27; AS source_tbl_name
FROM sap_product_db</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, product_id)) AS product_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    product_type AS product_type,
    product_sub_type AS product_sub_type,
    product_name AS product_name,
    product_start_date AS product_start_date,
    line_of_business AS line_of_business
FROM sap_product_db;</code></pre></details></td><td>Notebook adds columns: `line_of_business`, `product_name`, `product_start_date`, `product_sub_type`, `product_type`.<br>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `load_date`: Python `cast(load_ts as timestamp)`; Notebook `cast(pull_ts as timestamp)`; `product_hash_key`: Python `md5(concat(cast(product_id as string), cast(origin_sys as string)))`; Notebook `md5(concat(origin_sys, product_id))`.</td></tr>
<tr><td><code>sat_quote_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
  current_timestamp() AS load_date,
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
  rejection_reason AS rejection_reason,
  &#x27;sat_quote_crm&#x27; AS source_tbl_name
FROM crm_quote_register_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, quote_ref)) AS quote_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
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
FROM crm_quote_register_csv;</code></pre></details></td><td>Notebook removes columns: `source_tbl_name`.<br>Changed expressions: `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `quote_hash_key`: Python `md5(concat(quote_ref, origin_sys))`; Notebook `md5(concat(origin_sys, quote_ref))`.</td></tr>
<tr><td><code>sat_regulation_crm</code></td><td><details><summary>Python build_sql</summary><pre><code>SELECT DISTINCT
  md5(CONCAT(src_regulation_id, src_system)) AS regulation_hash_key,
  current_timestamp() AS load_date,
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
  src_is_regulation_on_time AS is_regulation_on_time,
  src_system AS record_source,
  &#x27;sat_regulation_crm&#x27; AS source_tbl_name
FROM crm_regulation_register_csv</code></pre></details></td><td><details><summary>Notebook build_sql</summary><pre><code>SELECT DISTINCT
    md5(concat(origin_sys, src_regulation_id)) AS regulation_hash_key,
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
FROM crm_regulation_register_csv;</code></pre></details></td><td>Notebook removes columns: `record_source`, `source_tbl_name`.<br>Changed expressions: `load_date`: Python `current_timestamp()`; Notebook `cast(pull_ts as timestamp)`; `regulation_hash_key`: Python `md5(concat(src_regulation_id, src_system))`; Notebook `md5(concat(origin_sys, src_regulation_id))`.</td></tr>
</tbody>
</table>
