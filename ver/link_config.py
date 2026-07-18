# Databricks notebook source
# DBTITLE 1,Link Configs - Product ID 3

link_customer_lead_3 =  {
  "product_id": "3",
  "vault_object_id": "link_customer_lead_3",
  "object_type": "link",
  "target_table": "link_customer_lead",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
    md5(CONCAT(lead_ref, origin_sys)) AS lead_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    'link_customer_lead' AS source_tbl_name
  FROM crm_customer_portfolio_csv
  UNION ALL
  SELECT
    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
    md5(CONCAT(lead_ref, origin_sys)) AS lead_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    'link_customer_lead' AS source_tbl_name
  FROM crm_lead_register_csv
)
SELECT
  md5(CONCAT(customer_ref, lead_ref)) AS customer_lead_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  customer_hash_key,
  lead_hash_key
FROM combined
""",
  "source_table": "crm_customer_portfolio_csv,crm_lead_register_csv"
}

link_customer_person_3 =  {
  "product_id": "3",
  "vault_object_id": "link_customer_person_3",
  "object_type": "link",
  "target_table": "link_customer_person",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    'crm_customer_portfolio_csv' AS source_tbl_name
  FROM crm_customer_portfolio_csv
  UNION ALL
  SELECT
    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    'crm_party_master_csv' AS source_tbl_name
  FROM crm_party_master_csv
)
SELECT
  md5(CONCAT(customer_hash_key, person_hash_key)) AS customer_person_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  customer_hash_key,
  person_hash_key
FROM combined
""",
  "source_table": "crm_customer_portfolio_csv,crm_party_master_csv"
}

link_person_account_3 =  {
  "product_id": "3",
  "vault_object_id": "link_person_account_3",
  "object_type": "link",
  "target_table": "link_person_account",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    SHA2(CONCAT_WS('||', CAST(party_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS person_hash_key,
    SHA2(CONCAT_WS('||', CAST(account_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS account_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    'link_person_account' AS source_tbl_name
  FROM crm_party_master_csv
  JOIN crm_account_book_csv
    ON crm_party_master_csv.party_ref = crm_account_book_csv.party_ref
)
SELECT
  SHA2(CONCAT_WS('||', CAST(person_hash_key AS STRING), CAST(account_hash_key AS STRING), CAST(origin_sys AS STRING)), 256) AS person_account_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  person_hash_key,
  account_hash_key
FROM combined
""",
  "source_table": "crm_party_master_csv,crm_account_book_csv"
}

link_person_consent_3 =  {
  "product_id": "3",
  "vault_object_id": "link_person_consent_3",
  "object_type": "link",
  "target_table": "link_person_consent",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(consent_ref, origin_sys)) AS consent_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    'link_person_consent' AS source_tbl_name
  FROM crm_party_master_csv
  CROSS JOIN crm_consent_snapshot_csv
)
SELECT
  md5(CONCAT(person_hash_key, consent_hash_key)) AS person_consent_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  person_hash_key,
  consent_hash_key
FROM combined
""",
  "source_table": "crm_party_master_csv,crm_consent_snapshot_csv"
}

link_person_contact_3 =  {
  "product_id": "3",
  "vault_object_id": "link_person_contact_3",
  "object_type": "link",
  "target_table": "link_person_contact",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(contact_ref, origin_sys)) AS contact_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    'link_person_contact' AS source_tbl_name
  FROM crm_party_master_csv
  UNION ALL
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(contact_ref, origin_sys)) AS contact_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    'link_person_contact' AS source_tbl_name
  FROM crm_contact_point_csv
)
SELECT
  md5(CONCAT(person_hash_key, contact_hash_key)) AS person_contact_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  person_hash_key,
  contact_hash_key
FROM combined
""",
  "source_table": "crm_party_master_csv,crm_contact_point_csv"
}

link_person_address_3 =  {
  "product_id": "3",
  "vault_object_id": "link_person_address_3",
  "object_type": "link",
  "target_table": "link_person_address",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(address_ref, origin_sys)) AS address_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    'link_person_address' AS source_tbl_name
  FROM crm_party_master_csv
  CROSS JOIN crm_address_book_csv
)
SELECT
  md5(CONCAT(person_hash_key, address_hash_key)) AS person_address_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  person_hash_key,
  address_hash_key
FROM combined
""",
  "source_table": "crm_party_master_csv,crm_address_book_csv"
}

link_person_identities_3 =  {
  "product_id": "3",
  "vault_object_id": "link_person_identities_3",
  "object_type": "link",
  "target_table": "link_person_identities",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(identity_ref, origin_sys)) AS identities_hash_key,
    load_timestamp AS source_load_timestamp_col,
    origin_sys,
    'crm_party_master_csv' AS source_tbl_name
  FROM crm_party_master_csv
  UNION ALL
  SELECT
    md5(CONCAT(person_id, origin_sys)) AS person_hash_key,
    md5(CONCAT(identity_ref, origin_sys)) AS identities_hash_key,
    load_timestamp AS source_load_timestamp_col,
    origin_sys,
    'crm_identity_registry_csv' AS source_tbl_name
  FROM crm_identity_registry_csv
)
SELECT
  md5(CONCAT(person_hash_key, identities_hash_key)) AS person_identities_hash_key,
  CAST(source_load_timestamp_col AS timestamp) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  person_hash_key,
  identities_hash_key
FROM combined
""",
  "source_table": "crm_party_master_csv,crm_identity_registry_csv"
}

link_person_lead_3 =  {
  "product_id": "3",
  "vault_object_id": "link_person_lead_3",
  "object_type": "link",
  "target_table": "link_person_lead",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(lead_ref, origin_sys)) AS lead_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    'link_person_lead' AS source_tbl_name
  FROM crm_party_master_csv
  CROSS JOIN crm_lead_register_csv
)
SELECT
  md5(CONCAT(person_hash_key, lead_hash_key)) AS person_lead_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  origin_sys AS record_source,
  person_hash_key,
  lead_hash_key
FROM combined
""",
  "source_table": "crm_party_master_csv,crm_lead_register_csv"
}

link_person_legal_person_3 =  {
  "product_id": "3",
  "vault_object_id": "link_person_legal_person_3",
  "object_type": "link",
  "target_table": "link_person_legal_person",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(legal_ref, origin_sys)) AS legal_person_hash_key,
    origin_sys,
    current_timestamp() AS etl_load_timestamp,
    'crm_party_master_csv' AS source_tbl_name
  FROM crm_party_master_csv

  UNION ALL

  SELECT
    md5(CONCAT(person_id, origin_sys)) AS person_hash_key,
    md5(CONCAT(person_id, origin_sys)) AS legal_person_hash_key,
    origin_sys,
    current_timestamp() AS etl_load_timestamp,
    'sap_person_db' AS source_tbl_name
  FROM sap_person_db
)
SELECT
  md5(CONCAT(person_hash_key, legal_person_hash_key)) AS person_legal_person_hash_key,
  CAST(etl_load_timestamp AS timestamp) AS load_date,
  origin_sys AS record_source,
  person_hash_key,
  legal_person_hash_key
FROM combined
""",
  "source_table": "crm_party_master_csv,sap_person_db"
}

link_person_marketing_engagement_3 =  {
  "product_id": "3",
  "vault_object_id": "link_person_marketing_engagement_3",
  "object_type": "link",
  "target_table": "link_person_marketing_engagement",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(src_marketing_engagement_ref, origin_sys)) AS marketing_engagement_hash_key,
    current_timestamp() AS etl_load_timestamp,
    origin_sys,
    'crm_party_master_csv' AS source_tbl_name
  FROM crm_party_master_csv
  UNION ALL
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(src_marketing_engagement_ref, origin_sys)) AS marketing_engagement_hash_key,
    current_timestamp() AS etl_load_timestamp,
    origin_sys,
    'crm_enhanced_enrichments_csv' AS source_tbl_name
  FROM crm_enhanced_enrichments_csv
)
SELECT
  md5(CONCAT(person_hash_key, marketing_engagement_hash_key)) AS person_marketing_engagement_hash_key,
  CAST(etl_load_timestamp AS timestamp) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  person_hash_key,
  marketing_engagement_hash_key
FROM combined
""",
  "source_table": "crm_party_master_csv,crm_enhanced_enrichments_csv"
}

link_person_marketing_preference_3 =  {
  "product_id": "3",
  "vault_object_id": "link_person_marketing_preference_3",
  "object_type": "link",
  "target_table": "link_person_marketing_preference",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(preference_ref, origin_sys)) AS marketing_preference_hash_key,
    current_timestamp() AS etl_load_timestamp,
    origin_sys,
    'link_person_marketing_preference' AS source_tbl_name
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
FROM combined
""",
  "source_table": "crm_party_master_csv,crm_comm_preference_csv"
}

link_person_natural_person_3 =  {
  "product_id": "3",
  "vault_object_id": "link_person_natural_person_3",
  "object_type": "link",
  "target_table": "link_person_natural_person",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    SHA2(CONCAT_WS('||', CAST(party_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS person_hash_key,
    SHA2(CONCAT_WS('||', CAST(natural_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS natural_person_hash_key,
    source_load_timestamp AS source_load_timestamp_col,
    origin_sys,
    'crm_party_master_csv' AS source_tbl_name
  FROM crm_party_master_csv

  UNION ALL

  SELECT
    SHA2(CONCAT_WS('||', CAST(person_id AS STRING), CAST(origin_sys AS STRING)), 256) AS person_hash_key,
    SHA2(CONCAT_WS('||', CAST(person_id AS STRING), CAST(origin_sys AS STRING)), 256) AS natural_person_hash_key,
    source_load_timestamp AS source_load_timestamp_col,
    origin_sys,
    'sap_person_db' AS source_tbl_name
  FROM sap_person_db
)
SELECT
  SHA2(CONCAT_WS('||', CAST(person_hash_key AS STRING), CAST(natural_person_hash_key AS STRING), CAST(origin_sys AS STRING)), 256) AS person_natural_person_hash_key,
  CAST(source_load_timestamp_col AS TIMESTAMP) AS load_date,
  origin_sys AS record_source,
  person_hash_key,
  natural_person_hash_key
FROM combined
""",
  "source_table": "crm_party_master_csv,sap_person_db"
}

link_policy_customer_3 =  {
  "product_id": "3",
  "vault_object_id": "link_policy_customer_3",
  "object_type": "link",
  "target_table": "link_policy_customer",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    'crm_policy_register_csv' AS source_tbl_name
  FROM crm_policy_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    'crm_customer_portfolio_csv' AS source_tbl_name
  FROM crm_customer_portfolio_csv
)
SELECT
  md5(CONCAT(policy_hash_key, customer_hash_key)) AS policy_customer_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  customer_hash_key,
  policy_hash_key
FROM combined
""",
  "source_table": "crm_policy_register_csv,crm_customer_portfolio_csv"
}

link_policy_product_3 =  {
  "product_id": "3",
  "vault_object_id": "link_policy_product_3",
  "object_type": "link",
  "target_table": "link_policy_product",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(product_ref, origin_sys)) AS product_hash_key,
    current_timestamp() AS etl_load_timestamp,
    origin_sys,
    'link_policy_product' AS source_tbl_name
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
FROM combined
""",
  "source_table": "crm_policy_register_csv,crm_product_catelog_csv"
}

link_quote_person_3 =  {
  "product_id": "3",
  "vault_object_id": "link_quote_person_3",
  "object_type": "link",
  "target_table": "link_quote_person",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    'crm_quote_register_csv' AS source_tbl_name
  FROM crm_quote_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    current_timestamp() AS load_ts,
    origin_sys,
    'crm_party_master_csv' AS source_tbl_name
  FROM crm_party_master_csv
)
SELECT
  md5(CONCAT(quote_hash_key, person_hash_key)) AS quote_person_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  quote_hash_key,
  person_hash_key
FROM combined
""",
  "source_table": "crm_quote_register_csv,crm_party_master_csv"
}

link_quote_product_3 =  {
  "product_id": "3",
  "vault_object_id": "link_quote_product_3",
  "object_type": "link",
  "target_table": "link_quote_product",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
    md5(CONCAT(product_ref, origin_sys)) AS product_hash_key,
    current_timestamp() AS etl_load_timestamp,
    origin_sys,
    'crm_quote_register_csv' AS source_tbl_name
  FROM crm_quote_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
    md5(CONCAT(product_ref, origin_sys)) AS product_hash_key,
    current_timestamp() AS etl_load_timestamp,
    origin_sys,
    'crm_product_catelog_csv' AS source_tbl_name
  FROM crm_product_catelog_csv
)
SELECT
  md5(CONCAT(quote_hash_key, product_hash_key)) AS quote_product_hash_key,
  CAST(etl_load_timestamp AS timestamp) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  quote_hash_key,
  product_hash_key
FROM combined
""",
  "source_table": "crm_quote_register_csv,crm_product_catelog_csv"
}

link_broker_person_3 =  {
  "product_id": "3",
  "vault_object_id": "link_broker_person_3",
  "object_type": "link",
  "target_table": "link_broker_person",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
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
FROM combined
""",
  "source_table": "crm_broker_book_csv,crm_party_master_csv"
}

link_policy_override_3 =  {
  "product_id": "3",
  "vault_object_id": "link_policy_override_3",
  "object_type": "link",
  "target_table": "link_policy_override",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
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
FROM combined
""",
  "source_table": "crm_policy_register_csv,crm_override_register_csv"
}

link_policy_channel_3 =  {
  "product_id": "3",
  "vault_object_id": "link_policy_channel_3",
  "object_type": "link",
  "target_table": "link_policy_channel",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
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
FROM combined
""",
  "source_table": "crm_policy_register_csv,crm_channel_catalog_csv"
}

link_person_campaign_3 =  {
  "product_id": "3",
  "vault_object_id": "link_person_campaign_3",
  "object_type": "link",
  "target_table": "link_person_campaign",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
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
FROM combined
""",
  "source_table": "crm_party_master_csv,crm_campaign_register_csv"
}

link_claim_policy_3 =  {
  "product_id": "3",
  "vault_object_id": "link_claim_policy_3",
  "object_type": "link",
  "target_table": "link_claim_policy",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
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
FROM combined
""",
  "source_table": "crm_claim_register_csv,crm_policy_register_csv"
}

link_complaint_regulation_3 =  {
  "product_id": "3",
  "vault_object_id": "link_complaint_regulation_3",
  "object_type": "link",
  "target_table": "link_complaint_regulation",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(src_complaint_id, src_system)) AS complaint_hash_key,
    md5(CONCAT(src_regulation_id, src_system)) AS regulation_hash_key,
    current_timestamp() AS load_ts,
    src_system AS origin_sys,
    'crm_complaint_register_csv' AS source_tbl_name
  FROM crm_complaint_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(src_complaint_id, src_system)) AS complaint_hash_key,
    md5(CONCAT(src_regulation_id, src_system)) AS regulation_hash_key,
    current_timestamp() AS load_ts,
    src_system AS origin_sys,
    'crm_regulation_register_csv' AS source_tbl_name
  FROM crm_regulation_register_csv
)
SELECT
  md5(CONCAT(src_complaint_id, src_regulation_id)) AS complaint_regulation_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  complaint_hash_key,
  regulation_hash_key
FROM combined
""",
  "source_table": "crm_complaint_register_csv,crm_regulation_register_csv"
}

link_complaint_policy_3 =  {
  "product_id": "3",
  "vault_object_id": "link_complaint_policy_3",
  "object_type": "link",
  "target_table": "link_complaint_policy",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
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
FROM combined
""",
  "source_table": "crm_complaint_register_csv,crm_policy_register_csv"
}

link_insured_object_motor_3 =  {
  "product_id": "3",
  "vault_object_id": "link_insured_object_motor_3",
  "object_type": "link",
  "target_table": "link_insured_object_motor",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
    md5(CONCAT(vehicle_ref, origin_sys)) AS motor_hash_key,
    current_timestamp() AS load_timestamp,
    origin_sys,
    'crm_vehicle_asset_csv' AS source_tbl_name
  FROM crm_vehicle_asset_csv
)
SELECT
  md5(CONCAT(insured_object_hash_key, motor_hash_key, origin_sys)) AS insured_object_motor_hash_key,
  CAST(load_timestamp AS TIMESTAMP) AS load_date,
  origin_sys AS record_source,
  insured_object_hash_key,
  motor_hash_key
FROM combined
""",
  "source_table": "crm_vehicle_asset_csv"
}

link_insured_object_home_3 =  {
  "product_id": "3",
  "vault_object_id": "link_insured_object_home_3",
  "object_type": "link",
  "target_table": "link_insured_object_home",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(src_insured_object_id, property_ref)) AS insured_object_home_hash_key,
    md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
    md5(CONCAT(property_ref, origin_sys)) AS home_hash_key,
    current_timestamp() AS load_timestamp,
    origin_sys,
    'crm_property_asset_csv' AS source_tbl_name
  FROM crm_property_asset_csv
)
SELECT
  md5(CONCAT(insured_object_hash_key, home_hash_key, origin_sys)) AS insured_object_home_hash_key,
  CAST(load_timestamp AS timestamp) AS load_date,
  origin_sys AS record_source,
  insured_object_hash_key,
  home_hash_key
FROM combined
""",
  "source_table": "crm_property_asset_csv"
}

link_policy_insured_object_3 =  {
  "product_id": "3",
  "vault_object_id": "link_policy_insured_object_3",
  "object_type": "link",
  "target_table": "link_policy_insured_object",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
    source_load_timestamp AS source_load_timestamp,
    origin_sys,
    'crm_policy_register_csv' AS source_tbl_name
  FROM crm_policy_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
    source_load_timestamp AS source_load_timestamp,
    origin_sys,
    'crm_vehicle_asset_csv' AS source_tbl_name
  FROM crm_vehicle_asset_csv
  UNION ALL
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
    source_load_timestamp AS source_load_timestamp,
    origin_sys,
    'crm_property_asset_csv' AS source_tbl_name
  FROM crm_property_asset_csv
)
SELECT
  md5(CONCAT(policy_hash_key, insured_object_hash_key)) AS policy_insured_object_hash_key,
  CAST(source_load_timestamp AS TIMESTAMP) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  policy_hash_key,
  insured_object_hash_key
FROM combined
""",
  "source_table": "crm_policy_register_csv,crm_vehicle_asset_csv,crm_property_asset_csv"
}

link_quote_channel_3 =  {
  "product_id": "3",
  "vault_object_id": "link_quote_channel_3",
  "object_type": "link",
  "target_table": "link_quote_channel",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    SHA2(CONCAT_WS('||', CAST(quote_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS quote_hash_key,
    SHA2(CONCAT_WS('||', CAST(src_channel_id AS STRING), CAST(src_system AS STRING)), 256) AS channel_hash_key,
    load_timestamp AS source_load_timestamp_col,
    COALESCE(origin_sys, src_system) AS record_source
  FROM crm_quote_register_csv
  JOIN crm_channel_catalog_csv
    ON crm_quote_register_csv.src_channel_id = crm_channel_catalog_csv.src_channel_id
)
SELECT
  SHA2(CONCAT_WS('||', CAST(quote_hash_key AS STRING), CAST(channel_hash_key AS STRING)), 256) AS quote_channel_hash_key,
  CAST(source_load_timestamp_col AS TIMESTAMP) AS load_date,
  record_source,
  quote_hash_key,
  channel_hash_key
FROM combined
""",
  "source_table": "crm_quote_register_csv,crm_channel_catalog_csv"
}

link_policy_quote_3 =  {
  "product_id": "3",
  "vault_object_id": "link_policy_quote_3",
  "object_type": "link",
  "target_table": "link_policy_quote",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
    load_timestamp AS source_load_timestamp_col,
    origin_sys,
    'crm_policy_register_csv' AS source_tbl_name
  FROM crm_policy_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
    load_timestamp AS source_load_timestamp_col,
    origin_sys,
    'crm_quote_register_csv' AS source_tbl_name
  FROM crm_quote_register_csv
)
SELECT
  md5(CONCAT(policy_ref, quote_ref)) AS policy_quote_hash_key,
  CAST(source_load_timestamp_col AS TIMESTAMP) AS load_date,
  COALESCE(origin_sys, origin_sys) AS record_source,
  policy_hash_key,
  quote_hash_key
FROM combined
""",
  "source_table": "crm_policy_register_csv,crm_quote_register_csv"
}

link_policy_broker_3 =  {
  "product_id": "3",
  "vault_object_id": "link_policy_broker_3",
  "object_type": "link",
  "target_table": "link_policy_broker",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
  SELECT
    SHA2(CONCAT_WS('||', CAST(policy_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS policy_hash_key,
    SHA2(CONCAT_WS('||', CAST(src_agent_id AS STRING), CAST(src_system AS STRING)), 256) AS broker_hash_key,
    current_timestamp() AS load_ts,
    COALESCE(origin_sys, src_system) AS record_source
  FROM crm_policy_register_csv
  UNION ALL
  SELECT
    SHA2(CONCAT_WS('||', CAST(policy_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS policy_hash_key,
    SHA2(CONCAT_WS('||', CAST(src_agent_id AS STRING), CAST(src_system AS STRING)), 256) AS broker_hash_key,
    current_timestamp() AS load_ts,
    COALESCE(origin_sys, src_system) AS record_source
  FROM crm_broker_book_csv
)
SELECT
  SHA2(CONCAT_WS('||', CAST(policy_hash_key AS STRING), CAST(broker_hash_key AS STRING)), 256) AS policy_broker_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  record_source,
  policy_hash_key,
  broker_hash_key
FROM combined
""",
  "source_table": "crm_policy_register_csv,crm_broker_book_csv"
}

link_quote_broker_3 =  {
  "product_id": "3",
  "vault_object_id": "link_quote_broker_3",
  "object_type": "link",
  "target_table": "link_quote_broker",
  "process_order": "2",
  "build_sql": f"""
WITH combined AS (
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
FROM combined
""",
  "source_table": "crm_quote_register_csv,crm_broker_book_csv"
}
