# Databricks notebook source
# DBTITLE 1,Hub Configs - Product ID 3

hub_account_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_account_3",
  "object_type": "hub",
  "target_table": "hub_account",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(account_ref, origin_sys)) AS account_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  account_ref AS account_id,
  'hub_account' AS source_tbl_name
FROM crm_account_book_csv
""",
  "source_table": "crm_account_book_csv"
}

hub_consent_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_consent_3",
  "object_type": "hub",
  "target_table": "hub_consent",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(consent_ref, origin_sys)) AS consent_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  consent_ref AS consent_id,
  'hub_consent' AS source_tbl_name
FROM crm_consent_snapshot_csv
""",
  "source_table": "crm_consent_snapshot_csv"
}

hub_contact_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_contact_3",
  "object_type": "hub",
  "target_table": "hub_contact",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(contact_ref, origin_sys)) AS contact_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  contact_ref AS contact_id,
  'hub_contact' AS source_tbl_name
FROM crm_contact_point_csv
""",
  "source_table": "crm_contact_point_csv"
}

hub_customer_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_customer_3",
  "object_type": "hub",
  "target_table": "hub_customer",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  customer_ref AS customer_id,
  'hub_customer' AS source_tbl_name
FROM crm_customer_portfolio_csv
""",
  "source_table": "crm_customer_portfolio_csv"
}

hub_home_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_home_3",
  "object_type": "hub",
  "target_table": "hub_home",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(property_ref, origin_sys)) AS home_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  property_ref AS insured_object_home_id,
  'hub_home' AS source_tbl_name
FROM crm_property_asset_csv
UNION ALL
SELECT
  md5(CONCAT(home_id, origin_sys)) AS home_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  home_id AS insured_object_home_id,
  'hub_home' AS source_tbl_name
FROM sap_home_db
""",
  "source_table": "crm_property_asset_csv,sap_home_db"
}

hub_address_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_address_3",
  "object_type": "hub",
  "target_table": "hub_address",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(address_ref, origin_sys)) AS address_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  address_ref AS address_id,
  'hub_address' AS source_tbl_name
FROM crm_address_book_csv

UNION ALL

SELECT
  md5(CONCAT(address_id, origin_sys)) AS address_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  address_id AS address_id,
  'hub_address' AS source_tbl_name
FROM sap_address_db
""",
  "source_table": "crm_address_book_csv,sap_address_db"
}

hub_identities_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_identities_3",
  "object_type": "hub",
  "target_table": "hub_identities",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(identity_ref, origin_sys)) AS identities_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  identity_ref AS identities_id,
  'hub_identities' AS source_tbl_name
FROM crm_identity_registry_csv
""",
  "source_table": "crm_identity_registry_csv"
}

hub_lead_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_lead_3",
  "object_type": "hub",
  "target_table": "hub_lead",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(lead_ref, origin_sys)) AS lead_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  lead_ref AS lead_id,
  'hub_lead' AS source_tbl_name
FROM crm_lead_register_csv
""",
  "source_table": "crm_lead_register_csv"
}

hub_legal_person_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_legal_person_3",
  "object_type": "hub",
  "target_table": "hub_legal_person",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(legal_ref, origin_sys)) AS legal_person_hash_key,
  CAST(current_timestamp() AS timestamp) AS load_date,
  origin_sys AS record_source,
  legal_ref AS legal_person_id,
  'hub_legal_person' AS source_tbl_name
FROM crm_party_master_csv
UNION ALL
SELECT
  md5(CONCAT(person_id, origin_sys)) AS legal_person_hash_key,
  CAST(current_timestamp() AS timestamp) AS load_date,
  origin_sys AS record_source,
  person_id AS legal_person_id,
  'hub_legal_person' AS source_tbl_name
FROM sap_person_db
""",
  "source_table": "crm_party_master_csv,sap_person_db"
}

hub_marketing_engagement_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_marketing_engagement_3",
  "object_type": "hub",
  "target_table": "hub_marketing_engagement",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(src_marketing_engagement_ref, origin_sys)) AS marketing_engagement_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  src_marketing_engagement_ref AS marketing_engagement_id,
  'hub_marketing_engagement' AS source_tbl_name
FROM crm_enhanced_enrichments_csv
""",
  "source_table": "crm_enhanced_enrichments_csv"
}

hub_marketing_preference_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_marketing_preference_3",
  "object_type": "hub",
  "target_table": "hub_marketing_preference",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(preference_ref, origin_sys)) AS marketing_preference_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  preference_ref AS marketing_preference_id,
  'hub_marketing_preference' AS source_tbl_name
FROM crm_comm_preference_csv
""",
  "source_table": "crm_comm_preference_csv"
}

hub_motor_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_motor_3",
  "object_type": "hub",
  "target_table": "hub_motor",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(vehicle_ref, origin_sys)) AS motor_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  vehicle_ref AS insured_object_motor_id,
  'hub_motor' AS source_tbl_name
FROM crm_vehicle_asset_csv
UNION ALL
SELECT
  md5(CONCAT(motor_id, origin_sys)) AS motor_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  motor_id AS insured_object_motor_id,
  'hub_motor' AS source_tbl_name
FROM sap_motor_db
""",
  "source_table": "crm_vehicle_asset_csv,sap_motor_db"
}

hub_natural_person_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_natural_person_3",
  "object_type": "hub",
  "target_table": "hub_natural_person",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(natural_ref, origin_sys)) AS natural_person_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  natural_ref AS natural_person_id,
  'hub_natural_person' AS source_tbl_name
FROM crm_party_master_csv
UNION ALL
SELECT
  md5(CONCAT(person_id, origin_sys)) AS natural_person_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  person_id AS natural_person_id,
  'hub_natural_person' AS source_tbl_name
FROM sap_person_db
""",
  "source_table": "crm_party_master_csv,sap_person_db"
}

hub_person_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_person_3",
  "object_type": "hub",
  "target_table": "hub_person",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  party_ref AS person_id,
  'hub_person' AS source_tbl_name
FROM crm_party_master_csv
UNION ALL
SELECT
  md5(CONCAT(person_id, origin_sys)) AS person_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  person_id AS person_id,
  'hub_person' AS source_tbl_name
FROM sap_person_db
""",
  "source_table": "crm_party_master_csv,sap_person_db"
}

hub_policy_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_policy_3",
  "object_type": "hub",
  "target_table": "hub_policy",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  policy_ref AS policy_id,
  'hub_policy' AS source_tbl_name
FROM crm_policy_register_csv
""",
  "source_table": "crm_policy_register_csv"
}

hub_product_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_product_3",
  "object_type": "hub",
  "target_table": "hub_product",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(product_ref, origin_sys)) AS product_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  product_ref AS product_id,
  'hub_product' AS source_tbl_name
FROM crm_product_catelog_csv

UNION ALL

SELECT
  md5(CONCAT(product_id, origin_sys)) AS product_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  product_id AS product_id,
  'hub_product' AS source_tbl_name
FROM sap_product_db
""",
  "source_table": "crm_product_catelog_csv,sap_product_db"
}

hub_quote_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_quote_3",
  "object_type": "hub",
  "target_table": "hub_quote",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  quote_ref AS quote_id,
  'hub_quote' AS source_tbl_name
FROM crm_quote_register_csv
""",
  "source_table": "crm_quote_register_csv"
}

hub_broker_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_broker_3",
  "object_type": "hub",
  "target_table": "hub_broker",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(src_agent_id, src_system)) AS broker_hash_key,
  current_timestamp() AS load_date,
  src_system AS record_source,
  src_agent_id AS agent_id,
  'hub_broker' AS source_tbl_name
FROM crm_broker_book_csv
""",
  "source_table": "crm_broker_book_csv"
}

hub_channel_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_channel_3",
  "object_type": "hub",
  "target_table": "hub_channel",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(src_channel_id, src_system)) AS channel_hash_key,
  CAST(load_ts AS timestamp) AS load_date,
  src_system AS record_source,
  src_channel_id AS channel_id,
  'hub_channel' AS source_tbl_name
FROM crm_channel_catalog_csv
""",
  "source_table": "crm_channel_catalog_csv"
}

hub_campaign_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_campaign_3",
  "object_type": "hub",
  "target_table": "hub_campaign",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(src_campaign_id, src_system)) AS campaign_hash_key,
  current_timestamp() AS load_date,
  src_system AS record_source,
  src_campaign_id AS campaign_id,
  'hub_campaign' AS source_tbl_name
FROM crm_campaign_register_csv
""",
  "source_table": "crm_campaign_register_csv"
}

hub_regulation_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_regulation_3",
  "object_type": "hub",
  "target_table": "hub_regulation",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(src_regulation_id, src_system)) AS regulation_hash_key,
  current_timestamp() AS load_date,
  src_system AS record_source,
  src_regulation_id AS regulation_id,
  'hub_regulation' AS source_tbl_name
FROM crm_regulation_register_csv
""",
  "source_table": "crm_regulation_register_csv"
}

hub_override_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_override_3",
  "object_type": "hub",
  "target_table": "hub_override",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(src_override_id, src_system)) AS override_hash_key,
  current_timestamp() AS load_date,
  src_system AS record_source,
  src_override_id AS override_id,
  'hub_override' AS source_tbl_name
FROM crm_override_register_csv
""",
  "source_table": "crm_override_register_csv"
}

hub_claim_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_claim_3",
  "object_type": "hub",
  "target_table": "hub_claim",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(src_claim_id, src_system)) AS claim_hash_key,
  current_timestamp() AS load_date,
  src_system AS record_source,
  src_claim_id AS claim_id,
  'hub_claim' AS source_tbl_name
FROM crm_claim_register_csv
""",
  "source_table": "crm_claim_register_csv"
}

hub_complaint_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_complaint_3",
  "object_type": "hub",
  "target_table": "hub_complaint",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(src_complaint_id, src_system)) AS complaint_hash_key,
  current_timestamp() AS load_date,
  src_system AS record_source,
  src_complaint_id AS complaint_id,
  'hub_complaint' AS source_tbl_name
FROM crm_complaint_register_csv
""",
  "source_table": "crm_complaint_register_csv"
}

hub_insured_object_3 =  {
  "product_id": "3",
  "vault_object_id": "hub_insured_object_3",
  "object_type": "hub",
  "target_table": "hub_insured_object",
  "process_order": "1",
  "build_sql": f"""
SELECT
  md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  insured_object_id AS insured_object_id,
  'hub_insured_object' AS source_tbl_name
FROM crm_property_asset_csv

UNION ALL

SELECT
  md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
  current_timestamp() AS load_date,
  origin_sys AS record_source,
  insured_object_id AS insured_object_id,
  'hub_insured_object' AS source_tbl_name
FROM crm_vehicle_asset_csv
""",
  "source_table": "crm_property_asset_csv,crm_vehicle_asset_csv"
}
