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
    pull_ts,
    origin_sys
  FROM crm_customer_portfolio_csv
  UNION ALL
  SELECT
    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
    md5(CONCAT(lead_ref, origin_sys)) AS lead_hash_key,
    pull_ts,
    origin_sys
  FROM crm_lead_register_csv
)
SELECT
  md5(CONCAT(customer_hash_key, lead_hash_key, origin_sys)) AS customer_lead_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
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
    pull_ts,
    origin_sys
  FROM crm_customer_portfolio_csv
  UNION ALL
  SELECT
    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    pull_ts,
    origin_sys
  FROM crm_party_master_csv
)
SELECT
  md5(CONCAT(customer_hash_key, person_hash_key)) AS customer_person_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(account_ref, origin_sys)) AS account_hash_key,
    pull_ts,
    origin_sys
  FROM crm_party_master_csv
  UNION ALL
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(account_ref, origin_sys)) AS account_hash_key,
    pull_ts,
    origin_sys
  FROM crm_account_book_csv
)
SELECT
  md5(CONCAT(person_hash_key, account_hash_key, origin_sys)) AS person_account_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    pull_ts,
    origin_sys
  FROM crm_party_master_csv
  UNION ALL
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(consent_ref, origin_sys)) AS consent_hash_key,
    pull_ts,
    origin_sys
  FROM crm_consent_snapshot_csv
)
SELECT
  md5(CONCAT(person_hash_key, consent_hash_key, origin_sys)) AS person_consent_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    pull_ts,
    origin_sys
  FROM crm_party_master_csv
  UNION ALL
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(contact_ref, origin_sys)) AS contact_hash_key,
    pull_ts,
    origin_sys
  FROM crm_contact_point_csv
)
SELECT
  md5(CONCAT(person_hash_key, contact_hash_key, origin_sys)) AS person_contact_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    pull_ts,
    origin_sys
  FROM crm_party_master_csv
  UNION ALL
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(address_ref, origin_sys)) AS address_hash_key,
    pull_ts,
    origin_sys
  FROM crm_address_book_csv
)
SELECT
  md5(CONCAT(person_hash_key, address_hash_key, origin_sys)) AS person_address_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    pull_ts,
    origin_sys
  FROM crm_party_master_csv
  UNION ALL
  SELECT
    md5(CONCAT(person_id, origin_sys)) AS person_hash_key,
    md5(CONCAT(identity_ref, origin_sys)) AS identities_hash_key,
    pull_ts,
    origin_sys
  FROM crm_identity_registry_csv
)
SELECT
  md5(CONCAT(person_hash_key, identities_hash_key, origin_sys)) AS person_identities_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    pull_ts,
    origin_sys
  FROM crm_party_master_csv
  UNION ALL
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(lead_ref, origin_sys)) AS lead_hash_key,
    pull_ts,
    origin_sys
  FROM crm_lead_register_csv
)
SELECT
  md5(CONCAT(person_hash_key, lead_hash_key, origin_sys)) AS person_lead_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
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
    pull_ts,
    origin_sys
  FROM crm_party_master_csv
  WHERE party_kind = 'Legal'
  UNION ALL
  SELECT
    md5(CONCAT(person_id, origin_sys)) AS person_hash_key,
    md5(CONCAT(person_id, origin_sys)) AS legal_person_hash_key,
    pull_ts,
    origin_sys
  FROM sap_person_db
  WHERE person_type = 'Legal'
)
SELECT
  md5(CONCAT(person_hash_key, legal_person_hash_key, origin_sys)) AS person_legal_person_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
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
    pull_ts,
    origin_sys
  FROM crm_party_master_csv
  UNION ALL
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(src_marketing_engagement_ref, origin_sys)) AS marketing_engagement_hash_key,
    pull_ts,
    origin_sys
  FROM crm_enhanced_enrichments_csv
)
SELECT
  md5(CONCAT(person_hash_key, marketing_engagement_hash_key)) AS person_marketing_engagement_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
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
    pull_ts,
    origin_sys
  FROM crm_party_master_csv
  UNION ALL
  SELECT
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(preference_ref, origin_sys)) AS marketing_preference_hash_key,
    pull_ts,
    origin_sys
  FROM crm_comm_preference_csv
)
SELECT
  md5(CONCAT(person_hash_key, marketing_preference_hash_key)) AS person_marketing_preference_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
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
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(natural_ref, origin_sys)) AS natural_person_hash_key,
    pull_ts,
    origin_sys
  FROM crm_party_master_csv
  WHERE party_kind = 'Natural'
  UNION ALL
  SELECT
    md5(CONCAT(person_id, origin_sys)) AS person_hash_key,
    md5(CONCAT(person_id, origin_sys)) AS natural_person_hash_key,
    pull_ts,
    origin_sys
  FROM sap_person_db
  WHERE person_type = 'Natural'
)
SELECT
  md5(CONCAT(person_hash_key, natural_person_hash_key, origin_sys)) AS person_natural_person_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
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
    pull_ts,
    origin_sys
  FROM crm_policy_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
    pull_ts,
    origin_sys
  FROM crm_customer_portfolio_csv
)
SELECT
  md5(CONCAT(policy_hash_key, customer_hash_key, origin_sys)) AS policy_customer_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
  policy_hash_key,
  customer_hash_key
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
    pull_ts,
    origin_sys
  FROM crm_policy_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(product_ref, origin_sys)) AS product_hash_key,
    pull_ts,
    origin_sys
  FROM crm_product_catelog_csv
)
SELECT
  md5(CONCAT(policy_hash_key, product_hash_key, origin_sys)) AS policy_product_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    pull_ts,
    origin_sys
  FROM crm_quote_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    pull_ts,
    origin_sys
  FROM crm_party_master_csv
)
SELECT
  md5(CONCAT(quote_hash_key, person_hash_key, origin_sys)) AS quote_person_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    pull_ts,
    origin_sys
  FROM crm_quote_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
    md5(CONCAT(product_ref, origin_sys)) AS product_hash_key,
    pull_ts,
    origin_sys
  FROM crm_product_catelog_csv
)
SELECT
  md5(CONCAT(quote_hash_key, product_hash_key)) AS quote_product_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    pull_ts,
    COALESCE(src_system, origin_sys) AS origin_sys
  FROM crm_broker_book_csv
  CROSS JOIN crm_party_master_csv
)
SELECT
  md5(CONCAT(broker_hash_key, person_hash_key)) AS broker_person_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(src_override_id, src_system)) AS override_hash_key,
    pull_ts,
    COALESCE(origin_sys, src_system) AS origin_sys
  FROM crm_policy_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(src_override_id, src_system)) AS override_hash_key,
    pull_ts,
    COALESCE(origin_sys, src_system) AS origin_sys
  FROM crm_override_register_csv
)
SELECT
  md5(CONCAT(policy_hash_key, override_hash_key)) AS policy_override_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(src_channel_id, src_system)) AS channel_hash_key,
    pull_ts,
    COALESCE(origin_sys, src_system) AS origin_sys
  FROM crm_policy_register_csv
  JOIN crm_channel_catalog_csv
    ON crm_policy_register_csv.origin_sys = crm_channel_catalog_csv.src_system
)
SELECT
  md5(CONCAT(policy_hash_key, channel_hash_key, origin_sys)) AS policy_channel_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
    md5(CONCAT(src_campaign_id, src_system)) AS campaign_hash_key,
    pull_ts,
    COALESCE(origin_sys, src_system) AS origin_sys
  FROM crm_party_master_csv
  CROSS JOIN crm_campaign_register_csv
)
SELECT
  md5(CONCAT(person_hash_key, campaign_hash_key, origin_sys)) AS person_campaign_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    pull_ts,
    COALESCE(src_system, origin_sys) AS origin_sys
  FROM crm_claim_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(src_claim_id, src_system)) AS claim_hash_key,
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    pull_ts,
    COALESCE(src_system, origin_sys) AS origin_sys
  FROM crm_policy_register_csv
)
SELECT
  md5(CONCAT(claim_hash_key, policy_hash_key)) AS claim_policy_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    pull_ts,
    src_system AS origin_sys
  FROM crm_complaint_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(src_complaint_id, src_system)) AS complaint_hash_key,
    md5(CONCAT(src_regulation_id, src_system)) AS regulation_hash_key,
    pull_ts,
    src_system AS origin_sys
  FROM crm_regulation_register_csv
)
SELECT
  md5(CONCAT(complaint_hash_key, regulation_hash_key)) AS complaint_regulation_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    pull_ts,
    COALESCE(src_system, origin_sys) AS origin_sys
  FROM crm_complaint_register_csv
  CROSS JOIN crm_policy_register_csv
)
SELECT
  md5(CONCAT(complaint_hash_key, policy_hash_key, origin_sys)) AS complaint_policy_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    pull_ts,
    origin_sys
  FROM crm_vehicle_asset_csv
)
SELECT
  md5(CONCAT(insured_object_hash_key, motor_hash_key, origin_sys)) AS insured_object_motor_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
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
    md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
    md5(CONCAT(property_ref, origin_sys)) AS home_hash_key,
    pull_ts,
    origin_sys
  FROM crm_property_asset_csv
)
SELECT
  md5(CONCAT(insured_object_hash_key, home_hash_key, origin_sys)) AS insured_object_home_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
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
    pull_ts,
    origin_sys
  FROM crm_policy_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
    pull_ts,
    origin_sys
  FROM crm_vehicle_asset_csv
  UNION ALL
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
    pull_ts,
    origin_sys
  FROM crm_property_asset_csv
)
SELECT
  md5(CONCAT(policy_hash_key, insured_object_hash_key, origin_sys)) AS policy_insured_object_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
    md5(CONCAT(src_channel_id, origin_sys)) AS channel_hash_key,
    pull_ts,
    origin_sys
  FROM crm_quote_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(quote_ref, src_system)) AS quote_hash_key,
    md5(CONCAT(src_channel_id, src_system)) AS channel_hash_key,
    pull_ts,
    src_system AS origin_sys
  FROM crm_channel_catalog_csv
)
SELECT
  md5(CONCAT(quote_hash_key, channel_hash_key, origin_sys)) AS quote_channel_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    pull_ts,
    origin_sys
  FROM crm_policy_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
    pull_ts,
    origin_sys
  FROM crm_quote_register_csv
)
SELECT
  md5(CONCAT(policy_hash_key, quote_hash_key)) AS policy_quote_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(src_agent_id, src_system)) AS broker_hash_key,
    pull_ts,
    COALESCE(origin_sys, src_system) AS origin_sys
  FROM crm_policy_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
    md5(CONCAT(src_agent_id, src_system)) AS broker_hash_key,
    pull_ts,
    COALESCE(origin_sys, src_system) AS origin_sys
  FROM crm_broker_book_csv
)
SELECT
  md5(CONCAT(policy_hash_key, broker_hash_key, origin_sys)) AS policy_broker_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
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
    md5(CONCAT(src_agent_id, origin_sys)) AS broker_hash_key,
    pull_ts,
    origin_sys
  FROM crm_quote_register_csv
  UNION ALL
  SELECT
    md5(CONCAT(quote_ref, src_system)) AS quote_hash_key,
    md5(CONCAT(src_agent_id, src_system)) AS broker_hash_key,
    pull_ts,
    src_system AS origin_sys
  FROM crm_broker_book_csv
)
SELECT
  md5(CONCAT(quote_hash_key, broker_hash_key, origin_sys)) AS quote_broker_hash_key,
  CAST(pull_ts AS timestamp) AS load_date,
  origin_sys AS record_source,
  quote_hash_key,
  broker_hash_key
FROM combined
""",
  "source_table": "crm_quote_register_csv,crm_broker_book_csv"
}
