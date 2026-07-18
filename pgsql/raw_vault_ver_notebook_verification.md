# Raw Vault Verification for ver Notebooks

Generated on 2026-07-18 09:59:40.

## Summary

- Notebook configs found: 90
- Unique target tables loaded: 87
- Duplicate target configs skipped by loader: hub_regulation, link_quote_channel, sat_policy_crm
- Missing bronze source tables: none
- Missing raw_vault target tables: none
- Extra raw_vault target tables: none
- Raw-vault FK-like link/sat-to-hub checks: 92
- Raw-vault missing references: 0

## CRM/SAP Hub Coverage

| Hub | Source counts |
|---|---|
| `hub_person` | CRM: 10000; SAP: 10000 |
| `hub_address` | CRM: 10000; SAP: 10000 |
| `hub_policy` | CRM: 2525; SAP: 1669 |
| `hub_product` | CRM: 7; SAP: 7 |
| `hub_home` | CRM: 792; SAP: 792 |
| `hub_motor` | CRM: 877; SAP: 877 |
| `hub_natural_person` | CRM: 8000; SAP: 8000 |
| `hub_legal_person` | CRM: 2000; SAP: 2000 |

## Target Counts

| Object Type | Target Table | Rows | Source Tables | Notebook |
|---|---|---:|---|---|
| hub | `hub_account` | 1799 | `crm_account_book_csv` | `hub_config (3).ipynb` |
| hub | `hub_address` | 20000 | `crm_address_book_csv, crm_enhanced_address_book_csv, sap_address_db` | `hub_config (3).ipynb` |
| hub | `hub_broker` | 20 | `crm_broker_book_csv` | `hub_config (3).ipynb` |
| hub | `hub_campaign` | 10 | `crm_campaign_register_csv` | `hub_config (3).ipynb` |
| hub | `hub_channel` | 3 | `crm_channel_catalog_csv` | `hub_config (3).ipynb` |
| hub | `hub_claim` | 202 | `crm_claim_register_csv` | `hub_config (3).ipynb` |
| hub | `hub_complaint` | 63 | `crm_complaint_register_csv` | `hub_config (3).ipynb` |
| hub | `hub_consent` | 2500 | `crm_consent_snapshot_csv` | `hub_config (3).ipynb` |
| hub | `hub_contact` | 10000 | `crm_contact_point_csv` | `hub_config (3).ipynb` |
| hub | `hub_customer` | 1799 | `crm_customer_portfolio_csv` | `hub_config (3).ipynb` |
| hub | `hub_home` | 1584 | `crm_property_asset_csv, sap_home_db` | `hub_config (3).ipynb` |
| hub | `hub_identities` | 10000 | `crm_identity_registry_csv` | `hub_config (3).ipynb` |
| hub | `hub_insured_object` | 1669 | `crm_property_asset_csv, crm_vehicle_asset_csv` | `hub_config (3).ipynb` |
| hub | `hub_lead` | 3706 | `crm_lead_register_csv` | `hub_config (3).ipynb` |
| hub | `hub_legal_person` | 4000 | `crm_party_master_csv, sap_person_db` | `hub_config (3).ipynb` |
| hub | `hub_marketing_engagement` | 1766 | `crm_campaign_touch_csv` | `hub_config (3).ipynb` |
| hub | `hub_marketing_preference` | 2500 | `crm_comm_preference_csv` | `hub_config (3).ipynb` |
| hub | `hub_motor` | 1754 | `crm_vehicle_asset_csv, sap_motor_db` | `hub_config (3).ipynb` |
| hub | `hub_natural_person` | 16000 | `crm_party_master_csv, sap_person_db` | `hub_config (3).ipynb` |
| hub | `hub_override` | 88 | `crm_override_register_csv` | `hub_config (3).ipynb` |
| hub | `hub_person` | 20000 | `crm_party_master_csv, sap_person_db` | `hub_config (3).ipynb` |
| hub | `hub_policy` | 4194 | `crm_policy_register_csv, sap_home_db, sap_motor_db` | `hub_config (3).ipynb` |
| hub | `hub_product` | 14 | `crm_product_catalog_csv, sap_home_db, sap_motor_db, sap_product_db` | `hub_config (3).ipynb` |
| hub | `hub_quote` | 5040 | `crm_quote_register_csv` | `hub_config (3).ipynb` |
| hub | `hub_regulation` | 100 | `crm_regulation_register_csv` | `hub_config (3).ipynb` |
| link | `link_broker_person` | 1059 | `crm_enhanced_person_relationships_csv` | `link_config (2).ipynb` |
| link | `link_claim_policy` | 202 | `crm_enhanced_policy_relationships_csv` | `link_config (2).ipynb` |
| link | `link_complaint_policy` | 63 | `crm_enhanced_policy_relationships_csv` | `link_config (2).ipynb` |
| link | `link_complaint_regulation` | 100 | `crm_enhanced_policy_relationships_csv` | `link_config (2).ipynb` |
| link | `link_customer_lead` | 904 | `crm_customer_lead_bridge_csv` | `link_config (2).ipynb` |
| link | `link_customer_person` | 1799 | `crm_customer_portfolio_csv` | `link_config (2).ipynb` |
| link | `link_insured_object_home` | 792 | `crm_enhanced_policy_relationships_csv` | `link_config (2).ipynb` |
| link | `link_insured_object_motor` | 877 | `crm_enhanced_policy_relationships_csv` | `link_config (2).ipynb` |
| link | `link_person_account` | 1799 | `crm_account_book_csv` | `link_config (2).ipynb` |
| link | `link_person_address` | 20000 | `crm_address_book_csv, crm_enhanced_person_relationships_csv, sap_address_db` | `link_config (2).ipynb` |
| link | `link_person_campaign` | 2500 | `crm_enhanced_person_relationships_csv` | `link_config (2).ipynb` |
| link | `link_person_consent` | 2500 | `crm_consent_snapshot_csv` | `link_config (2).ipynb` |
| link | `link_person_contact` | 10000 | `crm_contact_point_csv` | `link_config (2).ipynb` |
| link | `link_person_identities` | 10000 | `crm_identity_registry_csv` | `link_config (2).ipynb` |
| link | `link_person_lead` | 3706 | `crm_lead_register_csv` | `link_config (2).ipynb` |
| link | `link_person_legal_person` | 4000 | `crm_party_master_csv, sap_person_db` | `link_config (2).ipynb` |
| link | `link_person_marketing_engagement` | 1766 | `crm_campaign_touch_csv` | `link_config (2).ipynb` |
| link | `link_person_marketing_preference` | 2500 | `crm_comm_preference_csv` | `link_config (2).ipynb` |
| link | `link_person_natural_person` | 16000 | `crm_party_master_csv, sap_person_db` | `link_config (2).ipynb` |
| link | `link_policy_broker` | 895 | `crm_enhanced_policy_relationships_csv` | `link_config (2).ipynb` |
| link | `link_policy_channel` | 2525 | `crm_enhanced_policy_relationships_csv` | `link_config (2).ipynb` |
| link | `link_policy_customer` | 2525 | `crm_policy_register_csv` | `link_config (2).ipynb` |
| link | `link_policy_insured_object` | 1669 | `crm_enhanced_policy_relationships_csv` | `link_config (2).ipynb` |
| link | `link_policy_override` | 88 | `crm_enhanced_policy_relationships_csv` | `link_config (2).ipynb` |
| link | `link_policy_product` | 4194 | `crm_policy_register_csv, sap_home_db, sap_motor_db` | `link_config (2).ipynb` |
| link | `link_policy_quote` | 2525 | `crm_enhanced_policy_relationships_csv, crm_policy_register_csv` | `link_config (2).ipynb` |
| link | `link_quote_broker` | 1226 | `crm_enhanced_policy_relationships_csv` | `link_config (2).ipynb` |
| link | `link_quote_channel` | 5040 | `crm_enhanced_policy_relationships_csv` | `link_config (2).ipynb` |
| link | `link_quote_person` | 5040 | `crm_quote_register_csv` | `link_config (2).ipynb` |
| link | `link_quote_product` | 5040 | `crm_quote_register_csv` | `link_config (2).ipynb` |
| sat | `sat_account_crm` | 1799 | `crm_account_book_csv` | `sat_config (2).ipynb` |
| sat | `sat_address_crm` | 10000 | `crm_address_book_csv` | `sat_config (2).ipynb` |
| sat | `sat_address_sap` | 10000 | `sap_address_db` | `sat_config (2).ipynb` |
| sat | `sat_broker_crm` | 20 | `crm_broker_book_csv` | `sat_config (2).ipynb` |
| sat | `sat_campaign_crm` | 10 | `crm_campaign_register_csv` | `sat_config (2).ipynb` |
| sat | `sat_channel_crm` | 3 | `crm_channel_catalog_csv` | `sat_config (2).ipynb` |
| sat | `sat_claim_crm` | 202 | `crm_claim_register_csv` | `sat_config (2).ipynb` |
| sat | `sat_complaint_crm` | 63 | `crm_complaint_register_csv` | `sat_config (2).ipynb` |
| sat | `sat_consent_crm` | 2500 | `crm_consent_snapshot_csv` | `sat_config (2).ipynb` |
| sat | `sat_contact_crm` | 10000 | `crm_contact_point_csv` | `sat_config (2).ipynb` |
| sat | `sat_customer_crm` | 1799 | `crm_customer_portfolio_csv, crm_enhanced_enrichments_csv` | `sat_config (2).ipynb` |
| sat | `sat_home_crm` | 792 | `crm_property_asset_csv` | `sat_config (2).ipynb` |
| sat | `sat_home_sap` | 792 | `sap_home_db` | `sat_config (2).ipynb` |
| sat | `sat_identities_crm` | 10000 | `crm_identity_registry_csv` | `sat_config (2).ipynb` |
| sat | `sat_insured_object` | 1669 | `crm_property_asset_csv, crm_vehicle_asset_csv` | `sat_config (2).ipynb` |
| sat | `sat_lead_crm` | 3706 | `crm_lead_register_csv` | `sat_config (2).ipynb` |
| sat | `sat_legal_person_crm` | 2000 | `crm_party_master_csv` | `sat_config (2).ipynb` |
| sat | `sat_legal_person_sap` | 2000 | `sap_person_db` | `sat_config (2).ipynb` |
| sat | `sat_marketing_engagement_crm` | 1766 | `crm_campaign_touch_csv` | `sat_config (2).ipynb` |
| sat | `sat_marketing_preference_crm` | 2500 | `crm_comm_preference_csv` | `sat_config (2).ipynb` |
| sat | `sat_motor_crm` | 877 | `crm_vehicle_asset_csv` | `sat_config (2).ipynb` |
| sat | `sat_motor_sap` | 877 | `sap_motor_db` | `sat_config (2).ipynb` |
| sat | `sat_natural_person_crm` | 8000 | `crm_party_master_csv` | `sat_config (2).ipynb` |
| sat | `sat_natural_person_sap` | 8000 | `sap_person_db` | `sat_config (2).ipynb` |
| sat | `sat_override_crm` | 88 | `crm_override_register_csv` | `sat_config (2).ipynb` |
| sat | `sat_person_crm` | 10000 | `crm_party_master_csv` | `sat_config (2).ipynb` |
| sat | `sat_person_sap` | 10000 | `sap_person_db` | `sat_config (2).ipynb` |
| sat | `sat_policy_crm` | 2525 | `crm_enhanced_enrichments_csv, crm_policy_register_csv` | `sat_config (2).ipynb` |
| sat | `sat_product_crm` | 7 | `crm_product_catalog_csv` | `sat_config (2).ipynb` |
| sat | `sat_product_sap` | 7 | `sap_product_db` | `sat_config (2).ipynb` |
| sat | `sat_quote_crm` | 5040 | `crm_quote_register_csv` | `sat_config (2).ipynb` |
| sat | `sat_regulation_crm` | 100 | `crm_regulation_register_csv` | `sat_config (2).ipynb` |