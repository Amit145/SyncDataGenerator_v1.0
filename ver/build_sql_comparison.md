# Build SQL Comparison: Source Python vs Notebook

Generated from `ver` on 2026-07-17 15:52:35.

Comparison key: `target_table`. SQL comparison ignores only whitespace and letter case. Product ids and variable/object ids are reported as metadata, but table matching is based on table name because the source files use product `3` and the notebooks use product `9`.

## Summary

| Area | Source configs | Notebook configs | Common tables | SQL matches | SQL differences | Source only | Notebook only |
|---|---:|---:|---:|---:|---:|---:|---:|
| Hub | 25 | 25 | 25 | 0 | 25 | 0 | 0 |
| Link | 30 | 30 | 30 | 0 | 30 | 0 | 0 |
| Satellite | 32 | 32 | 30 | 0 | 30 | 2 | 2 |

## Hub

- Source Python: `ver/hub_config.py`
- Notebook: `ver/hub_config (3).ipynb`

| Table | Status | Source object | Notebook object |
|---|---|---|---|
| `hub_account` | Different | `hub_account_3` | `hub_account_9` |
| `hub_address` | Different | `hub_address_3` | `hub_address_9` |
| `hub_broker` | Different | `hub_broker_3` | `hub_broker_9` |
| `hub_campaign` | Different | `hub_campaign_3` | `hub_campaign_9` |
| `hub_channel` | Different | `hub_channel_3` | `hub_channel_9` |
| `hub_claim` | Different | `hub_claim_3` | `hub_claim_9` |
| `hub_complaint` | Different | `hub_complaint_3` | `hub_complaint_9` |
| `hub_consent` | Different | `hub_consent_3` | `hub_consent_9` |
| `hub_contact` | Different | `hub_contact_3` | `hub_contact_9` |
| `hub_customer` | Different | `hub_customer_3` | `hub_customer_9` |
| `hub_home` | Different | `hub_home_3` | `hub_home_9` |
| `hub_identities` | Different | `hub_identities_3` | `hub_identities_9` |
| `hub_insured_object` | Different | `hub_insured_object_3` | `hub_insured_object_9` |
| `hub_lead` | Different | `hub_lead_3` | `hub_lead_9` |
| `hub_legal_person` | Different | `hub_legal_person_3` | `hub_legal_person_9` |
| `hub_marketing_engagement` | Different | `hub_marketing_engagement_3` | `hub_marketing_engagement_9` |
| `hub_marketing_preference` | Different | `hub_marketing_preference_3` | `hub_marketing_preference_9` |
| `hub_motor` | Different | `hub_motor_3` | `hub_motor_9` |
| `hub_natural_person` | Different | `hub_natural_person_3` | `hub_natural_person_9` |
| `hub_override` | Different | `hub_override_3` | `hub_override_9` |
| `hub_person` | Different | `hub_person_3` | `hub_person_9` |
| `hub_policy` | Different | `hub_policy_3` | `hub_policy_9` |
| `hub_product` | Different | `hub_product_3` | `hub_product_9` |
| `hub_quote` | Different | `hub_quote_3` | `hub_quote_9` |
| `hub_regulation` | Different | `hub_regulation_3` | `hub_regulation_9` |

### hub_account

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_account_3` | `hub_account_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_account` | `hub_account` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_account_book_csv` | `crm_account_book_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,7 +1,6 @@
-SELECT
-  md5(CONCAT(account_ref, origin_sys)) AS account_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  account_ref AS account_id,
-  'hub_account' AS source_tbl_name
-FROM crm_account_book_csv
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, account_ref)) AS account_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    account_ref AS account_id
+FROM crm_account_book_csv;
```

</details>

### hub_address

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_address_3` | `hub_address_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_address` | `hub_address` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_address_book_csv,sap_address_db` | `crm_address_book_csv, crm_enhanced_address_book_csv, sap_address_db` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,17 +1,22 @@
-SELECT
-  md5(CONCAT(address_ref, origin_sys)) AS address_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  address_ref AS address_id,
-  'hub_address' AS source_tbl_name
+with combined as (
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, address_ref)) AS address_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    address_ref AS address_id
 FROM crm_address_book_csv
-
 UNION ALL
-
-SELECT
-  md5(CONCAT(address_id, origin_sys)) AS address_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  address_id AS address_id,
-  'hub_address' AS source_tbl_name
-FROM sap_address_db
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, src_address_id)) AS address_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    src_address_id AS address_id
+FROM crm_enhanced_address_book_csv
+UNION ALL
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, address_id)) AS address_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    address_id AS address_id
+FROM sap_address_db)
+select distinct * from combined;
```

</details>

### hub_broker

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_broker_3` | `hub_broker_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_broker` | `hub_broker` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_broker_book_csv` | `crm_broker_book_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,7 +1,6 @@
-SELECT
-  md5(CONCAT(src_agent_id, src_system)) AS broker_hash_key,
-  current_timestamp() AS load_date,
-  src_system AS record_source,
-  src_agent_id AS agent_id,
-  'hub_broker' AS source_tbl_name
-FROM crm_broker_book_csv
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, src_agent_id)) AS broker_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    src_agent_id AS agent_id
+FROM crm_broker_book_csv;
```

</details>

### hub_campaign

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_campaign_3` | `hub_campaign_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_campaign` | `hub_campaign` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_campaign_register_csv` | `crm_campaign_register_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,7 +1,6 @@
-SELECT
-  md5(CONCAT(src_campaign_id, src_system)) AS campaign_hash_key,
-  current_timestamp() AS load_date,
-  src_system AS record_source,
-  src_campaign_id AS campaign_id,
-  'hub_campaign' AS source_tbl_name
-FROM crm_campaign_register_csv
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, src_campaign_id)) AS campaign_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    src_campaign_id AS campaign_id
+FROM crm_campaign_register_csv;
```

</details>

### hub_channel

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_channel_3` | `hub_channel_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_channel` | `hub_channel` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_channel_catalog_csv` | `crm_channel_catalog_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,7 +1,6 @@
-SELECT
-  md5(CONCAT(src_channel_id, src_system)) AS channel_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  src_system AS record_source,
-  src_channel_id AS channel_id,
-  'hub_channel' AS source_tbl_name
-FROM crm_channel_catalog_csv
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, src_channel_id)) AS channel_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    src_channel_id AS channel_id
+FROM crm_channel_catalog_csv;
```

</details>

### hub_claim

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_claim_3` | `hub_claim_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_claim` | `hub_claim` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_claim_register_csv` | `crm_claim_register_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,7 +1,6 @@
-SELECT
-  md5(CONCAT(src_claim_id, src_system)) AS claim_hash_key,
-  current_timestamp() AS load_date,
-  src_system AS record_source,
-  src_claim_id AS claim_id,
-  'hub_claim' AS source_tbl_name
-FROM crm_claim_register_csv
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, src_claim_id)) AS claim_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    src_claim_id AS claim_id
+FROM crm_claim_register_csv;
```

</details>

### hub_complaint

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_complaint_3` | `hub_complaint_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_complaint` | `hub_complaint` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_complaint_register_csv` | `crm_complaint_register_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,7 +1,6 @@
-SELECT
-  md5(CONCAT(src_complaint_id, src_system)) AS complaint_hash_key,
-  current_timestamp() AS load_date,
-  src_system AS record_source,
-  src_complaint_id AS complaint_id,
-  'hub_complaint' AS source_tbl_name
-FROM crm_complaint_register_csv
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, src_complaint_id)) AS complaint_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    src_complaint_id AS complaint_id
+FROM crm_complaint_register_csv;
```

</details>

### hub_consent

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_consent_3` | `hub_consent_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_consent` | `hub_consent` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_consent_snapshot_csv` | `crm_consent_snapshot_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,7 +1,6 @@
-SELECT
-  md5(CONCAT(consent_ref, origin_sys)) AS consent_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  consent_ref AS consent_id,
-  'hub_consent' AS source_tbl_name
-FROM crm_consent_snapshot_csv
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, consent_ref)) AS consent_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    consent_ref AS consent_id
+FROM crm_consent_snapshot_csv;
```

</details>

### hub_contact

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_contact_3` | `hub_contact_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_contact` | `hub_contact` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_contact_point_csv` | `crm_contact_point_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,7 +1,6 @@
-SELECT
-  md5(CONCAT(contact_ref, origin_sys)) AS contact_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  contact_ref AS contact_id,
-  'hub_contact' AS source_tbl_name
-FROM crm_contact_point_csv
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, contact_ref)) AS contact_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    contact_ref AS contact_id
+FROM crm_contact_point_csv;
```

</details>

### hub_customer

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_customer_3` | `hub_customer_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_customer` | `hub_customer` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_customer_portfolio_csv` | `crm_customer_portfolio_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,7 +1,6 @@
-SELECT
-  md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  customer_ref AS customer_id,
-  'hub_customer' AS source_tbl_name
-FROM crm_customer_portfolio_csv
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, customer_ref)) AS customer_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    customer_ref AS customer_id
+FROM crm_customer_portfolio_csv;
```

</details>

### hub_home

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_home_3` | `hub_home_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_home` | `hub_home` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_property_asset_csv,sap_home_db` | `crm_property_asset_csv, sap_home_db` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,15 +1,15 @@
-SELECT
-  md5(CONCAT(property_ref, origin_sys)) AS home_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  property_ref AS insured_object_home_id,
-  'hub_home' AS source_tbl_name
+with combined as (
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, property_ref)) AS home_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    property_ref AS insured_object_home_id
 FROM crm_property_asset_csv
 UNION ALL
-SELECT
-  md5(CONCAT(home_id, origin_sys)) AS home_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  home_id AS insured_object_home_id,
-  'hub_home' AS source_tbl_name
-FROM sap_home_db
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, home_id)) AS home_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    home_id AS insured_object_home_id
+FROM sap_home_db)
+select distinct * from combined;
```

</details>

### hub_identities

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_identities_3` | `hub_identities_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_identities` | `hub_identities` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_identity_registry_csv` | `crm_identity_registry_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,7 +1,6 @@
-SELECT
-  md5(CONCAT(identity_ref, origin_sys)) AS identities_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  identity_ref AS identities_id,
-  'hub_identities' AS source_tbl_name
-FROM crm_identity_registry_csv
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, identity_ref)) AS identities_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    identity_ref AS identities_id
+FROM crm_identity_registry_csv;
```

</details>

### hub_insured_object

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_insured_object_3` | `hub_insured_object_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_insured_object` | `hub_insured_object` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_property_asset_csv,crm_vehicle_asset_csv` | `crm_property_asset_csv, crm_vehicle_asset_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,17 +1,15 @@
-SELECT
-  md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  insured_object_id AS insured_object_id,
-  'hub_insured_object' AS source_tbl_name
+with combined as (
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, insured_object_id)) AS insured_object_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    insured_object_id AS insured_object_id
 FROM crm_property_asset_csv
-
 UNION ALL
-
-SELECT
-  md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  insured_object_id AS insured_object_id,
-  'hub_insured_object' AS source_tbl_name
-FROM crm_vehicle_asset_csv
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, insured_object_id)) AS insured_object_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    insured_object_id AS insured_object_id
+FROM crm_vehicle_asset_csv)
+select distinct * from combined;
```

</details>

### hub_lead

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_lead_3` | `hub_lead_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_lead` | `hub_lead` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_lead_register_csv` | `crm_lead_register_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,7 +1,6 @@
-SELECT
-  md5(CONCAT(lead_ref, origin_sys)) AS lead_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  lead_ref AS lead_id,
-  'hub_lead' AS source_tbl_name
-FROM crm_lead_register_csv
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, lead_ref)) AS lead_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    lead_ref AS lead_id
+FROM crm_lead_register_csv;
```

</details>

### hub_legal_person

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_legal_person_3` | `hub_legal_person_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_legal_person` | `hub_legal_person` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_party_master_csv,sap_person_db` | `crm_party_master_csv, sap_person_db` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,15 +1,18 @@
-SELECT
-  md5(CONCAT(legal_ref, origin_sys)) AS legal_person_hash_key,
-  CAST(current_timestamp() AS timestamp) AS load_date,
-  origin_sys AS record_source,
-  legal_ref AS legal_person_id,
-  'hub_legal_person' AS source_tbl_name
+with combined as (
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, legal_ref)) AS legal_person_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    natural_ref AS legal_person_id
 FROM crm_party_master_csv
+WHERE party_kind = 'LEGAL'
 UNION ALL
-SELECT
-  md5(CONCAT(person_id, origin_sys)) AS legal_person_hash_key,
-  CAST(current_timestamp() AS timestamp) AS load_date,
-  origin_sys AS record_source,
-  person_id AS legal_person_id,
-  'hub_legal_person' AS source_tbl_name
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, person_id)) AS legal_person_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    person_id AS legal_person_id
 FROM sap_person_db
+WHERE person_type = 'LEGAL')
+
+select distinct * from combined;
```

</details>

### hub_marketing_engagement

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_marketing_engagement_3` | `hub_marketing_engagement_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_marketing_engagement` | `hub_marketing_engagement` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_enhanced_enrichments_csv` | `crm_campaign_touch_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,7 +1,6 @@
-SELECT
-  md5(CONCAT(src_marketing_engagement_ref, origin_sys)) AS marketing_engagement_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  src_marketing_engagement_ref AS marketing_engagement_id,
-  'hub_marketing_engagement' AS source_tbl_name
-FROM crm_enhanced_enrichments_csv
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, engagement_ref)) AS marketing_engagement_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    engagement_ref AS marketing_engagement_id
+FROM crm_campaign_touch_csv;
```

</details>

### hub_marketing_preference

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_marketing_preference_3` | `hub_marketing_preference_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_marketing_preference` | `hub_marketing_preference` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_comm_preference_csv` | `crm_comm_preference_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,7 +1,6 @@
-SELECT
-  md5(CONCAT(preference_ref, origin_sys)) AS marketing_preference_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  preference_ref AS marketing_preference_id,
-  'hub_marketing_preference' AS source_tbl_name
-FROM crm_comm_preference_csv
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, preference_ref)) AS marketing_preference_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    preference_ref AS marketing_preference_id
+FROM crm_comm_preference_csv;
```

</details>

### hub_motor

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_motor_3` | `hub_motor_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_motor` | `hub_motor` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_vehicle_asset_csv,sap_motor_db` | `crm_vehicle_asset_csv, sap_motor_db` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,15 +1,15 @@
-SELECT
-  md5(CONCAT(vehicle_ref, origin_sys)) AS motor_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  vehicle_ref AS insured_object_motor_id,
-  'hub_motor' AS source_tbl_name
+with combined as (
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, vehicle_ref)) AS motor_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    vehicle_ref AS insured_object_motor_id
 FROM crm_vehicle_asset_csv
 UNION ALL
-SELECT
-  md5(CONCAT(motor_id, origin_sys)) AS motor_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  motor_id AS insured_object_motor_id,
-  'hub_motor' AS source_tbl_name
-FROM sap_motor_db
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, motor_id)) AS motor_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    motor_id AS insured_object_motor_id
+FROM sap_motor_db)
+select distinct * from combined;
```

</details>

### hub_natural_person

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_natural_person_3` | `hub_natural_person_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_natural_person` | `hub_natural_person` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_party_master_csv,sap_person_db` | `crm_party_master_csv, sap_person_db` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,15 +1,18 @@
-SELECT
-  md5(CONCAT(natural_ref, origin_sys)) AS natural_person_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  natural_ref AS natural_person_id,
-  'hub_natural_person' AS source_tbl_name
+with combined as (
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, natural_ref)) AS natural_person_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    natural_ref AS natural_person_id
 FROM crm_party_master_csv
+WHERE party_kind = 'NATURAL'
 UNION ALL
-SELECT
-  md5(CONCAT(person_id, origin_sys)) AS natural_person_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  person_id AS natural_person_id,
-  'hub_natural_person' AS source_tbl_name
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, person_id)) AS natural_person_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    person_id AS natural_person_id
 FROM sap_person_db
+WHERE person_type = 'NATURAL')
+
+select distinct * from combined;
```

</details>

### hub_override

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_override_3` | `hub_override_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_override` | `hub_override` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_override_register_csv` | `crm_override_register_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,7 +1,6 @@
-SELECT
-  md5(CONCAT(src_override_id, src_system)) AS override_hash_key,
-  current_timestamp() AS load_date,
-  src_system AS record_source,
-  src_override_id AS override_id,
-  'hub_override' AS source_tbl_name
-FROM crm_override_register_csv
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, src_override_id)) AS override_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    src_override_id AS override_id
+FROM crm_override_register_csv;
```

</details>

### hub_person

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_person_3` | `hub_person_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_person` | `hub_person` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_party_master_csv,sap_person_db` | `crm_party_master_csv, sap_person_db` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,15 +1,15 @@
-SELECT
-  md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  party_ref AS person_id,
-  'hub_person' AS source_tbl_name
+with combined as (
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    party_ref AS person_id
 FROM crm_party_master_csv
 UNION ALL
-SELECT
-  md5(CONCAT(person_id, origin_sys)) AS person_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  person_id AS person_id,
-  'hub_person' AS source_tbl_name
-FROM sap_person_db
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, person_id)) AS person_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    person_id AS person_id
+FROM sap_person_db)
+select distinct * from combined;
```

</details>

### hub_policy

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_policy_3` | `hub_policy_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_policy` | `hub_policy` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_policy_register_csv` | `crm_policy_register_csv, sap_home_db, sap_motor_db` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,7 +1,22 @@
-SELECT
-  md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  policy_ref AS policy_id,
-  'hub_policy' AS source_tbl_name
+with combined as (
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, policy_ref)) AS policy_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    policy_ref AS policy_id
 FROM crm_policy_register_csv
+UNION ALL
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, policy_id)) AS policy_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    policy_id AS policy_id
+FROM sap_home_db
+UNION ALL
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, policy_id)) AS policy_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    policy_id AS policy_id
+FROM sap_motor_db)
+select distinct * from combined;
```

</details>

### hub_product

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_product_3` | `hub_product_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_product` | `hub_product` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_product_catelog_csv,sap_product_db` | `crm_product_catalog_csv, sap_product_db, sap_home_db, sap_motor_db` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,17 +1,29 @@
-SELECT
-  md5(CONCAT(product_ref, origin_sys)) AS product_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  product_ref AS product_id,
-  'hub_product' AS source_tbl_name
-FROM crm_product_catelog_csv
-
+with combined as (
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, product_ref)) AS product_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    product_ref AS product_id
+FROM crm_product_catalog_csv
 UNION ALL
-
-SELECT
-  md5(CONCAT(product_id, origin_sys)) AS product_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  product_id AS product_id,
-  'hub_product' AS source_tbl_name
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, product_id)) AS product_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    product_id AS product_id
 FROM sap_product_db
+UNION ALL
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, product_id)) AS product_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    product_id AS product_id
+FROM sap_home_db
+UNION ALL
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, product_id)) AS product_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    product_id AS product_id
+FROM sap_motor_db)
+select distinct * from combined;
```

</details>

### hub_quote

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_quote_3` | `hub_quote_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_quote` | `hub_quote` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_quote_register_csv` | `crm_quote_register_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,7 +1,6 @@
-SELECT
-  md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
-  current_timestamp() AS load_date,
-  origin_sys AS record_source,
-  quote_ref AS quote_id,
-  'hub_quote' AS source_tbl_name
-FROM crm_quote_register_csv
+SELECT DISTINCT
+    MD5(CONCAT(quote_ref, quote_ref)) AS quote_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    quote_ref AS quote_id
+FROM crm_quote_register_csv;
```

</details>

### hub_regulation

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `hub_regulation_3` | `hub_regulation_9` |
| `object_type` | `hub` | `hub` |
| `target_table` | `hub_regulation` | `hub_regulation` |
| `process_order` | `1` | `1` |
| `source_table` | `crm_regulation_register_csv` | `crm_regulation_register_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,7 +1,6 @@
-SELECT
-  md5(CONCAT(src_regulation_id, src_system)) AS regulation_hash_key,
-  current_timestamp() AS load_date,
-  src_system AS record_source,
-  src_regulation_id AS regulation_id,
-  'hub_regulation' AS source_tbl_name
-FROM crm_regulation_register_csv
+SELECT DISTINCT
+    MD5(CONCAT(origin_sys, src_regulation_id)) AS regulation_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    src_regulation_id AS regulation_id
+FROM crm_regulation_register_csv;
```

</details>

## Link

- Source Python: `ver/link_config.py`
- Notebook: `ver/link_config (2).ipynb`

| Table | Status | Source object | Notebook object |
|---|---|---|---|
| `link_broker_person` | Different | `link_broker_person_3` | `link_broker_person_9` |
| `link_claim_policy` | Different | `link_claim_policy_3` | `link_claim_policy_9` |
| `link_complaint_policy` | Different | `link_complaint_policy_3` | `link_complaint_policy_9` |
| `link_complaint_regulation` | Different | `link_complaint_regulation_3` | `link_complaint_regulation_9` |
| `link_customer_lead` | Different | `link_customer_lead_3` | `link_customer_lead_9` |
| `link_customer_person` | Different | `link_customer_person_3` | `link_customer_person_9` |
| `link_insured_object_home` | Different | `link_insured_object_home_3` | `link_insured_object_home_9` |
| `link_insured_object_motor` | Different | `link_insured_object_motor_3` | `link_insured_object_motor_9` |
| `link_person_account` | Different | `link_person_account_3` | `link_person_account_9` |
| `link_person_address` | Different | `link_person_address_3` | `link_person_address_9` |
| `link_person_campaign` | Different | `link_person_campaign_3` | `link_person_campaign_9` |
| `link_person_consent` | Different | `link_person_consent_3` | `link_person_consent_9` |
| `link_person_contact` | Different | `link_person_contact_3` | `link_person_contact_9` |
| `link_person_identities` | Different | `link_person_identities_3` | `link_person_identities_9` |
| `link_person_lead` | Different | `link_person_lead_3` | `link_person_lead_9` |
| `link_person_legal_person` | Different | `link_person_legal_person_3` | `link_person_legal_person_9` |
| `link_person_marketing_engagement` | Different | `link_person_marketing_engagement_3` | `link_person_marketing_engagement_9` |
| `link_person_marketing_preference` | Different | `link_person_marketing_preference_3` | `link_person_marketing_preference_9` |
| `link_person_natural_person` | Different | `link_person_natural_person_3` | `link_person_natural_person_9` |
| `link_policy_broker` | Different | `link_policy_broker_3` | `link_policy_broker_9` |
| `link_policy_channel` | Different | `link_policy_channel_3` | `link_policy_channel_9` |
| `link_policy_customer` | Different | `link_policy_customer_3` | `link_policy_customer_9` |
| `link_policy_insured_object` | Different | `link_policy_insured_object_3` | `link_policy_insured_object_9` |
| `link_policy_override` | Different | `link_policy_override_3` | `link_policy_override_9` |
| `link_policy_product` | Different | `link_policy_product_3` | `link_policy_product_9` |
| `link_policy_quote` | Different | `link_policy_quote_3` | `link_policy_quote_9` |
| `link_quote_broker` | Different | `link_quote_broker_3` | `link_quote_broker_9` |
| `link_quote_channel` | Different | `link_quote_channel_3` | `link_quote_channel_9` |
| `link_quote_person` | Different | `link_quote_person_3` | `link_quote_person_9` |
| `link_quote_product` | Different | `link_quote_product_3` | `link_quote_product_9` |

### link_broker_person

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_broker_person_3` | `link_broker_person_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_broker_person` | `link_broker_person` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_broker_book_csv,crm_party_master_csv` | `crm_enhanced_person_relationships_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,16 +1,8 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(src_agent_id, src_system)) AS broker_hash_key,
-    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
-    current_timestamp() AS load_ts,
-    COALESCE(src_system, origin_sys) AS record_source
-  FROM crm_broker_book_csv
-  CROSS JOIN crm_party_master_csv
-)
-SELECT
-  md5(CONCAT(broker_hash_key, person_hash_key)) AS broker_person_hash_key,
-  CAST(load_ts AS TIMESTAMP) AS load_date,
-  record_source,
-  broker_hash_key,
-  person_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, src_agent_id)),MD5(CONCAT(origin_sys, src_person_id))))AS broker_person_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, src_agent_id)) AS broker_hash_key,
+    MD5(CONCAT(origin_sys, src_person_id)) AS person_hash_key
+FROM crm_enhanced_person_relationships_csv
+WHERE source_extract = 'broker_person_bridge.csv';
```

</details>

### link_claim_policy

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_claim_policy_3` | `link_claim_policy_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_claim_policy` | `link_claim_policy` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_claim_register_csv,crm_policy_register_csv` | `crm_enhanced_policy_relationships_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,24 +1,8 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(src_claim_id, src_system)) AS claim_hash_key,
-    CAST(NULL AS STRING) AS policy_hash_key,
-    current_timestamp() AS etl_load_timestamp,
-    COALESCE(src_system, origin_sys) AS record_source
-  FROM crm_claim_register_csv
-
-  UNION ALL
-
-  SELECT
-    CAST(NULL AS STRING) AS claim_hash_key,
-    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
-    current_timestamp() AS etl_load_timestamp,
-    COALESCE(src_system, origin_sys) AS record_source
-  FROM crm_policy_register_csv
-)
-SELECT
-  md5(CONCAT(claim_hash_key, policy_hash_key)) AS claim_policy_hash_key,
-  CAST(etl_load_timestamp AS timestamp) AS load_date,
-  record_source AS record_source,
-  claim_hash_key,
-  policy_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, src_claim_id)),MD5(CONCAT(origin_sys, src_policy_id))))AS claim_policy_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, src_claim_id)) AS claim_hash_key,
+    MD5(CONCAT(origin_sys, src_policy_id)) AS policy_hash_key
+FROM crm_enhanced_policy_relationships_csv
+WHERE source_extract = 'claim_policy_bridge.csv';
```

</details>

### link_complaint_policy

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_complaint_policy_3` | `link_complaint_policy_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_complaint_policy` | `link_complaint_policy` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_complaint_register_csv,crm_policy_register_csv` | `crm_enhanced_policy_relationships_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,16 +1,8 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(src_complaint_id, src_system)) AS complaint_hash_key,
-    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
-    current_timestamp() AS load_date,
-    COALESCE(src_system, origin_sys) AS record_source
-  FROM crm_complaint_register_csv
-  CROSS JOIN crm_policy_register_csv
-)
-SELECT
-  md5(CONCAT(complaint_hash_key, policy_hash_key)) AS complaint_policy_hash_key,
-  CAST(load_date AS TIMESTAMP) AS load_date,
-  record_source AS record_source,
-  complaint_hash_key,
-  policy_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, src_complaint_id)),MD5(CONCAT(origin_sys, src_policy_id))))AS complaint_policy_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, src_complaint_id)) AS complaint_hash_key,
+    MD5(CONCAT(origin_sys, src_policy_id)) AS policy_hash_key
+FROM crm_enhanced_policy_relationships_csv
+WHERE source_extract = 'complaint_policy_bridge.csv';
```

</details>

### link_complaint_regulation

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_complaint_regulation_3` | `link_complaint_regulation_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_complaint_regulation` | `link_complaint_regulation` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_complaint_register_csv,crm_regulation_register_csv` | `crm_enhanced_policy_relationships_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,24 +1,8 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(src_complaint_id, src_system)) AS complaint_hash_key,
-    md5(CONCAT(src_regulation_id, src_system)) AS regulation_hash_key,
-    current_timestamp() AS load_ts,
-    src_system AS origin_sys,
-    'crm_complaint_register_csv' AS source_tbl_name
-  FROM crm_complaint_register_csv
-  UNION ALL
-  SELECT
-    md5(CONCAT(src_complaint_id, src_system)) AS complaint_hash_key,
-    md5(CONCAT(src_regulation_id, src_system)) AS regulation_hash_key,
-    current_timestamp() AS load_ts,
-    src_system AS origin_sys,
-    'crm_regulation_register_csv' AS source_tbl_name
-  FROM crm_regulation_register_csv
-)
-SELECT
-  md5(CONCAT(src_complaint_id, src_regulation_id)) AS complaint_regulation_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  COALESCE(origin_sys, origin_sys) AS record_source,
-  complaint_hash_key,
-  regulation_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, src_complaint_id)),MD5(CONCAT(origin_sys, src_regulation_id))))AS complaint_regulation_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, src_complaint_id)) AS complaint_hash_key,
+    MD5(CONCAT(origin_sys, src_regulation_id)) AS regulation_hash_key
+FROM crm_enhanced_policy_relationships_csv
+WHERE source_extract = 'complaint_regulation_bridge.csv';
```

</details>

### link_customer_lead

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_customer_lead_3` | `link_customer_lead_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_customer_lead` | `link_customer_lead` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_customer_portfolio_csv,crm_lead_register_csv` | `crm_customer_lead_bridge_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,24 +1,7 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
-    md5(CONCAT(lead_ref, origin_sys)) AS lead_hash_key,
-    current_timestamp() AS load_ts,
-    origin_sys,
-    'link_customer_lead' AS source_tbl_name
-  FROM crm_customer_portfolio_csv
-  UNION ALL
-  SELECT
-    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
-    md5(CONCAT(lead_ref, origin_sys)) AS lead_hash_key,
-    current_timestamp() AS load_ts,
-    origin_sys,
-    'link_customer_lead' AS source_tbl_name
-  FROM crm_lead_register_csv
-)
-SELECT
-  md5(CONCAT(customer_ref, lead_ref)) AS customer_lead_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  COALESCE(origin_sys, origin_sys) AS record_source,
-  customer_hash_key,
-  lead_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, lead_ref)),MD5(CONCAT(origin_sys, customer_ref))))AS customer_lead_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, lead_ref)) AS lead_hash_key,
+    MD5(CONCAT(origin_sys,customer_ref)) AS customer_hash_key
+FROM crm_customer_lead_bridge_csv;
```

</details>

### link_customer_person

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_customer_person_3` | `link_customer_person_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_customer_person` | `link_customer_person` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_customer_portfolio_csv,crm_party_master_csv` | `crm_customer_portfolio_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,24 +1,7 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
-    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
-    current_timestamp() AS load_ts,
-    origin_sys,
-    'crm_customer_portfolio_csv' AS source_tbl_name
-  FROM crm_customer_portfolio_csv
-  UNION ALL
-  SELECT
-    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
-    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
-    current_timestamp() AS load_ts,
-    origin_sys,
-    'crm_party_master_csv' AS source_tbl_name
-  FROM crm_party_master_csv
-)
-SELECT
-  md5(CONCAT(customer_hash_key, person_hash_key)) AS customer_person_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  COALESCE(origin_sys, origin_sys) AS record_source,
-  customer_hash_key,
-  person_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, customer_ref))))AS customer_person_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
+    MD5(CONCAT(origin_sys,customer_ref)) AS customer_hash_key
+FROM crm_customer_portfolio_csv;
```

</details>

### link_insured_object_home

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_insured_object_home_3` | `link_insured_object_home_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_insured_object_home` | `link_insured_object_home` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_property_asset_csv` | `crm_enhanced_policy_relationships_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,17 +1,8 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(src_insured_object_id, property_ref)) AS insured_object_home_hash_key,
-    md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
-    md5(CONCAT(property_ref, origin_sys)) AS home_hash_key,
-    current_timestamp() AS load_timestamp,
-    origin_sys,
-    'crm_property_asset_csv' AS source_tbl_name
-  FROM crm_property_asset_csv
-)
-SELECT
-  md5(CONCAT(insured_object_hash_key, home_hash_key, origin_sys)) AS insured_object_home_hash_key,
-  CAST(load_timestamp AS timestamp) AS load_date,
-  origin_sys AS record_source,
-  insured_object_hash_key,
-  home_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, src_insured_object_id)),MD5(CONCAT(origin_sys, src_home_id))))AS insured_object_home_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, src_insured_object_id)) AS insured_object_hash_key,
+    MD5(CONCAT(origin_sys, src_home_id)) AS home_hash_key
+FROM crm_enhanced_policy_relationships_csv
+WHERE source_extract = 'insured_object_home_bridge.csv';
```

</details>

### link_insured_object_motor

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_insured_object_motor_3` | `link_insured_object_motor_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_insured_object_motor` | `link_insured_object_motor` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_vehicle_asset_csv` | `crm_enhanced_policy_relationships_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,16 +1,8 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
-    md5(CONCAT(vehicle_ref, origin_sys)) AS motor_hash_key,
-    current_timestamp() AS load_timestamp,
-    origin_sys,
-    'crm_vehicle_asset_csv' AS source_tbl_name
-  FROM crm_vehicle_asset_csv
-)
-SELECT
-  md5(CONCAT(insured_object_hash_key, motor_hash_key, origin_sys)) AS insured_object_motor_hash_key,
-  CAST(load_timestamp AS TIMESTAMP) AS load_date,
-  origin_sys AS record_source,
-  insured_object_hash_key,
-  motor_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, src_insured_object_id)),MD5(CONCAT(origin_sys, src_motor_id))))AS insured_object_motor_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, src_insured_object_id)) AS insured_object_hash_key,
+    MD5(CONCAT(origin_sys, src_motor_id)) AS motor_hash_key
+FROM crm_enhanced_policy_relationships_csv
+WHERE source_extract = 'insured_object_motor_bridge.csv';
```

</details>

### link_person_account

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_person_account_3` | `link_person_account_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_person_account` | `link_person_account` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_party_master_csv,crm_account_book_csv` | `crm_account_book_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,18 +1,7 @@
-WITH combined AS (
-  SELECT
-    SHA2(CONCAT_WS('||', CAST(party_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS person_hash_key,
-    SHA2(CONCAT_WS('||', CAST(account_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS account_hash_key,
-    current_timestamp() AS load_ts,
-    origin_sys,
-    'link_person_account' AS source_tbl_name
-  FROM crm_party_master_csv
-  JOIN crm_account_book_csv
-    ON crm_party_master_csv.party_ref = crm_account_book_csv.party_ref
-)
-SELECT
-  SHA2(CONCAT_WS('||', CAST(person_hash_key AS STRING), CAST(account_hash_key AS STRING), CAST(origin_sys AS STRING)), 256) AS person_account_hash_key,
-  CAST(load_ts AS TIMESTAMP) AS load_date,
-  COALESCE(origin_sys, origin_sys) AS record_source,
-  person_hash_key,
-  account_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, account_ref))))AS person_account_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
+    MD5(CONCAT(origin_sys,account_ref)) AS account_hash_key
+FROM crm_account_book_csv;
```

</details>

### link_person_address

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_person_address_3` | `link_person_address_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_person_address` | `link_person_address` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_party_master_csv,crm_address_book_csv` | `crm_address_book_csv, crm_enhanced_person_relationships_csv, sap_address_db` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,17 +1,26 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
-    md5(CONCAT(address_ref, origin_sys)) AS address_hash_key,
-    current_timestamp() AS load_ts,
-    origin_sys,
-    'link_person_address' AS source_tbl_name
-  FROM crm_party_master_csv
-  CROSS JOIN crm_address_book_csv
-)
-SELECT
-  md5(CONCAT(person_hash_key, address_hash_key)) AS person_address_hash_key,
-  CAST(load_ts AS TIMESTAMP) AS load_date,
-  COALESCE(origin_sys, origin_sys) AS record_source,
-  person_hash_key,
-  address_hash_key
-FROM combined
+with combined as (
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, address_ref))))AS person_address_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
+    MD5(CONCAT(origin_sys,address_ref)) AS address_hash_key
+FROM crm_address_book_csv
+UNION ALL
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, src_person_id)),MD5(CONCAT(origin_sys, src_address_id))))AS person_address_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, src_person_id)) AS person_hash_key,
+    MD5(CONCAT(origin_sys,src_address_id)) AS address_hash_key
+FROM crm_enhanced_person_relationships_csv
+WHERE source_extract = 'person_address_bridge.csv'
+UNION ALL
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, person_id)),MD5(CONCAT(origin_sys, address_id))))AS person_address_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, person_id)) AS person_hash_key,
+    MD5(CONCAT(origin_sys,address_id)) AS address_hash_key
+FROM sap_address_db)
+select distinct * from combined;
```

</details>

### link_person_campaign

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_person_campaign_3` | `link_person_campaign_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_person_campaign` | `link_person_campaign` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_party_master_csv,crm_campaign_register_csv` | `crm_enhanced_person_relationships_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,17 +1,8 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(party_ref, src_campaign_id)) AS person_campaign_hash_key,
-    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
-    md5(CONCAT(src_campaign_id, src_system)) AS campaign_hash_key,
-    current_timestamp() AS load_timestamp,
-    coalesce(origin_sys, src_system) AS record_source
-  FROM crm_party_master_csv
-  CROSS JOIN crm_campaign_register_csv
-)
-SELECT
-  person_campaign_hash_key,
-  CAST(load_timestamp AS timestamp) AS load_date,
-  record_source,
-  person_hash_key,
-  campaign_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, src_person_id)),MD5(CONCAT(origin_sys, src_campaign_id))))AS person_campaign_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, src_person_id)) AS person_hash_key,
+    MD5(CONCAT(origin_sys, src_campaign_id)) AS campaign_hash_key
+FROM crm_enhanced_person_relationships_csv
+WHERE source_extract = 'person_campaign_bridge.csv';
```

</details>

### link_person_consent

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_person_consent_3` | `link_person_consent_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_person_consent` | `link_person_consent` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_party_master_csv,crm_consent_snapshot_csv` | `crm_consent_snapshot_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,17 +1,7 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
-    md5(CONCAT(consent_ref, origin_sys)) AS consent_hash_key,
-    current_timestamp() AS load_ts,
-    origin_sys,
-    'link_person_consent' AS source_tbl_name
-  FROM crm_party_master_csv
-  CROSS JOIN crm_consent_snapshot_csv
-)
-SELECT
-  md5(CONCAT(person_hash_key, consent_hash_key)) AS person_consent_hash_key,
-  CAST(load_ts AS TIMESTAMP) AS load_date,
-  COALESCE(origin_sys, origin_sys) AS record_source,
-  person_hash_key,
-  consent_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, consent_ref))))AS person_consent_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
+    MD5(CONCAT(origin_sys,consent_ref)) AS consent_hash_key
+FROM crm_consent_snapshot_csv;
```

</details>

### link_person_contact

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_person_contact_3` | `link_person_contact_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_person_contact` | `link_person_contact` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_party_master_csv,crm_contact_point_csv` | `crm_contact_point_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,24 +1,7 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
-    md5(CONCAT(contact_ref, origin_sys)) AS contact_hash_key,
-    current_timestamp() AS load_ts,
-    origin_sys,
-    'link_person_contact' AS source_tbl_name
-  FROM crm_party_master_csv
-  UNION ALL
-  SELECT
-    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
-    md5(CONCAT(contact_ref, origin_sys)) AS contact_hash_key,
-    current_timestamp() AS load_ts,
-    origin_sys,
-    'link_person_contact' AS source_tbl_name
-  FROM crm_contact_point_csv
-)
-SELECT
-  md5(CONCAT(person_hash_key, contact_hash_key)) AS person_contact_hash_key,
-  CAST(load_ts AS TIMESTAMP) AS load_date,
-  COALESCE(origin_sys, origin_sys) AS record_source,
-  person_hash_key,
-  contact_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, contact_ref))))AS person_contact_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
+    MD5(CONCAT(origin_sys,contact_ref)) AS contact_hash_key
+FROM crm_contact_point_csv;
```

</details>

### link_person_identities

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_person_identities_3` | `link_person_identities_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_person_identities` | `link_person_identities` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_party_master_csv,crm_identity_registry_csv` | `crm_identity_registry_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,24 +1,7 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
-    md5(CONCAT(identity_ref, origin_sys)) AS identities_hash_key,
-    load_timestamp AS source_load_timestamp_col,
-    origin_sys,
-    'crm_party_master_csv' AS source_tbl_name
-  FROM crm_party_master_csv
-  UNION ALL
-  SELECT
-    md5(CONCAT(person_id, origin_sys)) AS person_hash_key,
-    md5(CONCAT(identity_ref, origin_sys)) AS identities_hash_key,
-    load_timestamp AS source_load_timestamp_col,
-    origin_sys,
-    'crm_identity_registry_csv' AS source_tbl_name
-  FROM crm_identity_registry_csv
-)
-SELECT
-  md5(CONCAT(person_hash_key, identities_hash_key)) AS person_identities_hash_key,
-  CAST(source_load_timestamp_col AS timestamp) AS load_date,
-  COALESCE(origin_sys, origin_sys) AS record_source,
-  person_hash_key,
-  identities_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, identity_ref))))AS person_identities_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
+    MD5(CONCAT(origin_sys,identity_ref)) AS identities_hash_key
+FROM crm_identity_registry_csv;
```

</details>

### link_person_lead

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_person_lead_3` | `link_person_lead_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_person_lead` | `link_person_lead` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_party_master_csv,crm_lead_register_csv` | `crm_lead_register_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,17 +1,7 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
-    md5(CONCAT(lead_ref, origin_sys)) AS lead_hash_key,
-    current_timestamp() AS load_ts,
-    origin_sys,
-    'link_person_lead' AS source_tbl_name
-  FROM crm_party_master_csv
-  CROSS JOIN crm_lead_register_csv
-)
-SELECT
-  md5(CONCAT(person_hash_key, lead_hash_key)) AS person_lead_hash_key,
-  CAST(load_ts AS TIMESTAMP) AS load_date,
-  origin_sys AS record_source,
-  person_hash_key,
-  lead_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, lead_ref))))AS person_lead_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
+    MD5(CONCAT(origin_sys,lead_ref)) AS lead_hash_key
+FROM crm_lead_register_csv;
```

</details>

### link_person_legal_person

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_person_legal_person_3` | `link_person_legal_person_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_person_legal_person` | `link_person_legal_person` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_party_master_csv,sap_person_db` | `crm_party_master_csv, sap_person_db` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,26 +1,19 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
-    md5(CONCAT(legal_ref, origin_sys)) AS legal_person_hash_key,
-    origin_sys,
-    current_timestamp() AS etl_load_timestamp,
-    'crm_party_master_csv' AS source_tbl_name
-  FROM crm_party_master_csv
-
-  UNION ALL
-
-  SELECT
-    md5(CONCAT(person_id, origin_sys)) AS person_hash_key,
-    md5(CONCAT(person_id, origin_sys)) AS legal_person_hash_key,
-    origin_sys,
-    current_timestamp() AS etl_load_timestamp,
-    'sap_person_db' AS source_tbl_name
-  FROM sap_person_db
-)
-SELECT
-  md5(CONCAT(person_hash_key, legal_person_hash_key)) AS person_legal_person_hash_key,
-  CAST(etl_load_timestamp AS timestamp) AS load_date,
-  origin_sys AS record_source,
-  person_hash_key,
-  legal_person_hash_key
-FROM combined
+with combined as (
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, legal_ref))))AS person_legal_person_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
+    MD5(CONCAT(origin_sys, legal_ref)) AS legal_person_hash_key
+FROM crm_party_master_csv
+WHERE where party_kind = 'LEGAL'
+UNION ALL
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, person_id)),MD5(CONCAT(origin_sys, person_id))))AS person_legal_person_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, person_id)) AS person_hash_key,
+    MD5(CONCAT(origin_sys, person_id)) AS legal_person_hash_key
+FROM sap_person_db 
+WHERE person_type = 'LEGAL')
+select distinct * from combined;
```

</details>

### link_person_marketing_engagement

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_person_marketing_engagement_3` | `link_person_marketing_engagement_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_person_marketing_engagement` | `link_person_marketing_engagement` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_party_master_csv,crm_enhanced_enrichments_csv` | `crm_campaign_touch_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,24 +1,7 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
-    md5(CONCAT(src_marketing_engagement_ref, origin_sys)) AS marketing_engagement_hash_key,
-    current_timestamp() AS etl_load_timestamp,
-    origin_sys,
-    'crm_party_master_csv' AS source_tbl_name
-  FROM crm_party_master_csv
-  UNION ALL
-  SELECT
-    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
-    md5(CONCAT(src_marketing_engagement_ref, origin_sys)) AS marketing_engagement_hash_key,
-    current_timestamp() AS etl_load_timestamp,
-    origin_sys,
-    'crm_enhanced_enrichments_csv' AS source_tbl_name
-  FROM crm_enhanced_enrichments_csv
-)
-SELECT
-  md5(CONCAT(person_hash_key, marketing_engagement_hash_key)) AS person_marketing_engagement_hash_key,
-  CAST(etl_load_timestamp AS timestamp) AS load_date,
-  COALESCE(origin_sys, origin_sys) AS record_source,
-  person_hash_key,
-  marketing_engagement_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, engagement_ref))))AS person_marketing_engagement_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
+    MD5(CONCAT(origin_sys,engagement_ref)) AS marketing_engagement_hash_key
+FROM crm_campaign_touch_csv;
```

</details>

### link_person_marketing_preference

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_person_marketing_preference_3` | `link_person_marketing_preference_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_person_marketing_preference` | `link_person_marketing_preference` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_party_master_csv,crm_comm_preference_csv` | `crm_comm_preference_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,18 +1,7 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
-    md5(CONCAT(preference_ref, origin_sys)) AS marketing_preference_hash_key,
-    current_timestamp() AS etl_load_timestamp,
-    origin_sys,
-    'link_person_marketing_preference' AS source_tbl_name
-  FROM crm_party_master_csv
-  JOIN crm_comm_preference_csv
-    ON crm_party_master_csv.party_ref = crm_comm_preference_csv.party_ref
-)
-SELECT
-  md5(CONCAT(person_hash_key, marketing_preference_hash_key)) AS person_marketing_preference_hash_key,
-  CAST(etl_load_timestamp AS timestamp) AS load_date,
-  COALESCE(origin_sys, origin_sys) AS record_source,
-  person_hash_key,
-  marketing_preference_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, preference_ref))))AS person_marketing_preference_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
+    MD5(CONCAT(origin_sys,preference_ref)) AS marketing_preference_hash_key
+FROM crm_comm_preference_csv;
```

</details>

### link_person_natural_person

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_person_natural_person_3` | `link_person_natural_person_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_person_natural_person` | `link_person_natural_person` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_party_master_csv,sap_person_db` | `crm_party_master_csv, sap_person_db` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,26 +1,19 @@
-WITH combined AS (
-  SELECT
-    SHA2(CONCAT_WS('||', CAST(party_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS person_hash_key,
-    SHA2(CONCAT_WS('||', CAST(natural_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS natural_person_hash_key,
-    source_load_timestamp AS source_load_timestamp_col,
-    origin_sys,
-    'crm_party_master_csv' AS source_tbl_name
-  FROM crm_party_master_csv
-
-  UNION ALL
-
-  SELECT
-    SHA2(CONCAT_WS('||', CAST(person_id AS STRING), CAST(origin_sys AS STRING)), 256) AS person_hash_key,
-    SHA2(CONCAT_WS('||', CAST(person_id AS STRING), CAST(origin_sys AS STRING)), 256) AS natural_person_hash_key,
-    source_load_timestamp AS source_load_timestamp_col,
-    origin_sys,
-    'sap_person_db' AS source_tbl_name
-  FROM sap_person_db
-)
-SELECT
-  SHA2(CONCAT_WS('||', CAST(person_hash_key AS STRING), CAST(natural_person_hash_key AS STRING), CAST(origin_sys AS STRING)), 256) AS person_natural_person_hash_key,
-  CAST(source_load_timestamp_col AS TIMESTAMP) AS load_date,
-  origin_sys AS record_source,
-  person_hash_key,
-  natural_person_hash_key
-FROM combined
+with combined as (
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, natural_ref))))AS person_natural_person_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
+    MD5(CONCAT(origin_sys, natural_ref)) AS natural_person_hash_key
+FROM crm_party_master_csv
+WHERE where party_kind = 'NATURAL'
+UNION ALL
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, person_id)),MD5(CONCAT(origin_sys, person_id))))AS person_natural_person_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, person_id)) AS person_hash_key,
+    MD5(CONCAT(origin_sys, person_id)) AS natural_person_hash_key
+FROM sap_person_db 
+WHERE person_type = 'NATURAL')
+select distinct * from combined;
```

</details>

### link_policy_broker

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_policy_broker_3` | `link_policy_broker_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_policy_broker` | `link_policy_broker` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_policy_register_csv,crm_broker_book_csv` | `crm_enhanced_policy_relationships_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,22 +1,8 @@
-WITH combined AS (
-  SELECT
-    SHA2(CONCAT_WS('||', CAST(policy_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS policy_hash_key,
-    SHA2(CONCAT_WS('||', CAST(src_agent_id AS STRING), CAST(src_system AS STRING)), 256) AS broker_hash_key,
-    current_timestamp() AS load_ts,
-    COALESCE(origin_sys, src_system) AS record_source
-  FROM crm_policy_register_csv
-  UNION ALL
-  SELECT
-    SHA2(CONCAT_WS('||', CAST(policy_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS policy_hash_key,
-    SHA2(CONCAT_WS('||', CAST(src_agent_id AS STRING), CAST(src_system AS STRING)), 256) AS broker_hash_key,
-    current_timestamp() AS load_ts,
-    COALESCE(origin_sys, src_system) AS record_source
-  FROM crm_broker_book_csv
-)
-SELECT
-  SHA2(CONCAT_WS('||', CAST(policy_hash_key AS STRING), CAST(broker_hash_key AS STRING)), 256) AS policy_broker_hash_key,
-  CAST(load_ts AS TIMESTAMP) AS load_date,
-  record_source,
-  policy_hash_key,
-  broker_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, src_policy_id)),MD5(CONCAT(origin_sys, src_agent_id))))AS policy_broker_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, src_policy_id)) AS policy_hash_key,
+    MD5(CONCAT(origin_sys, src_agent_id)) AS broker_hash_key
+FROM crm_enhanced_policy_relationships_csv
+WHERE source_extract = 'policy_broker_bridge.csv';
```

</details>

### link_policy_channel

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_policy_channel_3` | `link_policy_channel_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_policy_channel` | `link_policy_channel` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_policy_register_csv,crm_channel_catalog_csv` | `crm_enhanced_policy_relationships_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,17 +1,8 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(policy_ref, src_channel_id)) AS policy_channel_hash_key,
-    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
-    md5(CONCAT(src_channel_id, src_system)) AS channel_hash_key,
-    current_timestamp() AS load_ts,
-    coalesce(origin_sys, src_system) AS record_source
-  FROM crm_policy_register_csv
-  CROSS JOIN crm_channel_catalog_csv
-)
-SELECT
-  policy_channel_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  record_source,
-  policy_hash_key,
-  channel_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, src_policy_id)),MD5(CONCAT(origin_sys, src_channel_id))))AS policy_channel_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, src_policy_id)) AS policy_hash_key,
+    MD5(CONCAT(origin_sys, src_channel_id)) AS channel_hash_key
+FROM crm_enhanced_policy_relationships_csv
+WHERE source_extract = 'policy_channel_bridge.csv';
```

</details>

### link_policy_customer

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_policy_customer_3` | `link_policy_customer_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_policy_customer` | `link_policy_customer` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_policy_register_csv,crm_customer_portfolio_csv` | `crm_policy_register_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,24 +1,7 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
-    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
-    current_timestamp() AS load_ts,
-    origin_sys,
-    'crm_policy_register_csv' AS source_tbl_name
-  FROM crm_policy_register_csv
-  UNION ALL
-  SELECT
-    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
-    md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
-    current_timestamp() AS load_ts,
-    origin_sys,
-    'crm_customer_portfolio_csv' AS source_tbl_name
-  FROM crm_customer_portfolio_csv
-)
-SELECT
-  md5(CONCAT(policy_hash_key, customer_hash_key)) AS policy_customer_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  COALESCE(origin_sys, origin_sys) AS record_source,
-  customer_hash_key,
-  policy_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, policy_ref)),MD5(CONCAT(origin_sys, customer_ref))))AS policy_customer_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, policy_ref)) AS policy_hash_key,
+    MD5(CONCAT(origin_sys,customer_ref)) AS customer_hash_key
+FROM crm_policy_register_csv;
```

</details>

### link_policy_insured_object

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_policy_insured_object_3` | `link_policy_insured_object_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_policy_insured_object` | `link_policy_insured_object` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_policy_register_csv,crm_vehicle_asset_csv,crm_property_asset_csv` | `crm_enhanced_policy_relationships_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,32 +1,8 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
-    md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
-    source_load_timestamp AS source_load_timestamp,
-    origin_sys,
-    'crm_policy_register_csv' AS source_tbl_name
-  FROM crm_policy_register_csv
-  UNION ALL
-  SELECT
-    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
-    md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
-    source_load_timestamp AS source_load_timestamp,
-    origin_sys,
-    'crm_vehicle_asset_csv' AS source_tbl_name
-  FROM crm_vehicle_asset_csv
-  UNION ALL
-  SELECT
-    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
-    md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
-    source_load_timestamp AS source_load_timestamp,
-    origin_sys,
-    'crm_property_asset_csv' AS source_tbl_name
-  FROM crm_property_asset_csv
-)
-SELECT
-  md5(CONCAT(policy_hash_key, insured_object_hash_key)) AS policy_insured_object_hash_key,
-  CAST(source_load_timestamp AS TIMESTAMP) AS load_date,
-  COALESCE(origin_sys, origin_sys) AS record_source,
-  policy_hash_key,
-  insured_object_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, src_policy_id)),MD5(CONCAT(origin_sys, src_insured_object_id))))AS policy_insured_object_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, src_policy_id)) AS policy_hash_key,
+    MD5(CONCAT(origin_sys, src_insured_object_id)) AS insured_object_hash_key
+FROM crm_enhanced_policy_relationships_csv
+WHERE source_extract = 'policy_insured_object_bridge.csv';
```

</details>

### link_policy_override

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_policy_override_3` | `link_policy_override_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_policy_override` | `link_policy_override` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_policy_register_csv,crm_override_register_csv` | `crm_enhanced_policy_relationships_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,24 +1,8 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(policy_ref, src_override_id)) AS policy_override_hash_key,
-    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
-    md5(CONCAT(src_override_id, src_system)) AS override_hash_key,
-    etl_load_timestamp AS source_load_timestamp,
-    COALESCE(origin_sys, src_system) AS record_source
-  FROM crm_policy_register_csv
-  UNION ALL
-  SELECT
-    md5(CONCAT(policy_ref, src_override_id)) AS policy_override_hash_key,
-    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
-    md5(CONCAT(src_override_id, src_system)) AS override_hash_key,
-    etl_load_timestamp AS source_load_timestamp,
-    COALESCE(origin_sys, src_system) AS record_source
-  FROM crm_override_register_csv
-)
-SELECT
-  policy_override_hash_key,
-  CAST(source_load_timestamp AS TIMESTAMP) AS load_date,
-  record_source,
-  policy_hash_key,
-  override_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, src_policy_id)),MD5(CONCAT(origin_sys, src_override_id))))AS policy_override_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, src_policy_id)) AS policy_hash_key,
+    MD5(CONCAT(origin_sys, src_override_id)) AS override_hash_key
+FROM crm_enhanced_policy_relationships_csv
+WHERE source_extract = 'policy_override_bridge.csv';
```

</details>

### link_policy_product

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_policy_product_3` | `link_policy_product_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_policy_product` | `link_policy_product` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_policy_register_csv,crm_product_catelog_csv` | `crm_policy_register_csv, sap_home_db, sap_motor_db` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,18 +1,25 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
-    md5(CONCAT(product_ref, origin_sys)) AS product_hash_key,
-    current_timestamp() AS etl_load_timestamp,
-    origin_sys,
-    'link_policy_product' AS source_tbl_name
-  FROM crm_policy_register_csv
-  JOIN crm_product_catelog_csv
-    ON crm_policy_register_csv.product_ref = crm_product_catelog_csv.product_ref
-)
-SELECT
-  md5(CONCAT(policy_ref, product_ref)) AS policy_product_hash_key,
-  CAST(etl_load_timestamp AS timestamp) AS load_date,
-  COALESCE(origin_sys, origin_sys) AS record_source,
-  policy_hash_key,
-  product_hash_key
-FROM combined
+with combined as (
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, policy_ref)),MD5(CONCAT(origin_sys, product_ref))))AS policy_product_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, policy_ref)) AS policy_hash_key,
+    MD5(CONCAT(origin_sys, product_ref)) AS product_hash_key
+FROM crm_policy_register_csv
+UNION ALL
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, policy_id)),MD5(CONCAT(origin_sys, product_id))))AS policy_product_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, policy_id)) AS policy_hash_key,
+    MD5(CONCAT(origin_sys, product_id)) AS product_hash_key
+FROM sap_home_db
+UNION ALL
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, policy_id)),MD5(CONCAT(origin_sys, product_id))))AS policy_product_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, policy_id)) AS policy_hash_key,
+    MD5(CONCAT(origin_sys, product_id)) AS product_hash_key
+FROM sap_motor_db)
+select distinct * from combined;
```

</details>

### link_policy_quote

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_policy_quote_3` | `link_policy_quote_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_policy_quote` | `link_policy_quote` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_policy_register_csv,crm_quote_register_csv` | `crm_policy_register_csv, crm_enhanced_policy_relationships_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,24 +1,17 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
-    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
-    load_timestamp AS source_load_timestamp_col,
-    origin_sys,
-    'crm_policy_register_csv' AS source_tbl_name
-  FROM crm_policy_register_csv
-  UNION ALL
-  SELECT
-    md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
-    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
-    load_timestamp AS source_load_timestamp_col,
-    origin_sys,
-    'crm_quote_register_csv' AS source_tbl_name
-  FROM crm_quote_register_csv
-)
-SELECT
-  md5(CONCAT(policy_ref, quote_ref)) AS policy_quote_hash_key,
-  CAST(source_load_timestamp_col AS TIMESTAMP) AS load_date,
-  COALESCE(origin_sys, origin_sys) AS record_source,
-  policy_hash_key,
-  quote_hash_key
-FROM combined
+with combined as (
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, policy_ref)),MD5(CONCAT(origin_sys, quote_ref))))AS policy_quote_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, policy_ref)) AS policy_hash_key,
+    MD5(CONCAT(origin_sys, quote_ref)) AS quote_hash_key
+FROM crm_policy_register_csv
+UNION ALL
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, src_policy_id)),MD5(CONCAT(origin_sys, src_quote_id))))AS policy_quote_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, src_policy_id)) AS policy_hash_key,
+    MD5(CONCAT(origin_sys, src_quote_id)) AS quote_hash_key
+FROM crm_enhanced_policy_relationships_csv where source_extract = 'policy_quote_bridge.csv')
+select distinct * from combined;
```

</details>

### link_quote_broker

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_quote_broker_3` | `link_quote_broker_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_quote_broker` | `link_quote_broker` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_quote_register_csv,crm_broker_book_csv` | `crm_enhanced_policy_relationships_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,22 +1,8 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
-    md5(CONCAT(src_agent_id, src_system)) AS broker_hash_key,
-    current_timestamp() AS load_ts,
-    COALESCE(origin_sys, src_system) AS record_source
-  FROM crm_quote_register_csv
-  UNION ALL
-  SELECT
-    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
-    md5(CONCAT(src_agent_id, src_system)) AS broker_hash_key,
-    current_timestamp() AS load_ts,
-    COALESCE(origin_sys, src_system) AS record_source
-  FROM crm_broker_book_csv
-)
-SELECT
-  md5(CONCAT(quote_hash_key, broker_hash_key)) AS quote_broker_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  record_source,
-  quote_hash_key,
-  broker_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, src_quote_id)),MD5(CONCAT(origin_sys, src_agent_id))))AS quote_broker_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, src_quote_id)) AS quote_hash_key,
+    MD5(CONCAT(origin_sys, src_agent_id)) AS broker_hash_key
+FROM crm_enhanced_policy_relationships_csv
+WHERE source_extract = 'quote_broker_bridge.csv';
```

</details>

### link_quote_channel

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_quote_channel_3` | `link_quote_channel_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_quote_channel` | `link_quote_channel` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_quote_register_csv,crm_channel_catalog_csv` | `crm_enhanced_policy_relationships_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,17 +1,8 @@
-WITH combined AS (
-  SELECT
-    SHA2(CONCAT_WS('||', CAST(quote_ref AS STRING), CAST(origin_sys AS STRING)), 256) AS quote_hash_key,
-    SHA2(CONCAT_WS('||', CAST(src_channel_id AS STRING), CAST(src_system AS STRING)), 256) AS channel_hash_key,
-    load_timestamp AS source_load_timestamp_col,
-    COALESCE(origin_sys, src_system) AS record_source
-  FROM crm_quote_register_csv
-  JOIN crm_channel_catalog_csv
-    ON crm_quote_register_csv.src_channel_id = crm_channel_catalog_csv.src_channel_id
-)
-SELECT
-  SHA2(CONCAT_WS('||', CAST(quote_hash_key AS STRING), CAST(channel_hash_key AS STRING)), 256) AS quote_channel_hash_key,
-  CAST(source_load_timestamp_col AS TIMESTAMP) AS load_date,
-  record_source,
-  quote_hash_key,
-  channel_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, src_quote_id)),MD5(CONCAT(origin_sys, src_channel_id))))AS quote_channel_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, src_quote_id)) AS quote_hash_key,
+    MD5(CONCAT(origin_sys, src_channel_id)) AS channel_hash_key
+FROM crm_enhanced_policy_relationships_csv
+WHERE  source_extract = 'quote_channel_bridge.csv';
```

</details>

### link_quote_person

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_quote_person_3` | `link_quote_person_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_quote_person` | `link_quote_person` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_quote_register_csv,crm_party_master_csv` | `crm_quote_register_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,24 +1,7 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
-    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
-    current_timestamp() AS load_ts,
-    origin_sys,
-    'crm_quote_register_csv' AS source_tbl_name
-  FROM crm_quote_register_csv
-  UNION ALL
-  SELECT
-    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
-    md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
-    current_timestamp() AS load_ts,
-    origin_sys,
-    'crm_party_master_csv' AS source_tbl_name
-  FROM crm_party_master_csv
-)
-SELECT
-  md5(CONCAT(quote_hash_key, person_hash_key)) AS quote_person_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  COALESCE(origin_sys, origin_sys) AS record_source,
-  quote_hash_key,
-  person_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, party_ref)),MD5(CONCAT(origin_sys, quote_ref))))AS quote_person_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, party_ref)) AS person_hash_key,
+    MD5(CONCAT(origin_sys,quote_ref)) AS quote_hash_key
+FROM crm_quote_register_csv;
```

</details>

### link_quote_product

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `link_quote_product_3` | `link_quote_product_9` |
| `object_type` | `link` | `link` |
| `target_table` | `link_quote_product` | `link_quote_product` |
| `process_order` | `2` | `2` |
| `source_table` | `crm_quote_register_csv,crm_product_catelog_csv` | `crm_quote_register_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,24 +1,7 @@
-WITH combined AS (
-  SELECT
-    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
-    md5(CONCAT(product_ref, origin_sys)) AS product_hash_key,
-    current_timestamp() AS etl_load_timestamp,
-    origin_sys,
-    'crm_quote_register_csv' AS source_tbl_name
-  FROM crm_quote_register_csv
-  UNION ALL
-  SELECT
-    md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
-    md5(CONCAT(product_ref, origin_sys)) AS product_hash_key,
-    current_timestamp() AS etl_load_timestamp,
-    origin_sys,
-    'crm_product_catelog_csv' AS source_tbl_name
-  FROM crm_product_catelog_csv
-)
-SELECT
-  md5(CONCAT(quote_hash_key, product_hash_key)) AS quote_product_hash_key,
-  CAST(etl_load_timestamp AS timestamp) AS load_date,
-  COALESCE(origin_sys, origin_sys) AS record_source,
-  quote_hash_key,
-  product_hash_key
-FROM combined
+SELECT DISTINCT
+    md5(concat( MD5(CONCAT(origin_sys, product_ref)),MD5(CONCAT(origin_sys, quote_ref))))AS quote_product_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    origin_sys AS record_source,
+    MD5(CONCAT(origin_sys, product_ref)) AS product_hash_key,
+    MD5(CONCAT(origin_sys,quote_ref)) AS quote_hash_key
+FROM crm_quote_register_csv;
```

</details>

## Satellite

- Source Python: `ver/sat_config.py`
- Notebook: `ver/sat_config (2).ipynb`

| Table | Status | Source object | Notebook object |
|---|---|---|---|
| `sat_account_crm` | Different | `sat_account_crm_3` | `sat_account_crm_9` |
| `sat_address_crm` | Different | `sat_address_crm_3` | `sat_address_crm_9` |
| `sat_address_sap` | Different | `sat_address_sap_3` | `sat_address_sap_9` |
| `sat_broker_crm` | Different | `sat_broker_crm_3` | `sat_broker_crm_9` |
| `sat_campaign_crm` | Different | `sat_campaign_crm_3` | `sat_campaign_crm_9` |
| `sat_channel_crm` | Different | `sat_channel_crm_3` | `sat_channel_crm_9` |
| `sat_claim_crm` | Different | `sat_claim_crm_3` | `sat_claim_crm_9` |
| `sat_complaint_crm` | Different | `sat_complaint_crm_3` | `sat_complaint_crm_9` |
| `sat_consent_crm` | Different | `sat_consent_crm_3` | `sat_consent_crm_9` |
| `sat_contact_crm` | Different | `sat_contact_crm_3` | `sat_contact_crm_9` |
| `sat_customer_crm` | Different | `sat_customer_crm_3` | `sat_customer_crm_9` |
| `sat_home_crm` | Different | `sat_home_crm_3` | `sat_home_crm_9` |
| `sat_home_sap` | Different | `sat_home_sap_3` | `sat_home_sap_9` |
| `sat_identities_crm` | Different | `sat_identities_crm_3` | `sat_identities_crm_9` |
| `sat_insured_object` | Different | `sat_insured_object_3` | `sat_insured_object_crm_9` |
| `sat_lead_crm` | Different | `sat_lead_crm_3` | `sat_lead_crm_9` |
| `sat_legal_person_crm` | Different | `sat_legal_person_crm_3` | `sat_legal_person_crm_9` |
| `sat_legal_person_sap` | Different | `sat_legal_person_sap_3` | `sat_legal_person_db_9` |
| `sat_marketing_engagement_crm` | Different | `sat_marketing_engagement_crm_3` | `sat_marketing_engagement_crm_9` |
| `sat_marketing_preference_crm` | Different | `sat_marketing_preference_crm_3` | `sat_marketing_preference_crm_9` |
| `sat_motor_crm` | Different | `sat_motor_crm_3` | `sat_motor_crm_9` |
| `sat_motor_sap` | Different | `sat_motor_sap_3` | `sat_motor_sap_9` |
| `sat_natural_person_crm` | Different | `sat_natural_person_crm_3` | `sat_natural_person_crm_9` |
| `sat_natural_person_sap` | Different | `sat_natural_person_sap_3` | `sat_natural_person_db_9` |
| `sat_override_crm` | Different | `sat_override_crm_3` | `sat_override_crm_9` |
| `sat_person_CRM` | Source only | `sat_person_CRM_3` | `` |
| `sat_person_SAP` | Source only | `sat_person_SAP_3` | `` |
| `sat_person_crm` | Notebook only | `` | `sat_person_crm_9` |
| `sat_person_sap` | Notebook only | `` | `sat_person_sap_9` |
| `sat_policy_crm` | Different | `sat_policy_crm_3` | `sat_policy_crm_9` |
| `sat_product_crm` | Different | `sat_product_crm_3` | `sat_product_crm_9` |
| `sat_product_sap` | Different | `sat_product_sap_3` | `sat_product_sap_9` |
| `sat_quote_crm` | Different | `sat_quote_crm_3` | `sat_quote_crm_9` |
| `sat_regulation_crm` | Different | `sat_regulation_crm_3` | `sat_regulation_crm_9` |

### sat_account_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_account_crm_3` | `sat_account_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_account_crm` | `sat_account_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_account_book_csv` | `crm_account_book_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,12 +1,10 @@
 SELECT DISTINCT
-  md5(CONCAT(account_ref, origin_sys)) AS account_hash_key,
-  current_timestamp() AS load_date,
-  account_no AS account_number,
-  account_type_txt AS account_type,
-  last_access_dt AS account_last_access,
-  last_change_dt AS account_last_change,
-  account_create_type_txt AS account_creation_type,
-  account_status_txt AS account_status,
-  'sat_account_crm' AS source_tbl_name,
-  origin_sys AS record_source
-FROM crm_account_book_csv
+    md5(concat(origin_sys, account_ref)) AS account_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    account_no AS account_number,
+    account_type_txt AS account_type,
+    last_access_dt AS account_last_access,
+    last_change_dt AS account_last_change,
+    account_create_type_txt AS account_creation_type,
+    account_status_txt AS account_status
+FROM crm_account_book_csv;
```

</details>

### sat_address_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_address_crm_3` | `sat_address_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_address_crm` | `sat_address_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_address_book_csv` | `crm_address_book_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,13 +1,11 @@
 SELECT DISTINCT
-  md5(CONCAT(address_ref, origin_sys)) AS address_hash_key,
-  current_timestamp() AS load_date,
-  street_txt AS street,
-  postal_cd AS postcode,
-  city_nm AS city,
-  state_cd AS state,
-  country_cd AS country,
-  address_type_txt AS type,
-  region_txt AS region,
-  origin_sys AS record_source,
-  'sat_address_crm' AS source_tbl_name
-FROM crm_address_book_csv
+    md5(concat(origin_sys, address_ref)) AS address_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    street_txt AS street,
+    postal_cd AS postcode,
+    city_nm AS city,
+    state_cd AS state,
+    country_cd AS country,
+    address_type_txt AS type,
+    region_txt AS region
+FROM crm_address_book_csv;
```

</details>

### sat_address_sap

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_address_sap_3` | `sat_address_sap_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_address_sap` | `sat_address_sap` |
| `process_order` | `3` | `3` |
| `source_table` | `sap_address_db` | `sap_address_db` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,13 +1,11 @@
 SELECT DISTINCT
-  md5(CONCAT(address_id, origin_sys)) AS address_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  address_line_1 AS address_line_1,
-  address_line_2 AS address_line_2,
-  city AS city,
-  state AS state,
-  country AS country,
-  zipcode AS zipcode,
-  person_id AS person_id,
-  origin_sys AS record_source,
-  'sat_address_sap' AS source_tbl_name
-FROM sap_address_db
+    md5(concat(origin_sys, address_id)) AS address_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    person_id AS person_id,
+    address_line_1 AS address_line_1,
+    address_line_2 AS address_line_2,
+    city AS city,
+    state AS state,
+    country AS country,
+    zipcode AS zipcode
+FROM sap_address_db;
```

</details>

### sat_broker_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_broker_crm_3` | `sat_broker_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_broker_crm` | `sat_broker_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_broker_book_csv` | `crm_broker_book_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,12 +1,10 @@
-SELECT DISTINCT
-  md5(CONCAT(src_agent_id, src_system)) AS broker_hash_key,
-  current_timestamp() AS load_date,
-  src_agent_name AS agent_name,
-  src_agent_type AS agent_type,
-  src_agent_status AS agent_status,
-  src_agent_license_number AS agent_license_number,
-  src_agent_net_promoter_score AS agent_net_promoter_score,
-  src_agent_commission_percentage AS agent_commission_percentage,
-  src_system AS record_source,
-  'sat_broker_crm' AS source_tbl_name
-FROM crm_broker_book_csv
+SELECT DISTINCT 
+    md5(concat(origin_sys, src_agent_id)) AS broker_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    src_agent_name AS agent_name,
+    src_agent_type AS agent_type,
+    src_agent_status AS agent_status,
+    src_agent_license_number AS agent_license_number,
+    src_agent_net_promoter_score AS agent_net_promoter_score,
+    src_agent_commission_percentage AS agent_commission_percentage
+FROM crm_broker_book_csv;
```

</details>

### sat_campaign_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_campaign_crm_3` | `sat_campaign_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_campaign_crm` | `sat_campaign_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_campaign_register_csv` | `crm_campaign_register_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,41 +1,39 @@
-SELECT DISTINCT
-  md5(CONCAT(src_campaign_id, src_system)) AS campaign_hash_key,
-  current_timestamp() AS load_date,
-  src_campaign_name AS campaign_name,
-  src_campaign_type AS campaign_type,
-  src_campaign_start_date AS campaign_start_date,
-  src_campaign_end_date AS campaign_end_date,
-  src_campaign_status AS campaign_status,
-  src_campaign_budget AS campaign_budget,
-  src_campaign_target_audience AS campaign_target_audience,
-  src_campaign_marketing_source AS campaign_marketing_source,
-  src_campaign_owner_department AS campaign_owner_department,
-  src_campaign_country AS campaign_country,
-  src_campaign_conversion_goal AS campaign_conversion_goal,
-  src_number_of_impressions AS number_of_impressions,
-  src_number_of_clicks AS number_of_clicks,
-  src_is_active AS is_active,
-  src_number_of_visits AS number_of_visits,
-  src_number_of_policy_purchases AS number_of_policy_purchases,
-  src_number_of_emails_sent AS number_of_emails_sent,
-  src_number_of_email_bounced AS number_of_email_bounced,
-  src_number_of_emails_delivered AS number_of_emails_delivered,
-  src_number_of_emails_opened AS number_of_emails_opened,
-  src_click_through_rate AS click_through_rate,
-  src_spend_amount AS spend_amount,
-  src_incremental_revenue AS incremental_revenue,
-  src_survey_wave AS survey_wave,
-  src_total_number_of_respondents AS total_number_of_respondents,
-  src_number_of_respondents_aware AS number_of_respondents_aware,
-  src_number_of_promoters AS number_of_promoters,
-  src_number_of_passives AS number_of_passives,
-  src_number_of_detractors AS number_of_detractors,
-  src_number_of_followers AS number_of_followers,
-  src_number_of_likes AS number_of_likes,
-  src_number_of_comments AS number_of_comments,
-  src_number_of_shares AS number_of_shares,
-  src_number_of_brand_mentions AS number_of_brand_mentions,
-  src_number_of_category_mentions AS number_of_category_mentions,
-  src_system AS record_source,
-  'sat_campaign_crm' AS source_tbl_name
-FROM crm_campaign_register_csv
+SELECT DISTINCT 
+    md5(concat(origin_sys, src_campaign_id)) AS campaign_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    src_campaign_name AS campaign_name,
+    src_campaign_type AS campaign_type,
+    src_campaign_start_date AS campaign_start_date,
+    src_campaign_end_date AS campaign_end_date,
+    src_campaign_status AS campaign_status,
+    src_campaign_budget AS campaign_budget,
+    src_campaign_target_audience AS campaign_target_audience,
+    src_campaign_marketing_source AS campaign_marketing_source,
+    src_campaign_owner_department AS campaign_owner_department,
+    src_campaign_country AS campaign_country,
+    src_campaign_conversion_goal AS campaign_conversion_goal,
+    src_number_of_impressions AS number_of_impressions,
+    src_number_of_clicks AS number_of_clicks,
+    src_is_active AS is_active,
+    src_number_of_visits AS number_of_visits,
+    src_number_of_policy_purchases AS number_of_policy_purchases,
+    src_number_of_emails_sent AS number_of_emails_sent,
+    src_number_of_email_bounced AS number_of_email_bounced,
+    src_number_of_emails_delivered AS number_of_emails_delivered,
+    src_number_of_emails_opened AS number_of_emails_opened,
+    src_click_through_rate AS click_through_rate,
+    src_spend_amount AS spend_amount,
+    src_incremental_revenue AS incremental_revenue,
+    src_survey_wave AS survey_wave,
+    src_total_number_of_respondents AS total_number_of_respondents,
+    src_number_of_respondents_aware AS number_of_respondents_aware,
+    src_number_of_promoters AS number_of_promoters,
+    src_number_of_passives AS number_of_passives,
+    src_number_of_detractors AS number_of_detractors,
+    src_number_of_followers AS number_of_followers,
+    src_number_of_likes AS number_of_likes,
+    src_number_of_comments AS number_of_comments,
+    src_number_of_shares AS number_of_shares,
+    src_number_of_brand_mentions AS number_of_brand_mentions,
+    src_number_of_category_mentions AS number_of_category_mentions
+FROM crm_campaign_register_csv;
```

</details>

### sat_channel_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_channel_crm_3` | `sat_channel_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_channel_crm` | `sat_channel_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_channel_catalog_csv` | `crm_channel_catalog_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,8 +1,6 @@
-SELECT DISTINCT
-  md5(CONCAT(src_channel_id, src_system)) AS channel_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  src_channel_name AS channel_name,
-  src_channel_type AS channel_type,
-  src_system AS record_source,
-  'sat_channel_crm' AS source_tbl_name
-FROM crm_channel_catalog_csv
+SELECT DISTINCT 
+    md5(concat(origin_sys, src_channel_id)) AS channel_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    src_channel_name AS channel_name,
+    src_channel_type AS channel_type
+FROM crm_channel_catalog_csv;
```

</details>

### sat_claim_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_claim_crm_3` | `sat_claim_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_claim_crm` | `sat_claim_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_claim_register_csv` | `crm_claim_register_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,59 +1,57 @@
-SELECT DISTINCT
-  md5(CONCAT(src_claim_id, src_system)) AS claim_hash_key,
-  current_timestamp() AS load_date,
-  src_claim_number AS claim_number,
-  src_claim_type AS claim_type,
-  src_claim_status AS claim_status,
-  src_claim_reason AS claim_reason,
-  src_claim_channel AS claim_channel,
-  src_claim_handler AS claim_handler,
-  to_timestamp(src_claim_reported_date) AS claim_reported_date,
-  to_timestamp(src_claim_settlement_date) AS claim_settlement_date,
-  src_claim_product AS claim_product,
-  src_is_claim_suspicious AS is_claim_suspicious,
-  src_is_claim_fraud AS is_claim_fraud,
-  src_claim_fraud_status AS claim_fraud_status,
-  src_claim_fraud_type AS claim_fraud_type,
-  src_claim_fraud_detection_method AS claim_fraud_detection_method,
-  src_is_litigation AS is_litigation,
-  src_litigation_reason AS litigation_reason,
-  to_timestamp(src_litigation_start_date) AS litigation_start_date,
-  to_timestamp(src_litigation_end_date) AS litigation_end_date,
-  src_litigation_outcome AS litigation_outcome,
-  src_litigation_duration_days AS litigation_duration_days,
-  src_claim_fraud_detection_time_in_days AS claim_fraud_detection_time_in_days,
-  src_is_recovery_opportunity AS is_recovery_opportunity,
-  src_recovery_priority_score AS recovery_priority_score,
-  src_recovery_category AS recovery_category,
-  src_recovery_source AS recovery_source,
-  to_timestamp(src_first_recovery_date) AS first_recovery_date,
-  to_timestamp(src_last_recovery_date) AS last_recovery_date,
-  src_is_recovery_happened AS is_recovery_happened,
-  src_days_to_first_recovery AS days_to_first_recovery,
-  src_days_to_last_recovery AS days_to_last_recovery,
-  src_avg_days_to_close_claim AS avg_days_to_close_claim,
-  src_claim_fraud_outcome AS claim_fraud_outcome,
-  src_recovery_type AS recovery_type,
-  src_recovery_band AS recovery_band,
-  src_third_party_involved AS third_party_involved,
-  src_third_party_involved_overall_score AS third_party_involved_overall_score,
-  src_solicitor AS solicitor,
-  src_claim_amount AS claim_amount,
-  src_claims_paid AS claims_paid,
-  src_outstanding_reserve AS outstanding_reserve,
-  src_claims_expenses AS claims_expenses,
-  src_recovery_received AS recovery_received,
-  src_compensation_offered AS compensation_offered,
-  src_remediation_amount AS remediation_amount,
-  src_suspectd_amount AS suspected_amount,
-  src_fraud_amount AS fraud_amount,
-  src_legal_expenses AS legal_expenses,
-  src_claim_band AS claim_band,
-  src_claim_band_sort AS claim_band_sort,
-  src_is_fault_claim AS is_fault_claim,
-  src_claim_satisfaction_score AS claim_satisfaction_score,
-  src_claims_feedback AS claims_feedback,
-  src_is_claim_complaint_raised AS is_claim_complaint_raised,
-  src_system AS record_source,
-  'sat_claim_crm' AS source_tbl_name
-FROM crm_claim_register_csv
+SELECT DISTINCT 
+    md5(concat(origin_sys, src_claim_id)) AS claim_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    src_claim_number AS claim_number,
+    src_claim_type AS claim_type,
+    src_claim_status AS claim_status,
+    src_claim_reason AS claim_reason,
+    src_claim_channel AS claim_channel,
+    src_claim_handler AS claim_handler,
+    src_claim_reported_date AS claim_reported_date,
+    src_claim_settlement_date AS claim_settlement_date,
+    src_claim_product AS claim_product,
+    src_is_claim_suspicious AS is_claim_suspicious,
+    src_is_claim_fraud AS is_claim_fraud,
+    src_claim_fraud_status AS claim_fraud_status,
+    src_claim_fraud_type AS claim_fraud_type,
+    src_claim_fraud_detection_method AS claim_fraud_detection_method,
+    src_is_litigation AS is_litigation,
+    src_litigation_reason AS litigation_reason,
+    src_litigation_start_date AS litigation_start_date,
+    src_litigation_end_date AS litigation_end_date,
+    src_litigation_outcome AS litigation_outcome,
+    src_litigation_duration_days AS litigation_duration_days,
+    src_claim_fraud_detection_time_in_days AS claim_fraud_detection_time_in_days,
+    src_is_recovery_opportunity AS is_recovery_opportunity,
+    src_recovery_priority_score AS recovery_priority_score,
+    src_recovery_category AS recovery_category,
+    src_recovery_source AS recovery_source,
+    src_first_recovery_date AS first_recovery_date,
+    src_last_recovery_date AS last_recovery_date,
+    src_is_recovery_happened AS is_recovery_happened,
+    src_days_to_first_recovery AS days_to_first_recovery,
+    src_days_to_last_recovery AS days_to_last_recovery,
+    src_avg_days_to_close_claim AS avg_days_to_close_claim,
+    src_claim_fraud_outcome AS claim_fraud_outcome,
+    src_recovery_type AS recovery_type,
+    src_recovery_band AS recovery_band,
+    src_third_party_involved AS third_party_involved,
+    src_third_party_involved_overall_score AS third_party_involved_overall_score,
+    src_solicitor AS solicitor,
+    src_claim_amount AS claim_amount,
+    src_claims_paid AS claims_paid,
+    src_outstanding_reserve AS outstanding_reserve,
+    src_claims_expenses AS claims_expenses,
+    src_recovery_received AS recovery_received,
+    src_compensation_offered AS compensation_offered,
+    src_remediation_amount AS remediation_amount,
+    src_suspectd_amount AS suspected_amount,
+    src_fraud_amount AS fraud_amount,
+    src_legal_expenses AS legal_expenses,
+    src_claim_band AS claim_band,
+    src_claim_band_sort AS claim_band_sort,
+    src_is_fault_claim AS is_fault_claim,
+    src_claim_satisfaction_score AS claim_satisfaction_score,
+    src_claims_feedback AS claims_feedback,
+    src_is_claim_complaint_raised AS is_claim_complaint_raised
+FROM crm_claim_register_csv;
```

</details>

### sat_complaint_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_complaint_crm_3` | `sat_complaint_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_complaint_crm` | `sat_complaint_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_complaint_register_csv` | `crm_complaint_register_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,17 +1,16 @@
-SELECT DISTINCT
-  md5(CONCAT(src_complaint_id, src_system)) AS complaint_hash_key,
-  current_timestamp() AS load_date,
-  src_complaint_date AS complaint_date,
-  src_complaint_acknowledgement_date AS complaint_acknowledgement_date,
-  src_complaint_resolved_date AS complaint_resolved_date,
-  src_complaint_upheld_status AS complaint_upheld_status,
-  src_is_financial_ombudsman_service_referral AS is_financial_ombudsman_service_referral,
-  src_complaint_driver AS complaint_driver,
-  src_complaint_channel AS complaint_channel,
-  src_compensation_amount AS compensation_amount,
-  src_insurance_category AS insurance_category,
-  src_complaint_status AS complaint_status,
-  src_complaint_feedback AS complaint_feedback,
-  src_customer_complaint_satisfaction_score AS customer_complaint_satisfaction_score,
-  'sat_complaint_crm' AS source_tbl_name
-FROM crm_complaint_register_csv
+SELECT DISTINCT 
+    md5(concat(origin_sys, src_complaint_id)) AS complaint_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    src_complaint_date AS complaint_date,
+    src_complaint_acknowledgement_date AS complaint_acknowledgement_date,
+    src_complaint_resolved_date AS complaint_resolved_date,
+    src_complaint_upheld_status AS complaint_upheld_status,
+    src_is_financial_ombudsman_service_referral AS is_financial_ombudsman_service_referral,
+    src_complaint_driver AS complaint_driver,
+    src_complaint_channel AS complaint_channel,
+    src_compensation_amount AS compensation_amount,
+    src_insurance_category AS insurance_category,
+    src_complaint_status AS complaint_status,
+    src_customer_complaint_satisfaction_score AS customer_complaint_satisfaction_score,
+    src_complaint_feedback AS complaint_feedback
+FROM crm_complaint_register_csv;
```

</details>

### sat_consent_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_consent_crm_3` | `sat_consent_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_consent_crm` | `sat_consent_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_consent_snapshot_csv` | `crm_consent_snapshot_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,8 +1,6 @@
 SELECT DISTINCT
-  md5(CONCAT(consent_ref, origin_sys)) AS consent_hash_key,
-  current_timestamp() AS load_date,
-  opt_in_valid_ind AS opt_in_validated,
-  opt_in_legit_ind AS opt_in_legitimate_interest,
-  origin_sys AS record_source,
-  'sat_consent_crm' AS source_tbl_name
-FROM crm_consent_snapshot_csv
+    md5(concat(origin_sys, consent_ref)) AS consent_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    opt_in_valid_ind AS opt_in_validated,
+    opt_in_legit_ind AS opt_in_legitimate_interest
+FROM crm_consent_snapshot_csv;
```

</details>

### sat_contact_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_contact_crm_3` | `sat_contact_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_contact_crm` | `sat_contact_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_contact_point_csv` | `crm_contact_point_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,10 +1,8 @@
 SELECT DISTINCT
-  md5(CONCAT(contact_ref, origin_sys)) AS contact_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  email_home_txt AS personal_email,
-  email_work_txt AS work_email,
-  phone_work_txt AS work_phone,
-  phone_home_txt AS home_phone,
-  origin_sys AS record_source,
-  'sat_contact_crm' AS source_tbl_name
-FROM crm_contact_point_csv
+    md5(concat(origin_sys, contact_ref)) AS contact_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    email_home_txt AS personal_email,
+    email_work_txt AS work_email,
+    phone_work_txt AS work_phone,
+    phone_home_txt AS home_phone
+FROM crm_contact_point_csv;
```

</details>

### sat_customer_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_customer_crm_3` | `sat_customer_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_customer_crm` | `sat_customer_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_customer_portfolio_csv,crm_enhanced_enrichments_csv` | `crm_customer_portfolio_csv, crm_enhanced_enrichments_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,43 +1,23 @@
-SELECT DISTINCT
-  md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
-  CAST(load_ts AS TIMESTAMP) AS load_date,
-  customer_no AS customer_number,
-  customer_status_txt AS customer_status,
-  customer_status_reason_txt AS customer_status_reason,
-  customer_since_dt AS customer_since,
-  customer_rating_no AS customer_rating,
-  customer_segment_txt AS customer_segment,
-  lob_txt AS line_of_business,
-  nps_score_no AS nps_score,
-  NULL AS income_band,
-  NULL AS customer_satisfaction,
-  NULL AS customer_age_band,
-  NULL AS net_promotor_code_segment,
-  customer_onboarding_satisfaction_score AS customer_onboarding_satisfaction_score,
-  customer_onboarding_feedback AS customer_onboarding_feedback,
-  origin_sys AS record_source,
-  'crm_customer_portfolio_csv' AS source_tbl_name
-FROM crm_customer_portfolio_csv
-
-UNION ALL
-
-SELECT DISTINCT
-  md5(CONCAT(customer_ref, origin_sys)) AS customer_hash_key,
-  CAST(load_ts AS TIMESTAMP) AS load_date,
-  NULL AS customer_number,
-  NULL AS customer_status,
-  NULL AS customer_status_reason,
-  NULL AS customer_since,
-  NULL AS customer_rating,
-  NULL AS customer_segment,
-  NULL AS line_of_business,
-  NULL AS nps_score,
-  src_income_band AS income_band,
-  src_customer_satisfaction AS customer_satisfaction,
-  src_customer_age_band AS customer_age_band,
-  src_net_promotor_code_segment AS net_promotor_code_segment,
-  NULL AS customer_onboarding_satisfaction_score,
-  NULL AS customer_onboarding_feedback,
-  origin_sys AS record_source,
-  'crm_enhanced_enrichments_csv' AS source_tbl_name
-FROM crm_enhanced_enrichments_csv
+with combined as (
+    SELECT DISTINCT 
+    md5(concat(s.origin_sys, s.customer_ref)) AS customer_hash_key,
+    CAST(s.pull_ts AS timestamp) AS load_date,
+    s.customer_no AS customer_number,
+    s.customer_status_txt AS customer_status,
+    s.customer_status_reason_txt AS customer_status_reason,
+    s.customer_since_dt AS customer_since,
+    s.customer_rating_no AS customer_rating,
+    s.customer_segment_txt AS customer_segment,
+    s.lob_txt AS line_of_business,
+    s.nps_score_no AS nps_score,
+    p.src_income_band as income_band,
+    p.src_customer_satisfaction as customer_satisfaction,
+    p.src_customer_age_band as customer_age_band,
+    p.src_net_promotor_code_segment as net_promotor_code_segment,
+    s.customer_onboarding_satisfaction_score AS customer_onboarding_satisfaction_score,
+    s.customer_onboarding_feedback AS customer_onboarding_feedback 
+    FROM crm_customer_portfolio_csv s 
+    left join crm_enhanced_enrichments_csv p
+    on s.customer_ref = p.src_customer_ref
+)
+SELECT DISTINCT * FROM combined;
```

</details>

### sat_home_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_home_crm_3` | `sat_home_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_home_crm` | `sat_home_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_property_asset_csv` | `crm_property_asset_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,11 +1,10 @@
 SELECT DISTINCT
-  md5(CONCAT(property_ref, origin_sys)) AS home_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  wall_material_txt AS wall_construction,
-  risk_address_txt AS home_risk_address,
-  roof_material_txt AS roof_construction,
-  property_type_txt AS home_type,
-  property_state_cd AS home_state,
-  existing_home_ind AS is_existing_home_customer,
-  'sat_home_crm' AS source_tbl_name
-FROM crm_property_asset_csv
+    md5(concat(origin_sys, property_ref)) AS home_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    wall_material_txt AS wall_construction,
+    risk_address_txt AS home_risk_address,
+    roof_material_txt AS roof_construction,
+    property_type_txt AS home_type,
+    property_state_cd AS home_state,
+    existing_home_ind AS is_existing_home_customer
+FROM crm_property_asset_csv;
```

</details>

### sat_home_sap

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_home_sap_3` | `sat_home_sap_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_home_sap` | `sat_home_sap` |
| `process_order` | `3` | `3` |
| `source_table` | `sap_home_db,sap_motor_db` | `sap_home_db` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,25 +1,10 @@
 SELECT DISTINCT
-  md5(CONCAT(home_id, origin_sys)) AS home_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  home_type,
-  home_location,
-  roof_material,
-  wall_type,
-  NULL AS policy_id,
-  NULL AS product_id,
-  origin_sys AS record_source,
-  'sat_home_sap' AS source_tbl_name
-FROM sap_home_db
-UNION ALL
-SELECT DISTINCT
-  md5(CONCAT(policy_id, origin_sys)) AS home_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  NULL AS home_type,
-  NULL AS home_location,
-  NULL AS roof_material,
-  NULL AS wall_type,
-  policy_id,
-  product_id,
-  origin_sys AS record_source,
-  'sat_home_sap' AS source_tbl_name
-FROM sap_motor_db
+    md5(concat(origin_sys, home_id)) AS home_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    policy_id AS policy_id,
+    product_id AS product_id,
+    home_type AS home_type,
+    home_location AS home_location,
+    wall_type AS wall_type,
+    roof_material AS roof_material
+FROM sap_home_db;
```

</details>

### sat_identities_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_identities_crm_3` | `sat_identities_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_identities_crm` | `sat_identities_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_identity_registry_csv` | `crm_identity_registry_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,8 +1,6 @@
 SELECT DISTINCT
-  md5(CONCAT(identity_ref, origin_sys)) AS identities_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  ecid_txt AS ecid,
-  hashed_email_txt AS hashed_email,
-  origin_sys AS record_source,
-  'sat_identities_crm' AS source_tbl_name
-FROM crm_identity_registry_csv
+    md5(concat(origin_sys, identity_ref)) AS identities_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    ecid_txt AS ecid,
+    hashed_email_txt AS hashed_email
+FROM crm_identity_registry_csv;
```

</details>

### sat_insured_object

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_insured_object_3` | `sat_insured_object_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_insured_object` | `sat_insured_object` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_property_asset_csv,crm_vehicle_asset_csv` | `crm_property_asset_csv, crm_vehicle_asset_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,31 +1,27 @@
+with combined as (
 SELECT DISTINCT
-  md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
-  current_timestamp() AS load_date,
-  insured_object_type,
-  insured_object_sub_type,
-  insured_object_description,
-  insured_value,
-  currency_code,
-  insured_object_start_date,
-  insured_object_end_date,
-  insured_object_current_status,
-  origin_sys AS record_source,
-  'sat_insured_object' AS source_tbl_name
+    md5(concat(origin_sys, insured_object_id)) AS insured_object_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    insured_object_type AS insured_object_type,
+    insured_object_sub_type AS insured_object_sub_type,
+    insured_object_description AS insured_object_description,
+    insured_value AS insured_value,
+    currency_code AS currency_code,
+    insured_object_start_date AS insured_object_start_date,
+    insured_object_end_date AS insured_object_end_date,
+    insured_object_current_status AS insured_object_current_status
 FROM crm_property_asset_csv
-
 UNION ALL
-
 SELECT DISTINCT
-  md5(CONCAT(insured_object_id, origin_sys)) AS insured_object_hash_key,
-  current_timestamp() AS load_date,
-  insured_object_type,
-  insured_object_sub_type,
-  insured_object_description,
-  insured_value,
-  currency_code,
-  insured_object_start_date,
-  insured_object_end_date,
-  insured_object_current_status,
-  origin_sys AS record_source,
-  'sat_insured_object' AS source_tbl_name
-FROM crm_vehicle_asset_csv
+    md5(concat(origin_sys, insured_object_id)) AS insured_object_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    insured_object_type AS insured_object_type,
+    insured_object_sub_type AS insured_object_sub_type,
+    insured_object_description AS insured_object_description,
+    insured_value AS insured_value,
+    currency_code AS currency_code,
+    insured_object_start_date AS insured_object_start_date,
+    insured_object_end_date AS insured_object_end_date,
+    insured_object_current_status AS insured_object_current_status
+FROM crm_vehicle_asset_csv)
+select * from combined;
```

</details>

### sat_lead_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_lead_crm_3` | `sat_lead_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_lead_crm` | `sat_lead_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_lead_register_csv` | `crm_lead_register_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,11 +1,9 @@
 SELECT DISTINCT
-  md5(CONCAT(lead_ref, origin_sys)) AS lead_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  interest_bucket AS interested_level,
-  contact_pref AS preferred_contact_method,
-  person_score_no AS person_score,
-  person_status_txt AS person_status,
-  converted_dt AS converted_date,
-  origin_sys AS record_source,
-  'sat_lead_crm' AS source_tbl_name
-FROM crm_lead_register_csv
+    md5(concat(origin_sys, lead_ref)) AS lead_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    interest_bucket AS interested_level,
+    contact_pref AS preferred_contact_method,
+    person_score_no AS person_score,
+    person_status_txt AS person_status,
+    converted_dt AS converted_date
+FROM crm_lead_register_csv;
```

</details>

### sat_legal_person_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_legal_person_crm_3` | `sat_legal_person_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_legal_person_crm` | `sat_legal_person_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_party_master_csv` | `crm_party_master_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,14 +1,12 @@
 SELECT DISTINCT
-  md5(CONCAT(legal_ref, origin_sys)) AS legal_person_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  legal_job_title_txt AS job_title,
-  lead_conv_dt AS converted_date,
-  legal_status_txt AS person_status,
-  legal_score_no AS person_score,
-  legal_name AS company_name,
-  legal_src_type AS source_type,
-  legal_src_ref AS source_id,
-  constitution_dt AS date_of_constitution,
-  origin_sys AS record_source,
-  'sat_legal_person_crm' AS source_tbl_name
-FROM crm_party_master_csv
+    md5(concat(origin_sys, legal_ref)) AS legal_person_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    legal_job_title_txt AS job_title,
+    CAST(lead_conv_dt AS timestamp) AS converted_date,
+    legal_status_txt AS person_status,
+    legal_score_no AS person_score,
+    legal_name AS company_name,
+    legal_src_type AS source_type,
+    legal_src_ref AS source_id,
+    to_date(constitution_dt,'y-M-d') AS date_of_constitution
+From crm_party_master_csv where party_kind = 'LEGAL';
```

</details>

### sat_legal_person_sap

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_legal_person_sap_3` | `sat_legal_person_db_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_legal_person_sap` | `sat_legal_person_sap` |
| `process_order` | `3` | `3` |
| `source_table` | `sap_person_db` | `sap_person_db` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,8 +1,6 @@
 SELECT DISTINCT
-  md5(CONCAT(person_id, origin_sys)) AS legal_person_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  organization AS organization,
-  org_establishment_date AS org_establishment_date,
-  origin_sys AS record_source,
-  'sat_legal_person_sap' AS source_tbl_name
-FROM sap_person_db
+    md5(concat(origin_sys, person_id)) AS legal_person_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    organization as organization,
+    to_date(org_establishment_date, 'y-M-d') as org_establishment_date
+From sap_person_db where person_type = 'LEGAL';
```

</details>

### sat_marketing_engagement_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_marketing_engagement_crm_3` | `sat_marketing_engagement_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_marketing_engagement_crm` | `sat_marketing_engagement_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_enhanced_enrichments_csv,crm_campaign_touch_csv` | `crm_campaign_touch_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,31 +1,12 @@
 SELECT DISTINCT
-  md5(CONCAT(src_marketing_engagement_ref, origin_sys)) AS marketing_engagement_hash_key,
-  CAST(load_ts AS TIMESTAMP) AS load_date,
-  src_promotion_code AS promotion_code,
-  src_opened_email AS opened_email,
-  src_marketing_status AS marketing_status,
-  NULL AS has_retention_team_interaction,
-  NULL AS customer_service_call_frequency,
-  NULL AS average_call_sentiment,
-  NULL AS engagement_score,
-  NULL AS first_contact_resolution,
-  origin_sys AS record_source,
-  'sat_marketing_engagement_crm' AS source_tbl_name
-FROM crm_enhanced_enrichments_csv
-
-UNION ALL
-
-SELECT DISTINCT
-  md5(CONCAT(src_marketing_engagement_ref, origin_sys)) AS marketing_engagement_hash_key,
-  CAST(load_ts AS TIMESTAMP) AS load_date,
-  NULL AS promotion_code,
-  NULL AS opened_email,
-  NULL AS marketing_status,
-  has_retention_team_interaction AS has_retention_team_interaction,
-  customer_service_call_frequency AS customer_service_call_frequency,
-  average_call_sentiment AS average_call_sentiment,
-  engagement_score AS engagement_score,
-  first_contact_resolution AS first_contact_resolution,
-  origin_sys AS record_source,
-  'sat_marketing_engagement_crm' AS source_tbl_name
-FROM crm_campaign_touch_csv
+    md5(concat(origin_sys, engagement_ref)) AS marketing_engagement_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    promo_cd AS promotion_code,
+    email_opened_ind AS opened_email,
+    campaign_status_txt AS marketing_status,
+    has_retention_team_interaction AS has_retention_team_interaction,
+    customer_service_call_frequency AS customer_service_call_frequency,
+    average_call_sentiment AS average_call_sentiment,
+    engagement_score AS engagement_score,
+    first_contact_resolution AS first_contact_resolution
+FROM crm_campaign_touch_csv;
```

</details>

### sat_marketing_preference_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_marketing_preference_crm_3` | `sat_marketing_preference_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_marketing_preference_crm` | `sat_marketing_preference_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_comm_preference_csv` | `crm_comm_preference_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,13 +1,11 @@
 SELECT DISTINCT
-  md5(CONCAT(preference_ref, origin_sys)) AS marketing_preference_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  sms_ind AS sms,
-  email_ind AS email,
-  email_sub_ind AS email_subscriptions,
-  call_ind AS call,
-  any_ind AS any,
-  commercial_email_ind AS commercial_email,
-  postal_mail_ind AS postal_mail,
-  origin_sys AS record_source,
-  'sat_marketing_preference_crm' AS source_tbl_name
-FROM crm_comm_preference_csv
+    md5(concat(origin_sys, preference_ref)) AS marketing_preference_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    sms_ind AS sms,
+    email_ind AS email,
+    email_sub_ind AS email_subscriptions,
+    call_ind AS call,
+    any_ind AS any,
+    commercial_email_ind AS commercial_email,
+    postal_mail_ind AS postal_mail
+FROM crm_comm_preference_csv;
```

</details>

### sat_motor_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_motor_crm_3` | `sat_motor_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_motor_crm` | `sat_motor_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_vehicle_asset_csv,crm_enhanced_enrichments_csv` | `crm_vehicle_asset_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,49 +1,22 @@
 SELECT DISTINCT
-  md5(CONCAT(CAST(vehicle_ref AS STRING), CAST(origin_sys AS STRING))) AS motor_hash_key,
-  CAST(load_ts AS TIMESTAMP) AS load_date,
-  auto_decline_ind AS auto_decline_vehicle,
-  body_style_txt AS body_type,
-  fuel_type_txt AS fuel_type,
-  license_status_txt AS license_status,
-  existing_motor_ind AS is_existing_motor_customer,
-  motor_lapse_cnt AS motor_lapsed_policies,
-  garage_address_txt AS motor_risk_address,
-  risk_class_cd AS risk_class_code,
-  variant_nm AS variant,
-  owner_type_txt AS vehicle_owner_type,
-  registration_state_cd AS vehicle_regstate,
-  vehicle_class_txt AS vehicle_class,
-  model_nm AS vehicle_model,
-  vehicle_type_txt AS vehicle_type,
-  insured_value_amt AS motor_sum_insrd,
-  vehicle_age_yrs AS vehicle_year,
-  CAST(NULL AS INT) AS vehicle_age,
-  driver_experience_years AS driver_experience_years,
-  origin_sys AS record_source,
-  'sat_motor_crm' AS source_tbl_name
-FROM crm_vehicle_asset_csv
-UNION ALL
-SELECT DISTINCT
-  md5(CONCAT(CAST(vehicle_ref AS STRING), CAST(origin_sys AS STRING))) AS motor_hash_key,
-  CAST(load_ts AS TIMESTAMP) AS load_date,
-  CAST(NULL AS STRING) AS auto_decline_vehicle,
-  CAST(NULL AS STRING) AS body_type,
-  CAST(NULL AS STRING) AS fuel_type,
-  CAST(NULL AS STRING) AS license_status,
-  CAST(NULL AS STRING) AS is_existing_motor_customer,
-  CAST(NULL AS BIGINT) AS motor_lapsed_policies,
-  CAST(NULL AS STRING) AS motor_risk_address,
-  CAST(NULL AS STRING) AS risk_class_code,
-  CAST(NULL AS STRING) AS variant,
-  CAST(NULL AS STRING) AS vehicle_owner_type,
-  CAST(NULL AS STRING) AS vehicle_regstate,
-  CAST(NULL AS STRING) AS vehicle_class,
-  CAST(NULL AS STRING) AS vehicle_model,
-  CAST(NULL AS STRING) AS vehicle_type,
-  CAST(NULL AS DECIMAL(18,2)) AS motor_sum_insrd,
-  CAST(NULL AS BIGINT) AS vehicle_year,
-  src_vehicle_age AS vehicle_age,
-  CAST(NULL AS BIGINT) AS driver_experience_years,
-  origin_sys AS record_source,
-  'sat_motor_crm' AS source_tbl_name
-FROM crm_enhanced_enrichments_csv
+    md5(concat(origin_sys, vehicle_ref)) AS motor_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    auto_decline_ind AS auto_decline_vehicle,
+    body_style_txt AS body_type,
+    fuel_type_txt AS fuel_type,
+    license_status_txt AS license_status,
+    existing_motor_ind AS is_existing_motor_customer,
+    motor_lapse_cnt AS motor_lapsed_policies,
+    garage_address_txt AS motor_risk_address,
+    risk_class_cd AS risk_class_code,
+    variant_nm AS variant,
+    owner_type_txt AS vehicle_owner_type,
+    registration_state_cd AS vehicle_regstate,
+    vehicle_class_txt AS vehicle_class,
+    model_nm AS vehicle_model,
+    vehicle_type_txt AS vehicle_type,
+    insured_value_amt AS motor_sum_insrd,
+    manufacture_yr AS vehicle_year,
+    vehicle_age_yrs AS vehicle_age,
+    driver_experience_years AS driver_experience_years
+FROM crm_vehicle_asset_csv;
```

</details>

### sat_motor_sap

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_motor_sap_3` | `sat_motor_sap_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_motor_sap` | `sat_motor_sap` |
| `process_order` | `3` | `3` |
| `source_table` | `sap_motor_db` | `sap_motor_db` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,16 +1,14 @@
 SELECT DISTINCT
-  md5(CONCAT(motor_id, origin_sys)) AS motor_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  motor_class,
-  motor_model,
-  motor_type,
-  fuel_type,
-  motor_parked_location,
-  manufacturing_date,
-  gear_type,
-  body_colour,
-  policy_id,
-  product_id,
-  origin_sys AS record_source,
-  'sat_motor_sap' AS source_tbl_name
-FROM sap_motor_db
+    md5(concat(origin_sys, motor_id)) AS motor_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    policy_id AS policy_id,
+    product_id AS product_id,
+    motor_class AS motor_class,
+    motor_model AS motor_model,
+    motor_type AS motor_type,
+    manufacturing_date AS manufacturing_date,
+    body_colour AS body_colour,
+    fuel_type AS fuel_type,
+    gear_type AS gear_type,
+    motor_parked_location AS motor_parked_location
+FROM sap_motor_db;
```

</details>

### sat_natural_person_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_natural_person_crm_3` | `sat_natural_person_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_natural_person_crm` | `sat_natural_person_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_party_master_csv` | `crm_party_master_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,20 +1,19 @@
 SELECT DISTINCT
-  md5(CONCAT(natural_ref, origin_sys)) AS natural_person_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  given_nm AS first_name,
-  family_nm AS last_name,
-  display_nm AS full_name,
-  title_txt AS courtesy_title,
-  role_txt AS role,
-  occupation_txt AS occupation,
-  dob AS birth_date,
-  birth_yr AS birth_year,
-  nationality_txt AS nationality,
-  gender_txt AS gender,
-  marital_txt AS marital_status,
-  disability_degree AS assesed_disability_degree,
-  language_pref AS preferred_language,
-  job_title_txt AS job_title,
-  origin_sys AS record_source,
-  'sat_natural_person_crm' AS source_tbl_name
+    md5(concat(origin_sys, natural_ref)) AS natural_person_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    given_nm AS first_name,
+    family_nm AS last_name,
+    display_nm AS full_name,
+    title_txt AS courtesy_title,
+    role_txt AS role,
+    occupation_txt AS occupation,
+    to_date(dob, 'y-M-d') AS birth_date,
+    cast(birth_yr AS int) AS birth_year,
+    nationality_txt AS nationality,
+    gender_txt AS gender,
+    marital_txt AS marital_status,
+    disability_degree AS assesed_disability_degree,
+    language_pref AS preferred_language,
+    job_title_txt AS job_title
 FROM crm_party_master_csv
+where party_kind = 'NATURAL';
```

</details>

### sat_natural_person_sap

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_natural_person_sap_3` | `sat_natural_person_db_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_natural_person_sap` | `sat_natural_person_sap` |
| `process_order` | `3` | `3` |
| `source_table` | `sap_person_db` | `sap_person_db` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,12 +1,11 @@
 SELECT DISTINCT
-  md5(CONCAT(person_id, origin_sys)) AS natural_person_hash_key,
-  current_timestamp() AS load_date,
-  first_name AS first_name,
-  middle_name AS middle_name,
-  last_name AS last_name,
-  date_of_birth AS date_of_birth,
-  gender AS gender,
-  occupation AS occupation,
-  origin_sys AS record_source,
-  'sat_natural_person_sap' AS source_tbl_name
+    md5(concat(origin_sys, person_id)) AS natural_person_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    first_name AS first_name,
+    middle_name AS middle_name,
+    last_name AS last_name,
+    to_date(date_of_birth, 'y-M-d') AS date_of_birth,
+    gender AS gender,
+    occupation AS occupation
 FROM sap_person_db
+where person_type = 'NATURAL';
```

</details>

### sat_override_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_override_crm_3` | `sat_override_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_override_crm` | `sat_override_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_override_register_csv` | `crm_override_register_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,7 +1,5 @@
 SELECT DISTINCT
-  md5(CONCAT(src_override_id, src_system)) AS override_hash_key,
-  current_timestamp() AS load_date,
-  src_override_reason AS override_reason,
-  src_system AS record_source,
-  'sat_override_crm' AS source_tbl_name
-FROM crm_override_register_csv
+    md5(concat(origin_sys, src_override_id)) AS override_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    src_override_reason AS override_reason
+FROM crm_override_register_csv;
```

</details>

### sat_person_CRM

**Status:** Source only

Present in source Python only.

```sql
SELECT DISTINCT
  md5(CONCAT(party_ref, origin_sys)) AS person_hash_key,
  current_timestamp() AS load_date,
  tenant_cd AS tenant_id,
  lead_ind AS is_lead,
  party_kind AS type,
  paperless_ind AS operational_paperless_consent,
  src_party_ref AS source_id,
  src_party_type AS source_type,
  origin_sys AS record_source,
  'sat_person_CRM' AS source_tbl_name
FROM crm_party_master_csv
```

### sat_person_SAP

**Status:** Source only

Present in source Python only.

```sql
SELECT DISTINCT
  md5(CONCAT(person_id, origin_sys)) AS person_hash_key,
  CAST(load_ts AS TIMESTAMP) AS load_date,
  person_type,
  email_address,
  phone_number,
  origin_sys AS record_source,
  'sat_person_SAP' AS source_tbl_name
FROM sap_person_db
```

### sat_person_crm

**Status:** Notebook only

Present in notebook only.

```sql
SELECT DISTINCT
    md5(concat(origin_sys, party_ref)) AS person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    party_kind AS type,
    tenant_cd AS tenant_id,
    lead_ind AS is_lead,
    paperless_ind AS operational_paperless_consent,
    src_party_ref AS source_id,
    src_party_type AS source_type
FROM crm_party_master_csv;
```

### sat_person_sap

**Status:** Notebook only

Present in notebook only.

```sql
SELECT DISTINCT
    md5(concat(origin_sys, person_id)) AS person_hash_key,
    CAST(pull_ts AS timestamp) AS load_date,
    person_type AS person_type,
    email_address AS email_address,
    phone_number AS phone_number
FROM sap_person_db;
```

### sat_policy_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_policy_crm_3` | `sat_policy_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_policy_crm` | `sat_policy_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_policy_register_csv,crm_enhanced_enrichments_csv` | `crm_policy_register_csv, crm_enhanced_enrichments_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,129 +1,68 @@
-SELECT DISTINCT
-  md5(CONCAT(policy_ref, origin_sys)) AS policy_hash_key,
-  current_timestamp() AS load_date,
-  cover_option_txt AS cover_option,
-  declined_claim_cnt AS declined_claims,
-  fraud_ind AS fraud_flag,
-  gross_amt AS gross_revenue,
-  net_amt AS net_revenue,
-  active_claim_cnt AS number_of_active_claim,
-  previous_claim_cnt AS number_of_previous_claim,
-  policy_cycle_no AS policy_cycle,
-  policy_end_dt AS policy_end_date,
-  policy_term_months AS policy_length,
-  policy_no AS policy_number,
-  policy_start_dt AS policy_start_date,
-  policy_status_txt AS policy_status,
-  renewal_premium_curr AS renewal_amount_current_period,
-  renewal_premium_next AS renewal_amount_next_period,
-  renewal_dt AS renewal_date,
-  sales_channel_txt AS sales_channel,
-  NULL AS quote_id,
-  NULL AS policy_type,
-  NULL AS policy_issue_date,
-  NULL AS is_policy_renewal,
-  NULL AS policy_cancellation_reason,
-  NULL AS policy_sum_insured,
-  NULL AS policy_retention_limit,
-  NULL AS policy_risk_score,
-  NULL AS policy_risk_band,
-  NULL AS policy_base_premium,
-  NULL AS gross_written_premium,
-  NULL AS earned_premium,
-  NULL AS incurred_but_not_reported,
-  NULL AS operating_expenses,
-  NULL AS administrative_expenses,
-  NULL AS profit_margin,
-  NULL AS taxes_and_levies,
-  NULL AS amount_approved,
-  NULL AS ceded_premium,
-  NULL AS commission_paid,
-  NULL AS ceded_commission,
-  NULL AS exposure_amount,
-  NULL AS investment_income,
-  NULL AS underwriting_cycle_time_in_days,
-  NULL AS underwriting_expenses,
-  NULL AS transaction_date,
-  NULL AS record_type,
-  NULL AS discount,
-  NULL AS override_commission,
-  NULL AS partial_recovery_percentage,
-  is_auto_renew_enabled AS is_auto_renew_enabled,
-  no_claims_discount_years AS no_claims_discount_years,
-  payment_method AS payment_method,
-  is_direct_debit_cancellation AS is_direct_debit_cancellation,
-  missed_payment_count AS missed_payment_count,
-  loyalty_discount_usage AS loyalty_discount_usage,
-  is_installment_default AS is_installment_default,
-  policy_renewal_satisfaction_score AS policy_renewal_satisfaction_score,
-  crm_policy_register_csv AS policy_renewal_feedback,
-  crm_policy_register_csv AS is_renewal_escalation,
-  origin_sys AS record_source,
-  'sat_policy_crm' AS source_tbl_name
-FROM crm_policy_register_csv
+WITH combined AS (
+    SELECT DISTINCT
+        md5(concat(s.origin_sys, s.policy_ref)) AS policy_hash_key,
+        CAST(s.pull_ts AS timestamp) AS load_date,
+        s.cover_option_txt AS cover_option,
+        s.declined_claim_cnt AS declined_claims,
+        s.fraud_ind AS fraud_flag,
+        s.gross_amt AS gross_revenue,
+        s.net_amt AS net_revenue,
+        s.active_claim_cnt AS number_of_active_claim,
+        s.previous_claim_cnt AS number_of_previous_claim,
+        s.policy_cycle_no AS policy_cycle,
+        s.policy_end_dt AS policy_end_date,
+        s.policy_term_months AS policy_length,
+        s.policy_no AS policy_number,
+        s.policy_start_dt AS policy_start_date,
+        s.policy_status_txt AS policy_status,
+        s.renewal_premium_curr AS renewal_amount_current_period,
+        s.renewal_premium_next AS renewal_amount_next_period,
+        s.renewal_dt AS renewal_date,
+        s.sales_channel_txt AS sales_channel,
+        p.src_quote_id AS quote_id,
+        p.src_policy_type AS policy_type,
+        p.src_policy_issue_date AS policy_issue_date,
+        p.src_is_policy_renewal AS is_policy_renewal,
+        p.src_policy_cancellation_reason AS policy_cancellation_reason,
+        p.src_policy_sum_insured AS policy_sum_insured,
+        p.src_policy_retention_limit AS policy_retention_limit,
+        p.src_policy_risk_score AS policy_risk_score,
+        p.src_policy_risk_band AS policy_risk_band,
+        p.src_policy_base_premium AS policy_base_premium,
+        p.src_gross_written_premium AS gross_written_premium,
+        p.src_operating_expenses AS earned_premium,
+        p.src_incurred_but_not_reported AS incurred_but_not_reported,
+        p.src_operating_expenses AS operating_expenses,
+        p.src_administrative_expenses AS administrative_expenses,
+        p.src_profit_margin AS profit_margin,
+        p.src_taxes_and_levies AS taxes_and_levies,
+        p.src_amount_approved AS amount_approved,
+        p.src_ceded_premium AS ceded_premium,
+        p.src_commission_paid AS commission_paid,
+        p.src_ceded_commission AS ceded_commission,
+        p.src_exposure_amount AS exposure_amount,
+        p.src_investment_income AS investment_income,
+        p.src_underwriting_cycle_time_in_days AS underwriting_cycle_time_in_days,
+        p.src_underwriting_expenses AS underwriting_expenses,
+        p.src_transaction_date AS transaction_date,
+        p.src_record_type AS record_type,
+        p.src_discount AS discount,
+        p.src_override_commission AS override_commission,
+        p.src_partial_recovery_percentage AS partial_recovery_percentage,
+        s.is_auto_renew_enabled AS is_auto_renew_enabled,
+        s.no_claims_discount_years AS no_claims_discount_years,
+        s.payment_method AS payment_method,
+        s.is_direct_debit_cancellation AS is_direct_debit_cancellation,
+        s.missed_payment_count AS missed_payment_count,
+        s.loyalty_discount_usage AS loyalty_discount_usage,
+        s.is_installment_default AS is_installment_default,
+        s.policy_renewal_satisfaction_score AS policy_renewal_satisfaction_score,
+        s.policy_renewal_feedback AS policy_renewal_feedback,
+        s.is_renewal_escalation AS is_renewal_escalation
 
-UNION ALL
-
-SELECT DISTINCT
-  md5(CONCAT(src_quote_id, origin_sys)) AS policy_hash_key,
-  current_timestamp() AS load_date,
-  NULL AS cover_option,
-  NULL AS declined_claims,
-  NULL AS fraud_flag,
-  NULL AS gross_revenue,
-  NULL AS net_revenue,
-  NULL AS number_of_active_claim,
-  NULL AS number_of_previous_claim,
-  NULL AS policy_cycle,
-  NULL AS policy_end_date,
-  NULL AS policy_length,
-  NULL AS policy_number,
-  NULL AS policy_start_date,
-  NULL AS policy_status,
-  NULL AS renewal_amount_current_period,
-  NULL AS renewal_amount_next_period,
-  NULL AS renewal_date,
-  NULL AS sales_channel,
-  src_quote_id AS quote_id,
-  src_policy_type AS policy_type,
-  src_policy_issue_date AS policy_issue_date,
-  src_is_policy_renewal AS is_policy_renewal,
-  src_policy_cancellation_reason AS policy_cancellation_reason,
-  src_policy_sum_insured AS policy_sum_insured,
-  src_policy_retention_limit AS policy_retention_limit,
-  src_policy_risk_score AS policy_risk_score,
-  src_policy_risk_band AS policy_risk_band,
-  src_policy_base_premium AS policy_base_premium,
-  src_gross_written_premium AS gross_written_premium,
-  src_operating_expenses AS earned_premium,
-  src_incurred_but_not_reported AS incurred_but_not_reported,
-  src_operating_expenses AS operating_expenses,
-  src_administrative_expenses AS administrative_expenses,
-  src_profit_margin AS profit_margin,
-  src_taxes_and_levies AS taxes_and_levies,
-  src_amount_approved AS amount_approved,
-  src_ceded_premium AS ceded_premium,
-  src_commission_paid AS commission_paid,
-  src_ceded_commission AS ceded_commission,
-  src_exposure_amount AS exposure_amount,
-  src_investment_income AS investment_income,
-  src_underwriting_cycle_time_in_days AS underwriting_cycle_time_in_days,
-  src_underwriting_expenses AS underwriting_expenses,
-  src_transaction_date AS transaction_date,
-  src_record_type AS record_type,
-  src_discount AS discount,
-  src_override_commission AS override_commission,
-  src_partial_recovery_percentage AS partial_recovery_percentage,
-  NULL AS is_auto_renew_enabled,
-  NULL AS no_claims_discount_years,
-  NULL AS payment_method,
-  NULL AS is_direct_debit_cancellation,
-  NULL AS missed_payment_count,
-  NULL AS loyalty_discount_usage,
-  NULL AS is_installment_default,
-  NULL AS policy_renewal_satisfaction_score,
-  NULL AS policy_renewal_feedback,
-  NULL AS is_renewal_escalation,
-  origin_sys AS record_source,
-  'sat_policy_crm' AS source_tbl_name
-FROM crm_enhanced_enrichments_csv
+    FROM crm_policy_register_csv s
+    LEFT JOIN crm_enhanced_enrichments_csv p
+        ON s.customer_ref = p.src_customer_ref
+)
+SELECT DISTINCT *
+FROM combined;
```

</details>

### sat_product_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_product_crm_3` | `sat_product_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_product_crm` | `sat_product_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_product_catelog_csv,crm_product_catalog_csv` | `crm_product_catalog_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,27 +1,12 @@
 SELECT DISTINCT
-  md5(CONCAT(product_ref, origin_sys)) AS product_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  NULL AS type,
-  NULL AS product_variant,
-  product_cd AS product_name,
-  product_launch_dt AS product_launch_date,
-  product_status_txt AS product_status,
-  product_lob_cd AS product_line_of_business_code,
-  underwriting_group_txt AS underwriting_group,
-  regulatory_approval_cd AS regulatory_approval_code,
-  'sat_product_crm' AS source_tbl_name
-FROM crm_product_catelog_csv
-UNION ALL
-SELECT DISTINCT
-  md5(CONCAT(product_ref, origin_sys)) AS product_hash_key,
-  CAST(load_ts AS timestamp) AS load_date,
-  product_type_txt AS type,
-  product_variant AS product_variant,
-  product_cd AS product_name,
-  product_launch_dt AS product_launch_date,
-  product_status_txt AS product_status,
-  product_lob_cd AS product_line_of_business_code,
-  underwriting_group_txt AS underwriting_group,
-  regulatory_approval_cd AS regulatory_approval_code,
-  'sat_product_crm' AS source_tbl_name
-FROM crm_product_catalog_csv
+    md5(concat(origin_sys, product_ref)) AS product_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    product_type_txt AS type,
+    product_variant AS product_variant,
+    product_cd AS product_name,
+    product_launch_dt AS product_launch_date,
+    product_status_txt AS product_status,
+    product_lob_cd as product_line_of_business_code,
+    underwriting_group_txt as underwriting_group,
+    regulatory_approval_cd as regulatory_approval_code
+FROM crm_product_catalog_csv;
```

</details>

### sat_product_sap

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_product_sap_3` | `sat_product_sap_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_product_sap` | `sat_product_sap` |
| `process_order` | `3` | `3` |
| `source_table` | `sap_product_db` | `sap_product_db` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,11 +1,9 @@
 SELECT DISTINCT
-  md5(CONCAT(CAST(product_id AS STRING), CAST(origin_sys AS STRING))) AS product_hash_key,
-  CAST(load_ts AS TIMESTAMP) AS load_date,
-  product_type,
-  product_sub_type,
-  product_name,
-  product_start_date,
-  line_of_business,
-  origin_sys AS record_source,
-  'sat_product_sap' AS source_tbl_name
-FROM sap_product_db
+    md5(concat(origin_sys, product_id)) AS product_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    product_type AS product_type,
+    product_sub_type AS product_sub_type,
+    product_name AS product_name,
+    product_start_date AS product_start_date,
+    line_of_business AS line_of_business
+FROM sap_product_db;
```

</details>

### sat_quote_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_quote_crm_3` | `sat_quote_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_quote_crm` | `sat_quote_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_quote_register_csv` | `crm_quote_register_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,18 +1,17 @@
 SELECT DISTINCT
-  md5(CONCAT(quote_ref, origin_sys)) AS quote_hash_key,
-  current_timestamp() AS load_date,
-  gross_amt AS gross_revenue,
-  net_amt AS net_revenue,
-  quote_no AS quote_number,
-  quote_status_txt AS quote_status,
-  renewal_amt_curr AS renewal_amt_current_period,
-  renewal_amt_next AS renewal_amt_next_period,
-  quoted_premium AS quoted_premium,
-  quoted_date AS quote_date,
-  quote_month_name AS quote_month_name,
-  risk_score AS risk_score,
-  policy_complexity AS policy_complexity,
-  uw_approval_type AS uw_approval_type,
-  rejection_reason AS rejection_reason,
-  'sat_quote_crm' AS source_tbl_name
-FROM crm_quote_register_csv
+    md5(concat(origin_sys, quote_ref)) AS quote_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    gross_amt AS gross_revenue,
+    net_amt AS net_revenue,
+    quote_no AS quote_number,
+    quote_status_txt AS quote_status,
+    renewal_amt_curr AS renewal_amt_current_period,
+    renewal_amt_next AS renewal_amt_next_period,
+    quoted_premium AS quoted_premium,
+    quoted_date AS quote_date,
+    quote_month_name AS quote_month_name,
+    risk_score AS risk_score,
+    policy_complexity AS policy_complexity,
+    uw_approval_type AS uw_approval_type,
+    rejection_reason AS rejection_reason
+FROM crm_quote_register_csv;
```

</details>

### sat_regulation_crm

**Status:** Different

| Field | Source Python | Notebook |
|---|---|---|
| `product_id` | `3` | `9` |
| `vault_object_id` | `sat_regulation_crm_3` | `sat_regulation_crm_9` |
| `object_type` | `sat` | `sat` |
| `target_table` | `sat_regulation_crm` | `sat_regulation_crm` |
| `process_order` | `3` | `3` |
| `source_table` | `crm_regulation_register_csv` | `crm_regulation_register_csv` |

<details>
<summary>SQL diff</summary>

```diff
--- source_python_build_sql
+++ notebook_build_sql
@@ -1,17 +1,15 @@
 SELECT DISTINCT
-  md5(CONCAT(src_regulation_id, src_system)) AS regulation_hash_key,
-  current_timestamp() AS load_date,
-  src_regulation_number AS regulation_number,
-  src_regulation_name AS regulation_name,
-  src_regulation_department AS regulation_department,
-  src_regulation_region AS regulation_region,
-  src_regulation_risk_level AS regulation_risk_level,
-  src_regulation_compliance_status AS regulation_compliance_status,
-  src_regulation_date_raised AS regulation_date_raised,
-  src_regulation_date_closed AS regulation_date_closed,
-  src_regulation_owner AS regulation_owner,
-  src_regulation_deadline_date AS regulation_deadline_date,
-  src_is_regulation_on_time AS is_regulation_on_time,
-  src_system AS record_source,
-  'sat_regulation_crm' AS source_tbl_name
-FROM crm_regulation_register_csv
+    md5(concat(origin_sys, src_regulation_id)) AS regulation_hash_key,
+    CAST(pull_ts AS timestamp) AS load_date,
+    src_regulation_number AS regulation_number,
+    src_regulation_name AS regulation_name,
+    src_regulation_department AS regulation_department,
+    src_regulation_region AS regulation_region,
+    src_regulation_risk_level AS regulation_risk_level,
+    src_regulation_compliance_status AS regulation_compliance_status,
+    src_regulation_date_raised AS regulation_date_raised,
+    src_regulation_date_closed AS regulation_date_closed,
+    src_regulation_owner AS regulation_owner,
+    src_regulation_deadline_date AS regulation_deadline_date,
+    src_is_regulation_on_time AS is_regulation_on_time
+FROM crm_regulation_register_csv;
```

</details>
