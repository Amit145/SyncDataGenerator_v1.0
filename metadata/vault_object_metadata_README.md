# Vault Object Metadata

This file documents the single-table metadata model used by [Bronze to Silver Vault Delta Tables.ipynb](F:/SyncDataGenerator_v1.0/Bronze%20to%20Silver%20Vault%20Delta%20Tables.ipynb).

The seed SQL for this metadata table is at [vault_object_metadata_seed.sql](F:/SyncDataGenerator_v1.0/metadata/vault_object_metadata_seed.sql).

## Purpose

`vault_object_metadata` drives Hub, Link, and Satellite generation from one control table.

The notebook is now strict metadata-driven:
- no Hub/Link/Satellite object definitions remain hardcoded in the notebook
- the metadata table must exist before the notebook runs
- the seed SQL is the source used to initialize the table

Each row represents one vault object definition:
- one `hub`
- one `link`
- one `sat`

## Columns

`object_type`
- Object category.
- Current values: `hub`, `link`, `sat`.

`target_table`
- Silver Delta table to build.
- Example: `hub_person`, `link_policy_product`, `sat_customer`.

`source_alias`
- Logical alias used by the notebook runtime.
- Example: `person`, `policy`, `customer`, `mpr`.

`source_table`
- Raw source table name read from the input schema.
- Example: `crm_person`, `crm_policy`.

`schema_json`
- JSON array listing target columns in write order.
- Used to create ordered output rows and target schemas.

`hash_alias`
- Logical hash-key family used for hubs and satellites.
- Example: `person`, `customer`, `policy`.

`hash_source_field`
- Raw source field whose value is hashed to derive the object hash key.
- Example: `person_id`, `policy_id`.

`business_key_target`
- Hub-only target business key column.
- Example: `person_id`.

`business_key_source`
- Hub-only raw source business key field.
- Example: `person_id`.

`mappings_json`
- Satellite-only JSON object mapping target satellite columns to raw source columns.
- Example: `{"customer_number":"customer_number","customer_status":"customer_status"}`.

`left_ref_json`
- Link-only JSON object describing the left side of the link.
- Structure:
  - `hash_map`
  - `row_field`
  - `column`

`right_ref_json`
- Link-only JSON object describing the right side of the link.
- Same structure as `left_ref_json`.

`link_pk`
- Link-only primary hash key column name.
- Example: `policy_product_hash_key`.

`required_fields_json`
- JSON array of raw source fields required before generating the target row.
- Example: `["policy_id","product_id"]`.

`record_source`
- Audit/source-system label written to hubs and links.
- Current seed value: `CRM`.

`process_order`
- Execution order for deterministic metadata processing.
- Lower values are processed first.

`is_active`
- Soft enable/disable flag.
- Runtime currently loads rows where `is_active = 'Y'`.

## How Runtime Uses It

For `hub` rows, runtime uses:
- `target_table`
- `schema_json`
- `hash_alias`
- `hash_source_field`
- `business_key_target`
- `business_key_source`

For `sat` rows, runtime uses:
- `target_table`
- `schema_json`
- `hash_alias`
- `hash_source_field`
- `mappings_json`

For `link` rows, runtime uses:
- `target_table`
- `schema_json`
- `left_ref_json`
- `right_ref_json`
- `link_pk`

## Current Seed

The current seed generated from the notebook contains:
- `52` active rows
- `17` hubs
- `18` links
- `17` satellites

## Recommended Run Order

1. Run [vault_object_metadata_seed.sql](F:/SyncDataGenerator_v1.0/metadata/vault_object_metadata_seed.sql).
2. Run [Bronze to Silver Vault Delta Tables.ipynb](F:/SyncDataGenerator_v1.0/Bronze%20to%20Silver%20Vault%20Delta%20Tables.ipynb).
3. Ensure notebook metadata parameters point to that table.
