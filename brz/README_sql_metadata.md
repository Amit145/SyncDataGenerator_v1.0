# SQL-Driven `brz_vault` Metadata

This document describes the new SQL-driven metadata model for building silver vault tables from bronze/raw Delta tables.

## Purpose

The earlier `brz_vault.csv` model stored transformation logic across many metadata columns:

- `source_table`
- `schema_json`
- `business_key_source`
- `mappings_json`
- `left_ref_json`
- `right_ref_json`
- `required_fields_json`

The new model simplifies that by keeping a small control row and moving transformation detail into one SQL column.

## Files

- Current legacy metadata: [brz_vault.csv](/F:/SyncDataGenerator_v1.0/brz/brz_vault.csv)
- New SQL metadata CSV: [brz_vault_sql.csv](/F:/SyncDataGenerator_v1.0/brz/brz_vault_sql.csv)
- SQL DDL/load script: [brz_vault_sql_metadata.sql](/F:/SyncDataGenerator_v1.0/brz/brz_vault_sql_metadata.sql)
- Silver notebook: [bsnew.ipynb](/F:/SyncDataGenerator_v1.0/notebook/bsnew.ipynb)

## New Metadata Shape

Each row in `brz_vault_sql.csv` contains:

- `object_id`
- `object_type`
- `target_table`
- `record_source`
- `process_order`
- `is_active`
- `merge_keys`
- `build_sql`

## Meaning Of Columns

- `object_id`
  Stable unique key for the metadata row. Used to `MERGE` metadata updates.

- `object_type`
  One of:
  - `hub`
  - `link`
  - `sat`

- `target_table`
  The silver vault table to write.

- `record_source`
  The source label written into vault rows where applicable.

- `process_order`
  Execution order for the notebook.

- `is_active`
  `Y` rows are processed.

- `merge_keys`
  The key columns used by the notebook when merging into the Delta target table.

- `build_sql`
  SQL statement that returns the final vault-shaped rowset for `target_table`.

## Merge Rules

The metadata explicitly preserves current vault behavior:

- Hubs
  - immutable
  - `merge_keys` = hub hash key
  - rerun inserts only new business keys

- Links
  - immutable
  - `merge_keys` = link hash key
  - rerun inserts only new relationships

- Satellites
  - versioned
  - `merge_keys` = parent hash key + `load_date`
  - same-version duplicates are prevented
  - later versions are allowed

Current satellite versioning uses:

- `<parent_hash_key>|load_date`

If hashdiff is introduced later, this can be extended without changing the vault structure.

## `build_sql` Contract

`build_sql` must return the final target columns for the target vault table.

Examples:

- Hub SQL returns:
  - hash key
  - `load_date`
  - `record_source`
  - business key

- Link SQL returns:
  - link hash key
  - `load_date`
  - `record_source`
  - referenced hash keys

- Satellite SQL returns:
  - parent hash key
  - `load_date`
  - descriptive columns

The notebook does not derive the row shape from other metadata columns in SQL mode. It writes whatever `build_sql` returns.

## SQL Tokens

`build_sql` supports these placeholders:

- `${hub_load_date}`
- `${link_load_date}`
- `${sat_load_date}`
- `${record_source}`

These are replaced by the notebook before SQL execution.

## Metadata Load Behavior

The SQL file [brz_vault_sql_metadata.sql](/F:/SyncDataGenerator_v1.0/brz/brz_vault_sql_metadata.sql) is idempotent for metadata loading:

- `CREATE TABLE IF NOT EXISTS`
- `MERGE` on `object_id`

So rerunning the metadata load:

- does not recreate the table
- does not duplicate metadata rows
- updates changed metadata rows

## Notebook Behavior

The silver notebook [bsnew.ipynb](/F:/SyncDataGenerator_v1.0/notebook/bsnew.ipynb) now supports two modes:

1. Legacy mode
- used when metadata table does not have `build_sql`
- continues to read the older JSON-style metadata columns

2. SQL mode
- used when metadata table contains `build_sql`
- executes `build_sql` row by row
- uses `merge_keys` for Delta merge

This keeps backward compatibility while enabling the simplified SQL-driven model.

## How To Use In Databricks

1. Create/load the metadata table using:
- [brz_vault_sql_metadata.sql](/F:/SyncDataGenerator_v1.0/brz/brz_vault_sql_metadata.sql)

2. In the notebook, set:
- `metadata_table = brz_vault_sql_metadata`

3. Run the notebook normally.

## What Does Not Change

This metadata change does not alter:

- vault table names
- vault column structure
- hub immutability
- link immutability
- satellite multi-version behavior

Only metadata storage and notebook execution logic change.

## Recommended Rule For New Rows

For every future metadata row:

- one row = one vault object
- `build_sql` must output final target columns
- `merge_keys` must reflect vault behavior
- `process_order` must keep hub/link/sat sequencing sensible
- `object_id` must be stable
