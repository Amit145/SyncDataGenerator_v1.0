# Raw To Vault SQL Templates

These SQL files document how to build vault-shaped outputs from the generated raw PRD source extracts.

They are templates, not runtime code. Replace the raw staging table names with the names used by your database load process.

## Folder Purpose

- `base_prd1_to_vault.sql` builds the base vault from `data/raw/base/prd_01/<run_id>` source files.
- `enhanced_mlops_prd2_to_vault.sql` applies PRD2 raw deltas to an existing base vault to build enhanced/MLOps vault tables.
- `compare_synthetic_vs_raw_vault.sql` contains comparison checks to validate raw-built vault output against synthetic output as the source of truth.

## Naming Assumption

The SQL assumes raw files are loaded into staging tables with these names:

- `raw_prd1_party_master`
- `raw_prd1_contact_point`
- `raw_prd1_identity_registry`
- `raw_prd1_address_book`
- `raw_prd1_lead_register`
- `raw_prd1_customer_portfolio`
- `raw_prd1_customer_lead_bridge`
- `raw_prd1_consent_snapshot`
- `raw_prd1_comm_preference`
- `raw_prd1_campaign_touch`
- `raw_prd1_account_book`
- `raw_prd1_product_catalog`
- `raw_prd1_quote_register`
- `raw_prd1_policy_register`
- `raw_prd1_property_asset`
- `raw_prd1_vehicle_asset`

PRD2 raw files use `raw_prd2_<file_name_without_csv>`, for example:

- `raw_prd2_channel_catalog`
- `raw_prd2_policy_channel_bridge`
- `raw_prd2_policy_enrichment`

## Hashing Rules

The templates use MySQL-compatible `MD5(...)`.

Base PRD1 links use the same left/right order as the base generator. Enhanced/MLOps PRD2 links use the enhanced generator order. This matters because link PKs are hashes of ordered components.

Examples:

- Base `link_policy_customer`: `MD5(policy_hash_key || '|' || customer_hash_key)`
- Enhanced `link_policy_channel`: `MD5(channel_hash_key || '|' || policy_hash_key)`
- Enhanced `link_policy_quote`: `MD5(quote_hash_key || '|' || policy_hash_key)`

## Validation Flow

After loading raw-built vault tables:

```powershell
.\venv\Scripts\python.exe .\verify_csv.py .\data\silver\base\<run_id>
.\venv\Scripts\python.exe .\verify_csv.py .\data\silver\mlops\<run_id>
```

For strict synthetic-as-source-of-truth comparison, compare every generated raw-built vault table against the matching `data/synthetic/<mode>/<run_id>` table by schema, row count, key set, and full row values.
