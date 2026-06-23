# SyncDataGenerator

This repo generates synthetic insurance Data Vault outputs for base, enhanced, and MLOps use cases. Normal runs focus on synthetic outputs and synthetic SCD2. Raw and silver rebuild outputs are configurable in `config/scenario_v1.json` because they add significant runtime on large runs.

## Current Outputs

Default `main.py` output:

- normalized synthetic base under `data/synthetic/base/<run_id>`
- enhanced 360 under `data/synthetic/enhanced/<run_id>`
- MLOps synthetic Data Vault under `data/synthetic/mlops/<run_id>`
- base/enhanced/MLOps SCD2 when prior synthetic runs exist

`data/output/<run_id>` is an intermediate base build folder. Normal `main.py` keeps it after successful validation and normalization. Use `--remove-working-output` only when you intentionally want to delete that intermediate folder after the synthetic base copy is written.

Optional output:

- mode-scoped PRD raw under `data/raw/base`, `data/raw/enhanced`, and `data/raw/mlops` is generated when `output_settings.generate_prd_raw=true`.
- mode-scoped silver vault rebuilds from PRD raw under `data/silver/base`, `data/silver/enhanced`, and `data/silver/mlops` are generated when `output_settings.generate_prd_raw=true` and `output_settings.generate_prd_silver=true`.
- legacy raw CRM/API/claims/data_source, canonical raw, API silver, and raw SCD2 are generated when `output_settings.generate_legacy_raw_silver=true` or `--include-raw-silver` is passed.
- `data/new_outputs_src/<source>/data/<run_id>` and `data/new_outputs_src/<source>/scd2/<run_id>` are generated when legacy raw/silver is enabled and `output_settings.generate_new_outputs_src=true` or `--include-new-outputs-src` is passed.

## Sources

Implemented default sources:

- `crm`
- `api`
- `claims`
- `data_source`

Optional `new_outputs_src` sources:

- `crm`
- `adp`
- `transunion`
- `experian`

`crm`, `api`, `claims`, and `data_source` use independent synthetic contexts where applicable. They follow the same churn and lifecycle rules but do not reuse the same raw business IDs.

## Main Commands

Run normal generation:

```powershell
.\venv\Scripts\python.exe .\main.py
```

Run only synthetic MLOps for faster NPS/MLOps iteration:

```powershell
.\venv\Scripts\python.exe .\main.py --mlops-only
```

This still builds the shared base context in memory, but writes only `data/synthetic/mlops/<run_id>` and skips base, enhanced, PRD raw, silver, and SCD2 outputs.

Run normal generation plus optional raw/canonical/silver outputs:

```powershell
.\venv\Scripts\python.exe .\main.py --include-raw-silver
```

Enable product raw and silver rebuilds in `config/scenario_v1.json` when needed:

```json
"output_settings": {
  "generate_legacy_raw_silver": false,
  "generate_prd_raw": true,
  "generate_prd_silver": true,
  "generate_new_outputs_src": false
}
```

Remove the intermediate `data/output/<run_id>` folder after normalization:

```powershell
.\venv\Scripts\python.exe .\main.py --remove-working-output
```

Verify mode-scoped PRD raw outputs after a run where `generate_prd_raw=true`:

```powershell
.\venv\Scripts\python.exe .\misc\verify_prd_raw_mlops.py --mode base --run-id <run_id>
.\venv\Scripts\python.exe .\misc\verify_prd_raw_mlops.py --mode enhanced --run-id <run_id>
.\venv\Scripts\python.exe .\misc\verify_prd_raw_mlops.py --mode mlops --run-id <run_id>
```

Verify the silver vault rebuilt from PRD raw after a run where `generate_prd_silver=true`:

```powershell
.\venv\Scripts\python.exe .\verify_csv.py .\data\silver\base\<run_id>
.\venv\Scripts\python.exe .\verify_csv.py .\data\silver\mlops\<run_id>
```

Build a combined product vault from PRD1/base plus PRD2 raw:

```powershell
.\venv\Scripts\python.exe .\misc\build_product_combined_vault.py --run-id <run_id>
```

Then validate that combined vault with the existing MLOps vault verifier:

```powershell
.\venv\Scripts\python.exe .\misc\verify_mlops_synthetic.py .\data\product_combined\<combined_run_id>
```

Run base-style business/date/churn checks against the combined vault:

```powershell
.\venv\Scripts\python.exe .\verify_csv.py .\data\product_combined\<combined_run_id>
```

Run normal generation plus optional source-specific outputs:

```powershell
.\venv\Scripts\python.exe .\main.py --include-raw-silver --include-new-outputs-src
```

Transform latest raw sources to silver:

```powershell
.\venv\Scripts\python.exe .\misc\transform_all_raw_to_silver.py
```

Verify all latest silver folders:

```powershell
.\venv\Scripts\python.exe .\misc\verify_all_silver.py
```

Validate churn KPI fields in the latest base run:

```powershell
.\venv\Scripts\python.exe .\validate_churn_kpis.py
```

Validate a specific base run:

```powershell
.\venv\Scripts\python.exe .\validate_churn_kpis.py --path .\data\synthetic\base\<run_id>
```

Generate direct MLOps dimensional output from an existing MLOps synthetic vault run:

```powershell
.\venv\Scripts\python.exe .\misc\generate_direct_dim_fact.py --run-id <run_id>
```

This writes all 22 dimensional-model CSVs under `data/dim_fact_direct/mlops/<run_id>` using `mlops/Enhanced_Customer360_Dimensional_Model_DDL.sql`.

Validate the direct dimensional output structure, surrogate keys, SCD2 columns, and fact-to-dimension referential integrity:

```powershell
.\venv\Scripts\python.exe .\misc\verify_direct_dim_fact.py .\data\dim_fact_direct\mlops\<run_id>
```

Validate NPS ratios at the ML notebook `master_df` grain:

```powershell
.\venv\Scripts\python.exe .\misc\verify_nps_dim_fact.py .\data\dim_fact_direct\mlops\<run_id>
```

## Config Reference

The full meaning of every `config/scenario_v1.json` setting is documented in [docs/scenario_config_reference.md](F:/SyncDataGenerator_v1.0/docs/scenario_config_reference.md).

The current detailed rule set for base, raw, silver, enhanced, churn, and SCD2 outputs is documented in [docs/current_rules_reference.md](F:/SyncDataGenerator_v1.0/docs/current_rules_reference.md).

The latest generated-run validation summary, including expected vs current churn ratios, is documented in [docs/latest_run_validation.md](F:/SyncDataGenerator_v1.0/docs/latest_run_validation.md).

The new MLOps Data Vault output is written under `data/synthetic/mlops/<run_id>`, with SCD2 deltas under `data/scd2/mlops/<run_id>` when a prior MLOps run exists. The DDL schema review, column delta, and validation command are documented in [docs/mlops_gen_schema_review.md](F:/SyncDataGenerator_v1.0/docs/mlops_gen_schema_review.md).

The active MLOps Data Vault DDL is `mlops/Enhanced_Customer360_DataVault_Model_DDL.sql`. The current MLOps schema includes 9 latest feedback/satisfaction fields across `sat_claim`, `sat_complaint`, `sat_customer`, `sat_marketing_engagement`, and `sat_policy`. These fields are generated in synthetic MLOps, carried into MLOps PRD2 raw, rebuilt into MLOps silver, and included in MLOps SCD2 when those rows mutate.

MLOps-only churn KPI ratios from the workbook are validated with `misc/verify_mlops_churn_kpis.py`. The MLOps generator calibrates coupled KPI families together, so policy type/policy renewal, payment method/direct-debit cancellation/missed payments/installment default, claim fault/satisfaction, customer satisfaction, complaint resolution days, and marketing sentiment/engagement fields stay logically consistent while targeting workbook churn bands.

NPS workbook features from `churnps/Data Req Churn NPS.xlsx` sheet `NPS_Features` are validated at vault level with `misc/verify_nps_features.py`. ML dim/fact exports can be validated at the notebook `master_df` grain with `misc/verify_nps_dim_fact.py .\nps_june\data`. Direct MLOps dim/fact output can be generated from a synthetic MLOps vault run with `misc/generate_direct_dim_fact.py` and validated with `misc/verify_direct_dim_fact.py` plus `misc/verify_nps_dim_fact.py`. Safe NPS ratios are configured in `config/scenario_v1.json` under `nps_settings`; the generator uses existing columns and derived proxies only, so it does not add or change Data Vault schemas, PKs, FKs, date rules, churn rules, or claim financial rules.

Some MLOps workbook ratios are marginal KPI targets over fields that are dependent in the generated model. The main examples are `policy_type` versus `is_policy_renewal`, `payment_method` versus direct-debit cancellation, missed payments versus installment default, and engagement score versus policy-linked churn. Policy renewal is separate from auto-renew: `is_policy_renewal` is renewal/new-business context, while `is_auto_renew_enabled` is automatic renewal enrollment. Engagement keeps the workbook direction, where high engagement is lower churn and low engagement is higher churn.

Enhanced claim financials are configurable through `claim_financial_settings`. They populate enhanced `sat_claim` amount, paid, reserve, expense, recovery/fraud/legal financials, `claim_band`, and `claim_band_sort`.

## Churn Rules

Churn source fields are generated in the base satellites and inherited by raw, canonical raw, silver, and enhanced outputs.

The churn distributions are configurable in `config/scenario_v1.json` under `churn_settings`:

- `renewal_current_premium_band_weights`
- `renewal_movement_band_weights`
- `claim_count_weights`
- `active_claim_count_weights`
- `declined_claim_count_weights`
- `cover_option_weights`
- `vehicle_segment_weights`
- `marketing_engagement_band_weights`
- `service_call_band_weights`
- `driver_experience_band_weights`
- `customer_status_weights`
- `account_status_weights`
- `tenure_churn_probability`
- `sales_channel_by_policy_status`
- `suspended_policy_status_weights`
- `churned_policy_status_weights`

Important churn behavior:

- `Policy Cycle` is completed annual tenure from policy start date to load/snapshot date.
- `Policy Cycle` is not the number of policies purchased by a customer.
- Churn decreases as `Policy Cycle` increases: `<1` has the highest churn, then `1-2`, then `3-5`, then `>5`.
- Current `Policy Cycle` churn is tuned to the workbook tenure ranges: `<1 year 35-50%`, `1-2 years 25-35%`, `3-5 years 15-25%`, and `>5 years 8-15%`.
- Renewal current/next amounts follow the configured churn movement bands.
- Current premium churn follows workbook ranges: low `10-18%`, medium `15-25%`, high `25-40%`, and very high `40-55%`.
- Percentage premium increase churn follows workbook ranges: `<0%` movement `8-12%`, `0-5%` movement `15-20%`, `5-10%` movement `25-35%`, and `>10%` movement `45-65%`.
- Absolute premium increase churn follows workbook ranges: `<=0` increase `8-12%`, `1-50` increase `15-22%`, `51-100` increase `25-38%`, and `>100` increase `45-65%`.
- Claim-count, add-on, marketing proxy, driver-experience proxy, and vehicle model churn bands are validated.
- Claim-count churn follows workbook ranges: `0` claims `12-18%`, `1` claim `20-30%`, `2` claims `30-45%`, and `3+` claims `45-60%`.
- Add-on churn follows workbook ranges: `0` add-ons `25-40%`, `1` add-on `18-28%`, `2` add-ons `12-22%`, and `3+` add-ons `8-18%`.
- Marketing engagement churn follows workbook ranges: high `8-15%`, medium `18-30%`, low `35-55%`, and none `50-70%`, using existing marketing flags as the proxy.
- Driver experience churn follows workbook ranges: `<2y` `25-40%`, `2-5y` `18-30%`, `6-10y` `15-25%`, and `>10y` `10-18%`, using existing `birth_date` as the proxy when licence issue date is unavailable.
- Vehicle segment churn follows workbook ranges: standard `12-22%`, premium `20-35%`, and high-risk `30-50%`; enhanced validation checks the rate through direct policy-to-motor links.
- Policy status uses the configured churn factors first, then applies a calibration pass so premium, claim-count, add-on, marketing, driver, vehicle, current-premium, and tenure bands target workbook ranges without changing output columns.
- Sales-channel churn variance is preserved using existing channel values: `AGENT` carries broker/aggregator-like higher churn behavior; `AGGREGATOR` is not emitted. The workbook does not define a sales-channel benchmark range, so this variance is scenario-config driven.

Validation coverage:

- `validate_churn_kpis.py` checks churn source fields, configured workbook churn ranges, and churn direction by tenure/channel.
- `verify_csv.py` checks the same churn rules as part of full silver validation.

## Policy Date Rules

Policy date rules are validated in base and silver checks:

- `Policy Start Date <= Policy End Date`
- `Renewal Date` is within the configured renewal window before `Policy End Date`
- `ACTIVE`, `LAPSED`, and `CANCELLED` statuses are date-consistent
- long-tenure active/lapsed policies use the current annual term boundary
- `LAPSED` only represents a completed renewal cycle; sub-one-year churn is represented as `CANCELLED`

## Output Folders

Intermediate base working output, kept after successful normalization unless `--remove-working-output` is passed:

- `data/output/<run_id>`

Synthetic folders:

- `data/synthetic/base/<run_id>`
- `data/synthetic/enhanced/<run_id>`
- `data/synthetic/mlops/<run_id>`

Optional legacy raw folders, generated with `output_settings.generate_legacy_raw_silver=true` or `--include-raw-silver`:

- `data/raw/crm/<run_id>`
- `data/raw/crm_canonical/<run_id>`
- `data/raw/api/<run_id>`
- `data/raw/claims/<run_id>`
- `data/raw/claims_canonical/<run_id>`
- `data/raw/data_source/motor/<run_id>`
- `data/raw/data_source/home/<run_id>`
- `data/raw/data_source_canonical/<run_id>`

Mode-scoped PRD raw folders are generated when `output_settings.generate_prd_raw=true`:

- `data/raw/base/prd_01/<run_id>`
- `data/raw/base/prd_02/<run_id>`
- `data/raw/enhanced/prd_01/<run_id>`
- `data/raw/enhanced/prd_02/<run_id>`
- `data/raw/enhanced/prd_delta/<run_id>`
- `data/raw/mlops/prd_01/<run_id>`
- `data/raw/mlops/prd_02/<run_id>`
- `data/raw/mlops/prd_delta/<run_id>`

`prd_01` is source 1. For `base`, it is the CRM raw shape. For `enhanced` and `mlops`, it contains the CRM raw shape plus mirrored enhanced/MLOps source-1 delta files prefixed with `source1_`, so all source-1 relevant tables are available from one folder without overwriting CRM files. `prd_02` is always the SAP/source-2 raw shape from `business_vault/bv.xlsx` sheet `Source2_Structure`; it uses different `SAP_*` source IDs while preserving matchable business attributes for later Business Vault mastering.

Raw CRM and PRD1 file names do not repeat the folder source prefix. For example, the generated files are `party_master.csv`, `address_book.csv`, and `account_book.csv`, not `crm_party_master.csv`, `crm_address_book.csv`, or `crm_account_book.csv`.

PRD2 SAP/source-2 files:

| File | Main columns |
|---|---|
| `Person.csv` | `batch_ref`, `pull_ts`, `origin_sys`, `person_id`, `person_type`, `organization`, `org_establishment_date`, `first_name`, `middle_name`, `last_name`, `date_of_birth`, `gender`, `occupation`, `email_address`, `phone_number` |
| `Address.csv` | `batch_ref`, `pull_ts`, `origin_sys`, `address_id`, `person_id`, `address_line_1`, `address_line_2`, `city`, `state`, `country`, `zipcode` |
| `Product.csv` | `batch_ref`, `pull_ts`, `origin_sys`, `product_id`, `product_type`, `product_sub_type`, `product_name`, `product_start_date`, `line_of_business` |
| `Home.csv` | `batch_ref`, `pull_ts`, `origin_sys`, `home_id`, `policy_id`, `product_id`, `home_type`, `home_location`, `wall_type`, `roof_material` |
| `Motor.csv` | `batch_ref`, `pull_ts`, `origin_sys`, `motor_id`, `policy_id`, `product_id`, `motor_class`, `motor_model`, `motor_type`, `manufacturing_date`, `body_colour`, `fuel_type`, `gear_type`, `motor_parked_location` |

Every SAP PRD2 file starts with source metadata columns `batch_ref`, `pull_ts`, and `origin_sys`; `origin_sys` is always `SAP`. SAP IDs are intentionally different from CRM IDs. For example, CRM `PER_...` becomes SAP `SAP_PER_...`, CRM `PRD_...` becomes SAP `SAP_PRD_...`, and CRM `POL_...` becomes SAP `SAP_POL_...`.

Business Vault person matching uses only the agreed match-key fields. Natural-person matching uses CRM `given_nm`, `family_nm`, `dob` against SAP `first_name`, `last_name`, `date_of_birth`. Legal-entity matching uses CRM `legal_name`, `constitution_dt` against SAP `organization`, `org_establishment_date`. Email, phone, and gender fields are explicitly excluded from the match-key contract.

Verify the raw Business Vault source contract after generation:

```powershell
.\venv\Scripts\python.exe .\misc\verify_business_vault_raw.py --mode base --run-id <run_id>
.\venv\Scripts\python.exe .\misc\verify_business_vault_raw.py --mode enhanced --run-id <run_id>
.\venv\Scripts\python.exe .\misc\verify_business_vault_raw.py --mode mlops --run-id <run_id>
```

`data/raw/enhanced/prd_delta/<run_id>` and `data/raw/mlops/prd_delta/<run_id>` contain source-style raw extracts for the additional enhanced/MLOps product. They do not contain vault-shaped `hub_`, `link_`, or `sat_` files. `prd_delta` has seven added entity registers plus bridge/enrichment extracts needed to rebuild the enhanced/MLOps vault without losing relationships or added satellite columns.

The same delta files are mirrored into enhanced/MLOps `prd_01` using a `source1_` prefix, for example `source1_complaint_register.csv` and `source1_override_register.csv`. The unprefixed `prd_delta` copy is retained for rebuild tooling.

`prd_delta` raw columns use `src_*` source names instead of vault names. For example, vault columns such as `policy_hash_key`, `load_date`, and `record_source` are stored as `src_policy_ref`, `src_extract_ts`, and `src_system`; `misc/build_product_combined_vault.py` maps them back to the MLOps vault schema during rebuild.

`prd_delta` added entity registers:

- `broker_book.csv`
- `campaign_register.csv`
- `channel_catalog.csv`
- `complaint_register.csv`
- `insured_object_register.csv`
- `override_register.csv`
- `regulation_register.csv`

`prd_delta` supporting relationship/enrichment extracts:

- `address_book.csv`
- `claim_register.csv`
- `broker_person_bridge.csv`
- `policy_broker_bridge.csv`
- `policy_channel_bridge.csv`
- `policy_quote_bridge.csv`
- `claim_policy_bridge.csv`
- `complaint_policy_bridge.csv`
- `complaint_regulation_bridge.csv`
- `person_address_bridge.csv`
- `person_campaign_bridge.csv`
- `policy_insured_object_bridge.csv`
- `policy_override_bridge.csv`
- `insured_object_home_bridge.csv`
- `insured_object_motor_bridge.csv`
- `quote_broker_bridge.csv`
- `quote_channel_bridge.csv`
- `policy_enrichment.csv`
- `customer_enrichment.csv`
- `marketing_engagement_enrichment.csv`
- `motor_enrichment.csv`

Optional legacy silver folders, generated with `output_settings.generate_legacy_raw_silver=true`, `--include-raw-silver`, or follow-up silver tools:

- `data/silver/rebuild/<run_id>` for CRM
- `data/silver/api/<run_id>`
- `data/silver/claims/<run_id>`
- `data/silver/data_source/<run_id>`

Mode-scoped silver folders generated when both `output_settings.generate_prd_raw=true` and `output_settings.generate_prd_silver=true`:

- `data/silver/base/<run_id>` rebuilt from `data/raw/base/prd_01/<run_id>`
- `data/silver/enhanced/<run_id>` rebuilt from enhanced PRD1 plus PRD2 raw
- `data/silver/mlops/<run_id>` rebuilt from MLOps PRD1 plus PRD2 raw

These folders use the same vault CSV structure as the corresponding synthetic mode. Base silver is built directly from PRD1 source fields; enhanced/MLOps silver starts from the base silver vault and applies PRD2 source-style bridge, entity, and enrichment extracts.

SCD2 folders:

- `data/scd2/base/<run_id>`
- `data/scd2/enhanced/<run_id>`
- `data/scd2/mlops/<run_id>`
- optional `data/scd2/raw/crm/<run_id>` and `data/scd2/raw/api/<run_id>` with `--include-raw-silver`
- optional `data/new_outputs_src/<source>/scd2/<run_id>` with `--include-raw-silver --include-new-outputs-src`

## SCD2 Coverage

Synthetic base/enhanced/MLOps SCD2 uses sampled satellite mutations.
SCD2 now skips sampled rows that do not produce a real business-value change, and stable reference satellites such as `sat_channel` are not emitted unless a meaningful mutable field is added.

Raw SCD2 currently covers:

- `crm`
- `api`

Optional source-specific SCD2 covers `new_outputs_src` only when `--include-raw-silver --include-new-outputs-src` is used.

## Large Runs

For large base-only generation:

```powershell
.\venv\Scripts\python.exe .\main.py --streaming-base --total-people 10000000 --chunk-size 100000
```

The streaming path is intended for large unique base output. Raw/silver validation requires `--include-raw-silver`; the default normal workflow focuses on synthetic base, enhanced, MLOps, churn, and synthetic SCD2.
