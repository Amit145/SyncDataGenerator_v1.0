# SyncDataGenerator

This repo generates synthetic insurance data across multiple raw sources, maps selected sources into a common raw contract, builds vault-compatible silver tables, and creates SCD2 outputs.

## Current Outputs

Default `main.py` output:

- base Data Vault CSVs under `data/output/<run_id>`
- normalized synthetic base under `data/synthetic/base/<run_id>`
- enhanced 360 under `data/synthetic/enhanced/<run_id>`
- raw CRM/API/claims/data_source outputs
- canonical raw for CRM and data_source
- API silver under `data/silver/api/<run_id>`
- raw CRM/API SCD2 when prior raw batches exist
- base/enhanced SCD2 when prior synthetic runs exist

Optional output:

- `data/new_outputs_src/<source>/data/<run_id>` and `data/new_outputs_src/<source>/scd2/<run_id>` are generated only when `--include-new-outputs-src` is passed.

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

Run normal generation plus optional source-specific outputs:

```powershell
.\venv\Scripts\python.exe .\main.py --include-new-outputs-src
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

## Config Reference

The full meaning of every `config/scenario_v1.json` setting is documented in [docs/scenario_config_reference.md](F:/SyncDataGenerator_v1.0/docs/scenario_config_reference.md).

The current detailed rule set for base, raw, silver, enhanced, churn, and SCD2 outputs is documented in [docs/current_rules_reference.md](F:/SyncDataGenerator_v1.0/docs/current_rules_reference.md).

The latest generated-run validation summary, including expected vs current churn ratios, is documented in [docs/latest_run_validation.md](F:/SyncDataGenerator_v1.0/docs/latest_run_validation.md).

The new MLOps Data Vault output is written under `data/synthetic/mlops/<run_id>`, with SCD2 deltas under `data/scd2/mlops/<run_id>` when a prior MLOps run exists. The DDL schema review, column delta, and validation command are documented in [docs/mlops_gen_schema_review.md](F:/SyncDataGenerator_v1.0/docs/mlops_gen_schema_review.md).

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
- Policy status uses the configured churn factors first, then applies a narrow calibration pass so premium, claim-count, and add-on marginal bands stay close to the workbook ranges without changing output columns.
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

Main normalized output:

- `data/output/<run_id>`

Raw folders:

- `data/raw/crm/<run_id>`
- `data/raw/crm_canonical/<run_id>`
- `data/raw/api/<run_id>`
- `data/raw/claims/<run_id>`
- `data/raw/claims_canonical/<run_id>`
- `data/raw/data_source/motor/<run_id>`
- `data/raw/data_source/home/<run_id>`
- `data/raw/data_source_canonical/<run_id>`

Silver folders:

- `data/silver/rebuild/<run_id>` for CRM
- `data/silver/api/<run_id>`
- `data/silver/claims/<run_id>`
- `data/silver/data_source/<run_id>`

Synthetic folders:

- `data/synthetic/base/<run_id>`
- `data/synthetic/enhanced/<run_id>`

SCD2 folders:

- `data/scd2/base/<run_id>`
- `data/scd2/enhanced/<run_id>`
- `data/scd2/raw/crm/<run_id>`
- `data/scd2/raw/api/<run_id>`
- optional `data/new_outputs_src/<source>/scd2/<run_id>`

## SCD2 Coverage

Synthetic base/enhanced SCD2 uses sampled satellite mutations.

Raw SCD2 currently covers:

- `crm`
- `api`

Optional source-specific SCD2 covers `new_outputs_src` only when `--include-new-outputs-src` is used.

## Large Runs

For large base-only generation:

```powershell
.\venv\Scripts\python.exe .\main.py --streaming-base --total-people 10000000 --chunk-size 100000
```

The streaming path is intended for large unique base output. Full raw/silver/churn/SCD2 validation is still intended for the normal full workflow.
