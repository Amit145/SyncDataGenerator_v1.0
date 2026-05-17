# Unified Data Runbook

This runbook describes the current storage layout and the commands to run:

- base synthetic generation
- enhanced synthetic generation
- raw source generation
- source-specific `new_outputs_src` generation
- normalized synthetic output
- SCD2 delta generation
- Kaggle raw ingestion
- Kaggle vault-style silver generation
- verification

## Storage Layout

All generated data now lands under:

`data/`

Main folders:

- `data/input/kaggle`
- `data/raw/crm`
- `data/raw/api`
- `data/raw/kaggle`
- `data/raw/data_source`
- `data/raw/data_source_canonical`
- `data/output`
- `data/synthetic/base`
- `data/synthetic/enhanced`
- `data/silver/rebuild`
- `data/silver/api`
- `data/silver/kaggle`
- `data/silver/data_source`
- `data/scd2/base`
- `data/scd2/enhanced`
- `data/scd2/raw`
- `data/scd2/updated`
- `data/scd2/reports`
- `data/new_outputs_src`

Meaning:

- `data/output/<run_id>`: primary vault-style generator output from `main.py`
- `data/raw/crm/<run_id>`: raw CRM landing generated from the synthetic vault output
- `data/raw/api/<run_id>`: raw API landing
- `data/synthetic/base/<run_id>`: normalized synthetic CSVs produced from `data/output/<run_id>`
- `data/synthetic/enhanced/<run_id>`: enhanced synthetic output aligned to the enhanced 360 DDL
- `data/scd2/base/<run_id>`: incremental SCD2 satellite delta rows derived from previous runs
- `data/scd2/enhanced/<run_id>`: incremental enhanced SCD2 satellite delta rows derived from previous enhanced runs
- `data/scd2/raw/crm/<run_id>`: raw CRM delta rows versus the prior raw CRM batch
- `data/scd2/raw/api/<run_id>`: raw API delta rows versus the prior raw API batch
- `data/scd2/raw/kaggle/<dataset>/<run_id>`: raw Kaggle delta rows versus the prior Kaggle batch for that dataset
- `data/scd2/updated/<run_id>`: optional manually updated SCD2 delta set
- `data/raw/kaggle/<dataset>/<batch_id>`: canonical raw landing for external Kaggle datasets
- `data/raw/data_source/motor/<run_id>`: source-native motor raw extracts
- `data/raw/data_source/home/<run_id>`: source-native home raw extracts
- `data/raw/data_source_canonical/<run_id>`: merged canonical raw built from data_source extracts
- `data/silver/kaggle/<batch_id>`: vault-style silver output rebuilt from Kaggle raw
- `data/silver/data_source/<run_id>`: vault-style silver output rebuilt from data_source canonical raw
- `data/silver/rebuild/<run_id>`: local silver rebuild from CRM raw
- `data/silver/api/<run_id>`: local silver rebuild from API raw
- `data/new_outputs_src/<source>/data/<run_id>`: source-specific exported datasets
- `data/new_outputs_src/<source>/scd2/<run_id>`: per-source SCD2 output

## Prerequisites

Run commands from repo root:

```powershell
cd F:\SyncDataGenerator_v1.0
```

If your Windows console has trouble printing Unicode from `main.py`, use:

```powershell
$env:PYTHONUTF8='1'
```

## Base Load

Generate the main synthetic run:

```powershell
python .\main.py
```

Outputs:

- `data/output/<run_id>`
- `data/raw/crm/<run_id>`
- `data/raw/api/<run_id>`
- `data/raw/data_source_canonical/<run_id>`
- `data/silver/api/<run_id>`
- `data/synthetic/base/<run_id>`
- `data/synthetic/enhanced/<run_id>`

What `main.py` also does:

- validates DDL/file structure
- validates referential and business integrity
- if there is a previous normalized run, generates SCD2 deltas under `data/scd2/base/<run_id>`
- if there is a previous enhanced run, generates SCD2 deltas under `data/scd2/enhanced/<run_id>`
- if there is a previous raw batch for CRM, API, or Kaggle, generates raw SCD2 deltas under `data/scd2/raw/...`
- generates `new_outputs_src` for `crm`, `adp`, `transunion`, and `experian`
- if there is a previous source batch, generates source-specific deltas under `data/new_outputs_src/<source>/scd2/<run_id>`
- generates raw CRM
- generates raw API
- generates enhanced synthetic 360 output
- generates raw Kaggle for any discoverable dataset under `data/input/kaggle` or legacy `kaggle_input`

## One-Command Raw To Silver

Transform the latest raw CRM, API, and Kaggle batches into silver:

```powershell
python .\misc\transform_all_raw_to_silver.py
```

This writes to:

- `data/silver/rebuild/<run_id>`
- `data/silver/api/<run_id>`
- `data/silver/kaggle/<run_id>`

## One-Command Silver Verification

Verify all current silver outputs:

```powershell
python .\misc\verify_all_silver.py
```

## Base Verification

Verify the latest rebuilt silver folder:

```powershell
python .\verify_csv.py
```

Verify a specific normalized or rebuilt folder:

```powershell
python .\verify_csv.py .\data\silver\rebuild\<run_id>
```

Compare normalized base output with rebuilt CRM silver:

```powershell
python .\misc\verify_silver_rebuild.py
```

Rebuild silver manually from the latest CRM raw:

```powershell
python .\misc\raw_to_silver_sample.py
```

That writes to:

- `data/silver/rebuild/<run_id>`

## Incremental / SCD2 Load

SCD2 deltas are created only when there is at least one previous normalized base run.

Normal incremental flow:

1. Run a second or later base load.
2. `main.py` reads historical normalized runs from `data/synthetic/base`.
3. It writes new satellite deltas to `data/scd2/base/<run_id>`.
4. It also compares the latest raw snapshots and writes raw deltas to:
   - `data/scd2/raw/crm/<run_id>`
   - `data/scd2/raw/api/<run_id>`
   - `data/scd2/raw/kaggle/<dataset>/<run_id>`
5. It compares prior enhanced runs and writes:
   - `data/scd2/enhanced/<run_id>`
6. It compares `new_outputs_src` source batches and writes:
   - `data/new_outputs_src/<source>/scd2/<run_id>`

Generate a new base run:

```powershell
python .\main.py
```

The new SCD2 delta appears in:

- `data/scd2/base/<run_id>`
- `data/scd2/raw/.../<run_id>`

## Manual SCD2 Update

Create a modified SCD2 delta set from the latest SCD2 folder:

```powershell
python .\update_scd2_records.py
```

Or explicitly:

```powershell
python .\update_scd2_records.py `
  --input .\data\scd2\base\<run_id> `
  --output .\data\scd2\updated\<run_id>
```

Compare original and updated SCD2 rows:

```powershell
python .\compare_scd2_updates.py
```

Or explicitly:

```powershell
python .\compare_scd2_updates.py `
  --original .\data\scd2\base\<run_id> `
  --updated .\data\scd2\updated\<run_id>
```

Compare all current SCD2 sources:

```powershell
python .\misc\compare_all_scd2.py
```

This reports change counts for:

- raw CRM SCD2
- raw API SCD2
- raw Kaggle SCD2
- synthetic satellite SCD2

## Kaggle Input

Place Kaggle datasets under:

- `data/input/kaggle/<dataset_folder>`

Example:

- `data/input/kaggle/insurance_customer_data/AutoInsurance.csv`

## Kaggle Raw Only

Convert a Kaggle dataset into canonical raw CRM files:

```powershell
python .\generators\raw_kaggle_generator.py `
  --input-dir .\data\input\kaggle\insurance_customer_data `
  --config .\config\kaggle_mappings\auto_insurance_kaggle.json
```

Output:

- `data/raw/kaggle/auto_insurance_kaggle/<batch_id>`

## Kaggle Vault-Style Silver

Run the full Kaggle flow in one command:

```powershell
python .\misc\run_kaggle_to_silver.py `
  --input-dir .\data\input\kaggle\insurance_customer_data `
  --config .\config\kaggle_mappings\auto_insurance_kaggle.json
```

Outputs:

- `data/raw/kaggle/auto_insurance_kaggle/<batch_id>`
- `data/silver/kaggle/<batch_id>`

The Kaggle silver output writes the full vault-style file set defined by the local silver builder, including empty files for unsupported domains.

## Kaggle Verification

Verify a Kaggle raw batch and its vault-style silver output:

```powershell
python .\misc\verify_kaggle_silver.py `
  .\data\raw\kaggle\auto_insurance_kaggle\<batch_id> `
  .\data\silver\kaggle\<batch_id>
```

Or with no arguments, verify the latest Kaggle batch and matching silver folder:

```powershell
python .\misc\verify_kaggle_silver.py
```

## Data Source Pipeline

`data_source` is a separate source-native pipeline for line-of-business extracts like `motor` and `home`.

It does not change the existing CRM, API, or Kaggle flows.

Current output path:

- `data/raw/data_source/motor/<run_id>`
- `data/raw/data_source/home/<run_id>`
- `data/raw/data_source_canonical/<run_id>`
- `data/silver/data_source/<run_id>`

Run the full flow:

```powershell
python .\misc\data_source_to_silver.py
```

That command:

- generates source-native `motor` raw files
- generates source-native `home` raw files
- maps them into merged canonical raw
- builds a single merged silver vault
- verifies the resulting silver folder

Verify the latest `data_source` silver again:

```powershell
python .\misc\verify_data_source_silver.py
```

## Enhanced Verification

Enhanced-specific verification:

```powershell
$env:PYTHONUTF8='1'
python .\misc\verify_enhanced_synthetic.py
```

Or for a specific run:

```powershell
$env:PYTHONUTF8='1'
python .\misc\verify_enhanced_synthetic.py .\data\synthetic\enhanced\<run_id>
```

## Notes

- `AutoInsurance.csv` is only a partial fit for the full vault model.
- It populates person, customer, policy, product, motor, contact, identity, account, and address domains well.
- It does not naturally populate lead, quote, consent, marketing, legal-person, or home domains.
- Those tables still exist in Kaggle silver output, but many will be empty and some verification checks will be reported as skipped.
- Raw CRM and raw API SCD2 may show many `new_rows` because the synthetic generator regenerates source identifiers each run.
- Kaggle raw SCD2 is more stable when the source file itself is unchanged because business keys come from the dataset mapping.
- Enhanced rules and alignment details are documented in `docs/enhanced_synthetic_plan.md`.
