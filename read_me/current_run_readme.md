# Current Run README

This runbook captures the current operational flow for:

- base load
- raw generation for all current sources
- common raw-to-silver transformation
- silver verification
- second-run SCD2 generation

Run all commands from:

```powershell
cd F:\SyncDataGenerator_v1.0
```

If your Windows console cannot print Unicode from `main.py`, set:

```powershell
$env:PYTHONUTF8='1'
```

## Storage Layout

All runtime data lands under:

`data/`

Main folders:

- `data/input/kaggle`
- `data/output`
- `data/raw/crm`
- `data/raw/api`
- `data/raw/kaggle`
- `data/raw/data_source`
- `data/raw/data_source_canonical`
- `data/silver/rebuild`
- `data/silver/api`
- `data/silver/kaggle`
- `data/silver/data_source`
- `data/synthetic/base`
- `data/scd2/base`
- `data/scd2/raw`
- `data/scd2/updated`
- `data/scd2/reports`

Meaning:

- `data/output/<run_id>`: primary vault-style synthetic output from `main.py`
- `data/raw/crm/<run_id>`: CRM raw snapshot derived from the synthetic output
- `data/raw/api/<run_id>`: API raw snapshot
- `data/raw/kaggle/<dataset>/<run_id>`: Kaggle canonical raw snapshot for a dataset
- `data/raw/data_source/motor/<run_id>`: source-native motor raw extract
- `data/raw/data_source/home/<run_id>`: source-native home raw extract
- `data/raw/data_source_canonical/<run_id>`: canonical raw mapped from the data source extracts
- `data/silver/rebuild/<run_id>`: vault silver rebuilt from CRM raw
- `data/silver/api/<run_id>`: vault silver rebuilt from API raw
- `data/silver/kaggle/<run_id>`: vault silver rebuilt from Kaggle canonical raw
- `data/silver/data_source/<run_id>`: vault silver rebuilt from data source canonical raw
- `data/synthetic/base/<run_id>`: normalized synthetic output derived from `data/output/<run_id>`
- `data/scd2/base/<run_id>`: synthetic satellite SCD2 delta rows for a later run
- `data/scd2/raw/crm/<run_id>`: raw CRM delta versus previous CRM raw batch
- `data/scd2/raw/api/<run_id>`: raw API delta versus previous API raw batch
- `data/scd2/raw/kaggle/<dataset>/<run_id>`: raw Kaggle delta versus previous batch for that dataset
- `data/scd2/updated/<run_id>`: optional manually modified SCD2 output

## Source Inputs

### Scenario-driven inputs

These drive the synthetic base generation:

- `config/scenario_v1.json`
- `config/cardinality.json`
- `enums/product_catalog.py`
- `enums/sat_enums.py`

These influence:

- `data/output`
- `data/raw/crm`
- `data/raw/api`
- `data/raw/data_source`
- `data/synthetic/base`
- `data/scd2/base`
- `data/scd2/raw/crm`
- `data/scd2/raw/api`

### Kaggle inputs

Kaggle is external-input-driven, not scenario-count-driven.

Input folders:

- `data/input/kaggle/<dataset_folder>`
- legacy fallback: `kaggle_input/<dataset_folder>`

Mapping configs:

- `config/kaggle_mappings/*.json`

Kaggle row counts come from the dataset itself.

## Base Load

Run:

```powershell
python .\main.py
```

What `main.py` currently does:

- creates base vault-style synthetic output
- generates raw CRM
- generates raw API
- generates raw data source extracts for `motor` and `home`
- generates raw Kaggle for discoverable dataset/config matches
- builds API silver
- normalizes the latest synthetic output into `data/synthetic/base/<run_id>`
- on later runs, generates synthetic-base SCD2
- on later runs, generates raw SCD2 for CRM, API, and Kaggle
- runs file and integrity validation on the primary output

Base load outputs:

- `data/output/<run_id>`
- `data/raw/crm/<run_id>`
- `data/raw/api/<run_id>`
- `data/raw/data_source/motor/<run_id>`
- `data/raw/data_source/home/<run_id>`
- `data/raw/kaggle/<dataset>/<run_id>` when matching Kaggle input exists
- `data/silver/api/<run_id>`
- `data/synthetic/base/<run_id>`

Important note:

- `main.py` does not currently build `data_source` canonical raw
- `main.py` does not currently build `data_source` silver
- `main.py` does not currently build CRM silver or Kaggle silver

Those are handled by the shared raw-to-silver step below.

## Common Raw To Silver

Run:

```powershell
python .\misc\transform_all_raw_to_silver.py
```

This command uses the common Python silver transformation and builds the latest available silver outputs for:

- CRM raw
- API raw
- Kaggle raw
- Data source canonical raw

Outputs:

- `data/silver/rebuild/<run_id>`
- `data/silver/api/<run_id>`
- `data/silver/kaggle/<run_id>`
- `data/silver/data_source/<run_id>`

Notes:

- API uses the same common silver builder, even though the raw format is JSONL
- Kaggle and data source both pass through canonical raw before silver

## Data Source Full Flow

If you want to run the data source pipeline explicitly:

```powershell
python .\misc\data_source_to_silver.py
```

That command:

- reads the latest CRM raw batch
- creates `data/raw/data_source/motor/<run_id>`
- creates `data/raw/data_source/home/<run_id>`
- maps those into `data/raw/data_source_canonical/<run_id>`
- builds `data/silver/data_source/<run_id>`
- verifies the resulting data source silver

## Silver Verification

### Verify all current silver outputs

```powershell
python .\misc\verify_all_silver.py
```

This verifies the latest current folders under:

- `data/silver/rebuild`
- `data/silver/api`
- `data/silver/data_source`
- every run folder under `data/silver/kaggle`

### Verify one specific silver folder

```powershell
python .\verify_csv.py .\data\silver\rebuild\<run_id>
```

You can replace that path with any silver folder:

- `data/silver/api/<run_id>`
- `data/silver/kaggle/<run_id>`
- `data/silver/data_source/<run_id>`

## First Run Behavior

On the first run:

- `data/output/<run_id>` is created
- raw CRM, API, data source, and eligible Kaggle outputs are created
- API silver is created by `main.py`
- no synthetic-base SCD2 is created yet
- no raw SCD2 is created yet unless an older raw batch already exists

Usually on a clean workspace, the first run creates no SCD2 output because there is no previous run to compare against.

## Second Run / Incremental SCD2 Behavior

On a second or later run, after running:

```powershell
python .\main.py
```

the following comparisons happen.

### Synthetic base SCD2

Input:

- historical runs under `data/synthetic/base`

Output:

- `data/scd2/base/<run_id>`

Behavior:

- previous latest synthetic satellite versions are loaded
- a small percentage of satellite rows are mutated
- changed rows are written as the new SCD2 delta set

### Raw CRM SCD2

Input:

- previous `data/raw/crm/<previous_run_id>`
- current `data/raw/crm/<run_id>`

Output:

- `data/scd2/raw/crm/<run_id>`

### Raw API SCD2

Input:

- previous `data/raw/api/<previous_run_id>`
- current `data/raw/api/<run_id>`

Output:

- `data/scd2/raw/api/<run_id>`

### Raw Kaggle SCD2

Input:

- previous `data/raw/kaggle/<dataset>/<previous_run_id>`
- current `data/raw/kaggle/<dataset>/<run_id>`

Output:

- `data/scd2/raw/kaggle/<dataset>/<run_id>`

Important note:

- Kaggle raw SCD2 is dataset-specific
- if the Kaggle source files are unchanged, the raw delta should usually be small or empty

### Data Source raw SCD2

Current state:

- not implemented in the raw SCD2 generator

That means there is currently no:

- `data/scd2/raw/data_source/<run_id>`

## Manual SCD2 Update Flow

Create a modified SCD2 output from the latest base SCD2:

```powershell
python .\update_scd2_records.py
```

Or explicitly:

```powershell
python .\update_scd2_records.py `
  --input .\data\scd2\base\<run_id> `
  --output .\data\scd2\updated\<run_id>
```

Compare original and updated SCD2:

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

## Recommended Operational Sequences

### Sequence A: First run on a clean workspace

```powershell
$env:PYTHONUTF8='1'
python .\main.py
python .\misc\transform_all_raw_to_silver.py
python .\misc\verify_all_silver.py
```

### Sequence B: Second run with SCD2 generation

```powershell
$env:PYTHONUTF8='1'
python .\main.py
python .\misc\transform_all_raw_to_silver.py
python .\misc\verify_all_silver.py
python .\misc\compare_all_scd2.py
```

### Sequence C: Data source only

```powershell
$env:PYTHONUTF8='1'
python .\main.py
python .\misc\data_source_to_silver.py
python .\misc\verify_data_source_silver.py
```

## Current Caveats

- `main.py` prints Unicode status messages, so `PYTHONUTF8=1` is recommended on Windows consoles
- `data_source` raw depends on CRM raw from the same run
- `data_source` canonical raw is an intermediate normalization layer, not an independent source landing
- Kaggle silver may legitimately have skipped validations for unsupported domains
- raw SCD2 currently covers CRM, API, and Kaggle, but not data source
