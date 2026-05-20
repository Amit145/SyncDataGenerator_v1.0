# Unified Data Runbook

This runbook describes the current commands and storage layout.

## Commands

Normal generation:

```powershell
.\venv\Scripts\python.exe .\main.py
```

Normal generation with optional source-specific outputs:

```powershell
.\venv\Scripts\python.exe .\main.py --include-new-outputs-src
```

Raw to silver:

```powershell
.\venv\Scripts\python.exe .\misc\transform_all_raw_to_silver.py
```

Silver verification:

```powershell
.\venv\Scripts\python.exe .\misc\verify_all_silver.py
```

Churn KPI validation:

```powershell
.\venv\Scripts\python.exe .\validate_churn_kpis.py
```

Large base-only generation:

```powershell
.\venv\Scripts\python.exe .\main.py --streaming-base --total-people 10000000 --chunk-size 100000
```

## Storage Layout

Default folders:

- `data/output/<run_id>`
- `data/synthetic/base/<run_id>`
- `data/synthetic/enhanced/<run_id>`
- `data/raw/crm/<run_id>`
- `data/raw/crm_canonical/<run_id>`
- `data/raw/api/<run_id>`
- `data/raw/claims/<run_id>`
- `data/raw/claims_canonical/<run_id>`
- `data/raw/data_source/motor/<run_id>`
- `data/raw/data_source/home/<run_id>`
- `data/raw/data_source_canonical/<run_id>`
- `data/silver/rebuild/<run_id>`
- `data/silver/api/<run_id>`
- `data/silver/claims/<run_id>`
- `data/silver/data_source/<run_id>`
- `data/scd2/base/<run_id>`
- `data/scd2/enhanced/<run_id>`
- `data/scd2/raw/crm/<run_id>`
- `data/scd2/raw/api/<run_id>`

Optional `new_outputs_src` folders, only when requested:

- `data/new_outputs_src/<source>/data/<run_id>`
- `data/new_outputs_src/<source>/scd2/<run_id>`

## SCD2 Flow

SCD2 is generated only when prior comparable runs exist.

- base synthetic SCD2: `data/scd2/base/<run_id>`
- enhanced SCD2: `data/scd2/enhanced/<run_id>`
- raw CRM/API SCD2: `data/scd2/raw/<source>/<run_id>`
- optional source-specific SCD2: `data/new_outputs_src/<source>/scd2/<run_id>`

## Churn And MLOps Rules

Current churn alignment:

- churn distributions are configured in `config/scenario_v1.json` under `churn_settings`
- config key meanings are documented in `docs/scenario_config_reference.md`
- `Policy Cycle` is completed annual tenure.
- Higher `Policy Cycle` produces lower churn.
- `Policy Cycle` is not number of policies/products held.
- Multi-product ownership is a separate concept and should not be inferred from `Policy Cycle`.
- `AGENT` carries broker/aggregator-like higher churn behavior using existing channel values.
- `AGGREGATOR` is not emitted.
- Policy dates and status rules remain valid for `ACTIVE`, `LAPSED`, and `CANCELLED`.

Validation tools:

- `validate_churn_kpis.py` checks base churn fields and directionality.
- `verify_csv.py` checks full silver relationship, timeline, policy, and churn rules.
- `verify_all_silver.py` runs `verify_csv.py` across latest silver outputs.
