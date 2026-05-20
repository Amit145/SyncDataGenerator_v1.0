# Current Run README

This runbook captures the current operational flow for base, enhanced, raw, silver, churn validation, and SCD2 generation.

Run all commands from:

```powershell
cd F:\SyncDataGenerator_v1.0
```

## Base Load

Run:

```powershell
.\venv\Scripts\python.exe .\main.py
```

Default outputs:

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
- `data/silver/api/<run_id>`
- `data/scd2/base/<run_id>` when a prior base run exists
- `data/scd2/enhanced/<run_id>` when a prior enhanced run exists
- `data/scd2/raw/crm/<run_id>` and `data/scd2/raw/api/<run_id>` when prior raw batches exist

Optional source-specific outputs:

```powershell
.\venv\Scripts\python.exe .\main.py --include-new-outputs-src
```

This adds:

- `data/new_outputs_src/<source>/data/<run_id>`
- `data/new_outputs_src/<source>/scd2/<run_id>` when prior source batches exist

## Silver And Verification

Transform latest raw sources to silver:

```powershell
.\venv\Scripts\python.exe .\misc\transform_all_raw_to_silver.py
```

Verify latest silver outputs:

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

## Churn Rules

The current churn behavior follows `new_rules/Data Req Churn NPS.xlsx` for available and proxy rows.

Tune churn distributions in `config/scenario_v1.json` under `churn_settings`.

Key points:

- `Policy Cycle` means completed annual tenure from policy start date to load/snapshot date.
- `Policy Cycle` does not mean number of policies purchased.
- Churn decreases as completed `Policy Cycle` increases.
- Sales-channel variance uses existing values only: `AGENT` carries broker/aggregator-like higher churn behavior; `AGGREGATOR` is not emitted.
- Renewal premium movement, claim counts, add-ons, marketing proxy, driver experience, and vehicle model segments are generated and validated.

Policy date rules:

- `Policy Start Date <= Policy End Date`
- `Renewal Date` is within 0 to 10 days before `Policy End Date`
- `ACTIVE`, `LAPSED`, and `CANCELLED` statuses are date-consistent
- long-tenure active/lapsed policies use current annual term boundaries
- `LAPSED` requires a completed renewal cycle; sub-one-year churn is `CANCELLED`

## Recommended Complete Check

For a normal full check:

```powershell
.\venv\Scripts\python.exe .\main.py
.\venv\Scripts\python.exe .\validate_churn_kpis.py
.\venv\Scripts\python.exe .\misc\transform_all_raw_to_silver.py
.\venv\Scripts\python.exe .\misc\verify_all_silver.py
.\venv\Scripts\python.exe .\misc\compare_all_scd2.py
```

`compare_all_scd2.py` needs prior SCD2 outputs; it may report no comparison data on a first run.
