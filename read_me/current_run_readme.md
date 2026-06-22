# Current Run README

This runbook captures the current operational flow for base, enhanced, MLOps, PRD raw, silver rebuilds, churn/NPS validation, and SCD2 generation.

Detailed rules are maintained in:

- `docs/current_rules_reference.md`
- `docs/scenario_config_reference.md`
- `docs/llm_codebase_context.md`
- `docs/latest_run_validation.md`

Run all commands from:

```powershell
cd F:\SyncDataGenerator_v1.0
```

## Main Generation

Run:

```powershell
.\venv\Scripts\python.exe .\main.py
```

Default outputs:

- `data/output/<run_id>` intermediate base build, kept by default
- `data/synthetic/base/<run_id>`
- `data/synthetic/enhanced/<run_id>`
- `data/synthetic/mlops/<run_id>`
- `data/raw/base/prd_01/<run_id>`
- `data/raw/enhanced/prd_01/<run_id>`
- `data/raw/enhanced/prd_02/<run_id>`
- `data/raw/mlops/prd_01/<run_id>`
- `data/raw/mlops/prd_02/<run_id>`
- `data/silver/base/<run_id>`
- `data/silver/enhanced/<run_id>`
- `data/silver/mlops/<run_id>`
- `data/scd2/base/<run_id>` when prior base history exists
- `data/scd2/enhanced/<run_id>` when prior enhanced history exists
- `data/scd2/mlops/<run_id>` when prior MLOps history exists

Remove the intermediate `data/output/<run_id>` folder only when needed:

```powershell
.\venv\Scripts\python.exe .\main.py --remove-working-output
```

## Optional Legacy Outputs

Legacy raw CRM/API/claims/data_source, canonical raw, API silver, and raw SCD2 outputs are disabled by default.

```powershell
.\venv\Scripts\python.exe .\main.py --include-raw-silver
```

`new_outputs_src` is also disabled by default and requires both flags:

```powershell
.\venv\Scripts\python.exe .\main.py --include-raw-silver --include-new-outputs-src
```

## Verification

Validate base synthetic:

```powershell
.\venv\Scripts\python.exe .\verify_csv.py .\data\synthetic\base\<run_id>
```

Validate enhanced synthetic:

```powershell
.\venv\Scripts\python.exe .\misc\verify_enhanced_synthetic.py .\data\synthetic\enhanced\<run_id>
```

Validate MLOps synthetic:

```powershell
.\venv\Scripts\python.exe .\misc\verify_mlops_synthetic.py .\data\synthetic\mlops\<run_id>
```

Validate PRD raw structure:

```powershell
.\venv\Scripts\python.exe .\misc\verify_prd_raw_mlops.py --mode base --run-id <run_id>
.\venv\Scripts\python.exe .\misc\verify_prd_raw_mlops.py --mode enhanced --run-id <run_id>
.\venv\Scripts\python.exe .\misc\verify_prd_raw_mlops.py --mode mlops --run-id <run_id>
```

Validate churn and NPS rules:

```powershell
.\venv\Scripts\python.exe .\validate_churn_kpis.py --path .\data\synthetic\base\<run_id>
.\venv\Scripts\python.exe .\misc\verify_nps_features.py .\data\synthetic\mlops\<run_id>
```

## Current Rule Scope

The current data flow preserves:

- base, enhanced, and MLOps vault table structure
- PRD raw to silver rebuild compatibility
- PK/FK referential integrity
- policy issue/start/end/renewal date rules
- churn workbook ratios for available/proxy KPIs where supported by the model
- NPS workbook ratios for available/proxy KPIs where supported by the model
- claim amount, outstanding amount, claim band, claim satisfaction, and complaint feedback rules
- SCD2 history generation from prior comparable synthetic runs
