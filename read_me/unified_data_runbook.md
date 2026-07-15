# Unified Data Runbook

Detailed current rules are documented in:

- `docs/current_rules_reference.md`
- `docs/scenario_config_reference.md`
- `docs/llm_codebase_context.md`

## Commands

Normal generation:

```powershell
.\venv\Scripts\python.exe .\main.py
```

Normal generation keeps `data/output/<run_id>`. Remove that intermediate folder only when explicitly requested:

```powershell
.\venv\Scripts\python.exe .\main.py --remove-working-output
```

Legacy raw/silver generation:

```powershell
.\venv\Scripts\python.exe .\main.py --include-raw-silver
```

Legacy source-specific output generation:

```powershell
.\venv\Scripts\python.exe .\main.py --include-raw-silver --include-new-outputs-src
```

Claims product generation:

```powershell
.\venv\Scripts\python.exe .\main.py --include-claims-product
```

Claims product creates a two-source claims raw split and a vault-ready consolidation:

- `data/raw/claims/<run_id>/raw`
- `data/raw/claims/<run_id>/prd_01`
- `data/raw/claims/<run_id>/prd_02`
- `data/raw/claims/<run_id>/raw_vault`
- `data/bronze/claims/<run_id>`
- `data/silver/claims/<run_id>`
- `data/gold/claims/<run_id>`

`prd_01` is CRM/source-1, `prd_02` is SAP/source-2, and `raw_vault` is the consolidated 23-file claims LDM input used by the claims vault builder.

Large base-only streaming generation:

```powershell
.\venv\Scripts\python.exe .\main.py --streaming-base --total-people 10000000 --chunk-size 100000
```

## Storage Layout

Default synthetic and raw/silver folders:

- `data/output/<run_id>`
- `data/synthetic/base/<run_id>`
- `data/synthetic/enhanced/<run_id>`
- `data/synthetic/mlops/<run_id>`
- `data/raw/base/prd_01/<run_id>`
- `data/raw/enhanced/prd_01/<run_id>`
- `data/raw/enhanced/prd_02/<run_id>`
- `data/raw/enhanced/raw_vault/<run_id>`
- `data/raw/mlops/prd_01/<run_id>`
- `data/raw/mlops/prd_02/<run_id>`
- `data/silver/base/<run_id>`
- `data/silver/enhanced/<run_id>`
- `data/silver/mlops/<run_id>`
- `data/scd2/base/<run_id>`
- `data/scd2/enhanced/<run_id>`
- `data/scd2/mlops/<run_id>`

Optional legacy folders, only when requested:

- `data/raw/crm/<run_id>`
- `data/raw/crm_canonical/<run_id>`
- `data/raw/api/<run_id>`
- `data/raw/claims/<run_id>/raw`
- `data/raw/claims/<run_id>/prd_01`
- `data/raw/claims/<run_id>/prd_02`
- `data/raw/claims/<run_id>/raw_vault`
- `data/raw/claims_canonical/<run_id>`
- `data/raw/data_source/<source>/<run_id>`
- `data/raw/data_source_canonical/<run_id>`
- `data/silver/api/<run_id>`
- `data/silver/claims/<run_id>`
- `data/silver/data_source/<run_id>`
- `data/scd2/raw/crm/<run_id>`
- `data/scd2/raw/api/<run_id>`

## Verification

Base:

```powershell
.\venv\Scripts\python.exe .\verify_csv.py .\data\synthetic\base\<run_id>
```

Enhanced:

```powershell
.\venv\Scripts\python.exe .\misc\verify_enhanced_synthetic.py .\data\synthetic\enhanced\<run_id>
```

Claims product:

```powershell
.\venv\Scripts\python.exe .\misc\verify_claims_two_source_raw.py --run-id <run_id>
.\venv\Scripts\python.exe .\misc\verify_claims_product.py --run-id <run_id>
```

MLOps:

```powershell
.\venv\Scripts\python.exe .\misc\verify_mlops_synthetic.py .\data\synthetic\mlops\<run_id>
```

PRD raw:

```powershell
.\venv\Scripts\python.exe .\misc\verify_prd_raw_mlops.py --mode base --run-id <run_id>
.\venv\Scripts\python.exe .\misc\verify_prd_raw_mlops.py --mode enhanced --run-id <run_id>
.\venv\Scripts\python.exe .\misc\verify_prd_raw_mlops.py --mode mlops --run-id <run_id>
```

## SCD2 Flow

SCD2 is generated only when a prior comparable synthetic run exists.

- base SCD2: `data/scd2/base/<run_id>`
- enhanced SCD2: `data/scd2/enhanced/<run_id>`
- MLOps SCD2: `data/scd2/mlops/<run_id>`

SCD2 preserves the same PK/FK, date, churn, NPS, claim, complaint, and MLOps enrichment logic as the synthetic mode that produced it.
