# Copilot Project Context

This repo generates synthetic insurance data and now includes an ODCS v3.1.0 data-contract workflow. Keep changes consistent with the existing Python scripts and YAML config style.

## Data Contract Goal

For each `main.py` run, when the relevant raw data is generated, the project should also generate and verify data contracts for:

- Claims raw PRD1
- Claims raw PRD2
- Policy raw PRD1
- Policy raw PRD2
- Enhanced raw PRD1
- Enhanced raw PRD2

The intended automated sequence is:

```text
main.py
  -> generate raw data
  -> generate ODCS YAML contract
  -> verify YAML contract against source CSV files
  -> export official ODCS Excel workbook
  -> run datacontract import excel to convert Excel back to YAML
  -> run datacontract lint on the Excel-imported YAML
  -> print all inputs, outputs, reports, and pass/fail status
```

## Important Files

- `main.py`
  - Calls `run_data_contracts(...)` unless `--skip-data-contracts` is supplied.
  - Prints data-contract input, YAML, Excel, imported YAML, report, and verification status.

- `data_contract/contract_run_config.yaml`
  - Main runtime config for contract generation.
  - Controls enabled contracts, input references, output YAML paths, report paths, and Excel round-trip settings.

- `data_contract/tools/create_data_contract.py`
  - Creates source-of-truth ODCS YAML from a folder of CSV files plus YAML/CSV rule config.
  - Supports table rules, column rules, primary keys, business keys, source table/key metadata, and `allowNoPrimaryKey`.

- `data_contract/tools/verify_data_contract.py`
  - Verifies ODCS YAML structure, source files, schema columns, quality rule references, notebook metadata, and key/null/duplicate checks.

- `data_contract/tools/run_data_contracts.py`
  - Orchestrates generation, verification, Excel export, Excel import, and CLI lint.
  - Uses `datacontract import excel --source <xlsx> --output <yaml>` and `datacontract lint <yaml>`.

- `data_contract/tools/export_odcs_excel.py`
  - Exports ODCS YAML into the official ODCS Excel template shape.
  - Creates one `Schema <table_name>` sheet per table/source object.
  - Recreates sheet-local named ranges because the Data Contract CLI Excel importer depends on them.

- `dc_nb/odcs-template.xlsx`
  - Official-style ODCS Excel template used for Excel export.

- `data_contract/notebooks/validate_data_contract.py`
  - Databricks notebook source for executing contracts against files/tables/Postgres-style inputs.

- `dc_nb/`
  - Portable Databricks notebook examples for template creation, validation, contract creation, and execution.

## Output Locations

Generated runtime artifacts are intentionally not source-controlled:

```text
data/raw/...                         # generated source data
data_contract/generated/...          # source-of-truth generated ODCS YAML
data_contract/reports/...            # verifier JSON reports
data_contract/gen_excels/.../*.xlsx  # generated ODCS Excel workbooks
data_contract/gen_excels/.../*_imported.yaml
```

The executable contract source of truth is:

```text
data_contract/generated/<domain>/raw/<source>/<contract>_ODCS.yaml
```

The Excel-imported YAML under `data_contract/gen_excels/` is only a round-trip compatibility artifact for Data Contract CLI import/lint.

## Current Configured Contracts

`data_contract/contract_run_config.yaml` currently enables:

```text
claims_raw_prd01
claims_raw_prd02
policy_raw_prd01
policy_raw_prd02
enhanced_raw_prd01
enhanced_raw_prd02
```

Policy source behavior:

- PRD1 acts as CRM and contains the broader CRM-style raw policy source feed.
- PRD2 acts as SAP and should only contain:
  - `sap_policy.csv`
  - `sap_policy_coverage.csv`
  - `sap_coverage.csv`

Policy raw generation is implemented in:

```text
generators/raw_policy_generator.py
```

## ODCS Excel Notes

The official ODCS Excel template expects one schema sheet per table:

```text
Schema account_book
Schema claim_register
Schema sap_policy
```

Do not replace this with one combined `Schema` sheet if the workbook must be compatible with:

```powershell
datacontract import excel --source <xlsx> --output <yaml>
```

Excel sheet names have a 31-character limit. For long table names, the sheet title may be shortened, but the full ODCS schema name must remain in cell `B5`.

The current Data Contract CLI Excel importer writes library quality checks as `rule`, while ODCS lint expects `metric`. To keep Excel import/lint passing, `export_odcs_excel.py` converts library checks into equivalent SQL checks in the Excel workbook. The source-of-truth generated ODCS YAML still keeps the original library quality metrics.

## Commands

Run normal generation:

```powershell
python main.py
```

Skip contracts:

```powershell
python main.py --skip-data-contracts
```

Use a different contract config:

```powershell
python main.py --data-contract-config data_contract/contract_run_config.yaml
```

Manual Excel import/lint example:

```powershell
datacontract import excel --source data_contract\gen_excels\enhanced\raw\prd_01\enhanced_prd_01_raw_ODCS.xlsx --output data_contract\gen_excels\enhanced\raw\prd_01\enhanced_prd_01_raw_imported.yaml
datacontract lint data_contract\gen_excels\enhanced\raw\prd_01\enhanced_prd_01_raw_imported.yaml
```

Install the CLI if needed:

```powershell
pip install datacontract-cli
```

## Development Rules

- Keep YAML under `data_contract/generated/` as generated output, not hand-authored source.
- Update source configs under `data_contract/tools/*_contract_config.yaml` for keys/rules.
- For layer-level C360 metadata, use inputs under `data_contract/inputs/` and generated configs under `data_contract/tools/bronze_c360_contract_config.yaml`, `silver_c360_contract_config.yaml`, and `gold_c360_contract_config.yaml`.
- Do not store secrets in contracts. Use Databricks secrets or environment-specific secret management.
- Keep Databricks notebooks self-contained where intended. `dc_nb/create_data_contract_databricks.py` and `dc_nb/execute_data_contract.py` should run in Databricks without importing repo-local Python modules.
- Generated folders are ignored in `.gitignore`; commit code/config/template changes, not generated data or generated contracts unless explicitly requested.

## Last Verified Round Trip

The Excel export, `datacontract import excel`, and `datacontract lint` round-trip was verified for:

```text
claims_raw_prd01      pass
claims_raw_prd02      pass
policy_raw_prd01      pass
policy_raw_prd02      pass
enhanced_raw_prd01    pass
enhanced_raw_prd02    pass
```
