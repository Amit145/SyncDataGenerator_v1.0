# Data Contracts

This folder stores ODCS v3.1.0 data contracts for source and pipeline interfaces.

A data contract is the agreed definition of what a dataset must look like before a pipeline trusts it. In this repo, a contract captures expected files or tables, columns, data types, required fields, primary/business keys, quality rules, SLA, ownership, support, and notebook-readable DQ metadata.

## Current Scope

The current implemented contracts cover raw Claims source feeds:

```text
data_contract/claims/raw/prd_01/claims_prd_01_raw_ODCS.yaml
data_contract/claims/raw/prd_02/claims_prd_02_raw_ODCS.yaml
```

Both use the source-feed pattern: one contract file describes all files delivered by one source feed.

PRD1 covers the full 23-file claims LDM raw source feed under:

```text
data/raw/claims/<run_id>/prd_01
```

PRD2 covers the SAP/source-2 claims feed under:

```text
data/raw/claims/<run_id>/prd_02
```

PRD2 currently includes:

- `claim.csv`
- `claim_investigation.csv`
- `loss_event.csv`

## Folder Layout

```text
data_contract/
  README.md
  odcs-v3.1.0.schema_ODCS.json
  data_contract_template_ODCS.yaml
  data_contract_example_ODCS.yaml
  new 2.txt
  Data Contract Workstreams and Required Tasks.docx
  claims/
    raw/
      prd_01/
        claims_prd_01_raw_ODCS.yaml
      prd_02/
        claims_prd_02_raw_ODCS.yaml
  tools/
    create_data_contract.py
    verify_data_contract.py
    contract_config_example.yaml
    layer_rule_config_examples.yaml
```

## Contract Inputs

To create a contract, use these inputs:

- Physical data files or tables, for example `data/raw/claims/<run_id>/prd_01/*.csv`.
- Domain, for example `claims`.
- Layer, for example `raw`.
- Source feed, for example `prd_01` or `prd_02`.
- Source system, for example `Claims LDM source-1` or `SAP`.
- Primary/business key overrides when inference is not enough.
- ETL rule file, currently `data_contract/new 2.txt`.
- ODCS schema, currently `data_contract/odcs-v3.1.0.schema_ODCS.json`.

The optional override file is:

```text
data_contract/tools/contract_config_example.yaml
```

Use that file to control keys, table names, business names, grain, SLA, approvers, support, and other governance metadata.

## Quick Start

Use this sequence for a new contract:

```text
1. Create or update a config file with key overrides and custom rules.
2. Run create_data_contract.py against the source folder.
3. Run verify_data_contract.py against the generated contract and source folder.
4. Review the generated YAML with data modelling, DQ, and business owners.
5. Deploy the approved YAML to Databricks/Unity Catalog Volume.
6. Run validate_data_contract.py before ingestion.
7. Ingest only when the notebook validation passes.
8. Optionally run validate_data_contract.py again after PGSQL/table insertion.
```

Custom DQ rules can also be added in the config before generating a contract.

Layer-specific examples are available here:

```text
data_contract/tools/layer_rule_config_examples.yaml
```

Use table-level rules for checks across a full file/table:

```yaml
tables:
  claim_register.csv:
    primaryKey: claim_identifier
    businessKey: claim_identifier
    tableRules:
      - ruleId: dq_claim_register_amounts_non_negative
        ruleName: Claim amounts non-negative
        type: sql
        query: >
          SELECT COUNT(*) AS invalid_amounts
          FROM {full_table_name}
          WHERE claim_requested_amount < 0
            AND {target_watermak_exp}
        mustBe:
          invalid_amounts: 0
        dimension: accuracy
        severity: error
        description: "Claim requested amount must not be negative."
        referencedColumns:
          - claim_requested_amount
        failureAction: reject
```

Use column-level rules for checks scoped to one field:

```yaml
tables:
  claim_register.csv:
    columns:
      claim_status:
        required: true
        allowedValues:
          - Open
          - Closed
          - Reopened
      claim_requested_amount:
        required: true
        minValue: 0
        columnRules:
          - ruleId: dq_claim_requested_amount_not_null
            ruleName: Claim requested amount not null
            type: library
            metric: nullValues
            mustBe: 0
            dimension: completeness
            severity: error
            description: "claim_requested_amount must be populated."
            failureAction: reject
```

The generator writes these into ODCS `quality` blocks. Framework-only fields such as `ruleId`, `ruleName`, `failureAction`, and `referencedColumns` are converted to ODCS-compatible fields or `customProperties`, so the generated YAML still validates against the official ODCS schema.

## Create A Contract

Use `create_data_contract.py` to generate a draft ODCS YAML from a folder of CSV files.

Command syntax:

```powershell
.\venv\Scripts\python.exe .\data_contract\tools\create_data_contract.py `
  --input-path <folder-containing-csv-files> `
  --domain <business-domain> `
  --layer <raw|bronze|silver|gold> `
  --source-feed <source-feed-name> `
  --source-system "<source-system-name>" `
  --contract-id <unique-contract-id> `
  --contract-name "<human-readable-contract-name>" `
  --config <optional-config-yaml> `
  --output <output-contract-yaml>
```

Important arguments:

- `--input-path`: folder with the CSV files to describe.
- `--domain`: business domain, for example `claims`.
- `--layer`: pipeline layer, for example `raw`, `bronze`, `silver`, or `gold`.
- `--source-feed`: source feed name, for example `prd_01`.
- `--source-system`: source system name, for example `SAP`.
- `--contract-id`: stable unique ID for the contract.
- `--contract-name`: readable display name.
- `--config`: optional YAML file with key overrides and custom rules.
- `--output`: generated contract path.

Example for Claims PRD1:

```powershell
.\venv\Scripts\python.exe .\data_contract\tools\create_data_contract.py `
  --input-path .\data\raw\claims\20260730184046\prd_01 `
  --domain claims `
  --layer raw `
  --source-feed prd_01 `
  --source-system "Claims LDM source-1" `
  --contract-id claims-prd-01-raw-0001 `
  --contract-name "Claims PRD 01 Raw Source Feed" `
  --config .\data_contract\tools\contract_config_example.yaml `
  --output .\data_contract\claims\raw\prd_01\claims_prd_01_raw_ODCS.yaml
```

Example for Claims PRD2:

```powershell
.\venv\Scripts\python.exe .\data_contract\tools\create_data_contract.py `
  --input-path .\data\raw\claims\20260730184046\prd_02 `
  --domain claims `
  --layer raw `
  --source-feed prd_02 `
  --source-system "SAP" `
  --contract-id claims-prd-02-raw-0001 `
  --contract-name "Claims PRD 02 Raw Source Feed" `
  --config .\data_contract\tools\contract_config_example.yaml `
  --output .\data_contract\claims\raw\prd_02\claims_prd_02_raw_ODCS.yaml
```

The creator:

- reads CSV headers and profiles values
- infers logical and physical types
- infers required fields from null profiling
- infers likely primary keys
- applies key overrides from config when provided
- adds ODCS `schema[].quality` checks
- adds `notebookValidationConfig`
- adds `dqRuleCatalog`
- validates the generated YAML against the ODCS schema before writing

The generated contract is a draft. Review keys, data classifications, descriptions, SLA, and rule applicability before approval.

## Verify A Contract

Use `verify_data_contract.py` to check an existing contract against ODCS, physical files, notebook metadata, and primary keys.

Command syntax:

```powershell
.\venv\Scripts\python.exe .\data_contract\tools\verify_data_contract.py `
  --contract <contract-yaml-path> `
  --input-path <folder-containing-csv-files> `
  --check-keys `
  --report <optional-json-report-path>
```

Important arguments:

- `--contract`: YAML contract to verify.
- `--input-path`: folder containing the physical CSV files.
- `--check-keys`: also checks primary key nulls and duplicate keys.
- `--report`: optional JSON output report.

Example for Claims PRD1:

```powershell
.\venv\Scripts\python.exe .\data_contract\tools\verify_data_contract.py `
  --contract .\data_contract\claims\raw\prd_01\claims_prd_01_raw_ODCS.yaml `
  --input-path .\data\raw\claims\20260730184046\prd_01 `
  --check-keys
```

Example for Claims PRD2:

```powershell
.\venv\Scripts\python.exe .\data_contract\tools\verify_data_contract.py `
  --contract .\data_contract\claims\raw\prd_02\claims_prd_02_raw_ODCS.yaml `
  --input-path .\data\raw\claims\20260730184046\prd_02 `
  --check-keys
```

The verifier checks:

- YAML can be parsed
- contract follows ODCS v3.1.0 schema
- mandatory sections are present
- expected files exist
- contract columns match CSV headers
- duplicate contract objects/columns are not present
- `notebookValidationConfig` is readable
- `dqRuleCatalog` is readable
- primary keys have no nulls or duplicates when `--check-keys` is passed

You can also write a JSON report:

```powershell
.\venv\Scripts\python.exe .\data_contract\tools\verify_data_contract.py `
  --contract .\data_contract\claims\raw\prd_02\claims_prd_02_raw_ODCS.yaml `
  --input-path .\data\raw\claims\20260730184046\prd_02 `
  --check-keys `
  --report .\data_contract\reports\claims_prd_02_contract_validation.json
```

## Rule Handling

The source ETL rule file is:

```text
data_contract/new 2.txt
```

For raw source validation, these rules are active:

- `rule_0001` BK Check
- `rule_0003` Not Null Check
- `rule_0007` BK Not Null Check

These rules validate that primary/business keys are populated and unique.

These rules are retained as inactive metadata for raw contracts:

- `rule_0002` Missing Keys Check
- `rule_0004` SCD2 Effective Dates Check
- `rule_0005` Overlapping Period Check
- `rule_0006` Duplicate Active Records Check

`rule_0002` needs source-to-target reconciliation, so use it after ingestion or during raw-to-bronze validation. `rule_0004`, `rule_0005`, and `rule_0006` need SCD2 effective dating columns, which raw PRD1 and PRD2 files do not contain.

## Notebook Validation Design

The Databricks notebook-style validator is:

```text
data_contract/notebooks/validate_data_contract.py
```

Import this file into Databricks as a Python notebook, or keep it in a Databricks Repo and run it as a job task. The notebook accepts the contract as an input and runs all applicable tests on the data.

Recommended notebook inputs:

```text
contract_path
input_base_path
batch_ref
mode
run_mode
```

Notebook widget meaning:

- `contract_path`: YAML contract path.
- `input_base_path`: folder containing files for `file` mode, or optional table namespace helper.
- `batch_ref`: batch/run ID used to render `{target_watermak_exp}`.
- `mode`: `file`, `table`, or `postgres`.
- `run_mode`: `report_only` prints failures but does not fail the job; `fail_fast` raises an exception when any rule fails.
- `result_table`: optional Delta table to append validation results.
- `table_prefix`: table schema/prefix for `table` or `postgres` mode, for example `raw_claims`.
- `jdbc_url`: JDBC URL for `postgres` mode.
- `jdbc_user`: JDBC user for `postgres` mode.
- `jdbc_password_secret_scope` and `jdbc_password_secret_key`: Databricks secret location for the PGSQL password.

Example file-mode inputs:

```text
contract_path = /Volumes/governance/contracts/claims/raw/prd_02/claims_prd_02_raw_ODCS.yaml
input_base_path = /Volumes/raw/claims/prd_02/batch_ref=20260730184046
batch_ref = 20260730184046
mode = file
run_mode = fail_fast
```

Example local repo file-mode inputs for this project:

```text
contract_path = /Workspace/Repos/<repo>/data_contract/claims/raw/prd_02/claims_prd_02_raw_ODCS.yaml
input_base_path = /Workspace/Repos/<repo>/data/raw/claims/20260730184046/prd_02
batch_ref = 20260730184046
mode = file
run_mode = report_only
```

Example PGSQL post-insert mode inputs:

```text
contract_path = /Volumes/governance/contracts/claims/raw/prd_01/claims_prd_01_raw_ODCS.yaml
mode = postgres
table_prefix = raw_claims
jdbc_url = jdbc:postgresql://<host>:5432/<database>
jdbc_user = <user>
jdbc_password_secret_scope = <secret_scope>
jdbc_password_secret_key = <secret_key>
batch_ref = 20260730184046
run_mode = fail_fast
```

The notebook should:

- read the YAML contract
- validate required contract sections
- read `schema[]` objects
- read `customProperties.notebookValidationConfig`
- read `customProperties.dqRuleCatalog`
- load each source file or table
- check expected files/tables exist
- check required columns exist
- check required fields are not null
- check primary/business keys are not null
- check primary/business keys are unique
- run active `schema[].quality` SQL checks
- skip inactive rules unless the required context is provided
- write validation results to a Delta table
- fail the job when critical/error rules fail and `run_mode = fail_fast`

Recommended validation result table:

```text
governance.dq_contract_execution_log
```

Recommended columns:

```text
validation_run_id
contract_id
contract_version
contract_path
domain
layer
source_feed
entity
physical_name
rule_id
rule_name
rule_type
severity
expected_value
actual_value
status
failure_action
error_message
batch_ref
validation_ts
```

Optional exception table:

```text
governance.dq_contract_exception_log
```

## Databricks Usage

Keep Git as the source of truth. For Databricks runtime usage, deploy a copy of the contract to a Unity Catalog Volume.

Recommended contract location:

```text
/Volumes/governance/contracts/claims/raw/prd_01/claims_prd_01_raw_ODCS.yaml
/Volumes/governance/contracts/claims/raw/prd_02/claims_prd_02_raw_ODCS.yaml
```

Recommended raw landing locations:

```text
/Volumes/raw/claims/prd_01/batch_ref=<run_id>/*.csv
/Volumes/raw/claims/prd_02/batch_ref=<run_id>/*.csv
```

Databricks should not use local Windows paths such as `F:\...`. Use Unity Catalog Volumes, DBFS paths, or table names.

The notebook may need `PyYAML` on the cluster:

```python
%pip install PyYAML
dbutils.library.restartPython()
```

For each contract object, the notebook can read CSV data and register a temp view:

```python
df = spark.read.option("header", True).csv(file_path)
df.createOrReplaceTempView(entity_name)
```

Then SQL quality rules can replace:

```text
{full_table_name}
{table_pk}
{table_bk}
{target_watermak_exp}
```

and execute with:

```python
spark.sql(rendered_query)
```

## PGSQL Ingestion Validation

Use the same contract as a gate before and after PGSQL insertion.

Recommended flow:

```text
Raw CSV files
  -> validate with contract in file mode
  -> insert into PGSQL raw/staging tables only if validation passes
  -> validate PGSQL tables with the same contract in postgres mode
  -> reconcile CSV source to PGSQL target counts and keys
  -> continue bronze/raw-vault processing only if validation passes
```

Before ingestion, validate:

- file exists
- expected columns exist
- required fields are populated
- primary/business key is populated
- primary/business key is unique
- row count is greater than zero

After PGSQL insertion, validate:

- target PGSQL table exists
- target columns match the contract
- target required fields are populated
- target primary/business key is populated
- target primary/business key is unique
- source CSV row count equals PGSQL row count
- source distinct key count equals PGSQL distinct key count
- source keys missing in PGSQL equals zero

This is where `rule_0002` Missing Keys Check becomes active, because both source and target context are available.

PGSQL passwords should not be stored in contracts. Use Databricks secrets or environment-specific secret management.

## Contract Governance

Contract changes should be reviewed with the same discipline as pipeline changes. Breaking changes include removing a file, removing or renaming a field, changing a required field to a different meaning, changing key behavior, or changing a data type in a way that downstream consumers cannot handle.

Version contracts semantically:

- Patch: documentation or non-behavioral metadata changes.
- Minor: backward-compatible additions, such as a new optional field.
- Major: breaking changes, such as removing or renaming a required field.

Recommended ownership:

- Data Engineering CoE owns contract generation and validation tooling.
- Data Modelling CoE reviews keys, grain, mappings, and model alignment.
- DQ team reviews rules, thresholds, severity, and failure actions.
- Business/product owner approves SLA, business meaning, and breaking changes.
