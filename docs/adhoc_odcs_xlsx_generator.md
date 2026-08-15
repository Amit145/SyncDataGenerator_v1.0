# Ad Hoc ODCS Excel Generator

This guide explains how to use the root-level script:

```text
create_odcs_xlsx_from_csv_folder.py
```

The script creates an ODCS-style Excel data contract from either:

- a single CSV file
- a folder/volume containing CSV files
- a Databricks `catalog.schema` using `information_schema`

It uses the approved Excel template:

```text
dimdc/odcs-template-updated.xlsx
```

The output workbook can be converted to YAML with `datacontract import excel` and validated with `datacontract lint`.

## What It Creates

For each input table, the script creates one schema sheet:

```text
Schema <table_name>
```

Each schema sheet includes:

- table name
- physical name
- description
- data granularity
- tags
- column/property list
- inferred logical type
- inferred physical type
- required flag
- unique flag
- classification
- primary key tag when a key is inferred

The script also populates the `Quality` sheet.

By default, it adds:

- table-level row count check
- primary key null check
- primary key duplicate check
- custom rule references from `dimdc/rules.csv`

Default custom rules:

```text
rule_0001
rule_0007
```

The Excel does **not** embed SQL. SQL remains in the rule catalog:

```text
dimdc/rules.csv
```

## Input Modes

### 1. CSV Mode

Use this mode for either:

- one CSV file
- one folder containing multiple CSV files

Each CSV becomes one schema/table in the generated ODCS Excel.

#### Single CSV

```powershell
python create_odcs_xlsx_from_csv_folder.py `
  --input-mode csv `
  --input-path data\raw\policy\20260813095628\prd_02\policy.csv `
  --output dimdc\outputs\policy_single_adhoc_odcs.xlsx `
  --contract-name "Policy Single CSV Adhoc ODCS" `
  --domain policy `
  --data-product raw_policy_single
```

#### Folder Of CSV Files

```powershell
python create_odcs_xlsx_from_csv_folder.py `
  --input-mode csv `
  --input-path data\raw\policy\20260813095628\prd_02 `
  --output dimdc\outputs\policy_prd02_adhoc_odcs.xlsx `
  --contract-name "Policy PRD02 Adhoc ODCS" `
  --domain policy `
  --data-product raw_policy_prd02
```

## 2. Catalog.Schema Mode

Use this mode when the source is already in Databricks and you want the script to read table and column metadata from `information_schema`.

Input is passed as:

```text
catalog.schema
```

Example:

```powershell
python create_odcs_xlsx_from_csv_folder.py `
  --input-mode catalog-schema `
  --input-path dev_allianz_raw.bvault_gold `
  --output dimdc\outputs\bvault_gold_odcs.xlsx `
  --contract-name "BVault Gold ODCS" `
  --domain insurance `
  --data-product bvault_gold
```

You can filter specific tables:

```powershell
python create_odcs_xlsx_from_csv_folder.py `
  --input-mode catalog-schema `
  --input-path dev_allianz_raw.bvault_gold `
  --table-names dim_policy,dim_person,fact_policy `
  --output dimdc\outputs\bvault_gold_selected_odcs.xlsx
```

`information-schema` is still accepted as a backward-compatible alias, but prefer:

```text
catalog-schema
```

## Databricks Connection

Catalog-schema mode requires:

```text
databricks-sql-connector
```

It is listed in:

```text
requirements.txt
```

Install dependencies if needed:

```powershell
pip install -r requirements.txt
```

Connection can be supplied using command-line arguments:

```powershell
python create_odcs_xlsx_from_csv_folder.py `
  --input-mode catalog-schema `
  --input-path dev_allianz_raw.bvault_gold `
  --databricks-server-hostname "<server-hostname>" `
  --databricks-http-path "<sql-warehouse-http-path>" `
  --databricks-token "<token>" `
  --output dimdc\outputs\bvault_gold_odcs.xlsx
```

Or environment variables:

```powershell
$env:DATABRICKS_SERVER_HOSTNAME="<server-hostname>"
$env:DATABRICKS_HTTP_PATH="<sql-warehouse-http-path>"
$env:DATABRICKS_TOKEN="<token>"

python create_odcs_xlsx_from_csv_folder.py `
  --input-mode catalog-schema `
  --input-path dev_allianz_raw.bvault_gold `
  --output dimdc\outputs\bvault_gold_odcs.xlsx
```

The script queries:

```sql
<catalog>.information_schema.tables
<catalog>.information_schema.columns
```

## Custom Rules

The script reads custom rule definitions from:

```text
dimdc/rules.csv
```

Default custom rules added to inferred primary keys:

```text
rule_0001,rule_0007
```

These appear in the Excel `Quality` sheet as:

```text
Quality Type: custom
Rule (Library): rule_0001
Quality Engine (Custom): business_rule_catalog
Implementation (Custom): rule_0001: BK Check
Query (SQL): blank
```

To choose different rules:

```powershell
python create_odcs_xlsx_from_csv_folder.py `
  --input-mode csv `
  --input-path data\raw\policy\20260813095628\prd_02 `
  --custom-rule-ids rule_0001,rule_0003,rule_0007 `
  --output dimdc\outputs\policy_prd02_custom_rules_odcs.xlsx
```

To disable custom rules:

```powershell
python create_odcs_xlsx_from_csv_folder.py `
  --input-mode csv `
  --input-path data\raw\policy\20260813095628\prd_02 `
  --custom-rule-ids "" `
  --output dimdc\outputs\policy_prd02_no_custom_rules_odcs.xlsx
```

To use a different rule catalog:

```powershell
python create_odcs_xlsx_from_csv_folder.py `
  --input-mode csv `
  --input-path data\raw\policy\20260813095628\prd_02 `
  --rules path\to\rules.csv `
  --custom-rule-ids rule_0001,rule_0007 `
  --output dimdc\outputs\policy_prd02_rules_odcs.xlsx
```

## Type And Key Inference

For CSV mode, the script samples rows from each CSV.

Default sample size:

```text
1000 rows
```

Override sample size:

```powershell
python create_odcs_xlsx_from_csv_folder.py `
  --input-mode csv `
  --input-path data\raw\policy\20260813095628\prd_02 `
  --sample-rows 5000 `
  --output dimdc\outputs\policy_prd02_odcs.xlsx
```

Inferred logical types include:

- `integer`
- `number`
- `boolean`
- `date`
- `timestamp`
- `string`

Primary key inference checks common patterns:

```text
<table_name>_id
<table_name>_identifier
<table_name>_sk
id
identifier
first *_identifier / *_id / *_sk
first unique sampled column
```

Sensitive classifications are inferred for column names containing values such as:

```text
email
phone
name
address
dob
birth
gender
postcode
zipcode
```

## Convert Excel To YAML

After generating Excel:

```powershell
datacontract import excel `
  --source dimdc\outputs\policy_prd02_adhoc_odcs.xlsx `
  --output dimdc\outputs\policy_prd02_adhoc_odcs.yaml
```

Validate YAML:

```powershell
$env:PYTHONIOENCODING="utf-8"
datacontract lint dimdc\outputs\policy_prd02_adhoc_odcs.yaml
```

`PYTHONIOENCODING=utf-8` avoids Windows console encoding issues from rich CLI output.

## Verified Example

The script was verified using:

```text
data\raw\policy\20260813095628\prd_02
```

Generated:

```text
dimdc\outputs\policy_prd02_adhoc_odcs.xlsx
dimdc\outputs\policy_prd02_adhoc_odcs.yaml
```

Validation result:

```text
schemas=3
properties=62
quality=15
datacontract lint=passed
```

## Current Limitations

- CSV mode infers metadata from headers and sampled values, so review generated types and keys before approval.
- Catalog-schema mode reads table and column metadata only. It does not read constraints unless they are available and explicitly added later.
- The generated Excel keeps SQL blank by design. SQL execution logic belongs in the external rule catalog and validation notebook/framework.
- Composite keys are not inferred from CSV files automatically. Use table metadata/rule mapping for stronger governed contracts when composite keys matter.
