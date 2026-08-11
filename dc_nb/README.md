# Data Contract Notebooks

Run order:

1. `create_template_data_contract.ipynb`
2. `validate_data_contract_template_with_ODCS_standards.ipynb`
3. `create_data_contract_databricks.py`
4. `execute_data_contract.py`

Required reusable inputs are stored in `dc_nb/inputs`:

- `odcs-v3.1.0.schema_ODCS.json`: official ODCS schema used for template and contract validation.
- `enhanced_prd01_contract_config.yaml`: table, PK/BK, and custom rule config for enhanced PRD01.
- `enhanced_prd02_contract_config.yaml`: table, PK/BK, and custom rule config for enhanced PRD02.
- `enhanced_prd01_contract_config.csv` and `enhanced_prd02_contract_config.csv`: CSV equivalents for simple config editing.
- `etl_rule_catalog.txt`: optional ETL rule catalog reference.

Generated notebook outputs should go under `dc_nb/outputs`:

- `outputs/templates`: generated ODCS template YAML.
- `outputs/validation`: template validation report CSV.
- `outputs/contracts`: generated ODCS contract YAML.

The physical data path is not copied here because it is runtime data. Pass it through the `input_data_path` widget for contract generation and `input_base_path` widget for execution.

`create_data_contract_databricks.py` and `execute_data_contract.py` are Databricks notebook source files. They are self-contained and do not import project Python modules.

For raw enhanced PRD01 local data, use:

```text
input_data_path = F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260811151951/vault_ready_28
input_base_path = F:/SyncDataGenerator_v1.0/data/raw/enhanced/prd_01/20260811151951/vault_ready_28
batch_ref = 20260811151951
```
