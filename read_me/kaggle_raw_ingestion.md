# Kaggle Raw Ingestion

This repo can now convert a Kaggle insurance dataset into the canonical raw CRM shape already used by the silver rebuild flow.

## Goal

Instead of mapping Kaggle files directly to vault tables, the new path is:

1. Kaggle dataset
2. Canonical raw CRM files
3. Existing raw-to-silver build
4. Existing verification flow

## New Files

- [raw_kaggle_generator.py](F:/SyncDataGenerator_v1.0/generators/raw_kaggle_generator.py)
- [kaggle_mapper.py](F:/SyncDataGenerator_v1.0/helper/kaggle_mapper.py)
- [canonical_raw_schema.py](F:/SyncDataGenerator_v1.0/helper/canonical_raw_schema.py)
- [insurance_customers_policies_template.json](F:/SyncDataGenerator_v1.0/config/kaggle_mappings/insurance_customers_policies_template.json)

## What It Produces

The generator writes canonical raw files under:

- `data/raw/kaggle/<dataset_name>/<batch_id>/`

Files produced are the same CRM-shaped files expected by the local silver rebuild flow, for example:

- `crm_person.csv`
- `crm_contact.csv`
- `crm_customer.csv`
- `crm_product.csv`
- `crm_policy.csv`

## Mapping Model

The JSON config defines:

- source file aliases
- target canonical raw file
- filters
- dedupe keys
- target-column mapping rules

Supported mapping rule kinds:

- `source`
- `literal`
- `coalesce`
- `concat`
- `hash_id`
- `map`
- `date_format`

## Example Command

```powershell
python .\generators\raw_kaggle_generator.py `
  --input-dir C:\data\kaggle\insurance_dataset `
  --config .\config\kaggle_mappings\insurance_customers_policies_template.json
```

That prints the output directory, for example:

```text
F:\SyncDataGenerator_v1.0\data\raw\kaggle\insurance_customers_policies_template\20260410_120000
```

## Convert to Silver

Use the Kaggle vault-style rebuild:

```powershell
python .\misc\run_kaggle_to_silver.py `
  --input-dir C:\data\kaggle\insurance_dataset `
  --config .\config\kaggle_mappings\insurance_customers_policies_template.json
```

Example:

```powershell
python .\misc\run_kaggle_to_silver.py `
  --input-dir .\data\input\kaggle\insurance_customer_data `
  --config .\config\kaggle_mappings\auto_insurance_kaggle.json
```

## Verify

Verify the rebuilt silver folder:

```powershell
python .\misc\verify_kaggle_silver.py `
  .\data\raw\kaggle\insurance_customers_policies_template\20260410_120000 `
  .\data\silver\kaggle\20260410_120000
```

## Important Limitation

This is a generic mapper, not a universal semantic inference engine.

For each Kaggle dataset, you still need to decide:

- which source columns represent person, customer, product, policy, quote, home, or motor data
- how IDs should be derived
- which fields should be defaulted or ignored

The provided JSON file is a template for a typical customer/policy dataset, not a guarantee for arbitrary Kaggle schemas.
