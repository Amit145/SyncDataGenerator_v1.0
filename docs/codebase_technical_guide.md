# SyncDataGenerator Technical Guide

This document explains how the codebase works, what it generates, how to run it, and the main business rules enforced by the current implementation.

## 1. Purpose

`SyncDataGenerator` generates synthetic insurance data in a Data Vault style model.

It produces:

- base synthetic Data Vault tables
- enhanced Customer 360 synthetic Data Vault tables
- raw source extracts for multiple source systems
- canonical raw outputs
- silver vault-style rebuild outputs
- SCD2-style delta outputs

The current enhanced model uses the update DDL:

- `enhanced_360/update/New_Enhanced Customer 360 Data Vault DDL.sql`

The enhanced output currently contains:

- `80` tables total
- `25` hubs
- `30` links
- `25` satellites

## 2. Main Entry Points

Run the full generation flow:

```powershell
$env:PYTHONUTF8='1'
python .\main.py
```

Verify latest enhanced synthetic output:

```powershell
$env:PYTHONUTF8='1'
python .\misc\verify_enhanced_synthetic.py
```

Verify a specific enhanced output:

```powershell
$env:PYTHONUTF8='1'
python .\misc\verify_enhanced_synthetic.py .\data\synthetic\enhanced\<run_id>
```

Transform latest raw sources to silver:

```powershell
python .\misc\transform_all_raw_to_silver.py
```

Verify all latest silver outputs:

```powershell
python .\misc\verify_all_silver.py
```

## 3. Core Folders

Configuration:

- `config/scenario_v1.json`
- `config/cardinality.json`
- `config/storage_paths.py`
- `config/kaggle_mappings/*.json`

Main generation code:

- `main.py`
- `generators/`
- `helper/`
- `enums/`

Validation and verification:

- `verify_csv.py`
- `validators/`
- `misc/verify_enhanced_synthetic.py`
- `misc/verify_all_silver.py`

Enhanced 360 assets:

- `enhanced_360/update/New_Enhanced Customer 360 Data Vault DDL.sql`
- `enhanced_360/update/DV_Table_Column_Changes_Old_vs_New.xlsx`
- `enhanced_360/data_example/*.csv`

Generated runtime data:

- `data/output`
- `data/raw`
- `data/synthetic`
- `data/silver`
- `data/scd2`
- `data/new_outputs_src`

## 4. Output Layout

Main synthetic output:

- `data/output/<run_id>`

Normalized base synthetic output:

- `data/synthetic/base/<run_id>`

Enhanced synthetic output:

- `data/synthetic/enhanced/<run_id>`

Raw source outputs:

- `data/raw/crm/<run_id>`
- `data/raw/api/<run_id>`
- `data/raw/claims/<run_id>`
- `data/raw/data_source/motor/<run_id>`
- `data/raw/data_source/home/<run_id>`
- `data/raw/data_source_canonical/<run_id>`
- `data/raw/kaggle/<dataset>/<run_id>`

Silver outputs:

- `data/silver/rebuild/<run_id>`
- `data/silver/api/<run_id>`
- `data/silver/claims/<run_id>`
- `data/silver/data_source/<run_id>`
- `data/silver/kaggle/<run_id>`

SCD2 outputs:

- `data/scd2/base/<run_id>`
- `data/scd2/enhanced/<run_id>`
- `data/scd2/raw/crm/<run_id>`
- `data/scd2/raw/api/<run_id>`
- `data/scd2/raw/kaggle/<dataset>/<run_id>`
- `data/new_outputs_src/<source>/scd2/<run_id>`

## 5. Runtime Flow in `main.py`

The full run is orchestrated by `main.py`.

High-level sequence:

1. Load config and create run IDs.
2. Parse base DDL metadata if metadata JSON does not already exist.
3. Generate base hubs.
4. Assign quote products.
5. Generate policies from converted quotes.
6. Generate customer and account hubs for policy holders.
7. Generate motor/home assets from policy products.
8. Generate base links.
9. Generate base satellites.
10. Write base output to `data/output/<run_id>`.
11. Build `base_context`, which is reused by raw and enhanced generation.
12. Generate raw CRM.
13. Map raw CRM to canonical raw.
14. Generate independent raw API context and raw API.
15. Generate independent raw claims context and raw claims.
16. Generate independent `data_source` context and data source raw extracts.
17. Map data source raw to canonical raw.
18. Generate Kaggle raw if configured input data exists.
19. Generate raw SCD2 for eligible raw sources.
20. Generate `new_outputs_src` source-specific outputs and SCD2.
21. Build API silver.
22. Generate enhanced synthetic output.
23. Generate enhanced SCD2 if prior enhanced history exists.
24. Validate base output structure and integrity.
25. Normalize base output into `data/synthetic/base/<run_id>`.
26. Generate base SCD2 if prior base history exists.

## 6. Base Synthetic Model

The base synthetic model is the spine of the whole project.

Base tables:

- `17` hubs
- `18` links
- `17` satellites
- `52` total tables

Main base entity flow:

```text
Person
-> Natural Person or Legal Person
-> Contact, Identity, Home Address
-> Lead
-> Consent, Marketing Preference, Marketing Engagement
-> Quote
-> Policy
-> Customer and Account
-> Motor or Home asset
```

Important base rule:

- customers and accounts are created only for policy-holder persons.

## 7. Base Generation Inputs

`config/scenario_v1.json` controls:

- total people
- country
- random seed
- natural/legal person split
- lifecycle distribution
- sales channel distribution
- conversion rates
- enhanced settings

`config/cardinality.json` controls relationship counts for selected links.

`enums/product_catalog.py` controls product codes and product selection weights by person type.

`enums/sat_enums.py` controls allowed enum values for satellites.

## 8. Important Base Rules

Person rules:

- every person has exactly one `Hub_Person`
- every person is either natural or legal
- no person should be both natural and legal
- no person should be neither natural nor legal
- natural-person age is generated in an adult range

Lead rules:

- only lead persons get lead hubs
- lead persons can get consent, marketing preference, and marketing engagement
- quote persons must be lead persons

Quote rules:

- quotes are generated only for lead persons
- each quote links to exactly one person
- each quote links to exactly one product
- product choice follows `product_catalog.py`

Policy rules:

- policies are created from quotes
- policy-to-product inherits from quote-to-product
- policy holder persons get customers and accounts
- `policy_start_date < policy_end_date`
- non-cancelled policies are annual policies
- renewal date is near policy end date
- account state can force policy out of active status

Product and asset rules:

- product semantics live in `sat_product.type`
- motor-like products generate motor assets
- home/property-like products generate home assets
- home assets can get address context

Load date rules:

- hub, link, and satellite load dates are sequenced
- hub load date is earliest
- link load date is after hub load date
- satellite load date is after link load date

## 9. Enhanced Synthetic Model

Enhanced generation is implemented in:

- `generators/enhanced_synthetic_generator.py`
- `helper/enhanced_ddl.py`
- `helper/enhanced_rules.py`

Enhanced output is written to:

- `data/synthetic/enhanced/<run_id>`

The enhanced schema is parsed from:

- `enhanced_360/update/New_Enhanced Customer 360 Data Vault DDL.sql`

Enhanced sample/reference data comes from:

- `enhanced_360/data_example/*.csv`

Enhanced generation does not replace base generation. It starts from the `base_context` produced by `main.py` and adds enhanced entities and attributes around that spine.

## 10. Enhanced Entity Groups

Enhanced entity groups:

- broker
- campaign
- channel
- claim
- complaint
- insured object
- override
- regulation

Enhanced-specific hubs include:

- `hub_broker`
- `hub_campaign`
- `hub_channel`
- `hub_claim`
- `hub_complaint`
- `hub_insured_object`
- `hub_override`
- `hub_regulation`

Enhanced-specific satellites include:

- `sat_broker`
- `sat_campaign`
- `sat_channel`
- `sat_claim`
- `sat_complaint`
- `sat_insured_object`
- `sat_override`
- `sat_regulation`

## 11. Enhanced Link Rules

Broker links:

- `link_broker_person`
- `link_policy_broker`
- `link_quote_broker`

Campaign links:

- `link_person_campaign`

Channel links:

- `link_policy_channel`
- `link_quote_channel`

Claim and complaint links:

- `link_claim_policy`
- `link_complaint_policy`
- `link_complaint_regulation`

Policy and quote links:

- `link_policy_quote`

Override links:

- `link_policy_override`

Insured object links:

- `link_policy_insured_object`
- `link_insured_object_home`
- `link_insured_object_motor`

## 12. Enhanced Business Rules

Base spine:

- enhanced records must attach to valid base entities
- do not generate standalone enhanced facts and match them later
- derive enhanced rows from base context first

Channel:

- base policy sales channel is the source of truth
- canonical channels are `ONLINE`, `AGENT`, `BRANCH`
- enhanced `sat_channel.channel_name` must match linked policy `sat_policy.sales_channel`
- `AGENT` is the broker/agent channel in this codebase

Broker:

- brokers are sampled from `DimBroker.csv`
- AGENT-channel policy persons must have broker references
- broker links are generated for person, policy, and quote where applicable

Campaign:

- campaigns are sampled from `DimCampaign.csv`
- campaign marketing metrics are populated from `FactQuote.csv`
- campaigns attach to lead persons through `link_person_campaign`

Claims:

- claims are generated from active policies where possible
- every claim links to a policy through `link_claim_policy`
- claim channel follows linked policy sales channel
- claim product follows linked policy product family
- claim reported and settlement dates are clamped into valid policy timelines
- claim financial fields live on `sat_claim`

Complaints:

- complaints are generated from customer policy context
- every complaint links to a policy through `link_complaint_policy`
- complaint date must be on or after customer since date
- acknowledgement and resolution dates must be ordered
- complaint channel and insurance category align to linked policy context

Overrides:

- overrides are generated from active policies where possible
- every override links to a policy through `link_policy_override`
- override reason is required
- linked policy override commission is populated for override policies

Regulations:

- regulations are sampled from `DimRegulations.csv`
- regulation dates must be ordered
- regulation rows can link to complaints through `link_complaint_regulation`

Insured objects:

- insured objects are derived from policy-linked motor/home assets
- each insured object links back to policy
- motor insured objects link to motor assets
- home insured objects link to home assets

## 13. Enhanced Verification

Enhanced verification is implemented in:

- `misc/verify_enhanced_synthetic.py`

It checks:

- all expected files exist
- columns match the enhanced DDL
- primary keys are unique and nonblank
- foreign keys reference existing parent records
- shared base enum values remain valid
- base policy and lead timeline checks
- policy-channel alignment
- AGENT policy broker alignment
- claim date/channel/product rules
- complaint date/channel/category rules
- override policy/reason/commission rules
- regulation date ordering

Run:

```powershell
$env:PYTHONUTF8='1'
python .\misc\verify_enhanced_synthetic.py
```

## 14. Raw Source Generation

Raw CRM:

- generated by `generators/raw_crm_generator.py`
- uses main base context
- output: `data/raw/crm/<run_id>`

Raw API:

- generated by `generators/raw_api_generator.py`
- uses independent synthetic context
- output: `data/raw/api/<run_id>`

Raw claims:

- generated by `generators/raw_claims_generator.py`
- uses independent synthetic context
- output: `data/raw/claims/<run_id>`

Raw data source:

- generated by `generators/raw_data_source_generator.py`
- uses independent synthetic context
- writes motor and home source-native extracts
- mapped to canonical raw by `helper/data_source_mapper.py`

Raw Kaggle:

- generated by `generators/raw_kaggle_generator.py`
- driven by external files under `data/input/kaggle`
- configured by JSON files under `config/kaggle_mappings`

## 15. Silver Generation

Shared local silver builder:

- `misc/raw_to_silver_sample.py`

API wrapper:

- `helper/api_silver_builder.py`

Transform all latest raw sources:

```powershell
python .\misc\transform_all_raw_to_silver.py
```

Verify silver outputs:

```powershell
python .\misc\verify_all_silver.py
```

Silver output shape follows the base 52-table vault-style schema.

## 16. SCD2 Behavior

SCD2 generation is implemented in:

- `helper/scd2_generator.py`
- `helper/scd2_diff_engine.py`
- `helper/raw_scd2_generator.py`
- `helper/new_outputs_src.py`

Base/enhanced synthetic SCD2:

- scans historical runs under `data/synthetic/base` or `data/synthetic/enhanced`
- excludes the current run
- keeps latest historical version per satellite hash key
- mutates a small percentage of rows using configured mutation columns
- writes changed satellite rows to `data/scd2/base/<run_id>` or `data/scd2/enhanced/<run_id>`

Current mutation percentage:

- `CHANGE_PERCENT = 0.001`

Enhanced SCD2 includes new-schema satellite files such as:

- `sat_complaint.csv`
- `sat_insured_object.csv`
- `sat_regulation.csv`

Raw SCD2:

- compares prior and current raw folders
- writes changed and new raw records
- currently covers CRM, API, and Kaggle

Important distinction:

- synthetic SCD2 is a sampled synthetic change feed from prior satellite history
- it is not full CDC for every new current-run entity
- use `data/synthetic/enhanced/<run_id>` as the current full enhanced snapshot and `data/scd2/enhanced/<run_id>` as the sampled CDC-style change set

## 17. Recommended Run Sequences

Clean or first run:

```powershell
$env:PYTHONUTF8='1'
python .\main.py
python .\misc\verify_enhanced_synthetic.py
python .\misc\transform_all_raw_to_silver.py
python .\misc\verify_all_silver.py
```

Second run for SCD2:

```powershell
$env:PYTHONUTF8='1'
python .\main.py
python .\misc\verify_enhanced_synthetic.py
python .\misc\compare_all_scd2.py
```

Enhanced-only verification:

```powershell
$env:PYTHONUTF8='1'
python .\misc\verify_enhanced_synthetic.py .\data\synthetic\enhanced\<run_id>
```

## 18. Common Change Points

Change population size or seed:

- edit `config/scenario_v1.json`

Change relationship cardinality:

- edit `config/cardinality.json`

Change product catalog:

- edit `enums/product_catalog.py`

Change allowed satellite enums:

- edit `enums/sat_enums.py`

Change enhanced table contract:

- update `enhanced_360/update/New_Enhanced Customer 360 Data Vault DDL.sql`
- update `enhanced_360/update/DV_Table_Column_Changes_Old_vs_New.xlsx`
- update `generators/enhanced_synthetic_generator.py`
- update `misc/verify_enhanced_synthetic.py`
- update `helper/scd2_generator.py` if satellite names or mutation columns change

Change enhanced business rules:

- update `helper/enhanced_rules.py`
- update `generators/enhanced_synthetic_generator.py`
- update `misc/verify_enhanced_synthetic.py`

## 19. Known Caveats

- `main.py` is a script with top-level execution, not a function-based CLI entry point.
- The old root enhanced DDL remains as a historical reference, but active enhanced generation uses the update DDL.
- Synthetic SCD2 is sampled mutation-based, not complete CDC.
- Kaggle ingestion is mapping-config-driven and depends on external input shape.
- Some raw source contexts are independent, so business IDs can differ between CRM/API/data_source outputs for the same run.
- Full 25,000-person runs can take time and produce large output folders.
