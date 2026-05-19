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

The current enhanced model uses the active `new` DDL:

- `enhanced_360/new/newEnhanced Customer 360 Data Vault DDL.sql`

The enhanced output currently contains:

- `80` tables total
- `25` hubs
- `30` links
- `25` satellites
- `588` DDL columns including Data Vault metadata
- `453` DDL columns excluding common metadata columns
- `313` descriptive/business attributes excluding common metadata and hash keys

## 2. Main Entry Points

Run the full generation flow:

```powershell
$env:PYTHONUTF8='1'
python .\main.py
```

Generate only enhanced synthetic output:

```powershell
$env:PYTHONUTF8='1'
python .\main.py --enhanced-only
```

`--enhanced-only` still builds the base context in memory because enhanced tables depend on base people, quotes, policies, products, assets, links, and satellites. It skips base CSV output, raw/source outputs, silver output, base validation, and base SCD2.

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

- `enhanced_360/new/newEnhanced Customer 360 Data Vault DDL.sql`
- `enhanced_360/new/newEnhanced_Customer360_S2T_Mapping_DV&3NF_to_Dimensional_Model.xlsx`
- `enhanced_360/data_example/*.csv`

Generated runtime data:

- `data/output`
- `data/raw`
- `data/synthetic`
- `data/silver`
- `data/scd2`
- `data/new_outputs_src`

## 4. Key File Responsibilities

Main orchestration:

- `main.py`: runs the end-to-end generation flow, builds the base context, writes full outputs, and supports `--enhanced-only`

Base generation:

- `helper/hub_builder.py`: creates person, subtype, product, lead, quote, contact, identity, consent, address, and marketing hubs
- `helper/link_builder.py`: creates base Data Vault links between people, leads, contacts, quotes, policies, products, customers, accounts, and assets
- `helper/satellite_builder.py`: creates base satellites and owns most base timeline/business rules
- `generators/transaction_generator.py`: creates policies from quotes and motor/home assets from policy products
- `generators/supporting_generator.py`: creates supporting hubs such as account
- `generators/lifecycle_generator.py`: creates customer hubs for policy-holder persons
- `helper/source_context_builder.py`: builds independent source contexts for raw/API/claims/data-source outputs

Enhanced generation:

- `generators/enhanced_synthetic_generator.py`: maps base context into the 80-table enhanced model and creates broker, campaign, channel, claim, complaint, insured object, override, and regulation records
- `helper/enhanced_ddl.py`: parses the active enhanced DDL for table columns, column types, PKs, and FKs
- `helper/enhanced_rules.py`: shared enhanced product/category/channel rules used by generation and verification
- `misc/verify_enhanced_synthetic.py`: verifies enhanced schema, PKs, FKs, timestamp fields, base/enhanced business rules, and lifecycle alignment

Raw, silver, and source outputs:

- `generators/raw_crm_generator.py`: writes raw CRM extracts from the main base context
- `generators/raw_api_generator.py`: writes raw API extracts from an independent source context
- `generators/raw_claims_generator.py`: writes raw claims extracts from an independent source context
- `generators/raw_data_source_generator.py`: writes source-native home and motor extracts
- `generators/raw_kaggle_generator.py`: writes configured Kaggle raw extracts
- `helper/crm_mapper.py`: maps raw CRM to canonical raw
- `helper/data_source_mapper.py`: maps source-native home/motor extracts to canonical raw
- `helper/api_silver_builder.py`: builds API silver output
- `misc/transform_all_raw_to_silver.py`: rebuilds silver outputs from latest raw sources
- `misc/verify_all_silver.py`: verifies silver outputs

SCD2:

- `helper/scd2_generator.py`: creates base/enhanced sampled synthetic SCD2 deltas using the 10% per-satellite rule
- `helper/raw_scd2_generator.py`: creates raw SCD2 deltas for supported raw sources
- `helper/scd2_diff_engine.py`: locates previous runs and supports raw/source diffing
- `helper/new_outputs_src.py`: creates source-specific outputs and source-specific SCD2
- `misc/compare_all_scd2.py`: compares/report-checks generated SCD2 outputs

## 5. Output Layout

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

## 6. Runtime Flow in `main.py`

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

## 7. Base Synthetic Model

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

## 8. Base Generation Inputs

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

## 9. Important Base Rules

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

## 10. End-to-End Business Lifecycle

The generated data follows one main lifecycle spine. Enhanced 360 adds entities around this same spine instead of creating unrelated standalone facts.

Primary lifecycle:

```text
Person
-> Lead
-> Quote
-> Policy
-> Customer and Account
-> Product-linked Motor/Home asset
-> Enhanced Insured Object
-> Enhanced Claim, Complaint, Override, Broker, Campaign, Regulation
```

Lead to quote:

- lead persons are selected from the configured lifecycle distribution
- quote persons must be lead persons
- lead interest is behavior-driven, so quoted or policy-holder persons become higher-intent leads

Quote to policy:

- quotes choose products from `enums/product_catalog.py`
- policies are created only from converted quotes
- policy product inherits from the quote product
- policy-holder persons become customers and receive accounts

Policy and renewal:

- policy start date is generated after the lead conversion date
- policy end date is after policy start date
- non-cancelled policies are annual policies
- renewal date is a field on `sat_policy`, not a separate renewal-policy row
- renewal date is near policy end date and must not be after policy end date
- renewal current/next amounts follow churn movement bands rather than a fixed uplift
- account state can force a policy out of active status
- policy cycle is standardized as `policy_cycle`; the legacy misspelled column name is not used
- churn-available and proxy fields are generated once in base satellites and reused by CRM, API, data-source, canonical raw, silver, and enhanced outputs

Large-volume generation:

- `main.py --streaming-base --total-people <n> --chunk-size <n>` generates base Data Vault output in bounded chunks and appends rows to the same output files
- streaming mode reuses the same base hub/link/satellite builders per chunk, then drops the chunk context before generating the next chunk
- streaming normalization writes `data/synthetic/base/<run_id>` row by row and avoids pandas full-file reads
- the normal `main.py` path is unchanged and remains the full-output path for raw, silver, enhanced, SCD2, and detailed validation runs

Campaign:

- campaigns are enhanced reference-style entities sampled from `DimCampaign.csv`
- campaign metrics come from `FactQuote.csv`
- campaigns link to lead persons through `link_person_campaign`
- campaigns enrich the lead journey but do not create policies directly

Insured object:

- motor/home assets are created from policy product type in the base flow
- enhanced insured objects are derived from those policy-linked motor/home assets
- `link_policy_insured_object` ties the insured object to the policy
- `link_insured_object_motor` ties motor insured objects to `hub_motor`
- `link_insured_object_home` ties home insured objects to `hub_home`
- insured object start/end timestamps follow linked policy start/end timestamps
- `sat_insured_object.insured_value` is a positive integer derived from linked enhanced policy sum insured, with object-type fallback values when policy sum insured is unavailable

Claim:

- claims are generated from active policies where possible
- every claim links to a policy through `link_claim_policy`
- claim reported timestamp is clamped to the linked policy period
- claim settlement timestamp is not before claim reported timestamp
- claim channel follows policy sales channel
- claim product follows linked policy product family

Complaint:

- complaints are generated from customer policy context
- every complaint links to a policy through `link_complaint_policy`
- the linked policy resolves to customer through `link_policy_customer`
- complaint timestamp is not before customer since timestamp
- acknowledgement timestamp is not before complaint timestamp
- resolved timestamp is not before acknowledgement timestamp, or complaint timestamp when acknowledgement is missing
- complaint channel and category follow linked policy context

Regulation:

- regulations are sampled from `DimRegulations.csv`
- regulations may link to complaints through `link_complaint_regulation`
- regulation raised timestamp must be before or equal to deadline and close timestamps

Timestamp rule:

- enhanced DDL `TIMESTAMP` columns must contain parseable timestamp values with time components
- `misc/verify_enhanced_synthetic.py` enforces this from the DDL, so newly added timestamp columns are checked automatically

Numeric rule:

- enhanced DDL numeric columns are normalized before CSV output
- blank `INT`-style columns are emitted as `0`
- blank floating/decimal columns are emitted as `0.0`
- this prevents Databricks `inferSchema=true` from seeing all-empty numeric columns as non-numeric
- `misc/verify_enhanced_synthetic.py` enforces numeric columns from the DDL, including integer-only checks for `INT` columns

## 11. Enhanced Synthetic Model

Enhanced generation is implemented in:

- `generators/enhanced_synthetic_generator.py`
- `helper/enhanced_ddl.py`
- `helper/enhanced_rules.py`

Enhanced output is written to:

- `data/synthetic/enhanced/<run_id>`

The enhanced schema is parsed from:

- `enhanced_360/new/newEnhanced Customer 360 Data Vault DDL.sql`

Enhanced sample/reference data comes from:

- `enhanced_360/data_example/*.csv`

Enhanced generation does not replace base generation. It starts from the `base_context` produced by `main.py` and adds enhanced entities and attributes around that spine.

## 12. Enhanced Entity Groups

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

## 13. Enhanced Link Rules

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

Address tables:

- base person home-address context is written to enhanced as `hub_address`, `link_person_address`, and `sat_address`
- enhanced `hub_home` uses `insured_object_home_id`
- enhanced `hub_motor` uses `insured_object_motor_id`

## 14. Enhanced Business Rules

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
- `sat_campaign.is_active` is generated as `Y` or `N` from the source active flag
- `sat_campaign.campaign_start_date` and `sat_campaign.campaign_end_date` are generated as timestamp values with time components
- campaigns attach to lead persons through `link_person_campaign`

Claims:

- claims are generated from active policies where possible
- every claim links to a policy through `link_claim_policy`
- claim channel follows linked policy sales channel
- claim product follows linked policy product family
- claim reported and settlement dates are clamped into valid policy timelines
- claim reported, settlement, litigation, and recovery date fields are generated as timestamp values with time components
- no-recovery claims use `1900-01-01T00:00:00` for `first_recovery_date` and `last_recovery_date`; recovery claims use real lifecycle-safe recovery timestamps
- `recovery_priority_score`, `days_to_first_recovery`, and `days_to_last_recovery` default to `0` when source recovery values are absent
- claim financial fields live on `sat_claim`

Complaints:

- complaints are generated from customer policy context
- every complaint links to a policy through `link_complaint_policy`
- complaint date must be on or after customer since date
- acknowledgement and resolution dates must be ordered
- complaint date, acknowledgement date, and resolved date are generated as timestamp values with time components
- complaint channel and insurance category align to linked policy context

Customer enrichment:

- `sat_customer.customer_satisfaction` is a DDL `STRING`
- numeric source CSAT scores are mapped to labels to avoid CSV schema inference treating the column as an integer:
  - `9-10` -> `VERY_SATISFIED`
  - `7-8` -> `SATISFIED`
  - `5-6` -> `NEUTRAL`
  - `<5` -> `DISSATISFIED`
  - missing or invalid -> `UNKNOWN`

Overrides:

- overrides are generated from active policies where possible
- every override links to a policy through `link_policy_override`
- override reason is required
- linked policy override commission is populated for override policies

Regulations:

- regulations are sampled from `DimRegulations.csv`
- regulation dates must be ordered
- regulation date fields are generated as timestamp values with time components
- DDL `STRING` boolean/flag columns are generated as `Y` or `N` to prevent Databricks `inferSchema=true` from treating them as Boolean.
- regulation rows can link to complaints through `link_complaint_regulation`

Insured objects:

- insured objects are derived from policy-linked motor/home assets
- each insured object links back to policy
- motor insured objects link to motor assets
- home insured objects link to home assets
- insured object start and end dates are generated as timestamp values with time components
- insured object value is populated as a positive integer so CSV schema inference does not see an all-empty integer column
- `link_insured_object_motor` uses `insured_object_motor_hash_key` as its primary key and `motor_hash_key` as the motor foreign key

## 15. Enhanced Verification

Enhanced verification is implemented in:

- `misc/verify_enhanced_synthetic.py`

It checks:

- all expected files exist
- columns match the enhanced DDL
- DDL `TIMESTAMP` columns contain parseable timestamp values with time components
- DDL numeric columns are populated and parse as the declared numeric type
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

## 16. Raw Source Generation

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

## 17. Silver Generation

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

## 18. SCD2 Behavior

SCD2 generation is implemented in:

- `helper/scd2_generator.py`
- `helper/scd2_diff_engine.py`
- `helper/raw_scd2_generator.py`
- `helper/new_outputs_src.py`

Base/enhanced synthetic SCD2:

- scans historical runs under `data/synthetic/base` or `data/synthetic/enhanced`
- excludes the current run
- keeps latest historical version per satellite hash key
- mutates `10%` of eligible rows per configured satellite file using configured mutation columns
- writes changed satellite rows to `data/scd2/base/<run_id>` or `data/scd2/enhanced/<run_id>`

Current mutation percentage:

- `CHANGE_PERCENT = 0.10`

Example:

- `100,000` eligible historical rows in one satellite produce about `10,000` SCD2 rows for that satellite
- `10,000` eligible historical rows in one satellite produce about `1,000` SCD2 rows for that satellite
- the percentage is not calculated across all enhanced tables as one combined total

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
- the first run after a clean history creates the full snapshot only; a later run is needed before synthetic SCD2 can mutate prior history
- use `data/synthetic/enhanced/<run_id>` as the current full enhanced snapshot and `data/scd2/enhanced/<run_id>` as the sampled CDC-style change set

## 19. Recommended Run Sequences

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

Enhanced-only generation and verification:

```powershell
$env:PYTHONUTF8='1'
python .\main.py --enhanced-only
python .\misc\verify_enhanced_synthetic.py
```

Enhanced-only verification:

```powershell
$env:PYTHONUTF8='1'
python .\misc\verify_enhanced_synthetic.py .\data\synthetic\enhanced\<run_id>
```

## 20. Common Change Points

Change population size or seed:

- edit `config/scenario_v1.json`

Change relationship cardinality:

- edit `config/cardinality.json`

Change product catalog:

- edit `enums/product_catalog.py`

Change allowed satellite enums:

- edit `enums/sat_enums.py`

Change enhanced table contract:

- update `enhanced_360/new/newEnhanced Customer 360 Data Vault DDL.sql`
- update `enhanced_360/new/newEnhanced_Customer360_S2T_Mapping_DV&3NF_to_Dimensional_Model.xlsx`
- update `generators/enhanced_synthetic_generator.py`
- update `misc/verify_enhanced_synthetic.py`
- update `helper/scd2_generator.py` if satellite names or mutation columns change

Change enhanced business rules:

- update `helper/enhanced_rules.py`
- update `generators/enhanced_synthetic_generator.py`
- update `misc/verify_enhanced_synthetic.py`

## 21. Known Caveats

- `main.py` is a script with top-level execution, not a function-based CLI entry point.
- The old root enhanced DDL remains as a historical reference, but active enhanced generation uses the `enhanced_360/new` DDL.
- Synthetic SCD2 is sampled mutation-based, not complete CDC.
- Kaggle ingestion is mapping-config-driven and depends on external input shape.
- Some raw source contexts are independent, so business IDs can differ between CRM/API/data_source outputs for the same run.
- Full 25,000-person runs can take time and produce large output folders.
