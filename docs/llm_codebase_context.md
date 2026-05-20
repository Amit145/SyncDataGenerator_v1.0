# LLM Codebase Context Brief

Use this document as context when asking an LLM to modify or reason about this repository.

## Identity of the Project

This repository is `SyncDataGenerator`.

It generates synthetic insurance data in Data Vault style CSV outputs. It also creates raw source extracts, canonical raw, silver vault rebuilds, and SCD2-style delta outputs.

The project is Python-based. The main executable script is:

- `main.py`

The current enhanced Customer 360 model uses the active `new` DDL:

- `enhanced_360/new/newEnhanced Customer 360 Data Vault DDL.sql`

Do not assume the older root enhanced DDL is active. The old file:

- `enhanced_360/Enhanced_Customer360_DataVault_DDL.sql`

is only a historical/reference baseline unless the user explicitly says otherwise.

## Current Enhanced Contract

Enhanced synthetic output must follow the active `new` DDL:

- `80` tables total
- `25` hubs
- `30` links
- `25` satellites
- `588` total DDL columns including Data Vault metadata
- `453` DDL columns excluding common metadata columns
- `313` descriptive/business attributes excluding common metadata and hash keys

Enhanced output path:

- `data/synthetic/enhanced/<run_id>`

Enhanced SCD2 output path:

- `data/scd2/enhanced/<run_id>`

Enhanced change mapping workbook:

- `enhanced_360/new/newEnhanced_Customer360_S2T_Mapping_DV&3NF_to_Dimensional_Model.xlsx`

Enhanced sample/reference data:

- `enhanced_360/data_example/*.csv`

## How the System Runs

Normal full generation:

```powershell
python .\main.py
```

Enhanced-only generation:

```powershell
$env:PYTHONUTF8='1'
python .\main.py --enhanced-only
```

`--enhanced-only` still builds base context in memory, then writes only enhanced synthetic output and enhanced SCD2 when prior enhanced history exists. It skips base CSV output, raw/source outputs, silver output, base validation, and base SCD2.

Verify latest enhanced output:

```powershell
$env:PYTHONUTF8='1'
python .\misc\verify_enhanced_synthetic.py
```

Transform raw to silver:

```powershell
python .\misc\transform_all_raw_to_silver.py
```

Verify all silver:

```powershell
python .\misc\verify_all_silver.py
```

## High-Level Runtime Flow

`main.py` runs top-level code directly. It is not structured as a `main()` function.

The main flow is:

1. Load configuration.
2. Generate base hubs.
3. Assign products to quotes.
4. Generate policies from quotes.
5. Generate customers and accounts for policy holders.
6. Generate motor/home assets from policy products.
7. Generate base links.
8. Generate base satellites.
9. Write base output to `data/output/<run_id>`.
10. Build `base_context`.
11. Generate raw CRM/API/claims/data_source outputs.
12. Generate raw SCD2 when previous CRM/API raw runs exist.
13. Generate optional `new_outputs_src` only when `--include-new-outputs-src` is passed.
14. Generate API silver.
15. Generate enhanced synthetic output from `base_context`.
16. Generate enhanced SCD2 when previous enhanced runs exist.
17. Validate base output and normalize to `data/synthetic/base/<run_id>`.
18. Generate base SCD2 when previous base runs exist.

## Important Code Files

Base generation:

- `main.py`
- `helper/hub_builder.py`
- `helper/link_builder.py`
- `helper/satellite_builder.py`
- `helper/source_context_builder.py`
- `generators/person_generator.py`
- `generators/lifecycle_generator.py`
- `generators/supporting_generator.py`
- `generators/transaction_generator.py`
- `generators/product_generator.py`

Enhanced generation:

- `generators/enhanced_synthetic_generator.py`
- `helper/enhanced_ddl.py`
- `helper/enhanced_rules.py`
- `misc/verify_enhanced_synthetic.py`

Raw and silver:

- `generators/raw_crm_generator.py`
- `generators/raw_api_generator.py`
- `generators/raw_claims_generator.py`
- `generators/raw_data_source_generator.py`
- `helper/data_source_mapper.py`
- `misc/raw_to_silver_sample.py`
- `misc/transform_all_raw_to_silver.py`

SCD2:

- `helper/scd2_generator.py`
- `helper/scd2_diff_engine.py`
- `helper/raw_scd2_generator.py`
- `helper/new_outputs_src.py`
- `misc/compare_all_scd2.py`

Validation:

- `verify_csv.py`
- `validators/file_cols_validator.py`
- `validators/integrity_checker.py`
- `misc/verify_enhanced_synthetic.py`

Configuration and enum sources:

- `config/scenario_v1.json`
- `config/cardinality.json`
- `config/storage_paths.py`
- `enums/product_catalog.py`
- `enums/sat_enums.py`

`config/scenario_v1.json` includes `churn_settings`, which controls renewal, claim-count, add-on, vehicle segment, marketing proxy, customer/account status, tenure-churn, sales-channel, and churned-status distributions.

Use `docs/scenario_config_reference.md` for the meaning of each scenario config key before changing generator behavior.

## Base Model Rules

Base model has:

- `17` hubs
- `18` links
- `17` satellites
- `52` total tables

The base entity spine is:

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

Base rules:

- every person has one person hub
- every person is natural or legal, not both
- quote persons must be lead persons
- policies are created only from quotes
- policy product comes from quote product
- customers and accounts are created only for policy-holder persons
- motor/home assets derive from policy product type
- hub/link/satellite load dates are sequenced
- historical business dates are capped to satellite load date where applicable
- policy start/end/renewal dates must be coherent

## End-to-End Lifecycle

The data should be understood as one lifecycle spine with enhanced entities attached to it:

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

Lifecycle rules:

- quote records come from lead persons
- policies come from converted quotes
- policy products inherit quote products
- policy holders become customers and accounts
- renewal is represented as fields on `sat_policy`, not as a new policy term row
- renewal current/next amounts follow churn movement bands, including decreases and higher uplifts
- policy cycle is standardized as `policy_cycle`; it means completed annual tenure, not number of policies purchased
- churn probability decreases as completed `policy_cycle` tenure increases
- sales-channel churn variance uses existing values only; `AGENT` carries broker/aggregator-like higher churn behavior and `AGGREGATOR` is not emitted
- churn distribution weights are configured in `config/scenario_v1.json` under `churn_settings`
- active/lapsed policy end dates use the current annual term boundary for long-tenure policies
- `LAPSED` requires a completed renewal cycle; sub-one-year churn is represented as `CANCELLED`
- do not emit the legacy misspelled policy-cycle column name
- churn available/proxy fields from `new_rules/Data Req Churn NPS.xlsx` are enforced in base satellite generation before source-specific raw outputs are written
- campaign rows link to lead persons and enrich the lead journey
- insured objects are derived from policy-linked motor/home assets
- insured object value is a positive integer derived from linked enhanced policy sum insured, with object-type fallback values when policy sum insured is unavailable
- claims are generated from active policies where possible and stay inside the linked policy period
- complaints are generated from customer policy context and occur after `customer_since`
- regulations can link to complaints and must have ordered raised/deadline/closed timestamps
- enhanced DDL `TIMESTAMP` columns must include time components and are checked generically by `misc/verify_enhanced_synthetic.py`
- large base runs use `main.py --streaming-base --total-people <n> --chunk-size <n>` to append chunked base Data Vault outputs without loading the whole population into memory

## Enhanced Model Rules

Enhanced generation starts from `base_context`.

Do not generate enhanced records as standalone random rows and match later. Generate them from valid base entities.

Enhanced entity groups:

- broker
- campaign
- channel
- claim
- complaint
- insured object
- override
- regulation

Enhanced-specific hubs:

- `hub_broker`
- `hub_campaign`
- `hub_channel`
- `hub_claim`
- `hub_complaint`
- `hub_insured_object`
- `hub_override`
- `hub_regulation`

Enhanced-specific satellites:

- `sat_broker`
- `sat_campaign`
- `sat_channel`
- `sat_claim`
- `sat_complaint`
- `sat_insured_object`
- `sat_override`
- `sat_regulation`

Enhanced-specific links:

- `link_broker_person`
- `link_policy_broker`
- `link_quote_broker`
- `link_person_campaign`
- `link_policy_channel`
- `link_quote_channel`
- `link_claim_policy`
- `link_complaint_policy`
- `link_complaint_regulation`
- `link_policy_override`
- `link_policy_quote`
- `link_policy_insured_object`
- `link_insured_object_home`
- `link_insured_object_motor`
- `link_person_address`

Enhanced address and asset ID rules:

- base person home-address context is mapped to `hub_address`, `link_person_address`, and `sat_address`
- `hub_home` uses `insured_object_home_id`, not `home_id`
- `hub_motor` uses `insured_object_motor_id`, not `motor_id`
- `link_insured_object_motor` uses `insured_object_motor_hash_key` as its primary key and `motor_hash_key` as its motor foreign key

## Enhanced Business Rules

Channels:

- allowed channels are `ONLINE`, `AGENT`, `BRANCH`
- `AGENT` is the broker/agent channel in this codebase
- `BROKER` is not currently an allowed channel value unless enums are changed
- policy-channel links must match `sat_policy.sales_channel`
- quote-channel links must point to valid quotes and channels

Broker:

- brokers use `DimBroker.csv`
- AGENT-channel policy persons must have broker linkage
- broker links should include person, policy, and quote where applicable

Campaign:

- campaigns use `DimCampaign.csv`
- campaign metrics use `FactQuote.csv`
- `sat_campaign.is_active` must be `Y` or `N`; do not generate `number_of_is_active`
- `sat_campaign.campaign_start_date` and `sat_campaign.campaign_end_date` must include timestamp time components
- campaigns link to lead persons

Claims:

- claims use `DimClaim.csv` and `FactPolicy.csv`
- claims prefer active policies
- claim dates must fit policy dates
- claim reported, settlement, litigation, and recovery date fields must include timestamp time components
- no-recovery claims use `1900-01-01T00:00:00` for first/last recovery timestamps
- recovery claims use lifecycle-safe recovery timestamps; blank recovery numeric fields are emitted as `0`
- claim channel follows linked policy sales channel
- claim product follows linked policy product family
- claim financial fields live on `sat_claim`, not `sat_policy`

Complaints:

- complaints use `FactComplaints.csv` or `DimComplaints.csv`
- complaints link to policies through `link_complaint_policy`
- complaint policy resolves to customer through `link_policy_customer`
- complaint date must be on or after `customer_since`
- acknowledgement/resolution dates must be ordered
- complaint date, acknowledgement date, and resolved date must include timestamp time components
- complaint channel/category must match linked policy context

Customer enrichment:

- `sat_customer.customer_satisfaction` is a DDL `STRING`
- map numeric `CustomerSatisfaction` source scores to labels:
  - `9-10` -> `VERY_SATISFIED`
  - `7-8` -> `SATISFIED`
  - `5-6` -> `NEUTRAL`
  - `<5` -> `DISSATISFIED`
  - missing or invalid -> `UNKNOWN`
- do not emit bare numeric values for this column

Overrides:

- overrides use `DimOverride.csv`
- overrides prefer active policies
- overrides link through `link_policy_override`
- linked policy `override_commission` must be populated

Regulations:

- regulations use `DimRegulations.csv`
- regulations link to complaints through `link_complaint_regulation`
- regulation raised date must be before or equal to deadline and close date
- regulation date fields must include timestamp time components
- DDL `STRING` boolean/flag columns must be generated as string `Y` or `N`, not numeric `1` or `0` and not boolean-looking `TRUE` or `FALSE`
- DDL numeric columns are normalized centrally before CSV output: blank `INT`-style fields become `0`, and blank floating/decimal fields become `0.0`
- enhanced verification checks numeric population and parseability from the DDL

Insured object:

- insured objects are derived from policy-linked motor/home assets
- insured object links must reference valid policy, home, and motor objects
- insured object start and end dates must include timestamp time components
- `sat_insured_object.insured_value` must be populated as a positive integer

## Active Enhanced DDL Details

The active enhanced DDL has no `sat_quote.agent_id`.

`agent_id` exists on `hub_broker`, not `sat_quote`.

`link_complaint_regulation` includes:

- `complaint_regulation_hash_key`
- `load_date`
- `record_source`
- `regulation_hash_key`
- `complaint_hash_key`

If a future user says the Excel mapping and DDL are inconsistent, compare:

- `enhanced_360/new/newEnhanced Customer 360 Data Vault DDL.sql`
- `enhanced_360/new/newEnhanced_Customer360_S2T_Mapping_DV&3NF_to_Dimensional_Model.xlsx`

## Enhanced Verification Rules

`misc/verify_enhanced_synthetic.py` checks:

- expected files exist
- columns match active enhanced DDL
- DDL `TIMESTAMP` columns contain parseable timestamp values with time components
- DDL numeric columns are populated and parse as the declared numeric type
- primary keys are unique and nonblank
- foreign keys point to parent records
- base enums remain valid
- policy date ordering
- lead-to-policy ordering
- policy-channel alignment
- AGENT policy broker linkage
- claim policy/date/channel/product alignment
- complaint policy/customer/date/channel/category alignment
- override policy/reason/commission alignment
- regulation date ordering

If modifying enhanced generation, update the verifier in the same change.

## SCD2 Rules

Synthetic base/enhanced SCD2 is sampled mutation-based.

It:

- scans previous synthetic runs
- excludes the current run
- keeps latest historical row per satellite hash key
- samples `10%` of eligible rows per configured satellite file
- mutates configured columns
- writes changed satellite rows to `data/scd2/base/<run_id>` or `data/scd2/enhanced/<run_id>`

It does not emit all new entities as full CDC.

Count examples:

- `10,000` eligible historical rows in one satellite produce about `1,000` SCD2 rows for that satellite
- `100,000` eligible historical rows in one satellite produce about `10,000` SCD2 rows for that satellite
- the 10% rate is per satellite, not across all base or enhanced tables as one combined total

Use:

- `data/synthetic/enhanced/<run_id>` as the full current enhanced snapshot
- `data/scd2/enhanced/<run_id>` as the sampled CDC-style change feed

Current enhanced SCD2 should include new-schema satellite names such as:

- `sat_complaint.csv`
- `sat_insured_object.csv`
- `sat_regulation.csv`

## Important Do/Don't Guidance for LLMs

Do:

- read the active DDL before changing enhanced output
- keep generator and verifier aligned
- keep docs aligned when table names or rules change
- use `helper/enhanced_rules.py` for shared enhanced product/channel logic
- use `enums/sat_enums.py` and `enums/product_catalog.py` as sources of truth
- preserve base-context-driven enhanced generation
- run `python -m py_compile` after Python edits
- run `misc/verify_enhanced_synthetic.py` after enhanced changes

Do not:

- assume old enhanced table names are active
- reintroduce `hub_complaints`, `sat_complaints`, `hub_regulations`, or `sat_regulations`
- reintroduce `link_person_broker`, `link_policy_claim`, `link_override_policy`, `link_complaints_customer`, or `link_regulations_product`
- reintroduce enhanced `hub_home_address`, `link_person_home_address`, or `sat_home_address`
- put claim financial fields back on `sat_policy`
- put campaign metrics back on `sat_quote`
- reintroduce `sat_campaign.number_of_is_active`
- add `sat_quote.agent_id`
- use literal channel value `BROKER` without updating shared enums and validators
- delete the old root enhanced DDL unless explicitly instructed
- treat synthetic SCD2 as complete CDC

## Typical Tasks and Where to Edit

Add or remove enhanced table/column:

- DDL: `enhanced_360/new/newEnhanced Customer 360 Data Vault DDL.sql`
- Mapping: `enhanced_360/new/newEnhanced_Customer360_S2T_Mapping_DV&3NF_to_Dimensional_Model.xlsx`
- Parser usually does not need changes unless SQL shape changes: `helper/enhanced_ddl.py`
- Generator: `generators/enhanced_synthetic_generator.py`
- Verifier: `misc/verify_enhanced_synthetic.py`
- SCD2 mutation config if satellite changed: `helper/scd2_generator.py`
- Docs: `docs/enhanced_synthetic_plan.md`

Change product behavior:

- `enums/product_catalog.py`
- `helper/enhanced_rules.py`
- generation and verification code if category/family logic changes

Change channel behavior:

- `enums/sat_enums.py`
- `helper/enhanced_rules.py`
- `helper/satellite_builder.py`
- `generators/enhanced_synthetic_generator.py`
- `misc/verify_enhanced_synthetic.py`

Change population size:

- `config/scenario_v1.json`

Change cardinality:

- `config/cardinality.json`

## Current Known Good State

The latest validated run during the churn-tenure update was:

- `data/synthetic/base/20260520182650`
- `data/synthetic/enhanced/20260520182650`
- `data/scd2/base/20260520182650`
- `data/scd2/enhanced/20260520182650`

Validation passed for:

- `main.py`
- `validate_churn_kpis.py`
- `misc/transform_all_raw_to_silver.py`
- `misc/verify_all_silver.py`

The validation confirmed:

- churn decreases as completed `policy_cycle` tenure increases
- sales-channel churn variance is preserved with `AGENT` as the higher-risk broker/aggregator-like channel
- policy start/end/renewal dates remain valid for active, lapsed, and cancelled policies
- CRM, API, claims, and data_source silver outputs pass full rule validation
