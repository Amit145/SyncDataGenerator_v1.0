# Enhanced Synthetic 360 Rules

This document describes the implemented enhanced synthetic pipeline and the rules it is expected to follow.

It covers:

- scope and outputs
- source inputs
- how enhanced extends base 360
- shared enum and product rules
- count and linkage rules
- enhanced business rules
- SCD2 behavior
- verification behavior
- run and verify commands


## 1. Goal

The enhanced pipeline adds a separate enhanced synthetic output without breaking the existing base 360 flow.

The base pipeline remains in place:

- `data/output/<run_id>`
- `data/synthetic/base/<run_id>`
- `data/scd2/base/<run_id>`
- existing raw, canonical, silver, and base validation behavior

The enhanced pipeline writes:

- `data/synthetic/enhanced/<run_id>`
- `data/scd2/enhanced/<run_id>`

The enhanced output contains the full enhanced Data Vault model:

- all 52 base tables
- plus 28 enhanced-only tables
- total expected enhanced tables: `80`


## 2. Source Inputs

Enhanced schema contract:

- `enhanced_360/new/newEnhanced Customer 360 Data Vault DDL.sql`

Enhanced change mapping:

- `enhanced_360/new/newEnhanced_Customer360_S2T_Mapping_DV&3NF_to_Dimensional_Model.xlsx`

Enhanced sample/reference data:

- `enhanced_360/data_example/*.csv`

Base runtime inputs still drive the synthetic universe:

- `config/scenario_v1.json`
- `enums/product_catalog.py`
- `enums/sat_enums.py`
- existing base generation logic in `main.py` and `helper/source_context_builder.py`

Usage model:

- enhanced DDL defines the table and column contract
- enhanced sample files provide realistic value pools and distributions
- base generation provides the actual entity universe and relationship spine

Enhanced sample data is used as a reference source, not copied blindly.


## 3. Model Scope

Base DDL:

- `17` hubs
- `18` links
- `17` satellites
- `52` tables total

Enhanced DDL:

- `25` hubs
- `30` links
- `25` satellites
- `80` tables total
- `588` columns including Data Vault metadata
- `453` columns excluding common metadata columns
- `313` descriptive/business attributes excluding common metadata and hash keys

Enhanced-only additions:

- `8` hubs
- `13` links
- `7` satellites

Enhanced-only entity groups:

- `broker`
- `campaign`
- `channel`
- `claim`
- `complaint`
- `insured_object`
- `override`
- `regulation`


## 4. Output Locations

Implemented output layout:

- enhanced synthetic: `data/synthetic/enhanced/<run_id>`
- enhanced SCD2: `data/scd2/enhanced/<run_id>`

Enhanced output is fully separate from base output.


## 5. Core Integration Rule

Base 360 is the spine.

Enhanced records are generated from the same run context as base and must attach to valid base entities.

Enhanced entities are linked as follows:

- `broker -> person` via `link_broker_person`
- `broker -> policy` via `link_policy_broker`
- `broker -> quote` via `link_quote_broker`
- `campaign -> person` via `link_person_campaign`
- `channel -> policy` via `link_policy_channel`
- `channel -> quote` via `link_quote_channel`
- `claim -> policy` via `link_claim_policy`
- `complaint -> policy` via `link_complaint_policy`
- `complaint -> regulation` via `link_complaint_regulation`
- `override -> policy` via `link_policy_override`
- `policy -> quote` via `link_policy_quote`
- `policy -> insured_object` via `link_policy_insured_object`
- `insured_object -> home` via `link_insured_object_home`
- `insured_object -> motor` via `link_insured_object_motor`
- base person address output is exposed in enhanced as `hub_address`, `link_person_address`, and `sat_address`

Important implementation rule:

- do not generate standalone enhanced facts first and try to match them later
- derive enhanced rows from base context first, then populate attributes

Base context used by enhanced generation includes:

- `person_to_quote`
- `person_to_customer`
- `person_to_lead`
- `policy_person_map`
- `policy_to_person_map`
- `policy_to_product_id`
- `policy_to_quote_map`
- `sat_policy`
- `sat_customer`
- `sat_quote`


## 6. Enhanced Lifecycle Flow

Enhanced generation follows the same lifecycle spine created by base generation:

```text
Person
-> Lead
-> Quote
-> Policy
-> Customer and Account
-> Motor/Home asset
-> Insured Object
-> Claim, Complaint, Override, Broker, Campaign, Regulation
```

How enhanced tables attach to the lifecycle:

- campaigns link to lead persons through `link_person_campaign`
- channels link to policies and quotes through `link_policy_channel` and `link_quote_channel`
- brokers link to AGENT-channel persons, policies, and quotes
- insured objects are derived from actual policy-linked motor/home assets
- claims link to active policies where possible
- complaints link to policies from the customer's policy context
- regulations can link to generated complaints
- overrides link to active policies where possible

Date and timestamp lifecycle:

- policy dates are produced by the base policy generator and reused by enhanced
- renewal remains a `sat_policy` attribute, not a separate renewal-policy row
- insured object start/end timestamps follow linked policy start/end timestamps
- insured object value is a positive integer derived from linked enhanced policy sum insured, with object-type fallback values when policy sum insured is unavailable
- claim reported and settlement timestamps are clamped inside the linked policy period
- complaint timestamps occur after customer since and remain ordered through acknowledgement and resolution
- regulation raised timestamps must be before or equal to deadline and close timestamps
- all enhanced DDL `TIMESTAMP` columns must be generated with time components


## 7. Shared Domain Rules

Enhanced must reuse the same core domain vocabulary as base wherever the business concept already exists in base.

Shared domains that must stay aligned:

- product codes
- policy statuses
- quote statuses
- customer statuses
- account statuses
- policy sales channel
- lead interest and base lifecycle-style enums
- existing base Y/N flags where enhanced uses the same semantics

Current base sources of truth:

- `enums/product_catalog.py`
- `enums/sat_enums.py`
- `helper/enhanced_rules.py`

### Channel Rule

In base, channel already exists as:

- `Sat_Policy.Sales Channel`

In enhanced, channel is normalized into:

- `hub_channel`
- `sat_channel`
- `link_policy_channel`

Rule:

- same business concept
- same canonical enum set
- enhanced policy-channel links are derived from base `sat_policy.sales_channel`

Current canonical channel set:

- `ONLINE`
- `AGENT`
- `BRANCH`

Enhanced is not allowed to drift from that base set unless the shared constant source is changed intentionally.


## 8. Product Rules

Enhanced uses the same product catalog as base.

Current product source:

- `enums/product_catalog.py`

Current product codes:

- `PRD_MOTOR_PERSONAL`
- `PRD_HOME_PERSONAL`
- `PRD_HEALTH_PERSONAL`
- `PRD_TRAVEL`
- `PRD_COMMERCIAL_MOTOR`
- `PRD_PROPERTY_COMMERCIAL`
- `PRD_CYBER_INSURANCE`

Rules:

- `sat_product.type` must stay within the base product catalog
- enhanced claim and complaint product/category logic must be derived from linked base policy/product context
- enhanced regulations link to generated complaints through `link_complaint_regulation`
- enhanced insured objects must be derived from valid generated motor/home policy assets

Current product-family mapping used for enhanced validation/generation:

- motor-like codes -> `Motor`
- home/property-like codes -> `Home`
- health -> `Health`
- travel -> `Travel`
- cyber -> `Cyber`


## 9. Count Rules

Base config creates the synthetic universe. Enhanced counts are derived from that universe.

Do not apply `total_people` directly to every enhanced table.

Implemented configuration section:

```json
{
  "enhanced_settings": {
    "enabled": true,
    "broker_count": 20,
    "campaign_count": 10,
    "channel_count": 4,
    "claim_policy_rate": 0.08,
    "complaint_customer_rate": 0.02,
    "override_policy_rate": 0.05,
    "regulation_count": 100
  }
}
```

Practical behavior:

- `broker`: fixed reference pool
- `campaign`: fixed reference pool
- `channel`: controlled reference set aligned to base channels
- `claim`: percentage of eligible policies
- `complaint`: percentage of eligible policy/customers, linked to policy
- `override`: percentage of eligible policies
- `regulation`: fixed/capped reference pool, linked to complaints when complaints exist
- `insured_object`: derived from generated policy motor/home assets


## 10. Eligibility Rules

Enhanced records should not attach everywhere randomly.

Implemented or intended eligibility behavior:

- claims are generated from active policies
- overrides are generated from active policies
- complaints are generated from customers and linked to a policy from that customer's policy context
- broker links attach to AGENT-channel policy persons and their related policies/quotes
- campaign links attach to lead persons
- channel links attach to policies and quotes
- regulations link to complaints
- insured objects are generated from policy-linked home and motor assets


## 11. Base Rule Inheritance

Enhanced does not replace base rules. It inherits them for the base 52 tables inside enhanced output.

That means enhanced output is still expected to follow the base logic for:

- product catalog eligibility
- policy start/end ordering
- renewal timing
- lead-to-policy conversion timing
- customer/account lifecycle consistency
- asset/product consistency
- base enum vocabularies

Base validation layers that still matter conceptually:

- `validators/integrity_checker.py`
- `verify_csv.py`

Enhanced verification re-checks a subset of those base business rules directly on enhanced output.


## 12. Enhanced Business Rules

### 11.1 Policy and Channel

Rules:

- every policy-channel link must reference a valid policy and a valid channel
- every quote-channel link must reference a valid quote and a valid channel
- each policy should have a single enhanced channel link
- enhanced `sat_channel.channel_name` must match linked base `sat_policy.sales_channel`
- if `sales_channel = AGENT`, the related policy-holder person must have a broker reference in `link_broker_person`
- AGENT policy brokers should also be represented through `link_policy_broker` and `link_quote_broker`

### 11.2 Claims

Rules:

- every claim must link to an existing policy
- claims are expected only on active policies
- `claim_reported_date >= policy_start_date`
- `claim_reported_date <= policy_end_date` when policy end exists
- `claim_settlement_date >= claim_reported_date` when settlement exists
- claim reported, settlement, litigation, and recovery date fields must be timestamp values with time components
- no-recovery claims use `1900-01-01T00:00:00` for `first_recovery_date` and `last_recovery_date`
- recovery claims use lifecycle-safe recovery timestamps, and recovery numeric fields default to `0` when source values are absent
- `claim_channel` must match linked policy sales channel
- `claim_product` must match the linked policy product family

### 11.3 Complaints

Rules:

- every complaint must link to an existing policy
- the linked policy must resolve back to an existing customer
- `complaint_date >= customer_since`
- `complaint_acknowledgement_date >= complaint_date` when present
- `complaint_resolved_date >= complaint_acknowledgement_date` when acknowledgement exists
- otherwise `complaint_resolved_date >= complaint_date`
- complaint date, acknowledgement date, and resolved date must be timestamp values with time components
- complaint channel should align with the linked policy channel
- complaint insurance category should align with the linked policy product category

### 11.4 Overrides

### 11.4 Customer Enrichment

Rules:

- `sat_customer.customer_satisfaction` is a DDL `STRING`
- numeric `CustomerSatisfaction` source scores are mapped to labels:
  - `9-10` -> `VERY_SATISFIED`
  - `7-8` -> `SATISFIED`
  - `5-6` -> `NEUTRAL`
  - `<5` -> `DISSATISFIED`
  - missing or invalid -> `UNKNOWN`
- this avoids Databricks CSV inference treating the column as an integer

### 11.5 Overrides

Rules:

- every override must link to an existing policy
- overrides are expected only on active policies
- `override_reason` must be populated
- linked base policy `override_commission` must be populated when an override exists

### 11.6 Regulations

Rules:

- every regulation row must point to an existing regulation hub
- regulation-to-complaint links must point to existing regulation and complaint hubs
- `regulation_date_raised <= regulation_deadline_date`
- `regulation_date_raised <= regulation_date_closed` when close date exists
- regulation date fields must be timestamp values with time components
- DDL `STRING` boolean/flag columns must be string `Y` or `N`, not numeric `1` or `0` and not boolean-looking `TRUE` or `FALSE`
- DDL numeric columns are normalized before output: blank `INT`-style fields become `0`, and blank floating/decimal fields become `0.0`
- enhanced verification checks numeric columns from the DDL so all numeric fields remain populated and parseable

### 11.7 Broker, Campaign, and Insured Object

Rules:

- broker links must reference valid person and broker hubs
- broker references are required for persons behind AGENT-channel policies
- broker-policy and broker-quote links must reference valid generated objects
- campaign links must reference valid person and campaign hubs
- campaigns are generated from lead-side context
- campaign marketing metrics are populated from `FactQuote.csv` reference values
- `sat_campaign.is_active` is generated as `Y` or `N`, not as a numeric count
- `sat_campaign.campaign_start_date` and `sat_campaign.campaign_end_date` are timestamp values with time components
- insured objects must reference valid policy, home, or motor objects
- insured object dates should follow the linked policy start/end dates
- insured object start and end dates must be timestamp values with time components
- `insured_value` must be populated as a positive integer
- enhanced `hub_home` uses `insured_object_home_id` as its business ID column
- enhanced `hub_motor` uses `insured_object_motor_id` as its business ID column
- `link_insured_object_motor` uses `insured_object_motor_hash_key` as its primary key and `motor_hash_key` as its motor foreign key


## 13. Sample Data Usage Rules

Enhanced sample files are used as seed/reference sources for:

- broker details
- campaign details
- claim status/type/reason distributions
- complaint values
- override reasons
- regulation attributes
- enriched customer, quote, product, and policy attributes

Important rule:

- sample values can inform distributions
- base linkage and base domain alignment take priority over raw sample text

Example:

- sample complaint or claim channel text is not allowed to override the canonical base channel linkage
- instead, enhanced channel-related values are normalized back to the base channel set


## 14. SCD2 Support

Enhanced supports SCD2 generation in parallel to base.

Implemented behavior:

- first enhanced run creates baseline enhanced synthetic output
- later runs can create enhanced SCD2 rows under:
  - `data/scd2/enhanced/<run_id>`

Enhanced SCD2 follows the same general pattern as base:

- previous enhanced runs are used as history
- `10%` of eligible historical rows are sampled per configured enhanced satellite file
- changed satellite rows are emitted as new rows with the current run satellite load date
- the 10% rate is per satellite, not across all enhanced tables as one combined total

Examples:

- `10,000` eligible historical rows in `sat_address.csv` produce about `1,000` enhanced SCD2 rows for that file
- `100,000` eligible historical rows in one enhanced satellite produce about `10,000` enhanced SCD2 rows for that file

Enhanced satellites included in mutation support:

- `sat_broker.csv`
- `sat_campaign.csv`
- `sat_channel.csv`
- `sat_claim.csv`
- `sat_complaint.csv`
- `sat_insured_object.csv`
- `sat_override.csv`
- `sat_regulation.csv`


## 15. Verification

Enhanced verification is implemented in:

- `misc/verify_enhanced_synthetic.py`

It performs three layers of checking.

### 14.1 Structural Checks

- all expected enhanced files exist
- columns match enhanced DDL
- DDL `TIMESTAMP` columns contain parseable timestamp values with time components
- DDL numeric columns are populated and parse as the declared numeric type
- primary keys are unique and nonblank
- foreign keys match referenced parent tables

### 14.2 Shared Domain Checks

- base-controlled enums remain valid on enhanced output
- `sat_product.type` remains within the base product catalog
- channel fields remain within the shared base channel enum set

### 14.3 Business Rule Checks

- base policy date ordering
- base lead-to-policy ordering
- policy-channel alignment
- AGENT policy to broker alignment
- claim policy/date/channel/product alignment
- complaint policy/customer/date/channel/category alignment
- override policy/reason/commission alignment
- regulation date ordering

Expected successful verification summary:

- `80` enhanced tables found
- `25` hubs
- `30` links
- `25` satellites
- structural checks pass
- shared-domain checks pass
- enhanced business checks pass


## 16. Current Implementation Files

Main enhanced files:

- `generators/enhanced_synthetic_generator.py`
- `helper/enhanced_ddl.py`
- `helper/enhanced_rules.py`
- `misc/verify_enhanced_synthetic.py`
- `helper/scd2_generator.py`
- `main.py`

Base/shared files that influence enhanced behavior:

- `helper/source_context_builder.py`
- `helper/satellite_builder.py`
- `enums/product_catalog.py`
- `enums/sat_enums.py`
- `validators/integrity_checker.py`
- `verify_csv.py`


## 17. Run And Verify

Run the full generation flow:

```powershell
$env:PYTHONUTF8='1'
python .\main.py
```

Run enhanced output only:

```powershell
$env:PYTHONUTF8='1'
python .\main.py --enhanced-only
```

Enhanced-only mode still builds the base generation context in memory, then writes only `data/synthetic/enhanced/<run_id>` and enhanced SCD2 when prior enhanced history exists. It skips base CSV output, raw/source outputs, silver output, base validation, and base SCD2.

This writes:

- base output: `data/output/<run_id>`
- base normalized synthetic: `data/synthetic/base/<run_id>`
- enhanced synthetic: `data/synthetic/enhanced/<run_id>`
- enhanced SCD2 when prior enhanced history exists: `data/scd2/enhanced/<run_id>`

Verify the latest enhanced synthetic output:

```powershell
$env:PYTHONUTF8='1'
python .\misc\verify_enhanced_synthetic.py
```

Verify a specific enhanced run:

```powershell
$env:PYTHONUTF8='1'
python .\misc\verify_enhanced_synthetic.py .\data\synthetic\enhanced\<run_id>
```

Optional raw-to-silver rebuild:

```powershell
python .\misc\transform_all_raw_to_silver.py
python .\misc\verify_all_silver.py
```


## 18. Latest Verified State

Latest verified enhanced run during implementation:

- full local generation and verification against the active `enhanced_360/new` DDL
- latest full verified run: `data/synthetic/enhanced/20260520182650`

Verified outcomes:

- base generation passed
- enhanced synthetic generation passed
- base SCD2 generation passed with the `10%` per-satellite rule
- enhanced SCD2 generation passed with the `10%` per-satellite rule
- enhanced verification passed
- all `80` enhanced tables were generated
- schema, primary-key, foreign-key, enum, timeline, and enhanced business checks passed
- DDL-driven timestamp checks passed for all enhanced `TIMESTAMP` columns
- churn and policy-date alignment remained valid in the full silver verification path


## 19. Operational Notes

- enhanced verification is intentionally stricter than the original structural-only version
- shared domain alignment is treated as a hard rule for enhanced, especially for channels and products
