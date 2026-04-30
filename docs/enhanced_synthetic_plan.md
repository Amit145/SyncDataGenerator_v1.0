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
- plus 21 enhanced-only tables
- total expected enhanced tables: `73`


## 2. Source Inputs

Enhanced schema contract:

- `enhanced_360/Enhanced_Customer360_DataVault_DDL.sql`

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

- `24` hubs
- `25` links
- `24` satellites
- `73` tables total

Enhanced-only additions:

- `7` hubs
- `7` links
- `7` satellites

Enhanced-only entity groups:

- `broker`
- `campaign`
- `channel`
- `claim`
- `complaints`
- `override`
- `regulations`


## 4. Output Locations

Implemented output layout:

- enhanced synthetic: `data/synthetic/enhanced/<run_id>`
- enhanced SCD2: `data/scd2/enhanced/<run_id>`

Enhanced output is fully separate from base output.


## 5. Core Integration Rule

Base 360 is the spine.

Enhanced records are generated from the same run context as base and must attach to valid base entities.

Enhanced entities are linked as follows:

- `broker -> person` via `link_person_broker`
- `campaign -> person` via `link_person_campaign`
- `channel -> policy` via `link_policy_channel`
- `claim -> policy` via `link_policy_claim`
- `complaints -> customer` via `link_complaints_customer`
- `override -> policy` via `link_override_policy`
- `regulations -> product` via `link_regulations_product`

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


## 6. Shared Domain Rules

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


## 7. Product Rules

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
- enhanced regulations must link to valid generated products

Current product-family mapping used for enhanced validation/generation:

- motor-like codes -> `Motor`
- home/property-like codes -> `Home`
- health -> `Health`
- travel -> `Travel`
- cyber -> `Cyber`


## 8. Count Rules

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
- `complaints`: percentage of eligible customers
- `override`: percentage of eligible policies
- `regulations`: fixed/capped reference pool


## 9. Eligibility Rules

Enhanced records should not attach everywhere randomly.

Implemented or intended eligibility behavior:

- claims are generated from active policies
- overrides are generated from active policies
- complaints are generated from customers and aligned to customer policy context
- broker links attach to persons involved in policy flow
- campaign links attach to lead persons
- channel links attach to policies
- regulations link to products


## 10. Base Rule Inheritance

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


## 11. Enhanced Business Rules

### 11.1 Policy and Channel

Rules:

- every policy-channel link must reference a valid policy and a valid channel
- each policy should have a single enhanced channel link
- enhanced `sat_channel.channel_name` must match linked base `sat_policy.sales_channel`
- if `sales_channel = AGENT`, the related policy-holder person must have a broker reference in `link_person_broker`

### 11.2 Claims

Rules:

- every claim must link to an existing policy
- claims are expected only on active policies
- `claim_reported_date >= policy_start_date`
- `claim_reported_date <= policy_end_date` when policy end exists
- `claim_settlement_date >= claim_reported_date` when settlement exists
- `claim_channel` must match linked policy sales channel
- `claim_product` must match the linked policy product family

### 11.3 Complaints

Rules:

- every complaint must link to an existing customer
- only customers can complain
- `complaint_date >= customer_since`
- `complaint_acknowledgement_date >= complaint_date` when present
- `complaint_resolved_date >= complaint_acknowledgement_date` when acknowledgement exists
- otherwise `complaint_resolved_date >= complaint_date`
- complaint channel should align with one of the linked customer policy channels
- complaint insurance category should align with the customer policy product portfolio

### 11.4 Overrides

Rules:

- every override must link to an existing policy
- overrides are expected only on active policies
- `override_reason` must be populated
- linked base policy `override_commission` must be populated when an override exists

### 11.5 Regulations

Rules:

- every regulation link must point to an existing product
- `regulation_date_raised <= regulation_deadline_date`
- `regulation_date_raised <= regulation_date_closed` when close date exists

### 11.6 Broker and Campaign

Rules:

- broker links must reference valid person and broker hubs
- broker references are required for persons behind AGENT-channel policies
- campaign links must reference valid person and campaign hubs
- campaigns are generated from lead-side context


## 12. Sample Data Usage Rules

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


## 13. SCD2 Support

Enhanced supports SCD2 generation in parallel to base.

Implemented behavior:

- first enhanced run creates baseline enhanced synthetic output
- later runs can create enhanced SCD2 rows under:
  - `data/scd2/enhanced/<run_id>`

Enhanced SCD2 follows the same general pattern as base:

- previous enhanced runs are used as history
- changed satellite rows are emitted as new rows with the current run satellite load date

Enhanced satellites included in mutation support:

- `sat_broker.csv`
- `sat_campaign.csv`
- `sat_channel.csv`
- `sat_claim.csv`
- `sat_complaints.csv`
- `sat_override.csv`
- `sat_regulations.csv`


## 14. Verification

Enhanced verification is implemented in:

- `misc/verify_enhanced_synthetic.py`

It performs three layers of checking.

### 14.1 Structural Checks

- all expected enhanced files exist
- columns match enhanced DDL
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
- complaint customer/date/channel/category alignment
- override policy/reason/commission alignment
- regulation date ordering

Expected successful verification summary:

- `73` enhanced tables found
- `24` hubs
- `25` links
- `24` satellites
- structural checks pass
- shared-domain checks pass
- enhanced business checks pass


## 15. Current Implementation Files

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


## 16. Run And Verify

Run the full generation flow:

```powershell
$env:PYTHONUTF8='1'
python .\main.py
```

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


## 17. Latest Verified State

Latest verified enhanced run during implementation:

- `20260430140901`

Verified outcomes:

- base generation passed
- base integrity passed
- enhanced synthetic generation passed
- enhanced verification passed
- enhanced SCD2 was generated


## 18. Operational Notes

- `main.py` still prints Unicode log symbols, so on this Windows console `PYTHONUTF8=1` is recommended
- enhanced verification is intentionally stricter than the original structural-only version
- shared domain alignment is treated as a hard rule for enhanced, especially for channels and products
