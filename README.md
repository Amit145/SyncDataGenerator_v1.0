# SyncDataGenerator v1.0

Synthetic Data Vault-style data generator for hubs, links, satellites, validations, and SCD2-style updates.

This README is the root-level operational guide. The detailed technical rulebook is in:

- `docs/end_to_end_generation_rules.md`


## 1. What This Project Does

The generator creates synthetic CSV data for a Data Vault-style model including:

- hubs
- links
- satellites
- normalized output copies
- SCD2-style satellite changes
- updated SCD2 change sets
- comparison output between original and updated SCD2 rows

Primary orchestration entry point:

- `main.py`


## 2. High-Level Flow

Implemented business flow:

`Person`
-> `Natural Person` or `Legal Person`
-> `Contact`, `Identities`, `Home Address`
-> `Lead`
-> `Consent`
-> `Marketing Preference`
-> `Marketing Engagement`
-> `Quote`
-> `Policy`
-> `Account`
-> `Customer`
-> `Motor` or `Home`

Important behavior:

- all persons get contact, identity, and person-level home address
- only leads get consent, marketing, and quotes
- only policy holders get customer and account
- products and assets are assigned from policy/product meaning


## 3. Main Files

Core generation:

- `main.py`
- `helper/hub_builder.py`
- `helper/link_builder.py`
- `helper/satellite_builder.py`

Generators:

- `generators/person_generator.py`
- `generators/lifecycle_generator.py`
- `generators/supporting_generator.py`
- `generators/product_generator.py`
- `generators/transaction_generator.py`

Validation:

- `verify_csv.py`
- `validators/integrity_checker.py`

SCD2-related:

- `helper/scd2_generator.py`
- `update_scd2_records.py`
- `compare_scd2_updates.py`

Configuration:

- `config/scenario_v1.json`
- `config/cardinality.json`
- `enums/product_catalog.py`
- `enums/sat_enums.py`


## 4. Product Catalog

Product definitions and weights are centralized in:

- `enums/product_catalog.py`

Current categories:

- `NATURAL`
  - `PRD_MOTOR_PERSONAL`
  - `PRD_HOME_PERSONAL`
  - `PRD_HEALTH_PERSONAL`
- `LEGAL`
  - `PRD_COMMERCIAL_MOTOR`
  - `PRD_PROPERTY_COMMERCIAL`

`Hub_Product.Product Id` now uses the same generated business ID format as other hubs.

Semantic product meaning is preserved in:

- `Sat_Product.Type`


## 5. Current Business Rules

### Person and subtype

- every person is exactly one of:
  - natural
  - legal

### Lead and consent

- `Sat_Person.Is Lead = Y` only if person has `Link_Person_Lead`
- `Sat_Person.Operational Paperless Consent = Y` only if person has `Link_Person_Consent`

### Quote rules

- only leads can have quotes
- every quote links to exactly one person
- every quote links to exactly one product

### Policy rules

- policies are created from quotes
- first policy tenure is `1 year`
- `Policy Length = 12`
- `Policy Start Date = latest lead converted date + 1 to 90 days`
- `Policy End Date = Policy Start Date + 1 year` for non-cancelled policies
- `Renewal Date = Policy End Date - 0 to 10 days`
- `Renewal Amount Next Period = Renewal Amount Current Period * 1.01`
- statuses used:
  - `ACTIVE`
  - `LAPSED`
  - `CANCELLED`

Status meaning:

- `ACTIVE` if policy end is after policy load date
- `LAPSED` if policy end is on/before policy load date
- `CANCELLED` for early-cancelled policies

### Account lifecycle rules

Account statuses:

- `OPEN`
- `SUSPENDED`
- `CLOSED`

Lifecycle behavior:

- account dates are kept on or before account load date
- `OPEN` accounts keep access on/after last change
- `SUSPENDED` and `CLOSED` keep access on/before last change
- non-open account holders cannot have `ACTIVE` policies


## 6. Load Dates and Business Dates

Load dates:

- hub, link, and satellite load dates include `HH:MM:SS`
- sequencing is:
  - `Hub Load Date < Link Load Date < Sat Load Date`

Historical business dates are capped to satellite load date:

- `Lead Converted Date <= Sat_Lead.Load Date`
- `Policy Start Date <= Sat_Policy.Load Date`
- `Customer Since <= Sat_Customer.Load Date`
- `Legal Person Converted Date <= Sat_Legal_Person.Load Date`
- `Date of Constitution <= Sat_Legal_Person.Load Date`
- `Birth Date <= Sat_Natural_Person.Load Date`

Dates that may be after load date:

- `Policy End Date`
- `Renewal Date`


## 7. Timeline Rules

Currently validated timelines:

- `Lead Converted Date < Policy Start Date < Policy End Date`
- lead-to-policy conversion window is `1 to 90 days`
- policy renewal window is `0 to 10 days` before end date
- non-cancelled policy duration is exactly `1 year`
- renewal uplift is exactly `1%`


## 8. Validation

Primary validator:

- `verify_csv.py`

It checks:

- required files exist
- hub uniqueness
- person subtype correctness
- link cardinality rules
- quote rules
- policy rules
- lead-dependent rules
- policy-holder customer/account rules
- product/asset sanity
- `Sat_Person` flag consistency
- load date sequence
- historical business dates before load date
- policy annual duration
- renewal window
- renewal uplift
- policy status timeline
- account lifecycle rules
- account-to-policy lifecycle consistency


## 9. Output Folders

Generated raw output:

- `output/<run_id>`

Normalized output:

- `synthetic_data/<run_id>`

Generated SCD2-style delta output:

- `scd2_sat/<run_id>`

Updated SCD2 output:

- `scd2_updated/<run_id>`


## 10. Commands

Generate a run:

```bash
python main.py
```

Validate a run:

```bash
python verify_csv.py synthetic_data/<run_id>
```

Validate default configured folder:

```bash
python verify_csv.py
```

Update existing SCD2 rows into a new folder:

```bash
python update_scd2_records.py --input scd2_sat/<run_id>
```

Compare original and updated SCD2 rows:

```bash
python compare_scd2_updates.py --original scd2_sat/<run_id> --updated scd2_updated/<run_id>
```


## 11. Configuration Files

Main runtime config:

- `config/scenario_v1.json`

Link cardinality config:

- `config/cardinality.json`

Product catalog:

- `enums/product_catalog.py`

Satellite enum values:

- `enums/sat_enums.py`


## 12. Notes

- customer creation is currently driven by policy-holder status
- some lifecycle config exists beyond what is currently used in final customer generation
- renewal is represented on the same policy row, not as a separate renewed policy term row
- the technical source of truth for implemented rules is:
  - `docs/end_to_end_generation_rules.md`
