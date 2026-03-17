# SyncDataGenerator End-to-End Rules

This document describes the implemented behavior of the generator as it exists in the codebase.

It covers:

- execution flow
- entity creation sequence
- identifier rules
- load date rules
- business timeline rules
- product assignment rules
- SCD2 mutation rules
- validation rules


## 1. Purpose

The project generates synthetic Data Vault style CSV outputs for hubs, links, and satellites.

The main orchestration entry point is:

- `main.py`

The generator builds a run in this order:

1. metadata bootstrap
2. hub generation
3. quote to product assignment
4. policy generation
5. asset generation
6. link generation
7. satellite generation
8. output writing
9. validation
10. normalized synthetic export
11. SCD2-style delta generation
12. optional SCD2 update generation
13. optional SCD2 comparison reporting


## 2. Main Execution Flow

The implemented flow from person is:

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
-> `Motor` or `Home` asset depending on product

Important clarifications:

- `Contact`, `Identities`, and person-level `Home Address` are created for all persons.
- `Lead` is created only for persons selected into the lead population.
- `Consent`, `Marketing Preference`, and `Marketing Engagement` are created only for lead persons.
- `Quote` is created only for lead persons.
- `Policy` is created only from quotes.
- `Account` and `Customer` are created only for policy-holder persons.
- `Motor` and `Home` hubs are created only from policy/product combinations.


## 3. Configuration Inputs

Primary runtime config:

- `config/scenario_v1.json`
- `config/cardinality.json`
- `enums/product_catalog.py`

### `scenario_v1.json`

Controls:

- total people
- random seed
- natural vs legal split
- lifecycle distribution
- sales channel distribution
- conversion rates

Note:

- some lifecycle config fields exist but are not fully driving the current customer creation path.
- current customer creation is driven by policy-holder status, not by the `customers_initial` output of lifecycle assignment.

### `cardinality.json`

Controls min/max link counts for selected relationships.

Examples:

- `Link_Person_Contact`
- `Link_Person_Consent`
- `Link_Person_Lead`
- `Link_Quote_Person`
- `Link_Policy_Product`

### `product_catalog.py`

Controls available products and selection weights by person type.

Current catalog:

- `NATURAL`
  - `PRD_MOTOR_PERSONAL` weight `0.4`
  - `PRD_HOME_PERSONAL` weight `0.4`
  - `PRD_HEALTH_PERSONAL` weight `0.2`
- `LEGAL`
  - `PRD_COMMERCIAL_MOTOR` weight `0.6`
  - `PRD_PROPERTY_COMMERCIAL` weight `0.4`


## 4. Metadata Bootstrap Rules

The metadata preparation step is used to parse DDL and build ordered metadata JSON.

Files involved:

- `modules/module_parser.py`
- `modules/inference.py`
- `modules/openai_call.py`

Rules:

- if parsed DDL file already exists and is non-empty, parsing is skipped
- if ordered metadata file already exists and is non-empty, inference is skipped
- DDL parsing keeps `CREATE TABLE` and `ALTER TABLE FK` statements
- metadata ordering is based on inferred dependency graph


## 5. Identifier Rules

### Business ID format

Standard generated business IDs use:

`<PREFIX>_<run_id>_<zero_padded_sequence>`

Implemented by:

- `helper/key_factory.py`

Examples:

- `PER_20260312_174713_42_000001`
- `CUS_20260312_174713_42_000001`
- `PRD_20260312_174713_42_000001`

### Hash Key rules

Hash keys are MD5 hashes of business identifiers or relationship values.

Examples:

- hubs: hash of business ID
- links: hash of concatenated FK values

### Product identifier rule

`Hub_Product.Product Id` now follows the same generated business ID format as other hubs.

Product semantic meaning is preserved separately in-memory and exposed through:

- `Sat_Product.Type`

Examples of `Sat_Product.Type`:

- `PRD_MOTOR_PERSONAL`
- `PRD_HOME_PERSONAL`
- `PRD_COMMERCIAL_MOTOR`


## 6. Person and Subtype Rules

Files involved:

- `generators/person_generator.py`
- `helper/hub_builder.py`

Rules:

- total person count comes from config
- every generated person gets exactly one `Hub_Person` row
- every person is split into exactly one subtype:
  - natural
  - legal
- natural/legal split percentage comes from `natural_person_pct`

Validation expectation:

- a person must never be both natural and legal
- a person must never be neither natural nor legal


## 7. Supporting Hub Rules

Files involved:

- `generators/supporting_generator.py`
- `helper/hub_builder.py`

Rules:

- every person gets exactly one contact
- every person gets exactly one identity
- every person gets exactly one person-level home address
- every lead person gets exactly one consent
- every lead person gets exactly one marketing preference
- every lead person gets one or more marketing engagement rows based on cardinality
- only policy-holder persons get accounts


## 8. Lifecycle and Lead Rules

Files involved:

- `generators/lifecycle_generator.py`
- `helper/hub_builder.py`
- `helper/link_builder.py`

Rules:

- lead population is selected from lifecycle distribution
- each lead person can have 1 to 2 lead rows by default
- `Link_Person_Lead` links persons to all generated lead rows

### `Sat_Lead.Interested Level`

`Interested Level` is no longer random.

Current implemented rule:

- `HIGH`
  - if the lead person has a quote
  - or if the lead person became a policy holder
- `MEDIUM`
  - if the lead person has marketing engagement
  - or if person score is `>= 70`
- `LOW`
  - otherwise

This makes the lead interest level behavior-driven instead of independent of funnel progression.

### `Sat_Person.Is Lead`

Implemented rule:

- `Y` if the person has at least one `Link_Person_Lead`
- `N` otherwise

It is no longer random.


## 9. Consent Rules

Files involved:

- `generators/supporting_generator.py`
- `helper/satellite_builder.py`

Rules:

- consent is generated only for lead persons
- each lead person gets exactly one consent row

### `Sat_Person.Operational Paperless Consent`

Implemented rule:

- `Y` if the person has at least one `Link_Person_Consent`
- `N` otherwise

It is no longer random.


## 10. Quote Rules

Files involved:

- `generators/transaction_generator.py`
- `main.py`

Rules:

- only lead persons can get quotes
- each quote links to exactly one person
- each quote links to exactly one product
- quote product selection depends on person type and product catalog weights

Validation expectation:

- every quote person must be a lead person


## 11. Policy Rules

Files involved:

- `generators/transaction_generator.py`
- `helper/satellite_builder.py`

Rules:

- policies are created only from quotes
- quote to policy conversion is probabilistic
- each policy is linked to exactly one customer
- each policy is linked to exactly one product
- only policy-holder persons get customer and account rows

### Current implemented policy lifecycle

The current generator enforces these policy rules:

- first policy tenure is `1 year`
- `Policy Length = 12`
- `Policy Start Date = latest Lead Converted Date + 1 to 90 days` when lead history exists
- fallback policies without usable lead history still get a valid start date capped by policy satellite load date
- non-cancelled policies have:
  - `Policy End Date = Policy Start Date + 1 year`
- cancelled policies have:
  - `Policy End Date` before the planned annual end date
  - `Policy Status = CANCELLED`
- non-cancelled policies use date-driven status:
  - `ACTIVE` if `Policy End Date > policy satellite Load Date`
  - `LAPSED` if `Policy End Date <= policy satellite Load Date`
- `Renewal Date = Policy End Date - 0 to 10 days`
- `Renewal Amount Next Period = Renewal Amount Current Period * 1.01`

Important clarification:

- the current schema does not model renewal as a separate new policy term row
- renewal is represented as fields on the same policy satellite row
- current allowed statuses are:
  - `ACTIVE`
  - `LAPSED`
  - `CANCELLED`
- `EXPIRED` is not currently used

### `Sat_Policy.Fraud Flag`

`Fraud Flag` is no longer random.

Current implemented rule:

- start with fraud risk score `0`
- add:
  - `+2` if `Declined Claims > 0`
  - `+1` if `Number of Previous Claim >= 3`
  - `+1` if `Number of Active Claim >= 2`
  - `+2` if `Policy Status = CANCELLED`
  - `+1` if `Policy Status = LAPSED`
  - `+2` if related `Account Status = SUSPENDED`
  - `+3` if related `Account Status = CLOSED`
- set:
  - `Fraud Flag = Y` if total fraud risk score is `>= 3`
  - `Fraud Flag = N` otherwise

Dependency direction:

- policy and account risk signals are generated first
- `Fraud Flag` is then derived from those signals
- customer segment and customer rating use the resulting fraud flag as a negative input


## 12. Customer and Account Rules

Files involved:

- `generators/lifecycle_generator.py`
- `generators/supporting_generator.py`
- `main.py`

Rules:

- `Hub_Customer` is created only for policy-holder persons
- `Hub_Account` is created only for policy-holder persons
- customer and account are created from the same policy-holder person set

Important clarification:

- customer is not derived from account
- account is not derived from customer
- both are derived from policy-holder status

### Account lifecycle

`Sat_Account.Account Status` is lifecycle-driven.

Current implemented statuses:

- `OPEN`
- `SUSPENDED`
- `CLOSED`

Current implemented rules:

- all account business dates must be `<= Sat_Account.Load Date`
- `OPEN`
  - `Account Last Access >= Account Last Change`
- `SUSPENDED`
  - `Account Last Access <= Account Last Change`
- `CLOSED`
  - `Account Last Access <= Account Last Change`

### Account to policy dependency

Policy lifecycle is not fully independent of account state.

Current implemented rule:

- if the related account is `SUSPENDED` or `CLOSED`
- then the related policy cannot remain active
- current implementation forces the policy into a non-active path
  - currently `CANCELLED`


## 13. Product and Asset Rules

Files involved:

- `enums/product_catalog.py`
- `generators/product_generator.py`
- `generators/transaction_generator.py`
- `helper/satellite_builder.py`

Rules:

- product hub rows are built from the catalog
- quote products are chosen from the catalog based on person type
- policies inherit product meaning from the converted quote
- products containing `MOTOR` create motor assets
- products containing `HOME` or `PROPERTY` create home assets
- home assets also get asset-level home addresses

### Current product meaning rules

- natural persons can receive:
  - motor personal
  - home personal
  - health personal
- legal persons can receive:
  - commercial motor
  - property commercial


## 14. Load Date Rules

Files involved:

- `main.py`

Rules:

- hub, link, and satellite load dates are derived from a root timestamp
- root timestamp is based on previous run load date if available, otherwise current timestamp
- the current clock time is preserved in new runs
- load dates include `HH:MM:SS`

Implemented sequencing:

- `HUB_DATE = root + random day offset`
- `LINK_DATE = HUB_DATE + 5 days`
- `SAT_DATE = LINK_DATE + 5 days`

Validation expectation:

- `hub load date < link load date < sat load date`

### Historical business dates vs load date

Historical business-event dates are now capped to their satellite `Load Date`.

Currently enforced or validated as `<= Load Date`:

- `Sat_Lead.Converted Date <= Sat_Lead.Load Date`
- `Sat_Policy.Policy Start Date <= Sat_Policy.Load Date`
- `Sat_Customer.Customer Since <= Sat_Customer.Load Date`
- `Sat_Legal_Person.Converted Date <= Sat_Legal_Person.Load Date`
- `Sat_Legal_Person.Date of Constitution <= Sat_Legal_Person.Load Date`
- `Sat_Natural_Person.Birth Date <= Sat_Natural_Person.Load Date`

Dates intentionally allowed to be after `Load Date`:

- `Sat_Policy.Policy End Date`
- `Sat_Policy.Renewal Date`

because those can represent future contractual milestones for active policies.


## 15. Business Timeline Rules

Files involved:

- `helper/satellite_builder.py`
- `main.py`
- `verify_csv.py`
- `validators/integrity_checker.py`

### Lead to policy timeline

Implemented rule:

- `Lead.Converted Date < Policy Start Date < Policy End Date`

The generator enforces this by:

- generating policy start from the latest lead conversion for the person
- applying a `1 to 90 day` lead-to-policy conversion window
- capping policy start so it cannot exceed policy satellite load date

Validation expectation:

- every lead conversion tied to a policy-holder person must be earlier than that person's earliest policy start
- lead-to-policy window must stay within `1 to 90 days`

### Policy lifecycle timeline

Implemented rules:

- `Policy Start Date <= Policy Load Date`
- `Policy End Date > Policy Start Date`
- non-cancelled policies must be annual policies
- `Renewal Date` must be within `0 to 10 days` before `Policy End Date`
- renewal uplift must be exactly `1%`
- policy status must match policy dates relative to the policy row `Load Date`

Validation expectation:

- `policy_start_end`
- `policy_annual_duration`
- `policy_renewal_window`
- `policy_renewal_uplift`
- `policy_status_timeline`

### Customer since timeline

Implemented rule:

- `Customer Since <= earliest policy start date for that person`

Generator behavior:

- `sat_customer()` uses earliest policy start as an upper bound when available

### Customer segment and rating

`Customer Segment` is no longer random.

Current implemented segment rule:

- `PREMIUM`
  - if enough positive signals exist across:
    - active policy
    - open account
    - high NPS
    - higher policy revenue
- `STANDARD`
  - otherwise

Negative signals reduce premium eligibility:

- fraud flag
- cancelled policy
- lapsed policy

`Customer Rating` is derived after segment assignment and is clamped to `1..5`.

Current implemented rating inputs include:

- customer status
- customer segment
- NPS
- policy status
- account status
- fraud flag
- declined claims

Rating guardrail:

- final customer rating is always clamped to the valid range
  - minimum `1`
  - maximum `5`

### Other datetime fields

These now include real `HH:MM:SS` values:

- natural person birth date
- legal person converted date
- legal person date of constitution
- lead converted date
- customer since
- policy start date
- policy end date
- renewal date

Not every datetime in satellites participates in one universal global sequence.

Historical dates should be interpreted as already occurred by the time of the row load.

Future dates should be interpreted as contractual milestones where applicable.


## 16. Satellite Attribute Consistency Rules

Files involved:

- `helper/satellite_builder.py`

### Person profile consistency

The generator caches a canonical person profile so related attributes remain aligned across satellites.

Examples of shared values:

- name
- gender
- marital status
- email
- hashed email
- occupation

### Address consistency

The generator caches home address values so:

- `Sat_Home_Address`
- `Sat_Home`
- `Sat_Motor`

can reuse consistent city/postcode/state values when linked.


## 17. Validation Rules

Two validation layers exist:

- `validators/integrity_checker.py`
- `verify_csv.py`

### `integrity_checker.py`

Checks:

- hub/link referential integrity
- policy date sanity
- lead-to-policy timeline sanity

### `verify_csv.py`

Checks:

- required files exist
- hub uniqueness
- person subtype correctness
- link cardinality rules
- quote rules
- policy rules
- lead-dependent object rules
- policy-holder customer/account rules
- product/asset FK sanity
- `Sat_Person.Is Lead` consistency
- `Sat_Person.Operational Paperless Consent` consistency
- load date sequence
- historical business dates before or on satellite load date
- `Lead.Converted Date < Policy Start Date`
- lead-to-policy window `1 to 90 days`
- `Policy Start Date <= Policy End Date`
- annual policy duration for non-cancelled policies
- renewal date window `0 to 10 days`
- renewal uplift exactly `1%`
- policy status timeline consistency relative to policy load date
- account lifecycle dates before or on account load date
- account open and non-open timeline consistency
- account to policy status consistency


## 18. SCD2 Mutation Rules

Files involved:

- `helper/scd2_generator.py`
- `enums/sat_enums.py`
- `update_scd2_records.py`
- `compare_scd2_updates.py`

Rules:

- SCD2 generator mutates a small percentage of rows from prior normalized satellite files
- mutation candidates are controlled by a per-file column map
- enum-backed columns mutate using `SAT_ENUMS`
- email and phone mutate through fallback logic when not enum-backed

Examples:

- `sat_account.account_status`
- `sat_customer.customer_status_reason`
- `sat_policy.cover_option`
- `sat_contact.personal_email`

Important note:

- the SCD2 generator is only partially driven by `SAT_ENUMS`
- it uses `SAT_ENUMS` for columns it is configured to mutate

### Existing helper

`helper/scd2_generator.py` creates delta-style SCD2 satellite rows from historical normalized satellite runs.

Current implemented behavior:

- input history is the normalized `synthetic_data` root
- the current run is excluded from the historical candidate pool
- all previous normalized runs are scanned
- for each `sat_*.csv`:
  - rows from all previous runs are combined
  - the latest known version per business key is retained based on `load_date`
- mutation sampling is taken from that latest-version pool
- selected rows are mutated using the configured per-file column rules
- new changed rows are written with the current run `SAT_DATE`

This means the helper no longer depends only on the immediately previous run.
It now produces the current run delta from the latest historical version across all prior runs.

### New standalone updater

`update_scd2_records.py` is a separate utility and does not modify the existing helper.

It:

- reads an existing `scd2_sat/<run_id>` folder
- mutates existing SCD2 rows using the same style of mutation rules
- writes the result to:
  - `scd2_updated/<run_id>`

Behavior:

- defaults to updating all rows in each `sat_*.csv`
- can optionally accept a replacement `sat-date`
- uses the same enum/fallback mutation model as the helper

### New standalone comparison tool

`compare_scd2_updates.py` compares:

- `scd2_sat/<run_id>`
- `scd2_updated/<run_id>`

and reports:

- changed files
- changed rows
- changed columns
- sample before/after value changes


## 19. Output Locations

Primary outputs:

- `output/<run_id>`

Normalized export:

- `synthetic_data/<run_id>`

SCD2-style delta output:

- `scd2_sat/<current_run_id>`

Updated SCD2 output:

- `scd2_updated/<run_id>`


## 20. Operational Notes

### If you change products

Edit:

- `enums/product_catalog.py`

This is now the single source of truth for:

- available product codes
- product selection weights by person type

### If you change cardinalities

Edit:

- `config/cardinality.json`

### If you change lifecycle population sizes

Edit:

- `config/scenario_v1.json`

### If you change policy lifecycle rules

Edit:

- `helper/satellite_builder.py`
- `verify_csv.py`

Those two should stay aligned.


## 21. Known Design Notes

- lifecycle config exists beyond what is currently used in final customer generation
- customer creation currently depends on policy-holder status, not initial lifecycle customer status
- product meaning is preserved in `Sat_Product.Type`, while `Hub_Product.Product Id` now follows standard generated business ID format
- not all satellite datetime fields are part of one global business-event sequence
- renewal is represented on the same policy row, not as a separate next-term policy record


## 22. Recommended Commands

After generating a run:

```bash
python main.py
python verify_csv.py synthetic_data/<run_id>
```

If you want to validate a default folder without passing a path:

```bash
python verify_csv.py
```

To update existing SCD2 rows into a new folder:

```bash
python update_scd2_records.py --input scd2_sat/<run_id>
```

To compare original and updated SCD2 rows:

```bash
python compare_scd2_updates.py --original scd2_sat/<run_id> --updated scd2_updated/<run_id>
```

Important note about standard pipeline behavior:

- the normal pipeline SCD2 flow uses all previous normalized runs as history input
- it writes only the new delta for the current run
- `update_scd2_records.py` remains an optional standalone utility and is not required for the normal SCD2 pipeline


## 23. Source Files Most Relevant to Rules

- `main.py`
- `helper/hub_builder.py`
- `helper/link_builder.py`
- `helper/satellite_builder.py`
- `generators/person_generator.py`
- `generators/lifecycle_generator.py`
- `generators/supporting_generator.py`
- `generators/product_generator.py`
- `generators/transaction_generator.py`
- `enums/product_catalog.py`
- `enums/sat_enums.py`
- `validators/integrity_checker.py`
- `verify_csv.py`
- `update_scd2_records.py`
- `compare_scd2_updates.py`
