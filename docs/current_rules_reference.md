# Current Rules Reference

This document describes the currently implemented generation and validation rules for the base synthetic vault, raw/source outputs, silver rebuilds, enhanced outputs, MLOps outputs, churn KPI fields, and SCD2 outputs.

For scenario configuration meanings, see [scenario_config_reference.md](F:/SyncDataGenerator_v1.0/docs/scenario_config_reference.md).

For the latest generated-run validation results and expected-vs-current churn ratios, see [latest_run_validation.md](F:/SyncDataGenerator_v1.0/docs/latest_run_validation.md).

## Output Scope

Default `main.py` generation creates the synthetic workflow only:

- base vault output under `data/output/<run_id>`
- normalized synthetic base output under `data/synthetic/base/<run_id>`
- enhanced synthetic output under `data/synthetic/enhanced/<run_id>`
- MLOps synthetic output under `data/synthetic/mlops/<run_id>`
- synthetic base/enhanced/MLOps SCD2 outputs when prior synthetic history exists

Raw, canonical, silver, and raw SCD2 outputs are disabled by default. Generate them only when needed with `--include-raw-silver`.

`new_outputs_src` is also disabled by default. It is generated only when `--include-raw-silver --include-new-outputs-src` is passed.

## Scenario Rules

`config/scenario_v1.json` controls run scale, lifecycle mix, channel mix, conversion rates, enhanced-output behavior, and churn distributions.

Key behavior:

- `run_settings.total_people` controls the base population size for normal runs.
- `lifecycle_distribution` controls the broad person lifecycle mix before conversion logic is applied.
- `sales_channel_distribution` controls generated sales-channel mix for source generation.
- `conversion_rates` controls quote-to-policy conversion behavior.
- `enhanced_settings` controls enhanced output volume and related enhanced tables.
- `churn_settings` controls churn-related bands, weights, and status/channel behavior.

Weights in config are relative weights. They do not need to add to 1 or 100. Probability values are decimal probabilities from `0.0` to `1.0`.

## Entity Rules

People are generated once per base context and are reused consistently across related base satellites.

Person-level rules:

- every person has one hub person row
- person subtype is generated from configured lifecycle behavior
- identity, contact, address, and related profile rows are linked to the same person key
- birth date, driver experience, occupation, consent, marketing, and customer profile fields are aligned through shared person context

Lead and quote rules:

- leads are generated from eligible people
- quotes are generated for quote-capable leads
- quote status controls whether a policy can be created
- accepted/converted quotes can become policies
- declined or abandoned quotes do not create active policy holder relationships

Customer and account rules:

- customer rows are generated only for policy holders
- account rows are generated only where policy/customer context requires them
- account status is aligned to policy behavior
- closed/suspended account behavior can force or restrict policy status behavior
- customer status, account status, and policy status are kept directionally consistent
- churn calibration keeps closed/suspended account policies locked as churned and does not flip them back to active

Asset rules:

- motor assets are generated for motor policy context
- vehicle make/model/segment values are aligned
- vehicle model segment feeds churn KPI validation
- driver-experience proxy is derived from person age and is validated as a churn proxy field

## Policy And Date Rules

Policy status, policy start date, policy end date, renewal date, and policy cycle are generated together.

Policy date behavior:

- `Policy Start Date` is the beginning of the policy tenure.
- `Policy End Date` is based on the current policy term and status.
- `Renewal Date` is aligned to policy end/current-term behavior.
- `Policy Cycle` means completed annual policy tenure, not number of policies purchased.
- Higher completed `Policy Cycle` means longer completed tenure.
- Enhanced `sat_policy.policy_issue_date` is equal to `policy_start_date`.

Status behavior:

- `ACTIVE` policies are valid for the current term and can renew.
- `LAPSED` policies require at least one completed annual renewal cycle.
- sub-one-year churn is represented as `CANCELLED`, not `LAPSED`.
- `CANCELLED` policies can end before the full expected tenure window.
- non-open account states cannot continue to imply active policy behavior.

The MLOps interpretation of `Policy Cycle` is supported as completed tenure. Churn probability decreases as completed tenure increases.

## Churn Source Rules

Churn fields are based on available/proxy rows from `new_rules/Data Req Churn NPS.xlsx`.

The implemented workbook mapping follows:

- column E marks whether a row is available or proxy
- column F identifies the source table or source columns
- column G describes the logic
- column H describes distribution logic or expected distribution
- column I gives examples
- column J gives business rules

Available and proxy churn fields are generated at the base satellite layer first. Raw, canonical raw, silver, and enhanced outputs inherit those values through normal generation and transformation paths.

Configured churn distributions live under `churn_settings` in `config/scenario_v1.json`.

## Churn KPI Rules

Premium rules:

- current premium amount follows configured low/medium/high/very-high bands
- current premium amount churn is configurable and validated against workbook ranges: low `10-18%`, medium `15-25%`, high `25-40%`, and very high `40-55%`
- churn probability increases as current annual premium amount rises
- renewal current amount follows configured current-premium band weights
- renewal next amount follows configured movement bands
- movement bands include decreases, `0-5%`, `5-10%`, and `>10%`
- percentage premium increase churn is configurable and validated against workbook ranges: `<0%` movement `8-12%`, `0-5%` movement `15-20%`, `5-10%` movement `25-35%`, and `>10%` movement `45-65%`
- churn probability increases as percentage renewal premium increase rises
- absolute premium increase churn is configurable and validated against workbook ranges: `<=0` increase `8-12%`, `1-50` increase `15-22%`, `51-100` increase `25-38%`, and `>100` increase `45-65%`
- churn probability increases as absolute renewal premium increase rises
- premium values are validated by band, not by exact repeated values

Claim rules:

- claim count follows configured churn claim-count weights
- active policies can use separate active claim-count weights
- declined claims follow configured declined-claim weights
- claim-count distribution is validated as a churn KPI input
- claim-count churn is configurable and validated against workbook ranges: `0` claims `12-18%`, `1` claim `20-30%`, `2` claims `30-45%`, and `3+` claims `45-60%`
- churn probability increases as recent claim count increases

Add-on and coverage rules:

- cover option/add-on behavior follows configured cover-option weights
- add-on counts are validated as a churn feature
- add-on churn is configurable and validated against workbook ranges: `0` add-ons `25-40%`, `1` add-on `18-28%`, `2` add-ons `12-22%`, and `3+` add-ons `8-18%`
- churn probability decreases as add-on count increases
- coverage behavior remains consistent with policy/product context
- after initial policy status sampling, a weighted calibration pass can adjust selected policies between active and churned statuses when the flip improves the total workbook fit across premium-increase, claim-count, add-on, marketing, vehicle, driver, current-premium, and tenure bands while preserving date, renewal, and non-open-account rules

Marketing and engagement proxy rules:

- marketing engagement band follows configured marketing proxy weights
- marketing engagement churn is configurable and validated against workbook ranges: high `8-15%`, medium `18-30%`, low `35-55%`, and none `50-70%`
- churn probability increases as marketing engagement decreases
- service-call band follows configured service proxy weights
- marketing and service proxies are validated as directional churn fields

Vehicle and driver proxy rules:

- vehicle model maps to a configured vehicle segment
- vehicle segment distribution follows configured churn weights
- vehicle segment churn is configurable, calibrated, and validated where direct policy-to-motor links exist against workbook ranges: standard `12-22%`, premium `20-35%`, and high-risk `30-50%`
- motor policy churn probability increases by vehicle segment risk: standard lower, premium medium, high-risk higher
- motor policies preassign a vehicle segment before policy status is sampled, and the same vehicle profile is reused in `sat_motor`
- driver experience is derived from `sat_natural_person.birth_date` as a proxy when no driving licence issue date exists; the proxy is `max(age - 17, 0)`
- driver experience churn is configurable, calibrated, and validated against workbook ranges: `<2y` `25-40%`, `2-5y` `18-30%`, `6-10y` `15-25%`, and `>10y` `10-18%`
- driver churn probability decreases as proxy driving experience increases

Tenure churn rules:

- `Policy Cycle` is completed annual tenure
- `<1` completed cycle has the highest churn probability and is targeted to the workbook range `35-50%`
- `1-2` completed cycles has lower churn than `<1` and is targeted to `25-35%`
- `3-5` completed cycles has lower churn than `1-2` and is targeted to `15-25%`
- `>5` completed cycles has the lowest churn and is targeted to `8-15%`
- the configured `tenure_churn_probability` values should remain non-increasing by tenure band

Sales-channel churn rules:

- emitted sales-channel values remain `ONLINE`, `BRANCH`, and `AGENT`
- `AGENT` carries broker/aggregator-like higher churn behavior
- `AGGREGATOR` is not emitted as a separate value
- sales-channel churn rates are intentionally allowed to vary
- the churn workbook does not specify a sales-channel percentage benchmark, so channel variance is controlled by scenario config rather than Excel ranges
- enhanced tables preserve broker-like behavior through the `AGENT` channel mapping

The latest validated run after the workbook-range tuning was `20260521075140`:

- overall churn: `30.81%`
- sales channel churn: `AGENT 47.74%`, `BRANCH 28.50%`, `ONLINE 23.04%`
- policy cycle churn: `<1 44.13%`, `1-2 30.63%`, `3-5 17.73%`, `>5 12.12%`
- synthetic base and enhanced `sat_policy` rates matched

Status churn rules:

- churned policies use configured churned-status weights
- suspended account behavior uses configured suspended-policy status weights
- lapsed status requires a completed renewal cycle
- sub-one-year churn is cancelled

## Raw, Silver, And Enhanced Rules

Raw source rules:

- CRM, API, claims, and data-source outputs are generated as separate source contexts where applicable.
- Source systems follow the same lifecycle and churn rules but do not need to share the same raw source business IDs.
- Canonical raw outputs normalize selected raw source extracts into common contracts.

Silver rules:

- silver vault outputs are rebuilt from canonical raw outputs
- hub/link/satellite relationships are validated after transformation
- policy, customer, account, asset, claim, consent, marketing, and relationship rules are checked at silver level
- churn fields inherited into silver are validated by `verify_csv.py`

Enhanced rules:

- enhanced output is built from the base context
- enhanced tables preserve base policy, customer, account, channel, churn, and product consistency
- `--enhanced-only` still builds base context in memory but writes only enhanced synthetic output and enhanced SCD2 when prior enhanced history exists
- broker/aggregator-like sales behavior is represented through existing `AGENT` values
- enhanced `sat_claim` financials are controlled by `claim_financial_settings`
- enhanced claim financials use populated `FactPolicy` claim rows first, then derive from linked `policy_sum_insured` when needed
- `claim_amount`, `claims_paid`, `outstanding_reserve`, `claims_expenses`, `claim_band`, and `claim_band_sort` are populated consistently
- `claim_band_sort` is the numeric ordering key for `claim_band`
- some claim financial fields can validly remain zero when recovery, fraud, or litigation does not apply

MLOps rules:

- MLOps output is built from the same base context as enhanced output and is written under `data/synthetic/mlops/<run_id>`.
- MLOps uses the DDL in `mlops/mlops_gen/Enhanced_Customer360_DataVault_Model_DDL.sql`.
- MLOps preserves the same 80-table Data Vault structure as enhanced output, with additional MLOps-facing columns.
- Base and enhanced DDLs are not changed to add these MLOps-only columns.
- MLOps enrichment runs after base/enhanced entities are assembled, so policy, customer, account, claim, channel, churn, and product relationships remain inherited from the existing generation flow.
- `sat_address.region` is derived from existing `state`, falling back to `city`.
- `sat_claim.suspected_amount` is the corrected spelling for old enhanced `suspectd_amount`; it is nonnegative and capped at `claim_amount`.
- `sat_claim.is_fault_claim` is derived from fraud, suspicious-claim, third-party, and third-party score indicators.
- `sat_claim.claim_satisfaction_score` is a `1-10` score reduced by open/pending claim status, fraud/suspicion, litigation, fault, high claim amount, or outstanding reserve.
- `sat_marketing_engagement.has_retention_team_interaction` is more likely for churned or low-engagement policy context.
- `sat_marketing_engagement.customer_service_call_frequency` is higher for churned customers and lower for stable customers.
- `sat_marketing_engagement.average_call_sentiment` is `NEGATIVE`, `NEUTRAL`, or `POSITIVE` based on service-call frequency and engagement score.
- `sat_marketing_engagement.engagement_score` is a `0-100` score derived from opened-email, marketing status, and churn context.
- `sat_motor.driver_experience_years` is derived from linked `sat_natural_person.birth_date` as `max(age - 17, 0)`.
- `sat_policy.is_auto_renew_enabled` is more likely for active and longer-tenure policies, and less likely for churned policies.
- `sat_policy.no_claims_discount_years` is derived from completed `policy_cycle`, reduced by claim pressure, and clipped to nonnegative years.
- `sat_policy.payment_method` is one of `DIRECT_DEBIT`, `CARD`, or `BANK_TRANSFER`, with direct debit more common for stable policies.
- `sat_policy.is_direct_debit_cancellation` can be `Y` only when `payment_method` is `DIRECT_DEBIT`, the policy is churned, and missed payments exist.
- `sat_policy.missed_payment_count` is higher for churned or higher-risk policy context and lower for active/stable policies.
- `sat_policy.loyalty_discount_usage` uses `RETAINED`, `NOT_APPLIED`, or `REMOVED`; retained is more common for stable policies, removed is more common for churned policies.
- `sat_policy.is_installment_default` is `Y` when `missed_payment_count >= 2`.
- MLOps schema and MLOps-only column rules are validated by `misc/verify_mlops_synthetic.py`.
- MLOps churn KPI ratios are validated by `misc/verify_mlops_churn_kpis.py` against workbook ranges configured in `churn_settings.mlops_churn_expected_ranges`.
- Newly coverable MLOps churn KPIs include auto-renew enabled, at-fault claim, NCD years, payment method, direct debit cancellation, missed payments, retention interaction, claim satisfaction, loyalty discount status, installment default, call sentiment, and engagement score.
- `sat_policy.loyalty_discount_usage` uses workbook-aligned status values: `RETAINED`, `NOT_APPLIED`, and `REMOVED`.
- Claim fault and claim satisfaction churn ratios are validated from linked claims. Claims may be linked to active, lapsed, or cancelled policies when the claim reported date remains within the policy coverage window.

## SCD2 Rules

SCD2 outputs are generated only when prior comparable history exists.

Synthetic base/enhanced/MLOps SCD2:

- uses prior normalized synthetic history
- samples about `10%` of eligible rows per configured satellite
- mutates configured satellite attributes only
- writes changed rows to `data/scd2/base/<run_id>`, `data/scd2/enhanced/<run_id>`, or `data/scd2/mlops/<run_id>`
- preserves satellite schema compatibility
- is a sampled synthetic change feed, not complete CDC

Raw SCD2:

- compares current raw CRM/API output against previous comparable raw runs
- writes source-level raw deltas under `data/scd2/raw/<source>/<run_id>`

First-run behavior:

- a clean first run creates snapshots but may not create synthetic SCD2 because no prior comparable history exists
- a second run can create SCD2 using the first run as history

## Validation Coverage

Use this normal synthetic-run validation flow:

```powershell
.\venv\Scripts\python.exe .\main.py
.\venv\Scripts\python.exe .\validate_churn_kpis.py
.\venv\Scripts\python.exe .\misc\verify_enhanced_synthetic.py
.\venv\Scripts\python.exe .\misc\verify_mlops_synthetic.py
.\venv\Scripts\python.exe .\misc\verify_mlops_churn_kpis.py
.\venv\Scripts\python.exe .\misc\compare_all_scd2.py
```

Use this optional raw/silver validation flow only when raw and silver outputs were generated:

```powershell
.\venv\Scripts\python.exe .\main.py --include-raw-silver
.\venv\Scripts\python.exe .\misc\transform_all_raw_to_silver.py
.\venv\Scripts\python.exe .\misc\verify_all_silver.py
.\venv\Scripts\python.exe .\misc\compare_all_scd2.py
```

`validate_churn_kpis.py` checks base churn KPI fields directly, including:

- premium amount bands
- renewal movement bands
- claim-count bands
- add-on/cover-option behavior
- marketing/service proxy behavior
- driver-experience proxy behavior
- vehicle model/segment behavior
- decreasing churn by completed `Policy Cycle`
- sales-channel churn variance

`verify_csv.py` and `misc/verify_all_silver.py` check broader raw/silver rules, including:

- hub/link/satellite relationships
- required columns
- lifecycle consistency
- policy/account/customer consistency
- policy date validity
- churn source-field logic and distributions
- enhanced inherited consistency where applicable

`misc/compare_all_scd2.py` checks SCD2 outputs where comparable prior/current outputs exist.

`misc/verify_mlops_synthetic.py` checks the MLOps Data Vault DDL shape, required MLOps-only column population, boolean values, numeric ranges, corrected `suspected_amount` spelling, and direct-debit cancellation consistency.

`misc/verify_mlops_churn_kpis.py` checks the MLOps-only churn KPI ratios enabled by the new DDL and scenario workbook ranges.

## Large-Run Note

The normal `main.py` path is optimized for base synthetic, enhanced, MLOps, churn validation, and synthetic SCD2.

Large unique base generation can be run through the streaming path. Raw/silver validation requires `--include-raw-silver`.
