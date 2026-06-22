# Current Rules Reference

This document describes the currently implemented generation and validation rules for the base synthetic vault, raw/source outputs, silver rebuilds, enhanced outputs, MLOps outputs, churn KPI fields, and SCD2 outputs.

For scenario configuration meanings, see [scenario_config_reference.md](F:/SyncDataGenerator_v1.0/docs/scenario_config_reference.md).

For the latest generated-run validation results and expected-vs-current churn ratios, see [latest_run_validation.md](F:/SyncDataGenerator_v1.0/docs/latest_run_validation.md).

## Output Scope

Default `main.py` generation creates the synthetic workflow:

- normalized synthetic base output under `data/synthetic/base/<run_id>`
- enhanced synthetic output under `data/synthetic/enhanced/<run_id>`
- MLOps synthetic output under `data/synthetic/mlops/<run_id>`
- synthetic base/enhanced/MLOps SCD2 outputs when prior synthetic history exists

`data/output/<run_id>` is an intermediate base build folder. It is kept after successful validation and normalization unless `--remove-working-output` is passed.

Raw and silver outputs are disabled by default because they add runtime. Enable PRD raw with `output_settings.generate_prd_raw=true`, and enable PRD silver rebuilds with both `output_settings.generate_prd_raw=true` and `output_settings.generate_prd_silver=true`.

Legacy raw CRM/API/claims/data_source, canonical, silver, and raw SCD2 outputs are disabled by default. Generate them only when needed with `output_settings.generate_legacy_raw_silver=true` or `--include-raw-silver`.

`new_outputs_src` is also disabled by default. It is generated only when legacy raw/silver is enabled and `output_settings.generate_new_outputs_src=true` or `--include-new-outputs-src` is passed.

Direct MLOps dimensional output is generated on demand from an existing MLOps synthetic vault run:

```powershell
.\venv\Scripts\python.exe .\misc\generate_direct_dim_fact.py --run-id <run_id>
```

Rules for `data/dim_fact_direct/mlops/<run_id>`:

- exactly 22 CSV files are written: 18 dimensions and 4 facts from `mlops/Enhanced_Customer360_Dimensional_Model_DDL.sql`
- every table includes all DDL columns, even when a value is optional or unknown
- dimensions use unique surrogate keys and include `-1` unknown rows for optional fact references
- dimensions with SCD2 columns have populated `effective_from_ts`, `effective_to_ts`, and `record_version`
- facts resolve their surrogate keys to the corresponding dimension rows
- the direct dim/fact builder reads the MLOps Data Vault as source and does not mutate the source vault files
- NPS analytical features are calibrated at the ML notebook `master_df` grain; this can clone descriptive dimension rows with new surrogate keys so joins remain one-to-one and workbook ratios survive dim/fact analysis
- validation is done with `misc/verify_direct_dim_fact.py` for structure/RI/SCD2 fields and `misc/verify_nps_dim_fact.py` for post-dim/fact NPS ratios

Mode-scoped PRD raw folders:

- `data/raw/base/prd_01/<run_id>` preserves the existing base CRM raw file shape.
- `data/raw/base/prd_02/<run_id>` carries the same 16 base raw entities as PRD1 but with PRD2-specific raw table names and renamed source columns. It is an alternate raw feed for product separation; it does not replace the existing base PRD1 silver/vault path.
- `data/raw/enhanced/prd_01/<run_id>` and `data/raw/mlops/prd_01/<run_id>` preserve the same base raw extract scoped to the target mode.
- Raw CRM and PRD1 file names omit the redundant `crm_` prefix because the source is already represented by the folder. Examples: `party_master.csv`, `address_book.csv`, `account_book.csv`.
- `party_master.csv.legal_job_title_txt` is always populated for database imports. Legal-person rows use realistic roles such as `DIRECTOR`, `COMPANY_SECRETARY`, `OWNER`, `PARTNER`, `AUTHORIZED_SIGNATORY`, `MANAGING_DIRECTOR`, `TRUSTEE`, or `SOLE_PROPRIETOR`; non-legal rows use `NOT_APPLICABLE`.
- `data/raw/enhanced/prd_02/<run_id>` and `data/raw/mlops/prd_02/<run_id>` contain the seven added entity registers: broker, campaign, channel, complaint, insured object, override, and regulation.
- PRD2 also contains source-style bridge and enrichment extracts so PRD1+PRD2 can rebuild the enhanced/MLOps vault without losing relationships or added satellite fields.
- PRD2 column headers use `src_*` source names instead of vault names. The combined-vault builder maps those source columns back to MLOps hub/link/satellite columns.
- `misc/verify_prd_raw_mlops.py` checks PRD1 against base PRD1 and PRD2 against the corresponding added entities and relationships.

Base PRD1-to-PRD2 table mapping:

| PRD1 table | Base PRD2 table |
|---|---|
| `account_book.csv` | `billing_account_feed.csv` |
| `address_book.csv` | `location_contact_feed.csv` |
| `campaign_touch.csv` | `marketing_touch_feed.csv` |
| `comm_preference.csv` | `contact_preference_feed.csv` |
| `consent_snapshot.csv` | `consent_state_feed.csv` |
| `contact_point.csv` | `communication_point_feed.csv` |
| `customer_lead_bridge.csv` | `client_lead_link_feed.csv` |
| `customer_portfolio.csv` | `client_portfolio_feed.csv` |
| `identity_registry.csv` | `identity_reference_feed.csv` |
| `lead_register.csv` | `prospect_register_feed.csv` |
| `party_master.csv` | `insured_party_feed.csv` |
| `policy_register.csv` | `contract_policy_feed.csv` |
| `product_catalog.csv` | `cover_product_feed.csv` |
| `property_asset.csv` | `home_asset_feed.csv` |
| `quote_register.csv` | `quotation_feed.csv` |
| `vehicle_asset.csv` | `motor_asset_feed.csv` |

Base PRD2 column rename rules:

- Exact names: `batch_ref -> extract_batch_id`, `pull_ts -> extract_timestamp`, `origin_sys -> source_application`, `tenant_cd -> tenant_code`
- Suffix rules: `_ref -> _reference_id`, `_src_ref -> _source_reference_id`, `_txt -> _desc`, `_amt -> _amount`, `_cnt -> _count`, `_ind -> _flag`, `_dt -> _date`, `_ts -> _timestamp`, `_cd -> _code`, `_nm -> _name`, `_no -> _num`

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
- Enhanced and MLOps `sat_policy.policy_issue_date` is on or before `policy_start_date`. Most policies issue on the start date or within the configured NPS policy-issuance TAT bands before start.

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

The latest validated run is documented in `docs/latest_run_validation.md`.

Current status:

- schema, PK/FK, RI, dates, claim rules, raw/silver rebuilds, and NPS rules pass
- policy issuance TAT follows the NPS workbook distribution
- churn remains directionally aligned, with some strict workbook bands still reported as partial because several churn features share the same policy-status rows

Status churn rules:

- churned policies use configured churned-status weights
- suspended account behavior uses configured suspended-policy status weights
- lapsed status requires a completed renewal cycle
- sub-one-year churn is cancelled

## MLOps Feedback And Satisfaction Columns

The current MLOps Data Vault DDL is `mlops/Enhanced_Customer360_DataVault_Model_DDL.sql`.

The latest DDL adds 9 MLOps feedback/satisfaction fields. They are generated only in the MLOps-shaped output and are carried into PRD2 raw enrichment/entity extracts and MLOps silver rebuilds through the existing raw-to-silver flow.

| Table | Column | Rule |
|---|---|---|
| `sat_claim` | `claims_feedback` | Populated when a claim exists and is no longer open/pending. Uses `claim_satisfaction_score`, litigation, fraud, and claim outcome context. Values: `POSITIVE`, `NEUTRAL`, `NEGATIVE`; blank while open/pending. |
| `sat_complaint` | `customer_complaint_satisfaction_score` | Populated for complaints with `complaint_resolved_date`. Score is `0-5`; lower scores follow longer resolution time, FOS referral, and upheld/partially upheld complaints. |
| `sat_complaint` | `complaint_feedback` | Optional feedback label derived from `customer_complaint_satisfaction_score` when feedback is available. Values: `POSITIVE`, `NEUTRAL`, `NEGATIVE`; blank is allowed. |
| `sat_customer` | `customer_onboarding_satisfaction_score` | Populated for customers. Score is `0-5` and is aligned with the configured onboarding feedback sentiment bucket. NPS remains `0-10`; this field is a separate 0-5 onboarding proxy. |
| `sat_customer` | `customer_onboarding_feedback` | Uses configured phrase text, not generic sentiment labels. The workbook ratio is enforced as `20%` negative, `30%` neutral, and `50%` positive. The generator expands the configured phrase bank into up to `500` equivalent unique values: `100` negative, `150` neutral, and `250` positive. Positive examples include `Comprehensive cover for the price for appropriate policy` and `Flexible excess options available`; negative examples include `Policy exclusions not clear` and `Courtesy car not in standard cover`. |
| `sat_marketing_engagement` | `first_contact_resolution` | `Y` when first/contact interaction is low-friction: call frequency <= 1, sentiment not negative, and engagement score >= 60. Otherwise `N`. |
| `sat_policy` | `policy_renewal_satisfaction_score` | Populated for renewal policies. Score is `0-5`; lower scores follow high premium increase, churned/lapsed/cancelled status, direct-debit cancellation, installment default, and missed payments. Non-renewal policies default to `0` in the numeric output column. |
| `sat_policy` | `policy_renewal_feedback` | Derived from renewal satisfaction score. Values: `POSITIVE`, `NEUTRAL`, `NEGATIVE`; blank when renewal context is not applicable. |
| `sat_policy` | `is_renewal_escalation` | `Y` when renewal satisfaction score is <= 2; `N` otherwise for renewal rows. |

Score-to-feedback mapping for claim, complaint, policy renewal, and onboarding phrase selection:

- score `4-5`: `POSITIVE`
- score `2-3`: `NEUTRAL`
- score `0-1`: `NEGATIVE`

For `sat_customer.customer_onboarding_feedback`, the final assignment uses `nps_settings.onboarding_feedback_distribution` and `nps_settings.onboarding_feedback_text`. Customers are ranked by NPS so lower NPS rows receive negative onboarding themes first, passive rows receive neutral themes first, and higher NPS rows receive positive themes first.

NPS remains separate:

- NPS `0-6`: detractor
- NPS `7-8`: passive
- NPS `9-10`: promoter

The MLOps validator checks these columns for schema presence, allowed score ranges, allowed feedback values, and Y/N flags.

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
- MLOps uses the DDL in `mlops/Enhanced_Customer360_DataVault_Model_DDL.sql`.
- MLOps preserves the same 80-table Data Vault structure as enhanced output, with additional MLOps-facing columns.
- Base and enhanced DDLs are not changed to add these MLOps-only columns.
- MLOps enrichment runs after base/enhanced entities are assembled, so policy, customer, account, claim, channel, churn, and product relationships remain inherited from the existing generation flow.
- MLOps churn KPI fields are calibrated by coupled KPI family instead of as isolated columns. Payment/DD/missed/default/loyalty fields, claim fault/satisfaction fields, and marketing sentiment/engagement/retention fields are assigned together so workbook ranges can be targeted while preserving dependent business rules.
- MLOps payment and engagement ratios are especially sensitive because the workbook defines marginal churn bands, while the model stores dependent fields on the same policy/customer. For example, direct-debit cancellation must remain a `DIRECT_DEBIT` payment case, installment default must follow missed payments of `2+`, and engagement score is validated through policy-to-person-to-marketing joins rather than raw marketing rows.
- Best-effort deterministic rebalancing is applied after initial MLOps assignment for payment method, missed-payment/default behavior, and engagement score. This keeps hard business rules intact first, then moves eligible retained/churned rows between bands to target workbook ranges.
- `sat_address.region` is derived from existing `state`, falling back to `city`.
- `sat_claim.suspected_amount` is the corrected spelling for old enhanced `suspectd_amount`; it is nonnegative and capped at `claim_amount`.
- `sat_claim.is_fault_claim` is derived from fraud, suspicious-claim, third-party, and third-party score indicators.
- `sat_claim.claim_satisfaction_score` is a `1-10` score reduced by open/pending claim status, fraud/suspicion, litigation, fault, high claim amount, or outstanding reserve.
- `sat_marketing_engagement.has_retention_team_interaction` is more likely for churned or low-engagement policy context.
- `sat_marketing_engagement.customer_service_call_frequency` is higher for churned customers and lower for stable customers.
- `sat_marketing_engagement.average_call_sentiment` is `NEGATIVE`, `NEUTRAL`, or `POSITIVE` based on service-call frequency and engagement score.
- `sat_marketing_engagement.engagement_score` is a `0-100` score derived from opened-email, marketing status, and churn context.
- `sat_motor.driver_experience_years` is derived from linked `sat_natural_person.birth_date` as `max(age - 17, 0)`.
- `sat_policy.policy_type` is calibrated separately from auto-renew: `NEW_BUSINESS` carries higher churn than `RENEWAL`.
- `sat_policy.is_policy_renewal` is calibrated separately from auto-renew: `N` carries higher churn than `Y`.
- `sat_policy.is_auto_renew_enabled` is more likely for active and longer-tenure policies, and less likely for churned policies.
- `sat_policy.no_claims_discount_years` is derived from completed `policy_cycle`, reduced by claim pressure, and clipped to nonnegative years.
- `sat_policy.payment_method` is one of `DIRECT_DEBIT`, `CARD`, or `BANK_TRANSFER`, with direct debit more common for stable policies.
- `sat_policy.is_direct_debit_cancellation` can be `Y` only when `payment_method` is `DIRECT_DEBIT`, the policy is churned, and missed payments exist.
- `sat_policy.missed_payment_count` is higher for churned or higher-risk policy context and lower for active/stable policies.
- `sat_policy.loyalty_discount_usage` uses `RETAINED`, `NOT_APPLIED`, or `REMOVED`; retained is more common for stable policies, removed is more common for churned policies.
- `sat_policy.is_installment_default` is `Y` when `missed_payment_count >= 2`.
- MLOps schema and MLOps-only column rules are validated by `misc/verify_mlops_synthetic.py`.
- MLOps churn KPI ratios are validated by `misc/verify_mlops_churn_kpis.py` against workbook ranges configured in `churn_settings.mlops_churn_expected_ranges`.
- Newly coverable MLOps churn KPIs include policy type, policy renewal, auto-renew enabled, at-fault claim, NCD years, payment method, direct debit cancellation, missed payments, retention interaction, claim satisfaction, customer satisfaction, complaint resolution days, loyalty discount status, installment default, call sentiment, and engagement score.
- `sat_policy.loyalty_discount_usage` uses workbook-aligned status values: `RETAINED`, `NOT_APPLIED`, and `REMOVED`.
- Claim fault and claim satisfaction churn ratios are validated from linked claims. Claims may be linked to active, lapsed, or cancelled policies when the claim reported date remains within the policy coverage window.
- Complaint resolution churn ratios are generated from a mixed retained/churned policy complaint sample so short-resolution bands still contain some churned policies, while longer-resolution bands retain higher churn.
- Engagement score follows the workbook direction: `HIGH` engagement is a lower-churn band, while `LOW` and `VERY_LOW` engagement are higher-churn bands.

NPS feature rules:

- NPS features are validated by `misc/verify_nps_features.py` against the NPS workbook, `churnps/Data Req Churn NPS.xlsx`, sheet `NPS_Features`.
- NPS features after ML dim/fact creation are validated by `misc/verify_nps_dim_fact.py .\nps_june\data`, which rebuilds the same `master_df` grain used by the ML notebook.
- The NPS layer uses existing Data Vault columns and derived proxies only; it does not add columns or change PK/FK/date/churn/claim rules.
- Enhanced/MLOps generation applies a final NPS alignment pass at policy/customer grain before writing CSVs, so dim/fact joins should preserve the intended workbook ratios more closely. This pass only updates descriptive satellite values and must not modify hubs, links, hash keys, business IDs, or referential integrity.
- Configurable NPS ratios live under `nps_settings` in `config/scenario_v1.json`.
- `sat_customer.nps_score` is generated as a `0-10` score with workbook-aligned bands: `DETRACTOR 30`, `PASSIVE 35`, and `PROMOTER 35`. Inside the detractor band, scores `2-4` are weighted lower than `0`, `1`, `5`, and `6` so the score-level chart has a visible dip without changing the overall NPS score.
- `sat_customer.net_promotor_code_segment` is derived from `nps_score`: `0-6` is `DETRACTORS`, `7-8` is `PASSIVE`, and `9-10` is `PROMOTERS`.
- Digital onboarding is proxied from `sat_account.account_creation_type` with a configurable `ONLINE 75` / `BRANCH 25` split. After NPS is assigned, account creation type is aligned so higher-NPS customers are more likely to be `ONLINE` and lower-NPS customers are more likely to be `BRANCH`.
- Quote drop-off is proxied from `sat_quote.quote_status` with a configurable accepted/drop-off split. Non-accepted drop-off statuses follow the workbook mix `CREATED 50`, `SENT 35`, `EXPIRED 15`, with `EXPIRED` skewing low NPS, `SENT` skewing moderate NPS, and `CREATED` skewing high NPS.
- Claim escalation is proxied from `sat_claim.is_litigation` with a configurable `92% non-escalated` / `8% escalated` split for enhanced/MLOps claim rows. Escalation is NPS-shaped so escalated/litigated claims skew low NPS and high-NPS claim customers skew non-escalated.
- Policy issuance turnaround time is derived from `sat_policy.policy_issue_date` and `sat_policy.policy_start_date`; enhanced/MLOps outputs follow the workbook distribution: `70% 0-2 days`, `20% 3-7 days`, and `10% >7 days`. The assignment is NPS-aware: promoters lean toward `0-2 days`, passives lean toward `3-7 days`, and low-NPS detractors lean toward `>7 days`.
- Premium increase is enforced for the NPS workbook from existing quote renewal amount fields: `<=5% 70`, `5-10% 20`, and `>10% 10`. The assignment is NPS-shaped without overriding the separate churn premium calibration. Digital renewal is enforced for renewal policies from `sat_policy.sales_channel`: `ONLINE 70`, `AGENT 20`, and `BRANCH 10`, with online skewing high NPS, agent-assisted skewing passive NPS, and branch skewing lower NPS. Claim complaint flag is enforced from claim-policy and complaint-policy links: `NO_COMPLAINT 85`, `COMPLAINT 15`, with complaint-linked claims skewing lower NPS. Self-service adoption is enforced from online account creation, paperless consent, and recent account access: `ADOPTED 65`, `NOT_ADOPTED 35`, with adopted customers skewing higher NPS. Complaint resolution turnaround is enforced from complaint dates: `0-2 days 60`, `3-7 days 30`, `>7 days 10`, while preserving valid complaint date ordering and status consistency.
- Workbook NPS rows that require missing operational data, such as first-contact resolution or full contact-center interaction logs, remain documented as not directly derivable from the current model.

Known MLOps ratio-calibration constraints:

- A single policy status drives all KPI churn calculations, so changing one row to fix a KPI also changes every other KPI band that row belongs to.
- `is_direct_debit_cancellation = Y` must keep `payment_method = DIRECT_DEBIT`; this can push `MONTHLY_DD` churn above range unless enough retained direct-debit rows are added.
- Lowering `MONTHLY_DD` churn can push churned rows into `CARD_MANUAL`, causing card/manual churn to spike.
- Moving churned rows into `missed_payment_count = 0` or `1` can conflict with `is_installment_default`, because installment default requires `missed_payment_count >= 2`.
- High/medium engagement bands are naturally retained-heavy, but the workbook still expects non-zero churn there. The generator therefore injects a controlled number of churned policy-linked marketing rows into `HIGH` and `MEDIUM`.
- Exact equality to every marginal workbook range is not always guaranteed for small generated claim/marketing subsets, but the generator now uses deterministic quota rebalancing to get as close as possible without breaking schema, date, relationship, or dependency rules.

## SCD2 Rules

SCD2 outputs are generated only when prior comparable history exists.

Synthetic base/enhanced/MLOps SCD2:

- uses prior normalized synthetic history
- samples about `10%` of eligible rows per configured satellite
- mutates configured satellite attributes only
- writes changed rows to `data/scd2/base/<run_id>`, `data/scd2/enhanced/<run_id>`, or `data/scd2/mlops/<run_id>`
- preserves satellite schema compatibility
- is a sampled synthetic change feed, not complete CDC
- writes only rows where a business value actually changes; sampled no-op rows are skipped
- excludes stable reference satellites such as `sat_channel` from mutation unless a real mutable business attribute is later introduced

Safe SCD2 mutation examples:

- `sat_account.account_status`
- `sat_contact.personal_email`
- `sat_customer.customer_status_reason`
- `sat_lead.preferred_contact_method`
- `sat_motor.license_status`
- `sat_natural_person.occupation`
- `sat_policy.cover_option`
- `sat_quote.renewal_amt_next_period`
- `sat_broker.agent_net_promoter_score`
- `sat_address.street`
- `sat_campaign.campaign_status`
- `sat_claim.claim_status`
- `sat_complaint.complaint_status`
- complaints with `complaint_resolved_date` must not remain `Open` or `Pending`
- SCD2 complaint-status mutation also preserves that rule: rows with `complaint_resolved_date` are forced to closed/resolved status, and rows without a resolved date are not emitted as closed/resolved.
- `sat_override.override_reason`
- `sat_regulation.regulation_compliance_status`
- `sat_insured_object.insured_object_current_status`

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
.\venv\Scripts\python.exe .\misc\verify_prd_raw_mlops.py --mode base --run-id <run_id>
.\venv\Scripts\python.exe .\misc\verify_prd_raw_mlops.py --mode enhanced --run-id <run_id>
.\venv\Scripts\python.exe .\misc\verify_prd_raw_mlops.py --mode mlops --run-id <run_id>
.\venv\Scripts\python.exe .\misc\build_product_combined_vault.py --run-id <run_id>
.\venv\Scripts\python.exe .\misc\verify_mlops_synthetic.py .\data\product_combined\<combined_run_id>
.\venv\Scripts\python.exe .\verify_csv.py .\data\product_combined\<combined_run_id>
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
