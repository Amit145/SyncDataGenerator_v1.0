# LLM Codebase Context Brief

Use this as the first-read guide for any AI agent or engineer working in this repository.

## Project

`SyncDataGenerator` generates synthetic UK insurance Customer 360 Data Vault CSV outputs.

Default generation is intentionally focused on:

- synthetic base: `data/synthetic/base/<run_id>`
- synthetic enhanced: `data/synthetic/enhanced/<run_id>`
- synthetic MLOps: `data/synthetic/mlops/<run_id>`
- synthetic SCD2 for base/enhanced/MLOps only when prior comparable synthetic history exists

Raw, canonical, silver, and `new_outputs_src` outputs are optional because they add runtime.

Do not assume generated data under `data/` is source code. Do not delete generated runs unless the user explicitly asks.

## Main Commands

Normal generation:

```powershell
.\venv\Scripts\python.exe .\main.py
```

Large base-only streaming generation:

```powershell
.\venv\Scripts\python.exe .\main.py --streaming-base --total-people 10000000 --chunk-size 100000
```

Enhanced-only generation:

```powershell
.\venv\Scripts\python.exe .\main.py --enhanced-only
```

Optional raw/canonical/silver outputs:

```powershell
.\venv\Scripts\python.exe .\main.py --include-raw-silver
```

Optional `new_outputs_src`:

```powershell
.\venv\Scripts\python.exe .\main.py --include-raw-silver --include-new-outputs-src
```

## Verification Commands

Run these after `main.py`:

```powershell
.\venv\Scripts\python.exe .\validate_churn_kpis.py
.\venv\Scripts\python.exe .\misc\verify_enhanced_synthetic.py
.\venv\Scripts\python.exe .\misc\verify_mlops_synthetic.py
.\venv\Scripts\python.exe .\misc\verify_mlops_churn_kpis.py
.\venv\Scripts\python.exe .\misc\verify_nps_features.py
.\venv\Scripts\python.exe .\misc\compare_all_scd2.py
```

Useful details:

- `validate_churn_kpis.py` validates base churn rules.
- `misc/verify_enhanced_synthetic.py` validates enhanced schema, PKs, FKs, dates, relationships, claim rules, and enhanced business rules.
- `misc/verify_mlops_synthetic.py` validates MLOps DDL alignment and MLOps-only column integrity.
- `misc/verify_mlops_churn_kpis.py` validates workbook churn ratios for MLOps-only KPIs.
- `misc/verify_nps_features.py` validates available/proxy NPS features from the latest NPS workbook.
- `misc/compare_all_scd2.py` reports SCD2 only when comparable SCD2 data exists.

For latest generated-run results, use `docs/latest_run_validation.md`.

## Source Workbooks And DDLs

Enhanced active DDL:

- `enhanced_360/new/newEnhanced Customer 360 Data Vault DDL.sql`

MLOps Data Vault DDL:

- `mlops/mlops_gen/Enhanced_Customer360_DataVault_Model_DDL.sql`

Churn workbook:

- `new_rules/Data Req Churn NPS.xlsx`

Latest NPS workbook:

- `new_rules/nps/npsn.xlsx`

Previous NPS workbook, retained for comparison only:

- `new_rules/nps/Data Req Churn NPS.xlsx`

MLOps dimensional/S2T references:

- `mlops/mlops_gen/*`

## Important Files

Generation:

- `main.py`
- `generators/enhanced_synthetic_generator.py`
- `helper/satellite_builder.py`
- `helper/link_builder.py`
- `helper/hub_builder.py`
- `helper/source_context_builder.py`
- `helper/streaming_base_generator.py`
- `helper/scd2_generator.py`

Validation:

- `validate_churn_kpis.py`
- `verify_csv.py`
- `misc/verify_enhanced_synthetic.py`
- `misc/verify_mlops_synthetic.py`
- `misc/verify_mlops_churn_kpis.py`
- `misc/verify_nps_features.py`
- `misc/compare_all_scd2.py`

Config:

- `config/scenario_v1.json`
- `config/cardinality.json`
- `config/storage_paths.py`

Docs to keep current:

- `README.md`
- `docs/current_rules_reference.md`
- `docs/scenario_config_reference.md`
- `docs/mlops_gen_schema_review.md`
- `docs/nps_features_reference.md`
- `docs/latest_run_validation.md`
- `docs/llm_codebase_context.md`

## Output Contract

Base output:

- normalized synthetic base: `data/synthetic/base/<run_id>`
- legacy/base staging output may exist under `data/output/<run_id>`

Enhanced output:

- `data/synthetic/enhanced/<run_id>`
- Must match the active enhanced DDL: 80 tables, 25 hubs, 30 links, 25 satellites.

MLOps output:

- `data/synthetic/mlops/<run_id>`
- Must match `mlops/mlops_gen/Enhanced_Customer360_DataVault_Model_DDL.sql`.

SCD2 output:

- `data/scd2/base/<run_id>`
- `data/scd2/enhanced/<run_id>`
- `data/scd2/mlops/<run_id>`

SCD2 is sampled mutation-style output, not full CDC. It is created only when the pipeline finds previous comparable synthetic history. SCD2 rows must contain actual business-value changes; no-op sampled rows are skipped. Stable reference satellites such as `sat_channel` are not emitted unless a meaningful mutable field exists.

Optional outputs:

- raw/canonical/silver only with `--include-raw-silver`
- `data/new_outputs_src` only with `--include-raw-silver --include-new-outputs-src`

## Core Lifecycle

The generated lifecycle spine is:

```text
Person
-> Natural Person or Legal Person
-> Contact, Identity, Address
-> Lead
-> Consent, Marketing Preference, Marketing Engagement
-> Quote
-> Policy
-> Customer and Account
-> Motor or Home asset
-> Enhanced Insured Object
-> Enhanced Claim, Complaint, Override, Broker, Campaign, Regulation
```

Rules:

- Every person has one person hub.
- A person is natural or legal, not both.
- Quotes are for lead persons.
- Policies are created from quotes.
- Policy products inherit quote products.
- Policy holders become customers and accounts.
- Motor/home assets derive from policy product type.
- Enhanced entities must derive from valid base context; do not create orphan enhanced rows.
- Claims link to policies through `link_claim_policy`.
- Complaints link to policies through `link_complaint_policy`.
- Insured objects derive from linked motor/home assets.

## Date Rules

Keep date rules intact:

- Hub/link/satellite load dates are sequenced.
- Historical business dates are capped to satellite load date where applicable.
- `policy_issue_date` equals `policy_start_date` for most policies.
- A small delayed-start population is allowed: policy start may be up to 7 days after issue date.
- `policy_end_date = policy_start_date + policy tenure/months`.
- `renewal_date` must not be after `policy_end_date`.
- Claims must stay inside the linked policy coverage window.
- Claim settlement date must not be before claim reported date.
- Complaint date must be on/after customer since date.
- Complaint acknowledgement/resolved dates must be ordered.
- Regulation raised/deadline/closed timestamps must be ordered.
- Enhanced DDL `TIMESTAMP` columns must include time components.

Do not loosen date validators to make data pass. Fix generation instead unless the workbook/model rule changes.

## Policy Cycle

Use `policy_cycle`, not the old typo `policy_cicle`.

Meaning:

- `policy_cycle` is completed annual policy tenure.
- It is not the number of policies purchased by the customer.
- Churn decreases as completed `policy_cycle` increases.
- `LAPSED` requires a completed renewal cycle.
- Sub-one-year churn is represented as `CANCELLED`.

## Churn Rules

Base churn rules come from available/proxy rows in:

- `new_rules/Data Req Churn NPS.xlsx`

MLOps churn rules are configured in:

- `config/scenario_v1.json` under `churn_settings.mlops_churn_expected_ranges`

Base churn features include:

- current premium amount
- percentage premium increase
- absolute premium increase
- claim count
- add-on count
- policy cycle/tenure
- sales-channel variance
- marketing engagement proxy
- driver-experience proxy
- vehicle segment

MLOps churn features include:

- policy type
- policy renewal
- auto-renew enabled
- NCD years
- payment method
- direct debit cancellation
- missed payments
- loyalty discount
- installment default
- customer satisfaction
- complaint resolution days
- fault claim
- claim satisfaction
- retention contacted
- call sentiment
- engagement score

Important MLOps distinction:

- `is_policy_renewal` is renewal/new-business context.
- `is_auto_renew_enabled` is automatic-renewal enrollment.
- They are separate KPIs and must not be merged.

Engagement follows workbook direction:

- `HIGH` engagement has lower churn.
- `LOW` and `VERY_LOW` engagement have higher churn.

Known ratio caveat:

- Some workbook ratios are marginal targets over coupled generated fields.
- `payment_method`, `is_direct_debit_cancellation`, `missed_payment_count`, and `is_installment_default` constrain the same policy rows.
- Complaint resolution bands are small sample sizes.
- Engagement is validated through policy-to-person-to-marketing joins.
- It is acceptable for `docs/latest_run_validation.md` to report partial churn pass when schema, PK/FK, dates, claims, NPS, and most churn rules are valid.

## Claim Rules

Claim rules are configured through `claim_financial_settings` in `config/scenario_v1.json`.

Rules:

- Claims link to policies.
- Claim amount is derived from configured severity/coverage rules.
- Claim amount should not exceed policy/insured context when configured.
- Paid amount must be nonnegative and not above claim amount.
- Outstanding reserve is valid because claims may be partially paid or still open.
- Claim band and sort derive from claim amount bands.
- `claim_satisfaction_score` is `1-10`, reduced by difficult claim conditions such as open/pending status, fraud/suspicion, litigation, fault, high amount, or outstanding reserve.

## NPS Rules

Latest NPS workbook:

- `new_rules/nps/npsn.xlsx`

NPS validator:

- `misc/verify_nps_features.py`

NPS is implemented using existing generated columns and derived proxies only. It must not add or change DDL columns unless the user explicitly asks for a schema change.

Covered available/proxy NPS features:

- NPS score
- NPS segment
- policy issuance TAT
- digital onboarding
- drop-off during onboarding proxy
- premium increase
- digital renewal
- claim CSAT proxy
- claim settlement TAT
- claim escalation via litigation
- claim complaint proxy
- claim channel
- SLA breach proxy
- self-service adoption proxy
- complaint/resolution TAT
- complaint escalation via FOS
- complaint status outcome
- repeat complaint proxy

Not directly coverable without new survey/contact/escalation tables:

- onboarding CSAT
- renewal CSAT
- customer contact count during renewal
- renewal escalation flag
- support/servicing CSAT
- first contact resolution
- complaint CSAT
- generic customer feedback text/category

`npsn.xlsx` adds an `Onboarding Feedback` row, but it currently has no source, logic, or expected distribution filled in. Treat it as not actionable until more workbook detail is provided.

## MLOps DDL Alignment

The MLOps output uses the MLOps DDL and keeps the same 80-table structure as enhanced output. It adds or populates MLOps-facing columns such as:

- `sat_customer.nps_score`
- `sat_customer.net_promotor_code_segment`
- `sat_policy.is_auto_renew_enabled`
- `sat_policy.no_claims_discount_years`
- `sat_policy.payment_method`
- `sat_policy.is_direct_debit_cancellation`
- `sat_policy.missed_payment_count`
- `sat_policy.loyalty_discount_usage`
- `sat_policy.is_installment_default`
- `sat_claim.is_fault_claim`
- `sat_claim.claim_satisfaction_score`
- `sat_marketing_engagement.has_retention_team_interaction`
- `sat_marketing_engagement.customer_service_call_frequency`
- `sat_marketing_engagement.average_call_sentiment`
- `sat_marketing_engagement.engagement_score`
- `sat_motor.driver_experience_years`

Run `misc/verify_mlops_synthetic.py` after generation to confirm DDL alignment.

## Config Guidelines

Prefer configuration over hardcoding. Use `config/scenario_v1.json` for distributions and expected ranges.

Important config areas:

- `run_settings`
- `churn_settings`
- `mlops_churn_expected_ranges`
- `mlops_churn_band_weights`
- `claim_financial_settings`
- `enhanced_settings`

Update `docs/scenario_config_reference.md` when adding or changing config keys.

## Editing Rules For Future Agents

Do:

- Read existing validators before changing generator behavior.
- Preserve PK/FK shape and DDL column order.
- Preserve date rules.
- Preserve claim financial logic.
- Preserve churn/NPS workbook intent.
- Update docs and validators when changing generation behavior.
- Use existing helper functions and local naming conventions.
- Treat generated output under `data/` as disposable only when the user explicitly asks to delete it.

Do not:

- Add columns to enhanced/base/MLOps output without checking DDL and validators.
- Reintroduce `policy_cicle`.
- Add `AGGREGATOR` as a sales channel; map aggregator-like behavior to existing `AGENT`.
- Generate Kaggle-specific outputs; Kaggle/new output sources are optional/legacy and disabled by default.
- Loosen validators just to make a generated run pass.
- Revert unrelated user changes.

## Current Validation Reality

As of the latest documented run in `docs/latest_run_validation.md`:

- Enhanced schema/PK/FK/business validation passes.
- MLOps schema validation passes.
- NPS validation passes.
- Base churn and MLOps churn may be partial because several workbook ranges are coupled marginal targets.
- SCD2 may be absent if there is no prior comparable synthetic history.

When asked “are we good,” answer with this distinction: schema, keys, dates, claim, MLOps DDL, and NPS can be good while some churn ratio bands are still partial.
