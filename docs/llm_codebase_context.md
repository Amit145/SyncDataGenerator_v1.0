# LLM Codebase Context Brief

Use this as the first-read guide for any AI agent or engineer working in this repository.

## Project

`SyncDataGenerator` generates synthetic UK insurance Customer 360 Data Vault CSV outputs.

Default generation is intentionally focused on:

- synthetic base: `data/synthetic/base/<run_id>`
- synthetic enhanced: `data/synthetic/enhanced/<run_id>`
- synthetic MLOps: `data/synthetic/mlops/<run_id>`
- synthetic SCD2 for base/enhanced/MLOps only when prior comparable synthetic history exists

Raw and silver outputs are optional because they add runtime. Scenario config controls mode-scoped PRD raw and PRD raw-to-silver generation. Legacy CRM/API/claims/data_source raw, canonical, silver, and `new_outputs_src` can also be enabled by scenario config or CLI flags.

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

MLOps-only generation for faster NPS/MLOps iteration:

```powershell
.\venv\Scripts\python.exe .\main.py --mlops-only
```

`--mlops-only` writes only `data/synthetic/mlops/<run_id>`. It still builds base context in memory, but skips base CSV normalization, enhanced synthetic output, PRD raw, silver, and SCD2.

Optional legacy raw/canonical/silver outputs:

```powershell
.\venv\Scripts\python.exe .\main.py --include-raw-silver
```

Optional claims product raw/bronze/silver/gold outputs:

```powershell
.\venv\Scripts\python.exe .\main.py --include-claims-product
```

Claims product outputs are generated from the claim LDM/CDM/DV references under `claims/` and write:

- `data/raw/claims/<run_id>/raw`
- `data/raw/claims/<run_id>/prd_01`
- `data/raw/claims/<run_id>/prd_02`
- `data/raw/claims/<run_id>/raw_vault`
- `data/bronze/claims/<run_id>`
- `data/silver/claims/<run_id>`
- `data/gold/claims/<run_id>`

Claims `prd_01` is the full 23-file claims LDM raw source. Claims `prd_02` is generated from `claims/Claims_2Sources_DataTables.xlsx` as the SAP/source-2 view for claim, loss event, and claim investigation. `raw_vault` is the consolidated 23-file claims LDM raw shape consumed by the existing claims bronze/silver/gold pipeline. See `docs/claims_product_flow.md` before changing claim raw, silver vault, or gold logic.

Optional policy product raw:

```powershell
.\venv\Scripts\python.exe .\main.py --include-policy-product
```

Policy product outputs are generated from `policy_prd_inputs/POLICY_LOGICAL_MODEL V3.PDF` and `policy_prd_inputs/nPolicy_BV_Mapping_Rules.xlsx` and write:

- `data/raw/policy/<run_id>/prd_01`
- `data/raw/policy/<run_id>/prd_02`

Policy `prd_01` is the CRM/full logical model and includes explicit `address.csv`, `natural_person.csv`, `legal_entity.csv`, and `insured_object.csv`. Policy `prd_02` is the SAP/source-2 subset from the workbook and emits only `policy.csv`, `coverage.csv`, and `policy_coverage.csv`. Verify with `misc/verify_policy_two_source_raw.py --run-id <run_id>`. The verifier checks workbook source scope, headers, PRD1/PRD2 match keys, FK resolution, policy renewal/date rules, and insured-object date/status alignment to policy.

Mode-scoped PRD raw folders are generated only when `config/scenario_v1.json` has `output_settings.generate_prd_raw=true`:

- `data/raw/base/prd_01/<run_id>`
- `data/raw/base/prd_02/<run_id>`
- `data/raw/enhanced/prd_01/<run_id>`
- `data/raw/enhanced/prd_02/<run_id>`
- `data/raw/enhanced/raw_vault/<run_id>`
- `data/raw/mlops/prd_01/<run_id>`
- `data/raw/mlops/prd_02/<run_id>`
- `data/raw/mlops/prd_delta/<run_id>`

Mode-scoped PRD raw-to-silver vault folders are generated only when both `output_settings.generate_prd_raw=true` and `output_settings.generate_prd_silver=true`:

- `data/silver/base/<run_id>`
- `data/silver/enhanced/<run_id>`
- `data/silver/mlops/<run_id>`

Silver rebuild flow: base silver is rebuilt from `data/raw/base/prd_01/<run_id>`. Enhanced first rebuilds a base-shaped intermediate vault from `data/raw/enhanced/prd_01/<run_id>/vault_ready_28`, then applies enhanced entity files plus flat relationship/enrichment group files from that same 28-file package. MLOps first rebuilds from `data/raw/mlops/prd_01/<run_id>`, then applies `data/raw/mlops/prd_delta/<run_id>`.

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
.\venv\Scripts\python.exe .\misc\verify_nps_dim_fact.py .\nps_june\data
.\venv\Scripts\python.exe .\misc\compare_all_scd2.py
```

Generate and verify direct MLOps dimensional output from an existing MLOps synthetic vault run:

```powershell
.\venv\Scripts\python.exe .\misc\generate_direct_dim_fact.py --run-id <run_id>
.\venv\Scripts\python.exe .\misc\verify_direct_dim_fact.py .\data\dim_fact_direct\mlops\<run_id>
.\venv\Scripts\python.exe .\misc\verify_nps_dim_fact.py .\data\dim_fact_direct\mlops\<run_id>
```

After a normal run, also verify PRD raw reconciliation:

```powershell
.\venv\Scripts\python.exe .\misc\verify_prd_raw_mlops.py --mode base --run-id <run_id>
.\venv\Scripts\python.exe .\misc\verify_prd_raw_mlops.py --mode enhanced --run-id <run_id>
.\venv\Scripts\python.exe .\misc\verify_prd_raw_mlops.py --mode mlops --run-id <run_id>
```

Build and verify the combined product vault from PRD1/base plus PRD2 raw:

```powershell
.\venv\Scripts\python.exe .\misc\build_product_combined_vault.py --run-id <run_id>
.\venv\Scripts\python.exe .\misc\verify_mlops_synthetic.py .\data\product_combined\<combined_run_id>
.\venv\Scripts\python.exe .\verify_csv.py .\data\product_combined\<combined_run_id>
```

Useful details:

- `validate_churn_kpis.py` validates base churn rules.
- `misc/verify_enhanced_synthetic.py` validates enhanced schema, PKs, FKs, dates, relationships, claim rules, and enhanced business rules.
- `misc/verify_mlops_synthetic.py` validates MLOps DDL alignment and MLOps-only column integrity.
- `misc/verify_mlops_churn_kpis.py` validates workbook churn ratios for MLOps-only KPIs.
- `misc/verify_nps_features.py` validates available/proxy NPS features from the latest NPS workbook.
- `misc/verify_nps_dim_fact.py` validates NPS ratios after ML dim/fact creation by rebuilding the notebook-style `master_df` from exported dim/fact CSV folders or zip files.
- `misc/generate_direct_dim_fact.py` creates the 22-table MLOps dimensional output directly from `data/synthetic/mlops/<run_id>`.
- `misc/verify_direct_dim_fact.py` validates the direct dim/fact output against `mlops/Enhanced_Customer360_Dimensional_Model_DDL.sql`, including all columns, dimension surrogate-key uniqueness, SCD2 field completeness, and fact-to-dimension FK resolution.
- `misc/compare_all_scd2.py` reports SCD2 only when comparable SCD2 data exists.

For latest generated-run results, use `docs/latest_run_validation.md`.

## Source Workbooks And DDLs

Enhanced active DDL:

- `enhanced_360/new/newEnhanced Customer 360 Data Vault DDL.sql`

MLOps Data Vault DDL:

- `mlops/Enhanced_Customer360_DataVault_Model_DDL.sql`

Churn workbook:

- `new_rules/Data Req Churn NPS.xlsx`

NPS workbook:

- `churnps/Data Req Churn NPS.xlsx`, sheet `NPS_Features`

MLOps dimensional/S2T references:

- `mlops/Enhanced_Customer360_Dimensional_Model_DDL.sql`
- `mlops/Enhanced_Customer360_S2T_Mapping_DV_to_Dimensional_Model.xlsx`
- `mlops/Enhanced_Customer360_Data_Dictionary_DataVault_Model.xlsx`

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
- `misc/verify_prd_raw_mlops.py`
- `misc/build_product_combined_vault.py`
- `misc/verify_enhanced_synthetic.py`
- `misc/verify_mlops_synthetic.py`
- `misc/verify_mlops_churn_kpis.py`
- `misc/verify_nps_features.py`
- `misc/generate_direct_dim_fact.py`
- `misc/verify_direct_dim_fact.py`
- `misc/verify_nps_dim_fact.py .\nps_june\data`
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
- intermediate base staging output is written under `data/output/<run_id>` and kept after successful normalization unless `--remove-working-output` is passed

Enhanced output:

- `data/synthetic/enhanced/<run_id>`
- Must match the active enhanced DDL: 80 tables, 25 hubs, 30 links, 25 satellites.

MLOps output:

- `data/synthetic/mlops/<run_id>`
- Must match `mlops/Enhanced_Customer360_DataVault_Model_DDL.sql`.
- Includes the latest 9 feedback/satisfaction fields:
  - `sat_claim.claims_feedback`
  - `sat_complaint.complaint_feedback`
  - `sat_complaint.customer_complaint_satisfaction_score`
  - `sat_customer.customer_onboarding_satisfaction_score`
  - `sat_customer.customer_onboarding_feedback`
  - `sat_marketing_engagement.first_contact_resolution`
  - `sat_policy.policy_renewal_satisfaction_score`
  - `sat_policy.policy_renewal_feedback`
  - `sat_policy.is_renewal_escalation`

`sat_customer.customer_onboarding_feedback` is phrase text, not a generic sentiment label. It follows `nps_settings.onboarding_feedback_distribution`: `20%` negative, `30%` neutral, and `50%` positive. Text starts from `nps_settings.onboarding_feedback_text` and is expanded using `nps_settings.onboarding_feedback_unique_counts` into up to `500` equivalent unique values: `100` negative, `150` neutral, and `250` positive. Positive phrases must include `Comprehensive cover for the price for appropriate policy` and `Flexible excess options available`; negative phrases must include `Policy exclusions not clear` and `Courtesy car not in standard cover`.

SCD2 output:

- `data/scd2/base/<run_id>`
- `data/scd2/enhanced/<run_id>`
- `data/scd2/mlops/<run_id>`

SCD2 is sampled mutation-style output, not full CDC. It is created only when the pipeline finds previous comparable synthetic history. SCD2 rows must contain actual business-value changes; no-op sampled rows are skipped. Stable reference satellites such as `sat_channel` are not emitted unless a meaningful mutable field exists.

Direct MLOps dimensional output:

- `data/dim_fact_direct/mlops/<run_id>`
- Created by `misc/generate_direct_dim_fact.py` from `data/synthetic/mlops/<run_id>`.
- Must contain exactly 22 CSV files from `mlops/Enhanced_Customer360_Dimensional_Model_DDL.sql`: 18 dimensions and 4 facts.
- Dimensions include the full DDL columns and SCD2-style fields where defined: `effective_from_ts`, `effective_to_ts`, `record_version`, and `attr_hash`.
- Facts resolve their surrogate-key columns to dimension surrogate keys. Optional relationships use the `-1` unknown dimension row.
- NPS analytical features are calibrated at the same policy/customer notebook `master_df` grain used by ML. The direct builder can clone descriptive dimension rows, especially account/channel/marketing/policy/claim rows, to preserve one-to-one analytical joins without changing the source MLOps vault hash keys.
- Validate with `misc/verify_direct_dim_fact.py` and `misc/verify_nps_dim_fact.py`.

Optional outputs:

- raw/canonical/silver only with `--include-raw-silver`
- mode-scoped PRD raw is generated by normal `main.py`
- mode-scoped PRD raw-to-silver vault output is generated by normal `main.py`
- `data/new_outputs_src` only with `--include-raw-silver --include-new-outputs-src`

PRD raw contract:

- `data/raw/<mode>/prd_01/<run_id>` is source 1. For `base`, it is the CRM raw extract. For `enhanced`, it contains the CRM raw extract plus internal unprefixed add-on staging under `addons/` and the final `vault_ready_28/` raw package. For `mlops`, it contains the CRM raw extract plus mirrored MLOps source-1 delta files prefixed with `source1_`.
- `data/raw/enhanced/prd_01/<run_id>/vault_ready_28` is the enhanced deliverable raw package. It contains 27 active flat CSV files: 16 CRM foundation files, 8 enhanced entity files, 2 enhanced relationship group files, and 1 enhanced enrichment group file. The folder name is retained for compatibility. This folder alone is sufficient to rebuild the full 80-table enhanced silver vault and does not require JSON bundle parsing because insured object rows are derived from bridge files.
- `data/raw/enhanced/raw_vault/<run_id>` is the enhanced consolidated raw-vault upload package. It contains the 27 `vault_ready_28` source-1 files plus the five enhanced SAP/source-2 files from `data/raw/enhanced/prd_02/<run_id>`, for 32 flat CSV files total.
- Enhanced raw 28 may carry raw-only NPS/feedback source extensions that are not part of enhanced silver DDL: `customer_portfolio.customer_onboarding_satisfaction_score`, `customer_portfolio.customer_onboarding_feedback`, `complaint_register.src_customer_complaint_satisfaction_score`, `complaint_register.src_complaint_feedback`, `claim_register.src_is_fault_claim`, `claim_register.src_claim_satisfaction_score`, `claim_register.src_claims_feedback`, and `claim_register.src_is_claim_complaint_raised`. Keep them in raw packages for Business Vault/modelling; silver builders ignore them when the target DDL lacks those columns.
- Claim raw source extensions must remain aligned to the claim lifecycle. Do not populate claim complaint raised independently of `link_claim_policy` and `link_complaint_policy`; derive it through the shared policy. Closed/settled claim rows should have a 0-5 satisfaction score and feedback, while genuinely open/pending unscored rows may be blank.
- Every enhanced `vault_ready_28` file must include `batch_ref`, `pull_ts`, and `origin_sys`. The package also carries raw-only MLOps-aligned extensions on `policy_register.csv`, `quote_register.csv`, and `campaign_touch.csv`; keep those for Business Vault/modelling even when enhanced silver DDL ignores them.
- `data/raw/<mode>/prd_02/<run_id>` is the SAP/source-2 raw feed from `business_vault/bv.xlsx` sheet `Source2_Structure`. Base/MLOps write lower-case files `person.csv`, `address.csv`, `product.csv`, `home.csv`, `motor.csv`, and `insured_object.csv`; enhanced writes only `person.csv`, `address.csv`, `product.csv`, `home.csv`, and `motor.csv`. All use different `SAP_*` source IDs and matchable business attributes for later Business Vault mastering.
- Every SAP PRD2 file starts with `batch_ref`, `pull_ts`, and `origin_sys`; `origin_sys` must be `SAP`.
- SAP PRD2 structure is documented in `docs/current_rules_reference.md` and `README.md`. The code source of truth is `BASE_PRD2_SAP_TABLES` and `write_raw_base_prd2_variant()` in `generators/raw_prd_generator.py`.
- SAP `person.csv.middle_name` is populated for natural persons and SAP `address.csv.address_line_2` is populated for addresses by deterministic source-2 enrichment. These are source attributes, not matching keys.
- SAP `insured_object.csv` is derived from PRD1 home/motor assets for base/MLOps. Enhanced omits the standalone SAP insured-object file and derives `hub_insured_object`, `sat_insured_object`, and insured-object links from `policy_insured_object_bridge.csv`, `insured_object_home_bridge.csv`, and `insured_object_motor_bridge.csv`, preserving policy start/end date alignment through the source relationships.
- SAP PRD2 deliberately does not apply Business Vault survivorship. It only produces CRM and SAP raw source data with different source IDs. Matching and survivorship rules belong to a later Business Vault layer.
- Business Vault raw matching contract is codified in `BUSINESS_VAULT_PERSON_MATCH_RULES` and checked by `misc/verify_business_vault_raw.py`. Natural matching uses CRM `given_nm/family_nm/dob` to SAP `first_name/last_name/date_of_birth`. Legal matching uses CRM `legal_name/constitution_dt` to SAP `organization/org_establishment_date`. Email, phone, and gender are explicitly excluded from match keys.
- `misc/verify_business_vault_raw.py` also checks SAP PRD2 files exist, are non-empty, match schema, and reconcile row counts/source IDs back to CRM PRD1 files. Base/MLOps expect six SAP files; enhanced expects five and validates insured-object support through bridge-driven derivation.
- Raw Vault loads should create the required hubs, links, and satellites from PRD1/PRD2. Links are needed for source relationships such as person-address, policy-product, product-home, and product-motor. PIT tables are not raw outputs; build PITs later in the Business Vault/consumption layer when point-in-time snapshots are required.
- The unprefixed CRM files in `data/raw/enhanced/prd_01/<run_id>` and `data/raw/mlops/prd_01/<run_id>` must match `data/raw/base/prd_01/<run_id>` file-for-file. Enhanced add-on extracts are staged as unprefixed CSVs under `data/raw/enhanced/prd_01/<run_id>/addons` and then packaged into `vault_ready_28`; MLOps mirrored add-on extracts use `source1_*.csv`.
- Raw CRM and PRD1 file names omit the redundant `crm_` prefix because the source is already represented by the folder. Examples: `party_master.csv`, `address_book.csv`, `account_book.csv`.
- PRD1 CRM `address_book.csv` includes `address_type_txt` and `region_txt`. Natural-person addresses use `personal`; legal-person addresses use `corporate`. `region_txt` is derived from `country_cd`; European countries map to `Europe`.
- PRD1 CRM `product_catalog.csv` includes `product_type_txt`, `product_variant`, `underwriting_group_txt`, `regulatory_approval_cd`, `product_status_txt`, `product_lob_cd`, and `product_launch_dt`. `product_type_txt` equals `product_cd`; `product_variant` equals `product_line`; codes follow `RAC###` and `LOB###`; launch date must be before the earliest policy start date for that product.
- PRD1 CRM `vehicle_asset.csv` includes `driver_experience_years`, derived from linked natural-person birth date as `max(age - 17, 0)` and capped to a realistic range. If the linked birth date is unavailable, use the deterministic vehicle-based fallback rather than leaving it blank.
- Enhanced PRD1 `property_asset.csv` and `vehicle_asset.csv` carry asset-level insured-object columns: `insured_object_id`, `insured_object_type`, `insured_object_sub_type`, `insured_object_description`, `insured_value`, `currency_code`, `insured_object_start_date`, `insured_object_end_date`, and `insured_object_current_status`. Keep these on the asset files; do not re-add a standalone enhanced `insured_object_register.csv`.
- PRD1 `party_master.csv.legal_job_title_txt` must not be blank. Legal-person rows use business roles; non-legal rows use `NOT_APPLICABLE` so database import does not infer a void/null-only type.
- Enhanced silver does not require `prd_02` or `prd_delta`; its final rebuild input is `prd_01/<run_id>/vault_ready_28`. Enhanced `prd_02` is still generated as the SAP/source-2 feed for Business Vault/raw-vault consolidation, and `raw_vault/<run_id>` is the flat upload package.
- MLOps `prd_delta` must not contain vault-shaped `hub_`, `link_`, or `sat_` files.
- MLOps `prd_delta` contains seven added entity registers: broker, campaign, channel, complaint, insured object, override, and regulation.
- MLOps `prd_delta` also contains source-style bridge and enrichment extracts required to rebuild the MLOps vault without losing relationships or added satellite fields.
- `prd_delta` raw headers must use `src_*` source names, not vault names such as `*_hash_key`, `load_date`, or `record_source`.
- MLOps `prd_delta` files are mirrored into MLOps PRD1 with `source1_` prefixes so source 1 has all relevant raw tables while rebuild tooling can still use the unprefixed `prd_delta` copy.
- `misc/verify_prd_raw_mlops.py` applies to this product delta contract.

Product combined contract:

- `misc/build_product_combined_vault.py` builds `data/product_combined/<timestamp>`.
- `prd_delta` raw includes the relationship and enrichment extracts needed by product-combined rebuilds. `misc/build_product_combined_vault.py` translates `src_*` delta columns back to the MLOps vault schema.
- MLOps `prd_delta` raw carries additional MLOps-only satellite fields through entity extracts and enrichment extracts. New MLOps columns added to the generated MLOps schema should therefore flow into `data/raw/mlops/prd_delta/<run_id>` and `data/silver/mlops/<run_id>`.
- The output is MLOps-DDL-shaped and should be validated with `misc/verify_mlops_synthetic.py`.
- `verify_csv.py` accepts a product-combined folder path and aliases MLOps `hub_address` / `link_person_address` to the base address checks.

Mode silver contract:

- `data/silver/base/<run_id>` is rebuilt from `data/raw/base/prd_01/<run_id>` using `misc/raw_to_silver_sample.py` with PRD1 source-field mapping.
- `data/silver/enhanced/<run_id>` is rebuilt from enhanced `vault_ready_28`; `data/silver/mlops/<run_id>` is rebuilt by applying MLOps `prd_delta` raw deltas to the MLOps PRD1 base-shaped rebuild with `misc/build_product_combined_vault.py`.
- Silver schemas are taken from the corresponding synthetic mode headers so `silver/enhanced` matches `synthetic/enhanced` and `silver/mlops` matches `synthetic/mlops`.
- Base silver should pass `verify_csv.py` for PK/FK, dates, lifecycle, claim/churn field logic, and base relationship rules. Enhanced/MLOps silver should also preserve the corresponding mode schema and PRD2 relationships.

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
- `policy_issue_date` is generated before or on `policy_start_date` for enhanced/MLOps policy rows.
- NPS policy issuance TAT follows the workbook distribution: `70% 0-2 days`, `20% 3-7 days`, and `10% >7 days`.
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

NPS workbook:

- `churnps/Data Req Churn NPS.xlsx`, sheet `NPS_Features`

NPS validator:

- `misc/verify_nps_features.py`

NPS is implemented using existing generated columns and derived proxies only. It must not add or change DDL columns unless the user explicitly asks for a schema change.

MLOps/enhanced generation includes a final NPS alignment pass at the policy/customer grain used by the ML notebook before CSV write. That pass may update descriptive satellite attributes, but it must not modify hub rows, link rows, hash keys, business keys, PK/FK relationships, or date-order validity.

Config:

- `config/scenario_v1.json` under `nps_settings`
- Apply safe workbook ratios through existing fields.
- Keep `nps_score_distribution` for the 30/35/35 band split and `nps_score_value_weights` for the score-level ripple. The detractor band should show lower volume at scores `2-4` than at `0`, `1`, `5`, and `6`.
- Policy issuance TAT now intentionally follows the NPS workbook, including the `>7 days` bucket. Do not reintroduce the older 7-day cap unless the user asks for that tradeoff.
- Digital onboarding preserves the configured `ONLINE 75` / `BRANCH 25` split and then aligns `sat_account.account_creation_type` to NPS: higher-NPS customers skew online and lower-NPS customers skew branch. `digital_onboarding_nps_overlap` must keep online visible for low NPS `1-4` and branch visible for high NPS `8-10`, per the 17/06 ML feedback.
- Drop-off during onboarding preserves the configured accepted/drop-off split, then applies `quote_dropoff_status_distribution` inside non-accepted `sat_quote.quote_status` rows. `EXPIRED` should skew low NPS, `SENT` moderate NPS, and `CREATED` high NPS.
- NPS premium increase uses both quote renewal amounts and policy/fact-style renewal amounts with `premium_increase_distribution`; it must not change `churn_settings` premium probabilities. `<=5%` skews high NPS, `5-10%` passive NPS, and `>10%` detractor NPS. The policy/fact-style fields are required because the ML notebook computes Premium Increase from `fact_policy.policy_renewal_current_period_amt` and `policy_renewal_next_period_amt`.
- Digital renewal uses `sat_policy.sales_channel` for renewal policies with `policy_cycle > 1` and `digital_renewal_distribution`; online skews high NPS, agent-assisted skews passive NPS, and branch skews lower NPS.
- Claim complaint flag uses claim-policy and complaint-policy links plus `claim_complaint_distribution`; about 15% of claim-linked policies should have complaints, selected from lower-NPS customers first.
- Self-service adoption is a derived proxy, not a new column: online account creation plus `operational_paperless_consent = Y` plus account last access within 30 days. `self_service_adoption_distribution` targets 65% adopted, selected from higher-NPS customers first.
- Complaint resolution turnaround uses `complaint_resolution_distribution` in the final NPS pass: 60% 0-2 days, 30% 3-7 days, 10% >7 days, preserving complaint date/status consistency.
- Renewal contacts use `sat_marketing_engagement.customer_service_call_frequency` with `renewal_contact_distribution`: 60% 0-1 contacts, 30% 2-3 contacts, 10% >3 contacts. Low contacts skew high NPS; high contacts skew low NPS.
- Claim settlement TAT uses `sat_claim.claim_reported_date` and `claim_settlement_date` with `claim_settlement_tat_distribution`: 70% 0-15 days, 20% 16-30 days, 10% >30 days. The pass preserves non-negative settlement dates and keeps dates within the linked policy period when possible.
- Claim channel uses `sat_claim.claim_channel` with `claim_channel_distribution`: 70% online, 20% agent, 10% branch. Agent at NPS 10 should remain very low per ML feedback.
- Claim CSAT uses `sat_claim.claim_satisfaction_score`; high scores skew high NPS and low scores skew low NPS. `claims_feedback` is recomputed after shaping.
- Customer CSAT uses `sat_customer.customer_satisfaction`; customer onboarding score/feedback are recomputed after shaping.
- Complaint escalation uses `sat_complaint.is_financial_ombudsman_service_referral` with `complaint_escalation_distribution`: 98% N, 2% Y, with Y selected from NPS 0-2 first.
- Complaint status outcome uses `sat_complaint.complaint_upheld_status` with `complaint_status_outcome_distribution`: 65% not upheld, 20% upheld, 15% partially upheld.
- Repeat complaint uses `link_complaint_policy` plus `link_policy_customer` with `repeat_complaint_distribution`: 90% no repeat, 10% repeat among complaint customers, with repeats selected from NPS 0-3 first.

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

Safe enforced NPS ratios:

- `sat_customer.nps_score`: `DETRACTOR 30`, `PASSIVE 35`, `PROMOTER 35`.
- `sat_account.account_creation_type`: `ONLINE 75`, `BRANCH 25`.
- `sat_quote.quote_status`: accepted/drop-off split from `quote_dropoff_distribution`.
- `sat_policy.policy_issue_date` to `policy_start_date`: overall `70% 0-2 days`, `20% 3-7 days`, `10% >7 days`, with NPS-aware shape from `policy_issuance_tat_by_nps_band` so promoters lean fast, passives lean medium, and low-NPS detractors lean slow.
- `sat_claim.is_litigation`: `92%` non-escalated, `8%` escalated for enhanced/MLOps claim rows.
- `sat_claim.claim_reported_date` to `claim_settlement_date`: `70%` 0-15 days, `20%` 16-30 days, `10%` >30 days, with NPS-aware shaping.
- `sat_claim.claim_channel`: `70%` online, `20%` agent, `10%` branch, with high-NPS customers skewing online.
- `sat_marketing_engagement.customer_service_call_frequency`: `60%` 0-1 contacts, `30%` 2-3 contacts, `10%` >3 contacts.
- `sat_complaint.is_financial_ombudsman_service_referral`: `98%` N, `2%` Y.
- `sat_complaint.complaint_upheld_status`: `65%` not upheld, `20%` upheld, `15%` partially upheld.
- Repeat complaint proxy: about `10%` repeat among complaint customers.

Report-only or inherited ratios:

- Complaint volume stays controlled by `enhanced_settings.complaint_customer_rate`.
- Complaint resolution TAT stays aligned to MLOps complaint-resolution churn calibration.

Not directly coverable without new survey/contact/escalation tables:

- onboarding CSAT
- renewal CSAT
- customer contact count during renewal
- renewal escalation flag
- support/servicing CSAT
- first contact resolution
- complaint CSAT
- generic customer feedback category

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
- Generate legacy source-specific outputs unless the user explicitly enables both `--include-raw-silver` and `--include-new-outputs-src`.
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
