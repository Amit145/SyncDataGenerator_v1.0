# NPS Features Reference

Source workbook: `nps_june/nps_june_updated.xlsx`

Sheet: `NPS_Features`

Validation command:

```powershell
.\venv\Scripts\python.exe .\misc\verify_nps_features.py
```

Post dim/fact validation command, using the same `master_df` grain as the ML notebook:

```powershell
.\venv\Scripts\python.exe .\misc\verify_nps_dim_fact.py .\nps_june\data
```

Direct MLOps dimensional generation and validation from a synthetic MLOps vault run:

```powershell
.\venv\Scripts\python.exe .\misc\generate_direct_dim_fact.py --run-id <run_id>
.\venv\Scripts\python.exe .\misc\verify_direct_dim_fact.py .\data\dim_fact_direct\mlops\<run_id>
.\venv\Scripts\python.exe .\misc\verify_nps_dim_fact.py .\data\dim_fact_direct\mlops\<run_id>
```

The NPS implementation uses existing generated columns and derived proxies only. It does not add columns, alter Data Vault DDL, change primary/foreign keys, or relax existing date, churn, and claim rules.

For MLOps/enhanced generation, the final NPS pass runs at the policy/customer grain used by the ML notebook before CSVs are written. It updates only existing satellite attributes such as premium amounts, account access/channel, claim service fields, complaint outcome fields, and marketing contact frequency. It does not change hubs, links, hash keys, business keys, PK/FK relationships, or configured date-order rules.

For direct MLOps dimensional output, `misc/generate_direct_dim_fact.py` reads `data/synthetic/mlops/<run_id>` and writes the 22 DDL-defined dimensional tables under `data/dim_fact_direct/mlops/<run_id>`. The direct output is calibrated at the ML notebook `master_df` grain, so account, channel, marketing, quote, complaint, and claim analytical rows can be cloned with new surrogate keys while preserving the original vault business flow. This avoids notebook join drift and keeps all NPS ratios testable after fact/dimension joins.

The direct dim/fact structural verifier checks:

- all 22 DDL tables exist
- CSV columns exactly match `mlops/Enhanced_Customer360_Dimensional_Model_DDL.sql`
- dimension surrogate keys are unique
- SCD2 fields are populated where the DDL has them
- fact surrogate keys resolve to dimension surrogate keys, including `-1` unknown rows for optional relationships

## Config

Workbook ratios that can be applied safely are configured in `config/scenario_v1.json` under `nps_settings`.

| Config key | Purpose | Current target |
|---|---|---|
| `nps_score_distribution` | NPS score band mix for `sat_customer.nps_score` | `DETRACTOR 30`, `PASSIVE 35`, `PROMOTER 35` |
| `digital_onboarding_distribution` | Onboarding channel proxy from `sat_account.account_creation_type`; assignment is NPS-shaped after customer NPS is known | `ONLINE 75`, `BRANCH 25` |
| `digital_onboarding_nps_overlap` | Keeps the ML-requested overlap so online is present for low NPS `1-4` and branch is present for high NPS `8-10` while preserving the overall `75/25` mix | `online_low_nps_1_4_rate 6`, `branch_high_nps_8_10_rate 5` |
| `quote_dropoff_distribution` | Quote funnel drop-off proxy from `sat_quote.quote_status`; controls accepted vs dropped onboarding | `ACCEPTED 92`, `DROPOFF 8` |
| `quote_dropoff_status_distribution` | Status mix inside non-accepted quote drop-offs; assignment is NPS-shaped after customer NPS is known | `CREATED 50`, `SENT 35`, `EXPIRED 15` |
| `premium_increase_distribution` | Renewal premium increase proxy from current and next renewal amounts; assignment is NPS-shaped after customer NPS is known | `<=5% 70`, `5-10% 20`, `>10% 10` |
| `digital_renewal_distribution` | Renewal channel proxy from `sat_policy.sales_channel` for policies with `policy_cycle > 1`; assignment is NPS-shaped after customer NPS is known | `ONLINE 70`, `AGENT 20`, `BRANCH 10` |
| `claim_complaint_distribution` | Claim complaint proxy from claim-policy and complaint-policy links; assignment is NPS-shaped after customer NPS is known | `NO_COMPLAINT 85`, `COMPLAINT 15` |
| `self_service_adoption_distribution` | Digital engagement flag derived from online account creation, paperless consent, and recent account access | `ADOPTED 65`, `NOT_ADOPTED 35` |
| `policy_issuance_tat_distribution` | Overall policy issue-to-start turnaround from `sat_policy.policy_issue_date` and `policy_start_date` | `0-2 days 70`, `3-7 days 20`, `>7 days 10` |
| `policy_issuance_tat_by_nps_band` | NPS-aware issuance TAT shape while preserving the overall 70/20/10 target | Promoters lean `0-2`, passives lean `3-7`, low-NPS detractors lean `>7` |
| `claim_escalation_distribution` | Claim escalation proxy from `sat_claim.is_litigation` | `NON_ESCALATED 92`, `ESCALATED 8` |
| `complaint_resolution_distribution` | Complaint resolution TAT ratio report | `0-2 days 60`, `3-7 days 30`, `>7 days 10` |
| `renewal_contact_distribution` | Number of customer contacts during renewal from `sat_marketing_engagement.customer_service_call_frequency` | `0-1 contacts 60`, `2-3 contacts 30`, `>3 contacts 10` |
| `claim_settlement_tat_distribution` | Claim settlement turnaround from `sat_claim.claim_reported_date` and `claim_settlement_date` | `0-15 days 70`, `16-30 days 20`, `>30 days 10` |
| `claim_channel_distribution` | Claim service channel from `sat_claim.claim_channel` | `ONLINE 70`, `AGENT 20`, `BRANCH 10` |
| `complaint_escalation_distribution` | Complaint escalation proxy from `sat_complaint.is_financial_ombudsman_service_referral` | `NON_ESCALATED 98`, `ESCALATED 2` |
| `complaint_status_outcome_distribution` | Complaint outcome from `sat_complaint.complaint_upheld_status` | `NOT_UPHELD 65`, `UPHELD 20`, `PARTIALLY_UPHELD 15` |
| `repeat_complaint_distribution` | Repeat complaint proxy from repeated complaint-policy/customer links | `NO_REPEAT 90`, `REPEAT 10` |
| `self_service_score_distribution` | Derived self-service adoption score report | `0 10`, `1 20`, `2 35`, `3 35` |
| `preserve_existing_churn_date_rules` | Legacy guard for older runs; current NPS policy issuance TAT follows the workbook distribution | `false` |

## Workbook Rows Covered

Columns G/H/I in the workbook are used as the source and logic contract:

| Workbook feature | Status | Workbook source and logic | Implementation |
|---|---|---|---|
| NPS score | Enforced | `dim_customer.net_promoter_score`; direct column, classify into promoter/passive/detractor | `sat_customer.nps_score` and `net_promotor_code_segment`; `0-6 = detractor`, `7-8 = passive`, `9-10 = promoter`; configured as `30/35/35`. |
| Policy issuance TAT | Enforced | `policy_start_ts` minus `created_ts` application-date proxy | `sat_policy.policy_start_date` minus `policy_issue_date`; generated as overall `70% 0-2 days`, `20% 3-7 days`, and `10% >7 days`, with NPS-aware shape by customer NPS band. |
| Digital onboarding | Enforced | `dim_account.account_creation_type`; online/digital/self-service = digital | `sat_account.account_creation_type`; `ONLINE` is digital and `BRANCH` is assisted; configured as `75/25`. Online is assigned preferentially to higher NPS customers, and branch is assigned preferentially to lower NPS customers. A configurable overlap keeps some online records in NPS `1-4` and some branch records in NPS `8-10`, matching the 17/06 ML feedback that both cross-over populations must be visible. |
| Drop-off during onboarding | Enforced | `quote_id` present and `quote_status` not accepted | `sat_quote.quote_status`; accepted quotes represent conversion and non-accepted statuses represent drop-off; configured around `8%` drop-off. Non-accepted statuses follow `CREATED 50`, `SENT 35`, `EXPIRED 15`; expired skews low NPS, sent skews moderate NPS, and created skews high NPS. |
| Premium increase | Enforced for NPS | `(next_period_amt - current_period_amt) / current_period_amt * 100` | Enforced on both `sat_quote.renewal_amt_current_period` / `renewal_amt_next_period` and policy/fact-style `sat_policy.renewal_amount_current_period` / `renewal_amount_next_period`, because the ML notebook analyzes `fact_policy.policy_renewal_current_period_amt` and `policy_renewal_next_period_amt`. Configured as `<=5% 70`, `5-10% 20`, `>10% 10`. `<=5%` skews high NPS, `5-10%` skews passive NPS, and `>10%` skews detractor NPS. Existing churn premium rules remain separate. |
| Digital renewal | Enforced for NPS | online/digital/web sales channel and `policy_cycle > 1` | `sat_policy.sales_channel` for renewal policies; configured as `ONLINE 70`, `AGENT 20`, `BRANCH 10`. Online skews high NPS, agent-assisted skews passive NPS, and branch skews lower NPS. |
| Claim escalation flag | Enforced for MLOps/enhanced claim rows | `dim_claim.is_litigation = Y` | `sat_claim.is_litigation`; configured around `8%` escalated claims. Escalated claims are selected from lower-NPS linked customers first, while high-NPS claim customers skew non-escalated. |
| Claim settlement TAT | Enforced for NPS | `claim_settlement_date - claim_reported_date` | `sat_claim.claim_reported_date` and `claim_settlement_date`; configured as `0-15 days 70`, `16-30 days 20`, `>30 days 10`. Fast settlement skews high NPS, medium settlement skews passive NPS, and slow settlement skews low NPS while keeping claim dates inside the linked policy period where possible. |
| Claim channel used | Enforced for NPS | Claim servicing channel | `sat_claim.claim_channel`; configured as `ONLINE 70`, `AGENT 20`, `BRANCH 10`. Online claims skew high NPS, agent claims skew passive NPS, and branch claims skew lower NPS. Agent volume at NPS 10 is intentionally minimized per ML feedback. |
| Claim CSAT | Enforced for NPS | Claim satisfaction score | `sat_claim.claim_satisfaction_score`; score `4-5` skews high NPS, score `3-4` skews passive/moderate NPS, and score `1-2` skews low NPS. `claims_feedback` is recomputed from the shaped score. |
| Claim complaint flag | Enforced for NPS | customer has both claim and complaint | Claim-linked policies are selected so about `15%` have a linked formal complaint and `85%` do not. Complaint-linked claims skew lower NPS; high-NPS claim customers skew no complaint. |
| Self-service adoption | Enforced for NPS | `(account_creation_type='Online') + paperless consent + account activity within 30 days` | Derived from `sat_account.account_creation_type`, `sat_account.account_last_access`, and `sat_person.operational_paperless_consent`. Configured as `ADOPTED 65`, `NOT_ADOPTED 35`; adopted customers skew higher NPS and non-adopters skew lower/moderate NPS. No dedicated self-service adoption column is added. |
| Resolution turnaround time | Enforced for NPS | `complaint_resolved_date - complaint_date` for formal complaints | Complaint resolution dates are generated as `0-2 days 60`, `3-7 days 30`, and `>7 days 10`. Fast resolution skews higher NPS, moderate resolution skews passive NPS, and slow resolution skews lower NPS. Complaint-linked rows are spread across exact NPS values inside each band, not only representative scores such as `0`, `1`, `8`, and `10`. |
| Number of customer contacts during renewal | Enforced for NPS | Call/contact frequency before renewal | `sat_marketing_engagement.customer_service_call_frequency`; configured as `0-1 contacts 60`, `2-3 contacts 30`, `>3 contacts 10`. Lower contacts skew high NPS, moderate contacts skew passive NPS, and high contacts skew low NPS. |
| Customer CSAT | Enforced for NPS | Overall customer satisfaction | `sat_customer.customer_satisfaction`; satisfied values skew high NPS, neutral values skew passive/moderate NPS, and dissatisfied values skew low NPS. Onboarding satisfaction score/feedback are recomputed from the shaped value. |
| Complaint escalation flag | Enforced for NPS | FOS/regulatory referral | `sat_complaint.is_financial_ombudsman_service_referral`; configured as `2% Y`, selected from lower NPS complaint rows with score spread across the detractor band, while passive/promoter complaint rows skew non-escalated. |
| Complaint status outcome | Enforced for NPS | Complaint upheld status | `sat_complaint.complaint_upheld_status`; configured as `Not Upheld 65`, `Upheld 20`, `Partially Upheld 15`, with lower NPS skewing not upheld and moderate/high NPS carrying upheld/partial outcomes. Complaint-linked rows are spread across exact scores within detractor, passive, and promoter bands. |
| Repeat complaint flag | Enforced for NPS | More than one complaint for a complaint customer | Derived from `link_complaint_policy` and `link_policy_customer`; configured around `10%` repeat among complaint customers, selected from NPS `0-3` first. |
| Onboarding feedback | Enforced for NPS | Customer onboarding feedback text | `sat_customer.customer_onboarding_feedback` and `dim_customer.customer_onboarding_feedback`; configured as `20%` negative, `30%` neutral, `50%` positive. Text is expanded into up to `500` equivalent unique values: `100` negative, `150` neutral, and `250` positive, instead of generic sentiment labels. |

## Not Directly Coverable

These workbook features still need missing operational data such as explicit survey response tables or detailed contact-center event history:

- Renewal CSAT
- Renewal escalation flag
- Support/servicing CSAT
- First contact resolution
- Generic customer feedback text/category

Those features can be added later if the model gets new interaction, survey, or escalation tables.
