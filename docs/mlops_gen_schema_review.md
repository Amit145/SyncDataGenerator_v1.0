# MLOps Gen Schema Review

Source reviewed: `mlops/mlops_gen/Enhanced_Customer360_DataVault_Model_DDL.sql`

Compared against:

- existing enhanced DDL: `enhanced_360/new/newEnhanced Customer 360 Data Vault DDL.sql`
- latest enhanced output: `data/synthetic/enhanced/20260522092135`

## DDL Integrity

| Check | Result |
|---|---:|
| New DDL tables | 80 |
| Existing enhanced DDL tables | 80 |
| Latest enhanced output tables | 80 |
| Primary-key definitions | 80 |
| Foreign-key definitions | 85 |
| Tables missing PK | 0 |
| Broken FK table references | 0 |

The new MLOps Data Vault DDL is structurally valid. It keeps the same table set as the current enhanced output, so the gap is column-level rather than table-level.

## Table-Level Delta

| Comparison | Result |
|---|---|
| Tables added in new DDL vs existing enhanced DDL | None |
| Tables removed in new DDL vs existing enhanced DDL | None |
| New DDL tables missing from latest enhanced output | None |
| Latest enhanced output tables not in new DDL | None |

## New or Changed Columns

| Table | Change | Columns |
|---|---|---|
| `sat_address` | Added | `region` |
| `sat_address` | Type changed | `type`: `VARCHAR(20)` to `STRING` |
| `sat_claim` | Added | `suspected_amount`, `is_fault_claim`, `claim_satisfaction_score` |
| `sat_claim` | Renamed/fixed typo | old `suspectd_amount` removed, new `suspected_amount` added |
| `sat_marketing_engagement` | Added | `has_retention_team_interaction`, `customer_service_call_frequency`, `average_call_sentiment`, `engagement_score` |
| `sat_motor` | Added | `driver_experience_years` |
| `sat_policy` | Added | `is_auto_renew_enabled`, `no_claims_discount_years`, `payment_method`, `is_direct_debit_cancellation`, `missed_payment_count`, `loyalty_discount_usage`, `is_installment_default` |

## Latest Enhanced Output Gaps

These columns exist in the new MLOps DDL but are not currently emitted by `data/synthetic/enhanced/20260522092135`.

| Table | Missing output columns |
|---|---|
| `sat_address` | `region` |
| `sat_claim` | `suspected_amount`, `is_fault_claim`, `claim_satisfaction_score` |
| `sat_marketing_engagement` | `has_retention_team_interaction`, `customer_service_call_frequency`, `average_call_sentiment`, `engagement_score` |
| `sat_motor` | `driver_experience_years` |
| `sat_policy` | `is_auto_renew_enabled`, `no_claims_discount_years`, `payment_method`, `is_direct_debit_cancellation`, `missed_payment_count`, `loyalty_discount_usage`, `is_installment_default` |

The latest enhanced output also still has `sat_claim.suspectd_amount`, while the new DDL expects the corrected column name `suspected_amount`.

## Lineage And Natural-Flow Rules

The MLOps columns are not generated independently from the rest of the vault. They are derived after the base and enhanced table context is built, so they inherit the same person, policy, claim, channel, churn, and vehicle relationships.

| Table | Column | Lineage / consistency rule |
|---|---|---|
| `sat_address` | `region` | Uses the existing address geography. `state` is preferred; `city` is the fallback. |
| `sat_claim` | `suspected_amount` | Uses the existing enhanced suspected-amount logic with corrected spelling. Value is nonnegative and cannot exceed `claim_amount`. |
| `sat_claim` | `is_fault_claim` | Uses existing fraud, suspicious-claim, third-party, and third-party score indicators. |
| `sat_claim` | `claim_satisfaction_score` | Uses claim status, fraud/suspicion, litigation, fault, amount, and reserve. Lower scores naturally follow more difficult claim cases. |
| `sat_marketing_engagement` | `has_retention_team_interaction` | Uses linked person-to-policy churn context and low-engagement behavior. |
| `sat_marketing_engagement` | `customer_service_call_frequency` | Uses churn context: churned customers get higher call-frequency distribution than stable customers. |
| `sat_marketing_engagement` | `average_call_sentiment` | Uses call frequency and engagement score. Higher call pressure or lower engagement pushes sentiment negative. |
| `sat_marketing_engagement` | `engagement_score` | Uses opened-email, marketing status, and churn context. |
| `sat_motor` | `driver_experience_years` | Uses the linked natural person `birth_date` proxy: `max(age - 17, 0)`. This is aligned with the driver-experience churn rule. |
| `sat_policy` | `is_auto_renew_enabled` | Uses policy status, completed cycle, and churn context. Active and longer-tenure policies are more likely to auto-renew. |
| `sat_policy` | `no_claims_discount_years` | Uses completed `policy_cycle` reduced by active/previous/declined claim pressure. |
| `sat_policy` | `payment_method` | Uses policy stability context. `DIRECT_DEBIT` is more common for stable policies; churned policies can still use any allowed method. |
| `sat_policy` | `is_direct_debit_cancellation` | Can be `Y` only when payment method is `DIRECT_DEBIT`, policy is churned, and missed payments exist. |
| `sat_policy` | `missed_payment_count` | Uses churn, risk score, and claim pressure. Churned or higher-risk policies get higher counts. |
| `sat_policy` | `loyalty_discount_usage` | Uses `RETAINED`, `NOT_APPLIED`, or `REMOVED` from completed cycle and churn context. |
| `sat_policy` | `is_installment_default` | Derived directly from missed payments; `Y` when missed payments are at least two. |

## Output Implication

The generator now supports a separate `data/synthetic/mlops/<run_id>` output based on this DDL. Existing base, synthetic enhanced, churn, and claim logic is preserved; the MLOps output uses the new DDL and applies only the MLOps-specific column enrichment needed by the added columns.

When a previous MLOps run exists, `main.py` also creates MLOps SCD2 deltas under `data/scd2/mlops/<run_id>`, using the same SCD2 mutation flow as enhanced output.

Validation command:

```powershell
.\venv\Scripts\python.exe misc\verify_mlops_synthetic.py
```

## Implemented MLOps Column Rules

| Table | Column | Implemented generation rule |
|---|---|---|
| `sat_address` | `region` | Derived from existing `state`, falling back to `city`. |
| `sat_claim` | `suspected_amount` | Corrected spelling from `suspectd_amount`; nonnegative and capped at `claim_amount`. |
| `sat_claim` | `is_fault_claim` | `Y` when fraud/suspicious/third-party indicators suggest fault; otherwise `N`. |
| `sat_claim` | `claim_satisfaction_score` | Score from `1` to `10`, reduced by open/pending status, fraud/suspicion, litigation, fault, high amount, or outstanding reserve. |
| `sat_marketing_engagement` | `has_retention_team_interaction` | `Y` for churned/low-engagement policy context, otherwise `N`. |
| `sat_marketing_engagement` | `customer_service_call_frequency` | Higher call-count distribution for churned customers, lower for non-churned customers. |
| `sat_marketing_engagement` | `average_call_sentiment` | `NEGATIVE`, `NEUTRAL`, or `POSITIVE` from call frequency and engagement score. |
| `sat_marketing_engagement` | `engagement_score` | Numeric `0-100` score from opened-email/status/churn context. |
| `sat_motor` | `driver_experience_years` | Derived from linked natural-person `birth_date` using `max(age - 17, 0)`, replacing the previous hidden proxy with an explicit MLOps column. |
| `sat_policy` | `is_auto_renew_enabled` | More likely `Y` for active/longer-tenure policies and more likely `N` for churned policies. |
| `sat_policy` | `no_claims_discount_years` | Derived from completed policy cycle minus recent claim pressure, clipped to nonnegative years. |
| `sat_policy` | `payment_method` | One of `DIRECT_DEBIT`, `CARD`, or `BANK_TRANSFER`; direct debit is more common for stable policies. |
| `sat_policy` | `is_direct_debit_cancellation` | `Y` only when payment method is `DIRECT_DEBIT`, policy is churned, and missed payments exist. |
| `sat_policy` | `missed_payment_count` | Higher for churned or higher-risk policies; lower for active/stable policies. |
| `sat_policy` | `loyalty_discount_usage` | Uses `RETAINED`, `NOT_APPLIED`, or `REMOVED`; retained is more common for stable policies, removed is more common for churned policies. |
| `sat_policy` | `is_installment_default` | `Y` when `missed_payment_count >= 2`. |

## MLOps Churn KPI Coverage

The new DDL makes 12 workbook rows coverable that were previously marked `NA` because the fields did not exist. Their expected churn ranges are configured in `config/scenario_v1.json` under `churn_settings.mlops_churn_expected_ranges`.

Validation command:

```powershell
.\venv\Scripts\python.exe misc\verify_mlops_churn_kpis.py
```

| KPI | MLOps source | Workbook range |
|---|---|---|
| Policy type | `sat_policy.policy_type` | New Business `35-55%`, Renewal `8-18%` |
| Policy renewal | `sat_policy.is_policy_renewal` | Y `8-18%`, N `35-55%` |
| Auto-renew enabled | `sat_policy.is_auto_renew_enabled` | ON `5-12%`, OFF `35-55%` |
| Fault claim | `sat_claim.is_fault_claim` via `link_claim_policy` | No `12-20%`, Yes `30-50%` |
| NCD years | `sat_policy.no_claims_discount_years` | `0-1` `25-40%`, `2-4` `18-30%`, `5-8` `15-25%`, `9+` `10-18%` |
| Payment method | `sat_policy.payment_method` | Annual `8-15%`, Monthly DD `15-25%`, Card/Manual `25-40%` |
| Direct debit cancellation | `sat_policy.is_direct_debit_cancellation` | No `10-18%`, Yes `55-75%` |
| Missed payments | `sat_policy.missed_payment_count` | `0` `10-18%`, `1` `25-35%`, `2` `40-55%`, `3+` `60-75%` |
| Retention interaction | `sat_marketing_engagement.has_retention_team_interaction` | No `12-22%`, Yes `35-55%` |
| Claim satisfaction | `sat_claim.claim_satisfaction_score` | High `8-15%`, Neutral `18-30%`, Low `40-65%` |
| Customer satisfaction | `sat_customer.customer_satisfaction` via `link_policy_customer` | Very Satisfied `8-15%`, Satisfied `15-25%`, Neutral `25-40%`, Dissatisfied `45-65%` |
| Complaint resolution days | `sat_complaint.complaint_date` to `complaint_resolved_date` via `link_complaint_policy` | `0-7` `8-15%`, `8-30` `18-30%`, `31-60` `35-50%`, `61+` `50-70%` |
| Loyalty discount | `sat_policy.loyalty_discount_usage` | Retained `8-18%`, Not Applied `18-30%`, Removed `40-60%` |
| Installment default | `sat_policy.is_installment_default` | No `10-18%`, Yes `50-75%` |
| Call sentiment | `sat_marketing_engagement.average_call_sentiment` | Positive `8-15%`, Neutral `18-30%`, Negative `40-65%` |
| Engagement score | `sat_marketing_engagement.engagement_score` | High `8-15%`, Medium `18-30%`, Low `35-55%`, Very Low `50-70%` |

Policy renewal is separate from auto-renew. `is_policy_renewal` describes renewal/new-business context; `is_auto_renew_enabled` describes whether automatic renewal is enabled. Engagement score follows the workbook direction: high engagement has lower expected churn than low or very low engagement.

Claim fault and claim satisfaction churn ratios are validated by the MLOps churn validator. Claims may be linked to active, lapsed, or cancelled policies when the claim reported date remains inside the linked policy coverage window.

## Workbook Note

The workbook sheet names and previews were inspected through the XLSX XML package because `openpyxl` is not installed in the current venv. The Data Vault DDL review and implemented column rules above are complete; full workbook tab parsing through pandas will need `openpyxl`.
