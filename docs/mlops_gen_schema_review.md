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
| `sat_policy` | `loyalty_discount_usage` | Uses completed cycle and churn context. Long-tenure active policies are more likely to use loyalty discount. |
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
| `sat_policy` | `loyalty_discount_usage` | More likely `Y` for non-churned policies with at least three completed cycles. |
| `sat_policy` | `is_installment_default` | `Y` when `missed_payment_count >= 2`. |

## Workbook Note

The workbook sheet names and previews were inspected through the XLSX XML package because `openpyxl` is not installed in the current venv. The Data Vault DDL review and implemented column rules above are complete; full workbook tab parsing through pandas will need `openpyxl`.
