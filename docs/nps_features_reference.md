# NPS Features Reference

Source workbook: `new_rules/nps/npsn.xlsx`

Previous workbook retained for comparison: `new_rules/nps/Data Req Churn NPS.xlsx`

Sheet: `NPS_Features`

Validation command:

```powershell
.\venv\Scripts\python.exe .\misc\verify_nps_features.py
```

The NPS implementation uses existing synthetic/enhanced/MLOps columns and derived proxies only. It does not add columns, alter Data Vault DDL, change primary or foreign keys, or relax date, churn, and claim rules.

## Covered Features

| Workbook feature | Source / proxy | Rule |
|---|---|---|
| NPS score | `sat_customer.nps_score` | Integer `0-10`, generated with a bimodal detractor/promoter shape. |
| NPS segment | `sat_customer.net_promotor_code_segment` | `0-6 = DETRACTORS`, `7-8 = PASSIVE`, `9-10 = PROMOTERS`. |
| Policy issuance TAT | `sat_policy.policy_issue_date`, `sat_policy.policy_start_date` | Start date must be on/after issue date and no more than 7 days later. |
| Digital onboarding | `sat_account.account_creation_type` | Online/digital/self-service maps to digital onboarding. |
| Drop-off during onboarding | `sat_quote` plus `link_policy_quote` | Quote records linked to policies are treated as converted; unlinked quotes represent funnel drop-off. |
| Premium increase | `sat_policy.renewal_amount_current_period`, `sat_policy.renewal_amount_next_period` | Covered by existing churn premium movement checks. |
| Digital renewal | `sat_policy.sales_channel`, `sat_policy.policy_cycle` | Online policy with completed cycle is treated as digital renewal proxy. |
| Claim CSAT | `sat_claim.claim_satisfaction_score` | Existing MLOps/churn claim satisfaction score. |
| Claim settlement TAT | `sat_claim.claim_reported_date`, `sat_claim.claim_settlement_date` | Settlement date must not be before reported date. |
| Claim escalation flag | `sat_claim.is_litigation` | Litigation is used as escalation proxy. |
| Claim complaint flag | complaint/policy/customer links | Partially derivable through complaint and policy context. |
| Claim channel used | `sat_claim.claim_channel` | Existing channel value is populated. |
| SLA breach | claim and complaint turnaround proxies | Derivable from TAT thresholds; no dedicated SLA table exists. |
| Self-service adoption | `sat_account.account_creation_type`, access timing, consent fields | Partially derivable as a composite proxy. |
| Resolution turnaround time | `sat_complaint.complaint_date`, `sat_complaint.complaint_resolved_date` | Complaint resolution TAT proxy. |
| Complaint resolution TAT | `sat_complaint.complaint_date`, `sat_complaint.complaint_resolved_date` | Same formal complaint TAT proxy. |
| Complaint escalation flag | `sat_complaint.is_financial_ombudsman_service_referral` | FOS referral is used as escalation proxy. |
| Complaint status outcome | `sat_complaint.complaint_status`, `sat_complaint.complaint_upheld_status` | Populated outcome fields. |
| Repeat complaint flag | complaint links by customer | Derivable when a customer has more than one linked complaint. |

## Not Directly Coverable

| Workbook feature | Reason |
|---|---|
| Onboarding CSAT | No separate onboarding survey response table exists. |
| Renewal CSAT | No separate renewal survey response table exists. |
| Number of customer contacts during renewal | No contact-center interaction table exists. |
| Renewal escalation flag | No escalation tracking table exists. |
| Support/servicing CSAT | No separate servicing survey response table exists. |
| First contact resolution | No contact-center interaction or first-resolution event table exists. |
| Complaint CSAT | No separate complaint survey response table exists. |

These unavailable fields can be approximated later if the model adds interaction, survey, or escalation tables. Until then, the validator checks only direct columns and defensible proxies.

## Latest Workbook Delta

Compared with the previous NPS workbook, `npsn.xlsx` adds:

- `Source Table (mlops_enhanced_gold)` on `NPS_Features`.
- A new `Onboarding Feedback` row under the `Onboarding` category.

`Onboarding Feedback` does not currently include source table, logic, expected distribution, examples, or business rules. It is therefore documented but not implemented until the workbook provides actionable detail.
