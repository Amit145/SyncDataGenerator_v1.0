# Latest Run Validation

Run validated: `20260526182423`

Base folder: `data/synthetic/base/20260526182423`

Enhanced folder: `data/synthetic/enhanced/20260526182423`

MLOps folder: `data/synthetic/mlops/20260526182423`

SCD2 folders: not generated for this run.

## Summary

| Area | Expected | Current | Status |
|---|---|---|---|
| Base churn KPI source fields | Required fields populated and valid | Source-field checks passed | Pass |
| Base churn workbook ratios | Directional and within workbook ranges | 6 validator checks still outside range | Partial |
| Enhanced synthetic Data Vault | 80 tables, valid schema, PK/FK, business rules | Enhanced validator passed | Pass |
| MLOps synthetic Data Vault | MLOps DDL schema and new columns valid | MLOps schema validator passed | Pass |
| MLOps churn workbook ratios | MLOps KPI ratios within workbook ranges | 10 band failures | Partial |
| NPS feature workbook rules | Available/proxy NPS features valid | NPS validator passed; quote drop-off warning only | Pass |
| Synthetic SCD2 | Generated when prior comparable history exists | No SCD2 folders found | Not available |

## Base Churn Ratios

| Rule | Band | Expected churn | Current churn | Status |
|---|---:|---:|---:|---|
| Current premium | LOW | 10-18% | 19.97% | Fail |
| Current premium | MEDIUM | 15-25% | 26.81% | Fail |
| Current premium | HIGH | 25-40% | 40.71% | Fail |
| Current premium | VERY_HIGH | 40-55% | 55.09% | Fail |
| Premium % increase | <0% | 8-12% | 10.26% | Pass |
| Premium % increase | 0-5% | 15-20% | 15.82% | Pass |
| Premium % increase | 5-10% | 25-35% | 25.19% | Pass |
| Premium % increase | >10% | 45-65% | 44.69% | Fail |
| Absolute premium increase | <=0 | 8-12% | 10.26% | Pass |
| Absolute premium increase | 1-50 | 15-22% | 16.64% | Pass |
| Absolute premium increase | 51-100 | 25-38% | 30.16% | Pass |
| Absolute premium increase | >100 | 45-65% | 48.19% | Pass |
| Claim count | 0 | 12-18% | 14.22% | Pass |
| Claim count | 1 | 20-30% | 21.23% | Pass |
| Claim count | 2 | 30-45% | 31.79% | Pass |
| Claim count | 3+ | 45-60% | 44.83% | Fail |
| Add-ons | 0 | 25-40% | 40.60% | Fail |
| Add-ons | 1 | 18-28% | 28.70% | Fail |
| Add-ons | 2 | 12-22% | 22.50% | Fail |
| Add-ons | 3+ | 8-18% | 18.20% | Fail |
| Tenure | <1 year | 35-50% | 39.66% | Pass |
| Tenure | 1-2 years | 25-35% | 30.10% | Pass |
| Tenure | 3-5 years | 15-25% | 21.86% | Pass |
| Tenure | >5 years | 8-15% | 14.33% | Pass |
| Marketing engagement proxy | HIGH | 8-15% | 7.70% | Fail |
| Marketing engagement proxy | MEDIUM | 18-30% | 16.36% | Fail |
| Marketing engagement proxy | LOW | 35-55% | 32.91% | Fail |
| Marketing engagement proxy | NONE | 50-70% | 48.27% | Fail |
| Driver experience | <2y | 25-40% | 33.33% | Pass |
| Driver experience | 2-5y | 18-30% | 27.01% | Pass |
| Driver experience | 6-10y | 15-25% | 24.07% | Pass |
| Driver experience | >10y | 10-18% | 18.22% | Fail |
| Sales channel variance | AGENT > BRANCH/ONLINE | AGENT 47.37%, BRANCH 28.42%, ONLINE 21.96% | Pass |

## Enhanced Rules

| Rule | Expected | Current | Status |
|---|---|---|---|
| Enhanced table count | 80 tables | 80 tables | Pass |
| Enhanced group count | 25 hubs, 30 links, 25 sats | 25 hubs, 30 links, 25 sats | Pass |
| Enhanced schema order | Matches enhanced DDL | All table columns ok | Pass |
| Enhanced primary keys | Unique and present | All PK checks ok | Pass |
| Enhanced foreign keys | Child keys resolve to parent hubs | All FK checks ok | Pass |
| Enhanced business rules | Date, claim, vehicle, and relationship checks | Enhanced validator passed | Pass |
| Enhanced claim rows | Generated claims linked to policy coverage | 995 claims | Pass |

## MLOps Churn Ratios

| Rule | Band | Expected churn | Current churn | Status |
|---|---:|---:|---:|---|
| Policy type | NEW_BUSINESS | 35-55% | 51.06% | Pass |
| Policy type | RENEWAL | 8-18% | 13.00% | Pass |
| Policy renewal | Y | 8-18% | 13.00% | Pass |
| Policy renewal | N | 35-55% | 51.06% | Pass |
| Auto-renew enabled | ON | 5-12% | 7.81% | Pass |
| Auto-renew enabled | OFF | 35-55% | 45.01% | Pass |
| NCD years | 0-1 | 25-40% | 35.55% | Pass |
| NCD years | 2-4 | 18-30% | 23.01% | Pass |
| NCD years | 5-8 | 15-25% | 16.97% | Pass |
| NCD years | 9+ | 10-18% | 12.06% | Pass |
| Payment method | Annual | 8-15% | 9.09% | Pass |
| Payment method | Monthly DD | 15-25% | 25.26% | Fail |
| Payment method | Card/Manual | 25-40% | 39.03% | Pass |
| Direct debit cancellation | No | 10-18% | 18.00% | Pass |
| Direct debit cancellation | Yes | 55-75% | 73.16% | Pass |
| Missed payments | 0 | 10-18% | 8.62% | Fail |
| Missed payments | 1 | 25-35% | 22.98% | Fail |
| Missed payments | 2 | 40-55% | 36.09% | Fail |
| Missed payments | 3+ | 60-75% | 59.97% | Fail |
| Installment default | No | 10-18% | 14.40% | Pass |
| Installment default | Yes | 50-75% | 46.97% | Fail |
| Customer satisfaction | Very Satisfied | 8-15% | 13.04% | Pass |
| Customer satisfaction | Satisfied | 15-25% | 21.63% | Pass |
| Customer satisfaction | Neutral | 25-40% | 33.72% | Pass |
| Customer satisfaction | Dissatisfied | 45-65% | 55.49% | Pass |
| Complaint resolution days | 0-7 | 8-15% | 35.48% | Fail |
| Complaint resolution days | 8-30 | 18-30% | 46.77% | Fail |
| Complaint resolution days | 31-60 | 35-50% | 48.57% | Pass |
| Complaint resolution days | 61+ | 50-70% | 64.71% | Pass |
| Fault claim | No | 12-20% | 16.06% | Pass |
| Fault claim | Yes | 30-50% | 42.86% | Pass |
| Claim satisfaction | High | 8-15% | 12.08% | Pass |
| Claim satisfaction | Neutral | 18-30% | 24.11% | Pass |
| Claim satisfaction | Low | 40-65% | 59.84% | Pass |
| Retention contacted | No | 12-22% | 16.44% | Pass |
| Retention contacted | Yes | 35-55% | 47.53% | Pass |
| Call sentiment | Positive | 8-15% | 11.28% | Pass |
| Call sentiment | Neutral | 18-30% | 22.48% | Pass |
| Call sentiment | Negative | 40-65% | 51.76% | Pass |
| Engagement score | High | 8-15% | 9.74% | Pass |
| Engagement score | Medium | 18-30% | 17.33% | Fail |
| Engagement score | Low | 35-55% | 33.35% | Fail |
| Engagement score | Very Low | 50-70% | 51.36% | Pass |

## NPS Features

| Feature | Expected | Current | Status |
|---|---|---|---|
| NPS score | Integer 0-10 | Range check passed | Pass |
| NPS segment mapping | 0-6 Detractors, 7-8 Passive, 9-10 Promoters | Segment follows score | Pass |
| NPS bimodal distribution | Detractor 30-50%, Passive 10-30%, Promoter 30-50% | Detractor 40.10%, Passive 27.95%, Promoter 31.95% | Pass |
| Digital onboarding proxy | Digital/assisted both 35-65% | Digital 49.02%, Assisted 50.98% | Pass |
| Policy issuance TAT | 0-7 days | Passed | Pass |
| Drop-off onboarding proxy | 5-30% target | 50.26%, warning only due quote funnel model shape | Warn |
| Digital renewal proxy | 15-45% | 21.72% | Pass |
| Claim settlement TAT | Settlement not before reported date | Median 0 days; range passed | Pass |
| Claim escalation proxy | Litigation <15% | 9.35% | Pass |
| Complaint resolution TAT | Valid nonnegative range | Median 16 days | Pass |
| Complaint escalation proxy | FOS referral <15% | 8.52% | Pass |
| Complaint status outcome | Populated status/upheld fields | Populated | Pass |

## Notes

- Enhanced and MLOps schema validation pass on this run.
- NPS available/proxy features pass, with quote drop-off reported as a warning because the current quote funnel creates many unlinked quotes.
- The latest MLOps churn tuning fixed long complaint-resolution bands and very-low engagement, but pushed short complaint-resolution bands, monthly DD, and low/medium engagement out of range.
- Missed-payment/default rules remain the hardest coupled family because `missed_payment_count`, `is_installment_default`, `is_direct_debit_cancellation`, and `payment_method` all constrain the same policy rows.
- Base churn remains directionally aligned, but some marginal workbook ranges are slightly outside target.
- SCD2 was not generated or not discoverable for this run, so SCD2 validation is not available.
