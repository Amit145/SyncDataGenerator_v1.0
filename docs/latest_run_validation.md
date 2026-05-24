# Latest Run Validation

Run validated: `20260524214157`

Base folder: `data/synthetic/base/20260524214157`

Enhanced folder: `data/synthetic/enhanced/20260524214157`

MLOps folder: `data/synthetic/mlops/20260524214157`

## Summary

| Area | Expected | Current | Status |
|---|---|---|---|
| Base churn KPI source fields | Required fields populated and valid | Source-field checks passed | Pass |
| Base churn workbook ratios | Directional and within workbook ranges | Directional, with 6 close-boundary failures | Partial |
| Enhanced synthetic Data Vault | 80 tables, valid schema, PK/FK, business rules | Enhanced validator passed | Pass |
| MLOps synthetic Data Vault | MLOps DDL schema and new columns valid | MLOps schema validator passed | Pass |
| MLOps churn workbook ratios | New MLOps KPI ratios within workbook ranges | 9 ratio issues | Partial |
| Enhanced claim financials | Non-null, non-negative, paid amount not above claim amount | Financial checks passed | Pass |
| SCD2 | Created only when prior comparable history exists | No latest SCD2 run folder found | Not available |
| Raw/canonical/silver outputs | Optional | Skipped by default; use `--include-raw-silver` when needed | Skipped |

## Base Churn Ratios

| Rule | Band | Expected churn | Current churn | Status |
|---|---:|---:|---:|---|
| Current premium | LOW | 10-18% | 20.11% | Fail |
| Current premium | MEDIUM | 15-25% | 26.84% | Fail |
| Current premium | HIGH | 25-40% | 40.64% | Fail |
| Current premium | VERY_HIGH | 40-55% | 54.72% | Pass |
| Premium % increase | <0% | 8-12% | 9.70% | Pass |
| Premium % increase | 0-5% | 15-20% | 15.32% | Pass |
| Premium % increase | 5-10% | 25-35% | 25.03% | Pass |
| Premium % increase | >10% | 45-65% | 44.29% | Fail |
| Absolute premium increase | <=0 | 8-12% | 9.70% | Pass |
| Absolute premium increase | 1-50 | 15-22% | 16.10% | Pass |
| Absolute premium increase | 51-100 | 25-38% | 30.08% | Pass |
| Absolute premium increase | >100 | 45-65% | 48.24% | Pass |
| Claim count | 0 | 12-18% | 13.96% | Pass |
| Claim count | 1 | 20-30% | 21.05% | Pass |
| Claim count | 2 | 30-45% | 31.70% | Pass |
| Claim count | 3+ | 45-60% | 44.82% | Fail |
| Add-ons | 0 | 25-40% | 40.69% | Fail |
| Add-ons | 1 | 18-28% | 28.85% | Fail |
| Add-ons | 2 | 12-22% | 22.54% | Fail |
| Add-ons | 3+ | 8-18% | 18.27% | Fail |
| Tenure | <1 year | 35-50% | 39.19% | Pass |
| Tenure | 1-2 years | 25-35% | 30.16% | Pass |
| Tenure | 3-5 years | 15-25% | 21.74% | Pass |
| Tenure | >5 years | 8-15% | 13.94% | Pass |
| Marketing engagement | HIGH | 8-15% | 7.73% | Fail |
| Marketing engagement | MEDIUM | 18-30% | 16.46% | Fail |
| Marketing engagement | LOW | 35-55% | 32.79% | Fail |
| Marketing engagement | NONE | 50-70% | 48.17% | Fail |
| Driver experience | <2y | 25-40% | 33.11% | Pass |
| Driver experience | 2-5y | 18-30% | 26.86% | Pass |
| Driver experience | 6-10y | 15-25% | 25.00% | Pass |
| Driver experience | >10y | 10-18% | 18.24% | Fail |
| Sales channel variance | AGENT > BRANCH/ONLINE, no workbook target | AGENT 48.16%, BRANCH 27.85%, ONLINE 21.64% | Pass |

## Enhanced And Claim Rules

| Rule | Expected | Current | Status |
|---|---|---|---|
| Enhanced table count | 80 tables | 80 tables | Pass |
| Enhanced group count | 25 hubs, 30 links, 25 sats | 25 hubs, 30 links, 25 sats | Pass |
| Enhanced schema order | Matches enhanced DDL | All table columns ok | Pass |
| Enhanced primary keys | Unique and present | All PK checks ok | Pass |
| Enhanced foreign keys | Child keys resolve to parent hubs | All FK checks ok | Pass |
| Enhanced business rules | Date, claim, vehicle, and relationship checks | Enhanced validator passed | Pass |
| Enhanced claim rows | Generated claims linked to policy coverage | Validated | Pass |
| Claim amount logic | Amounts derived from configured severity/coverage rules | Claim financial checks passed | Pass |

## MLOps Churn Ratios

| Rule | Band | Expected churn | Current churn | Status |
|---|---:|---:|---:|---|
| Auto-renew enabled | ON | 5-12% | 8.01% | Pass |
| Auto-renew enabled | OFF | 35-55% | 48.31% | Pass |
| NCD years | 0-1 | 25-40% | 34.49% | Pass |
| NCD years | 2-4 | 18-30% | 23.93% | Pass |
| NCD years | 5-8 | 15-25% | 19.21% | Pass |
| NCD years | 9+ | 10-18% | 14.47% | Pass |
| Payment method | Annual | 8-15% | 4.12% | Fail |
| Payment method | Monthly DD | 15-25% | 37.14% | Fail |
| Payment method | Card/Manual | 25-40% | 55.52% | Fail |
| Direct debit cancellation | No | 10-18% | 17.99% | Pass |
| Direct debit cancellation | Yes | 55-75% | 61.53% | Pass |
| Missed payments | 0 | 10-18% | 6.68% | Fail |
| Missed payments | 1 | 25-35% | 25.63% | Pass |
| Missed payments | 2 | 40-55% | 59.90% | Fail |
| Missed payments | 3+ | 60-75% | 67.35% | Pass |
| Loyalty discount | Retained | 8-18% | 13.96% | Pass |
| Loyalty discount | Not Applied | 18-30% | 26.95% | Pass |
| Loyalty discount | Removed | 40-60% | 66.37% | Fail |
| Installment default | No | 10-18% | 16.01% | Pass |
| Installment default | Yes | 50-75% | 63.28% | Pass |
| Fault claim | No | 12-20% | 16.31% | Pass |
| Fault claim | Yes | 30-50% | 55.74% | Fail |
| Claim satisfaction | High | 8-15% | 11.48% | Pass |
| Claim satisfaction | Neutral | 18-30% | 24.18% | Pass |
| Claim satisfaction | Low | 40-65% | 56.00% | Pass |
| Retention contacted | No | 12-22% | 15.11% | Pass |
| Retention contacted | Yes | 35-55% | 48.03% | Pass |
| Call sentiment | Positive | 8-15% | 8.94% | Pass |
| Call sentiment | Neutral | 18-30% | 29.34% | Pass |
| Call sentiment | Negative | 40-65% | 52.39% | Pass |
| Engagement score | High | 8-15% | 0.00% | Fail |
| Engagement score | Medium | 18-30% | 8.44% | Fail |
| Engagement score | Low | 35-55% | 48.32% | Pass |
| Engagement score | Very Low | 50-70% | 56.88% | Pass |

## Notes

- Enhanced and MLOps schema/business validation pass on this run.
- Claim satisfaction is present and passes all workbook ranges.
- Base churn rules are directionally aligned, but a few bands are outside the workbook range by small margins.
- MLOps churn has 9 ratio issues. The remaining misses are mostly coupled fields: payment method with direct-debit cancellation, missed payments with installment/default behavior, removed loyalty discount, fault-claim yes, and high/medium engagement score.
- Default `main.py` generation now focuses on synthetic, enhanced, MLOps, and SCD2 outputs. Raw/canonical/silver and `new_outputs_src` are opt-in because they add significant runtime.
