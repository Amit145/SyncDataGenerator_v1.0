# Latest Run Validation

Run validated: `20260522092135`

Base folder: `data/synthetic/base/20260522092135`

Enhanced folder: `data/synthetic/enhanced/20260522092135`

## Summary

- Base structural, relationship, account, and timeline rules: pass.
- Enhanced structural, primary-key, and foreign-key rules: pass.
- SCD2 validation: not run because `data/scd2` has no CSV files.
- Enhanced claim financial checks: pass.
- Churn workbook ratio checks: 6 rule groups still fail, mostly by small margins.

## Configured Distribution Ratios

These ratios compare configured source/input distribution weights with the latest generated current ratios.

| Rule | Band | Expected ratio | Current ratio | Current rows |
|---|---:|---:|---:|---:|
| Current premium source mix | LOW | 35.00% | 35.04% | 1784 |
| Current premium source mix | MEDIUM | 35.00% | 34.19% | 1741 |
| Current premium source mix | HIGH | 20.00% | 20.56% | 1047 |
| Current premium source mix | VERY_HIGH | 10.00% | 10.21% | 520 |
| Renewal movement source mix | DECREASE | 10.00% | 9.56% | 487 |
| Renewal movement source mix | 0_5 | 18.00% | 18.38% | 936 |
| Renewal movement source mix | 5_10 | 30.00% | 29.67% | 1511 |
| Renewal movement source mix | GT_10 | 42.00% | 42.38% | 2158 |
| Claim count source mix | 0 | 18.00% | 17.99% | 916 |
| Claim count source mix | 1 | 25.00% | 25.08% | 1277 |
| Claim count source mix | 2 | 25.00% | 25.51% | 1299 |
| Claim count source mix | 3 | 18.00% | 17.05% | 868 |
| Claim count source mix | 4 | 9.00% | 9.17% | 467 |
| Claim count source mix | 5 | 5.00% | 5.20% | 265 |
| Add-on source mix | BASE_ONLY | 35.00% | 35.35% | 1800 |
| Add-on source mix | ONE_ADD_ON | 30.00% | 29.79% | 1517 |
| Add-on source mix | TWO_ADD_ONS | 22.00% | 22.56% | 1149 |
| Add-on source mix | THREE_PLUS_ADD_ONS | 13.00% | 12.29% | 626 |
| Marketing engagement source mix | HIGH | 15.00% | 8.30% | 415 |
| Marketing engagement source mix | MEDIUM | 30.00% | 30.74% | 1537 |
| Marketing engagement source mix | LOW | 35.00% | 33.66% | 1683 |
| Marketing engagement source mix | NONE | 20.00% | 27.30% | 1365 |
| Driver experience source mix | LT_2Y | 12.00% | 11.84% | 1895 |
| Driver experience source mix | Y2_5 | 20.00% | 20.59% | 3294 |
| Driver experience source mix | Y6_10 | 25.00% | 25.09% | 4015 |
| Driver experience source mix | GT_10 | 43.00% | 42.48% | 6796 |
| Vehicle segment source mix | STANDARD | 65.00% | 65.11% | 1157 |
| Vehicle segment source mix | PREMIUM | 25.00% | 23.80% | 423 |
| Vehicle segment source mix | HIGH_RISK | 10.00% | 11.09% | 197 |
| Service call proxy source mix | NONE | 45.00% | 45.42% | 2271 |
| Service call proxy source mix | CALL | 55.00% | 54.58% | 2729 |

## Churn Ratios

These ratios compare configured workbook churn ranges with the latest generated churned-policy ratios.

| Rule | Band | Expected | Current | Status |
|---|---:|---:|---:|---|
| Current premium | LOW | 10-18% | 19.96% | Fail |
| Current premium | MEDIUM | 15-25% | 26.71% | Fail |
| Current premium | HIGH | 25-40% | 40.59% | Fail |
| Current premium | VERY_HIGH | 40-55% | 55.19% | Fail |
| Premium % increase | <0% | 8-12% | 9.86% | Pass |
| Premium % increase | 0-5% | 15-20% | 15.38% | Pass |
| Premium % increase | 5-10% | 25-35% | 24.95% | Fail |
| Premium % increase | >10% | 45-65% | 44.67% | Fail |
| Absolute premium increase | <=0 | 8-12% | 9.86% | Pass |
| Absolute premium increase | 1-50 | 15-22% | 16.11% | Pass |
| Absolute premium increase | 51-100 | 25-38% | 29.84% | Pass |
| Absolute premium increase | >100 | 45-65% | 48.58% | Pass |
| Claim count | 0 | 12-18% | 13.97% | Pass |
| Claim count | 1 | 20-30% | 21.93% | Pass |
| Claim count | 2 | 30-45% | 31.41% | Pass |
| Claim count | 3+ | 45-60% | 44.81% | Fail |
| Add-ons | 0 | 25-40% | 40.56% | Fail |
| Add-ons | 1 | 18-28% | 28.54% | Fail |
| Add-ons | 2 | 12-22% | 22.37% | Fail |
| Add-ons | 3+ | 8-18% | 18.05% | Fail |
| Tenure | <1 year | 35-50% | 39.30% | Pass |
| Tenure | 1-2 years | 25-35% | 30.51% | Pass |
| Tenure | 3-5 years | 15-25% | 20.89% | Pass |
| Tenure | >5 years | 8-15% | 14.39% | Pass |
| Marketing engagement | HIGH | 8-15% | 7.84% | Fail |
| Marketing engagement | MEDIUM | 18-30% | 16.46% | Fail |
| Marketing engagement | LOW | 35-55% | 33.11% | Fail |
| Marketing engagement | NONE | 50-70% | 48.47% | Fail |
| Driver experience | <2y | 25-40% | 33.54% | Pass |
| Driver experience | 2-5y | 18-30% | 27.59% | Pass |
| Driver experience | 6-10y | 15-25% | 24.86% | Pass |
| Driver experience | >10y | 10-18% | 18.19% | Fail |
| Vehicle segment | STANDARD | 12-22% | 20.74% | Pass |
| Vehicle segment | PREMIUM | 20-35% | 27.66% | Pass |
| Vehicle segment | HIGH_RISK | 30-50% | 40.10% | Pass |

Sales-channel churn variance passes:

| Channel | Current churn |
|---|---:|
| AGENT | 47.65% |
| BRANCH | 27.63% |
| ONLINE | 22.16% |

## Claim Financial Checks

Enhanced `sat_claim.csv` has `284` rows.

| Check | Result |
|---|---|
| Null claim amounts | 0 |
| Zero claim amounts | 0 |
| Negative claim amounts | 0 |
| `claims_paid > claim_amount` | 0 |
| Negative reserves | 0 |
| Null reserves | 0 |
| Negative expenses | 0 |
| Null expenses | 0 |

Claim band distribution:

| Claim band | Rows |
|---|---:|
| 6k-8k | 51 |
| 8k-10k | 61 |
| 10k-13k | 25 |
| 13+ | 147 |

Claim band sort distribution:

| Claim band sort | Rows |
|---:|---:|
| 2 | 51 |
| 3 | 61 |
| 4 | 25 |
| 5 | 147 |

## Notes

- Base and enhanced churn ratios are the same for shared policy-driven dimensions.
- Vehicle segment churn can only be fully validated where enhanced direct policy-to-motor links exist.
- SCD2 output was not available in this run, so SCD2 rules were not validated.
