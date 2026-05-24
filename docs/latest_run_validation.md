# Latest Run Validation

Run validated: `20260524132817`

Base folder: `data/synthetic/base/20260524132817`

Enhanced folder: `data/synthetic/enhanced/20260524132817`

MLOps folder: `data/synthetic/mlops/20260524132817`

## Summary

- Base structural, relationship, account, and timeline rules: pass.
- Enhanced structural, primary-key, and foreign-key rules: pass.
- MLOps Data Vault schema and MLOps-only column rules: pass.
- SCD2 validation: not run because `data/scd2` has no CSV files.
- Enhanced claim financial checks: pass.
- Churn workbook ratio checks: 6 rule groups still fail, mostly by small margins.

## Configured Distribution Ratios

These ratios compare configured source/input distribution weights with the latest generated current ratios.

| Rule | Band | Expected ratio | Current ratio | Current rows |
|---|---:|---:|---:|---:|
| Current premium source mix | LOW | 35.00% | 33.94% | 857 |
| Current premium source mix | MEDIUM | 35.00% | 35.96% | 908 |
| Current premium source mix | HIGH | 20.00% | 19.60% | 495 |
| Current premium source mix | VERY_HIGH | 10.00% | 10.50% | 265 |
| Renewal movement source mix | DECREASE | 10.00% | 10.97% | 277 |
| Renewal movement source mix | 0_5 | 18.00% | 18.34% | 463 |
| Renewal movement source mix | 5_10 | 30.00% | 28.59% | 722 |
| Renewal movement source mix | GT_10 | 42.00% | 42.10% | 1063 |
| Claim count source mix | 0 | workbook band | 19.33% | 488 |
| Claim count source mix | 1 | workbook band | 24.24% | 612 |
| Claim count source mix | 2 | workbook band | 24.44% | 617 |
| Claim count source mix | 3+ | workbook band | 32.00% | 808 |
| Add-on source mix | BASE_ONLY | 35.00% | 35.80% | 904 |
| Add-on source mix | ONE_ADD_ON | 30.00% | 28.95% | 731 |
| Add-on source mix | TWO_ADD_ONS | 22.00% | 21.54% | 544 |
| Add-on source mix | THREE_PLUS_ADD_ONS | 13.00% | 13.70% | 346 |
| Marketing engagement source mix | HIGH | 15.00% | 8.72% | 218 |
| Marketing engagement source mix | MEDIUM | 30.00% | 29.04% | 726 |
| Marketing engagement source mix | LOW | 35.00% | 33.52% | 838 |
| Marketing engagement source mix | NONE | 20.00% | 28.72% | 718 |
| Driver experience source mix | LT_2Y | 12.00% | 12.26% | 981 |
| Driver experience source mix | Y2_5 | 20.00% | 20.85% | 1668 |
| Driver experience source mix | Y6_10 | 25.00% | 25.00% | 2000 |
| Driver experience source mix | GT_10 | 43.00% | 41.89% | 3351 |
| Vehicle segment source mix | STANDARD | 65.00% | 66.02% | 579 |
| Vehicle segment source mix | PREMIUM | 25.00% | 22.46% | 197 |
| Vehicle segment source mix | HIGH_RISK | 10.00% | 11.52% | 101 |
| Service call proxy source mix | NONE | 45.00% | 43.76% | 1094 |
| Service call proxy source mix | CALL | 55.00% | 56.24% | 1406 |

## Churn Ratios

These ratios compare configured workbook churn ranges with the latest generated churned-policy ratios.

| Rule | Band | Expected | Current | Status |
|---|---:|---:|---:|---|
| Current premium | LOW | 10-18% | 20.07% | Fail |
| Current premium | MEDIUM | 15-25% | 27.09% | Fail |
| Current premium | HIGH | 25-40% | 40.81% | Fail |
| Current premium | VERY_HIGH | 40-55% | 55.09% | Fail |
| Premium % increase | <0% | 8-12% | 10.47% | Pass |
| Premium % increase | 0-5% | 15-20% | 16.41% | Pass |
| Premium % increase | 5-10% | 25-35% | 25.76% | Pass |
| Premium % increase | >10% | 45-65% | 44.68% | Fail |
| Absolute premium increase | <=0 | 8-12% | 10.47% | Pass |
| Absolute premium increase | 1-50 | 15-22% | 16.94% | Pass |
| Absolute premium increase | 51-100 | 25-38% | 29.93% | Pass |
| Absolute premium increase | >100 | 45-65% | 48.50% | Pass |
| Claim count | 0 | 12-18% | 15.57% | Pass |
| Claim count | 1 | 20-30% | 21.24% | Pass |
| Claim count | 2 | 30-45% | 32.09% | Pass |
| Claim count | 3+ | 45-60% | 44.80% | Fail |
| Add-ons | 0 | 25-40% | 40.93% | Fail |
| Add-ons | 1 | 18-28% | 28.73% | Fail |
| Add-ons | 2 | 12-22% | 22.61% | Fail |
| Add-ons | 3+ | 8-18% | 18.21% | Fail |
| Tenure | <1 year | 35-50% | 40.63% | Pass |
| Tenure | 1-2 years | 25-35% | 29.71% | Pass |
| Tenure | 3-5 years | 15-25% | 23.04% | Pass |
| Tenure | >5 years | 8-15% | 14.91% | Pass |
| Marketing engagement | HIGH | 8-15% | 7.66% | Fail |
| Marketing engagement | MEDIUM | 18-30% | 16.15% | Fail |
| Marketing engagement | LOW | 35-55% | 32.49% | Fail |
| Marketing engagement | NONE | 50-70% | 47.65% | Fail |
| Driver experience | <2y | 25-40% | 33.64% | Pass |
| Driver experience | 2-5y | 18-30% | 27.89% | Pass |
| Driver experience | 6-10y | 15-25% | 24.90% | Pass |
| Driver experience | >10y | 10-18% | 18.24% | Fail |
| Vehicle segment | STANDARD | 12-22% | 20.74% | Pass |
| Vehicle segment | PREMIUM | 20-35% | 27.66% | Pass |
| Vehicle segment | HIGH_RISK | 30-50% | 40.10% | Pass |

Sales-channel churn variance passes:

| Channel | Current churn |
|---|---:|
| AGENT | 48.17% |
| BRANCH | 29.13% |
| ONLINE | 21.09% |

## Claim Financial Checks

Enhanced `sat_claim.csv` has `140` rows.

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
| 6k-8k | 24 |
| 8k-10k | 28 |
| 10k-13k | 16 |
| 13+ | 72 |

Claim band sort distribution:

| Claim band sort | Rows |
|---:|---:|
| 2 | 24 |
| 3 | 28 |
| 4 | 16 |
| 5 | 72 |

## Notes

- Base and enhanced churn ratios are the same for shared policy-driven dimensions.
- MLOps validation passes for the latest run and uses `data/synthetic/mlops/<run_id>`.
- Vehicle segment churn can only be fully validated where enhanced direct policy-to-motor links exist.
- SCD2 output was not available in this run, so SCD2 rules were not validated.
