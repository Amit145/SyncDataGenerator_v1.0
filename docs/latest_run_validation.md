# Latest Run Validation

Run validated: `20260618123141`

Synthetic folders:

- `data/synthetic/mlops/20260618123141`

Direct MLOps dimensional folder:

- `data/dim_fact_direct/mlops/20260618123141`

Validated commands:

```powershell
.\venv\Scripts\python.exe .\misc\generate_direct_dim_fact.py --run-id 20260618123141
.\venv\Scripts\python.exe .\misc\verify_direct_dim_fact.py .\data\dim_fact_direct\mlops\20260618123141
.\venv\Scripts\python.exe .\misc\verify_nps_dim_fact.py .\data\dim_fact_direct\mlops\20260618123141
```

## Claims Two-Source Validation Addendum

Claims run validated: `20260715091202`

Claims raw folders:

- `data/raw/claims/20260715091202/raw`
- `data/raw/claims/20260715091202/prd_01`
- `data/raw/claims/20260715091202/prd_02`
- `data/raw/claims/20260715091202/raw_vault`

Validated commands:

```powershell
.\venv\Scripts\python.exe .\misc\verify_claims_two_source_raw.py --run-id 20260715091202
.\venv\Scripts\python.exe .\misc\verify_claims_product.py --run-id 20260715091202
```

Claims two-source results:

| Area | Expected | Current | Status |
|---|---|---|---|
| Claims PRD1/PRD2 source split | PRD1 CRM/source-1 and PRD2 SAP/source-2 from `Claims_2Sources_DataTables.xlsx` | Present under `data/raw/claims/20260715091202/prd_01` and `prd_02` | Pass |
| Claim rows | PRD1/PRD2 counts match source raw | `4817` in source, PRD1, and PRD2 | Pass |
| Loss event rows | PRD1/PRD2 counts match source raw | `4817` in source, PRD1, and PRD2 | Pass |
| Claim investigation rows | PRD1/PRD2 counts match source raw | `1081` in source, PRD1, and PRD2 | Pass |
| Claims raw vault | Consolidated 23-file LDM raw shape | `23` CSV files | Pass |
| Claims references | Claim/loss/investigation references resolve | Verified by `verify_claims_two_source_raw.py` and `verify_claims_product.py` | Pass |
| Claims silver vault | Bronze/silver/gold load from `raw_vault` | Claims product verifier passed | Pass |

`_source_manifest.csv` is informational only. Claims loading needs `prd_01` and `prd_02` to reconstruct `raw_vault`; claims bronze/silver/gold use `raw_vault`.

## Summary

| Area | Expected | Current | Status |
|---|---|---|---|
| MLOps synthetic vault source | Existing generated MLOps vault used as direct dim/fact source | `data/synthetic/mlops/20260618123141` | Pass |
| Direct MLOps dim/fact count | 22 DDL-defined tables | 22 CSV files generated | Pass |
| Direct MLOps dim/fact columns | Every CSV header exactly matches dimensional DDL | Passed | Pass |
| Direct dimension keys | Surrogate keys unique by dimension | Passed | Pass |
| Direct fact FKs | Fact surrogate keys resolve to dimensions; optional links use `-1` unknown rows | Passed | Pass |
| Direct SCD2 fields | `effective_from_ts`, `effective_to_ts`, `record_version` populated where defined | Passed | Pass |
| Direct post-dim/fact NPS workbook rules | Notebook `master_df` grain ratios | Passed | Pass |
| Base churn workbook ratios | Directional behavior plus strict target ranges | Directionally aligned; some strict bands remain just outside range | Partial |
| MLOps churn workbook ratios | MLOps KPI churn ranges | Mostly aligned; 9 strict band issues remain | Partial |

## Direct Dim/Fact NPS Ratios

| Feature | Expected | Current | Status |
|---|---:|---:|---|
| NPS detractor | 25-35% | 30.14% | Pass |
| NPS passive | 30-40% | 35.41% | Pass |
| NPS promoter | 30-40% | 34.46% | Pass |
| NPS 2-4 dip | Avg 2-4 lower than shoulders | 67.67 vs 139.50 | Pass |
| Digital onboarding - digital | 70-80% | 75.01% | Pass |
| Digital onboarding - assisted | 20-30% | 24.99% | Pass |
| Self-service adoption - adopted | 60-70% | 62.81% | Pass |
| Self-service adoption - not adopted | 30-40% | 37.19% | Pass |
| Policy issuance TAT 0-2 days | 65-75% | 69.27% | Pass |
| Policy issuance TAT 3-7 days | 15-25% | 20.40% | Pass |
| Policy issuance TAT >7 days | 8-12% | 10.34% | Pass |
| Quote drop-off | 5-10% | 8.00% | Pass |
| Quote accepted | 90-95% | 92.00% | Pass |
| Drop-off status created | 45-55% | 50.00% | Pass |
| Drop-off status sent | 30-40% | 35.15% | Pass |
| Drop-off status expired | 10-20% | 14.85% | Pass |
| Policy premium increase <=5% | 65-75% | 67.84% | Pass |
| Policy premium increase 5-10% | 15-25% | 21.47% | Pass |
| Policy premium increase >10% | 8-12% | 10.69% | Pass |
| Quote premium increase <=5% | 65-75% | 68.08% | Pass |
| Quote premium increase 5-10% | 15-25% | 21.39% | Pass |
| Quote premium increase >10% | 8-12% | 10.53% | Pass |
| Digital renewal online | 65-75% | 69.98% | Pass |
| Digital renewal agent | 15-25% | 20.01% | Pass |
| Digital renewal branch | 8-12% | 10.01% | Pass |
| Claim settlement 0-15 days | 65-75% | 72.28% | Pass |
| Claim settlement 16-30 days | 15-25% | 18.81% | Pass |
| Claim settlement >30 days | 8-12% | 8.91% | Pass |
| Claim channel online | 65-75% | 72.28% | Pass |
| Claim channel agent | 15-25% | 18.81% | Pass |
| Claim channel branch | 8-12% | 8.91% | Pass |
| Claim escalation non-escalated | 88-95% | 93.56% | Pass |
| Claim escalation escalated | 5-12% | 6.44% | Pass |
| Renewal contacts 0-1 | 55-65% | 60.00% | Pass |
| Renewal contacts 2-3 | 25-35% | 30.02% | Pass |
| Renewal contacts >3 | 8-12% | 9.98% | Pass |
| Complaint resolution 0-2 days | 55-65% | 59.92% | Pass |
| Complaint resolution 3-7 days | 25-35% | 30.16% | Pass |
| Complaint resolution >7 days | 8-12% | 9.92% | Pass |
| Complaint escalation non-escalated | 97.5-98.5% | 98.02% | Pass |
| Complaint escalation escalated | 1.5-2.5% | 1.98% | Pass |
| Complaint outcome not upheld | 60-70% | 65.08% | Pass |
| Complaint outcome upheld | 15-25% | 19.84% | Pass |
| Complaint outcome partially upheld | 10-20% | 15.08% | Pass |

## Base Churn Ratios

| KPI | Band | Expected churn | Current churn | Count | Status |
|---|---|---:|---:|---:|---|
| Premium increase % | <0% | 8-12% | 9.86% | 2434 | Pass |
| Premium increase % | 0-5% | 15-20% | 15.49% | 4584 | Pass |
| Premium increase % | 5-10% | 25-35% | 24.98% | 7577 | Near miss |
| Premium increase % | >10% | 45-65% | 44.74% | 10618 | Near miss |
| Current premium | LOW | 10-18% | 19.93% | 8759 | Miss |
| Current premium | MEDIUM | 15-25% | 26.78% | 8774 | Miss |
| Current premium | HIGH | 25-40% | 40.75% | 5134 | Near miss |
| Current premium | VERY_HIGH | 40-55% | 55.18% | 2546 | Near miss |
| Claim count | 0 | 12-18% | 13.78% | 4515 | Pass |
| Claim count | 1 | 20-30% | 21.14% | 6310 | Pass |
| Claim count | 2 | 30-45% | 31.93% | 6338 | Pass |
| Claim count | 3+ | 45-60% | 44.88% | 8050 | Near miss |
| Add-ons | 0 | 25-40% | 40.57% | 8896 | Near miss |
| Add-ons | 1 | 18-28% | 28.61% | 7561 | Near miss |
| Add-ons | 2 | 12-22% | 22.42% | 5451 | Near miss |
| Add-ons | 3+ | 8-18% | 18.12% | 3305 | Near miss |
| Marketing engagement | HIGH | 8-15% | 11.77% | 3712 | Pass |
| Marketing engagement | MEDIUM | 18-30% | 19.54% | 7506 | Pass |
| Marketing engagement | LOW | 35-55% | 35.69% | 8840 | Pass |
| Marketing engagement | NONE | 50-70% | 49.16% | 5155 | Near miss |
| Driver experience | <2y | 25-40% | 33.16% | 2521 | Pass |
| Driver experience | 2-5y | 18-30% | 26.42% | 4039 | Pass |
| Driver experience | 6-10y | 15-25% | 23.56% | 5107 | Pass |
| Driver experience | >10y | 10-18% | 18.11% | 8527 | Near miss |

Additional base churn checks:

- Absolute premium increase distribution and churn bands passed.
- Tenure/policy cycle distribution and churn bands passed.
- Vehicle segment distribution passed with `STANDARD=5612`, `PREMIUM=2124`, `HIGH_RISK=846`.
- Risk direction remains aligned: higher premium increase, higher claim count, fewer add-ons, lower engagement, and lower driver experience all increase churn.

## MLOps Churn Ratios

| KPI | Band | Expected churn | Current churn | Count | Status |
|---|---|---:|---:|---:|---|
| Policy type | NEW_BUSINESS | 35-55% | 51.03% | 11346 | Pass |
| Policy type | RENEWAL | 8-18% | 13.00% | 13867 | Pass |
| Policy renewal | Y | 8-18% | 13.00% | 13867 | Pass |
| Policy renewal | N | 35-55% | 51.03% | 11346 | Pass |
| Auto-renew enabled | ON | 5-12% | 7.78% | 10085 | Pass |
| Auto-renew enabled | OFF | 35-55% | 45.00% | 15128 | Pass |
| NCD years | 0-1 | 25-40% | 35.53% | 16388 | Pass |
| NCD years | 2-4 | 18-30% | 23.00% | 5547 | Pass |
| NCD years | 5-8 | 15-25% | 17.01% | 2017 | Pass |
| NCD years | 9+ | 10-18% | 11.97% | 1261 | Pass |
| Payment method | ANNUAL | 8-15% | 9.09% | 110 | Pass |
| Payment method | MONTHLY_DD | 15-25% | 25.27% | 16043 | Near miss |
| Payment method | CARD_MANUAL | 25-40% | 38.95% | 9060 | Pass |
| Direct debit cancellation | NO | 10-18% | 18.00% | 19666 | Pass |
| Direct debit cancellation | YES | 55-75% | 73.08% | 5547 | Pass |
| Missed payments | 0 | 10-18% | 8.74% | 7794 | Miss |
| Missed payments | 1 | 25-35% | 22.89% | 5255 | Miss |
| Missed payments | 2 | 40-55% | 35.98% | 6617 | Miss |
| Missed payments | 3+ | 60-75% | 60.00% | 5547 | Pass |
| Loyalty discount | RETAINED | 8-18% | 13.00% | 8825 | Pass |
| Loyalty discount | NOT_APPLIED | 18-30% | 25.00% | 8825 | Pass |
| Loyalty discount | REMOVED | 40-60% | 56.06% | 7563 | Pass |
| Installment default | NO | 10-18% | 14.44% | 13049 | Pass |
| Installment default | YES | 50-75% | 46.93% | 12164 | Miss |
| Customer satisfaction | VERY_SATISFIED | 8-15% | 13.11% | 4883 | Pass |
| Customer satisfaction | SATISFIED | 15-25% | 21.59% | 8660 | Pass |
| Customer satisfaction | NEUTRAL | 25-40% | 33.56% | 6386 | Pass |
| Customer satisfaction | DISSATISFIED | 45-65% | 55.64% | 5284 | Pass |
| Complaint resolution days | 0-7 | 8-15% | 31.20% | 125 | Miss |
| Complaint resolution days | 8-30 | 18-30% | 49.60% | 125 | Miss |
| Complaint resolution days | 31-60 | 35-50% | 49.30% | 71 | Pass |
| Complaint resolution days | 61+ | 50-70% | 68.57% | 35 | Pass |
| Fault claim | NO | 12-20% | 15.97% | 1008 | Pass |
| Fault claim | YES | 30-50% | 45.79% | 1009 | Pass |
| Claim satisfaction | HIGH | 8-15% | 12.07% | 605 | Pass |
| Claim satisfaction | NEUTRAL | 18-30% | 24.56% | 908 | Pass |
| Claim satisfaction | LOW | 40-65% | 64.88% | 504 | Pass |
| Retention contacted | NO | 12-22% | 16.73% | 10053 | Pass |
| Retention contacted | YES | 35-55% | 47.41% | 7735 | Pass |
| Call sentiment | POSITIVE | 8-15% | 10.85% | 4772 | Pass |
| Call sentiment | NEUTRAL | 18-30% | 22.95% | 6620 | Pass |
| Call sentiment | NEGATIVE | 40-65% | 51.78% | 6396 | Pass |
| Engagement score | HIGH | 8-15% | 9.74% | 1777 | Pass |
| Engagement score | MEDIUM | 18-30% | 17.69% | 4884 | Near miss |
| Engagement score | LOW | 35-55% | 33.40% | 7661 | Miss |
| Engagement score | VERY_LOW | 50-70% | 50.58% | 3466 | Pass |

## SCD2 Status

Historical SCD2 inspection from the prior full synthetic run remains valid for the generator behavior:

- No duplicate SCD2 keys were found.
- No no-op SCD2 rows were found.
- Only allowed mutable columns changed.
- SCD2 uses mutation-style output against the previous comparable run.

Enhanced SCD2 changed rows include `sat_policy=2521`, `sat_quote=5013`, `sat_claim=201`, `sat_complaint=35`, `sat_customer=1781`, `sat_motor=858`, `sat_insured_object=1627`, and other mutable satellites.

MLOps SCD2 follows the same mutation pattern, including the MLOps-only satellites and fields.

## Notes

- The current validation focus is direct MLOps dim/fact output from `data/synthetic/mlops/20260618123141`.
- `verify_direct_dim_fact.py` passed for all 22 dimensional-model files, DDL columns, surrogate keys, SCD2 fields, and fact FK references.
- `verify_nps_dim_fact.py` passed all NPS workbook ratios at the ML notebook `master_df` grain for the direct dim/fact output.
- Strict churn ratio notes below are retained as historical full synthetic-run context. Current churn behavior remains directionally realistic and aligned for modeling, but older strict workbook-ratio audits may still show close boundary misses on sparse complaint or churn slices.
