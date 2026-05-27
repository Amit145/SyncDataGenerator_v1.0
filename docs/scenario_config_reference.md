# Scenario Config Reference

This document explains `config/scenario_v1.json`.

All weight maps are relative weights. They do not need to add up to `1` or `100`. For example, weights `{A: 80, B: 20}` and `{A: 8, B: 2}` mean the same distribution.

Probability values are decimal probabilities from `0.0` to `1.0`.

## run_settings

- `total_people`: Number of person records to generate for a normal run. This drives base population size before lifecycle conversion creates leads, quotes, policies, customers, and accounts.
- `country`: Country profile used by generation. Current active profile is `UK`.
- `random_seed`: Seed used to make runs reproducible for the same config and code path.
- `natural_person_pct`: Share of generated people that should be natural persons. The remainder are legal persons.

## lifecycle_distribution

Controls initial person lifecycle mix before later conversion logic.

- `lead`: Relative share of people initially treated as leads.
- `prospect`: Relative share of people initially treated as prospects.
- `customer`: Relative share of people initially treated as customers.

## sales_channel_distribution

Controls lead acquisition channel mix used by the base hub/lifecycle setup.

- `online`: Relative lead share from online acquisition.
- `branch`: Relative lead share from branch acquisition.
- `call_center`: Relative lead share from call-center acquisition.

This is separate from `churn_settings.sales_channel_by_policy_status`, which controls `Sat_Policy.Sales Channel` churn behavior.

## conversion_rates

Controls lead-to-quote/customer conversion chance by acquisition channel.

- `online`: Conversion probability for online leads.
- `branch`: Conversion probability for branch leads.
- `call_center`: Conversion probability for call-center leads.

## churn_settings

Controls churn-related distributions used by policy, account, marketing, and motor satellite generation.

### renewal_current_premium_band_weights

Controls the starting/current premium amount band.

- `LOW`: Current premium sampled from the low premium range.
- `MEDIUM`: Current premium sampled from the medium premium range.
- `HIGH`: Current premium sampled from the high premium range.
- `VERY_HIGH`: Current premium sampled from the very-high premium range.

### renewal_movement_band_weights

Controls how the next renewal premium changes from current premium.

- `DECREASE`: Renewal premium decreases.
- `0_5`: Renewal premium increases from `0%` to `5%`.
- `5_10`: Renewal premium increases from `5%` to `10%`.
- `GT_10`: Renewal premium increases by more than `10%`.

### premium_abs_increase_churn_probability

Controls policy churn probability pressure by absolute renewal premium increase.

- `LE_0`: Churn pressure when renewal premium does not increase. Current value: `0.20`.
- `1_50`: Churn pressure when renewal premium increases by `1-50`. Current value: `0.50`.
- `51_100`: Churn pressure when renewal premium increases by `51-100`. Current value: `0.90`.
- `GT_100`: Churn pressure when renewal premium increases by more than `100`. Current value: `2.20`.

This implements the workbook rule that higher absolute premium increase increases churn probability.

### premium_abs_increase_churn_expected_ranges

Expected generated churn-rate ranges by absolute premium increase band:

- `LE_0`: `8-12%`
- `1_50`: `15-22%`
- `51_100`: `25-38%`
- `GT_100`: `45-65%`

### premium_abs_increase_churn_normalization_scale

Scales the absolute premium increase churn modifier so it works alongside tenure, claim-count, add-on, and vehicle-segment churn modifiers. Current value: `1.75`.

### current_premium_churn_probability

Controls policy churn probability pressure by current annual premium amount, using `renewal_amount_current_period`.

- `LOW`: Churn pressure for current premium up to `600`. Current value: `0.14`.
- `MEDIUM`: Churn pressure for current premium from `601` to `900`. Current value: `0.20`.
- `HIGH`: Churn pressure for current premium from `901` to `1200`. Current value: `0.32`.
- `VERY_HIGH`: Churn pressure for current premium above `1200`. Current value: `0.47`.

This implements the workbook rule that higher current premium increases price sensitivity and churn probability.

### current_premium_churn_expected_ranges

Expected generated churn-rate ranges by current premium amount band:

- `LOW`: `10-18%`
- `MEDIUM`: `15-25%`
- `HIGH`: `25-40%`
- `VERY_HIGH`: `40-55%`

### current_premium_churn_normalization_scale

Scales the current-premium churn modifier so it works alongside premium movement, tenure, claim-count, add-on, and vehicle-segment churn modifiers. Current value: `1.0`.

### premium_pct_increase_churn_probability

Controls policy churn probability pressure by percentage renewal premium movement.

- `LT_0`: Churn pressure when renewal premium decreases. Current value: `0.10`.
- `0_5`: Churn pressure when renewal premium increases from `0%` to `5%`. Current value: `0.17`.
- `5_10`: Churn pressure when renewal premium increases from `5%` to `10%`. Current value: `0.30`.
- `GT_10`: Churn pressure when renewal premium increases by more than `10%`. Current value: `0.52`.

This implements the workbook rule that higher percentage premium increase increases churn probability.

### premium_pct_increase_churn_expected_ranges

Expected generated churn-rate ranges by percentage premium movement:

- `LT_0`: `8-12%`
- `0_5`: `15-20%`
- `5_10`: `25-35%`
- `GT_10`: `45-65%`

### premium_pct_increase_churn_normalization_scale

Scales the percentage premium increase churn modifier so it works alongside absolute premium increase, tenure, claim-count, add-on, and vehicle-segment churn modifiers. Current value: `1.0`.

### claim_count_weights

Controls total recent claim-count distribution before it is split into active, previous, and declined claims.

- Keys `0` through `5` represent total generated claim count.

### claim_count_churn_probability

Controls policy churn probability pressure by total recent claim count.

- `0`: Target churn pressure for policies with no recent claims. Current value: `0.45`.
- `1`: Target churn pressure for policies with one recent claim. Current value: `1.10`.
- `2`: Target churn pressure for policies with two recent claims. Current value: `1.80`.
- `3_PLUS`: Target churn pressure for policies with three or more recent claims. Current value: `3.50`.

This implements the workbook rule that more claims increase churn probability.

### claim_count_churn_expected_ranges

Expected generated churn-rate ranges by recent claim-count band:

- `0`: `12-18%`
- `1`: `20-30%`
- `2`: `30-45%`
- `3_PLUS`: `45-60%`

### claim_count_churn_normalization_scale

Scales the claim-count churn modifier so it works alongside tenure and add-on churn modifiers without forcing every policy into the claim-count targets alone. Current value: `2.00`.

### active_claim_count_weights

Controls how many of the total claims are active claims.

- Keys `0`, `1`, and `2` represent active claim count before capping to total claims.

### declined_claim_count_weights

Controls how many remaining claims are declined claims.

- Keys `0`, `1`, and `2` represent declined claim count before capping to remaining claims.

### cover_option_weights

Controls policy add-on category distribution.

- `BASE_ONLY`: No add-ons.
- `ONE_ADD_ON`: One add-on.
- `TWO_ADD_ONS`: Two add-ons.
- `THREE_PLUS_ADD_ONS`: Three or more add-ons.

### addon_churn_probability

Controls policy churn probability pressure by add-on count.

- `BASE_ONLY`: Target churn pressure for no add-ons. Current value: `0.12`.
- `ONE_ADD_ON`: Target churn pressure for one add-on. Current value: `0.07`.
- `TWO_ADD_ONS`: Target churn pressure for two add-ons. Current value: `0.045`.
- `THREE_PLUS_ADD_ONS`: Target churn pressure for three or more add-ons. Current value: `0.025`.

This implements the workbook rule that more add-ons reduce churn probability.

### addon_churn_expected_ranges

Expected generated churn-rate ranges by add-on band:

- `BASE_ONLY`: `25-40%`
- `ONE_ADD_ON`: `18-28%`
- `TWO_ADD_ONS`: `12-22%`
- `THREE_PLUS_ADD_ONS`: `8-18%`

### addon_churn_normalization_scale

Scales the add-on churn modifier so it works alongside tenure and claim-count churn modifiers without forcing every policy into the add-on targets alone. Current value: `0.44`.

### policy churn calibration

After initial policy status sampling, `sat_policy` applies a narrow calibration pass across the generated rows. It keeps the same output columns and date rules, but can flip selected rows between `ACTIVE` and churned statuses so the marginal workbook bands for premium increase, claim count, add-ons, marketing engagement, vehicle segment, driver experience, current premium, and tenure stay close to their configured expected ranges.

The calibration now works toward configured target counts inside each workbook range, not only the range edge. It uses a weighted penalty check before flipping a row, so a policy is changed only when the total fit improves across the overlapping churn dimensions it belongs to. It keeps policies on `CLOSED` or `SUSPENDED` accounts locked as churned so account lifecycle rules are not broken.

### vehicle_segment_weights

Controls generated vehicle model risk segment.

- `STANDARD`: Standard vehicle models.
- `PREMIUM`: Premium vehicle models.
- `HIGH_RISK`: High-risk vehicle models.

### vehicle_segment_churn_probability

Controls policy churn probability pressure for motor policies by vehicle segment.

- `STANDARD`: Churn pressure for standard vehicles. Current value: `0.17`.
- `PREMIUM`: Churn pressure for premium vehicles. Current value: `0.27`.
- `HIGH_RISK`: Churn pressure for high-risk vehicles. Current value: `0.40`.

This implements the workbook rule that vehicle type/segment influences churn behavior.

### vehicle_segment_churn_expected_ranges

Expected generated churn-rate ranges by vehicle segment:

- `STANDARD`: `12-22%`
- `PREMIUM`: `20-35%`
- `HIGH_RISK`: `30-50%`

### vehicle_segment_churn_normalization_scale

Scales the vehicle-segment churn modifier so it works alongside tenure, premium-increase, claim-count, and add-on churn modifiers.

### marketing_engagement_band_weights

Controls marketing opt-in engagement proxy.

- `HIGH`: Many marketing channels opted in.
- `MEDIUM`: Some marketing channels opted in.
- `LOW`: One marketing channel opted in.
- `NONE`: No marketing channels opted in.

### marketing_engagement_churn_probability

Controls policy churn probability pressure by marketing engagement proxy, using existing marketing flags such as email subscriptions, commercial email, personal email, and SMS.

- `HIGH`: Churn pressure for high engagement. Current value: `0.12`.
- `MEDIUM`: Churn pressure for medium engagement. Current value: `0.24`.
- `LOW`: Churn pressure for low engagement. Current value: `0.45`.
- `NONE`: Churn pressure for no engagement. Current value: `0.60`.

This implements the workbook proxy rule that lower engagement indicates higher churn risk.

### marketing_engagement_churn_expected_ranges

Expected generated churn-rate ranges by marketing engagement proxy:

- `HIGH`: `8-15%`
- `MEDIUM`: `18-30%`
- `LOW`: `35-55%`
- `NONE`: `50-70%`

### marketing_engagement_churn_normalization_scale

Scales the marketing engagement churn modifier so it works alongside premium, tenure, claim-count, add-on, and vehicle-segment churn modifiers. Current value: `1.0`.

### service_call_band_weights

Controls service-call proxy behavior in marketing preference.

- `NONE`: No service-call signal.
- `LOW`: Low service-call signal.
- `MEDIUM`: Medium service-call signal.
- `HIGH`: High service-call signal.

Current output maps this to the existing `Call` Y/N field.

### driver_experience_band_weights

Controls the generated driver-experience proxy mix. The generator does not add a new licence-date output column; it derives experience from the existing natural-person `birth_date` using `max(age - 17, 0)` as the workbook proxy when a driving licence issue date is unavailable.

- `LT_2Y`: Less than 2 years of proxy driving experience.
- `Y2_5`: 2 to 5 years of proxy driving experience.
- `Y6_10`: 6 to 10 years of proxy driving experience.
- `GT_10`: More than 10 years of proxy driving experience.

### driver_experience_churn_probability

Controls policy churn pressure by driver-experience proxy.

- `LT_2Y`: Churn pressure for less than 2 years. Current value: `0.32`.
- `Y2_5`: Churn pressure for 2 to 5 years. Current value: `0.24`.
- `Y6_10`: Churn pressure for 6 to 10 years. Current value: `0.20`.
- `GT_10`: Churn pressure for more than 10 years. Current value: `0.14`.

This implements the workbook rule that lower driver experience carries higher churn risk.

### driver_experience_churn_expected_ranges

Expected generated churn-rate ranges by driver-experience proxy:

- `LT_2Y`: `25-40%`
- `Y2_5`: `18-30%`
- `Y6_10`: `15-25%`
- `GT_10`: `10-18%`

### driver_experience_churn_normalization_scale

Scales the driver-experience churn modifier so it works alongside premium, tenure, claim-count, add-on, vehicle-segment, and marketing churn modifiers. Current value: `1.0`.

### customer_status_weights

Controls generated customer status before downstream customer rating/segment logic.

- `ACTIVE`: Active customer.
- `LAPSED`: Lapsed customer.

### account_status_weights

Controls generated account status.

- `OPEN`: Account can support active policies.
- `SUSPENDED`: Account should not have active policies.
- `CLOSED`: Account forces policy churn behavior.

### tenure_churn_probability

Controls policy churn probability by completed `Policy Cycle` tenure.

- `LT_1`: Churn probability for policies with less than one completed annual cycle. Current value: `0.42`.
- `Y1_2`: Churn probability for one to two completed annual cycles. Current value: `0.28`.
- `Y3_5`: Churn probability for three to five completed annual cycles. Current value: `0.14`.
- `GT_5`: Churn probability for more than five completed annual cycles. Current value: `0.08`.

Keep these values decreasing to preserve the MLOps rule that higher completed tenure has lower churn.

The current values are tuned to land generated churn rates inside the workbook's customer-tenure expected ranges:

- `<1 year`: `35-50%`
- `1-2 years`: `25-35%`
- `3-5 years`: `15-25%`
- `>5 years`: `8-15%`

### tenure_churn_expected_ranges

Validator target ranges for generated churn rate by completed policy tenure:

- `LT_1`: `35-50%`
- `Y1_2`: `25-35%`
- `Y3_5`: `15-25%`
- `GT_5`: `8-15%`

### sales_channel_by_policy_status

Controls `Sat_Policy.Sales Channel` distribution after policy status is known.

- `ACTIVE`: Channel mix for active policies.
- `LAPSED`: Channel mix for lapsed policies.
- `CANCELLED`: Channel mix for cancelled policies.

Allowed emitted channel values remain `ONLINE`, `BRANCH`, and `AGENT`. `AGENT` carries the broker/aggregator-like higher churn behavior. `AGGREGATOR` is not emitted.

The workbook does not define a sales-channel benchmark range. The current config keeps channel variance for MLOps while avoiding the earlier extreme channel spike:

- active policies: `ONLINE 50`, `BRANCH 32`, `AGENT 18`
- lapsed policies: `ONLINE 35`, `BRANCH 30`, `AGENT 35`
- cancelled policies: `ONLINE 32`, `BRANCH 28`, `AGENT 40`

### suspended_policy_status_weights

Controls policy status split when the linked account is suspended and the policy has completed at least one cycle.

- `LAPSED`: Policy lapses.
- `CANCELLED`: Policy cancels.

### churned_policy_status_weights

Controls status split for churned policies not forced by account closure.

- `LAPSED`: Policy lapses.
- `CANCELLED`: Policy cancels.

For policies with less than one completed cycle, churn is represented as `CANCELLED` because `LAPSED` requires a completed renewal cycle.

### mlops_churn_expected_ranges

Validator target ranges for the MLOps-only churn KPIs that became coverable with the new MLOps DDL.

These ranges are used by `misc/verify_mlops_churn_kpis.py`; they do not change base or enhanced schemas.

- `auto_renew_enabled`: ON `5-12%`, OFF `35-55%`
- `policy_type`: NEW_BUSINESS `35-55%`, RENEWAL `8-18%`
- `policy_renewal`: Y `8-18%`, N `35-55%`
- `fault_claim`: NO `12-20%`, YES `30-50%`
- `ncd_years`: `0_1` `25-40%`, `2_4` `18-30%`, `5_8` `15-25%`, `9_PLUS` `10-18%`
- `payment_method`: ANNUAL `8-15%`, MONTHLY_DD `15-25%`, CARD_MANUAL `25-40%`
- `direct_debit_cancellation`: NO `10-18%`, YES `55-75%`
- `missed_payments`: `0` `10-18%`, `1` `25-35%`, `2` `40-55%`, `3_PLUS` `60-75%`
- `retention_contacted`: NO `12-22%`, YES `35-55%`
- `claim_satisfaction`: HIGH `8-15%`, NEUTRAL `18-30%`, LOW `40-65%`
- `customer_satisfaction`: VERY_SATISFIED `8-15%`, SATISFIED `15-25%`, NEUTRAL `25-40%`, DISSATISFIED `45-65%`
- `complaint_resolution_days`: `0_7` `8-15%`, `8_30` `18-30%`, `31_60` `35-50%`, `61_PLUS` `50-70%`
- `loyalty_discount`: RETAINED `8-18%`, NOT_APPLIED `18-30%`, REMOVED `40-60%`
- `installment_default`: NO `10-18%`, YES `50-75%`
- `call_sentiment`: POSITIVE `8-15%`, NEUTRAL `18-30%`, NEGATIVE `40-65%`
- `engagement_score`: HIGH `8-15%`, MEDIUM `18-30%`, LOW `35-55%`, VERY_LOW `50-70%`

`policy_renewal` is intentionally separate from `auto_renew_enabled`: `policy_renewal` describes whether the policy is a renewal/new-business policy, while `auto_renew_enabled` describes whether the policy is enrolled for automatic renewal. Engagement score keeps the workbook direction: higher engagement has lower expected churn, and lower engagement has higher expected churn.

### mlops_churn_band_weights

Controls generated band sizes for configurable MLOps KPI calibration where the workbook gives churn ranges but not row-count distribution.

- `policy_type`: NEW_BUSINESS `45`, RENEWAL `55`
- `policy_renewal`: Y `55`, N `45`
- `customer_satisfaction`: VERY_SATISFIED `20`, SATISFIED `35`, NEUTRAL `25`, DISSATISFIED `20`
- `complaint_resolution_days`: `0_7` `35`, `8_30` `35`, `31_60` `20`, `61_PLUS` `10`

## lifecycle_mode

- `initial_then_convert`: Current lifecycle mode. The generator creates an initial lifecycle population and then converts eligible leads through quote, policy, customer, and account stages.

## claim_financial_settings

Controls enhanced `sat_claim` financial values.

The enhanced generator first uses populated claim rows from `enhanced_360/data_example/FactPolicy.csv` when configured to do so. Those rows contain `ClaimID`, `ClaimAmount`, `ClaimsPaid`, `OutstandingReserve`, `Claims Expenses`, `ClaimBand`, and `ClaimBand Sort`.

If no usable claim fact row is available, the generator derives values from the linked enhanced policy's `policy_sum_insured`.

- `enabled`: Whether enhanced claim financial population is active.
- `source_priority`: Ordered source preference. Current default is `fact_policy_claim_rows` first, then `derived_from_policy_sum_insured`.
- `band_rules`: Claim amount band definitions. Each rule has `band`, `sort`, `min`, and `max`.
- `severity_weights`: Relative weights used when deriving claim amount bands.
- `paid_ratio_by_status`: Paid amount ratio by claim status. Open claims normally keep reserve; closed/settled claims are mostly paid; repudiated claims have low paid ratios.
- `expense_ratio`: Claims expense as a ratio of claim amount when derived.
- `recovery_received_ratio`: Recovery received ratio when recovery happened and source value is missing.
- `compensation_ratio`: Compensation offered ratio when source value is missing.
- `remediation_ratio`: Remediation amount ratio when source value is missing.
- `fraud_amount_ratio`: Suspected/fraud amount ratio when fraud is present and source value is missing.
- `legal_expense_ratio`: Legal expense ratio when litigation is present and source value is missing.
- `cap_claim_amount_at_policy_sum_insured`: Caps derived/source claim amount at the linked policy's `policy_sum_insured`.

Current band rules:

- `0-6k`: sort `1`, amount from `0` to below `6000`.
- `6k-8k`: sort `2`, amount from `6000` to below `8000`.
- `8k-10k`: sort `3`, amount from `8000` to below `10000`.
- `10k-13k`: sort `4`, amount from `10000` to below `13000`.
- `13+`: sort `5`, amount `13000` or above.

Enhanced claim financial consistency rules:

- `claim_band` is derived from `claim_amount`.
- `claim_band_sort` is the reporting sort order for `claim_band`.
- `claims_paid` must not exceed `claim_amount`.
- `outstanding_reserve` represents the unpaid expected amount for open claims.
- valid zeros remain possible for recovery, fraud, and legal fields when those situations do not apply.

## enhanced_settings

Controls enhanced 360 generation.

- `enabled`: Whether enhanced generation is enabled in config. The current `main.py` path still builds enhanced output unless using mode flags that skip it.
- `broker_count`: Number of broker reference records to generate.
- `campaign_count`: Number of campaign reference records to generate.
- `channel_count`: Number of channel reference records to generate.
- `claim_policy_rate`: Share of eligible policies that should get enhanced claim records.
- `complaint_customer_rate`: Share of eligible customers/policies that should get complaint records.
- `override_policy_rate`: Share of eligible policies that should get override records.
- `regulation_count`: Number of regulation reference/operational records to generate.
