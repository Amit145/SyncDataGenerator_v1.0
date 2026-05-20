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

### claim_count_weights

Controls total recent claim-count distribution before it is split into active, previous, and declined claims.

- Keys `0` through `5` represent total generated claim count.

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

### vehicle_segment_weights

Controls generated vehicle model risk segment.

- `STANDARD`: Standard vehicle models.
- `PREMIUM`: Premium vehicle models.
- `HIGH_RISK`: High-risk vehicle models.

### marketing_engagement_band_weights

Controls marketing opt-in engagement proxy.

- `HIGH`: Many marketing channels opted in.
- `MEDIUM`: Some marketing channels opted in.
- `LOW`: One marketing channel opted in.
- `NONE`: No marketing channels opted in.

### service_call_band_weights

Controls service-call proxy behavior in marketing preference.

- `NONE`: No service-call signal.
- `LOW`: Low service-call signal.
- `MEDIUM`: Medium service-call signal.
- `HIGH`: High service-call signal.

Current output maps this to the existing `Call` Y/N field.

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

- `LT_1`: Churn probability for policies with less than one completed annual cycle.
- `Y1_2`: Churn probability for one to two completed annual cycles.
- `Y3_5`: Churn probability for three to five completed annual cycles.
- `GT_5`: Churn probability for more than five completed annual cycles.

Keep these values decreasing to preserve the MLOps rule that higher completed tenure has lower churn.

### sales_channel_by_policy_status

Controls `Sat_Policy.Sales Channel` distribution after policy status is known.

- `ACTIVE`: Channel mix for active policies.
- `LAPSED`: Channel mix for lapsed policies.
- `CANCELLED`: Channel mix for cancelled policies.

Allowed emitted channel values remain `ONLINE`, `BRANCH`, and `AGENT`. `AGENT` carries the broker/aggregator-like higher churn behavior. `AGGREGATOR` is not emitted.

### suspended_policy_status_weights

Controls policy status split when the linked account is suspended and the policy has completed at least one cycle.

- `LAPSED`: Policy lapses.
- `CANCELLED`: Policy cancels.

### churned_policy_status_weights

Controls status split for churned policies not forced by account closure.

- `LAPSED`: Policy lapses.
- `CANCELLED`: Policy cancels.

For policies with less than one completed cycle, churn is represented as `CANCELLED` because `LAPSED` requires a completed renewal cycle.

## lifecycle_mode

- `initial_then_convert`: Current lifecycle mode. The generator creates an initial lifecycle population and then converts eligible leads through quote, policy, customer, and account stages.

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
