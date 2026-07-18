# Business Dimensional Review

## Files Checked

- `Enhanced_Customer360_Dimensional_Model_DDL (1).sql`: full dimensional DDL with 22 target tables.
- `Enhanced_Customer360_S2T_Mapping_DV_to_Dimensional_Model (1).xlsx`: legacy raw-vault-to-dimensional S2T. Covers all 22 DDL tables.
- `RawVault+BusinessVault_To_Dimensional_S2T_Mapping.xlsx`: desired BV-aware S2T. Covers 14 target tables.
- `approach_base_buisness_vault.sql`: older BV SQL approach, useful as logic reference only.

## Main Conclusion

The desired dimensional build should use `RawVault+BusinessVault_To_Dimensional_S2T_Mapping.xlsx` for BV-backed dimensions/facts, but it is not a full replacement for the legacy S2T. It only covers 14 of the 22 DDL tables.

For tables not covered by the BV-aware S2T, the legacy S2T is still needed if those dimensions/facts are required.

## Target Coverage

| Target table | DDL | Legacy S2T | BV-aware S2T | Recommendation |
|---|---:|---:|---:|---|
| dim_account | Yes | Yes | Yes | Use BV-aware S2T |
| dim_broker | Yes | Yes | No | Use legacy S2T |
| dim_campaign | Yes | Yes | No | Use legacy S2T |
| dim_channel | Yes | Yes | No | Use legacy S2T |
| dim_claim | Yes | Yes | No | Use legacy S2T |
| dim_customer | Yes | Yes | Yes | Use BV-aware S2T, but column coverage is reduced |
| dim_date | Yes | Yes | Yes | Use BV-aware S2T, add `month_short_name` if required |
| dim_geography | Yes | Yes | Yes | Use BV-aware S2T |
| dim_home | Yes | Yes | Yes | Fix DDL/S2T key mismatch first |
| dim_identity | Yes | Yes | Yes | Use BV-aware S2T |
| dim_insured_object | Yes | Yes | No | Use legacy S2T or retire if home/motor dims replace it |
| dim_marketing | Yes | Yes | Yes | Use BV-aware S2T, but column coverage is reduced |
| dim_motor | Yes | Yes | Yes | Fix DDL/S2T key mismatch first |
| dim_override | Yes | Yes | No | Use legacy S2T |
| dim_person | Yes | Yes | Yes | Use BV-aware S2T, fix naming mismatch |
| dim_policy | Yes | Yes | Yes | Use BV-aware S2T, but many legacy columns are not mapped |
| dim_product | Yes | Yes | Yes | Use BV-aware S2T, but column coverage is reduced |
| dim_regulation | Yes | Yes | No | Use legacy S2T |
| fact_complaint | Yes | Yes | No | Use legacy S2T |
| fact_lead | Yes | Yes | Yes | Use BV-aware S2T |
| fact_policy | Yes | Yes | Yes | Fix DDL/S2T FK mismatch first |
| fact_quote | Yes | Yes | Yes | Fix DDL/S2T FK mismatch first |

## DDL Versus BV-aware S2T Issues

- `dim_home`: BV-aware S2T expects `home_sk`, `product_sk`, `home_id`; DDL has `insured_object_sk`, `insured_object_home_id`.
- `dim_motor`: BV-aware S2T expects `motor_sk`, `product_sk`, `motor_id`; DDL has `insured_object_sk`, `insured_object_motor_id`.
- `fact_policy`: BV-aware S2T expects `home_sk` and `motor_sk`; DDL expects a generic `insured_object_sk`.
- `fact_quote`: BV-aware S2T expects `home_sk` and `motor_sk`; DDL expects a generic `insured_object_sk`.
- `dim_person`: BV-aware S2T uses `home_address_id` and correctly spelled `assessed_disability_degree`; DDL has `address_id`, `address_type`, and typo `assesed_disability_degree`.
- `dim_policy`: BV-aware S2T adds `product_id`, `product_type`, `active_claims_number`, `previous_claims_number`, `declined_claims_number`; DDL does not include these exact columns and still expects `product_sk`, `channel_sk`, `insured_object_sk`, and many renewal/risk fields.
- `dim_product`: BV-aware S2T maps only product id/type and audit columns. DDL also expects product variant/name/status/LOB/underwriting/regulatory columns.
- `dim_customer`, `dim_marketing`, `dim_date`: BV-aware S2T has fewer columns than the DDL.

## Source Alignment With Current PostgreSQL Schemas

The current PostgreSQL `business_vault` schema has the BV master tables required by the desired S2T:

- `bv_natural_person_master`
- `bv_legal_person_master`
- `bv_address_master`
- `bv_product_master`
- `bv_motor_master`
- `bv_home_master`

The desired S2T uses generic raw-vault satellite names such as `SAT_POLICY`, `SAT_CUSTOMER`, `SAT_MOTOR`, and `SAT_PRODUCT`. In PostgreSQL, the actual tables are source-specific:

- `sat_policy_crm`
- `sat_customer_crm`
- `sat_motor_crm`, `sat_motor_sap`
- `sat_product_crm`, `sat_product_sap`
- and similar `_crm` / `_sap` satellite names

So the dimensional SQL generator/load must translate generic S2T source names to the actual PostgreSQL raw-vault tables.

## Status Of Older BV SQL Approach

`approach_base_buisness_vault.sql` should not be run directly against the current PostgreSQL load because it:

- hard-codes `allianz_coe.bvault_silver_raw`;
- uses old/base raw-vault object names such as `sat_person`, `sat_natural_person`, `sat_motor`, `sat_home`;
- assumes SAP variants like `sat_person_sap`, `sat_motor_sap`;
- does not match the current enhanced PostgreSQL schema names exactly;
- is already superseded by the current `pgsql/load_business_vault.py` enhanced-load approach.

It is still useful as documentation for the intended BV match rules.

## Practical Next Step

Before loading dimensional tables, choose one of these two paths:

1. Update the DDL to match the BV-aware S2T for home/motor/facts by using `home_sk`, `motor_sk`, `home_id`, and `motor_id`.
2. Or update the BV-aware S2T/load SQL to fit the existing DDL's generic `insured_object_sk` design.

Given the desired model separates `dim_home` and `dim_motor`, option 1 is more aligned with the BV-aware S2T.
