# new_brz_vault Base SQL Review

Reviewed workbook: `new_brz_vault/new_brz_vault.xlsx` sheet `result`.

Compared against latest base vault schema: `data/synthetic/base/20260607212843`.

## Summary

- Rows reviewed: 52
- Tables without issues: 0
- Tables with issues: 52

## Major Findings

- Hash keys generally use `md5(concat(<ref>, source_name))`; our base vault uses `md5(<business_ref>)`. This will not match synthetic and will break comparison to our vault keys.
- Many outputs add `source_tbl_name`; base hubs/links/satellites do not have that column.
- Satellite SQL adds `record_source`; our base satellite schema does not include `record_source`.
- `sat_policy` still has `policy_cicle`; expected `policy_cycle`.
- `link_product_home` appears mapped to person/lead columns, not product/home columns.

## Table By Table

### hub_account (hub) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- expected hub_account hash = md5(account_ref)
- base hub schema does not include source_tbl_name

### hub_consent (hub) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- expected hub_consent hash = md5(consent_ref)
- base hub schema does not include source_tbl_name

### hub_contact (hub) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- expected hub_contact hash = md5(contact_ref)
- base hub schema does not include source_tbl_name

### hub_customer (hub) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- expected hub_customer hash = md5(customer_ref)
- base hub schema does not include source_tbl_name

### hub_home (hub) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- expected hub_home hash = md5(property_ref)
- base hub schema does not include source_tbl_name

### hub_home_address (hub) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- expected hub_home_address hash = md5(address_ref)
- base hub schema does not include source_tbl_name

### hub_identities (hub) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- expected hub_identities hash = md5(identity_ref)
- base hub schema does not include source_tbl_name

### hub_lead (hub) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- expected hub_lead hash = md5(lead_ref)
- base hub schema does not include source_tbl_name

### hub_legal_person (hub) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- expected hub_legal_person hash = md5(legal_ref)
- base hub schema does not include source_tbl_name

### hub_marketing_engagement (hub) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- expected hub_marketing_engagement hash = md5(engagement_ref)
- base hub schema does not include source_tbl_name

### hub_marketing_preference (hub) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- expected hub_marketing_preference hash = md5(preference_ref)
- base hub schema does not include source_tbl_name

### hub_motor (hub) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- expected hub_motor hash = md5(vehicle_ref)
- base hub schema does not include source_tbl_name

### hub_natural_person (hub) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- expected hub_natural_person hash = md5(natural_ref)
- base hub schema does not include source_tbl_name

### hub_person (hub) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- expected hub_person hash = md5(party_ref)
- base hub schema does not include source_tbl_name

### hub_policy (hub) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- expected hub_policy hash = md5(policy_ref)
- base hub schema does not include source_tbl_name

### hub_product (hub) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- expected hub_product hash = md5(product_ref)
- base hub schema does not include source_tbl_name

### hub_quote (hub) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- expected hub_quote hash = md5(quote_ref)
- base hub schema does not include source_tbl_name

### link_customer_lead (link) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- base link schema does not include source_tbl_name

### link_customer_person (link) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base link schema does not include source_tbl_name

### link_person_account (link) - ISSUES
- extra output aliases not in base vault: source_name_hash_key, source_tbl_name
- base link schema does not include source_tbl_name

### link_person_consent (link) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- base link schema does not include source_tbl_name

### link_person_contact (link) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- base link schema does not include source_tbl_name

### link_person_home_address (link) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- base link schema does not include source_tbl_name

### link_person_identities (link) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- base link schema does not include source_tbl_name

### link_person_lead (link) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- base link schema does not include source_tbl_name

### link_person_legal_person (link) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- base link schema does not include source_tbl_name

### link_person_marketing_engagement (link) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- base link schema does not include source_tbl_name

### link_person_marketing_preference (link) - ISSUES
- extra output aliases not in base vault: source_tbl_name, source_name_hash_key
- base link schema does not include source_tbl_name

### link_person_natural_person (link) - ISSUES
- missing expected aliases: person_natural_person_hash_key
- extra output aliases not in base vault: person_natural_person_origin_hash_key, source_tbl_name
- uses person_natural_person_origin_hash_key; expected person_natural_person_hash_key
- base link schema does not include source_tbl_name

### link_policy_customer (link) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base link schema does not include source_tbl_name

### link_policy_product (link) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base link schema does not include source_tbl_name

### link_product_home (link) - ISSUES
- missing expected aliases: product_home_hash_key, product_hash_key, home_hash_key
- extra output aliases not in base vault: person_hash_key, lead_hash_key, source_tbl_name, person_lead_hash_key
- appears to build person_lead link columns instead of product_home columns
- base link schema does not include source_tbl_name

### link_product_motor (link) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- base link schema does not include source_tbl_name

### link_quote_person (link) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base link schema does not include source_tbl_name

### link_quote_product (link) - ISSUES
- extra output aliases not in base vault: source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base link schema does not include source_tbl_name

### sat_account (sat) - ISSUES
- extra output aliases not in base vault: record_source, source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base satellite schema does not include record_source/source_tbl_name

### sat_consent (sat) - ISSUES
- extra output aliases not in base vault: record_source, source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base satellite schema does not include record_source/source_tbl_name

### sat_contact (sat) - ISSUES
- extra output aliases not in base vault: record_source, source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base satellite schema does not include record_source/source_tbl_name

### sat_customer (sat) - ISSUES
- extra output aliases not in base vault: record_source, source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base satellite schema does not include record_source/source_tbl_name

### sat_home (sat) - ISSUES
- extra output aliases not in base vault: record_source, source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base satellite schema does not include record_source/source_tbl_name

### sat_home_address (sat) - ISSUES
- extra output aliases not in base vault: record_source, source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base satellite schema does not include record_source/source_tbl_name

### sat_identities (sat) - ISSUES
- extra output aliases not in base vault: record_source, source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base satellite schema does not include record_source/source_tbl_name

### sat_lead (sat) - ISSUES
- extra output aliases not in base vault: record_source, source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base satellite schema does not include record_source/source_tbl_name

### sat_legal_person (sat) - ISSUES
- extra output aliases not in base vault: record_source, source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base satellite schema does not include record_source/source_tbl_name

### sat_marketing_engagement (sat) - ISSUES
- extra output aliases not in base vault: record_source, source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base satellite schema does not include record_source/source_tbl_name

### sat_marketing_preference (sat) - ISSUES
- extra output aliases not in base vault: record_source, source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base satellite schema does not include record_source/source_tbl_name

### sat_motor (sat) - ISSUES
- extra output aliases not in base vault: record_source, source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base satellite schema does not include record_source/source_tbl_name

### sat_natural_person (sat) - ISSUES
- extra output aliases not in base vault: record_source, source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base satellite schema does not include record_source/source_tbl_name

### sat_person (sat) - ISSUES
- extra output aliases not in base vault: record_source, source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base satellite schema does not include record_source/source_tbl_name

### sat_policy (sat) - ISSUES
- missing expected aliases: policy_cycle
- extra output aliases not in base vault: policy_cicle, record_source, source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- typo policy_cicle; expected policy_cycle
- base satellite schema does not include record_source/source_tbl_name

### sat_product (sat) - ISSUES
- extra output aliases not in base vault: record_source, source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base satellite schema does not include record_source/source_tbl_name

### sat_quote (sat) - ISSUES
- extra output aliases not in base vault: record_source, source_tbl_name
- hash includes source_name; expected hash only from business key/ref
- base satellite schema does not include record_source/source_tbl_name

