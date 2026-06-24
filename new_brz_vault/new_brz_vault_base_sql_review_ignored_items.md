# new_brz_vault Base SQL Review With Requested Ignores

Ignored checks: MD5 formula differences, additional output columns, `policy_cicle`, `record_source`, and `source_tbl_name`.

- Rows reviewed: 52
- Tables OK after ignores: 50
- Tables still with issues: 2

## Remaining Issues

### link_person_natural_person (link)
- missing expected aliases: person_natural_person_hash_key
- wrong output alias: person_natural_person_origin_hash_key should be person_natural_person_hash_key

### link_product_home (link)
- missing expected aliases: product_home_hash_key, product_hash_key, home_hash_key
- wrong mapping: SQL outputs person_hash_key/lead_hash_key/person_lead_hash_key; expected product_hash_key/home_hash_key/product_home_hash_key from property_asset product_ref/property_ref

## Tables OK After Ignores

- hub_account (hub)
- hub_consent (hub)
- hub_contact (hub)
- hub_customer (hub)
- hub_home (hub)
- hub_home_address (hub)
- hub_identities (hub)
- hub_lead (hub)
- hub_legal_person (hub)
- hub_marketing_engagement (hub)
- hub_marketing_preference (hub)
- hub_motor (hub)
- hub_natural_person (hub)
- hub_person (hub)
- hub_policy (hub)
- hub_product (hub)
- hub_quote (hub)
- link_customer_lead (link)
- link_customer_person (link)
- link_person_account (link)
- link_person_consent (link)
- link_person_contact (link)
- link_person_home_address (link)
- link_person_identities (link)
- link_person_lead (link)
- link_person_legal_person (link)
- link_person_marketing_engagement (link)
- link_person_marketing_preference (link)
- link_policy_customer (link)
- link_policy_product (link)
- link_product_motor (link)
- link_quote_person (link)
- link_quote_product (link)
- sat_account (sat)
- sat_consent (sat)
- sat_contact (sat)
- sat_customer (sat)
- sat_home (sat)
- sat_home_address (sat)
- sat_identities (sat)
- sat_lead (sat)
- sat_legal_person (sat)
- sat_marketing_engagement (sat)
- sat_marketing_preference (sat)
- sat_motor (sat)
- sat_natural_person (sat)
- sat_person (sat)
- sat_policy (sat)
- sat_product (sat)
- sat_quote (sat)
