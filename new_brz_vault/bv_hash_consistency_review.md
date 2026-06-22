# bv.xlsx Hash Consistency Review

Workbook reviewed: `new_brz_vault\bv.xlsx`

Sheet reviewed: `result`

Rows reviewed: `52`

## Current Status

`bv.xlsx` has been updated so all `52` vault SQLs follow one hash convention.

Validation checks after update:

| Check | Result |
|---|---|
| Workbook rows | `52` |
| Unique target tables | `52` |
| Missing target tables versus latest base vault | `0` |
| Extra target tables versus latest base vault | `0` |
| Duplicate target tables | `0` |
| Output alias mismatches | `0` |
| Link hash consistency issues | `0` |
| `""""` filter typo occurrences | `0` |

## Hash Convention

All hub and satellite parent keys use:

```sql
md5(concat(cast(<business_ref> AS STRING), source_name)) AS <hub_hash_key>
```

All link foreign keys now use the exact same expression as their parent hub key.

All link primary keys now use only the two resolved child hash keys:

```sql
md5(concat(<left_hash_key>, "|", <right_hash_key>)) AS <link_hash_key>
```

This prevents orphan joins where a hub uses `md5(concat(ref, source_name))` but a link uses only `md5(ref)`.

## Actual Link SQL Fix Pattern

Use this structure for every link table:

```sql
WITH combined_source AS (
  SELECT
    md5(concat(cast(<left_ref> AS STRING), source_name)) AS <left_hash_key>,
    md5(concat(cast(<right_ref> AS STRING), source_name)) AS <right_hash_key>,
    load_ts,
    source_name,
    target_table AS source_tbl_name
  FROM <source_table>_csv
  WHERE <left_ref> IS NOT NULL
    AND trim(cast(<left_ref> AS STRING)) <> ""
    AND <right_ref> IS NOT NULL
    AND trim(cast(<right_ref> AS STRING)) <> ""
    AND source_name IS NOT NULL
    AND trim(cast(source_name AS STRING)) <> ""

  UNION

  SELECT
    md5(concat(cast(<left_ref> AS STRING), source_name)) AS <left_hash_key>,
    md5(concat(cast(<right_ref> AS STRING), source_name)) AS <right_hash_key>,
    load_ts,
    source_name,
    target_table AS source_tbl_name
  FROM <source_table>_db
  WHERE <left_ref> IS NOT NULL
    AND trim(cast(<left_ref> AS STRING)) <> ""
    AND <right_ref> IS NOT NULL
    AND trim(cast(<right_ref> AS STRING)) <> ""
    AND source_name IS NOT NULL
    AND trim(cast(source_name AS STRING)) <> ""
)
SELECT DISTINCT
  md5(concat(<left_hash_key>, "|", <right_hash_key>)) AS <link_hash_key>,
  load_ts AS load_date,
  source_name AS record_source,
  <left_hash_key>,
  <right_hash_key>,
  source_tbl_name
FROM combined_source;
```

### Concrete Example

For `link_person_contact`, the old issue was that the link used `md5(party_ref)` and `md5(contact_ref)`, while `hub_person` and `hub_contact` use source-aware hashes.

Wrong:

```sql
md5(cast(party_ref AS STRING)) AS person_hash_key,
md5(cast(contact_ref AS STRING)) AS contact_hash_key,
md5(concat(md5(cast(party_ref AS STRING)), "|", md5(cast(contact_ref AS STRING)), "|", md5(cast(source_name AS STRING)))) AS person_contact_hash_key
```

Correct:

```sql
md5(concat(cast(party_ref AS STRING), source_name)) AS person_hash_key,
md5(concat(cast(contact_ref AS STRING), source_name)) AS contact_hash_key,
md5(concat(person_hash_key, "|", contact_hash_key)) AS person_contact_hash_key
```

The corrected child keys now match:

```sql
hub_person.person_hash_key
= link_person_contact.person_hash_key
= md5(concat(cast(party_ref AS STRING), source_name))

hub_contact.contact_hash_key
= link_person_contact.contact_hash_key
= md5(concat(cast(contact_ref AS STRING), source_name))
```

## Table-Level Fix Mapping

| Link table | Source table base | Left ref -> hash key | Right ref -> hash key | Link PK |
|---|---|---|---|---|
| `link_customer_lead` | `crm_customer_lead_bridge` | `customer_ref -> customer_hash_key` | `lead_ref -> lead_hash_key` | `customer_lead_hash_key` |
| `link_customer_person` | `crm_customer_portfolio` | `customer_ref -> customer_hash_key` | `party_ref -> person_hash_key` | `customer_person_hash_key` |
| `link_person_account` | `crm_account_book` | `party_ref -> person_hash_key` | `account_ref -> account_hash_key` | `person_account_hash_key` |
| `link_person_consent` | `crm_consent_snapshot` | `party_ref -> person_hash_key` | `consent_ref -> consent_hash_key` | `person_consent_hash_key` |
| `link_person_contact` | `crm_contact_point` | `party_ref -> person_hash_key` | `contact_ref -> contact_hash_key` | `person_contact_hash_key` |
| `link_person_home_address` | `crm_address_book` | `party_ref -> person_hash_key` | `address_ref -> home_address_hash_key` | `person_home_address_hash_key` |
| `link_person_identities` | `crm_identity_registry` | `party_ref -> person_hash_key` | `identity_ref -> identities_hash_key` | `person_identities_hash_key` |
| `link_person_lead` | `crm_lead_register` | `party_ref -> person_hash_key` | `lead_ref -> lead_hash_key` | `person_lead_hash_key` |
| `link_person_legal_person` | `crm_party_master` | `party_ref -> person_hash_key` | `legal_ref -> legal_person_hash_key` | `person_legal_person_hash_key` |
| `link_person_marketing_engagement` | `crm_campaign_touch` | `party_ref -> person_hash_key` | `engagement_ref -> marketing_engagement_hash_key` | `person_marketing_engagement_hash_key` |
| `link_person_marketing_preference` | `crm_comm_preference` | `party_ref -> person_hash_key` | `preference_ref -> marketing_preference_hash_key` | `person_marketing_preference_hash_key` |
| `link_person_natural_person` | `crm_party_master` | `party_ref -> person_hash_key` | `natural_ref -> natural_person_hash_key` | `person_natural_person_hash_key` |
| `link_policy_customer` | `crm_policy_register` | `policy_ref -> policy_hash_key` | `customer_ref -> customer_hash_key` | `policy_customer_hash_key` |
| `link_policy_product` | `crm_policy_register` | `policy_ref -> policy_hash_key` | `product_ref -> product_hash_key` | `policy_customer_hash_key` |
| `link_product_home` | `crm_property_asset` | `product_ref -> product_hash_key` | `property_ref -> home_hash_key` | `product_home_hash_key` |
| `link_product_motor` | `crm_vehicle_asset` | `product_ref -> product_hash_key` | `vehicle_ref -> motor_hash_key` | `product_motor_hash_key` |
| `link_quote_person` | `crm_quote_register` | `quote_ref -> quote_hash_key` | `party_ref -> person_hash_key` | `quote_person_hash_key` |
| `link_quote_product` | `crm_quote_register` | `quote_ref -> quote_hash_key` | `product_ref -> product_hash_key` | `quote_product_hash_key` |

Each child hash key in this table must be generated with:

```sql
md5(concat(cast(<ref> AS STRING), source_name))
```

Each link PK must be generated with:

```sql
md5(concat(<left_hash_key>, "|", <right_hash_key>))
```

## Tables Fixed

These link tables were standardized:

- `link_customer_lead`
- `link_customer_person`
- `link_person_account`
- `link_person_consent`
- `link_person_contact`
- `link_person_home_address`
- `link_person_identities`
- `link_person_lead`
- `link_person_legal_person`
- `link_person_marketing_engagement`
- `link_person_marketing_preference`
- `link_person_natural_person`
- `link_policy_customer`
- `link_policy_product`
- `link_product_home`
- `link_product_motor`
- `link_quote_person`
- `link_quote_product`

## Previous Issues Removed

The earlier inconsistent patterns were removed:

- Link child keys using `md5(<ref>)` while parent hubs used `md5(concat(<ref>, source_name))`.
- Link primary keys adding `source_name` again after child keys already contained `source_name`.
- Link primary keys hashing raw refs instead of resolved child hash keys.
- `link_policy_product` filter typo `<> """"`.

## Join Impact

With the current SQLs, link foreign-key columns should join back to their parent hubs because the child link hash expressions match the hub hash expressions.

Example:

```sql
hub_person.person_hash_key
= md5(concat(cast(party_ref AS STRING), source_name))

link_person_contact.person_hash_key
= md5(concat(cast(party_ref AS STRING), source_name))
```

The same pattern is now used across all link relationships in the workbook.
