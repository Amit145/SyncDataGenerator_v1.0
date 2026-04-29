CREATE TABLE IF NOT EXISTS brz_vault_sql_metadata (
  object_id STRING,
  object_type STRING,
  target_table STRING,
  record_source STRING,
  input_catalog STRING,
  input_schema STRING,
  output_catalog STRING,
  output_schema STRING,
  process_order INT,
  is_active STRING,
  merge_keys STRING,
  build_sql STRING
);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'hub_policy__001' AS object_id,
  'hub' AS object_type,
  'hub_policy' AS target_table,
  'CRM' AS record_source,
  10 AS process_order,
  'Y' AS is_active,
  'policy_hash_key' AS merge_keys,
  'select distinct md5(cast(policy_ref as string)) as policy_hash_key, ${hub_load_date} as load_date, ${record_source} as record_source, policy_ref as policy_id from crm_policy_register where policy_ref is not null and trim(cast(policy_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'sat_policy__002' AS object_id,
  'sat' AS object_type,
  'sat_policy' AS target_table,
  'CRM' AS record_source,
  20 AS process_order,
  'Y' AS is_active,
  'policy_hash_key|load_date' AS merge_keys,
  'select distinct md5(cast(policy_ref as string)) as policy_hash_key, ${sat_load_date} as load_date, cover_option_txt as cover_option, declined_claim_cnt as declined_claims, fraud_ind as fraud_flag, gross_amt as gross_revenue, net_amt as net_revenue, active_claim_cnt as number_of_active_claim, previous_claim_cnt as number_of_previous_claim, policy_cycle_no as policy_cicle, policy_end_dt as policy_end_date, policy_term_months as policy_length, policy_no as policy_number, policy_start_dt as policy_start_date, policy_status_txt as policy_status, renewal_premium_curr as renewal_amount_current_period, renewal_premium_next as renewal_amount_next_period, renewal_dt as renewal_date, sales_channel_txt as sales_channel from crm_policy_register' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'link_policy_customer__003' AS object_id,
  'link' AS object_type,
  'link_policy_customer' AS target_table,
  'CRM' AS record_source,
  30 AS process_order,
  'Y' AS is_active,
  'policy_customer_hash_key' AS merge_keys,
  'with source_filtered as (select * from crm_policy_register where policy_ref is not null and trim(cast(policy_ref as string)) <> '''' and customer_ref is not null and trim(cast(customer_ref as string)) <> ''''), hashed as (select md5(cast(policy_ref as string)) as policy_hash_key, md5(cast(customer_ref as string)) as customer_hash_key from source_filtered) select distinct md5(concat(policy_hash_key, ''|'', customer_hash_key)) as policy_customer_hash_key, ${link_load_date} as load_date, ${record_source} as record_source, policy_hash_key, customer_hash_key from hashed' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'link_policy_product__004' AS object_id,
  'link' AS object_type,
  'link_policy_product' AS target_table,
  'CRM' AS record_source,
  40 AS process_order,
  'Y' AS is_active,
  'policy_product_hash_key' AS merge_keys,
  'with source_filtered as (select * from crm_policy_register where policy_ref is not null and trim(cast(policy_ref as string)) <> '''' and product_ref is not null and trim(cast(product_ref as string)) <> ''''), hashed as (select md5(cast(policy_ref as string)) as policy_hash_key, md5(cast(product_ref as string)) as product_hash_key from source_filtered) select distinct md5(concat(policy_hash_key, ''|'', product_hash_key)) as policy_product_hash_key, ${link_load_date} as load_date, ${record_source} as record_source, policy_hash_key, product_hash_key from hashed' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'hub_home__005' AS object_id,
  'hub' AS object_type,
  'hub_home' AS target_table,
  'CRM' AS record_source,
  50 AS process_order,
  'Y' AS is_active,
  'home_hash_key' AS merge_keys,
  'select distinct md5(cast(property_ref as string)) as home_hash_key, ${hub_load_date} as load_date, ${record_source} as record_source, property_ref as home_id from crm_property_asset where property_ref is not null and trim(cast(property_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'sat_home__006' AS object_id,
  'sat' AS object_type,
  'sat_home' AS target_table,
  'CRM' AS record_source,
  60 AS process_order,
  'Y' AS is_active,
  'home_hash_key|load_date' AS merge_keys,
  'select distinct md5(cast(property_ref as string)) as home_hash_key, ${sat_load_date} as load_date, wall_material_txt as wall_construction, risk_address_txt as home_risk_address, roof_material_txt as roof_construction, property_type_txt as home_type, property_state_cd as home_state, existing_home_ind as is_existing_home_customer from crm_property_asset' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'link_product_home__007' AS object_id,
  'link' AS object_type,
  'link_product_home' AS target_table,
  'CRM' AS record_source,
  70 AS process_order,
  'Y' AS is_active,
  'product_home_hash_key' AS merge_keys,
  'with source_filtered as (select * from crm_property_asset where product_ref is not null and trim(cast(product_ref as string)) <> '''' and property_ref is not null and trim(cast(property_ref as string)) <> ''''), hashed as (select md5(cast(product_ref as string)) as product_hash_key, md5(cast(property_ref as string)) as home_hash_key from source_filtered) select distinct md5(concat(product_hash_key, ''|'', home_hash_key)) as product_home_hash_key, ${link_load_date} as load_date, ${record_source} as record_source, product_hash_key, home_hash_key from hashed' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'hub_motor__008' AS object_id,
  'hub' AS object_type,
  'hub_motor' AS target_table,
  'CRM' AS record_source,
  80 AS process_order,
  'Y' AS is_active,
  'motor_hash_key' AS merge_keys,
  'select distinct md5(cast(vehicle_ref as string)) as motor_hash_key, ${hub_load_date} as load_date, ${record_source} as record_source, vehicle_ref as motor_id from crm_vehicle_asset where vehicle_ref is not null and trim(cast(vehicle_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'sat_motor__009' AS object_id,
  'sat' AS object_type,
  'sat_motor' AS target_table,
  'CRM' AS record_source,
  90 AS process_order,
  'Y' AS is_active,
  'motor_hash_key|load_date' AS merge_keys,
  'select distinct md5(cast(vehicle_ref as string)) as motor_hash_key, ${sat_load_date} as load_date, auto_decline_ind as auto_decline_vehicle, body_style_txt as body_type, fuel_type_txt as fuel_type, license_status_txt as license_status, existing_motor_ind as is_existing_motor_customer, motor_lapse_cnt as motor_lapsed_policies, garage_address_txt as motor_risk_address, risk_class_cd as risk_class_code, variant_nm as variant, owner_type_txt as vehicle_owner_type, registration_state_cd as vehicle_regstate, vehicle_class_txt as vehicle_class, model_nm as vehicle_model, vehicle_type_txt as vehicle_type, insured_value_amt as motor_sum_insrd, manufacture_yr as vehicle_year, vehicle_age_yrs as vehicle_age from crm_vehicle_asset' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'link_product_motor__010' AS object_id,
  'link' AS object_type,
  'link_product_motor' AS target_table,
  'CRM' AS record_source,
  100 AS process_order,
  'Y' AS is_active,
  'product_motor_hash_key' AS merge_keys,
  'with source_filtered as (select * from crm_vehicle_asset where product_ref is not null and trim(cast(product_ref as string)) <> '''' and vehicle_ref is not null and trim(cast(vehicle_ref as string)) <> ''''), hashed as (select md5(cast(product_ref as string)) as product_hash_key, md5(cast(vehicle_ref as string)) as motor_hash_key from source_filtered) select distinct md5(concat(product_hash_key, ''|'', motor_hash_key)) as product_motor_hash_key, ${link_load_date} as load_date, ${record_source} as record_source, product_hash_key, motor_hash_key from hashed' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'hub_marketing_preference__011' AS object_id,
  'hub' AS object_type,
  'hub_marketing_preference' AS target_table,
  'CRM' AS record_source,
  110 AS process_order,
  'Y' AS is_active,
  'marketing_preference_hash_key' AS merge_keys,
  'select distinct md5(cast(preference_ref as string)) as marketing_preference_hash_key, ${hub_load_date} as load_date, ${record_source} as record_source, preference_ref as marketing_preference_id from crm_comm_preference where preference_ref is not null and trim(cast(preference_ref as string)) <> '''' and party_ref is not null and trim(cast(party_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'link_person_marketing_preference__012' AS object_id,
  'link' AS object_type,
  'link_person_marketing_preference' AS target_table,
  'CRM' AS record_source,
  120 AS process_order,
  'Y' AS is_active,
  'person_marketing_preference_hash_key' AS merge_keys,
  'with source_filtered as (select * from crm_comm_preference where party_ref is not null and trim(cast(party_ref as string)) <> '''' and preference_ref is not null and trim(cast(preference_ref as string)) <> ''''), hashed as (select md5(cast(party_ref as string)) as person_hash_key, md5(cast(preference_ref as string)) as marketing_preference_hash_key from source_filtered) select distinct md5(concat(person_hash_key, ''|'', marketing_preference_hash_key)) as person_marketing_preference_hash_key, ${link_load_date} as load_date, ${record_source} as record_source, person_hash_key, marketing_preference_hash_key from hashed' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'sat_marketing_preference__013' AS object_id,
  'sat' AS object_type,
  'sat_marketing_preference' AS target_table,
  'CRM' AS record_source,
  130 AS process_order,
  'Y' AS is_active,
  'marketing_preference_hash_key|load_date' AS merge_keys,
  'select distinct md5(cast(preference_ref as string)) as marketing_preference_hash_key, ${sat_load_date} as load_date, sms_ind as sms, email_ind as email, email_sub_ind as email_subscriptions, call_ind as call, any_ind as any, commercial_email_ind as commercial_email, postal_mail_ind as postal_mail from crm_comm_preference where party_ref is not null and trim(cast(party_ref as string)) <> '''' and preference_ref is not null and trim(cast(preference_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'hub_marketing_engagement__014' AS object_id,
  'hub' AS object_type,
  'hub_marketing_engagement' AS target_table,
  'CRM' AS record_source,
  140 AS process_order,
  'Y' AS is_active,
  'marketing_engagement_hash_key' AS merge_keys,
  'select distinct md5(cast(engagement_ref as string)) as marketing_engagement_hash_key, ${hub_load_date} as load_date, ${record_source} as record_source, engagement_ref as marketing_engagement_id from crm_campaign_touch where engagement_ref is not null and trim(cast(engagement_ref as string)) <> '''' and party_ref is not null and trim(cast(party_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'link_person_marketing_engagement__015' AS object_id,
  'link' AS object_type,
  'link_person_marketing_engagement' AS target_table,
  'CRM' AS record_source,
  150 AS process_order,
  'Y' AS is_active,
  'person_marketing_engagement_hash_key' AS merge_keys,
  'with source_filtered as (select * from crm_campaign_touch where party_ref is not null and trim(cast(party_ref as string)) <> '''' and engagement_ref is not null and trim(cast(engagement_ref as string)) <> ''''), hashed as (select md5(cast(party_ref as string)) as person_hash_key, md5(cast(engagement_ref as string)) as marketing_engagement_hash_key from source_filtered) select distinct md5(concat(person_hash_key, ''|'', marketing_engagement_hash_key)) as person_marketing_engagement_hash_key, ${link_load_date} as load_date, ${record_source} as record_source, person_hash_key, marketing_engagement_hash_key from hashed' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'sat_marketing_engagement__016' AS object_id,
  'sat' AS object_type,
  'sat_marketing_engagement' AS target_table,
  'CRM' AS record_source,
  160 AS process_order,
  'Y' AS is_active,
  'marketing_engagement_hash_key|load_date' AS merge_keys,
  'select distinct md5(cast(engagement_ref as string)) as marketing_engagement_hash_key, ${sat_load_date} as load_date, promo_cd as promotion_code, email_opened_ind as opened_email, campaign_status_txt as marketing_status from crm_campaign_touch where party_ref is not null and trim(cast(party_ref as string)) <> '''' and engagement_ref is not null and trim(cast(engagement_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'hub_account__017' AS object_id,
  'hub' AS object_type,
  'hub_account' AS target_table,
  'CRM' AS record_source,
  170 AS process_order,
  'Y' AS is_active,
  'account_hash_key' AS merge_keys,
  'select distinct md5(cast(account_ref as string)) as account_hash_key, ${hub_load_date} as load_date, ${record_source} as record_source, account_ref as account_id from crm_account_book where account_ref is not null and trim(cast(account_ref as string)) <> '''' and party_ref is not null and trim(cast(party_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'link_person_account__018' AS object_id,
  'link' AS object_type,
  'link_person_account' AS target_table,
  'CRM' AS record_source,
  180 AS process_order,
  'Y' AS is_active,
  'person_account_hash_key' AS merge_keys,
  'with source_filtered as (select * from crm_account_book where party_ref is not null and trim(cast(party_ref as string)) <> '''' and account_ref is not null and trim(cast(account_ref as string)) <> ''''), hashed as (select md5(cast(party_ref as string)) as person_hash_key, md5(cast(account_ref as string)) as account_hash_key from source_filtered) select distinct md5(concat(person_hash_key, ''|'', account_hash_key)) as person_account_hash_key, ${link_load_date} as load_date, ${record_source} as record_source, person_hash_key, account_hash_key from hashed' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'sat_account__019' AS object_id,
  'sat' AS object_type,
  'sat_account' AS target_table,
  'CRM' AS record_source,
  190 AS process_order,
  'Y' AS is_active,
  'account_hash_key|load_date' AS merge_keys,
  'select distinct md5(cast(account_ref as string)) as account_hash_key, ${sat_load_date} as load_date, account_no as account_number, account_type_txt as account_type, last_access_dt as account_last_access, last_change_dt as account_last_change, account_create_type_txt as account_creation_type, account_status_txt as account_status from crm_account_book where party_ref is not null and trim(cast(party_ref as string)) <> '''' and account_ref is not null and trim(cast(account_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'hub_customer__020' AS object_id,
  'hub' AS object_type,
  'hub_customer' AS target_table,
  'CRM' AS record_source,
  200 AS process_order,
  'Y' AS is_active,
  'customer_hash_key' AS merge_keys,
  'select distinct md5(cast(customer_ref as string)) as customer_hash_key, ${hub_load_date} as load_date, ${record_source} as record_source, customer_ref as customer_id from crm_customer_portfolio where customer_ref is not null and trim(cast(customer_ref as string)) <> '''' and party_ref is not null and trim(cast(party_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'link_customer_person__021' AS object_id,
  'link' AS object_type,
  'link_customer_person' AS target_table,
  'CRM' AS record_source,
  210 AS process_order,
  'Y' AS is_active,
  'customer_person_hash_key' AS merge_keys,
  'with source_filtered as (select * from crm_customer_portfolio where customer_ref is not null and trim(cast(customer_ref as string)) <> '''' and party_ref is not null and trim(cast(party_ref as string)) <> ''''), hashed as (select md5(cast(customer_ref as string)) as customer_hash_key, md5(cast(party_ref as string)) as person_hash_key from source_filtered) select distinct md5(concat(customer_hash_key, ''|'', person_hash_key)) as customer_person_hash_key, ${link_load_date} as load_date, ${record_source} as record_source, customer_hash_key, person_hash_key from hashed' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'sat_customer__022' AS object_id,
  'sat' AS object_type,
  'sat_customer' AS target_table,
  'CRM' AS record_source,
  220 AS process_order,
  'Y' AS is_active,
  'customer_hash_key|load_date' AS merge_keys,
  'select distinct md5(cast(customer_ref as string)) as customer_hash_key, ${sat_load_date} as load_date, customer_no as customer_number, customer_status_txt as customer_status, customer_status_reason_txt as customer_status_reason, customer_since_dt as customer_since, customer_rating_no as customer_rating, customer_segment_txt as customer_segment, lob_txt as line_of_business, nps_score_no as nps_score from crm_customer_portfolio where customer_ref is not null and trim(cast(customer_ref as string)) <> '''' and party_ref is not null and trim(cast(party_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'link_customer_lead__023' AS object_id,
  'link' AS object_type,
  'link_customer_lead' AS target_table,
  'CRM' AS record_source,
  230 AS process_order,
  'Y' AS is_active,
  'customer_lead_hash_key' AS merge_keys,
  'with source_filtered as (select * from crm_customer_lead_bridge where customer_ref is not null and trim(cast(customer_ref as string)) <> '''' and lead_ref is not null and trim(cast(lead_ref as string)) <> ''''), hashed as (select md5(cast(customer_ref as string)) as customer_hash_key, md5(cast(lead_ref as string)) as lead_hash_key from source_filtered) select distinct md5(concat(customer_hash_key, ''|'', lead_hash_key)) as customer_lead_hash_key, ${link_load_date} as load_date, ${record_source} as record_source, customer_hash_key, lead_hash_key from hashed' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'hub_product__024' AS object_id,
  'hub' AS object_type,
  'hub_product' AS target_table,
  'CRM' AS record_source,
  240 AS process_order,
  'Y' AS is_active,
  'product_hash_key' AS merge_keys,
  'select distinct md5(cast(product_ref as string)) as product_hash_key, ${hub_load_date} as load_date, ${record_source} as record_source, product_ref as product_id from crm_product_catalog where product_ref is not null and trim(cast(product_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'sat_product__025' AS object_id,
  'sat' AS object_type,
  'sat_product' AS target_table,
  'CRM' AS record_source,
  250 AS process_order,
  'Y' AS is_active,
  'product_hash_key|load_date' AS merge_keys,
  'select distinct md5(cast(product_ref as string)) as product_hash_key, ${sat_load_date} as load_date, product_line as type from crm_product_catalog' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'hub_quote__026' AS object_id,
  'hub' AS object_type,
  'hub_quote' AS target_table,
  'CRM' AS record_source,
  260 AS process_order,
  'Y' AS is_active,
  'quote_hash_key' AS merge_keys,
  'select distinct md5(cast(quote_ref as string)) as quote_hash_key, ${hub_load_date} as load_date, ${record_source} as record_source, quote_ref as quote_id from crm_quote_register where quote_ref is not null and trim(cast(quote_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'sat_quote__027' AS object_id,
  'sat' AS object_type,
  'sat_quote' AS target_table,
  'CRM' AS record_source,
  270 AS process_order,
  'Y' AS is_active,
  'quote_hash_key|load_date' AS merge_keys,
  'select distinct md5(cast(quote_ref as string)) as quote_hash_key, ${sat_load_date} as load_date, gross_amt as gross_revenue, net_amt as net_revenue, quote_no as quote_number, quote_status_txt as quote_status, renewal_amt_curr as renewal_amt_current_period, renewal_amt_next as renewal_amt_next_period from crm_quote_register' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'link_quote_person__028' AS object_id,
  'link' AS object_type,
  'link_quote_person' AS target_table,
  'CRM' AS record_source,
  280 AS process_order,
  'Y' AS is_active,
  'quote_person_hash_key' AS merge_keys,
  'with source_filtered as (select * from crm_quote_register where quote_ref is not null and trim(cast(quote_ref as string)) <> '''' and party_ref is not null and trim(cast(party_ref as string)) <> ''''), hashed as (select md5(cast(quote_ref as string)) as quote_hash_key, md5(cast(party_ref as string)) as person_hash_key from source_filtered) select distinct md5(concat(quote_hash_key, ''|'', person_hash_key)) as quote_person_hash_key, ${link_load_date} as load_date, ${record_source} as record_source, quote_hash_key, person_hash_key from hashed' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'link_quote_product__029' AS object_id,
  'link' AS object_type,
  'link_quote_product' AS target_table,
  'CRM' AS record_source,
  290 AS process_order,
  'Y' AS is_active,
  'quote_product_hash_key' AS merge_keys,
  'with source_filtered as (select * from crm_quote_register where quote_ref is not null and trim(cast(quote_ref as string)) <> '''' and product_ref is not null and trim(cast(product_ref as string)) <> ''''), hashed as (select md5(cast(quote_ref as string)) as quote_hash_key, md5(cast(product_ref as string)) as product_hash_key from source_filtered) select distinct md5(concat(quote_hash_key, ''|'', product_hash_key)) as quote_product_hash_key, ${link_load_date} as load_date, ${record_source} as record_source, quote_hash_key, product_hash_key from hashed' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'hub_person__030' AS object_id,
  'hub' AS object_type,
  'hub_person' AS target_table,
  'CRM' AS record_source,
  300 AS process_order,
  'Y' AS is_active,
  'person_hash_key' AS merge_keys,
  'select distinct md5(cast(party_ref as string)) as person_hash_key, ${hub_load_date} as load_date, ${record_source} as record_source, party_ref as person_id from crm_party_master where party_ref is not null and trim(cast(party_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'sat_person__031' AS object_id,
  'sat' AS object_type,
  'sat_person' AS target_table,
  'CRM' AS record_source,
  310 AS process_order,
  'Y' AS is_active,
  'person_hash_key|load_date' AS merge_keys,
  'select distinct md5(cast(party_ref as string)) as person_hash_key, ${sat_load_date} as load_date, tenant_cd as tenant_id, lead_ind as is_lead, party_kind as type, paperless_ind as operational_paperless_consent, src_party_ref as source_id, src_party_type as source_type from crm_party_master' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'hub_natural_person__032' AS object_id,
  'hub' AS object_type,
  'hub_natural_person' AS target_table,
  'CRM' AS record_source,
  320 AS process_order,
  'Y' AS is_active,
  'natural_person_hash_key' AS merge_keys,
  'select distinct md5(cast(natural_ref as string)) as natural_person_hash_key, ${hub_load_date} as load_date, ${record_source} as record_source, natural_ref as natural_person_id from crm_party_master where natural_ref is not null and trim(cast(natural_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'link_person_natural_person__033' AS object_id,
  'link' AS object_type,
  'link_person_natural_person' AS target_table,
  'CRM' AS record_source,
  330 AS process_order,
  'Y' AS is_active,
  'person_natural_person_hash_key' AS merge_keys,
  'with source_filtered as (select * from crm_party_master where party_ref is not null and trim(cast(party_ref as string)) <> '''' and natural_ref is not null and trim(cast(natural_ref as string)) <> ''''), hashed as (select md5(cast(party_ref as string)) as person_hash_key, md5(cast(natural_ref as string)) as natural_person_hash_key from source_filtered) select distinct md5(concat(person_hash_key, ''|'', natural_person_hash_key)) as person_natural_person_hash_key, ${link_load_date} as load_date, ${record_source} as record_source, person_hash_key, natural_person_hash_key from hashed' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'sat_natural_person__034' AS object_id,
  'sat' AS object_type,
  'sat_natural_person' AS target_table,
  'CRM' AS record_source,
  340 AS process_order,
  'Y' AS is_active,
  'natural_person_hash_key|load_date' AS merge_keys,
  'select distinct md5(cast(natural_ref as string)) as natural_person_hash_key, ${sat_load_date} as load_date, given_nm as first_name, family_nm as last_name, display_nm as full_name, title_txt as courtesy_title, occupation_txt as occupation, dob as birth_date, birth_yr as birth_year, nationality_txt as nationality, gender_txt as gender, marital_txt as marital_status, disability_degree as assesed_disability_degree, language_pref as preferred_language, role_txt as role, job_title_txt as job_title from crm_party_master where natural_ref is not null and trim(cast(natural_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'hub_legal_person__035' AS object_id,
  'hub' AS object_type,
  'hub_legal_person' AS target_table,
  'CRM' AS record_source,
  350 AS process_order,
  'Y' AS is_active,
  'legal_person_hash_key' AS merge_keys,
  'select distinct md5(cast(legal_ref as string)) as legal_person_hash_key, ${hub_load_date} as load_date, ${record_source} as record_source, legal_ref as legal_person_id from crm_party_master where legal_ref is not null and trim(cast(legal_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'link_person_legal_person__036' AS object_id,
  'link' AS object_type,
  'link_person_legal_person' AS target_table,
  'CRM' AS record_source,
  360 AS process_order,
  'Y' AS is_active,
  'person_legal_person_hash_key' AS merge_keys,
  'with source_filtered as (select * from crm_party_master where party_ref is not null and trim(cast(party_ref as string)) <> '''' and legal_ref is not null and trim(cast(legal_ref as string)) <> ''''), hashed as (select md5(cast(party_ref as string)) as person_hash_key, md5(cast(legal_ref as string)) as legal_person_hash_key from source_filtered) select distinct md5(concat(person_hash_key, ''|'', legal_person_hash_key)) as person_legal_person_hash_key, ${link_load_date} as load_date, ${record_source} as record_source, person_hash_key, legal_person_hash_key from hashed' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'sat_legal_person__037' AS object_id,
  'sat' AS object_type,
  'sat_legal_person' AS target_table,
  'CRM' AS record_source,
  370 AS process_order,
  'Y' AS is_active,
  'legal_person_hash_key|load_date' AS merge_keys,
  'select distinct md5(cast(legal_ref as string)) as legal_person_hash_key, ${sat_load_date} as load_date, legal_score_no as person_score, legal_job_title_txt as job_title, legal_src_ref as source_id, legal_src_type as source_type, legal_status_txt as person_status, lead_conv_dt as converted_date, constitution_dt as date_of_constitution, legal_name as company_name from crm_party_master where legal_ref is not null and trim(cast(legal_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'hub_contact__038' AS object_id,
  'hub' AS object_type,
  'hub_contact' AS target_table,
  'CRM' AS record_source,
  380 AS process_order,
  'Y' AS is_active,
  'contact_hash_key' AS merge_keys,
  'select distinct md5(cast(contact_ref as string)) as contact_hash_key, ${hub_load_date} as load_date, ${record_source} as record_source, contact_ref as contact_id from crm_contact_point where contact_ref is not null and trim(cast(contact_ref as string)) <> '''' and party_ref is not null and trim(cast(party_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'link_person_contact__039' AS object_id,
  'link' AS object_type,
  'link_person_contact' AS target_table,
  'CRM' AS record_source,
  390 AS process_order,
  'Y' AS is_active,
  'person_contact_hash_key' AS merge_keys,
  'with source_filtered as (select * from crm_contact_point where party_ref is not null and trim(cast(party_ref as string)) <> '''' and contact_ref is not null and trim(cast(contact_ref as string)) <> ''''), hashed as (select md5(cast(party_ref as string)) as person_hash_key, md5(cast(contact_ref as string)) as contact_hash_key from source_filtered) select distinct md5(concat(person_hash_key, ''|'', contact_hash_key)) as person_contact_hash_key, ${link_load_date} as load_date, ${record_source} as record_source, person_hash_key, contact_hash_key from hashed' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'sat_contact__040' AS object_id,
  'sat' AS object_type,
  'sat_contact' AS target_table,
  'CRM' AS record_source,
  400 AS process_order,
  'Y' AS is_active,
  'contact_hash_key|load_date' AS merge_keys,
  'select distinct md5(cast(contact_ref as string)) as contact_hash_key, ${sat_load_date} as load_date, email_home_txt as personal_email, email_work_txt as work_email, phone_work_txt as work_phone, phone_home_txt as home_phone from crm_contact_point where party_ref is not null and trim(cast(party_ref as string)) <> '''' and contact_ref is not null and trim(cast(contact_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'hub_identities__041' AS object_id,
  'hub' AS object_type,
  'hub_identities' AS target_table,
  'CRM' AS record_source,
  410 AS process_order,
  'Y' AS is_active,
  'identities_hash_key' AS merge_keys,
  'select distinct md5(cast(identity_ref as string)) as identities_hash_key, ${hub_load_date} as load_date, ${record_source} as record_source, identity_ref as identities_id from crm_identity_registry where identity_ref is not null and trim(cast(identity_ref as string)) <> '''' and party_ref is not null and trim(cast(party_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'link_person_identities__042' AS object_id,
  'link' AS object_type,
  'link_person_identities' AS target_table,
  'CRM' AS record_source,
  420 AS process_order,
  'Y' AS is_active,
  'person_identities_hash_key' AS merge_keys,
  'with source_filtered as (select * from crm_identity_registry where party_ref is not null and trim(cast(party_ref as string)) <> '''' and identity_ref is not null and trim(cast(identity_ref as string)) <> ''''), hashed as (select md5(cast(party_ref as string)) as person_hash_key, md5(cast(identity_ref as string)) as identities_hash_key from source_filtered) select distinct md5(concat(person_hash_key, ''|'', identities_hash_key)) as person_identities_hash_key, ${link_load_date} as load_date, ${record_source} as record_source, person_hash_key, identities_hash_key from hashed' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'sat_identities__043' AS object_id,
  'sat' AS object_type,
  'sat_identities' AS target_table,
  'CRM' AS record_source,
  430 AS process_order,
  'Y' AS is_active,
  'identities_hash_key|load_date' AS merge_keys,
  'select distinct md5(cast(identity_ref as string)) as identities_hash_key, ${sat_load_date} as load_date, ecid_txt as ecid, hashed_email_txt as hashed_email from crm_identity_registry where party_ref is not null and trim(cast(party_ref as string)) <> '''' and identity_ref is not null and trim(cast(identity_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'hub_home_address__044' AS object_id,
  'hub' AS object_type,
  'hub_home_address' AS target_table,
  'CRM' AS record_source,
  440 AS process_order,
  'Y' AS is_active,
  'home_address_hash_key' AS merge_keys,
  'select distinct md5(cast(address_ref as string)) as home_address_hash_key, ${hub_load_date} as load_date, ${record_source} as record_source, address_ref as home_address_id from crm_address_book where address_ref is not null and trim(cast(address_ref as string)) <> '''' and party_ref is not null and trim(cast(party_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'link_person_home_address__045' AS object_id,
  'link' AS object_type,
  'link_person_home_address' AS target_table,
  'CRM' AS record_source,
  450 AS process_order,
  'Y' AS is_active,
  'person_home_address_hash_key' AS merge_keys,
  'with source_filtered as (select * from crm_address_book where party_ref is not null and trim(cast(party_ref as string)) <> '''' and address_ref is not null and trim(cast(address_ref as string)) <> ''''), hashed as (select md5(cast(party_ref as string)) as person_hash_key, md5(cast(address_ref as string)) as home_address_hash_key from source_filtered) select distinct md5(concat(person_hash_key, ''|'', home_address_hash_key)) as person_home_address_hash_key, ${link_load_date} as load_date, ${record_source} as record_source, person_hash_key, home_address_hash_key from hashed' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'sat_home_address__046' AS object_id,
  'sat' AS object_type,
  'sat_home_address' AS target_table,
  'CRM' AS record_source,
  460 AS process_order,
  'Y' AS is_active,
  'home_address_hash_key|load_date' AS merge_keys,
  'select distinct md5(cast(address_ref as string)) as home_address_hash_key, ${sat_load_date} as load_date, street_txt as street, postal_cd as postcode, city_nm as city, state_cd as state, country_cd as country from crm_address_book where party_ref is not null and trim(cast(party_ref as string)) <> '''' and address_ref is not null and trim(cast(address_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'hub_lead__047' AS object_id,
  'hub' AS object_type,
  'hub_lead' AS target_table,
  'CRM' AS record_source,
  470 AS process_order,
  'Y' AS is_active,
  'lead_hash_key' AS merge_keys,
  'select distinct md5(cast(lead_ref as string)) as lead_hash_key, ${hub_load_date} as load_date, ${record_source} as record_source, lead_ref as lead_id from crm_lead_register where lead_ref is not null and trim(cast(lead_ref as string)) <> '''' and party_ref is not null and trim(cast(party_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'link_person_lead__048' AS object_id,
  'link' AS object_type,
  'link_person_lead' AS target_table,
  'CRM' AS record_source,
  480 AS process_order,
  'Y' AS is_active,
  'person_lead_hash_key' AS merge_keys,
  'with source_filtered as (select * from crm_lead_register where party_ref is not null and trim(cast(party_ref as string)) <> '''' and lead_ref is not null and trim(cast(lead_ref as string)) <> ''''), hashed as (select md5(cast(party_ref as string)) as person_hash_key, md5(cast(lead_ref as string)) as lead_hash_key from source_filtered) select distinct md5(concat(person_hash_key, ''|'', lead_hash_key)) as person_lead_hash_key, ${link_load_date} as load_date, ${record_source} as record_source, person_hash_key, lead_hash_key from hashed' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'sat_lead__049' AS object_id,
  'sat' AS object_type,
  'sat_lead' AS target_table,
  'CRM' AS record_source,
  490 AS process_order,
  'Y' AS is_active,
  'lead_hash_key|load_date' AS merge_keys,
  'select distinct md5(cast(lead_ref as string)) as lead_hash_key, ${sat_load_date} as load_date, interest_bucket as interested_level, contact_pref as preferred_contact_method, person_score_no as person_score, person_status_txt as person_status, converted_dt as converted_date from crm_lead_register where party_ref is not null and trim(cast(party_ref as string)) <> '''' and lead_ref is not null and trim(cast(lead_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'hub_consent__050' AS object_id,
  'hub' AS object_type,
  'hub_consent' AS target_table,
  'CRM' AS record_source,
  500 AS process_order,
  'Y' AS is_active,
  'consent_hash_key' AS merge_keys,
  'select distinct md5(cast(consent_ref as string)) as consent_hash_key, ${hub_load_date} as load_date, ${record_source} as record_source, consent_ref as consent_id from crm_consent_snapshot where consent_ref is not null and trim(cast(consent_ref as string)) <> '''' and party_ref is not null and trim(cast(party_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'link_person_consent__051' AS object_id,
  'link' AS object_type,
  'link_person_consent' AS target_table,
  'CRM' AS record_source,
  510 AS process_order,
  'Y' AS is_active,
  'person_consent_hash_key' AS merge_keys,
  'with source_filtered as (select * from crm_consent_snapshot where party_ref is not null and trim(cast(party_ref as string)) <> '''' and consent_ref is not null and trim(cast(consent_ref as string)) <> ''''), hashed as (select md5(cast(party_ref as string)) as person_hash_key, md5(cast(consent_ref as string)) as consent_hash_key from source_filtered) select distinct md5(concat(person_hash_key, ''|'', consent_hash_key)) as person_consent_hash_key, ${link_load_date} as load_date, ${record_source} as record_source, person_hash_key, consent_hash_key from hashed' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);

MERGE INTO brz_vault_sql_metadata AS target
USING (SELECT
  'sat_consent__052' AS object_id,
  'sat' AS object_type,
  'sat_consent' AS target_table,
  'CRM' AS record_source,
  520 AS process_order,
  'Y' AS is_active,
  'consent_hash_key|load_date' AS merge_keys,
  'select distinct md5(cast(consent_ref as string)) as consent_hash_key, ${sat_load_date} as load_date, opt_in_valid_ind as opt_in_validated, opt_in_legit_ind as opt_in_legitimate_interest from crm_consent_snapshot where party_ref is not null and trim(cast(party_ref as string)) <> '''' and consent_ref is not null and trim(cast(consent_ref as string)) <> ''''' AS build_sql
) AS source
ON target.object_id = source.object_id
WHEN MATCHED THEN UPDATE SET
  target.object_type = source.object_type,
  target.target_table = source.target_table,
  target.record_source = source.record_source,
  target.process_order = source.process_order,
  target.is_active = source.is_active,
  target.merge_keys = source.merge_keys,
  target.build_sql = source.build_sql
WHEN NOT MATCHED THEN INSERT (object_id, object_type, target_table, record_source, process_order, is_active, merge_keys, build_sql)
VALUES (source.object_id, source.object_type, source.target_table, source.record_source, source.process_order, source.is_active, source.merge_keys, source.build_sql);
 
UPDATE brz_vault_sql_metadata
SET
  input_catalog = CASE upper(record_source)
    WHEN 'CRM' THEN 'allianz_coe'
    WHEN 'API' THEN 'allianz_coe'
    WHEN 'KAGGLE' THEN 'allianz_coe'
    WHEN 'DATA_SOURCE' THEN 'allianz_coe'
    ELSE input_catalog
  END,
  input_schema = CASE upper(record_source)
    WHEN 'CRM' THEN 'amit_raw_test'
    WHEN 'API' THEN 'amit_api_raw_test'
    WHEN 'KAGGLE' THEN 'amit_kaggle_raw_test'
    WHEN 'DATA_SOURCE' THEN 'amit_data_source_raw_test'
    ELSE input_schema
  END,
  output_catalog = CASE upper(record_source)
    WHEN 'CRM' THEN 'allianz_coe'
    WHEN 'API' THEN 'allianz_coe'
    WHEN 'KAGGLE' THEN 'allianz_coe'
    WHEN 'DATA_SOURCE' THEN 'allianz_coe'
    ELSE output_catalog
  END,
  output_schema = CASE upper(record_source)
    WHEN 'CRM' THEN 'amit_silver_test'
    WHEN 'API' THEN 'amit_silver_api_test'
    WHEN 'KAGGLE' THEN 'amit_silver_kaggle_test'
    WHEN 'DATA_SOURCE' THEN 'amit_silver_data_source_test'
    ELSE output_schema
  END
WHERE upper(record_source) IN ('CRM', 'API', 'KAGGLE', 'DATA_SOURCE');
