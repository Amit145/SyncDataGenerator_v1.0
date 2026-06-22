-- Raw PRD1 to base Data Vault.
-- Dialect: MySQL 8 compatible.
-- Replace target table names if your vault schema uses prefixes such as silver_ or dv_.
-- Replace raw_prd1_* staging table names if your raw loader uses another convention.

-- -------------------------
-- Hubs
-- -------------------------

INSERT INTO hub_person (person_hash_key, load_date, record_source, person_id)
SELECT DISTINCT MD5(party_ref), pull_ts, origin_sys, party_ref
FROM raw_prd1_party_master
WHERE party_ref IS NOT NULL AND party_ref <> '';

INSERT INTO hub_natural_person (natural_person_hash_key, load_date, record_source, natural_person_id)
SELECT DISTINCT MD5(natural_ref), pull_ts, origin_sys, natural_ref
FROM raw_prd1_party_master
WHERE natural_ref IS NOT NULL AND natural_ref <> '';

INSERT INTO hub_legal_person (legal_person_hash_key, load_date, record_source, legal_person_id)
SELECT DISTINCT MD5(legal_ref), pull_ts, origin_sys, legal_ref
FROM raw_prd1_party_master
WHERE legal_ref IS NOT NULL AND legal_ref <> '';

INSERT INTO hub_contact (contact_hash_key, load_date, record_source, contact_id)
SELECT DISTINCT MD5(contact_ref), pull_ts, origin_sys, contact_ref
FROM raw_prd1_contact_point
WHERE contact_ref IS NOT NULL AND contact_ref <> '';

INSERT INTO hub_identities (identities_hash_key, load_date, record_source, identities_id)
SELECT DISTINCT MD5(identity_ref), pull_ts, origin_sys, identity_ref
FROM raw_prd1_identity_registry
WHERE identity_ref IS NOT NULL AND identity_ref <> '';

INSERT INTO hub_home_address (home_address_hash_key, load_date, record_source, home_address_id)
SELECT DISTINCT MD5(address_ref), pull_ts, origin_sys, address_ref
FROM raw_prd1_address_book
WHERE address_ref IS NOT NULL AND address_ref <> '';

INSERT INTO hub_lead (lead_hash_key, load_date, record_source, lead_id)
SELECT DISTINCT MD5(lead_ref), pull_ts, origin_sys, lead_ref
FROM raw_prd1_lead_register
WHERE lead_ref IS NOT NULL AND lead_ref <> '';

INSERT INTO hub_customer (customer_hash_key, load_date, record_source, customer_id)
SELECT DISTINCT MD5(customer_ref), pull_ts, origin_sys, customer_ref
FROM raw_prd1_customer_portfolio
WHERE customer_ref IS NOT NULL AND customer_ref <> '';

INSERT INTO hub_consent (consent_hash_key, load_date, record_source, consent_id)
SELECT DISTINCT MD5(consent_ref), pull_ts, origin_sys, consent_ref
FROM raw_prd1_consent_snapshot
WHERE consent_ref IS NOT NULL AND consent_ref <> '';

INSERT INTO hub_marketing_preference (marketing_preference_hash_key, load_date, record_source, marketing_preference_id)
SELECT DISTINCT MD5(preference_ref), pull_ts, origin_sys, preference_ref
FROM raw_prd1_comm_preference
WHERE preference_ref IS NOT NULL AND preference_ref <> '';

INSERT INTO hub_marketing_engagement (marketing_engagement_hash_key, load_date, record_source, marketing_engagement_id)
SELECT DISTINCT MD5(engagement_ref), pull_ts, origin_sys, engagement_ref
FROM raw_prd1_campaign_touch
WHERE engagement_ref IS NOT NULL AND engagement_ref <> '';

INSERT INTO hub_account (account_hash_key, load_date, record_source, account_id)
SELECT DISTINCT MD5(account_ref), pull_ts, origin_sys, account_ref
FROM raw_prd1_account_book
WHERE account_ref IS NOT NULL AND account_ref <> '';

INSERT INTO hub_product (product_hash_key, load_date, record_source, product_id)
SELECT DISTINCT MD5(product_ref), pull_ts, origin_sys, product_ref
FROM raw_prd1_product_catalog
WHERE product_ref IS NOT NULL AND product_ref <> '';

INSERT INTO hub_quote (quote_hash_key, load_date, record_source, quote_id)
SELECT DISTINCT MD5(quote_ref), pull_ts, origin_sys, quote_ref
FROM raw_prd1_quote_register
WHERE quote_ref IS NOT NULL AND quote_ref <> '';

INSERT INTO hub_policy (policy_hash_key, load_date, record_source, policy_id)
SELECT DISTINCT MD5(policy_ref), pull_ts, origin_sys, policy_ref
FROM raw_prd1_policy_register
WHERE policy_ref IS NOT NULL AND policy_ref <> '';

INSERT INTO hub_home (home_hash_key, load_date, record_source, home_id)
SELECT DISTINCT MD5(property_ref), pull_ts, origin_sys, property_ref
FROM raw_prd1_property_asset
WHERE property_ref IS NOT NULL AND property_ref <> '';

INSERT INTO hub_motor (motor_hash_key, load_date, record_source, motor_id)
SELECT DISTINCT MD5(vehicle_ref), pull_ts, origin_sys, vehicle_ref
FROM raw_prd1_vehicle_asset
WHERE vehicle_ref IS NOT NULL AND vehicle_ref <> '';

-- -------------------------
-- Links
-- -------------------------

INSERT INTO link_person_natural_person (person_natural_person_hash_key, load_date, record_source, person_hash_key, natural_person_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(party_ref), '|', MD5(natural_ref))), pull_ts, origin_sys, MD5(party_ref), MD5(natural_ref)
FROM raw_prd1_party_master
WHERE party_ref <> '' AND natural_ref <> '';

INSERT INTO link_person_legal_person (person_legal_person_hash_key, load_date, record_source, person_hash_key, legal_person_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(party_ref), '|', MD5(legal_ref))), pull_ts, origin_sys, MD5(party_ref), MD5(legal_ref)
FROM raw_prd1_party_master
WHERE party_ref <> '' AND legal_ref <> '';

INSERT INTO link_person_contact (person_contact_hash_key, load_date, record_source, person_hash_key, contact_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(party_ref), '|', MD5(contact_ref))), pull_ts, origin_sys, MD5(party_ref), MD5(contact_ref)
FROM raw_prd1_contact_point
WHERE party_ref <> '' AND contact_ref <> '';

INSERT INTO link_person_identities (person_identities_hash_key, load_date, record_source, person_hash_key, identities_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(party_ref), '|', MD5(identity_ref))), pull_ts, origin_sys, MD5(party_ref), MD5(identity_ref)
FROM raw_prd1_identity_registry
WHERE party_ref <> '' AND identity_ref <> '';

INSERT INTO link_person_home_address (person_home_address_hash_key, load_date, record_source, person_hash_key, home_address_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(party_ref), '|', MD5(address_ref))), pull_ts, origin_sys, MD5(party_ref), MD5(address_ref)
FROM raw_prd1_address_book
WHERE party_ref <> '' AND address_ref <> '';

INSERT INTO link_person_lead (person_lead_hash_key, load_date, record_source, person_hash_key, lead_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(party_ref), '|', MD5(lead_ref))), pull_ts, origin_sys, MD5(party_ref), MD5(lead_ref)
FROM raw_prd1_lead_register
WHERE party_ref <> '' AND lead_ref <> '';

INSERT INTO link_customer_person (customer_person_hash_key, load_date, record_source, customer_hash_key, person_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(customer_ref), '|', MD5(party_ref))), pull_ts, origin_sys, MD5(customer_ref), MD5(party_ref)
FROM raw_prd1_customer_portfolio
WHERE customer_ref <> '' AND party_ref <> '';

INSERT INTO link_customer_lead (customer_lead_hash_key, load_date, record_source, customer_hash_key, lead_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(customer_ref), '|', MD5(lead_ref))), pull_ts, origin_sys, MD5(customer_ref), MD5(lead_ref)
FROM raw_prd1_customer_lead_bridge
WHERE customer_ref <> '' AND lead_ref <> '';

INSERT INTO link_person_consent (person_consent_hash_key, load_date, record_source, person_hash_key, consent_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(party_ref), '|', MD5(consent_ref))), pull_ts, origin_sys, MD5(party_ref), MD5(consent_ref)
FROM raw_prd1_consent_snapshot
WHERE party_ref <> '' AND consent_ref <> '';

INSERT INTO link_person_marketing_preference (person_marketing_preference_hash_key, load_date, record_source, person_hash_key, marketing_preference_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(party_ref), '|', MD5(preference_ref))), pull_ts, origin_sys, MD5(party_ref), MD5(preference_ref)
FROM raw_prd1_comm_preference
WHERE party_ref <> '' AND preference_ref <> '';

INSERT INTO link_person_marketing_engagement (person_marketing_engagement_hash_key, load_date, record_source, person_hash_key, marketing_engagement_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(party_ref), '|', MD5(engagement_ref))), pull_ts, origin_sys, MD5(party_ref), MD5(engagement_ref)
FROM raw_prd1_campaign_touch
WHERE party_ref <> '' AND engagement_ref <> '';

INSERT INTO link_person_account (person_account_hash_key, load_date, record_source, person_hash_key, account_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(party_ref), '|', MD5(account_ref))), pull_ts, origin_sys, MD5(party_ref), MD5(account_ref)
FROM raw_prd1_account_book
WHERE party_ref <> '' AND account_ref <> '';

INSERT INTO link_quote_person (quote_person_hash_key, load_date, record_source, quote_hash_key, person_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(quote_ref), '|', MD5(party_ref))), pull_ts, origin_sys, MD5(quote_ref), MD5(party_ref)
FROM raw_prd1_quote_register
WHERE quote_ref <> '' AND party_ref <> '';

INSERT INTO link_quote_product (quote_product_hash_key, load_date, record_source, quote_hash_key, product_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(quote_ref), '|', MD5(product_ref))), pull_ts, origin_sys, MD5(quote_ref), MD5(product_ref)
FROM raw_prd1_quote_register
WHERE quote_ref <> '' AND product_ref <> '';

INSERT INTO link_policy_customer (policy_customer_hash_key, load_date, record_source, policy_hash_key, customer_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(policy_ref), '|', MD5(customer_ref))), pull_ts, origin_sys, MD5(policy_ref), MD5(customer_ref)
FROM raw_prd1_policy_register
WHERE policy_ref <> '' AND customer_ref <> '';

INSERT INTO link_policy_product (policy_customer_hash_key, load_date, record_source, policy_hash_key, product_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(policy_ref), '|', MD5(product_ref))), pull_ts, origin_sys, MD5(policy_ref), MD5(product_ref)
FROM raw_prd1_policy_register
WHERE policy_ref <> '' AND product_ref <> '';

INSERT INTO link_product_home (product_home_hash_key, load_date, record_source, product_hash_key, home_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(product_ref), '|', MD5(property_ref))), pull_ts, origin_sys, MD5(product_ref), MD5(property_ref)
FROM raw_prd1_property_asset
WHERE product_ref <> '' AND property_ref <> '';

INSERT INTO link_product_motor (product_motor_hash_key, load_date, record_source, product_hash_key, motor_hash_key)
SELECT DISTINCT MD5(CONCAT(MD5(product_ref), '|', MD5(vehicle_ref))), pull_ts, origin_sys, MD5(product_ref), MD5(vehicle_ref)
FROM raw_prd1_vehicle_asset
WHERE product_ref <> '' AND vehicle_ref <> '';

-- -------------------------
-- Satellites
-- -------------------------

INSERT INTO sat_person (person_hash_key, load_date, tenant_id, is_lead, type, operational_paperless_consent, source_id, source_type)
SELECT MD5(party_ref), pull_ts, tenant_cd, lead_ind, party_kind, paperless_ind, src_party_ref, src_party_type
FROM raw_prd1_party_master
WHERE party_ref <> '';

INSERT INTO sat_natural_person (
    natural_person_hash_key, load_date, first_name, last_name, full_name, courtesy_title, occupation,
    birth_date, birth_year, nationality, gender, marital_status, assesed_disability_degree,
    preferred_language, role, job_title
)
SELECT MD5(natural_ref), pull_ts, given_nm, family_nm, display_nm, title_txt, occupation_txt,
       dob, birth_yr, nationality_txt, gender_txt, marital_txt, disability_degree,
       language_pref, role_txt, job_title_txt
FROM raw_prd1_party_master
WHERE natural_ref <> '';

INSERT INTO sat_legal_person (
    legal_person_hash_key, load_date, person_score, job_title, source_id, source_type,
    person_status, converted_date, date_of_constitution, company_name
)
SELECT MD5(legal_ref), pull_ts, legal_score_no, legal_job_title_txt, legal_src_ref, legal_src_type,
       legal_status_txt, lead_conv_dt, constitution_dt, legal_name
FROM raw_prd1_party_master
WHERE legal_ref <> '';

INSERT INTO sat_contact (contact_hash_key, load_date, personal_email, work_email, work_phone, home_phone)
SELECT MD5(contact_ref), pull_ts, email_home_txt, email_work_txt, phone_work_txt, phone_home_txt
FROM raw_prd1_contact_point
WHERE contact_ref <> '';

INSERT INTO sat_identities (identities_hash_key, load_date, ecid, hashed_email)
SELECT MD5(identity_ref), pull_ts, ecid_txt, hashed_email_txt
FROM raw_prd1_identity_registry
WHERE identity_ref <> '';

INSERT INTO sat_home_address (home_address_hash_key, load_date, street, postcode, city, state, country)
SELECT MD5(address_ref), pull_ts, street_txt, postal_cd, city_nm, state_cd, country_cd
FROM raw_prd1_address_book
WHERE address_ref <> '';

INSERT INTO sat_lead (lead_hash_key, load_date, interested_level, preferred_contact_method, person_score, person_status, converted_date)
SELECT MD5(lead_ref), pull_ts, interest_bucket, contact_pref, person_score_no, person_status_txt, converted_dt
FROM raw_prd1_lead_register
WHERE lead_ref <> '';

INSERT INTO sat_customer (
    customer_hash_key, load_date, customer_number, customer_status, customer_status_reason,
    customer_since, customer_rating, customer_segment, line_of_business, nps_score
)
SELECT MD5(customer_ref), pull_ts, customer_no, customer_status_txt, customer_status_reason_txt,
       customer_since_dt, customer_rating_no, customer_segment_txt, lob_txt, nps_score_no
FROM raw_prd1_customer_portfolio
WHERE customer_ref <> '';

INSERT INTO sat_consent (consent_hash_key, load_date, opt_in_validated, opt_in_legitimate_interest)
SELECT MD5(consent_ref), pull_ts, opt_in_valid_ind, opt_in_legit_ind
FROM raw_prd1_consent_snapshot
WHERE consent_ref <> '';

INSERT INTO sat_marketing_preference (
    marketing_preference_hash_key, load_date, sms, email, email_subscriptions, call, any, commercial_email, postal_mail
)
SELECT MD5(preference_ref), pull_ts, sms_ind, email_ind, email_sub_ind, call_ind, any_ind, commercial_email_ind, postal_mail_ind
FROM raw_prd1_comm_preference
WHERE preference_ref <> '';

INSERT INTO sat_marketing_engagement (marketing_engagement_hash_key, load_date, promotion_code, opened_email, marketing_status)
SELECT MD5(engagement_ref), pull_ts, promo_cd, email_opened_ind, campaign_status_txt
FROM raw_prd1_campaign_touch
WHERE engagement_ref <> '';

INSERT INTO sat_account (
    account_hash_key, load_date, account_number, account_type, account_last_access,
    account_last_change, account_creation_type, account_status
)
SELECT MD5(account_ref), pull_ts, account_no, account_type_txt, last_access_dt,
       last_change_dt, account_create_type_txt, account_status_txt
FROM raw_prd1_account_book
WHERE account_ref <> '';

INSERT INTO sat_product (product_hash_key, load_date, type)
SELECT MD5(product_ref), pull_ts, product_line
FROM raw_prd1_product_catalog
WHERE product_ref <> '';

INSERT INTO sat_quote (
    quote_hash_key, load_date, gross_revenue, net_revenue, quote_number, quote_status,
    renewal_amt_current_period, renewal_amt_next_period
)
SELECT MD5(quote_ref), pull_ts, gross_amt, net_amt, quote_no, quote_status_txt,
       renewal_amt_curr, renewal_amt_next
FROM raw_prd1_quote_register
WHERE quote_ref <> '';

INSERT INTO sat_policy (
    policy_hash_key, load_date, cover_option, declined_claims, fraud_flag, gross_revenue, net_revenue,
    number_of_active_claim, number_of_previous_claim, policy_cycle, policy_end_date, policy_length,
    policy_number, policy_start_date, policy_status, renewal_amount_current_period,
    renewal_amount_next_period, renewal_date, sales_channel
)
SELECT MD5(policy_ref), pull_ts, cover_option_txt, declined_claim_cnt, fraud_ind, gross_amt, net_amt,
       active_claim_cnt, previous_claim_cnt, policy_cycle_no, policy_end_dt, policy_term_months,
       policy_no, policy_start_dt, policy_status_txt, renewal_premium_curr,
       renewal_premium_next, renewal_dt, sales_channel_txt
FROM raw_prd1_policy_register
WHERE policy_ref <> '';

INSERT INTO sat_home (
    home_hash_key, load_date, wall_construction, home_risk_address, roof_construction,
    home_type, home_state, is_existing_home_customer
)
SELECT MD5(property_ref), pull_ts, wall_material_txt, risk_address_txt, roof_material_txt,
       property_type_txt, property_state_cd, existing_home_ind
FROM raw_prd1_property_asset
WHERE property_ref <> '';

INSERT INTO sat_motor (
    motor_hash_key, load_date, auto_decline_vehicle, body_type, fuel_type, license_status,
    is_existing_motor_customer, motor_lapsed_policies, motor_risk_address, risk_class_code,
    variant, vehicle_owner_type, vehicle_regstate, vehicle_class, vehicle_model, vehicle_type,
    motor_sum_insrd, vehicle_year, vehicle_age
)
SELECT MD5(vehicle_ref), pull_ts, auto_decline_ind, body_style_txt, fuel_type_txt, license_status_txt,
       existing_motor_ind, motor_lapse_cnt, garage_address_txt, risk_class_cd,
       variant_nm, owner_type_txt, registration_state_cd, vehicle_class_txt, model_nm, vehicle_type_txt,
       insured_value_amt, manufacture_yr, vehicle_age_yrs
FROM raw_prd1_vehicle_asset
WHERE vehicle_ref <> '';
