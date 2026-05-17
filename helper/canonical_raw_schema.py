CANONICAL_RAW_SCHEMAS = {
    "crm_person.csv": [
        "_batch_id", "_extract_ts", "_source_system", "person_id", "person_type",
        "tenant_id", "is_lead", "operational_paperless_consent", "source_id",
        "source_type", "natural_person_id", "first_name", "last_name", "full_name",
        "courtesy_title", "occupation", "birth_date", "birth_year", "nationality",
        "gender", "marital_status", "assesed_disability_degree", "preferred_language",
        "role", "job_title", "legal_person_id", "company_name", "legal_person_score",
        "legal_person_status", "legal_person_job_title", "legal_source_id",
        "legal_source_type", "date_of_constitution", "lead_converted_date",
    ],
    "crm_contact.csv": [
        "_batch_id", "_extract_ts", "_source_system", "person_id", "contact_id",
        "personal_email", "work_email", "work_phone", "home_phone",
    ],
    "crm_identity.csv": [
        "_batch_id", "_extract_ts", "_source_system", "person_id", "identities_id",
        "ecid", "hashed_email",
    ],
    "crm_person_address.csv": [
        "_batch_id", "_extract_ts", "_source_system", "person_id", "address_id",
        "street", "postcode", "city", "state", "country",
    ],
    "crm_lead.csv": [
        "_batch_id", "_extract_ts", "_source_system", "person_id", "lead_id",
        "interested_level", "preferred_contact_method", "person_score",
        "person_status", "converted_date",
    ],
    "crm_customer.csv": [
        "_batch_id", "_extract_ts", "_source_system", "person_id", "customer_id",
        "customer_number", "customer_status", "customer_status_reason",
        "customer_since", "customer_rating", "customer_segment",
        "line_of_business", "nps_score",
    ],
    "crm_customer_lead.csv": [
        "_batch_id", "_extract_ts", "_source_system", "customer_id", "lead_id",
    ],
    "crm_consent.csv": [
        "_batch_id", "_extract_ts", "_source_system", "person_id", "consent_id",
        "opt_in_validated", "opt_in_legitimate_interest",
    ],
    "crm_marketing_preference.csv": [
        "_batch_id", "_extract_ts", "_source_system", "person_id",
        "marketing_preference_id", "sms", "email", "email_subscriptions", "call",
        "any", "commercial_email", "postal_mail",
    ],
    "crm_marketing_engagement.csv": [
        "_batch_id", "_extract_ts", "_source_system", "person_id",
        "marketing_engagement_id", "promotion_code", "opened_email",
        "marketing_status",
    ],
    "crm_account.csv": [
        "_batch_id", "_extract_ts", "_source_system", "person_id", "account_id",
        "account_number", "account_type", "account_last_access",
        "account_last_change", "account_creation_type", "account_status",
    ],
    "crm_product.csv": [
        "_batch_id", "_extract_ts", "_source_system", "product_id", "product_code",
        "product_type",
    ],
    "crm_quote.csv": [
        "_batch_id", "_extract_ts", "_source_system", "person_id", "quote_id",
        "product_id", "product_code", "gross_revenue", "net_revenue", "quote_number",
        "quote_status", "renewal_amt_current_period", "renewal_amt_next_period",
    ],
    "crm_policy.csv": [
        "_batch_id", "_extract_ts", "_source_system", "person_id", "customer_id",
        "policy_id", "quote_id", "product_id", "product_code", "cover_option",
        "declined_claims", "fraud_flag", "gross_revenue", "net_revenue",
        "number_of_active_claim", "number_of_previous_claim", "policy_cycle",
        "policy_end_date", "policy_length", "policy_number", "policy_start_date",
        "policy_status", "renewal_amount_current_period",
        "renewal_amount_next_period", "renewal_date", "sales_channel",
    ],
    "crm_home.csv": [
        "_batch_id", "_extract_ts", "_source_system", "policy_id", "product_id",
        "product_code", "home_id", "wall_construction", "home_risk_address",
        "roof_construction", "home_type", "home_state",
        "is_existing_home_customer", "street", "postcode", "city", "state",
        "country",
    ],
    "crm_motor.csv": [
        "_batch_id", "_extract_ts", "_source_system", "policy_id", "product_id",
        "product_code", "motor_id", "auto_decline_vehicle", "body_type",
        "fuel_type", "license_status", "is_existing_motor_customer",
        "motor_lapsed_policies", "motor_risk_address", "risk_class_code",
        "variant", "vehicle_owner_type", "vehicle_regstate", "vehicle_class",
        "vehicle_model", "vehicle_type", "motor_sum_insrd", "vehicle_year",
        "vehicle_age",
    ],
}
