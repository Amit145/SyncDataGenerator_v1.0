SAT_ENUMS = {

    "sat_account": {
        "account_type": ["PERSONAL", "BUSINESS"],
        "account_creation_type": ["ONLINE", "BRANCH"],
        "account_status": ["OPEN", "SUSPENDED", "CLOSED"],
    },

    "sat_consent": {
        "opt_in_validated": ["Y", "N"],
        "opt_in_legitimate_interest": ["Y", "N"],
    },

    "sat_contact": {
    },

    "sat_customer": {
        "customer_status": ["ACTIVE", "LAPSED"],
        "customer_status_reason": ["RENEWAL", "PAYMENT", "CUSTOMER_REQUEST"],
        "customer_segment": ["STANDARD", "PREMIUM"],
        "line_of_business": ["MOTOR", "HOME"],
    },

    "sat_identities": {
    },

    "sat_lead": {
        "interested_level": ["LOW", "MEDIUM", "HIGH"],
        "preferred_contact_method": ["EMAIL", "SMS", "CALL"],
        "person_status": ["NEW", "QUALIFIED"],
    },

    "sat_legal_person": {
        "person_status": ["NEW", "QUALIFIED"],
        "source_id": ["CRM"],
        "source_type": ["INTERNAL"],
    },

    "sat_person": {
        "tenant_id": ["ALLIANZ_UK"],
        "is_lead": ["Y", "N"],
        "operational_paperless_consent": ["Y", "N"],
        "source_id": ["CRM"],
        "source_type": ["INTERNAL"],
    },

    "sat_natural_person": {
        "assesed_disability_degree": ["NONE", "LOW", "MEDIUM"],
        "preferred_language": ["EN"],
        "role": ["CUSTOMER"],
        "occupation": [
    "Technology",
    "Healthcare",
    "Finance",
    "Education",
    "Retail"
]
    },

    "sat_marketing_preference": {
        "sms": ["Y", "N"],
        "email": ["Y", "N"],
        "email_subscriptions": ["Y", "N"],
        "call": ["Y", "N"],
        "any": ["Y", "N"],
        "commercial_email": ["Y", "N"],
        "postal_mail": ["Y", "N"],
    },

    "sat_marketing_engagement": {
        "promotion_code": ["SPRING", "WELCOME", "RENEWAL"],
        "opened_email": ["Y", "N"],
        "marketing_status": ["ACTIVE", "INACTIVE"],
    },

    "sat_quote": {
        "quote_status": ["CREATED", "SENT", "EXPIRED"],
        "renewal_amt_next_period": []
    },

    "sat_policy": {
        "cover_option": ["BASIC", "FULL"],
        "fraud_flag": ["Y", "N"],
        "policy_status": ["ACTIVE", "LAPSED", "CANCELLED"],
        "sales_channel": ["ONLINE", "AGENT", "BRANCH"],
    },

    "sat_home_address": {
        "country": ["UK"],
        "street": ['New road', 'New street'],
        "state": [
            "Greater London", "West Midlands", "Greater Manchester", "Merseyside", "South Yorkshire",
            "West Yorkshire", "Tyne and Wear", "Kent", "Essex", "Surrey", "Hampshire", "Devon",
            "Cornwall", "Somerset", "Dorset", "Wiltshire", "Gloucestershire", "Oxfordshire",
            "Cambridgeshire", "Norfolk", "Suffolk", "Lincolnshire", "Nottinghamshire", "Derbyshire",
            "Leicestershire", "Northamptonshire", "Warwickshire", "Staffordshire", "Cheshire",
            "Lancashire", "Cumbria", "North Yorkshire", "East Sussex", "West Sussex",
            "Buckinghamshire", "Berkshire", "Hertfordshire"
        ],
    },

    "sat_home": {
        "wall_construction": ["BRICK", "CONCRETE"],
        "roof_construction": ["TILE", "METAL"],
        "home_type": ["HOUSE", "FLAT"],
        "is_existing_home_customer": ["Y", "N"],
    },

    "sat_motor": {
        "auto_decline_vehicle": ["Y", "N"],
        "body_type": ["SEDAN", "SUV", "HATCH"],
        "fuel_type": ["PETROL", "DIESEL", "EV"],
        "license_status": ["VALID", "EXPIRED"],
        "is_existing_motor_customer": ["Y", "N"],
        "risk_class_code": ["LOW", "MEDIUM", "HIGH"],
        "variant": ["BASE", "SPORT"],
        "vehicle_owner_type": ["SELF", "COMPANY"],
        "vehicle_class": ["PRIVATE", "COMMERCIAL"],
        "vehicle_model": ["Focus", "Corsa", "3 Series", "A3", "Corolla", "Qashqai"],
        "vehicle_type": ["CAR", "BIKE"],
    },

    "sat_product": {
    },
}