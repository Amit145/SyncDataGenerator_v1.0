from __future__ import annotations

from enums.product_catalog import get_all_product_codes
from enums.sat_enums import SAT_ENUMS


BASE_POLICY_CHANNELS = tuple(SAT_ENUMS["sat_policy"]["sales_channel"])
BASE_POLICY_STATUSES = tuple(SAT_ENUMS["sat_policy"]["policy_status"])
BASE_QUOTE_STATUSES = tuple(SAT_ENUMS["sat_quote"]["quote_status"])
BASE_CUSTOMER_STATUSES = tuple(SAT_ENUMS["sat_customer"]["customer_status"])
BASE_ACCOUNT_STATUSES = tuple(SAT_ENUMS["sat_account"]["account_status"])

PRODUCT_CODES = tuple(get_all_product_codes())

CHANNEL_TYPE_BY_NAME = {
    "ONLINE": "ONLINE",
    "AGENT": "OFFLINE",
    "BRANCH": "OFFLINE",
}

PRODUCT_FAMILY_BY_CODE = {
    "PRD_MOTOR_PERSONAL": "MOTOR",
    "PRD_HOME_PERSONAL": "HOME",
    "PRD_HEALTH_PERSONAL": "HEALTH",
    "PRD_TRAVEL": "TRAVEL",
    "PRD_COMMERCIAL_MOTOR": "MOTOR",
    "PRD_PROPERTY_COMMERCIAL": "HOME",
    "PRD_CYBER_INSURANCE": "CYBER",
}

INSURANCE_CATEGORY_BY_CODE = {
    "PRD_MOTOR_PERSONAL": "Motor",
    "PRD_HOME_PERSONAL": "Home",
    "PRD_HEALTH_PERSONAL": "Health",
    "PRD_TRAVEL": "Travel",
    "PRD_COMMERCIAL_MOTOR": "Motor",
    "PRD_PROPERTY_COMMERCIAL": "Home",
    "PRD_CYBER_INSURANCE": "Cyber",
}


def claim_product_for_code(product_code: str) -> str:
    return PRODUCT_FAMILY_BY_CODE.get(product_code, product_code)


def insurance_category_for_code(product_code: str) -> str:
    return INSURANCE_CATEGORY_BY_CODE.get(product_code, product_code)
