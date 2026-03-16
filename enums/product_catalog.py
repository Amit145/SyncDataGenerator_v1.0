PRODUCT_CATALOG = {
    "NATURAL": [
        {"code": "PRD_MOTOR_PERSONAL", "weight": 0.4},
        {"code": "PRD_HOME_PERSONAL", "weight": 0.3},
        {"code": "PRD_HEALTH_PERSONAL", "weight": 0.2},
        {"code": "PRD_TRAVEL", "weight": 0.1},
    ],
    "LEGAL": [
        {"code": "PRD_COMMERCIAL_MOTOR", "weight": 0.4},
        {"code": "PRD_PROPERTY_COMMERCIAL", "weight": 0.4},
        {"code": "PRD_CYBER_INSURANCE", "weight": 0.2}
    ],
}


def get_all_product_codes():
    seen = []
    for products in PRODUCT_CATALOG.values():
        for product in products:
            code = product["code"]
            if code not in seen:
                seen.append(code)
    return seen


def get_product_codes_for_person_type(person_type: str):
    return [product["code"] for product in PRODUCT_CATALOG[person_type]]


def get_product_weights_for_person_type(person_type: str):
    return [product["weight"] for product in PRODUCT_CATALOG[person_type]]
