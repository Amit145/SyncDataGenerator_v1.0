from helper.key_factory import make_business_id, md5_hasher
from enums.product_catalog import get_all_product_codes


def hub_product(hub_date, run_id):
    product_codes = get_all_product_codes()

    rows = []
    seq = 0
    product_hk_by_code = {}
    product_code_by_hk = {}

    for product_code in product_codes:
        seq += 1
        bid = make_business_id("PRD", run_id, seq)
        hk = md5_hasher(bid)

        rows.append({
            "Product Hash Key": hk,
            "Load Date": hub_date,
            "Record Source": "CRM",
            "Product Id": bid
        })

        product_hk_by_code[product_code] = hk
        product_code_by_hk[hk] = product_code

    return rows, product_hk_by_code, product_code_by_hk

