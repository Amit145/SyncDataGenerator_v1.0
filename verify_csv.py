import os
import sys
import calendar
from decimal import Decimal, ROUND_HALF_UP
import pandas as pd


def read_csv_safe(base_path: str, file_name: str) -> pd.DataFrame:
    path = os.path.join(base_path, file_name)
    if not os.path.exists(path):
        print(f"missing file: {file_name}")
        return pd.DataFrame()

    df = pd.read_csv(path)
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df


def check_exists(df: pd.DataFrame, file_name: str):
    if df.empty and len(df.columns) == 0:
        raise ValueError(f"missing required file: {file_name}")


def assert_unique(df: pd.DataFrame, col: str, label: str):
    if col not in df.columns:
        print(f"{label}: missing column {col}")
        return False

    total = len(df)
    uniq = df[col].nunique(dropna=True)

    if total != uniq:
        print(f"{label}: {col} not unique | rows={total}, unique={uniq}")
        return False

    print(f"{label}: {col} unique | rows={total}")
    return True


def value_counts_map(df: pd.DataFrame, key_col: str) -> pd.Series:
    if df.empty or key_col not in df.columns:
        return pd.Series(dtype="int64")
    return df.groupby(key_col).size()


def check_exactly_one(link_df: pd.DataFrame, key_col: str, expected_keys: set, label: str):
    counts = value_counts_map(link_df, key_col)
    bad = []

    for k in expected_keys:
        c = int(counts.get(k, 0))
        if c != 1:
            bad.append((k, c))

    if bad:
        print(f"{label}: expected exactly 1, failed={len(bad)}")
        print("sample:", bad[:10])
        return False

    print(f"{label}: exactly 1 for all {len(expected_keys)} keys")
    return True


def check_zero_to_n(link_df: pd.DataFrame, key_col: str, expected_keys: set, min_v: int, max_v: int, label: str):
    counts = value_counts_map(link_df, key_col)
    bad = []

    for k in expected_keys:
        c = int(counts.get(k, 0))
        if not (min_v <= c <= max_v):
            bad.append((k, c))

    if bad:
        print(f"{label}: expected [{min_v},{max_v}], failed={len(bad)}")
        print("sample:", bad[:10])
        return False

    print(f"{label}: counts within [{min_v},{max_v}] for {len(expected_keys)}")
    return True


def check_subset(child_keys: set, parent_keys: set, label: str):
    missing = child_keys - parent_keys

    if missing:
        print(f"{label}: {len(missing)} keys not in parent")
        print("sample:", list(missing)[:10])
        return False

    print(f"{label}: all child keys exist in parent")
    return True


def check_fk(child_df, child_col, parent_df, parent_col, label):
    if child_df.empty:
        print(f"{label}: skipped (no rows)")
        return True

    if child_col not in child_df.columns:
        print(f"{label}: missing child column {child_col}")
        return False

    if parent_col not in parent_df.columns:
        print(f"{label}: missing parent column {parent_col}")
        return False

    child_keys = set(child_df[child_col])
    parent_keys = set(parent_df[parent_col])

    missing = child_keys - parent_keys

    if missing:
        print(f"{label}: {len(missing)} invalid references")
        print("sample:", list(missing)[:10])
        return False

    print(f"{label}: valid")
    return True


def parse_dt(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series(dtype="datetime64[ns]")
    return pd.to_datetime(series, errors="coerce")


def first_load_date(df: pd.DataFrame):
    if df.empty or "load_date" not in df.columns:
        return pd.NaT
    parsed = parse_dt(df["load_date"])
    valid = parsed.dropna()
    if valid.empty:
        return pd.NaT
    return valid.iloc[0]


def add_one_year(ts: pd.Timestamp) -> pd.Timestamp:
    if pd.isna(ts):
        return pd.NaT
    try:
        return ts.replace(year=ts.year + 1)
    except ValueError:
        last_day = calendar.monthrange(ts.year + 1, ts.month)[1]
        return ts.replace(year=ts.year + 1, day=min(ts.day, last_day))


def currency_round(value) -> Decimal | None:
    if pd.isna(value):
        return None
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def main(base_path: str):
    # ---------- READ FILES ----------
    hub_person = read_csv_safe(base_path, "hub_person.csv")
    hub_nat = read_csv_safe(base_path, "hub_natural_person.csv")
    hub_leg = read_csv_safe(base_path, "hub_legal_person.csv")
    hub_contact = read_csv_safe(base_path, "hub_contact.csv")
    hub_identity = read_csv_safe(base_path, "hub_identities.csv")
    hub_address = read_csv_safe(base_path, "hub_home_address.csv")
    hub_consent = read_csv_safe(base_path, "hub_consent.csv")
    hub_mpr = read_csv_safe(base_path, "hub_marketing_preference.csv")
    hub_men = read_csv_safe(base_path, "hub_marketing_engagement.csv")
    hub_lead = read_csv_safe(base_path, "hub_lead.csv")
    hub_quote = read_csv_safe(base_path, "hub_quote.csv")
    hub_policy = read_csv_safe(base_path, "hub_policy.csv")
    hub_account = read_csv_safe(base_path, "hub_account.csv")
    hub_customer = read_csv_safe(base_path, "hub_customer.csv")
    hub_product = read_csv_safe(base_path, "hub_product.csv")
    hub_motor = read_csv_safe(base_path, "hub_motor.csv")
    hub_home = read_csv_safe(base_path, "hub_home.csv")

    sat_person = read_csv_safe(base_path, "sat_person.csv")
    sat_lead = read_csv_safe(base_path, "sat_lead.csv")
    sat_policy = read_csv_safe(base_path, "sat_policy.csv")
    sat_product = read_csv_safe(base_path, "sat_product.csv")
    sat_account = read_csv_safe(base_path, "sat_account.csv")
    sat_customer = read_csv_safe(base_path, "sat_customer.csv")
    sat_legal_person = read_csv_safe(base_path, "sat_legal_person.csv")
    sat_natural_person = read_csv_safe(base_path, "sat_natural_person.csv")

    l_p_nat = read_csv_safe(base_path, "link_person_natural_person.csv")
    l_p_leg = read_csv_safe(base_path, "link_person_legal_person.csv")
    l_p_contact = read_csv_safe(base_path, "link_person_contact.csv")
    l_p_identity = read_csv_safe(base_path, "link_person_identities.csv")
    l_p_address = read_csv_safe(base_path, "link_person_home_address.csv")
    l_p_consent = read_csv_safe(base_path, "link_person_consent.csv")
    l_p_lead = read_csv_safe(base_path, "link_person_lead.csv")
    l_p_account = read_csv_safe(base_path, "link_person_account.csv")
    l_p_mpr = read_csv_safe(base_path, "link_person_marketing_preference.csv")
    l_p_men = read_csv_safe(base_path, "link_person_marketing_engagement.csv")

    l_q_person = read_csv_safe(base_path, "link_quote_person.csv")
    l_q_product = read_csv_safe(base_path, "link_quote_product.csv")

    l_c_person = read_csv_safe(base_path, "link_customer_person.csv")
    l_c_lead = read_csv_safe(base_path, "link_customer_lead.csv")

    l_pol_customer = read_csv_safe(base_path, "link_policy_customer.csv")
    l_pol_product = read_csv_safe(base_path, "link_policy_product.csv")

    l_prod_motor = read_csv_safe(base_path, "link_product_motor.csv")
    l_prod_home = read_csv_safe(base_path, "link_product_home.csv")

    # ---------- REQUIRED FILE CHECK ----------
    required_files = {
        "hub_person.csv": hub_person,
        "hub_natural_person.csv": hub_nat,
        "hub_legal_person.csv": hub_leg,
        "hub_contact.csv": hub_contact,
        "hub_identities.csv": hub_identity,
        "hub_home_address.csv": hub_address,
        "link_person_contact.csv": l_p_contact,
        "link_person_identities.csv": l_p_identity,
        "link_person_home_address.csv": l_p_address,
        "link_quote_person.csv": l_q_person,
        "link_quote_product.csv": l_q_product,
        "link_policy_customer.csv": l_pol_customer,
        "link_policy_product.csv": l_pol_product,
    }

    for file_name, df in required_files.items():
        check_exists(df, file_name)

    print("\n===== BASIC UNIQUENESS =====\n")

    assert_unique(hub_person, "person_hash_key", "hub_person")
    assert_unique(hub_nat, "natural_person_hash_key", "hub_natural_person")
    assert_unique(hub_leg, "legal_person_hash_key", "hub_legal_person")
    assert_unique(hub_contact, "contact_hash_key", "hub_contact")
    assert_unique(hub_identity, "identities_hash_key", "hub_identities")
    assert_unique(hub_address, "home_address_hash_key", "hub_home_address")

    if not hub_lead.empty:
        assert_unique(hub_lead, "lead_hash_key", "hub_lead")

    if not hub_quote.empty:
        assert_unique(hub_quote, "quote_hash_key", "hub_quote")

    if not hub_policy.empty:
        assert_unique(hub_policy, "policy_hash_key", "hub_policy")

    if not hub_account.empty:
        assert_unique(hub_account, "account_hash_key", "hub_account")

    if not hub_customer.empty:
        assert_unique(hub_customer, "customer_hash_key", "hub_customer")

    if not hub_product.empty and "product_hash_key" in hub_product.columns:
        assert_unique(hub_product, "product_hash_key", "hub_product")

    if not hub_motor.empty and "motor_hash_key" in hub_motor.columns:
        assert_unique(hub_motor, "motor_hash_key", "hub_motor")

    if not hub_home.empty and "home_hash_key" in hub_home.columns:
        assert_unique(hub_home, "home_hash_key", "hub_home")

    print("\n===== PERSON RULES =====\n")

    person_keys = set(hub_person["person_hash_key"])

    nat_persons = set(l_p_nat["person_hash_key"]) if "person_hash_key" in l_p_nat.columns else set()
    leg_persons = set(l_p_leg["person_hash_key"]) if "person_hash_key" in l_p_leg.columns else set()

    both = nat_persons & leg_persons
    neither = person_keys - (nat_persons | leg_persons)

    if both:
        print(f"person_type error: {len(both)} both natural and legal")
        print("sample:", list(both)[:10])
    elif neither:
        print(f"person_type error: {len(neither)} neither natural nor legal")
        print("sample:", list(neither)[:10])
    else:
        print("person_type valid")

    check_exactly_one(l_p_contact, "person_hash_key", person_keys, "person_contact")
    check_exactly_one(l_p_identity, "person_hash_key", person_keys, "person_identity")
    check_exactly_one(l_p_address, "person_hash_key", person_keys, "person_home_address")

    check_zero_to_n(l_p_consent, "person_hash_key", person_keys, 0, 1, "person_consent")
    check_zero_to_n(l_p_lead, "person_hash_key", person_keys, 0, 2, "person_lead")
    check_zero_to_n(l_p_account, "person_hash_key", person_keys, 0, 1, "person_account")

    lead_persons = set(l_p_lead["person_hash_key"]) if "person_hash_key" in l_p_lead.columns else set()
    consent_persons = set(l_p_consent["person_hash_key"]) if "person_hash_key" in l_p_consent.columns else set()

    print("\n===== SAT_PERSON RULES =====\n")

    if sat_person.empty:
        print("sat_person_rules: skipped (sat_person.csv missing or empty)")
    else:
        required_sat_person_cols = {"person_hash_key", "is_lead", "operational_paperless_consent"}
        missing_sat_person_cols = required_sat_person_cols - set(sat_person.columns)

        if missing_sat_person_cols:
            print(f"sat_person_rules: missing columns {sorted(missing_sat_person_cols)}")
        else:
            sat_person_flags = sat_person[[
                "person_hash_key",
                "is_lead",
                "operational_paperless_consent",
            ]].copy()

            sat_person_flags["is_lead"] = sat_person_flags["is_lead"].astype(str).str.upper().str.strip()
            sat_person_flags["operational_paperless_consent"] = (
                sat_person_flags["operational_paperless_consent"].astype(str).str.upper().str.strip()
            )

            actual_lead_flags = {p: ("Y" if p in lead_persons else "N") for p in person_keys}
            actual_consent_flags = {p: ("Y" if p in consent_persons else "N") for p in person_keys}

            bad_is_lead = sat_person_flags[
                sat_person_flags["person_hash_key"].map(actual_lead_flags) != sat_person_flags["is_lead"]
            ]
            if not bad_is_lead.empty:
                print(f"sat_person_is_lead error: {len(bad_is_lead)} mismatches")
                print("sample:", bad_is_lead[["person_hash_key", "is_lead"]].head(10).to_dict("records"))
            else:
                print("sat_person_is_lead valid")

            bad_consent = sat_person_flags[
                sat_person_flags["person_hash_key"].map(actual_consent_flags)
                != sat_person_flags["operational_paperless_consent"]
            ]
            if not bad_consent.empty:
                print(f"sat_person_operational_paperless_consent error: {len(bad_consent)} mismatches")
                print(
                    "sample:",
                    bad_consent[["person_hash_key", "operational_paperless_consent"]].head(10).to_dict("records")
                )
            else:
                print("sat_person_operational_paperless_consent valid")

    print("\n===== QUOTE RULES =====\n")

    quote_keys = set(hub_quote["quote_hash_key"]) if "quote_hash_key" in hub_quote.columns else set()

    check_exactly_one(l_q_person, "quote_hash_key", quote_keys, "quote_person")
    check_exactly_one(l_q_product, "quote_hash_key", quote_keys, "quote_product")

    print("\n===== POLICY RULES =====\n")

    policy_keys = set(hub_policy["policy_hash_key"]) if "policy_hash_key" in hub_policy.columns else set()

    check_exactly_one(l_pol_customer, "policy_hash_key", policy_keys, "policy_customer")
    check_exactly_one(l_pol_product, "policy_hash_key", policy_keys, "policy_product")

    policy_customer_keys = set(l_pol_customer["customer_hash_key"]) if "customer_hash_key" in l_pol_customer.columns else set()
    customer_keys = set(hub_customer["customer_hash_key"]) if "customer_hash_key" in hub_customer.columns else set()

    check_subset(policy_customer_keys, customer_keys, "policy_customer_exists")

    print("\n===== LEAD DEPENDENCY RULES =====\n")

    mpr_persons = set(l_p_mpr["person_hash_key"]) if "person_hash_key" in l_p_mpr.columns else set()
    men_persons = set(l_p_men["person_hash_key"]) if "person_hash_key" in l_p_men.columns else set()

    missing_consent = lead_persons - consent_persons
    missing_mpr = lead_persons - mpr_persons
    missing_men = lead_persons - men_persons

    if missing_consent:
        print(f"lead_consent error: missing for {len(missing_consent)} lead persons")
        print("sample:", list(missing_consent)[:10])
    else:
        print("lead_consent valid")

    if missing_mpr:
        print(f"lead_mpr error: missing for {len(missing_mpr)} lead persons")
        print("sample:", list(missing_mpr)[:10])
    else:
        print("lead_mpr valid")

    if missing_men:
        print(f"lead_men error: missing for {len(missing_men)} lead persons")
        print("sample:", list(missing_men)[:10])
    else:
        print("lead_men valid")

    print("\n===== QUOTE SOURCE RULE =====\n")

    quote_persons = set(l_q_person["person_hash_key"]) if "person_hash_key" in l_q_person.columns else set()
    invalid_quote_persons = quote_persons - lead_persons

    if invalid_quote_persons:
        print(f"quote_source error: {len(invalid_quote_persons)} quote persons are not leads")
        print("sample:", list(invalid_quote_persons)[:10])
    else:
        print("quote_source valid")

    print("\n===== POLICY HOLDER RULES =====\n")

    customer_person_keys = set(l_c_person["person_hash_key"]) if "person_hash_key" in l_c_person.columns else set()
    account_person_keys = set(l_p_account["person_hash_key"]) if "person_hash_key" in l_p_account.columns else set()

    policy_holder_persons = set()
    if (
        "customer_hash_key" in l_pol_customer.columns
        and "customer_hash_key" in l_c_person.columns
        and "person_hash_key" in l_c_person.columns
    ):
        cust_to_person = dict(zip(l_c_person["customer_hash_key"], l_c_person["person_hash_key"]))
        for cust_hk in l_pol_customer["customer_hash_key"]:
            person_hk = cust_to_person.get(cust_hk)
            if person_hk:
                policy_holder_persons.add(person_hk)

    missing_customer = policy_holder_persons - customer_person_keys
    missing_account = policy_holder_persons - account_person_keys

    if missing_customer:
        print(f"policy_holder_customer error: missing for {len(missing_customer)} persons")
        print("sample:", list(missing_customer)[:10])
    else:
        print("policy_holder_customer valid")

    if missing_account:
        print(f"policy_holder_account error: missing for {len(missing_account)} persons")
        print("sample:", list(missing_account)[:10])
    else:
        print("policy_holder_account valid")

    print("\n===== ACCOUNT LIFECYCLE RULES =====\n")

    if sat_account.empty:
        print("account_lifecycle: skipped (sat_account.csv missing or empty)")
    else:
        required_account_cols = {
            "account_hash_key", "account_status", "account_last_access", "account_last_change", "load_date"
        }
        if not required_account_cols.issubset(sat_account.columns):
            print(f"account_lifecycle: missing columns {sorted(required_account_cols - set(sat_account.columns))}")
        else:
            acc = sat_account[[
                "account_hash_key", "account_status", "account_last_access", "account_last_change", "load_date"
            ]].copy()
            acc["account_last_access"] = parse_dt(acc["account_last_access"])
            acc["account_last_change"] = parse_dt(acc["account_last_change"])
            acc["load_date"] = parse_dt(acc["load_date"])

            bad_access_load = acc[
                (acc["account_last_access"] > acc["load_date"]) | (acc["account_last_change"] > acc["load_date"])
            ]
            if not bad_access_load.empty:
                print(f"account_dates_before_load error: {len(bad_access_load)} rows have account dates after load")
                print(
                    "sample:",
                    bad_access_load[["account_hash_key", "account_status", "account_last_access", "account_last_change", "load_date"]]
                    .head(10).to_dict("records")
                )
            else:
                print("account_dates_before_load valid")

            open_bad = acc[
                (acc["account_status"] == "OPEN") & (acc["account_last_access"] < acc["account_last_change"])
            ]
            if not open_bad.empty:
                print(f"account_open_timeline error: {len(open_bad)} OPEN rows have last_access before last_change")
                print("sample:", open_bad[["account_hash_key", "account_last_access", "account_last_change"]].head(10).to_dict("records"))
            else:
                print("account_open_timeline valid")

            closed_bad = acc[
                (acc["account_status"].isin(["SUSPENDED", "CLOSED"])) & (acc["account_last_access"] > acc["account_last_change"])
            ]
            if not closed_bad.empty:
                print(f"account_non_open_timeline error: {len(closed_bad)} non-open rows have last_access after last_change")
                print("sample:", closed_bad[["account_hash_key", "account_status", "account_last_access", "account_last_change"]].head(10).to_dict("records"))
            else:
                print("account_non_open_timeline valid")

            if {"account_hash_key", "person_hash_key"}.issubset(l_p_account.columns) and {"policy_hash_key", "customer_hash_key"}.issubset(l_pol_customer.columns):
                policy_persons = (
                    l_pol_customer[["policy_hash_key", "customer_hash_key"]]
                    .merge(l_c_person[["customer_hash_key", "person_hash_key"]], on="customer_hash_key", how="left")
                )
                person_accounts = l_p_account[["person_hash_key", "account_hash_key"]].merge(
                    acc[["account_hash_key", "account_status"]], on="account_hash_key", how="left"
                )
                policy_account = policy_persons.merge(person_accounts, on="person_hash_key", how="left").merge(
                    sat_policy[["policy_hash_key", "policy_status"]], on="policy_hash_key", how="left"
                )
                bad_policy_account = policy_account[
                    policy_account["account_status"].isin(["SUSPENDED", "CLOSED"])
                    & (policy_account["policy_status"] == "ACTIVE")
                ]
                if not bad_policy_account.empty:
                    print(f"account_policy_status error: {len(bad_policy_account)} policies are ACTIVE on non-open accounts")
                    print("sample:", bad_policy_account[["policy_hash_key", "person_hash_key", "account_status", "policy_status"]].head(10).to_dict("records"))
                else:
                    print("account_policy_status valid")

    print("\n===== PRODUCT ASSET RULES =====\n")

    check_fk(
        l_prod_motor,
        "product_hash_key",
        hub_product,
        "product_hash_key",
        "product_motor_product_fk"
    )

    check_fk(
        l_prod_motor,
        "motor_hash_key",
        hub_motor,
        "motor_hash_key",
        "product_motor_motor_fk"
    )

    check_fk(
        l_prod_home,
        "product_hash_key",
        hub_product,
        "product_hash_key",
        "product_home_product_fk"
    )

    check_fk(
        l_prod_home,
        "home_hash_key",
        hub_home,
        "home_hash_key",
        "product_home_home_fk"
    )

    print("\n===== POLICY -> ASSET SANITY =====\n")

    policy_product_map = {}
    if not l_pol_product.empty and {"policy_hash_key", "product_hash_key"}.issubset(l_pol_product.columns):
        policy_product_map = dict(zip(l_pol_product["policy_hash_key"], l_pol_product["product_hash_key"]))

    product_to_motor = set(l_prod_motor["product_hash_key"]) if "product_hash_key" in l_prod_motor.columns else set()
    product_to_home = set(l_prod_home["product_hash_key"]) if "product_hash_key" in l_prod_home.columns else set()

    bad_motor = []
    bad_home = []

    for policy_hk, product_hk in policy_product_map.items():
        prod_row = sat_product[sat_product["product_hash_key"] == product_hk]
        if prod_row.empty:
            continue

        product_id_col = "type" if "type" in prod_row.columns else None
        if not product_id_col:
            continue

        product_id = str(prod_row.iloc[0][product_id_col]).upper()

        # product-level sanity, not per-policy asset-instance sanity
        if "MOTOR" in product_id:
            if len(product_to_motor) == 0:
                bad_motor.append((policy_hk, product_hk, product_id))

        if ("HOME" in product_id) or ("PROPERTY" in product_id):
            if len(product_to_home) == 0:
                bad_home.append((policy_hk, product_hk, product_id))

    if bad_motor:
        print(f"policy_motor_asset error: {len(bad_motor)} motor-like policies exist but no product_motor links exist at all")
        print("sample:", bad_motor[:10])
    else:
        print("policy_motor_asset valid")

    if bad_home:
        print(f"policy_home_asset error: {len(bad_home)} home/property-like policies exist but no product_home links exist at all")
        print("sample:", bad_home[:10])
    else:
        print("policy_home_asset valid")

    print("\n===== TIMELINE RULES =====\n")

    hub_load_dt = first_load_date(hub_person)
    link_load_dt = first_load_date(l_p_lead if not l_p_lead.empty else l_q_person)
    sat_load_dt = first_load_date(sat_person if not sat_person.empty else sat_policy)

    if pd.isna(hub_load_dt) or pd.isna(link_load_dt) or pd.isna(sat_load_dt):
        print("load_date_sequence: skipped (missing/parsing issue in hub/link/sat load_date)")
    elif not (hub_load_dt < link_load_dt < sat_load_dt):
        print(
            "load_date_sequence error:",
            {
                "hub_load_date": str(hub_load_dt),
                "link_load_date": str(link_load_dt),
                "sat_load_date": str(sat_load_dt),
            },
        )
    else:
        print("load_date_sequence valid")

    def check_date_not_after_load(df: pd.DataFrame, date_col: str, load_col: str, label: str):
        if df.empty:
            print(f"{label}: skipped (missing or empty)")
            return
        if date_col not in df.columns or load_col not in df.columns:
            print(f"{label}: skipped (missing columns)")
            return
        left = parse_dt(df[date_col])
        right = parse_dt(df[load_col])
        bad = df[left > right].copy()
        if not bad.empty:
            print(f"{label} error: {len(bad)} rows have {date_col} > {load_col}")
            cols = [c for c in ["lead_hash_key", "policy_hash_key", "customer_hash_key", "legal_person_hash_key", "natural_person_hash_key", date_col, load_col] if c in bad.columns]
            print("sample:", bad[cols].head(10).to_dict("records"))
        else:
            print(f"{label} valid")

    check_date_not_after_load(sat_lead, "converted_date", "load_date", "lead_converted_before_load")
    check_date_not_after_load(sat_policy, "policy_start_date", "load_date", "policy_start_before_load")
    check_date_not_after_load(sat_customer, "customer_since", "load_date", "customer_since_before_load")
    check_date_not_after_load(sat_legal_person, "converted_date", "load_date", "legal_person_converted_before_load")
    check_date_not_after_load(sat_legal_person, "date_of_constitution", "load_date", "constitution_before_load")
    check_date_not_after_load(sat_natural_person, "birth_date", "load_date", "birth_date_before_load")

    if sat_lead.empty or sat_policy.empty:
        print("timeline_rules: skipped (sat_lead.csv or sat_policy.csv missing/empty)")
    else:
        lead_required = {"lead_hash_key", "converted_date"}
        policy_required = {"policy_hash_key", "policy_start_date", "policy_end_date"}
        if not lead_required.issubset(sat_lead.columns):
            print(f"timeline_rules: sat_lead missing columns {sorted(lead_required - set(sat_lead.columns))}")
        elif not policy_required.issubset(sat_policy.columns):
            print(f"timeline_rules: sat_policy missing columns {sorted(policy_required - set(sat_policy.columns))}")
        elif not {"person_hash_key", "lead_hash_key"}.issubset(l_p_lead.columns):
            print("timeline_rules: link_person_lead missing required columns")
        elif not {"customer_hash_key", "person_hash_key"}.issubset(l_c_person.columns):
            print("timeline_rules: link_customer_person missing required columns")
        elif not {"policy_hash_key", "customer_hash_key"}.issubset(l_pol_customer.columns):
            print("timeline_rules: link_policy_customer missing required columns")
        else:
            lead_dates = sat_lead[["lead_hash_key", "converted_date"]].copy()
            lead_dates["converted_date"] = parse_dt(lead_dates["converted_date"])

            policy_dates = sat_policy[["policy_hash_key", "policy_start_date", "policy_end_date"]].copy()
            policy_dates["policy_start_date"] = parse_dt(policy_dates["policy_start_date"])
            policy_dates["policy_end_date"] = parse_dt(policy_dates["policy_end_date"])

            latest_lead_by_person = (
                l_p_lead[["person_hash_key", "lead_hash_key"]]
                .merge(lead_dates, on="lead_hash_key", how="left")
                .groupby("person_hash_key", as_index=False)["converted_date"]
                .max()
            )

            policy_person_dates = (
                l_pol_customer[["policy_hash_key", "customer_hash_key"]]
                .merge(l_c_person[["customer_hash_key", "person_hash_key"]], on="customer_hash_key", how="left")
                .merge(policy_dates, on="policy_hash_key", how="left")
                .merge(latest_lead_by_person, on="person_hash_key", how="left")
            )

            bad_policy_order = policy_person_dates[
                policy_person_dates["policy_start_date"] > policy_person_dates["policy_end_date"]
            ]
            if not bad_policy_order.empty:
                print(f"policy_start_end error: {len(bad_policy_order)} rows with policy_start_date > policy_end_date")
                print(
                    "sample:",
                    bad_policy_order[["policy_hash_key", "policy_start_date", "policy_end_date"]]
                    .head(10)
                    .to_dict("records")
                )
            else:
                print("policy_start_end valid")

            if {"policy_hash_key", "policy_status", "renewal_date", "renewal_amount_current_period", "renewal_amount_next_period", "load_date"}.issubset(sat_policy.columns):
                policy_meta = sat_policy[[
                    "policy_hash_key",
                    "policy_status",
                    "load_date",
                    "renewal_date",
                    "renewal_amount_current_period",
                    "renewal_amount_next_period",
                ]].copy()
                policy_meta["load_date"] = parse_dt(policy_meta["load_date"])
                policy_meta["renewal_date"] = parse_dt(policy_meta["renewal_date"])
                policy_person_dates = policy_person_dates.merge(policy_meta, on="policy_hash_key", how="left")

                non_cancelled = policy_person_dates[policy_person_dates["policy_status"] != "CANCELLED"].copy()
                non_cancelled["expected_policy_end_date"] = non_cancelled["policy_start_date"].apply(add_one_year)
                bad_annual_duration = non_cancelled[
                    non_cancelled["policy_start_date"].notna()
                    & non_cancelled["policy_end_date"].notna()
                    & (non_cancelled["policy_end_date"] != non_cancelled["expected_policy_end_date"])
                ]
                if not bad_annual_duration.empty:
                    print(f"policy_annual_duration error: {len(bad_annual_duration)} non-cancelled rows are not exactly 1 year")
                    print(
                        "sample:",
                        bad_annual_duration[["policy_hash_key", "policy_start_date", "policy_end_date", "expected_policy_end_date"]]
                        .head(10)
                        .to_dict("records")
                    )
                else:
                    print("policy_annual_duration valid")

                renewal_delta_days = (
                    (policy_person_dates["policy_end_date"] - policy_person_dates["renewal_date"]).dt.total_seconds() / 86400.0
                )
                bad_renewal_window = policy_person_dates[
                    policy_person_dates["renewal_date"].notna()
                    & policy_person_dates["policy_end_date"].notna()
                    & ((renewal_delta_days < 0) | (renewal_delta_days > 10))
                ]
                if not bad_renewal_window.empty:
                    print(f"policy_renewal_window error: {len(bad_renewal_window)} rows outside 0-10 day window")
                    print(
                        "sample:",
                        bad_renewal_window[["policy_hash_key", "renewal_date", "policy_end_date"]]
                        .head(10)
                        .to_dict("records")
                    )
                else:
                    print("policy_renewal_window valid")

                expected_next = policy_person_dates["renewal_amount_current_period"].apply(
                    lambda value: currency_round(Decimal(str(value)) * Decimal("1.01")) if not pd.isna(value) else None
                )
                actual_next = policy_person_dates["renewal_amount_next_period"].apply(currency_round)
                bad_renewal_uplift = policy_person_dates[
                    policy_person_dates["renewal_amount_current_period"].notna()
                    & policy_person_dates["renewal_amount_next_period"].notna()
                    & (actual_next != expected_next)
                ]
                if not bad_renewal_uplift.empty:
                    print(f"policy_renewal_uplift error: {len(bad_renewal_uplift)} rows do not have 1% uplift")
                    print(
                        "sample:",
                        bad_renewal_uplift[["policy_hash_key", "renewal_amount_current_period", "renewal_amount_next_period"]]
                        .head(10)
                        .to_dict("records")
                    )
                else:
                    print("policy_renewal_uplift valid")

                bad_status = policy_person_dates[
                    policy_person_dates["policy_end_date"].notna()
                    & policy_person_dates["load_date"].notna()
                    & (
                        ((policy_person_dates["policy_status"] == "ACTIVE") & (policy_person_dates["policy_end_date"] <= policy_person_dates["load_date"]))
                        | ((policy_person_dates["policy_status"] == "LAPSED") & (policy_person_dates["policy_end_date"] > policy_person_dates["load_date"]))
                    )
                ]
                if not bad_status.empty:
                    print(f"policy_status_timeline error: {len(bad_status)} rows have status/date mismatch")
                    print(
                        "sample:",
                        bad_status[["policy_hash_key", "policy_status", "policy_end_date", "load_date"]]
                        .head(10)
                        .to_dict("records")
                    )
                else:
                    print("policy_status_timeline valid")

            bad_lead_policy_order = policy_person_dates[
                policy_person_dates["converted_date"].notna()
                & policy_person_dates["policy_start_date"].notna()
                & (policy_person_dates["converted_date"] >= policy_person_dates["policy_start_date"])
            ]
            if not bad_lead_policy_order.empty:
                print(
                    f"lead_to_policy_timeline error: {len(bad_lead_policy_order)} rows with "
                    f"converted_date >= policy_start_date"
                )
                print(
                    "sample:",
                    bad_lead_policy_order[["person_hash_key", "policy_hash_key", "converted_date", "policy_start_date"]]
                    .head(10)
                    .to_dict("records")
                )
            else:
                print("lead_to_policy_timeline valid")

            lead_policy_days = (
                (policy_person_dates["policy_start_date"] - policy_person_dates["converted_date"]).dt.total_seconds() / 86400.0
            )
            bad_lead_policy_window = policy_person_dates[
                policy_person_dates["converted_date"].notna()
                & policy_person_dates["policy_start_date"].notna()
                & ((lead_policy_days < 1) | (lead_policy_days > 30))
            ]
            if not bad_lead_policy_window.empty:
                print(f"lead_to_policy_window error: {len(bad_lead_policy_window)} rows outside 1-30 days")
                print(
                    "sample:",
                    bad_lead_policy_window[["person_hash_key", "policy_hash_key", "converted_date", "policy_start_date"]]
                    .head(10)
                    .to_dict("records")
                )
            else:
                print("lead_to_policy_window valid")


    print("\n===== SUMMARY =====\n")

    print(f"hub_person rows: {len(hub_person)}")
    print(f"hub_natural_person rows: {len(hub_nat)}")
    print(f"hub_legal_person rows: {len(hub_leg)}")
    print(f"hub_contact rows: {len(hub_contact)}")
    print(f"hub_identities rows: {len(hub_identity)}")
    print(f"hub_home_address rows: {len(hub_address)}")
    print(f"hub_consent rows: {len(hub_consent)}")
    print(f"hub_marketing_preference rows: {len(hub_mpr)}")
    print(f"hub_marketing_engagement rows: {len(hub_men)}")
    print(f"hub_lead rows: {len(hub_lead)}")
    print(f"hub_quote rows: {len(hub_quote)}")
    print(f"hub_policy rows: {len(hub_policy)}")
    print(f"hub_account rows: {len(hub_account)}")
    print(f"hub_customer rows: {len(hub_customer)}")
    print(f"hub_product rows: {len(hub_product)}")
    print(f"hub_motor rows: {len(hub_motor)}")
    print(f"hub_home rows: {len(hub_home)}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_path = sys.argv[1]
    else:
        target_path = "synthetic_data/20260316_103504_42"
    main(target_path)
