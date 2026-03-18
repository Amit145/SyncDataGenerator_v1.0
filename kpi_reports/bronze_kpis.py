import os

import pandas as pd


BASE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "output")


def latest_run():
    return max(
        (os.path.join(BASE, d) for d in os.listdir(BASE) if os.path.isdir(os.path.join(BASE, d))),
        key=os.path.getmtime,
    )


def _read(run_dir, name):
    path = os.path.join(run_dir, name)
    if not os.path.exists(path):
        return None
    return pd.read_csv(path)


def main():
    run = latest_run()
    print("Using:", run)

    hub_person = _read(run, "Hub_Person.csv")
    hub_customer = _read(run, "Hub_Customer.csv")
    hub_quote = _read(run, "Hub_Quote.csv")
    hub_policy = _read(run, "Hub_Policy.csv")
    sat_lead = _read(run, "Sat_Lead.csv")
    sat_policy = _read(run, "Sat_Policy.csv")
    sat_product = _read(run, "Sat_Product.csv")

    l_person_lead = _read(run, "Link_Person_Lead.csv")
    l_customer_person = _read(run, "Link_Customer_Person.csv")
    l_policy_customer = _read(run, "Link_Policy_Customer.csv")
    l_policy_product = _read(run, "Link_Policy_Product.csv")

    l_person_consent = _read(run, "Link_Person_Consent.csv")
    l_person_mkt = _read(run, "Link_Person_Marketing_Engagement.csv")

    total_people = len(hub_person)

    lead_people = set(l_person_lead["Person Hash Key"])
    customer_people = set(l_customer_person["Person Hash Key"])
    converted = lead_people.intersection(customer_people)

    lead_conv = (len(converted) / max(1, len(lead_people))) * 100

    quotes = len(hub_quote)
    policies = len(hub_policy)

    quote_to_policy = (policies / max(1, quotes)) * 100
    avg_pol_per_cust = policies / max(1, len(hub_customer))

    policy_product_types = (
        l_policy_product[["Policy Hash Key", "Product Hash Key"]]
        .merge(sat_product[["Product Hash Key", "Type"]], on="Product Hash Key", how="left")
    )
    motor = int(policy_product_types["Type"].astype(str).str.contains("MOTOR", na=False).sum())
    home = int(policy_product_types["Type"].astype(str).str.contains("HOME|PROPERTY", na=False).sum())
    motor_pct = (motor / max(1, policies)) * 100
    home_pct = (home / max(1, policies)) * 100

    consent_rate = (len(set(l_person_consent["Person Hash Key"])) / total_people) * 100
    engaged_rate = (
        len(set(l_person_mkt["Person Hash Key"])) / max(1, len(lead_people.union(customer_people)))
    ) * 100

    active_customers = l_policy_customer["Customer Hash Key"].nunique()
    multi_product_policy_people = 0
    multi_product_policy_people_pct = 0.0

    policy_person_products = (
        l_policy_customer[["Policy Hash Key", "Customer Hash Key"]]
        .merge(l_customer_person[["Customer Hash Key", "Person Hash Key"]], on="Customer Hash Key", how="left")
        .merge(l_policy_product[["Policy Hash Key", "Product Hash Key"]], on="Policy Hash Key", how="left")
        .merge(sat_product[["Product Hash Key", "Type"]], on="Product Hash Key", how="left")
    )
    if not policy_person_products.empty:
        distinct_products_per_person = (
            policy_person_products.dropna(subset=["Person Hash Key", "Type"])
            .groupby("Person Hash Key")["Type"]
            .nunique()
        )
        multi_product_policy_people = int((distinct_products_per_person > 1).sum())
        multi_product_policy_people_pct = (
            multi_product_policy_people / max(1, len(distinct_products_per_person))
        ) * 100

    product_mix_pct = {}
    if not policy_product_types.empty:
        product_type_counts = policy_product_types["Type"].dropna().astype(str).value_counts()
        product_mix_pct = {
            product_type: (count / max(1, policies)) * 100
            for product_type, count in product_type_counts.items()
        }

    avg_lead_to_policy_days = None
    if (
        sat_lead is not None and sat_policy is not None
        and {"Lead Hash Key", "Converted Date"}.issubset(sat_lead.columns)
        and {"Policy Hash Key", "Policy Start Date"}.issubset(sat_policy.columns)
    ):
        lead_dates = sat_lead[["Lead Hash Key", "Converted Date"]].copy()
        lead_dates["Converted Date"] = pd.to_datetime(lead_dates["Converted Date"], errors="coerce")

        latest_lead_by_person = (
            l_person_lead[["Person Hash Key", "Lead Hash Key"]]
            .merge(lead_dates, on="Lead Hash Key", how="left")
            .groupby("Person Hash Key", as_index=False)["Converted Date"]
            .max()
        )

        policy_dates = sat_policy[["Policy Hash Key", "Policy Start Date"]].copy()
        policy_dates["Policy Start Date"] = pd.to_datetime(policy_dates["Policy Start Date"], errors="coerce")

        policy_person_dates = (
            l_policy_customer[["Policy Hash Key", "Customer Hash Key"]]
            .merge(l_customer_person[["Customer Hash Key", "Person Hash Key"]], on="Customer Hash Key", how="left")
            .merge(policy_dates, on="Policy Hash Key", how="left")
            .merge(latest_lead_by_person, on="Person Hash Key", how="left")
        )

        conversion_days = (
            (policy_person_dates["Policy Start Date"] - policy_person_dates["Converted Date"]).dt.total_seconds()
            / 86400.0
        )
        valid_conversion_days = conversion_days.dropna()
        if not valid_conversion_days.empty:
            avg_lead_to_policy_days = valid_conversion_days.mean()

    print("\n===== BRONZE KPI REPORT =====")
    print("Total People:", total_people)
    print("Total Leads:", len(lead_people))
    print("Total Customers:", len(hub_customer))
    print("Converted Leads:", len(converted))
    print(f"Lead to Customer %: {lead_conv:.2f}")

    print("\nQuotes:", quotes)
    print("Policies:", policies)
    print(f"Quote to Policy %: {quote_to_policy:.2f}")
    print(f"Avg Policies / Customer: {avg_pol_per_cust:.2f}")
    print(f"People with Multiple Product Policies: {multi_product_policy_people}")
    print(f"Multiple Product Policy People %: {multi_product_policy_people_pct:.2f}")
    if avg_lead_to_policy_days is None:
        print("Avg Lead to Policy Days: unavailable")
    else:
        print(f"Avg Lead to Policy Days: {avg_lead_to_policy_days:.2f}")

    print(f"\nMotor Policies %: {motor_pct:.2f}")
    print(f"Home/Property Policies %: {home_pct:.2f}")
    if product_mix_pct:
        print("\nPolicy Mix by Product Type %:")
        for product_type, pct in product_mix_pct.items():
            print(f"{product_type}: {pct:.2f}")

    print(f"\nConsent Rate %: {consent_rate:.2f}")
    print(f"Marketing Engagement %: {engaged_rate:.2f}")

    print("Active Customers:", active_customers)
    print("=============================\n")

    checks = {
        "Link_Person_Contact.csv": "Person Hash Key",
        "Link_Person_Consent.csv": "Person Hash Key",
        "Link_Person_Identities.csv": "Person Hash Key",
        "Link_Person_Marketing_Preference.csv": "Person Hash Key",
        "Link_Person_Marketing_Engagement.csv": "Person Hash Key",
        "Link_Person_Account.csv": "Person Hash Key",
        "Link_Policy_Product.csv": "Policy Hash Key",
        "Link_Product_Motor.csv": "Product Hash Key",
        "Link_Product_Home.csv": "Product Hash Key",
        "Link_Quote_Person.csv": "Person Hash Key",
    }

    for file_name, left_key in checks.items():
        df = _read(run, file_name)
        counts = df.groupby(left_key).size()

        print(f"\n=== {file_name} ===")
        print("min:", counts.min())
        print("max:", counts.max())
        print("avg:", round(counts.mean(), 2))


if __name__ == "__main__":
    main()
