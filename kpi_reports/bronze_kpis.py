import os
import pandas as pd

BASE = r"D:\pythonProject\SyncDataGenerator\output"


def latest_run():
    return max(
        (os.path.join(BASE, d) for d in os.listdir(BASE) if os.path.isdir(os.path.join(BASE, d))),
        key=os.path.getmtime
    )


def _read(run_dir, name):
    p = os.path.join(run_dir, name)
    if not os.path.exists(p):
        return None
    return pd.read_csv(p)


def main():
    run = latest_run()
    print("Using:", run)

    hub_person = _read(run, "Hub_Person.csv")
    hub_customer = _read(run, "Hub_Customer.csv")
    hub_quote = _read(run, "Hub_Quote.csv")
    hub_policy = _read(run, "Hub_Policy.csv")

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

    # Product mix (based on Product Hash Key naming)
    prod_counts = l_policy_product["Product Hash Key"].astype(str).value_counts()

    motor = sum(v for k, v in prod_counts.items() if "MOTOR" in k)
    home = sum(v for k, v in prod_counts.items() if ("HOME" in k) or ("PROPERTY" in k))

    motor_pct = (motor / max(1, policies)) * 100
    home_pct = (home / max(1, policies)) * 100

    consent_rate = (len(set(l_person_consent["Person Hash Key"])) / total_people) * 100
    engaged_rate = (len(set(l_person_mkt["Person Hash Key"])) /
                    max(1, len(lead_people.union(customer_people)))) * 100

    active_customers = l_policy_customer["Customer Hash Key"].nunique()

    print("\n===== BRONZE KPI REPORT =====")
    print("Total People:", total_people)
    print("Total Leads:", len(lead_people))
    print("Total Customers:", len(hub_customer))
    print("Converted Leads:", len(converted))
    print(f"Lead → Customer %: {lead_conv:.2f}")

    print("\nQuotes:", quotes)
    print("Policies:", policies)
    print(f"Quote → Policy %: {quote_to_policy:.2f}")
    print(f"Avg Policies / Customer: {avg_pol_per_cust:.2f}")

    print(f"\nMotor Policies %: {motor_pct:.2f}")
    print(f"Home/Property Policies %: {home_pct:.2f}")

    print(f"\nConsent Rate %: {consent_rate:.2f}")
    print(f"Marketing Engagement %: {engaged_rate:.2f}")

    print("Active Customers:", active_customers)
    print("=============================\n")

    CHECKS = {
        "Link_Person_Contact.csv": "Person Hash Key",
        "Link_Person_Consent.csv": "Person Hash Key",
        "Link_Person_Identities.csv": "Person Hash Key",
        "Link_Person_Marketing_Preference.csv": "Person Hash Key",
        "Link_Person_Marketing_Engagement.csv": "Person Hash Key",
        "Link_Person_Account.csv": "Person Hash Key",
        "Link_Policy_Product.csv": "Policy Hash Key",
        "Link_Product_Motor.csv": "Product Hash Key",
        "Link_Product_Home.csv": "Product Hash Key",
        "Link_Quote_Person.csv": "Person Hash Key"
    }

    for file, left_key in CHECKS.items():
        df = _read(run, file)

        counts = df.groupby(left_key).size()

        print(f"\n=== {file} ===")
        print("min:", counts.min())
        print("max:", counts.max())
        print("avg:", round(counts.mean(), 2))


if __name__ == "__main__":
    main()
