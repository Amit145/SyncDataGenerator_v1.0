import os
import pandas as pd


def _read(run_dir, name):
    path = os.path.join(run_dir, name)
    if not os.path.exists(path):
        return None
    return pd.read_csv(path)


def main():
    # set this to your latest run folder path
    base = r"output"
    run_dir = max(
        (os.path.join(base, d) for d in os.listdir(base) if os.path.isdir(os.path.join(base, d))),
        key=os.path.getmtime
    )
    print("Using latest run:", run_dir)

    hub_person = _read(run_dir, "Hub_Person.csv")
    hub_cust = _read(run_dir, "Hub_Customer.csv")
    hub_quote = _read(run_dir, "Hub_Quote.csv")
    hub_policy = _read(run_dir, "Hub_Policy.csv")

    l_person_lead = _read(run_dir, "Link_Person_Lead.csv")
    l_cust_person = _read(run_dir, "Link_Customer_Person.csv")
    l_pol_prod = _read(run_dir, "Link_Policy_Product.csv")

    l_person_consent = _read(run_dir, "Link_Person_Consent.csv")
    l_person_mkt_eng = _read(run_dir, "Link_Person_Marketing_Engagement.csv")

    total_people = len(hub_person)

    # ---- Lead -> Customer conversion
    lead_people = set(l_person_lead["Person Hash Key"])
    customer_people = set(l_cust_person["Person Hash Key"])
    converted_leads = lead_people.intersection(customer_people)

    lead_conv_rate = (len(converted_leads) / max(1, len(lead_people))) * 100

    # ---- Conversion by channel
    # sat_lead: Lead Hash Key -> Sales Channel
    # link_person_lead: Person Hash Key -> Lead Hash Key
    # ---- Conversion by channel (disabled – Sales Channel not in DDL Sat_Lead)

    # ---- Quotes -> Policies rate (simple overall)
    quotes = len(hub_quote)
    policies = len(hub_policy)
    quote_to_policy_rate = (policies / max(1, quotes)) * 100

    # ---- Avg policies per customer
    customers = len(hub_cust)
    avg_policies_per_customer = policies / max(1, customers)

    # ---- Motor vs Home split (by product id pattern)
    # Link_Policy_Product has Product Hash Key = stable ids like PRD_MOTOR_PERSONAL etc.
    prod_counts = l_pol_prod["Product Hash Key"].value_counts()
    motor_pols = sum(v for k, v in prod_counts.items() if "MOTOR" in str(k))
    home_pols = sum(v for k, v in prod_counts.items() if ("HOME" in str(k)) or ("PROPERTY" in str(k)))
    motor_pct = (motor_pols / max(1, policies)) * 100
    home_pct = (home_pols / max(1, policies)) * 100

    # ---- Consent rate
    consented_people = set(l_person_consent["Person Hash Key"]) if l_person_consent is not None else set()
    consent_rate = (len(consented_people) / max(1, total_people)) * 100

    # ---- Marketing engagement rate (people with engagement out of (leads+customers) population)
    engaged_people = set(l_person_mkt_eng["Person Hash Key"]) if l_person_mkt_eng is not None else set()
    leads_plus_customers = lead_people.union(customer_people)
    mkt_eng_rate = (len(engaged_people) / max(1, len(leads_plus_customers))) * 100

    print("\n=== KPI REPORT ===")
    print(f"Total People: {total_people}")
    print(f"Total Leads: {len(lead_people)}")
    print(f"Total Customers: {customers}")
    print(f"Converted Leads: {len(converted_leads)}")
    print(f"Lead → Customer Conversion %: {lead_conv_rate:.2f}%")

    print("\nConversion % by Sales Channel: (skipped – not in DDL)")

    print(f"\nQuotes: {quotes}")
    print(f"Policies: {policies}")
    print(f"Quotes → Policies % (overall): {quote_to_policy_rate:.2f}%")
    print(f"Avg Policies / Customer: {avg_policies_per_customer:.2f}")

    print(f"\nMotor Policies %: {motor_pct:.2f}%")
    print(f"Home/Property Policies %: {home_pct:.2f}%")

    print(f"\nConsent Rate %: {consent_rate:.2f}%")
    print(f"Marketing Engagement Rate % (Leads+Customers): {mkt_eng_rate:.2f}%")
    print("==================\n")


if __name__ == "__main__":
    main()
