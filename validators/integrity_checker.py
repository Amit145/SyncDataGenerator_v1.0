import os
import pandas as pd


def _latest_run(csv_path):
    return max(
        (os.path.join(csv_path, d) for d in os.listdir(csv_path) if os.path.isdir(os.path.join(csv_path, d))),
        key=os.path.getmtime
    )


def _load(csv):
    return pd.read_csv(csv)


def _parse_date_series(s: pd.Series) -> pd.Series:
    """
    Parse YYYY-MM-DD or ISO strings safely to datetime (NaT if invalid).
    Works even if column contains time portion.
    """
    return pd.to_datetime(s, errors="coerce").dt.date


def _check_policy_dates(run_dir: str) -> int:
    """
    Business sanity checks (NO new columns):
      - Policy Start Date <= Policy End Date
      - Renewal Date <= Policy End Date
    Returns number of errors found.
    """
    f = os.path.join(run_dir, "Sat_Policy.csv")
    if not os.path.exists(f):
        print("ℹ️ Sat_Policy.csv not found; skipping policy date checks")
        return 0

    df = _load(f)

    required = ["Policy Start Date", "Policy End Date", "Renewal Date"]
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        print(f"ℹ️ Sat_Policy.csv missing columns {missing_cols}; skipping policy date checks")
        return 0

    start = _parse_date_series(df["Policy Start Date"])
    end = _parse_date_series(df["Policy End Date"])
    ren = _parse_date_series(df["Renewal Date"])

    errors = 0

    # If any unparsable dates exist, flag (helps catch format regressions)
    bad_start = start.isna().sum()
    bad_end = end.isna().sum()
    bad_ren = ren.isna().sum()
    if bad_start or bad_end or bad_ren:
        print(f"❌ Sat_Policy.csv has unparsable dates:"
              f" start={bad_start}, end={bad_end}, renewal={bad_ren}")
        errors += 1

    # Only compare rows where both sides are valid dates
    valid_se = (~start.isna()) & (~end.isna())
    invalid_order = (start[valid_se] > end[valid_se]).sum()
    if invalid_order:
        print(f"❌ Sat_Policy: Policy Start Date > Policy End Date in {int(invalid_order)} rows")
        errors += 1

    valid_re = (~ren.isna()) & (~end.isna())
    invalid_renewal = (ren[valid_re] > end[valid_re]).sum()
    if invalid_renewal:
        print(f"❌ Sat_Policy: Renewal Date > Policy End Date in {int(invalid_renewal)} rows")
        errors += 1

    if errors == 0:
        print("🎯 POLICY DATE CHECKS OK")

    return errors


def _check_lead_to_policy_timeline(run_dir: str) -> int:
    """
    Business sanity checks:
      - Lead.Converted Date < Policy.Policy Start Date for policy-holder persons
    Returns number of errors found.
    """
    required_files = {
        "Sat_Lead.csv": os.path.join(run_dir, "Sat_Lead.csv"),
        "Sat_Policy.csv": os.path.join(run_dir, "Sat_Policy.csv"),
        "Link_Person_Lead.csv": os.path.join(run_dir, "Link_Person_Lead.csv"),
        "Link_Customer_Person.csv": os.path.join(run_dir, "Link_Customer_Person.csv"),
        "Link_Policy_Customer.csv": os.path.join(run_dir, "Link_Policy_Customer.csv"),
    }

    missing = [name for name, path in required_files.items() if not os.path.exists(path)]
    if missing:
        print(f"ℹ️ Skipping lead-to-policy timeline checks; missing files: {missing}")
        return 0

    sat_lead = _load(required_files["Sat_Lead.csv"])
    sat_policy = _load(required_files["Sat_Policy.csv"])
    link_person_lead = _load(required_files["Link_Person_Lead.csv"])
    link_customer_person = _load(required_files["Link_Customer_Person.csv"])
    link_policy_customer = _load(required_files["Link_Policy_Customer.csv"])

    required_cols = {
        "Sat_Lead.csv": ["Lead Hash Key", "Converted Date"],
        "Sat_Policy.csv": ["Policy Hash Key", "Policy Start Date"],
        "Link_Person_Lead.csv": ["Person Hash Key", "Lead Hash Key"],
        "Link_Customer_Person.csv": ["Customer Hash Key", "Person Hash Key"],
        "Link_Policy_Customer.csv": ["Policy Hash Key", "Customer Hash Key"],
    }

    frames = {
        "Sat_Lead.csv": sat_lead,
        "Sat_Policy.csv": sat_policy,
        "Link_Person_Lead.csv": link_person_lead,
        "Link_Customer_Person.csv": link_customer_person,
        "Link_Policy_Customer.csv": link_policy_customer,
    }

    for file_name, cols in required_cols.items():
        missing_cols = [c for c in cols if c not in frames[file_name].columns]
        if missing_cols:
            print(f"ℹ️ Skipping lead-to-policy timeline checks; {file_name} missing columns {missing_cols}")
            return 0

    lead_dates = sat_lead[["Lead Hash Key", "Converted Date"]].copy()
    lead_dates["Converted Date"] = pd.to_datetime(lead_dates["Converted Date"], errors="coerce")

    policy_dates = sat_policy[["Policy Hash Key", "Policy Start Date"]].copy()
    policy_dates["Policy Start Date"] = pd.to_datetime(policy_dates["Policy Start Date"], errors="coerce")

    person_lead_dates = link_person_lead.merge(lead_dates, on="Lead Hash Key", how="left")
    latest_lead_by_person = (
        person_lead_dates.groupby("Person Hash Key", as_index=False)["Converted Date"].max()
    )

    policy_person = (
        link_policy_customer.merge(link_customer_person, on="Customer Hash Key", how="left")
        .merge(policy_dates, on="Policy Hash Key", how="left")
        .merge(latest_lead_by_person, on="Person Hash Key", how="left")
    )

    errors = 0

    bad_policy_dates = policy_person["Policy Start Date"].isna().sum()
    bad_lead_dates = policy_person["Converted Date"].isna().sum()
    if bad_policy_dates or bad_lead_dates:
        print(
            f"❌ Lead/policy timeline has unparsable or missing dates:"
            f" policy_start={int(bad_policy_dates)}, lead_converted={int(bad_lead_dates)}"
        )
        errors += 1

    valid = (~policy_person["Policy Start Date"].isna()) & (~policy_person["Converted Date"].isna())
    invalid_order = (
        policy_person.loc[valid, "Converted Date"] >= policy_person.loc[valid, "Policy Start Date"]
    ).sum()
    if invalid_order:
        print(f"❌ Lead.Converted Date >= Policy Start Date in {int(invalid_order)} policy rows")
        errors += 1

    if errors == 0:
        print("🎯 LEAD TO POLICY TIMELINE CHECKS OK")

    return errors


def validate_integrity(csv_path):
    run = _latest_run(csv_path)
    print("Checking RI in:", run)

    # Load hubs
    hubs = {}
    for f in os.listdir(run):
        if f.startswith("Hub_"):
            df = _load(os.path.join(run, f))
            hk_col = [c for c in df.columns if "Hash Key" in c][0]
            hubs[f.replace(".csv", "")] = set(df[hk_col])

    errors = 0

    # Check all links
    for f in os.listdir(run):
        if f.startswith("Link_"):
            df = _load(os.path.join(run, f))
            for col in df.columns:
                if col.endswith("Hash Key") and not col.startswith(f.replace("Link_", "").replace("_", " ")):
                    # FK column
                    hub = "Hub_" + col.replace(" Hash Key", "").replace(" ", "_")
                    if hub in hubs:
                        missing = set(df[col]) - hubs[hub]
                        if missing:
                            print(f"❌ {f}.{col} -> {hub} missing {len(missing)} keys")
                            errors += 1

    # Business checks (dates etc.)
    errors += _check_policy_dates(run)
    errors += _check_lead_to_policy_timeline(run)

    print("\n====================")
    if errors == 0:
        print("REFERENTIAL + BUSINESS INTEGRITY OK")
        return True
    else:
        print(f"❗ Integrity errors: {errors}")
        return False
