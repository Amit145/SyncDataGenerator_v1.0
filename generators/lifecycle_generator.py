import random
from helper.key_factory import make_business_id, md5_hasher

# Constant value to show where the data is coming from
RS = "CRM"


def assign_lifecycle(person_hks, cfg):
    """
    This function splits people into:
    - Leads
    - Prospects
    - Initial Customers

    Based on the percentage defined in config.
    """

    # Convert to list (in case input is some other type like set)
    person_hks = list(person_hks)

    # Shuffle list randomly so distribution is not biased
    random.shuffle(person_hks)

    # Total number of people
    n = len(person_hks)

    # Calculate how many should be leads
    lead_n = int(n * cfg["lifecycle_distribution"]["lead"])

    # Calculate how many should be prospects
    prospect_n = int(n * cfg["lifecycle_distribution"]["prospect"])

    # First portion becomes leads
    leads = person_hks[:lead_n]

    # Next portion becomes prospects
    prospects = person_hks[lead_n:lead_n + prospect_n]

    # Remaining become customers
    customers_initial = person_hks[lead_n + prospect_n:]

    return leads, prospects, customers_initial


def hub_lead(leads, run_id, ld, max_per_person=2):
    """
    Create Hub_Lead records.

    Each person can have 1 to max_per_person leads.
    """

    rows = []  # Final lead table rows
    person_to_leads = {}  # Mapping: person -> list of lead hash keys
    seq = 0  # Sequence counter for business IDs

    for p in leads:

        # Randomly decide how many leads this person gets
        k = random.randint(1, max_per_person)

        for _ in range(k):
            seq += 1

            # Create business ID (e.g., LEA_20260216_001)
            bid = make_business_id("LEA", run_id, seq)

            # Create hash key from business ID
            hk = md5_hasher(bid)

            # Add row to hub_lead table
            rows.append({
                "Lead Hash Key": hk,
                "Load Date": ld,
                "Record Source": RS,
                "Lead Id": bid
            })

            # Store mapping from person to lead
            person_to_leads.setdefault(p, []).append(hk)

    return rows, person_to_leads


def convert_leads(leads, channels, rates):
    """
    Decide which leads convert into customers.

    Each lead has a conversion chance based on its channel.
    """

    return [
        p for p in leads
        if random.random() < rates[channels[p]]
    ]


def hub_customer(customers, run_id, ld):
    """
    Create Hub_Customer records.

    Each person gets exactly 1 customer record.
    """

    # Get current timestamp
    rows = []  # Final customer table rows
    person_to_customer = {}  # Mapping: person -> customer hash key

    for i, p in enumerate(customers, 1):

        # Create business ID (e.g., CUS_20260216_001)
        bid = make_business_id("CUS", run_id, i)

        # Create hash key from business ID
        hk = md5_hasher(bid)

        # Add row to hub_customer table
        rows.append({
            "Customer Hash Key": hk,
            "Load Date": ld,
            "Record Source": RS,
            "Customer Id": bid
        })

        # Map person to customer hash key
        person_to_customer[p] = hk

    return rows, person_to_customer
