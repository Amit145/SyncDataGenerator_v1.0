from helper.key_factory import make_business_id, md5_hasher


def hub_person(n, run_id, ld):
    """
    This function creates Hub_Person records.

    n       → number of persons to generate
    run_id  → unique run identifier (used in business ID creation)
    """

    rows = []  # This will store all person records

    # Loop from 1 to n (generate n persons)
    for i in range(1, n + 1):
        # Create a business ID like: PER_20260216_001
        pid = make_business_id("PER", run_id, i)

        # Create hash key from business ID (Data Vault rule)
        hk = md5_hasher(pid)

        # Add one row to Hub_Person table
        rows.append({
            "Person Hash Key": hk,  # Primary key (hashed)
            "Load Date": ld,  # When record was loaded
            "Record Source": "CRM",  # Source system
            "Person Id": pid  # Business key
        })

    # Return all generated rows
    return rows
