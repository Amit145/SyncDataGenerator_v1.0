import psycopg

from db import connection_details


KEY_TO_HUB = {
    "account_hash_key": "hub_account",
    "address_hash_key": "hub_address",
    "broker_hash_key": "hub_broker",
    "campaign_hash_key": "hub_campaign",
    "channel_hash_key": "hub_channel",
    "claim_hash_key": "hub_claim",
    "complaint_hash_key": "hub_complaint",
    "consent_hash_key": "hub_consent",
    "contact_hash_key": "hub_contact",
    "customer_hash_key": "hub_customer",
    "home_hash_key": "hub_home",
    "identities_hash_key": "hub_identities",
    "insured_object_hash_key": "hub_insured_object",
    "lead_hash_key": "hub_lead",
    "legal_person_hash_key": "hub_legal_person",
    "marketing_engagement_hash_key": "hub_marketing_engagement",
    "marketing_preference_hash_key": "hub_marketing_preference",
    "motor_hash_key": "hub_motor",
    "natural_person_hash_key": "hub_natural_person",
    "override_hash_key": "hub_override",
    "person_hash_key": "hub_person",
    "policy_hash_key": "hub_policy",
    "product_hash_key": "hub_product",
    "quote_hash_key": "hub_quote",
    "regulation_hash_key": "hub_regulation",
}


def count(cursor: psycopg.Cursor, query: str, params: tuple = ()) -> int:
    cursor.execute(query, params)
    return cursor.fetchone()[0]


def verify_raw_vault(cursor: psycopg.Cursor) -> list[tuple[str, str, str, int]]:
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'raw_vault'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
    )
    tables = [row[0] for row in cursor.fetchall()]
    issues = []
    checks = 0
    for table in tables:
        if not (table.startswith("link_") or table.startswith("sat_")):
            continue
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'raw_vault'
              AND table_name = %s
            """,
            (table,),
        )
        columns = {row[0] for row in cursor.fetchall()}
        for key_column, hub in KEY_TO_HUB.items():
            if key_column not in columns:
                continue
            checks += 1
            missing = count(
                cursor,
                f"""
                SELECT COUNT(*)
                FROM raw_vault.{table} child
                LEFT JOIN raw_vault.{hub} parent
                  ON parent.{key_column} = child.{key_column}
                WHERE child.{key_column} IS NOT NULL
                  AND parent.{key_column} IS NULL
                """,
            )
            if missing:
                issues.append((table, key_column, hub, missing))
    print(f"raw_vault FK-like checks: {checks}")
    return issues


def verify_business_vault(cursor: psycopg.Cursor) -> list[tuple[str, str, str, int]]:
    issues = []
    checks = [
        ("bv_address_master_xref", "global_address_identifier", "bv_address_master"),
        ("bv_home_master_xref", "global_home_identifier", "bv_home_master"),
        ("bv_legal_person_master_xref", "global_person_identifier", "bv_legal_person_master"),
        ("bv_motor_master_xref", "global_motor_identifier", "bv_motor_master"),
        ("bv_natural_person_master_xref", "global_person_identifier", "bv_natural_person_master"),
        ("bv_product_master_xref", "global_product_identifier", "bv_product_master"),
    ]
    for xref, key_column, master in checks:
        missing = count(
            cursor,
            f"""
            SELECT COUNT(*)
            FROM business_vault.{xref} x
            LEFT JOIN business_vault.{master} m
              ON m.{key_column} = x.{key_column}
            WHERE m.{key_column} IS NULL
            """,
        )
        if missing:
            issues.append((xref, key_column, master, missing))

    for table in ["bv_home_master", "bv_motor_master"]:
        missing = count(
            cursor,
            f"""
            SELECT COUNT(*)
            FROM business_vault.{table} child
            LEFT JOIN business_vault.bv_product_master parent
              ON parent.global_product_identifier = child.global_product_identifier
            WHERE parent.global_product_identifier IS NULL
            """,
        )
        if missing:
            issues.append((table, "global_product_identifier", "bv_product_master", missing))

    missing = count(
        cursor,
        """
        SELECT COUNT(*)
        FROM business_vault.bv_address_master a
        LEFT JOIN (
            SELECT global_person_identifier FROM business_vault.bv_natural_person_master
            UNION
            SELECT global_person_identifier FROM business_vault.bv_legal_person_master
        ) p ON p.global_person_identifier = a.global_person_identifier
        WHERE p.global_person_identifier IS NULL
        """,
    )
    if missing:
        issues.append(("bv_address_master", "global_person_identifier", "person master union", missing))
    return issues


def print_counts(cursor: psycopg.Cursor) -> None:
    print("\nraw_vault source counts:")
    for table in [
        "hub_person",
        "hub_address",
        "hub_policy",
        "hub_product",
        "hub_home",
        "hub_motor",
        "hub_natural_person",
        "hub_legal_person",
    ]:
        cursor.execute(
            f"SELECT record_source, COUNT(*) FROM raw_vault.{table} GROUP BY record_source ORDER BY record_source"
        )
        print(f"{table}: {cursor.fetchall()}")

    print("\nbusiness_vault counts:")
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'business_vault'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
    )
    for (table,) in cursor.fetchall():
        print(f"{table}: {count(cursor, f'SELECT COUNT(*) FROM business_vault.{table}')}")

    print("\nsource match counts:")
    for label, table in [
        ("natural_person_crm_sap_matches", "bv_natural_person_master"),
        ("legal_person_crm_sap_matches", "bv_legal_person_master"),
        ("product_crm_sap_matches", "bv_product_master"),
        ("address_crm_sap_matches", "bv_address_master"),
        ("home_crm_sap_matches", "bv_home_master"),
        ("motor_crm_sap_matches", "bv_motor_master"),
    ]:
        print(f"{label}: {count(cursor, f'SELECT COUNT(*) FROM business_vault.{table}')}")

    print("\nduplicate legal match keys:")
    cursor.execute(
        """
        WITH crm AS (
            SELECT upper(trim(company_name)) AS company_name, date_of_constitution, COUNT(*) AS crm_count
            FROM raw_vault.sat_legal_person_crm
            GROUP BY 1, 2
        ),
        sap AS (
            SELECT upper(trim(organization)) AS organization, org_establishment_date, COUNT(*) AS sap_count
            FROM raw_vault.sat_legal_person_sap
            GROUP BY 1, 2
        )
        SELECT company_name, date_of_constitution, crm_count, sap_count, crm_count * sap_count AS match_rows
        FROM crm
        JOIN sap
          ON crm.company_name = sap.organization
         AND crm.date_of_constitution = sap.org_establishment_date
        WHERE crm_count * sap_count > 1
        ORDER BY match_rows DESC, company_name
        """
    )
    for row in cursor.fetchall():
        print(row)


def main() -> None:
    with psycopg.connect(**connection_details) as connection:
        with connection.cursor() as cursor:
            raw_issues = verify_raw_vault(cursor)
            bv_issues = verify_business_vault(cursor)
            print(f"raw_vault missing references: {len(raw_issues)}")
            for issue in raw_issues:
                print(issue)
            print(f"business_vault missing references: {len(bv_issues)}")
            for issue in bv_issues:
                print(issue)
            print_counts(cursor)


if __name__ == "__main__":
    main()
