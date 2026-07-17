from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "e_360" / "enhanced_raw_vault_source_split.sqlite"


def count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]


def missing_parent_count(conn: sqlite3.Connection, link: str, fk_col: str, hub: str, hub_pk: str) -> int:
    return conn.execute(
        f'''
        SELECT COUNT(*)
        FROM "{link}" l
        LEFT JOIN "{hub}" h ON h."{hub_pk}" = l."{fk_col}"
        WHERE l."{fk_col}" IS NOT NULL
          AND TRIM(CAST(l."{fk_col}" AS TEXT)) <> ''
          AND h."{hub_pk}" IS NULL
        '''
    ).fetchone()[0]


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify the local enhanced source-split SQLite raw vault.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    args = parser.parse_args()
    conn = sqlite3.connect(args.db_path)
    try:
        tables = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'hub_%' OR name LIKE 'link_%' OR name LIKE 'sat_%')"
            )
        ]
        hubs = [name for name in tables if name.startswith("hub_")]
        links = [name for name in tables if name.startswith("link_")]
        sats = [name for name in tables if name.startswith("sat_")]
        errors = 0
        expected = (25, 30, 30)
        actual = (len(hubs), len(links), len(sats))
        if actual == expected:
            print(f"PASS: table split hubs/links/sats={actual}")
        else:
            print(f"FAIL: table split expected={expected} actual={actual}")
            errors += 1

        required_nonempty = [
            "hub_person",
            "hub_policy",
            "hub_insured_object",
            "sat_person_crm",
            "sat_person_sap",
            "sat_address_sap",
            "sat_product_sap",
            "sat_home_sap",
            "sat_motor_sap",
            "link_person_address",
            "link_policy_product",
            "link_policy_insured_object",
        ]
        for table in required_nonempty:
            rows = count(conn, table)
            if rows:
                print(f"PASS: {table} rows={rows}")
            else:
                print(f"FAIL: {table} is empty")
                errors += 1

        fk_checks = [
            ("link_person_address", "person_hash_key", "hub_person", "person_hash_key"),
            ("link_person_address", "address_hash_key", "hub_address", "address_hash_key"),
            ("link_policy_product", "policy_hash_key", "hub_policy", "policy_hash_key"),
            ("link_policy_product", "product_hash_key", "hub_product", "product_hash_key"),
            ("link_policy_insured_object", "policy_hash_key", "hub_policy", "policy_hash_key"),
            ("link_policy_insured_object", "insured_object_hash_key", "hub_insured_object", "insured_object_hash_key"),
        ]
        for link, fk_col, hub, hub_pk in fk_checks:
            missing = missing_parent_count(conn, link, fk_col, hub, hub_pk)
            if missing == 0:
                print(f"PASS: {link}.{fk_col} resolves to {hub}.{hub_pk}")
            else:
                print(f"FAIL: {link}.{fk_col} missing parents in {hub}: {missing}")
                errors += 1
        print("SQLite source-split vault verification " + ("passed." if errors == 0 else f"failed with {errors} issue(s)."))
        return errors
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
