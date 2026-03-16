from __future__ import annotations

import argparse
import os
import random
from pathlib import Path

import pandas as pd
from faker import Faker

from enums.sat_enums import SAT_ENUMS

fake = Faker("en_GB")

MUTATION_COLUMNS = {
    "sat_account.csv": ["account_status"],
    "sat_consent.csv": [],
    "sat_contact.csv": ["personal_email"],
    "sat_customer.csv": ["customer_status_reason"],
    "sat_home.csv": [],
    "sat_home_address.csv": ["street"],
    "sat_identities.csv": [],
    "sat_lead.csv": ["preferred_contact_method"],
    "sat_legal_person.csv": [],
    "sat_marketing_engagement.csv": [],
    "sat_marketing_preference.csv": [],
    "sat_motor.csv": ["license_status"],
    "sat_natural_person.csv": ["occupation"],
    "sat_person.csv": [],
    "sat_policy.csv": ["cover_option"],
    "sat_product.csv": [],
    "sat_quote.csv": ["renewal_amt_next_period"],
}


def mutate_value(series: pd.Series, value, column_name: str, csv_file: Path):
    enum_values = SAT_ENUMS.get(csv_file.stem, {}).get(column_name)

    if enum_values:
        choices = [item for item in enum_values if item != value]
        if choices:
            return random.choice(choices)
        return value

    if "email" in str(column_name):
        return fake.email()
    if "phone" in str(column_name):
        return fake.phone_number()

    if pd.api.types.is_numeric_dtype(series):
        if pd.notna(value):
            return value + 10.0
        return value

    if pd.api.types.is_datetime64_any_dtype(series):
        if pd.notna(value):
            return value + pd.Timedelta(days=1)
        return pd.Timestamp.now()

    return value


def resolve_latest_run(base_dir: Path) -> Path:
    run_dirs = [path for path in base_dir.iterdir() if path.is_dir()]
    if not run_dirs:
        raise FileNotFoundError(f"No run directories found under {base_dir}")
    return max(run_dirs, key=lambda path: path.stat().st_mtime)


def update_scd_records(input_folder: str, output_folder: str, sat_date: str | None = None, change_percent: float = 1.0):
    base_input = Path(input_folder)
    base_output = Path(output_folder)
    base_output.mkdir(parents=True, exist_ok=True)

    if not base_input.exists():
        raise FileNotFoundError(f"Input folder not found: {base_input}")

    for csv_file in base_input.glob("*.csv"):
        if "sat_" not in csv_file.stem:
            continue

        df = pd.read_csv(csv_file)
        if df.empty:
            continue

        mutation_targets = MUTATION_COLUMNS.get(csv_file.name, [])
        if not mutation_targets:
            df.to_csv(base_output / csv_file.name, index=False)
            continue

        total_rows = len(df)
        num_changes = int(total_rows * change_percent)
        if num_changes <= 0 < total_rows and change_percent > 0:
            num_changes = 1

        chosen_indices = random.sample(list(df.index), k=min(num_changes, total_rows))
        updated_rows = []

        for idx in chosen_indices:
            old_row = df.loc[idx].copy()
            new_row = old_row.copy()

            col_to_modify = random.choice(mutation_targets)
            new_row[col_to_modify] = mutate_value(
                df[col_to_modify],
                new_row[col_to_modify],
                col_to_modify,
                csv_file,
            )

            if sat_date:
                if "load_date" in new_row.index:
                    new_row["load_date"] = sat_date
                elif "Load Date" in new_row.index:
                    new_row["Load Date"] = sat_date

            updated_rows.append(new_row)

        if updated_rows:
            pd.DataFrame(updated_rows).to_csv(base_output / csv_file.name, index=False)


def main():
    parser = argparse.ArgumentParser(description="Update existing SCD2 satellite rows into a new scd2_updated folder.")
    parser.add_argument("--input", dest="input_folder", help="Input folder containing existing SCD2 satellite CSVs.")
    parser.add_argument("--output", dest="output_folder", help="Output folder for updated SCD2 satellite CSVs.")
    parser.add_argument("--sat-date", dest="sat_date", help="Optional replacement load_date for updated rows.")
    parser.add_argument(
        "--change-percent",
        dest="change_percent",
        type=float,
        default=1.0,
        help="Fraction of rows to update in each file. Default is 1.0 (all rows).",
    )
    args = parser.parse_args()

    input_folder = Path(args.input_folder) if args.input_folder else resolve_latest_run(Path("scd2_sat"))
    output_folder = Path(args.output_folder) if args.output_folder else Path("scd2_updated") / input_folder.name

    update_scd_records(
        input_folder=str(input_folder),
        output_folder=str(output_folder),
        sat_date=args.sat_date,
        change_percent=args.change_percent,
    )

    print(f"Updated SCD2 records written to: {output_folder}")


if __name__ == "__main__":
    main()
