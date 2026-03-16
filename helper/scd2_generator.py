from __future__ import annotations

import os
import random
from pathlib import Path

import pandas as pd
from faker import Faker

from enums.sat_enums import SAT_ENUMS

fake = Faker("en_GB")


def create_scd_data(INPUT_FOLDER, OUTPUT_FOLDER, SAT_DATE):
    CHANGE_PERCENT = 0.01  # 1% rows will evolve

    cols = {
        "sat_account.csv": ['account_status'],
        "sat_consent.csv": [],
        "sat_contact.csv": ['personal_email'],
        "sat_customer.csv": ['customer_status_reason'],
        "sat_home.csv": [],
        "sat_home_address.csv": ['street'],
        "sat_identities.csv": [],
        "sat_lead.csv": ['preferred_contact_method'],
        "sat_legal_person.csv": [],
        "sat_marketing_engagement.csv": [],
        "sat_marketing_preference.csv": [],
        "sat_motor.csv": ['license_status'],
        "sat_natural_person.csv": ['occupation'],
        "sat_person.csv": [],
        "sat_policy.csv": ['cover_option'],
        "sat_product.csv": [],
        "sat_quote.csv": ['renewal_amt_next_period'],
    }

    base_input = Path(INPUT_FOLDER)

    def _mutate_value(series: pd.Series, value, coln, csv_file: Path):
        enum_values = SAT_ENUMS.get(csv_file.stem, {}).get(coln)

        # ENUM columns
        if enum_values:
            choices = [e for e in enum_values if e != value]
            if choices:
                return random.choice(choices)
            return value

        # Non-enum special cases
        if "email" in str(coln):
            return fake.email()
        if "phone" in str(coln):
            return fake.phone_number()

        # Numeric columns
        if pd.api.types.is_numeric_dtype(series):
            return value

        # Datetime columns
        if pd.api.types.is_datetime64_any_dtype(series):
            if pd.notna(value):
                return value + pd.Timedelta(days=1)
            return pd.Timestamp.now()

        # Default: keep value
        return value

    for csv_file in base_input.glob("*.csv"):
        if "sat_" not in csv_file.stem:
            continue

        df = pd.read_csv(csv_file)

        total_rows = len(df)
        num_changes = int(total_rows * CHANGE_PERCENT)
        if num_changes <= 0 < total_rows and CHANGE_PERCENT > 0:
            num_changes = 1

        chosen_indices = random.sample(list(df.index), k=min(num_changes, total_rows))

        new_rows = []
        for idx in chosen_indices:
            old_row = df.loc[idx].copy()
            new_row = old_row.copy()

            column_to_modify = cols[csv_file.name]
            if column_to_modify:
                col_to_modify = random.choice(column_to_modify)
                if isinstance(new_row[col_to_modify], float):
                    new_row[col_to_modify] = new_row[col_to_modify] + 10.0
                else:
                    new_row[col_to_modify] = _mutate_value(
                        df[col_to_modify],
                        new_row[col_to_modify],
                        col_to_modify,
                        csv_file,
                    )
                new_row["load_date"] = SAT_DATE
                new_rows.append(new_row)

            if new_rows:
                os.makedirs(OUTPUT_FOLDER, exist_ok=True)
                pd.DataFrame(new_rows).to_csv(
                    os.path.join(OUTPUT_FOLDER, csv_file.name),
                    index=False,
                )