from datetime import date, datetime
import hashlib
import random
from typing import List
from helper.scd2gen import generate_scd_data
from pathlib import Path
import pandas as pd

INPUT_FOLDER = './synthetic_data'
CHANGE_PERCENT = 0.01  # 1% rows will evolve

cols = {
                        'sat_account.csv': ['account_status', 'account_creation_type'],
                        'sat_consent.csv': [],
                        'sat_contact.csv': ['work_email'],
                        'sat_customer.csv': ['customer_status_reason'],

                        'sat_home.csv': ['home_risk_address'],
                        'sat_home_address.csv': ['street'],
                        'sat_identities.csv': [],
                        'sat_lead.csv': [],

                        'sat_legal_person.csv': ['person_status'],
                        'sat_marketing_engagement.csv': ['promotion_code'],
                        'sat_marketing_preference.csv': [],
                        'sat_motor.csv': ['license_status'],

                        'sat_natural_person.csv': ['occupation'],
                        'sat_person.csv': [],
                        'sat_policy.csv': [],
                        'sat_product.csv': [],

                        'sat_quote.csv': [],
                    }


base_input = Path(INPUT_FOLDER)

def _mutate_value(series: pd.Series, value):
    # Generic modification: numeric +1, else append suffix
    if pd.api.types.is_numeric_dtype(series):
        return value
    if pd.api.types.is_datetime64_any_dtype(series):
        if pd.notna(value):
            return value + pd.Timedelta(days=1)
        return pd.Timestamp.now()
    return (str(value) if pd.notna(value) else "") + "_UPDATED"

for csv_file in base_input.glob("*.csv"):
    if 'sat_' in csv_file.stem:
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
                new_row[col_to_modify] = _mutate_value(df[col_to_modify], new_row[col_to_modify])
                #new_row['load_date'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                new_row['load_date'] = datetime.combine(date(2025, 12, 10), datetime.min.time()).isoformat()
                new_rows.append(new_row)

            if new_rows:
                    pd.DataFrame(new_rows).to_csv('./scd2_sat/' + csv_file.name, index=False)

