from __future__ import annotations

import os
import random
from pathlib import Path

import pandas as pd
from faker import Faker

from enums.sat_enums import SAT_ENUMS

fake = Faker("en_GB")

CHANGE_PERCENT = 0.001

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


def _mutate_value(series: pd.Series, value, column_name: str, csv_file: Path):
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


def _get_key_column(df: pd.DataFrame) -> str | None:
    for column in df.columns:
        normalized = column.strip().lower()
        if normalized.endswith("hash key") or normalized.endswith("hash_key"):
            return column
    return None


def _load_latest_versions_from_history(history_root: Path, exclude_run_name: str | None = None) -> dict[str, pd.DataFrame]:
    run_dirs = sorted(
        [path for path in history_root.iterdir() if path.is_dir()],
        key=lambda path: path.stat().st_mtime,
    )

    if exclude_run_name:
        run_dirs = [path for path in run_dirs if path.name != exclude_run_name]

    latest_by_file: dict[str, list[pd.DataFrame]] = {}

    for run_dir in run_dirs:
        for csv_file in run_dir.glob("sat_*.csv"):
            df = pd.read_csv(csv_file)
            if df.empty:
                continue
            df["__source_run__"] = run_dir.name
            latest_by_file.setdefault(csv_file.name, []).append(df)

    result: dict[str, pd.DataFrame] = {}
    for file_name, frames in latest_by_file.items():
        combined = pd.concat(frames, ignore_index=True)
        key_column = _get_key_column(combined)
        if not key_column:
            continue

        load_col = "load_date" if "load_date" in combined.columns else "Load Date" if "Load Date" in combined.columns else None
        if load_col:
            combined["__parsed_load_date__"] = pd.to_datetime(combined[load_col], errors="coerce")
            combined = combined.sort_values(["__parsed_load_date__", "__source_run__"], ascending=[True, True])
            latest = combined.drop_duplicates(subset=[key_column], keep="last")
            latest = latest.drop(columns=["__parsed_load_date__"], errors="ignore")
        else:
            latest = combined.drop_duplicates(subset=[key_column], keep="last")

        result[file_name] = latest.drop(columns=["__source_run__"], errors="ignore")

    return result


def _load_input_frames(input_folder: Path, exclude_run_name: str | None = None) -> dict[str, pd.DataFrame]:
    if any(input_folder.glob("sat_*.csv")):
        return {
            csv_file.name: pd.read_csv(csv_file)
            for csv_file in input_folder.glob("sat_*.csv")
        }

    if any(path.is_dir() for path in input_folder.iterdir()):
        return _load_latest_versions_from_history(input_folder, exclude_run_name=exclude_run_name)

    return {}


def create_scd_data(input_folder: str, output_folder: str, sat_date: str, exclude_run_name: str | None = None):
    base_input = Path(input_folder)
    base_output = Path(output_folder)
    base_output.mkdir(parents=True, exist_ok=True)

    input_frames = _load_input_frames(base_input, exclude_run_name=exclude_run_name)

    for file_name, df in input_frames.items():
        if file_name not in MUTATION_COLUMNS:
            continue

        if df.empty:
            continue

        mutation_targets = MUTATION_COLUMNS[file_name]
        if not mutation_targets:
            continue

        total_rows = len(df)
        num_changes = int(total_rows * CHANGE_PERCENT)
        if num_changes <= 0 < total_rows and CHANGE_PERCENT > 0:
            num_changes = 1

        chosen_indices = random.sample(list(df.index), k=min(num_changes, total_rows))
        new_rows = []

        csv_file_ref = Path(file_name)

        for idx in chosen_indices:
            old_row = df.loc[idx].copy()
            new_row = old_row.copy()

            col_to_modify = random.choice(mutation_targets)
            if isinstance(new_row[col_to_modify], float):
                new_row[col_to_modify] = new_row[col_to_modify] + 10.0
            else:
                new_row[col_to_modify] = _mutate_value(
                    df[col_to_modify],
                    new_row[col_to_modify],
                    col_to_modify,
                    csv_file_ref,
                )

            if "load_date" in new_row.index:
                new_row["load_date"] = sat_date
            elif "Load Date" in new_row.index:
                new_row["Load Date"] = sat_date

            new_rows.append(new_row)

        if new_rows:
            pd.DataFrame(new_rows).to_csv(base_output / file_name, index=False)
