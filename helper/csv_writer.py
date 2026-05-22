import csv
import os
from pathlib import Path

import pandas as pd


def write_csv(folder, name, rows, fieldnames=None):
    os.makedirs(folder, exist_ok=True)

    name_lower_case = name.lower()

    # allow empty tables to still be created with headers
    if not rows and not fieldnames:
        return

    if rows and not fieldnames:
        fieldnames = list(rows[0].keys())

    with open(os.path.join(folder, name_lower_case), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        if rows:
            w.writerows(rows)


def append_csv(folder, name, rows, fieldnames=None):
    os.makedirs(folder, exist_ok=True)

    name_lower_case = name.lower()
    path = os.path.join(folder, name_lower_case)

    if not rows and not fieldnames:
        return

    if rows and not fieldnames:
        fieldnames = list(rows[0].keys())

    file_exists = os.path.exists(path) and os.path.getsize(path) > 0
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        if rows:
            w.writerows(rows)


def normalize_csv_streaming(base_input, output_dir):
    base_input = Path(base_input)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for csv_file in base_input.glob("*.csv"):
        with open(csv_file, "r", newline="", encoding="utf-8") as src:
            reader = csv.DictReader(src)
            if not reader.fieldnames:
                continue

            normalized_fields = [
                field.strip().lower().replace(" ", "_")
                for field in reader.fieldnames
            ]

            with open(output_dir / csv_file.name, "w", newline="", encoding="utf-8") as dst:
                writer = csv.DictWriter(dst, fieldnames=normalized_fields)
                writer.writeheader()
                for row in reader:
                    writer.writerow({
                        normalized: row.get(original, "")
                        for original, normalized in zip(reader.fieldnames, normalized_fields)
                    })

    print("All files processed successfully.")
    return True


def normalize_csv(base_input, output_dir):
    base_input = Path(base_input)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # ---- PROCESS ----
    for csv_file in base_input.glob("*.csv"):
        df = pd.read_csv(csv_file)

        # normalize column names
        df.columns = (
            df.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)
        )

        # write to new folder
        df.to_csv(output_dir / csv_file.name, index=False)

    print("All files processed successfully.")
    return True
