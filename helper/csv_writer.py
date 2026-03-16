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
