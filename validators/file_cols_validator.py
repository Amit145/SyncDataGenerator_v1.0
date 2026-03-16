import os
import json
import pandas as pd


def _get_latest_run(base):
    return max(
        (os.path.join(base, d) for d in os.listdir(base)
         if os.path.isdir(os.path.join(base, d))),
        key=os.path.getmtime
    )


def _normalize(cols):
    return {c.strip().lower() for c in cols}


def check_file_and_cols(ddl_path, csv_path):
    # ---- CONFIG ----
    DDL_JSON_PATH = ddl_path
    OUTPUT_BASE = csv_path

    run_dir = _get_latest_run(OUTPUT_BASE)
    print("Checking run:", run_dir)

    with open(DDL_JSON_PATH, "r", encoding="utf-8") as f:
        ddl = json.load(f)

    errors = 0

    for table in ddl:
        table_name = table["table_name"] + ".csv"
        table_name = table_name.replace(" ", "_")
        # ddl_cols = normalize(table["columns"])
        ddl_cols = _normalize(table["columns"].keys())

        csv_path = os.path.join(run_dir, table_name)

        if not os.path.exists(csv_path):
            print(f"❌ MISSING FILE: {table_name}")
            errors += 1
            continue

        df = pd.read_csv(csv_path, nrows=0)
        csv_cols = _normalize(df.columns)

        missing_cols = ddl_cols - csv_cols
        extra_cols = csv_cols - ddl_cols

        if missing_cols or extra_cols:
            print(missing_cols)
            print(extra_cols)

            print(f"\n⚠ Column mismatch in {table_name}")
            if missing_cols:
                print("  Missing:", missing_cols)
            if extra_cols:
                print("  Extra:", extra_cols)
            errors += 1
        else:
            print(f"✅ {table_name} OK")

    print("\n================================")
    if errors == 0:
        print("🎯 ALL TABLES MATCH DDL")
        return True
    else:
        print(f"❗ {errors} table(s) have issues")
        return False
