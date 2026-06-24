import os
import re
import pandas as pd
import mysql.connector
from mysql.connector import Error


# =====================================================
# CONFIGURATION
# =====================================================

"""
DB_HOST = "auth-db1282.hstgr.io"
DB_USER = "u941116359_db_user"
DB_PASSWORD = "db_user@June2026"
DB_NAME = "u941116359_enhanced_360"
"""

DB_HOST = "auth-db1282.hstgr.io"
DB_USER = "u941116359_admin1"
DB_PASSWORD = "Admin1@bvault"
DB_NAME = "u941116359_bvault"

CSV_FOLDER = r"F:\SyncDataGenerator_v1.0\data\raw\base\prd_02\20260623182109"   # Folder containing all CSV files


# PRD1 raw upload type overrides. Keep the upload raw-style by default:
# everything is TEXT unless the source mapping identifies a specific exception.
RAW_COLUMN_TYPE_OVERRIDES = {
    'dob': "DATE",
    'date_of_birth': "DATE",
    'org_establishment_date': "DATE",
    'product_start_date': "DATE"
}


# =====================================================
# CLEAN TABLE AND COLUMN NAMES
# =====================================================

def clean_name(name: str) -> str:
    """
    Converts file/column names into safe MySQL names.
    Example:
    'Customer Name' -> 'customer_name'
    'order-id'      -> 'order_id'
    """
    name = str(name).strip().lower()
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")

    if not name:
        name = "column_name"

    if name[0].isdigit():
        name = f"col_{name}"

    return name


def make_unique_columns(columns):
    """
    Handles duplicate column names.
    Example:
    name, name -> name, name_2
    """
    seen = {}
    unique_cols = []

    for col in columns:
        clean_col = clean_name(col)

        if clean_col in seen:
            seen[clean_col] += 1
            clean_col = f"{clean_col}_{seen[clean_col]}"
        else:
            seen[clean_col] = 1

        unique_cols.append(clean_col)

    return unique_cols


# =====================================================
# DATABASE FUNCTIONS
# =====================================================

def connect_to_mysql():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )


def drop_all_tables(cursor):
    """
    Drops all tables from the selected database.
    """
    print("Dropping all existing tables...")

    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")

    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        print(f"Dropping table: {table_name}")
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")

    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

    print("All existing tables dropped.")


def infer_mysql_type(col_name, series):
    """
    Keep raw-load behavior simple: DOB is DATE, raw *_dt/*_ts fields are
    DATETIME, monetary fields are DECIMAL, and everything else is TEXT.
    """
    col = str(col_name).lower()

    if col in RAW_COLUMN_TYPE_OVERRIDES:
        return RAW_COLUMN_TYPE_OVERRIDES[col]
    if col.endswith("_dt") or col.endswith("_ts"):
        return "DATETIME"
    if (
        col.endswith("_amt")
        or "_amt" in col
        or "amount" in col
        or "premium" in col
        or "revenue" in col
        or "reserve" in col
        or "expenses" in col
        or "sum_insrd" in col
        or "insured_value" in col
    ):
        return "DECIMAL(18,4)"

    return "TEXT"


def coerce_dataframe_for_mysql(df):
    """
    Convert DOB, raw datetime, and monetary fields. Everything else remains raw text.
    """
    typed_df = df.copy()

    for col in typed_df.columns:
        mysql_type = infer_mysql_type(col, typed_df[col])

        if mysql_type == "DATE":
            typed_df[col] = pd.to_datetime(typed_df[col], errors="coerce").dt.date
        elif mysql_type == "DATETIME":
            typed_df[col] = pd.to_datetime(typed_df[col], errors="coerce")
        elif mysql_type == "DECIMAL(18,4)":
            typed_df[col] = pd.to_numeric(typed_df[col], errors="coerce")

    return typed_df


def mysql_value(value):
    if pd.isna(value):
        return None
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    if hasattr(value, "item"):
        return value.item()
    return value


def create_table(cursor, table_name, df):
    """
    Creates table using cleaned CSV column names and inferred MySQL types.
    """
    column_definitions = []

    for col in df.columns:
        column_definitions.append(f"`{col}` {infer_mysql_type(col, df[col])}")

    create_sql = f"""
    CREATE TABLE `{table_name}` (
        {", ".join(column_definitions)}
    );
    """

    cursor.execute(create_sql)


def insert_csv_data(cursor, table_name, df):
    """
    Inserts dataframe rows into MySQL table.
    """
    columns = list(df.columns)

    column_sql = ", ".join([f"`{col}`" for col in columns])
    placeholders = ", ".join(["%s"] * len(columns))

    insert_sql = f"""
    INSERT INTO `{table_name}` ({column_sql})
    VALUES ({placeholders});
    """

    data = []

    for _, row in df.iterrows():
        values = [mysql_value(value) for value in row]
        data.append(tuple(values))

    if data:
        cursor.executemany(insert_sql, data)


# =====================================================
# MAIN PROCESS
# =====================================================

def import_all_csv_files():
    try:
        connection = connect_to_mysql()
        cursor = connection.cursor()

        print("Connected to Hostinger MySQL database.")

        # Step 1: Drop all existing tables
        drop_all_tables(cursor)
        connection.commit()

        # Step 2: Read all CSV files from folder
        csv_files = [
            file for file in os.listdir(CSV_FOLDER)
            if file.lower().endswith(".csv")
        ]

        if not csv_files:
            print("No CSV files found in folder.")
            return

        # Step 3: Import each CSV as a table
        for file_name in csv_files:
            file_path = os.path.join(CSV_FOLDER, file_name)

            table_name = clean_name(os.path.splitext(file_name)[0])

            print(f"\nProcessing CSV: {file_name}")
            print(f"Target table: {table_name}")

            df = pd.read_csv(file_path)

            if df.empty:
                print(f"Skipping empty file: {file_name}")
                continue

            # First row is automatically treated as column names by pandas
            df.columns = make_unique_columns(df.columns)
            df = coerce_dataframe_for_mysql(df)

            create_table(cursor, table_name, df)
            insert_csv_data(cursor, table_name, df)

            connection.commit()

            print(f"Imported successfully: {file_name} -> {table_name}")

        cursor.close()
        connection.close()

        print("\nAll CSV files imported successfully.")

    except Error as e:
        print("MySQL Error:", e)

    except Exception as e:
        print("Error:", e)


if __name__ == "__main__":
    import_all_csv_files()