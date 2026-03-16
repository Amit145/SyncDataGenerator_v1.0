# Databricks Notebook: Load SAT CSVs and apply SCD2 (close + insert) into Delta tables
# Works for any sat_*.csv where there is exactly one *_hash_key column.

from delta.tables import DeltaTable
import pyspark.sql.functions as F

# ----------------------------
# CONFIG
# ----------------------------
input_csv_path = "/Volumes/allianz/general/files_volume/amit/data/scd2/"  # your folder
catalog = "allianz_coe"
schema = "dvault_scd2"

# If your incoming CSV doesn't have effective_from, we'll use load_date as effective_from
EFFECTIVE_FROM_COL = "effective_from"
LOAD_DATE_COL = "load_date"

REQUIRED_SCD_COLS = {"hashdiff", "effective_to", "is_current"}  # effective_from handled separately


# ----------------------------
# HELPERS
# ----------------------------
def find_hk_col(cols):
    hk = [c for c in cols if c.endswith("_hash_key")]
    if len(hk) != 1:
        raise ValueError(f"Expected exactly 1 *_hash_key column, found: {hk}")
    return hk[0]


def ensure_scd_cols(df):
    cols = set(df.columns)

    # effective_from
    if EFFECTIVE_FROM_COL not in cols:
        if LOAD_DATE_COL in cols:
            df = df.withColumn(EFFECTIVE_FROM_COL, F.col(LOAD_DATE_COL))
        else:
            df = df.withColumn(EFFECTIVE_FROM_COL, F.current_timestamp())

    # cast effective_from to timestamp if it isn't
    df = df.withColumn(EFFECTIVE_FROM_COL, F.to_timestamp(F.col(EFFECTIVE_FROM_COL)))

    # effective_to
    if "effective_to" not in cols:
        df = df.withColumn("effective_to", F.lit(None).cast("timestamp"))
    else:
        df = df.withColumn("effective_to", F.to_timestamp(F.col("effective_to")))

    # is_current
    if "is_current" not in cols:
        df = df.withColumn("is_current", F.lit(1))
    else:
        df = df.withColumn("is_current", F.col("is_current").cast("int"))

    # hashdiff must exist for SCD2 change detection
    if "hashdiff" not in cols:
        raise ValueError("Incoming data must have 'hashdiff' column for SCD2 detection.")

    return df


def table_exists(full_name: str) -> bool:
    try:
        spark.table(full_name)
        return True
    except Exception:
        return False


# ----------------------------
# MAIN
# ----------------------------
files = dbutils.fs.ls(input_csv_path)

for f in files:
    name = f.name
    if not (name.endswith(".csv") and "sat_" in name):
        continue

    table_name = name[:-4]  # remove .csv
    full_table = f"{catalog}.{schema}.{table_name}"

    print(f"\n=== Processing: {name} -> {full_table} ===")

    # Read CSV
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .load(f.path)
    )

    # If empty -> skip
    if not df:
        print(" - Skipping (empty file)")
        continue

    # Ensure needed SCD2 columns
    df = ensure_scd_cols(df)

    # Determine HK column
    hk_col = find_hk_col(df.columns)

    # Create table if it doesn't exist (first load)
    if not table_exists(full_table):
        print(" - Target table not found. Creating with initial load (append).")
        (df.write.format("delta").mode("overwrite").saveAsTable(full_table))
        continue

    # Target Delta
    tgt = DeltaTable.forName(spark, full_table)

    # 1) CLOSE current rows that changed (UPDATE)
    # Condition: same HK, current row exists, and hashdiff differs
    print(" - Closing changed current rows...")
    tgt.alias("t").merge(
        df.alias("s"),
        f"t.{hk_col} = s.{hk_col} AND t.is_current = 1"
    ).whenMatchedUpdate(
        condition="t.hashdiff <> s.hashdiff",
        set={
            "effective_to": f"s.{EFFECTIVE_FROM_COL}",
            "is_current": "0"
        }
    ).execute()

    # 2) INSERT new rows for NEW or CHANGED entities (INSERT)
    # We'll compare incoming to current target to avoid inserting duplicates
    print(" - Inserting new/current versions...")

    current_t = (
        spark.table(full_table)
        .filter(F.col("is_current") == 1)
        .select(F.col(hk_col).alias("hk"), F.col("hashdiff").alias("cur_hashdiff"))
    )

    to_insert = (
        df.withColumnRenamed(hk_col, "hk")
        .join(current_t, on="hk", how="left")
        .where((F.col("cur_hashdiff").isNull()) | (F.col("cur_hashdiff") != F.col("hashdiff")))
        .drop("cur_hashdiff")
        .withColumnRenamed("hk", hk_col)
        # enforce "new rows are current" shape
        .withColumn("effective_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(1))
    )

    if not to_insert:
        print(" - No new changes to insert.")
    else:
        print(to_insert)
        (to_insert.write.format("delta").mode("append").option('overwriteSchema','true').saveAsTable(full_table))
        print(f" - Inserting rows.")

print("\nDone.")