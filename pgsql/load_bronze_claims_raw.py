import argparse
import csv
from pathlib import Path

import psycopg
from psycopg import sql

from db import connection_details


ROOT = Path(__file__).resolve().parents[1]
CLAIMS_ROOT = ROOT / "data" / "raw" / "claims"
DEFAULT_SCHEMA = "bronze"


SOURCE_FOLDERS = {
    "prd_01": "claims_prd1",
    "prd_02": "claims_prd2",
    "raw_vault": "claims_raw_vault",
}


def latest_run_id() -> str:
    runs = [path for path in CLAIMS_ROOT.iterdir() if path.is_dir()]
    if not runs:
        raise FileNotFoundError(f"No claims runs found under {CLAIMS_ROOT}")
    return max(runs, key=lambda path: (path.name, path.stat().st_mtime)).name


def read_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return next(csv.reader(handle))


def table_name_for(path: Path, source_prefix: str) -> str:
    return f"{source_prefix}_{path.stem.lower()}_csv"


def recreate_table(cursor: psycopg.Cursor, schema: str, table: str, columns: list[str]) -> None:
    cursor.execute(
        sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table),
        )
    )
    column_defs = sql.SQL(", ").join(
        sql.SQL("{} TEXT").format(sql.Identifier(column)) for column in columns
    )
    cursor.execute(
        sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            column_defs,
        )
    )


def copy_csv(cursor: psycopg.Cursor, schema: str, table: str, columns: list[str], path: Path) -> None:
    copy_sql = sql.SQL("COPY {}.{} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
    )
    with cursor.copy(copy_sql) as copy:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            for chunk in handle:
                copy.write(chunk)


def load_folder(
    cursor: psycopg.Cursor,
    schema: str,
    run_dir: Path,
    source_folder: str,
    source_prefix: str,
) -> list[tuple[str, int]]:
    folder = run_dir / source_folder
    if not folder.exists():
        raise FileNotFoundError(folder)

    loaded = []
    for path in sorted(folder.glob("*.csv")):
        if path.name.startswith("_"):
            continue
        table = table_name_for(path, source_prefix)
        columns = read_header(path)
        recreate_table(cursor, schema, table, columns)
        copy_csv(cursor, schema, table, columns, path)
        cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table),
            )
        )
        loaded.append((table, cursor.fetchone()[0]))
    return loaded


def main() -> None:
    parser = argparse.ArgumentParser(description="Load claims PRD1, PRD2, and raw_vault CSVs into PostgreSQL bronze schema.")
    parser.add_argument("--run-id", default=latest_run_id(), help="Claims run id. Defaults to latest data/raw/claims run.")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Target PostgreSQL schema. Defaults to bronze.")
    args = parser.parse_args()

    run_dir = CLAIMS_ROOT / args.run_id
    if not run_dir.exists():
        raise FileNotFoundError(run_dir)

    with psycopg.connect(**connection_details) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(args.schema)))
            loaded = []
            for source_folder, source_prefix in SOURCE_FOLDERS.items():
                loaded.extend(load_folder(cursor, args.schema, run_dir, source_folder, source_prefix))
        connection.commit()

    print(f"Loaded claims run {args.run_id} into schema {args.schema}: {len(loaded)} tables")
    for table, count in loaded:
        print(f"{args.schema}.{table}: {count}")


if __name__ == "__main__":
    main()
