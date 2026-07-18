import csv
from pathlib import Path

import psycopg
from psycopg import sql

from db import connection_details


ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "20260715193343"
SCHEMA = "bronze"

PRD1_DIR = ROOT / "data" / "raw" / "enhanced" / "prd_01" / RUN_ID / "vault_ready_28"
PRD2_DIR = ROOT / "data" / "raw" / "enhanced" / "prd_02" / RUN_ID


def table_name_for(path: Path, source: str) -> str:
    stem = path.stem.lower()
    if source == "prd_01":
        return f"crm_{stem}_csv"
    if source == "prd_02":
        return f"sap_{stem}_db"
    raise ValueError(f"Unsupported source: {source}")


def read_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        return next(reader)


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


def load_source(cursor: psycopg.Cursor, source: str, directory: Path) -> list[tuple[str, int]]:
    loaded = []
    for path in sorted(directory.glob("*.csv")):
        table = table_name_for(path, source)
        columns = read_header(path)
        recreate_table(cursor, SCHEMA, table, columns)
        copy_csv(cursor, SCHEMA, table, columns, path)
        cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                sql.Identifier(SCHEMA),
                sql.Identifier(table),
            )
        )
        loaded.append((table, cursor.fetchone()[0]))
    return loaded


def main() -> None:
    if not PRD1_DIR.exists():
        raise FileNotFoundError(PRD1_DIR)
    if not PRD2_DIR.exists():
        raise FileNotFoundError(PRD2_DIR)

    with psycopg.connect(**connection_details) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA)))
            loaded = []
            loaded.extend(load_source(cursor, "prd_01", PRD1_DIR))
            loaded.extend(load_source(cursor, "prd_02", PRD2_DIR))
        connection.commit()

    print(f"Loaded {len(loaded)} tables into schema {SCHEMA}:")
    for table, count in loaded:
        print(f"{SCHEMA}.{table}: {count}")


if __name__ == "__main__":
    main()
