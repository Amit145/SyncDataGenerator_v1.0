from __future__ import annotations

import argparse
import csv
import hashlib
import re
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = ROOT / "e_360" / "sql" / "enhanced_raw_vault_source_split_load.sql"
DEFAULT_DB_PATH = ROOT / "e_360" / "enhanced_raw_vault_source_split.sqlite"


def latest_run(folder: Path) -> str:
    runs = sorted([path.name for path in folder.iterdir() if path.is_dir()], reverse=True)
    if not runs:
        raise FileNotFoundError(f"No run folders found under {folder}")
    return runs[0]


def table_name_for(path: Path, source: str) -> str:
    stem = path.stem
    return f"{source}_{stem}_{'csv' if source == 'crm' else 'db'}"


def sqlite_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


def create_and_load_csv(conn: sqlite3.Connection, table_name: str, path: Path) -> int:
    columns, rows = read_csv_rows(path)
    conn.execute(f"DROP TABLE IF EXISTS {sqlite_identifier(table_name)}")
    column_sql = ", ".join(f"{sqlite_identifier(column)} TEXT" for column in columns)
    conn.execute(f"CREATE TABLE {sqlite_identifier(table_name)} ({column_sql})")
    if rows:
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO {sqlite_identifier(table_name)} ({', '.join(sqlite_identifier(c) for c in columns)}) VALUES ({placeholders})"
        conn.executemany(insert_sql, [[row.get(column, "") for column in columns] for row in rows])
    return len(rows)


def register_functions(conn: sqlite3.Connection) -> None:
    def md5(value) -> str:
        return hashlib.md5(str(value or "").encode("utf-8")).hexdigest()

    def concat(*values) -> str:
        return "".join("" if value is None else str(value) for value in values)

    def concat_ws(separator, *values) -> str:
        sep = "" if separator is None else str(separator)
        return sep.join(str(value) for value in values if value is not None and str(value) != "")

    conn.create_function("MD5", 1, md5)
    conn.create_function("CONCAT", -1, concat)
    conn.create_function("CONCAT_WS", -1, concat_ws)


def translate_mysql_to_sqlite(sql: str) -> str:
    sql = re.sub(r"TRUNCATE TABLE ([A-Za-z0-9_]+);", r"DELETE FROM \1;", sql)
    sql = sql.replace("CAST(", "CAST(")
    return sql


def split_sql(sql: str) -> list[str]:
    sql = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    statements: list[str] = []
    current: list[str] = []
    in_single = False
    for char in sql:
        current.append(char)
        if char == "'":
            in_single = not in_single
        if char == ";" and not in_single:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement[:-1].strip())
            current = []
    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return [statement for statement in statements if statement]


def execute_vault_sql(conn: sqlite3.Connection, sql_path: Path) -> None:
    sql = translate_mysql_to_sqlite(sql_path.read_text(encoding="utf-8"))
    for statement in split_sql(sql):
        conn.execute(statement)


def table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'hub_%' OR name LIKE 'link_%' OR name LIKE 'sat_%') ORDER BY name"
    ).fetchall()
    return {name: conn.execute(f"SELECT COUNT(*) FROM {sqlite_identifier(name)}").fetchone()[0] for (name,) in rows}


def build_database(run_id: str | None, db_path: Path, sql_path: Path) -> None:
    if run_id is None:
        run_id = latest_run(ROOT / "data" / "raw" / "enhanced" / "prd_01")

    prd1 = ROOT / "data" / "raw" / "enhanced" / "prd_01" / run_id / "vault_ready_28"
    prd2 = ROOT / "data" / "raw" / "enhanced" / "prd_02" / run_id
    if not prd1.exists():
        raise FileNotFoundError(f"Enhanced PRD1 vault_ready_28 not found: {prd1}")
    if not prd2.exists():
        raise FileNotFoundError(f"Enhanced PRD2 not found: {prd2}")
    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        register_functions(conn)
        loaded = []
        for path in sorted(prd1.glob("*.csv")):
            table = table_name_for(path, "crm")
            loaded.append((table, create_and_load_csv(conn, table, path)))
        for path in sorted(prd2.glob("*.csv")):
            table = table_name_for(path, "sap")
            loaded.append((table, create_and_load_csv(conn, table, path)))
        execute_vault_sql(conn, sql_path)
        conn.commit()

        counts = table_counts(conn)
        print(f"SQLite database: {db_path}")
        print(f"run_id: {run_id}")
        print(f"raw tables loaded: {len(loaded)}")
        print(f"vault tables built: {len(counts)}")
        print(f"hubs={sum(1 for name in counts if name.startswith('hub_'))} links={sum(1 for name in counts if name.startswith('link_'))} sats={sum(1 for name in counts if name.startswith('sat_'))}")
        for name in ["hub_person", "hub_policy", "hub_insured_object", "sat_person_crm", "sat_person_sap", "sat_motor_sap", "link_person_address"]:
            if name in counts:
                print(f"{name}: {counts[name]}")
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a local SQLite DB and execute enhanced 85-table source-split vault SQL.")
    parser.add_argument("--run-id", default=None, help="Enhanced raw run id. Defaults to latest.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="Output SQLite database path.")
    parser.add_argument("--sql-path", default=str(SQL_PATH), help="SQL file to execute.")
    args = parser.parse_args()
    build_database(args.run_id, Path(args.db_path), Path(args.sql_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
