from __future__ import annotations

import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MYSQLISH_SQL = ROOT / "e_360" / "sql" / "enhanced_raw_vault_source_split_load.sql"
POSTGRES_SQL = ROOT / "e_360" / "sql" / "enhanced_raw_vault_source_split_load_postgres.sql"
SOURCE_LOAD_SQL = ROOT / "e_360" / "sql" / "load_enhanced_raw_sources_postgres.sql"


def latest_run(folder: Path) -> str:
    runs = sorted([path.name for path in folder.iterdir() if path.is_dir()], reverse=True)
    if not runs:
        raise FileNotFoundError(f"No run folders found under {folder}")
    return runs[0]


def source_table_name(path: Path, source: str) -> str:
    suffix = "csv" if source == "crm" else "db"
    return f"{source}_{path.stem}_{suffix}"


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def read_header(path: Path) -> list[str]:
    with path.open("r", newline="", encoding="utf-8") as f:
        return next(csv.reader(f), [])


def create_source_table_sql(table: str, columns: list[str]) -> str:
    cols = ",\n    ".join(f"{quote_ident(column)} TEXT" for column in columns)
    return f"DROP TABLE IF EXISTS {quote_ident(table)};\nCREATE TABLE {quote_ident(table)} (\n    {cols}\n);"


def copy_sql(table: str, path: Path, columns: list[str]) -> str:
    path_text = str(path.resolve()).replace("\\", "/")
    cols = ", ".join(quote_ident(column) for column in columns)
    return f"\\copy {quote_ident(table)} ({cols}) FROM {quote_literal(path_text)} WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');"


def generate_source_load(run_id: str | None = None) -> Path:
    if run_id is None:
        run_id = latest_run(ROOT / "data" / "raw" / "enhanced" / "prd_01")
    prd1 = ROOT / "data" / "raw" / "enhanced" / "prd_01" / run_id / "vault_ready_28"
    prd2 = ROOT / "data" / "raw" / "enhanced" / "prd_02" / run_id
    if not prd1.exists():
        raise FileNotFoundError(f"Enhanced PRD1 vault_ready_28 not found: {prd1}")
    if not prd2.exists():
        raise FileNotFoundError(f"Enhanced PRD2 not found: {prd2}")

    parts = [
        "-- PostgreSQL source table loader for enhanced PRD1/PRD2 raw.",
        f"-- run_id: {run_id}",
        "BEGIN;",
    ]
    for source, folder in [("crm", prd1), ("sap", prd2)]:
        for path in sorted(folder.glob("*.csv")):
            table = source_table_name(path, source)
            columns = read_header(path)
            parts.append(create_source_table_sql(table, columns))
            parts.append(copy_sql(table, path, columns))
    parts.append("COMMIT;")
    SOURCE_LOAD_SQL.parent.mkdir(parents=True, exist_ok=True)
    SOURCE_LOAD_SQL.write_text("\n\n".join(parts) + "\n", encoding="utf-8")
    return SOURCE_LOAD_SQL


def generate_postgres_vault_sql() -> Path:
    sql = MYSQLISH_SQL.read_text(encoding="utf-8")
    sql = sql.replace(" AS CHAR", " AS TEXT")
    sql = sql.replace("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP")
    POSTGRES_SQL.parent.mkdir(parents=True, exist_ok=True)
    POSTGRES_SQL.write_text(sql, encoding="utf-8")
    return POSTGRES_SQL


def main() -> int:
    source_path = generate_source_load()
    vault_path = generate_postgres_vault_sql()
    print(source_path)
    print(vault_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
