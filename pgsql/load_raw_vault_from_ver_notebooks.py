import json
import re
from pathlib import Path

import psycopg
from psycopg import sql

from db import connection_details


ROOT = Path(__file__).resolve().parents[1]
RAW_VAULT_SCHEMA = "raw_vault"
SOURCE_SCHEMA = "bronze"

NOTEBOOKS = [
    ROOT / "ver" / "hub_config (3).ipynb",
    ROOT / "ver" / "link_config (2).ipynb",
    ROOT / "ver" / "sat_config (2).ipynb",
]


def load_notebook_configs(path: Path) -> list[dict]:
    notebook = json.loads(path.read_text(encoding="utf-8"))
    namespace: dict = {}
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        try:
            exec(compile(source, str(path), "exec"), namespace)
        except NameError as exc:
            # Later Databricks cells call spark. The config dictionaries are already defined.
            if "spark" in str(exc):
                break
            raise

    configs = [
        value
        for value in namespace.values()
        if isinstance(value, dict)
        and "target_table" in value
        and "build_sql" in value
        and "object_type" in value
    ]
    return configs


def safe_cast(expr: str, target_type: str) -> str:
    return f"NULLIF(({expr})::text, '')::{target_type}"


def rewrite_sql(query: str) -> str:
    query = query.strip().rstrip(";")
    query = re.sub(r"\bCHAR\b", "text", query, flags=re.IGNORECASE)
    query = re.sub(r"\bWHERE\s+where\b", "WHERE", query, flags=re.IGNORECASE)
    query = re.sub(r"'y-M-d'|'Y-M-d'|'yyyy-MM-dd'|'YYYY-MM-DD'", "'YYYY-MM-DD'", query)

    def to_date_repl(match: re.Match) -> str:
        expr = match.group(1).strip()
        fmt = match.group(2)
        return f"to_date(NULLIF(({expr})::text, ''), {fmt})"

    query = re.sub(
        r"\bto_date\s*\(\s*([^,]+?)\s*,\s*('YYYY-MM-DD')\s*\)",
        to_date_repl,
        query,
        flags=re.IGNORECASE,
    )

    query = re.sub(
        r"\bCAST\s*\(\s*([^)]+?)\s+AS\s+timestamp\s*\)",
        lambda m: safe_cast(m.group(1).strip(), "timestamp"),
        query,
        flags=re.IGNORECASE,
    )
    query = re.sub(
        r"\bCAST\s*\(\s*([^)]+?)\s+AS\s+int(?:eger)?\s*\)",
        lambda m: safe_cast(m.group(1).strip(), "integer"),
        query,
        flags=re.IGNORECASE,
    )
    return query


def materialize_config(cursor: psycopg.Cursor, config: dict) -> int:
    target_table = config["target_table"].lower()
    query = rewrite_sql(config["build_sql"])

    cursor.execute(
        sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
            sql.Identifier(RAW_VAULT_SCHEMA),
            sql.Identifier(target_table),
        )
    )
    cursor.execute(
        sql.SQL("CREATE TABLE {}.{} AS SELECT * FROM ({}) AS src").format(
            sql.Identifier(RAW_VAULT_SCHEMA),
            sql.Identifier(target_table),
            sql.SQL(query),
        )
    )
    cursor.execute(
        sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier(RAW_VAULT_SCHEMA),
            sql.Identifier(target_table),
        )
    )
    return cursor.fetchone()[0]


def main() -> None:
    configs: list[dict] = []
    for notebook in NOTEBOOKS:
        if not notebook.exists():
            raise FileNotFoundError(notebook)
        configs.extend(load_notebook_configs(notebook))

    order = {"hub": 1, "link": 2, "sat": 3}
    deduped: dict[str, dict] = {}
    duplicates: list[str] = []
    for config in configs:
        key = config["target_table"].lower()
        if key in deduped:
            duplicates.append(key)
        deduped[key] = config
    configs = sorted(
        deduped.values(),
        key=lambda c: (order.get(c["object_type"], 99), c["target_table"]),
    )

    loaded: list[tuple[str, str, int]] = []
    with psycopg.connect(**connection_details) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(RAW_VAULT_SCHEMA))
            )
            cursor.execute(
                sql.SQL("SET search_path TO {}, public").format(sql.Identifier(SOURCE_SCHEMA))
            )
            for config in configs:
                count = materialize_config(cursor, config)
                loaded.append((config["object_type"], config["target_table"], count))
        connection.commit()

    print(f"Loaded {len(loaded)} raw vault tables into schema {RAW_VAULT_SCHEMA}:")
    if duplicates:
        print("Skipped duplicate target configs: " + ", ".join(sorted(set(duplicates))))
    for object_type, table, count in loaded:
        print(f"{RAW_VAULT_SCHEMA}.{table} ({object_type}): {count}")


if __name__ == "__main__":
    main()
