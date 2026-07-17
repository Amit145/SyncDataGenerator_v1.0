import json
from pathlib import Path

from build_combined_configs import (
    OUTPUT_DIR,
    SOURCE_DIR,
    load_template,
    read_text,
    split_hub_queries,
    split_labeled_queries,
)


SQL_OUTPUT_DIR = OUTPUT_DIR / "standalone_sql"


def strip_semicolon(sql: str) -> str:
    return sql.strip().removesuffix(";").strip()


def sql_cell(target_table: str, sql: str) -> dict:
    statement = f"CREATE OR REPLACE TABLE {target_table} AS\n{strip_semicolon(sql)};\n"
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": statement.splitlines(keepends=True),
    }


def write_sql_notebook(object_type: str, blocks: list[tuple[str, str]]) -> Path:
    nb = load_template(object_type)
    nb["cells"] = [sql_cell(target, sql) for target, sql in blocks]
    SQL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = SQL_OUTPUT_DIR / f"{object_type}_create_or_replace_tables.ipynb"
    out_path.write_text(json.dumps(nb, indent=1), encoding="utf-8")
    return out_path


def main() -> None:
    definitions = {
        "hub": split_hub_queries(read_text(SOURCE_DIR / "hub_queries_vault.txt")),
        "link": split_labeled_queries(read_text(SOURCE_DIR / "LINK_queries_vault.txt")),
        "sat": split_labeled_queries(read_text(SOURCE_DIR / "sat_queries_vault.txt")),
    }
    for object_type, blocks in definitions.items():
        out_path = write_sql_notebook(object_type, blocks)
        print(f"{object_type}: {len(blocks)} SQL cells -> {out_path}")


if __name__ == "__main__":
    main()
