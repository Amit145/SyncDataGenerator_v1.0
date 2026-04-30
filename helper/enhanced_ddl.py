from __future__ import annotations

import re
from pathlib import Path


ENHANCED_DDL_PATH = Path(__file__).resolve().parents[1] / "enhanced_360" / "Enhanced_Customer360_DataVault_DDL.sql"


def parse_enhanced_ddl(path: str | Path = ENHANCED_DDL_PATH) -> dict:
    ddl_path = Path(path)
    text = ddl_path.read_text(encoding="utf-8")

    tables: dict[str, list[str]] = {}
    for match in re.finditer(r"CREATE\s+TABLE\s+(\w+)\s*\((.*?)\);", text, flags=re.IGNORECASE | re.DOTALL):
        table_name = match.group(1).lower()
        body = match.group(2)
        columns = []
        for raw_line in body.splitlines():
            line = raw_line.strip().rstrip(",")
            if not line or line.upper().startswith("CONSTRAINT"):
                continue
            column_name = line.split()[0].strip('"').lower()
            columns.append(column_name)
        tables[table_name] = columns

    primary_keys: dict[str, list[str]] = {}
    for match in re.finditer(
        r"ALTER\s+TABLE\s+(\w+)\s+ADD\s+CONSTRAINT\s+\w+\s+PRIMARY\s+KEY\s*\((.*?)\);",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        table_name = match.group(1).lower()
        primary_keys[table_name] = [
            col.strip().strip('"').lower()
            for col in match.group(2).replace("\n", " ").split(",")
        ]

    foreign_keys = []
    for match in re.finditer(
        r"ALTER\s+TABLE\s+(\w+)\s+ADD\s+CONSTRAINT\s+\w+\s+FOREIGN\s+KEY\s*\((.*?)\)\s+REFERENCES\s+(\w+)\s*\((.*?)\);",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        foreign_keys.append({
            "child_table": match.group(1).lower(),
            "child_columns": [
                col.strip().strip('"').lower()
                for col in match.group(2).replace("\n", " ").split(",")
            ],
            "parent_table": match.group(3).lower(),
            "parent_columns": [
                col.strip().strip('"').lower()
                for col in match.group(4).replace("\n", " ").split(",")
            ],
        })

    return {
        "tables": tables,
        "primary_keys": primary_keys,
        "foreign_keys": foreign_keys,
    }
