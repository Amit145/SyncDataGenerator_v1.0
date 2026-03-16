import json
import re
from pathlib import Path
from typing import List, Dict

from config.runConfig import PARSED_DDL_PATH


def split_sql_statements(ddl: str) -> List[str]:
    """
    Splits raw DDL into individual SQL statements.
    Preserves multi-line CREATE TABLE blocks.
    """
    # Remove excessive whitespace but keep structure
    ddl = ddl.strip()

    # Split on semicolon followed by optional whitespace/newline
    statements = re.split(r';\s*\n', ddl)

    # Clean up
    statements = [stmt.strip() + ';'
                  for stmt in statements if stmt.strip()]

    return statements


def classify_statement(stmt: str) -> Dict:
    """
    Classifies a single SQL statement and extracts minimal metadata.
    """

    stmt_upper = stmt.upper()

    # ---------- CREATE TABLE ----------
    if stmt_upper.startswith("CREATE TABLE"):
        table_name = re.search(r'CREATE TABLE\s+""\."([^"]+)"', stmt, re.I)
        return {
            "type": "CREATE_TABLE",
            "table_name": table_name.group(1) if table_name else None,
            "sql": stmt
        }

    # ---------- CREATE INDEX ----------
    if stmt_upper.startswith("CREATE INDEX"):
        index_name = re.search(r'CREATE INDEX\s+""\."([^"]+)"', stmt, re.I)
        table_name = re.search(r'ON\s+""\."([^"]+)"', stmt, re.I)
        return {
            "type": "CREATE_INDEX",
            "index_name": index_name.group(1) if index_name else None,
            "table_name": table_name.group(1) if table_name else None,
            "sql": stmt
        }

    # ---------- CREATE UNIQUE INDEX ----------
    if stmt_upper.startswith("CREATE UNIQUE INDEX"):
        index_name = re.search(r'CREATE UNIQUE INDEX\s+""\."([^"]+)"', stmt, re.I)
        table_name = re.search(r'ON\s+""\."([^"]+)"', stmt, re.I)
        return {
            "type": "CREATE_UNIQUE_INDEX",
            "index_name": index_name.group(1) if index_name else None,
            "table_name": table_name.group(1) if table_name else None,
            "sql": stmt
        }

    # ---------- ALTER TABLE (FOREIGN KEY) ----------
    if stmt_upper.startswith("ALTER TABLE"):
        src_table = re.search(r'ALTER TABLE\s+""\."([^"]+)"', stmt, re.I)
        fk_column = re.search(r'FOREIGN KEY\s*\("([^"]+)"\)', stmt, re.I)
        tgt_table = re.search(r'REFERENCES\s+""\."([^"]+)"', stmt, re.I)
        tgt_column = re.search(r'REFERENCES.*\("([^"]+)"\)', stmt, re.I)

        return {
            "type": "ALTER_TABLE_FK",
            "source_table": src_table.group(1) if src_table else None,
            "source_column": fk_column.group(1) if fk_column else None,
            "target_table": tgt_table.group(1) if tgt_table else None,
            "target_column": tgt_column.group(1) if tgt_column else None,
            "sql": stmt
        }

    # ---------- FALLBACK ----------
    return {
        "type": "UNKNOWN",
        "sql": stmt
    }


def parse_ddl(ddl: str) -> List[Dict]:
    statements = split_sql_statements(ddl)
    parsed = [classify_statement(stmt) for stmt in statements]
    return parsed


def read_ddl_from_file(file_path: str) -> str:
    """
    Reads DDL from a .sql file
    """
    return Path(file_path).read_text(encoding="utf-8")


def process_ddl(ddl_text: str):
    """
    Parses DDL and returns classified SQL statements
    """
    parsed_statements = parse_ddl(ddl_text)
    return parsed_statements


def filter_relevant_statements(parsed_statements):
    """
    Keeps only CREATE_TABLE and ALTER_TABLE_FK statements
    """
    allowed_types = {"CREATE_TABLE", "ALTER_TABLE_FK"}

    return [
        stmt for stmt in parsed_statements
        if stmt.get("type") in allowed_types
    ]

def file_ready(path: str) -> bool:
    p = Path(path)
    return p.exists() and p.is_file() and p.stat().st_size > 0

def parse_ddl_module(ddl_file_path):
    ddl_text = read_ddl_from_file(ddl_file_path)
    parsed = process_ddl(ddl_text)
    filtered = filter_relevant_statements(parsed)

    for i, stmt in enumerate(filtered, start=1):
        print(f"\n--- Statement {i} ---")
        print("Type:", stmt["type"])
        print("SQL:")
        print(stmt["sql"])

    with open(PARSED_DDL_PATH, "w", encoding="utf-8") as f:
        json.dump(filtered, f, indent=2)
