from dotenv import load_dotenv
from collections import defaultdict, deque
from tqdm import tqdm
import json

from config.runConfig import (
    PARSED_DDL_PATH,
    TABLE_METADATA_PATH,
    ORDERED_TABLE_METADATA_PATH
)

from modules.openai_call import convert_with_llm

load_dotenv()


def prompt_template(create_table_sql: str) -> str:
    return f"""
You are a senior Data Vault 2.0 architect.

Your task is to analyze a SINGLE SQL CREATE TABLE statement and return
a STRICTLY VALID JSON object describing the table.

CRITICAL OUTPUT RULES (MANDATORY):
1. Output MUST be valid JSON.
2. Do NOT include markdown, comments, explanations, or text outside JSON.
3. Do NOT wrap JSON in code fences.
4. Do NOT add trailing commas.
5. Do NOT infer foreign keys unless explicitly visible in the CREATE TABLE.
6. If unsure, use "NULL" as the column type.

-----------------------------------
CLASSIFICATION RULES:

TABLE TYPE:
- HUB:
  - Contains a single business key hash column (e.g. "* Hash Key")
  - Has no descriptive attributes
- SAT:
  - Contains a hash key + descriptive attributes
  - Typically has "Load Date" which is today's date
- LINK:
  - Contains two or more hash keys
  - No descriptive attributes

COLUMN TYPE:
- PRIMARY:
  - Column appears in PRIMARY KEY constraint
- FOREIGN:
  - ONLY mark FOREIGN if explicitly declared inside CREATE TABLE
- NULL:
  - All other columns

-----------------------------------
JSON STRUCTURE (MUST MATCH EXACTLY):

{{
  "schema": "<schema name or empty string>",
  "table_name": "<table name>",
  "table_type": "HUB | SAT | LINK",
  "columns": {{
    "<column_name>": {{
      "column_type": "PRIMARY | FOREIGN | NULL"
    }}
  }}
}}

-----------------------------------
DDL TO ANALYZE:

{create_table_sql}

-----------------------------------
REMINDER:
Return ONLY the JSON object. No extra text.
""".strip()


def classify_tables_with_llm(parsed_statements):
    """
    parsed_statements: output of your DDL parser
    """
    results = []

    create_table_stmts = [
        stmt for stmt in parsed_statements
        if stmt.get("type") == "CREATE_TABLE"
    ]

    for stmt in tqdm(
            create_table_stmts,
            desc="Processing CREATE TABLE statements",
            unit="table"
    ):
        prompt = prompt_template(stmt["sql"])

        raw_output, _ = convert_with_llm(
            prompt,
            model_name="openai"
        )

        # 🔒 CRITICAL: Parse & validate JSON
        try:
            table_json = json.loads(raw_output)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"LLM returned invalid JSON for table DDL:\n{stmt['sql']}\n\nOutput:\n{raw_output}"
            ) from e

        results.append(table_json)

    return results


# from llm_table_classifier import classify_tables_with_llm

def load_parsed_ddl(path: str) -> list:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def enrich_foreign_keys(table_metadata, alter_table_fks):
    table_lookup = {
        table["table_name"]: table
        for table in table_metadata
    }

    for fk in alter_table_fks:
        source_table = fk["source_table"]
        source_column = fk["source_column"]

        if source_table not in table_lookup:
            print(f"⚠️ FK source table not found: {source_table}")
            continue

        table = table_lookup[source_table]
        columns = table.get("columns", {})

        if source_column not in columns:
            print(
                f"⚠️ FK column not found: {source_table}.{source_column}"
            )
            continue

        current_type = columns[source_column]["column_type"]

        # Do not downgrade PRIMARY
        if current_type != "PRIMARY":
            columns[source_column]["column_type"] = "FOREIGN"

    return list(table_lookup.values())


def build_dependency_graph(table_metadata):
    """
    Returns:
      graph: { parent_table: set(child_tables) }
      in_degree: { table: number_of_dependencies }
    """

    graph = defaultdict(set)
    in_degree = {table["table_name"]: 0 for table in table_metadata}

    # Map column -> table for PRIMARY keys
    primary_key_owners = {}

    for table in table_metadata:
        table_name = table["table_name"]
        for col_name, col_meta in table["columns"].items():
            if col_meta["column_type"] == "PRIMARY":
                primary_key_owners[col_name] = table_name

    # Build edges using FOREIGN keys
    for table in table_metadata:
        child_table = table["table_name"]

        for col_name, col_meta in table["columns"].items():
            if col_meta["column_type"] == "FOREIGN":
                parent_table = primary_key_owners.get(col_name)

                if not parent_table:
                    raise ValueError(
                        f"Foreign key column '{col_name}' in '{child_table}' "
                        f"has no matching PRIMARY key table"
                    )

                if child_table not in graph[parent_table]:
                    graph[parent_table].add(child_table)
                    in_degree[child_table] += 1

    return graph, in_degree


def topological_sort(graph, in_degree):
    queue = deque(
        table for table, degree in in_degree.items() if degree == 0
    )

    sorted_tables = []

    while queue:
        table = queue.popleft()
        sorted_tables.append(table)

        for dependent in graph.get(table, []):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if len(sorted_tables) != len(in_degree):
        raise ValueError("Cycle detected in table dependencies")

    return sorted_tables


def reorder_tables_for_generation(table_metadata):
    graph, in_degree = build_dependency_graph(table_metadata)
    generation_order = topological_sort(graph, in_degree)

    metadata_lookup = {
        table["table_name"]: table
        for table in table_metadata
    }

    return [metadata_lookup[name] for name in generation_order]


def inference_module():
    parsed_ddl = load_parsed_ddl(PARSED_DDL_PATH)

    table_metadata = classify_tables_with_llm(parsed_ddl)

    alter_table_fks = [
        stmt for stmt in parsed_ddl
        if stmt.get("type") == "ALTER_TABLE_FK"
    ]

    table_metadata = enrich_foreign_keys(
        table_metadata=table_metadata,
        alter_table_fks=alter_table_fks
    )

    json.dumps(table_metadata)  # validation

    with open(TABLE_METADATA_PATH, "w", encoding="utf-8") as f:
        json.dump(table_metadata, f, indent=2, ensure_ascii=False)

    print(f"✅ Final table metadata written → {TABLE_METADATA_PATH}")

    ordered_metadata = reorder_tables_for_generation(table_metadata)

    with open(ORDERED_TABLE_METADATA_PATH, "w", encoding="utf-8") as f:
        json.dump(ordered_metadata, f, indent=2, ensure_ascii=False)
