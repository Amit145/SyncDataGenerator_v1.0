import copy
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SOURCE_DIR = ROOT / "source"
EXPECTED_DIR = ROOT / "expected"
OUTPUT_DIR = ROOT / "combined"
PRODUCT_ID = "9"
CREATED_TS = "2026-06-26 10:00:00.000000"
UPDATED_TS = "2026-06-26 10:00:00.000000"

HUB_TARGETS = [
    "hub_person",
    "hub_natural_person",
    "hub_legal_person",
    "hub_contact",
    "hub_identities",
    "hub_address",
    "hub_consent",
    "hub_marketing_preference",
    "hub_marketing_engagement",
    "hub_lead",
    "hub_quote",
    "hub_policy",
    "hub_account",
    "hub_customer",
    "hub_product",
    "hub_motor",
    "hub_home",
    "hub_broker",
    "hub_campaign",
    "hub_channel",
    "hub_claim",
    "hub_complaint",
    "hub_insured_object",
    "hub_override",
    "hub_regulation",
]


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8-sig").replace("\r\n", "\n")


def split_labeled_queries(text: str) -> list[tuple[str, str]]:
    labels = list(re.finditer(r"(?m)^\s*([A-Za-z][A-Za-z0-9_]*)\s*:\s*$", text))
    blocks: list[tuple[str, str]] = []
    for idx, match in enumerate(labels):
        label = match.group(1).strip()
        start = match.end()
        end = labels[idx + 1].start() if idx + 1 < len(labels) else len(text)
        sql = text[start:end].strip()
        if sql:
            blocks.append((label, ensure_semicolon(sql)))
    return blocks


def split_hub_queries(text: str) -> list[tuple[str, str]]:
    statements = [ensure_semicolon(part.strip()) for part in text.split(";") if part.strip()]
    if len(statements) != len(HUB_TARGETS):
        raise ValueError(
            f"hub query count mismatch: found {len(statements)}, expected {len(HUB_TARGETS)}"
        )
    return list(zip(HUB_TARGETS, statements))


def ensure_semicolon(sql: str) -> str:
    sql = sql.strip()
    return sql if sql.endswith(";") else f"{sql};"


def first_hash_key(sql: str, fallback_target: str) -> str:
    match = re.search(r"\bAS\s+([A-Za-z0-9_]*_hash_key)\b", sql, re.IGNORECASE)
    if match:
        return match.group(1)
    return f"{fallback_target.removeprefix('hub_').removeprefix('link_').removeprefix('sat_')}_hash_key"


def source_tables(sql: str) -> list[str]:
    tables = re.findall(r"\b(?:crm|sap)_[A-Za-z0-9_]+_(?:csv|db)\b", sql, re.IGNORECASE)
    seen: set[str] = set()
    ordered: list[str] = []
    for table in tables:
        key = table.lower()
        if key not in seen:
            seen.add(key)
            ordered.append(table)
    return ordered


def record_source_sql(sql: str) -> str:
    tables = source_tables(sql)
    if not tables:
        return "select origin_sys from source_table"
    if len(tables) == 1:
        return f"select origin_sys from {tables[0]}"
    return " union all ".join(f"select origin_sys from {table}" for table in tables)


def merge_keys(object_type: str, target: str, sql: str) -> str:
    key = first_hash_key(sql, target)
    if object_type == "sat":
        return f"{key}|load_date"
    return key


def config_variable(target: str) -> str:
    return f"{target}_{PRODUCT_ID}"


def sql_literal(sql: str) -> str:
    return sql.replace('"""', '\\"\\"\\"')


def build_config_cell(object_type: str, blocks: list[tuple[str, str]]) -> str:
    process_order = {"hub": "1", "link": "2", "sat": "3"}[object_type]
    chunks: list[str] = []
    for target, sql in blocks:
        variable = config_variable(target)
        tables = source_tables(sql)
        chunks.append(
            f'{variable} =  {{\n'
            f'  "product_id": "{PRODUCT_ID}",\n'
            f'  "vault_object_id": "{variable}",\n'
            f'  "object_type": "{object_type}",\n'
            f'  "target_table": "{target}",\n'
            f'  "record_source": "{record_source_sql(sql)}",\n'
            f'  "process_order": "{process_order}",\n'
            f'  "merge_keys": "{merge_keys(object_type, target, sql)}",\n'
            f'  "build_sql": f"""\n{sql_literal(sql)}\n""",\n'
            f'  "source_table": "{", ".join(tables)}",\n'
            f'  "created_ts": "{CREATED_TS}",\n'
            f'  "updated_ts": "{UPDATED_TS}"\n'
            f'}}'
        )
    return "\n\n".join(chunks) + "\n"


def build_details_cell(object_type: str, blocks: list[tuple[str, str]]) -> str:
    process_order = {"hub": "1", "link": "2", "sat": "3"}[object_type]
    detail_name = f"{object_type}_details"
    lines = [
        f"{detail_name} = {{",
        f'    "process_order": "{process_order}",',
        '    "status": {',
    ]
    for target, _sql in blocks:
        variable = config_variable(target)
        lines.append(f'        "{variable}": {{"config": {variable}, "is_active": True}},')
    lines.extend(["    }", "}"])
    return "\n".join(lines) + "\n"


def build_display_cell(object_type: str) -> str:
    detail_name = f"{object_type}_details"
    return (
        "from collections import OrderedDict\n\n"
        "active_configs = []\n"
        f'process_order = {detail_name}["process_order"]\n'
        f'# Use OrderedDict to preserve order as in {detail_name}["status"]\n'
        f"for k, v in OrderedDict({detail_name}[\"status\"]).items():\n"
        '    if v["is_active"]:\n'
        '        config = v["config"].copy()\n'
        '        config["process_order"] = str(process_order)\n'
        "        active_configs.append(config)\n"
        "df = spark.createDataFrame(active_configs)\n"
        "display(df)\n"
    )


def load_template(object_type: str) -> dict:
    match = next(EXPECTED_DIR.glob(f"{object_type}_config*.ipynb"))
    return json.loads(match.read_text(encoding="utf-8"))


def as_cell_source(text: str) -> list[str]:
    return text.splitlines(keepends=True)


def write_notebook(object_type: str, blocks: list[tuple[str, str]]) -> Path:
    nb = copy.deepcopy(load_template(object_type))
    cells = nb["cells"]
    cells[0]["source"] = as_cell_source(build_config_cell(object_type, blocks))
    cells[1]["source"] = as_cell_source(build_details_cell(object_type, blocks))
    cells[2]["source"] = as_cell_source(build_display_cell(object_type))
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"{object_type}_config_{PRODUCT_ID}.ipynb"
    out_path.write_text(json.dumps(nb, indent=1), encoding="utf-8")
    return out_path


def main() -> None:
    definitions = {
        "hub": split_hub_queries(read_text(SOURCE_DIR / "hub_queries_vault.txt")),
        "link": split_labeled_queries(read_text(SOURCE_DIR / "LINK_queries_vault.txt")),
        "sat": split_labeled_queries(read_text(SOURCE_DIR / "sat_queries_vault.txt")),
    }
    for object_type, blocks in definitions.items():
        out_path = write_notebook(object_type, blocks)
        print(f"{object_type}: {len(blocks)} configs -> {out_path}")


if __name__ == "__main__":
    main()
