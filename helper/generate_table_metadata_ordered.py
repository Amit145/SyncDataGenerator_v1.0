import json
import re
from collections import OrderedDict

DDL_TABLE_RE = re.compile(
    r'CREATE\s+TABLE\s+"(?P<schema>[^"]*)"\."(?P<table>[^"]+)"\s*\((?P<body>.*?)\);\s*',
    re.IGNORECASE | re.DOTALL
)


def _infer_table_type(table_name: str) -> str:
    n = table_name.strip().lower()
    if n.startswith("hub "):
        return "HUB"
    if n.startswith("sat "):
        return "SAT"
    if n.startswith("link "):
        return "LINK"
    return "UNKNOWN"


def _parse_create_table_blocks(ddl_text: str):
    """Return dict: table_name -> {schema, table_name, table_type, columns(OrderedDict)}"""
    out = {}
    for m in DDL_TABLE_RE.finditer(ddl_text):
        schema = m.group("schema")
        table = m.group("table")
        body = m.group("body")

        # Primary key columns from constraint
        pk_cols = []
        pk_m = re.search(r'PRIMARY\s+KEY\s*\((?P<cols>[^)]+)\)', body, re.IGNORECASE)
        if pk_m:
            pk_cols = [c.strip().strip('"') for c in pk_m.group("cols").split(",")]

        # Column order as defined in DDL
        cols_in_order = []
        for line in body.splitlines():
            line = line.strip()
            if not line.startswith('"'):
                continue
            # first token is "Column Name"
            cm = re.match(r'"(?P<col>[^"]+)"\s+.*', line)
            if cm:
                cols_in_order.append(cm.group("col"))

        cols = OrderedDict()
        for c in cols_in_order:
            cols[c] = {"column_type": "PRIMARY" if c in pk_cols else "NULL"}

        out[table] = {
            "schema": schema,
            "table_name": table,
            "table_type": _infer_table_type(table),
            "columns": cols
        }
    return out


def _build_ordered_metadata(ddl_path: str, template_json_path: str | None = None):
    ddl_text = open(ddl_path, "r", encoding="utf-8", errors="ignore").read()
    parsed = _parse_create_table_blocks(ddl_text)

    if template_json_path:
        template = json.load(open(template_json_path, "r", encoding="utf-8"))
        ordered = []
        missing = []
        for t in template:
            name = t.get("table_name")
            if name in parsed:
                # Use parsed columns/schema but keep exact table_name & table_type from template (if present)
                rec = parsed[name]
                rec["schema"] = t.get("schema", rec["schema"])
                rec["table_type"] = t.get("table_type", rec["table_type"])
                ordered.append(rec)
            else:
                missing.append(name)
                # Fallback: keep template record as-is (so output still matches template order)
                ordered.append(t)

        # Append any tables found in DDL but not in template (stable: HUB->SAT->LINK->UNKNOWN then name)
        extras = [v for k, v in parsed.items() if k not in {x.get("table_name") for x in template}]
        type_rank = {"HUB": 0, "SAT": 1, "LINK": 2, "UNKNOWN": 3}
        extras.sort(key=lambda x: (type_rank.get(x["table_type"], 9), x["table_name"]))
        ordered.extend(extras)

        return ordered, missing, [x["table_name"] for x in extras]

    # No template: simple grouping
    type_rank = {"HUB": 0, "SAT": 1, "LINK": 2, "UNKNOWN": 3}
    ordered = sorted(parsed.values(), key=lambda x: (type_rank.get(x["table_type"], 9), x["table_name"]))
    return ordered, [], []


def generate_meta(ddl, template, out):
    ordered, missing, extras = _build_ordered_metadata(ddl, template)

    with open(out, "w", encoding="utf-8") as f:
        json.dump(ordered, f, indent=2)

    if missing:
        print(f"[WARN] {len(missing)} tables present in template but not found in DDL:")
        for n in missing:
            print(f"  - {n}")
    if extras:
        print(f"[INFO] {len(extras)} extra tables found in DDL but not in template (appended at end):")
        for n in extras:
            print(f"  - {n}")
