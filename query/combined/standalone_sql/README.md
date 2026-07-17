# Standalone SQL Notebooks

Each notebook has one SQL cell per vault table.

Outputs:

- `hub_create_or_replace_tables.ipynb`: 25 cells
- `link_create_or_replace_tables.ipynb`: 30 cells
- `sat_create_or_replace_tables.ipynb`: 32 cells

Each cell follows:

```sql
CREATE OR REPLACE TABLE <vault_table> AS
<source query>;
```

Regenerate:

```powershell
python query\build_standalone_sql_notebooks.py
```
