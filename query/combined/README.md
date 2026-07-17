# Combined Vault Config Notebooks

Generated from `query/source` using `query/build_combined_configs.py`.

Product id: `9`

Outputs:

- `hub_config_9.ipynb`: 25 hub configs, process order `1`
- `link_config_9.ipynb`: 30 link configs, process order `2`
- `sat_config_9.ipynb`: 32 satellite configs, process order `3`

Notes:

- Link and satellite config names are taken from the labels in the source text files.
- Hub source SQL has no labels, so hub config names are mapped by source query order to the 25 hub names in `query/build_combined_configs.py`.
- Original expected notebooks under `query/expected` were not modified.

Regenerate:

```powershell
python query\build_combined_configs.py
```
