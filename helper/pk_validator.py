def assert_unique(rows, key):
    vals = [r[key] for r in rows]
    assert len(vals) == len(set(vals)), f"Duplicate {key}"


def fk_check(child_rows, fk, parent_rows, pk):
    parents = set(r[pk] for r in parent_rows)
    for r in child_rows:
        assert r[fk] in parents, f"Orphan FK {fk}"
