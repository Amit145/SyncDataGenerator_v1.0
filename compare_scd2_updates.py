from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def resolve_latest_run(base_dir: Path) -> Path:
    run_dirs = [path for path in base_dir.iterdir() if path.is_dir()]
    if not run_dirs:
        raise FileNotFoundError(f"No run directories found under {base_dir}")
    return max(run_dirs, key=lambda path: path.stat().st_mtime)


def get_key_column(df: pd.DataFrame) -> str:
    candidates = [col for col in df.columns if col.lower().endswith("hash key") or col.lower().endswith("hash_key")]
    if not candidates:
        raise ValueError("Could not determine key column from CSV")
    return candidates[0]


def compare_file(original_file: Path, updated_file: Path):
    original_df = pd.read_csv(original_file)
    updated_df = pd.read_csv(updated_file)

    if original_df.empty and updated_df.empty:
        return {
            "file": original_file.name,
            "status": "unchanged",
            "changed_rows": 0,
            "changed_columns": {},
        }

    key_column = get_key_column(original_df if not original_df.empty else updated_df)

    original_df = original_df.fillna("")
    updated_df = updated_df.fillna("")

    original_df = original_df.set_index(key_column, drop=False)
    updated_df = updated_df.set_index(key_column, drop=False)

    common_keys = original_df.index.intersection(updated_df.index)
    common_columns = [col for col in original_df.columns if col in updated_df.columns]

    changed_columns: dict[str, int] = {}
    changed_rows = []

    for key in common_keys:
        original_row = original_df.loc[key]
        updated_row = updated_df.loc[key]

        row_changes = {}
        for column in common_columns:
            original_value = str(original_row[column])
            updated_value = str(updated_row[column])
            if original_value != updated_value:
                row_changes[column] = {
                    "from": original_value,
                    "to": updated_value,
                }
                changed_columns[column] = changed_columns.get(column, 0) + 1

        if row_changes:
            changed_rows.append({
                "key": str(key),
                "changes": row_changes,
            })

    return {
        "file": original_file.name,
        "status": "changed" if changed_rows else "unchanged",
        "changed_rows": len(changed_rows),
        "changed_columns": changed_columns,
        "sample_changes": changed_rows[:10],
    }


def compare_runs(original_folder: str, updated_folder: str):
    original_path = Path(original_folder)
    updated_path = Path(updated_folder)

    if not original_path.exists():
        raise FileNotFoundError(f"Original folder not found: {original_path}")
    if not updated_path.exists():
        raise FileNotFoundError(f"Updated folder not found: {updated_path}")

    original_files = {file.name: file for file in original_path.glob("*.csv")}
    updated_files = {file.name: file for file in updated_path.glob("*.csv")}

    all_file_names = sorted(set(original_files) | set(updated_files))
    results = []

    for file_name in all_file_names:
        if file_name not in original_files:
            results.append({
                "file": file_name,
                "status": "missing_in_original",
            })
            continue
        if file_name not in updated_files:
            results.append({
                "file": file_name,
                "status": "missing_in_updated",
            })
            continue

        results.append(compare_file(original_files[file_name], updated_files[file_name]))

    return results


def print_results(results):
    changed_file_count = 0

    for result in results:
        print(f"\nFILE: {result['file']}")
        print(f"STATUS: {result['status']}")

        if result["status"] != "changed":
            continue

        changed_file_count += 1
        print(f"CHANGED ROWS: {result['changed_rows']}")

        if result.get("changed_columns"):
            print("CHANGED COLUMNS:")
            for column, count in sorted(result["changed_columns"].items()):
                print(f"  - {column}: {count}")

        if result.get("sample_changes"):
            print("SAMPLE CHANGES:")
            for sample in result["sample_changes"]:
                print(f"  KEY: {sample['key']}")
                for column, change in sample["changes"].items():
                    print(f"    {column}: {change['from']} -> {change['to']}")

    print("\nSUMMARY")
    print(f"Total files compared: {len(results)}")
    print(f"Files with changes: {changed_file_count}")


def main():
    parser = argparse.ArgumentParser(description="Compare original SCD2 rows with updated SCD2 rows.")
    parser.add_argument("--original", dest="original_folder", help="Original SCD2 folder, e.g. scd2_sat/<run_id>")
    parser.add_argument("--updated", dest="updated_folder", help="Updated SCD2 folder, e.g. scd2_updated/<run_id>")
    args = parser.parse_args()

    original_folder = Path(args.original_folder) if args.original_folder else resolve_latest_run(Path("scd2_sat"))
    updated_folder = Path(args.updated_folder) if args.updated_folder else Path("scd2_updated") / original_folder.name

    results = compare_runs(str(original_folder), str(updated_folder))
    print_results(results)


if __name__ == "__main__":
    main()
