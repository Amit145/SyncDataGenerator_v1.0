"""Import an ODCS Excel workbook to YAML and lint it with datacontract-cli.

This is the ad hoc terminal equivalent of the Excel import/lint step that
`main.py` runs through `run_data_contracts.py`.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path, help="Path to the ODCS Excel workbook.")
    parser.add_argument(
        "--output",
        type=Path,
        help="Output YAML path. Defaults to '<source_stem>_imported.yaml' beside the Excel file.",
    )
    parser.add_argument("--skip-lint", action="store_true", help="Only import Excel to YAML; do not run lint.")
    parser.add_argument("--quiet", action="store_true", help="Only print the final status.")
    return parser.parse_args()


def default_output_path(source: Path) -> Path:
    stem = source.stem
    if stem.endswith("_ODCS"):
        stem = stem[: -len("_ODCS")]
    return source.with_name(f"{stem}_imported.yaml")


def run_command(command: list[str]) -> dict[str, object]:
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
            env=env,
        )
    except FileNotFoundError:
        return {
            "status": "fail",
            "returnCode": None,
            "stdout": "",
            "stderr": "datacontract CLI not found. Install it with: pip install datacontract-cli",
        }

    return {
        "status": "pass" if completed.returncode == 0 else "fail",
        "returnCode": completed.returncode,
        "stdout": (completed.stdout or "").strip(),
        "stderr": (completed.stderr or "").strip(),
    }


def print_result(label: str, command: list[str], result: dict[str, object], quiet: bool) -> None:
    if quiet:
        return
    print(f"{label}: {result['status']}")
    print("  command:", " ".join(command))
    stdout = str(result.get("stdout") or "")
    stderr = str(result.get("stderr") or "")
    if stdout:
        print("  stdout:")
        print(stdout)
    if stderr:
        print("  stderr:")
        print(stderr)


def main() -> int:
    args = parse_args()
    source = args.source
    output = args.output or default_output_path(source)

    if not source.exists():
        print(f"ERROR: Excel source not found: {source}", file=sys.stderr)
        return 1

    output.parent.mkdir(parents=True, exist_ok=True)

    import_command = [
        "datacontract",
        "import",
        "excel",
        "--source",
        str(source),
        "--output",
        str(output),
    ]
    import_result = run_command(import_command)
    print_result("IMPORT", import_command, import_result, args.quiet)

    lint_result: dict[str, object] | None = None
    if import_result["status"] == "pass" and not args.skip_lint:
        lint_command = ["datacontract", "lint", str(output)]
        lint_result = run_command(lint_command)
        print_result("LINT", lint_command, lint_result, args.quiet)

    final_status = "pass"
    if import_result["status"] != "pass" or (lint_result and lint_result["status"] != "pass"):
        final_status = "fail"

    print(f"EXCEL CONTRACT IMPORT/LINT: {final_status}")
    print(f"  source: {source}")
    print(f"  output: {output}")
    return 0 if final_status == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
