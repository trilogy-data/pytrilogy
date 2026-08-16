"""Render every corpus query to SQL and dump {relpath: sql} as JSON.

Refactor gate: a change that is meant to be semantics-preserving must produce a
byte-identical dump. Usage: python local_scripts/_ab_render_corpus.py out.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from trilogy import Dialects, Environment

ROOT = Path(__file__).resolve().parents[1] / "tests" / "modeling"
SUITES = ("tpc_ds_duckdb", "tpc_h")


def main() -> int:
    out: dict[str, str] = {}
    for suite in SUITES:
        base = ROOT / suite
        for path in sorted(base.glob("query*.preql")):
            key = f"{suite}/{path.name}"
            try:
                executor = Dialects.DUCK_DB.default_executor(
                    environment=Environment(working_path=base)
                )
                out[key] = executor.generate_sql(path.read_text())[-1]
            except Exception as exc:  # a failure is itself a comparable fact
                out[key] = f"!!ERROR {type(exc).__name__}: {exc}"
    Path(sys.argv[1]).write_text(json.dumps(out, indent=1), encoding="utf-8")
    print(f"{len(out)} queries -> {sys.argv[1]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
