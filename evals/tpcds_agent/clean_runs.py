#!/usr/bin/env python
"""Deprecated shim: use ``evals/clean_results.py``.

This swept whole TPC-DS run dirs older than a cutoff. The replacement does the
same (``--runs``) across every eval suite, and adds the two sweeps that reclaim
far more for far less: ``--spill`` (dead DuckDB temp files, loses nothing) and
``--db-copies`` (per-run database copies, re-copied from ``.cache`` next run).
Almost all the disk is those two, not logs.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_REPLACEMENT = Path(__file__).resolve().parents[1] / "clean_results.py"


def main() -> int:
    args = sys.argv[1:]
    forwarded = ["--runs", "--eval", "tpcds"]
    # The old CLI took --hours; the new one takes --older-than in days.
    if "--hours" in args:
        hours = float(args[args.index("--hours") + 1])
        forwarded += ["--older-than", str(hours / 24)]
    else:
        forwarded += ["--older-than", "2"]
    if "--dry-run" not in args:
        forwarded.append("--yes")
    print(
        f"clean_runs.py is deprecated; running:\n"
        f"  python {_REPLACEMENT.relative_to(Path.cwd()) if _REPLACEMENT.is_relative_to(Path.cwd()) else _REPLACEMENT}"
        f" {' '.join(forwarded)}\n",
        file=sys.stderr,
    )
    return subprocess.call([sys.executable, str(_REPLACEMENT), *forwarded])


if __name__ == "__main__":
    raise SystemExit(main())
