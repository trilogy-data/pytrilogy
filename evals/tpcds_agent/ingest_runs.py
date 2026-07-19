#!/usr/bin/env python
"""Archive finished runs under ``results/`` into eval_history.db without deleting.

Full-run finishes now auto-publish (see common/main.py), so this is only needed
to backfill runs that finished before that hook, or to re-ingest on demand. The
cleanup sweep (clean_runs.py) still archives-then-reclaims older runs. Idempotent.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import archive  # noqa: E402
from spec import SPEC  # noqa: E402


def ingest(results_dir: Path, only: set[str] | None) -> int:
    if not results_dir.exists():
        print(f"no results dir: {results_dir}")
        return 0
    dirs = sorted(p for p in results_dir.iterdir() if p.is_dir())
    total = 0
    for path in dirs:
        if only is not None and path.name not in only:
            continue
        n = archive.publish_run(path, SPEC.short_name)
        if n:
            total += n
            print(f"archived {path.name} ({n}q)")
        elif only is not None:
            print(f"skipped {path.name} (no archivable report)")
    print(f"\narchived {total} question rows into {archive.default_db_path().name}")
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "runs", nargs="*", help="run dir names to ingest (default: all under results/)"
    )
    args = parser.parse_args()
    ingest(SPEC.results_dir, set(args.runs) or None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
