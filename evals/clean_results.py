#!/usr/bin/env python
"""Reclaim disk from eval run dirs, across every eval suite.

Three modes, cheapest first (see ``common/cleanup.py`` for what each costs):

    python evals/clean_results.py --spill                  # dead DuckDB temp files
    python evals/clean_results.py --spill --db-copies --older-than 7
    python evals/clean_results.py --runs --older-than 2    # archive, then delete run dirs

``--spill``/``--db-copies`` keep every log, report and agent query; ``--runs``
is the old whole-dir sweep and archives each run into the history db first.
Nothing touched in the last ``--skip-recent`` hours is considered, so a run in
flight is never disturbed. Prints the plan and asks for ``--yes`` to act.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import archive, cleanup
from viewer.suites import discover_suites


def _sweep_regenerable(args, suites) -> tuple[int, int]:
    """(planned bytes, freed bytes) for the spill / db-copy sweeps."""
    planned = freed = 0
    for suite in suites.values():
        plan = cleanup.plan_sweep(
            suite.results_dir,
            spill=args.spill,
            db_copies=args.db_copies,
            min_age_hours=args.older_than * 24,
            skip_recent_hours=args.skip_recent,
        )
        if not plan.targets:
            print(f"{suite.key}: nothing to reclaim", end="")
            print(
                f" ({len(plan.skipped_recent)} run(s) too recent to touch)"
                if plan.skipped_recent
                else ""
            )
            continue
        print(
            f"{suite.key}: {cleanup.human(plan.bytes)} across {len(plan.targets)} "
            f"item(s) in {len(plan.runs)} run(s)"
            + (
                f", skipping {len(plan.skipped_recent)} recent"
                if plan.skipped_recent
                else ""
            )
        )
        for target, size in sorted(plan.targets, key=lambda t: -t[1])[:5]:
            print(
                f"    {cleanup.human(size):>9}  {target.relative_to(suite.results_dir)}"
            )
        if len(plan.targets) > 5:
            print(f"    ... and {len(plan.targets) - 5} more")
        planned += plan.bytes
        if args.yes:
            freed += cleanup.apply_sweep(plan)
    return planned, freed


def _sweep_runs(args, suites) -> tuple[int, int]:
    """The old behaviour: archive a run, then delete the whole dir."""
    now = time.time()
    planned = freed = archived = 0
    conn = archive.connect() if args.yes else None
    try:
        for suite in suites.values():
            if not suite.results_dir.is_dir():
                continue
            for path in sorted(suite.results_dir.iterdir()):
                idle_hours = (now - path.stat().st_mtime) / 3600
                if idle_hours < max(args.older_than * 24, args.skip_recent):
                    continue
                size = cleanup.size_of(path)
                rows = 0
                if conn is not None and path.is_dir():
                    try:
                        rows = archive.archive_run(conn, path, suite.key)
                        archived += rows
                    except Exception as exc:
                        print(f"  ! archive failed for {path.name}: {exc}")
                        continue  # never delete what we failed to archive
                print(
                    f"{'removing' if args.yes else 'would remove'} {suite.key}/{path.name}"
                    f" ({cleanup.human(size)}{f', archived {rows}q' if rows else ''})"
                )
                planned += size
                if args.yes:
                    if path.is_dir():
                        shutil.rmtree(path)
                    else:
                        path.unlink()
                    freed += size
    finally:
        if conn is not None:
            conn.close()
    if archived:
        print(
            f"archived {archived} question rows into {archive.default_db_path().name}"
        )
    return planned, freed


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--spill", action="store_true", help="delete DuckDB temp spill (loses nothing)"
    )
    p.add_argument(
        "--db-copies", action="store_true", help="delete per-run database copies"
    )
    p.add_argument(
        "--runs", action="store_true", help="archive and delete whole run dirs"
    )
    p.add_argument(
        "--older-than", type=float, default=0.0, help="only runs idle this many DAYS"
    )
    p.add_argument(
        "--skip-recent",
        type=float,
        default=6.0,
        help="never touch runs used in the last N hours",
    )
    p.add_argument("--eval", dest="suite", help="one suite only (default: all)")
    p.add_argument(
        "--yes", action="store_true", help="actually delete (default: dry run)"
    )
    args = p.parse_args()
    if not (args.spill or args.db_copies or args.runs):
        p.error("pick at least one of --spill / --db-copies / --runs")

    suites = discover_suites()
    if args.suite:
        if args.suite not in suites:
            p.error(
                f"unknown eval {args.suite!r}; available: {', '.join(sorted(suites))}"
            )
        suites = {args.suite: suites[args.suite]}
    if not args.yes:
        print("DRY RUN - pass --yes to delete\n")

    planned, freed = (
        _sweep_runs(args, suites) if args.runs else _sweep_regenerable(args, suites)
    )
    if args.yes:
        print(f"\nfreed {cleanup.human(freed)}")
    else:
        print(f"\nwould free {cleanup.human(planned)} (re-run with --yes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
