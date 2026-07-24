#!/usr/bin/env python
"""Delete eval run data under ``results/`` older than a cutoff (default 48h).

Age is taken from each top-level entry's mtime so it covers both timestamped
run dirs and the ``_repeat_*`` / loose ``.log`` outputs uniformly.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import archive
from spec import SPEC


def _human_size(num_bytes: int) -> str:
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}PB"


def _entry_size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def clean(results_dir: Path, max_age_hours: float, dry_run: bool) -> int:
    if not results_dir.exists():
        print(f"no results dir: {results_dir}")
        return 0
    cutoff = time.time() - max_age_hours * 3600
    stale = sorted(p for p in results_dir.iterdir() if p.stat().st_mtime < cutoff)
    if not stale:
        print(f"nothing older than {max_age_hours}h in {results_dir}")
        return 0
    # Archive summary stats before the raw logs are reclaimed. Kept open across
    # the whole sweep; a bad report shouldn't abort the disk cleanup.
    conn = None if dry_run else archive.connect()
    freed = archived = 0
    for path in stale:
        size = _entry_size(path)
        freed += size
        n = 0
        if path.is_dir() and conn is not None:
            try:
                n = archive.archive_run(conn, path, SPEC.short_name)
                archived += n
            except Exception as exc:
                print(f"  ! archive failed for {path.name}: {exc}")
        action = "would remove" if dry_run else "removing"
        tag = f", archived {n}q" if n else ""
        print(f"{action} {path.name} ({_human_size(size)}{tag})")
        if not dry_run:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
    if conn is not None:
        conn.close()
    verb = "would free" if dry_run else "freed"
    extra = f", archived {archived} question rows" if archived else ""
    print(f"\n{len(stale)} entries, {verb} {_human_size(freed)}{extra}")
    return len(stale)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--hours", type=float, default=48.0, help="max age to keep (default 48)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="list what would be removed only"
    )
    args = parser.parse_args()
    clean(SPEC.results_dir, args.hours, args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
