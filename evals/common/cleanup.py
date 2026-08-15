"""Reclaiming disk from eval run dirs, in order of what it costs you.

Almost nothing under ``results/`` is evidence. A TPC-DS sweep leaves behind two
kinds of regenerable bulk that dwarf everything else:

``spill``      DuckDB temp files (``duckdb_temp_storage_*.tmp``, ``*.duckdb.tmp``)
               written when a query outgrew memory. The processes that wrote
               them are gone. Deleting them loses nothing at all.
``db copies``  The per-run, per-worker copy of the benchmark database. Re-copied
               from ``<eval>/.cache/`` on the next run; deleting one only costs
               in-place Replay for that run.

The agent logs, reports and the queries the agents wrote are a rounding error by
comparison, so they are never what a sweep should target first. Deleting whole
run dirs (``clean_runs.py`` with no mode flags) stays available for when the
disk really is full of logs - and archives them first.

Every sweep skips run dirs touched recently, so a run in flight is never
disturbed.
"""

from __future__ import annotations

import os
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

# Names that mark DuckDB's on-disk spill. `.duckdb.tmp` is the directory duckdb
# creates next to a database; the files inside it carry the storage prefix.
_SPILL_DIR_SUFFIX = ".duckdb.tmp"
_SPILL_FILE_MARK = "duckdb_temp_storage"
_DB_SUFFIXES = (".duckdb", ".duckdb.wal")


def size_of(path: Path) -> int:
    if path.is_file():
        try:
            return path.stat().st_size
        except OSError:
            return 0
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.stat(os.path.join(root, name)).st_size
            except OSError:
                pass
    return total


def _newest_mtime(path: Path) -> float:
    newest = 0.0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                newest = max(newest, os.stat(os.path.join(root, name)).st_mtime)
            except OSError:
                pass
    return newest


def find_spill(run_dir: Path) -> list[Path]:
    """Spill dirs (and any stray spill file) under one run dir."""
    found: list[Path] = []
    for root, dirs, files in os.walk(run_dir):
        for name in list(dirs):
            if name.endswith(_SPILL_DIR_SUFFIX):
                found.append(Path(root) / name)
                dirs.remove(name)  # take the whole dir, don't walk into it
        for name in files:
            if _SPILL_FILE_MARK in name:
                found.append(Path(root) / name)
    return found


def find_db_copies(run_dir: Path) -> list[Path]:
    """The run's own database copies. Only the databases - the agent's query
    files live in the same workspace and are what the archive reads."""
    return [
        Path(root) / name
        for root, _, files in os.walk(run_dir)
        for name in files
        if name.endswith(_DB_SUFFIXES)
    ]


@dataclass
class Plan:
    """What a sweep would do, before it does it."""

    targets: list[tuple[Path, int]] = field(default_factory=list)
    skipped_recent: list[str] = field(default_factory=list)
    runs: set[str] = field(default_factory=set)

    @property
    def bytes(self) -> int:
        return sum(size for _, size in self.targets)


def plan_sweep(
    results_dir: Path,
    *,
    spill: bool = True,
    db_copies: bool = False,
    min_age_hours: float = 0.0,
    skip_recent_hours: float = 6.0,
) -> Plan:
    """Everything a sweep of ``results_dir`` would delete.

    ``min_age_hours`` applies to the run (its newest file); ``skip_recent_hours``
    is the safety window that keeps an in-flight run untouched."""
    plan = Plan()
    if not results_dir.is_dir():
        return plan
    now = time.time()
    for run_dir in sorted(results_dir.iterdir()):
        if not run_dir.is_dir():
            continue
        idle_hours = (now - _newest_mtime(run_dir)) / 3600
        if idle_hours < max(skip_recent_hours, min_age_hours):
            if idle_hours < skip_recent_hours:
                plan.skipped_recent.append(run_dir.name)
            continue
        targets: list[Path] = []
        if spill:
            targets += find_spill(run_dir)
        if db_copies:
            targets += find_db_copies(run_dir)
        for target in targets:
            size = size_of(target)
            if size:
                plan.targets.append((target, size))
                plan.runs.add(run_dir.name)
    return plan


def purge_spill(root: Path, log: Callable[[str], None] | None = None) -> int:
    """Delete DuckDB spill under ``root`` right now, and say how much went.

    Called as a run finishes (and as each worker frees up): duckdb removes its
    own temp dir on a clean close, so what survives is what a killed or
    timed-out agent left behind. Sweeping at the source keeps peak disk near the
    size of one query's spill instead of a whole sweep's worth."""
    freed = 0
    for target in find_spill(root):
        size = size_of(target)
        try:
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        except OSError:
            continue  # a live process still holds it; the next sweep gets it
        freed += size
    if freed and log is not None:
        log(f"  reclaimed {human(freed)} of DuckDB spill from {root.name}")
    return freed


def apply_sweep(plan: Plan, log: Callable[[str], None] = print) -> int:
    """Delete what the plan found. Returns bytes actually reclaimed."""
    freed = 0
    for target, size in plan.targets:
        try:
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
            freed += size
        except OSError as exc:
            log(f"  ! could not remove {target}: {exc}")
    return freed


def human(num_bytes: float) -> str:
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}PB"
