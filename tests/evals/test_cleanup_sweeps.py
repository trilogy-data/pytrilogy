"""Unit tests for the disk-reclaim sweeps.

These delete things, so what they will and will not touch is pinned here: spill
and database copies yes, agent logs and the agent's query files never, and
nothing at all in a run that was written to recently."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "evals"))

from common import cleanup


def make_run(results: Path, name: str, *, age_hours: float = 48.0) -> Path:
    run = results / name
    worker = run / "workspace" / "_worker_0"
    (worker / "warehouse.duckdb.tmp").mkdir(parents=True)
    (worker / "warehouse.duckdb.tmp" / "duckdb_temp_storage_DEFAULT-3.tmp").write_bytes(
        b"x" * 5000
    )
    (worker / "warehouse.duckdb").write_bytes(b"d" * 900)
    (worker / "query05.preql").write_text("select 1;", encoding="utf-8")
    (run / "workspace" / "warehouse.duckdb").write_bytes(b"d" * 900)
    (run / "workspace" / "query05.preql").write_text("select 1;", encoding="utf-8")
    (run / "agent_log.q05.jsonl").write_text(
        '{"type":"session_start"}', encoding="utf-8"
    )
    (run / "report.json").write_text("{}", encoding="utf-8")
    old = time.time() - age_hours * 3600
    for root, _, files in os.walk(run):
        for f in files:
            os.utime(os.path.join(root, f), (old, old))
    return run


def test_spill_sweep_takes_only_spill(tmp_path):
    results = tmp_path / "results"
    run = make_run(results, "20260101-000000_enriched")
    plan = cleanup.plan_sweep(results, spill=True, db_copies=False)
    assert plan.bytes == 5000
    cleanup.apply_sweep(plan)
    survivors = {p.name for p in run.rglob("*") if p.is_file()}
    assert survivors == {
        "warehouse.duckdb",
        "query05.preql",
        "agent_log.q05.jsonl",
        "report.json",
    }
    assert not list(run.rglob("*.duckdb.tmp"))


def test_db_copy_sweep_keeps_logs_and_queries(tmp_path):
    results = tmp_path / "results"
    run = make_run(results, "20260101-000000_enriched")
    plan = cleanup.plan_sweep(results, spill=True, db_copies=True)
    assert plan.bytes == 5000 + 900 * 2
    cleanup.apply_sweep(plan)
    survivors = {p.name for p in run.rglob("*") if p.is_file()}
    assert survivors == {"query05.preql", "agent_log.q05.jsonl", "report.json"}


def test_a_run_in_flight_is_never_touched(tmp_path):
    results = tmp_path / "results"
    make_run(results, "20260101-000000_running", age_hours=0.5)
    plan = cleanup.plan_sweep(results, spill=True, db_copies=True, skip_recent_hours=6)
    assert plan.targets == []
    assert plan.skipped_recent == ["20260101-000000_running"]


def test_min_age_holds_back_recent_but_idle_runs(tmp_path):
    results = tmp_path / "results"
    make_run(results, "fresh", age_hours=24)
    make_run(results, "stale", age_hours=24 * 9)
    plan = cleanup.plan_sweep(
        results, spill=True, db_copies=True, min_age_hours=24 * 7, skip_recent_hours=6
    )
    assert plan.runs == {"stale"}


def test_sweeping_a_clean_tree_finds_nothing(tmp_path):
    results = tmp_path / "results"
    run = results / "20260101-000000_enriched"
    run.mkdir(parents=True)
    (run / "agent_log.q01.jsonl").write_text("{}", encoding="utf-8")
    os.utime(run / "agent_log.q01.jsonl", (time.time() - 99999, time.time() - 99999))
    plan = cleanup.plan_sweep(results, spill=True, db_copies=True)
    assert plan.bytes == 0 and cleanup.apply_sweep(plan) == 0


def test_purge_spill_takes_a_workers_leftovers_and_nothing_else(tmp_path):
    """What run_eval does as each worker frees up."""
    run = make_run(tmp_path / "results", "20260101-000000_enriched", age_hours=0)
    worker = run / "workspace" / "_worker_0"
    lines: list[str] = []
    freed = cleanup.purge_spill(worker, log=lines.append)
    assert freed == 5000
    assert lines and "reclaimed" in lines[0]
    assert (worker / "warehouse.duckdb").exists()
    assert (worker / "query05.preql").exists()
    assert (run / "agent_log.q05.jsonl").exists()
    # Second pass is a no-op, and says nothing.
    lines.clear()
    assert cleanup.purge_spill(worker, log=lines.append) == 0 and not lines


def test_purge_spill_ignores_a_run_with_none(tmp_path):
    run = tmp_path / "results" / "clean_run"
    run.mkdir(parents=True)
    (run / "agent_log.q01.jsonl").write_text("{}", encoding="utf-8")
    assert cleanup.purge_spill(run) == 0
