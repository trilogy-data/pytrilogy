"""The payload baked into a standalone ``viewer.html``.

Only the static file build uses this: with no server to fetch from, one run's
questions, trajectories and query pairs are all rendered up front. The served
page reads the same data lazily instead (``runs.py``), which is why it opens a
run in milliseconds and this takes tens of seconds.
"""

from __future__ import annotations

from pathlib import Path

from common.spec import BenchmarkSpec

from . import runs as runs_mod


def collect(results_dir: Path, spec: BenchmarkSpec, on_progress=None) -> dict:
    """``{index, trajectories, queries}`` for one run - the served endpoints'
    responses, precomputed. ``on_progress(done, total)`` fires per question:
    transpiling a full TPC-DS run is tens of seconds and should say so."""
    index = runs_mod.run_index(results_dir, spec)
    keys = [q["key"] for q in index["questions"] if q["key"]]
    trajectories: dict[str, dict] = {}
    queries: dict[str, dict] = {}
    for i, key in enumerate(keys, 1):
        traj = runs_mod.trajectory(results_dir, key)
        if traj is not None:
            trajectories[key] = traj
            queries[key] = runs_mod.query_pair(
                results_dir, spec, key, index["category"]
            )
        if on_progress is not None:
            on_progress(i, len(keys))
    return {"index": index, "trajectories": trajectories, "queries": queries}
