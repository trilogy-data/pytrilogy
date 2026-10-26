"""pytest plugin: the join census over every statement a test suite compiles.

    CENSUS_OUT=<file.jsonl> PYTHONPATH=local_scripts/sql_ab \\
        python -m pytest -p join_census_plugin tests ...

One JSON line per FINAL merge with two or more sides that resolve, left
unrestricted, would pair off its join plan (a key the plan does not state, or
none where the plan pairs): {nodeid, actual, planned, plan, sides}; one per test that exercised a counted planner path: {nodeid,
paths}. A final line counts every multi-side FINAL seen. See README.md.
"""

from __future__ import annotations

import json
import os
from typing import IO, Any

import pytest

_out: IO[str] | None = None
_state: dict[str, Any] = {"nodeid": None, "seen": 0, "mismatched": 0, "over": 0}


def pytest_configure(config) -> None:
    global _out
    path = os.environ.get("CENSUS_OUT")
    if not path:
        return
    # held for the session, closed in pytest_unconfigure
    _out = open(path, "w", encoding="utf-8", newline="\n")  # noqa: SIM115
    import join_census as jc

    jc.install()
    resolved = jc.mn.get_node_joins
    planner = jc.sb.plan_final_join

    def plan_final_join(group_graph, attrs, cover, *a, **k):
        plan = planner(group_graph, attrs, cover, *a, **k)
        _state["plan"] = {
            gid: {
                "derivation": str(attrs[gid].derivation),
                "grain": sorted(attrs[gid].grain_components),
                "outputs": sorted(attrs[gid].output_concepts),
                "inputs": sorted(attrs[gid].input_concepts),
                "identity": sorted(plan.identity[gid]),
                "atoms": [str(atom) for atom in attrs[gid].condition_atoms],
            }
            for gid in plan.contributors
        }
        _state["pairings"] = [
            [p.left, p.right, sorted(p.keys), p.kind.value] for p in plan.pairings
        ]
        return plan

    jc.sb.plan_final_join = plan_final_join

    def get_node_joins(datasources, *a, **k):
        planned = k.get("planned_keys")
        if planned is None or len(datasources) < 2:
            return resolved(datasources, *a, **k)
        # what resolve picks unrestricted, beside what the plan states
        shadow = resolved(datasources, *a, **{**k, "planned_keys": None})
        environment = k["environment"]
        actual = {
            side.address
            for join in shadow
            for pair in getattr(join, "pairs", None) or []
            for side in (pair.left, pair.right)
        }
        roots = environment.address_roots()
        _state["seen"] += 1
        actual_roots = {roots.get(a, a) for a in actual}
        planned_roots = {roots.get(a, a) for a in planned}
        # resolve may prune a planned key a surviving one determines; it may
        # never pair off the plan, nor drop every key the plan pairs on
        if actual_roots != planned_roots:
            _state["over"] += 1
        if not actual_roots <= planned_roots or (planned_roots and not actual_roots):
            _state["mismatched"] += 1
            if _out is not None:
                _out.write(
                    json.dumps(
                        {
                            "nodeid": _state["nodeid"],
                            "actual": sorted(actual),
                            "planned": sorted(planned),
                            "plan": _state.get("plan"),
                            "pairings": _state.get("pairings"),
                            "sides": [
                                {
                                    "id": d.identifier,
                                    "grain": sorted(d.grain.components),
                                    "outputs": sorted(
                                        c.address
                                        for c in d.output_concepts
                                        if c.address not in d.hidden_concepts
                                    ),
                                }
                                for d in datasources
                            ],
                        }
                    )
                    + "\n"
                )
        return resolved(datasources, *a, **k)

    jc.mn.get_node_joins = get_node_joins


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item, nextitem):
    import join_census as jc

    _state["nodeid"] = item.nodeid
    jc.REC.clear()
    yield
    paths = {k: n for k, n in jc.REC.items() if isinstance(n, int) and "." in k}
    paths.pop("final.merge_sides", None)
    paths.pop("final.cover_contributors", None)
    if paths and _out is not None:
        _out.write(json.dumps({"nodeid": item.nodeid, "paths": paths}) + "\n")


def pytest_unconfigure(config) -> None:
    if _out is not None:
        totals = {k: _state[k] for k in ("seen", "mismatched", "over")}
        _out.write(json.dumps(totals) + "\n")
        _out.close()
