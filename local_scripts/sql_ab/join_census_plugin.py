"""pytest plugin: the join census over every statement a test suite compiles.

    CENSUS_OUT=<file.jsonl> PYTHONPATH=local_scripts/sql_ab \\
        python -m pytest -p join_census_plugin tests ...

One JSON line per FINAL merge with two or more sides whose resolve-time join
keys differ from the keys its join plan states: {nodeid, sides, actual,
planned}; one per test that exercised a counted planner path: {nodeid,
paths}. A final line counts every multi-side FINAL seen. See README.md.
"""

from __future__ import annotations

import json
import os
from typing import IO, Any

import pytest

_out: IO[str] | None = None
_state: dict[str, Any] = {"nodeid": None, "seen": 0, "mismatched": 0}
_plans: dict[int, list[str]] = {}


def pytest_configure(config) -> None:
    global _out
    path = os.environ.get("CENSUS_OUT")
    if not path:
        return
    # held for the session, closed in pytest_unconfigure
    _out = open(path, "w", encoding="utf-8", newline="\n")  # noqa: SIM115
    import join_census as jc

    jc.install()
    init = jc.mn.MergeNode.__init__

    def merge_init(self, *a, **k):
        init(self, *a, **k)
        if id(self) in jc.FINAL_NODES:
            _plans[id(self)] = list(jc.REC.get("final.plan_keys") or [])

    jc.mn.MergeNode.__init__ = merge_init  # type: ignore[method-assign]
    resolved = jc.mn.get_node_joins

    def get_node_joins(datasources, environment, *a, **k):
        joins = resolved(datasources, environment, *a, **k)
        node = jc.CURRENT_MERGE[-1] if jc.CURRENT_MERGE else None
        if node is None or id(node) not in _plans or len(datasources) < 2:
            return joins
        planned = _plans[id(node)]
        canon = environment.address_roots(set(jc.REC["final.join_keys"]) | set(planned))
        actual = jc.canon(canon, jc.REC["final.join_keys"])
        stated = jc.canon(canon, planned)
        _state["seen"] += 1
        if actual != stated and _out is not None:
            _state["mismatched"] += 1
            _out.write(
                json.dumps(
                    {
                        "nodeid": _state["nodeid"],
                        "sides": len(datasources),
                        "actual": sorted(actual),
                        "planned": sorted(stated),
                    }
                )
                + "\n"
            )
        return joins

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
        _out.write(json.dumps(_state) + "\n")
        _out.close()
