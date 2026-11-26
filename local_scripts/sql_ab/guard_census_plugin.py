"""pytest plugin: record every `UpgradeJoinOnGuards` flip with the proof
channel it needs.

    GUARD_CENSUS=/tmp/guard.jsonl PYTHONPATH=local_scripts/sql_ab \\
        python -m pytest -p guard_census_plugin tests ...

Each flip is decided again on the CTE's condition as planned (before any
optimizer phase ran). `channel` is `planned` when that alone flips it (the
planner should have), `pushed` when it needs a WHERE the optimizer put there,
and `new_cte` for a CTE the optimizer built.
"""

from __future__ import annotations

import json
import os

import pytest

_current: list[str] = [""]
_planned: dict[int, object] = {}


def _types(cte) -> list:
    return [getattr(j, "join_type", None) for j in cte.joins or []]


def _restore(cte, types: list) -> None:
    for join, jt in zip(cte.joins or [], types):
        if jt is not None:
            join.join_type = jt


def pytest_configure(config) -> None:
    out = os.environ.get("GUARD_CENSUS")
    if not out:
        return
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    path = f"{out}.{worker}.jsonl" if worker else out
    from trilogy.core import optimization as opt
    from trilogy.core import query_processor as qp
    from trilogy.core.models.execute import CTE
    from trilogy.core.optimizations import join_upgrade as m

    original_optimize_ctes = opt.optimize_ctes

    def optimize_ctes(input, root_cte, *args, **kwargs):
        _planned.clear()
        for cte in [*input, root_cte]:
            if isinstance(cte, CTE):
                _planned[id(cte)] = cte.condition
        return original_optimize_ctes(input, root_cte, *args, **kwargs)

    opt.optimize_ctes = optimize_ctes
    qp.optimize_ctes = optimize_ctes
    rule = m.UpgradeJoinOnGuards
    original = rule.optimize

    def counterfactual(self, cte, inverse_map, condition) -> list:
        before = _types(cte)
        saved_cond = cte.condition
        try:
            cte.condition = condition
            original(self, cte, inverse_map)
            return _types(cte)
        finally:
            cte.condition = saved_cond
            _restore(cte, before)

    def optimize(self, cte, inverse_map):
        if not isinstance(cte, CTE):
            return original(self, cte, inverse_map)
        before = _types(cte)
        planned_known = id(cte) in _planned
        planned = (
            counterfactual(self, cte, inverse_map, _planned[id(cte)])
            if planned_known
            else None
        )
        result = original(self, cte, inverse_map)
        after = _types(cte)
        for idx, (b, a) in enumerate(zip(before, after)):
            if b == a:
                continue
            if planned is not None and planned[idx] == a:
                channel = "planned"
            else:
                channel = "pushed" if planned_known else "new_cte"
            with open(path, "a") as fh:
                fh.write(
                    json.dumps(
                        {
                            "test": _current[0],
                            "cte": cte.name,
                            "right": cte.joins[idx].right.name,
                            "from": b.value,
                            "to": a.value,
                            "channel": channel,
                            "condition": str(cte.condition),
                            "planned_condition": str(_planned.get(id(cte))),
                        }
                    )
                    + "\n"
                )
        return result

    rule.optimize = optimize


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    _current[0] = item.nodeid
    yield
