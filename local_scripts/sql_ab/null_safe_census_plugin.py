"""pytest plugin: record every key `SimplifyNullSafeJoins` strips the null-safe
form from, with the proofs that held for it.

    NULL_SAFE_CENSUS=/tmp/ns.jsonl PYTHONPATH=local_scripts/sql_ab \\
        python -m pytest -p null_safe_census_plugin tests ...

Each line: test, the joining CTE, the right CTE, and per side the first layer
of `proven_non_null` that answered (`nullable` the build-time set, `where` the
side's own condition, `walk` its parents, `union` every branch), plus `local`
when the joining CTE's own WHERE null-rejects the key, `planned_local` when
its planned source's condition already did, and `planned_type`, the type the
planner gave the join (none when no planned join matches).
"""

from __future__ import annotations

import json
import os

import pytest

from trilogy.core.enums import JoinType, Modifier

_current: list[str] = [""]


def _layer(concept, cte) -> str | None:
    from trilogy.core.models.execute import CTE, UnionCTE
    from trilogy.core.optimizations.null_safe_join import proven_non_null
    from trilogy.core.processing.condition_utility import condition_proves_non_null
    from trilogy.core.processing.utility import equivalent_addresses

    if not proven_non_null(concept, cte):
        return None
    if isinstance(cte, UnionCTE) or not isinstance(cte, CTE):
        return "union"
    addrs = concept.equivalent_addresses
    if addrs.isdisjoint(equivalent_addresses(list(cte.nullable_concepts))):
        return "nullable"
    if cte.condition is not None and not addrs.isdisjoint(
        condition_proves_non_null(cte.condition)
    ):
        return "where"
    return "walk"


def _planned_local(cte, pair) -> bool:
    from trilogy.core.processing.condition_utility import condition_proves_non_null

    condition = cte.source.condition
    if condition is None:
        return False
    proven = condition_proves_non_null(condition)
    return not (
        pair.left.equivalent_addresses.isdisjoint(proven)
        and pair.right.equivalent_addresses.isdisjoint(proven)
    )


def _planned_type(cte, join) -> str | None:
    """The planned type of the join `join` was built from, if the CTE's
    planned source still holds it."""
    from trilogy.core.models.execute import Join

    target = join.right.source
    for planned in cte.source.joins:
        if isinstance(planned, Join) and planned.right.name == target.name:
            return planned.join_type.value
    return None


def pytest_configure(config) -> None:
    out = os.environ.get("NULL_SAFE_CENSUS")
    if not out:
        return
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    path = f"{out}.{worker}.jsonl" if worker else out
    from trilogy.core.models.execute import CTE, Join
    from trilogy.core.optimizations import null_safe_join as m
    from trilogy.core.processing.condition_utility import condition_proves_non_null

    original = m.SimplifyNullSafeJoins.optimize

    def wrapper(self, cte, inverse_map):
        rows = []
        if isinstance(cte, CTE):
            local = (
                condition_proves_non_null(cte.condition)
                if cte.condition is not None
                else set()
            )
            for join in cte.joins or []:
                if not isinstance(join, Join) or join.join_type != JoinType.INNER:
                    continue
                for pair in join.pairs or []:
                    if Modifier.NULLABLE not in pair.modifiers:
                        continue
                    rows.append(
                        {
                            "test": _current[0],
                            "cte": cte.name,
                            "right": join.right.name,
                            "key": pair.right.address,
                            "local": not (
                                pair.left.equivalent_addresses.isdisjoint(local)
                                and pair.right.equivalent_addresses.isdisjoint(local)
                            ),
                            "left_layer": _layer(pair.left, pair.node),
                            "right_layer": _layer(pair.right, join.right),
                            "planned_local": _planned_local(cte, pair),
                            "planned_type": _planned_type(cte, join),
                            "_pair": pair,
                        }
                    )
        result = original(self, cte, inverse_map)
        stripped = [r for r in rows if Modifier.NULLABLE not in r["_pair"].modifiers]
        if stripped:
            with open(path, "a") as fh:
                for r in stripped:
                    r.pop("_pair")
                    fh.write(json.dumps(r) + "\n")
        return result

    m.SimplifyNullSafeJoins.optimize = wrapper


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    _current[0] = item.nodeid
    yield
