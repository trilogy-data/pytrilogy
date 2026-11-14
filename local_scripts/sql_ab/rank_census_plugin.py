"""pytest plugin: which `_pivot_rank` terms decide a join walk.

    RANK_OUT=/tmp/rank PYTHONPATH=local_scripts/sql_ab \\
        python -m pytest -p rank_census_plugin tests ...

Every `resolve_join_order_v2` over three or more sides is replayed once per
variant, each with one rank term held constant (`solid` also drops the solid
pivots). A variant whose join order differs is counted as deciding that merge.
Writes `<RANK_OUT>.<worker>.json`: {counts, hits: [{variant, nodeid}]}.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from typing import Any

import pytest

from trilogy.core.processing import join_resolution as jr

_resolve = jr.resolve_join_order_v2
_rank = jr._pivot_rank
_solid = jr._solid_value_null_pivots
_counts: dict[str, int] = {}
_hits: list[dict[str, Any]] = []
_nodeid: list[str | None] = [None]

# variant -> (rank terms held constant, drop the solid pivots)
VARIANTS: dict[str, tuple[tuple[int, ...], bool]] = {
    "authored": ((0,), False),
    "solid": ((1,), True),
    "held": ((2,), False),
    "max_held": ((3,), False),
    "holders": ((4,), False),
    "key_length": ((5,), False),
    "all_but_name": ((0, 1, 2, 3, 4, 5), True),
}


def _signature(joins: list[jr.JoinOrderOutput]) -> list[tuple]:
    return [
        (
            j.left,
            j.right,
            j.type.value,
            sorted((k, sorted(v)) for k, v in j.keys.items()),
        )
        for j in joins
    ]


def _held(terms: tuple[int, ...]):
    def rank(pivot, holders, facts):
        out = list(_rank(pivot, holders, facts))
        for i in terms:
            out[i] = 0
        return tuple(out)

    return rank


def _replay(facts: jr.JoinFacts, terms: tuple[int, ...], no_solid: bool) -> list:
    jr._pivot_rank = _held(terms)
    if no_solid:
        jr._solid_value_null_pivots = lambda pivot_map, facts: {}
    try:
        return _signature(_resolve(facts))
    finally:
        jr._pivot_rank = _rank
        jr._solid_value_null_pivots = _solid


def _census(facts: jr.JoinFacts) -> list[jr.JoinOrderOutput]:
    out = _resolve(facts)
    if len(facts.sides) < 3:
        return out
    _counts["merges"] = _counts.get("merges", 0) + 1
    base = _signature(out)
    for name, (terms, no_solid) in VARIANTS.items():
        if _replay(facts, terms, no_solid) != base:
            _counts[name] = _counts.get(name, 0) + 1
            _hits.append({"variant": name, "nodeid": _nodeid[0]})
    return out


def pytest_configure(config) -> None:
    if os.environ.get("RANK_OUT"):
        jr.resolve_join_order_v2 = _census


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item) -> Iterator[None]:
    _nodeid[0] = item.nodeid
    yield


def pytest_unconfigure(config) -> None:
    path = os.environ.get("RANK_OUT")
    if not path:
        return
    worker = os.environ.get("PYTEST_XDIST_WORKER", "main")
    with open(f"{path}.{worker}.json", "w", encoding="utf-8") as f:
        json.dump({"counts": _counts, "hits": _hits}, f)
