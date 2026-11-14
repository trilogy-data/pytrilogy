"""pytest plugin: which `_pivot_rank` and `_score_join_candidate` terms
decide a join walk.

    RANK_OUT=/tmp/rank PYTHONPATH=local_scripts/sql_ab \\
        python -m pytest -p rank_census_plugin tests ...

Every `resolve_join_order_v2` over three or more sides is replayed once per
variant, each with one rank term held constant (`solid` also drops the solid
pivots, `solid_nullable` reads any nullable key for a value-nullable one), or
one scorer term held constant in one of its two uses: `right:` the pick of the
next side, `left:` the order of the joined sides it is typed against
(`_lefts_in_order`). A variant whose join order differs is counted as deciding
that merge. `score_replica` counts merges the scorer's replica orders
differently from the scorer itself; it must stay 0.
Writes `<RANK_OUT>.<worker>.json`: {counts, hits: [{variant, nodeid}]}.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Iterator
from functools import partial
from typing import Any

import pytest

from trilogy.core.processing import join_resolution as jr

_resolve = jr.resolve_join_order_v2
_rank = jr._pivot_rank
_solid = jr._solid_value_null_pivots
_score = jr._score_join_candidate
_lefts = jr._lefts_in_order
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
SCORE_TERMS = (
    "anchor",
    "multi_partial",
    "partial",
    "hosts",
    "nullable",
    "grain",
    "name",
)


def _signature(joins: list[jr.JoinOrderOutput]) -> list[tuple]:
    return [
        (
            j.left,
            j.right,
            j.type.value,
            [(k, sorted(v)) for k, v in j.keys.items()],
            list(j.redundant),
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


def _solid_nullable(pivot_map, facts):
    if not facts.held_spans:
        return {}
    out = {}
    for pivot, sides in pivot_map.items():
        solid = [s for s in sides if not facts.side(s).held_spans]
        if len(solid) > 1 and any(pivot.key in facts.side(s).nullables for s in solid):
            out[jr._Pivot(pivot.key, solid=True)] = solid
    return out


def _scorer(off: frozenset[str]) -> Callable[..., tuple[int, int, str]]:
    """`_score_join_candidate` with the terms in `off` held constant."""

    def score(x, *, root, facts, multi_partial, anchor_sources):
        side = facts.side(x)
        base = 1
        if x in anchor_sources and "anchor" not in off:
            base += 10
        is_partial = root in side.partials
        if multi_partial and is_partial:
            base += 0 if "multi_partial" in off else 2
        elif is_partial:
            base -= 0 if "partial" in off else 1
        if side.hosts and not is_partial and "hosts" not in off:
            base += 3
        if root in side.nullables and "nullable" not in off:
            base += 1
        grain = 0 if "grain" in off else len(side.grain)
        return (base, grain, "" if "name" in off else x)

    return score


def _replay_score(facts: jr.JoinFacts, off: frozenset[str], use: str) -> list:
    variant = _scorer(off)

    def lefts(joined, score):
        return _lefts(
            joined, partial(variant if use == "left" else _score, **score.keywords)
        )

    if use == "right":
        jr._score_join_candidate = variant
    jr._lefts_in_order = lefts
    try:
        return _signature(_resolve(facts))
    finally:
        jr._score_join_candidate = _score
        jr._lefts_in_order = _lefts


def _hit(name: str) -> None:
    _counts[name] = _counts.get(name, 0) + 1
    _hits.append({"variant": name, "nodeid": _nodeid[0]})


def _census(facts: jr.JoinFacts) -> list[jr.JoinOrderOutput]:
    out = _resolve(facts)
    if len(facts.sides) < 3:
        return out
    _counts["merges"] = _counts.get("merges", 0) + 1
    base = _signature(out)
    for name, (terms, no_solid) in VARIANTS.items():
        if _replay(facts, terms, no_solid) != base:
            _hit(name)
    jr._solid_value_null_pivots = _solid_nullable
    try:
        if _signature(_resolve(facts)) != base:
            _hit("solid_nullable")
    finally:
        jr._solid_value_null_pivots = _solid
    if _replay_score(facts, frozenset(), "left") != base:
        _hit("score_replica")
    for use in ("right", "left"):
        for term in SCORE_TERMS:
            if _replay_score(facts, frozenset({term}), use) != base:
                _hit(f"{use}:{term}")
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
