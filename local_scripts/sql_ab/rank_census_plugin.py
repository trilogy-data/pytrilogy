"""pytest plugin: which terms of the join walk's stated order decide a walk.

    RANK_OUT=/tmp/rank PYTHONPATH=local_scripts/sql_ab \\
        python -m pytest -p rank_census_plugin tests ...

The walk reads one order, stated once per merge (`_stated_order`). Every
merge of three or more sides is walked again once per variant, each with one
term of that order held constant: a term of the key placement
(`_key_terms`: `authored`, or one of the key's shape; `solid` drops the
solid holders), the holders' shape
places (`holder_shape`), or a shape term (`_shape_terms`). A variant whose
join order differs is counted as deciding that merge.
Writes `<RANK_OUT>.<worker>.json`: {counts, hits: [{variant, nodeid}]}.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from trilogy.core.processing import join_resolution as jr

_stated = jr._stated_order
_keys = jr._key_terms
_shape = jr._shape_terms
_counts: dict[str, int] = {}
_hits: list[dict[str, Any]] = []
_nodeid: list[str | None] = [None]

# variant -> the key's shape term held constant
KEY_TERMS = {"solid": 0, "held": 1, "max_held": 2}
# variant -> the shape term held constant
SHAPE_TERMS = {"binding_tiers": 0, "plan_order": 1, "hosts": 2}


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


def _held(term: tuple, index: int) -> tuple:
    return (*term[:index], 0, *term[index + 1 :])


def _keys_held(name: str) -> Callable[..., list]:
    """`_key_terms` with one term held constant: `authored`, `holders`, or
    one of the key's shape (`KEY_TERMS`)."""
    index = KEY_TERMS.get(name)

    def keys(facts, holders):
        return [
            (
                True if name == "authored" else authored,
                shape if index is None else _held(shape, index),
                0 if name == "holders" else count,
                held_by,
            )
            for authored, shape, count, held_by in _keys(facts, holders)
            # the solid holders are a term of their own
            if name != "solid" or shape[0]
        ]

    return keys


def _shape_held(index: int) -> Callable[..., dict]:
    def shape(ranks, sides, pair_keys, declared):
        terms = _shape(ranks, sides, pair_keys, declared)
        return {node: _held(term, index) for node, term in terms.items()}

    return shape


def _no_holder_shape(ranks, sides, pair_keys, declared):
    """The shape with every side in one place while keys are placed."""
    return dict.fromkeys(sides, 0)


def _walked(facts: jr.JoinFacts, ranks: dict[str, int]) -> list[tuple]:
    return _signature(jr.resolve_join_order_v2(jr.replace(facts, side_ranks=ranks)))


def _variant(name: str, replacement: Callable, args: tuple) -> dict[str, int]:
    setattr(jr, name, replacement)
    try:
        return _stated(*args)
    finally:
        jr._key_terms = _keys
        jr._shape_terms = _shape


def _holder_shape_off(ranks, facts, declared) -> dict[str, int]:
    """The order with keys placed by their row terms alone, each side then
    in the shape's order."""
    shape = jr._places(_shape(ranks, facts.sides, facts.pair_keys, declared))
    keyed = _variant("_shape_terms", _no_holder_shape, (ranks, facts, declared))
    return jr._places({node: (keyed[node], shape[node]) for node in facts.sides})


def _hit(name: str) -> None:
    _counts[name] = _counts.get(name, 0) + 1
    _hits.append({"variant": name, "nodeid": _nodeid[0]})


def _census(ranks, facts, declared) -> dict[str, int]:
    out = _stated(ranks, facts, declared)
    if len(facts.sides) < 3:
        return out
    _counts["merges"] = _counts.get("merges", 0) + 1
    args = (ranks, facts, declared)
    base = _walked(facts, out)
    for name in ("authored", *KEY_TERMS, "holders"):
        if _walked(facts, _variant("_key_terms", _keys_held(name), args)) != base:
            _hit(name)
    for name, index in SHAPE_TERMS.items():
        if _walked(facts, _variant("_shape_terms", _shape_held(index), args)) != base:
            _hit(name)
    if _walked(facts, _holder_shape_off(*args)) != base:
        _hit("holder_shape")
    return out


def pytest_configure(config) -> None:
    if os.environ.get("RANK_OUT"):
        jr._stated_order = _census


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
