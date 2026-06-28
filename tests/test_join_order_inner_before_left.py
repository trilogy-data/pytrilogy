"""Render-time reorder: selective INNER joins are pulled ahead of optional LEFT
OUTER joins so the engine filters before the non-reducing outer join (TPC-DS q80:
153s -> 0.5s for the same rows).

The correctness-critical invariants locked here: an INNER join only jumps ahead of
a LEFT join, never past a FULL/RIGHT join (those null-extend the anchor, so a later
INNER filter legitimately drops rows a reorder would resurrect), and never ahead of
a join it depends on.
"""

from types import SimpleNamespace

from trilogy.core.enums import JoinType
from trilogy.core.models.execute import Join
from trilogy.core.optimizations.order_inner_joins import order_inner_joins_before_left


def _join(right: str, jointype: JoinType, lefts: list[str]) -> Join:
    pairs = [SimpleNamespace(cte=SimpleNamespace(name=n)) for n in lefts]
    return Join(
        right_cte=SimpleNamespace(name=right),  # type: ignore[arg-type]
        jointype=jointype,
        joinkey_pairs=pairs,  # type: ignore[arg-type]
    )


def _order(joins: list[Join], base: str) -> list[str]:
    return [j.right_cte.name for j in order_inner_joins_before_left(joins, base)]


def test_inner_bubbles_ahead_of_left():
    joins = [
        _join("ret", JoinType.LEFT_OUTER, ["fact"]),
        _join("dim1", JoinType.INNER, ["fact"]),
        _join("dim2", JoinType.INNER, ["fact"]),
    ]
    assert _order(joins, "fact") == ["dim1", "dim2", "ret"]


def test_full_join_is_a_barrier():
    joins = [
        _join("f", JoinType.FULL, ["fact"]),
        _join("dim", JoinType.INNER, ["fact"]),
    ]
    assert _order(joins, "fact") == ["f", "dim"]


def test_right_join_is_a_barrier():
    joins = [
        _join("r", JoinType.RIGHT_OUTER, ["fact"]),
        _join("dim", JoinType.INNER, ["fact"]),
    ]
    assert _order(joins, "fact") == ["r", "dim"]


def test_inner_does_not_jump_a_left_it_depends_on():
    joins = [
        _join("b", JoinType.LEFT_OUTER, ["a"]),
        _join("c", JoinType.INNER, ["b"]),
    ]
    assert _order(joins, "a") == ["b", "c"]


def test_already_optimal_order_unchanged():
    joins = [
        _join("dim", JoinType.INNER, ["fact"]),
        _join("ret", JoinType.LEFT_OUTER, ["fact"]),
    ]
    assert _order(joins, "fact") == ["dim", "ret"]


def test_inner_independent_of_deferred_left_still_jumps():
    # dim depends only on fact (available), so it clears the held-back left even
    # though a second left (opt2) sits between them in source order.
    joins = [
        _join("opt1", JoinType.LEFT_OUTER, ["fact"]),
        _join("opt2", JoinType.LEFT_OUTER, ["fact"]),
        _join("dim", JoinType.INNER, ["fact"]),
    ]
    assert _order(joins, "fact") == ["dim", "opt1", "opt2"]
