"""Emit reducing INNER joins ahead of optional LEFT OUTER joins.

An INNER join placed after a LEFT join makes the engine materialize the full
outer-join product before applying the inner filter, which does not get pushed
below the outer join. Bubbling inner joins forward avoids that.

Runs as the final optimization phase so join types are settled (the upgrade
passes can flip INNER<->OUTER) and nothing downstream re-disturbs the order. It is
the single place every CTE's join list is ordered, however the joins were assembled.

Why the reorder never changes results:
- An INNER join only jumps ahead of a LEFT OUTER join, and only when none of the
  sources its ON clause reads are produced by a deferred LEFT join. A LEFT join
  preserves every left row and only adds (nullable) right columns, so an INNER
  filter on non-LEFT columns commutes with it.
- FULL and RIGHT OUTER joins are hard barriers: they null-extend the anchor, so a
  later INNER filter on the anchor legitimately drops rows that reordering would
  resurrect. Unnest / non-``Join`` entries are barriers too. Relative order is
  otherwise preserved (stable).
"""

from __future__ import annotations

from trilogy.core.enums import JoinType
from trilogy.core.models.execute import (
    CTE,
    Join,
    UnionCTE,
    UnnestJoin,
)
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule


def _join_left_sources(join: Join) -> set[str]:
    """CTE names a join's ON clause reads from (its left/anchor sources)."""
    deps: set[str] = set()
    for pair in join.pairs or []:
        if pair.node is not None:
            deps.add(pair.node.name)
    if join.left is not None:
        deps.add(join.left.name)
    return deps


def order_inner_joins_before_left(
    joins: list[Join | UnnestJoin],
    base_name: str | None,
) -> list[Join | UnnestJoin]:
    if len(joins) < 2:
        return joins

    real = [j for j in joins if isinstance(j, Join)]
    produced = {j.right.name for j in real}
    available: set[str] = {s for j in real for s in _join_left_sources(j)} - produced
    if base_name:
        available.add(base_name)

    result: list[Join | UnnestJoin] = []
    deferred: list[Join] = []

    def flush() -> None:
        for d in deferred:
            result.append(d)
            available.add(d.right.name)
        deferred.clear()

    for join in joins:
        if not isinstance(join, Join):
            flush()
            result.append(join)
            continue
        if join.join_type == JoinType.LEFT_OUTER:
            deferred.append(join)
        elif join.join_type == JoinType.INNER:
            if not _join_left_sources(join) <= available:
                flush()
            result.append(join)
            available.add(join.right.name)
        else:
            flush()
            result.append(join)
            available.add(join.right.name)
    flush()
    return result


class OrderInnerJoinsFirst(OptimizationRule):
    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE) or len(cte.joins) < 2:
            return False, None
        reordered = order_inner_joins_before_left(cte.joins, cte.base_name)
        if all(a is b for a, b in zip(reordered, cte.joins)):
            return False, None
        cte.joins = reordered
        self.log(f"Reordered INNER joins ahead of LEFT joins in {cte.name}")
        return True, None
