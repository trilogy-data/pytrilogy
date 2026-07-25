"""Narrow a keyless FULL JOIN between two single-row relations to INNER.

Independent scalar aggregates (``sum(x)`` and ``count(y)`` sourced from
different scans) are combined with a keyless ``FULL JOIN ... on 1=1``, because
join resolution is row-preserving by default and there is no key to preserve
rows *on*. A grand-total aggregate emits exactly one row for any input,
including an empty one, so both sides always match and the FULL degenerates to
a plain cartesian — which INNER expresses, and which every dialect supports.

The gate is single-row-ness, not keylessness. A keyless FULL between *grouped*
relations is a genuine outer cartesian (and, historically here, a planner bug
that dropped join keys — see ``_widen_merge_join_keys``); rewriting that to
INNER would silently change row counts and hide the defect.
"""

from __future__ import annotations

from trilogy.core.enums import JoinType
from trilogy.core.models.build import get_grouped_aggregate_wrapper
from trilogy.core.models.execute import CTE, Join, RecursiveCTE, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.processing.condition_utility import is_scalar_condition


def is_single_row(cte: CTE | UnionCTE) -> bool:
    """True when ``cte`` provably emits exactly one row.

    Only an ungrouped aggregate qualifies: SQL guarantees one row for
    ``SELECT sum(x) FROM t`` regardless of ``t``. A ``UnionCTE`` stacks its
    branches, a LIMIT can truncate to zero, and a HAVING can delete the single
    aggregate row, so all three disqualify. A *scalar* condition renders as
    WHERE, which filters the aggregate's input rather than its output row.
    """
    if not isinstance(cte, CTE) or isinstance(cte, RecursiveCTE):
        return False
    if not cte.group_to_grain or cte.group_concepts or cte.rollup_concepts:
        return False
    if cte.limit is not None:
        return False
    if not any(
        get_grouped_aggregate_wrapper(c) is not None and not cte.source_map.get(c.address)
        for c in cte.output_columns
    ):
        return False
    if cte.condition is None:
        return True
    materialized = {address for address, v in cte.source_map.items() if v}
    return is_scalar_condition(cte.condition, materialized=materialized)


class NarrowSingleRowFullJoins(OptimizationRule):
    """Rewrite ``FULL JOIN <single-row> on 1=1`` to ``INNER JOIN``.

    Restricted to a CTE whose only join is the keyless FULL, so the relation
    on the left is exactly the FROM-base parent and its row count is knowable;
    with a join chain the accumulated left is not a single named node.
    """

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE) or not cte.render_from_clause:
            return False, None
        if len(cte.joins) != 1:
            return False, None
        join = cte.joins[0]
        if (
            not isinstance(join, Join)
            or join.jointype != JoinType.FULL
            or join.joinkey_pairs
            or join.condition is not None
        ):
            return False, None
        left = join.left_cte or next(
            (p for p in cte.dependency_nodes() if p.name == cte.base_alias), None
        )
        if left is None or not is_single_row(left) or not is_single_row(join.right_cte):
            return False, None
        join.jointype = JoinType.INNER
        self.log(
            f"{cte.name}: keyless FULL JOIN between single-row aggregates "
            f"{left.name}/{join.right_cte.name} narrowed to INNER"
        )
        return True, None
