"""Narrow keyless FULL JOINs between relations that provably have rows.

Independent scalar aggregates (``sum(x)`` and ``count(y)`` over different
scans) are combined with a keyless ``FULL JOIN ... on 1=1``, because join
resolution is row-preserving by default and there is no key to preserve rows
*on*. With a tautological ON clause every pair of rows matches, so the only
thing FULL adds over a cartesian is what happens when a side is *empty* — FULL
still emits the other side's rows, null-extended.

That makes emptiness the whole question:

===============  ===============  ==================================
left has rows    right has rows   equivalent join
===============  ===============  ==================================
yes              yes              INNER — every pair matches anyway
yes              unknown          LEFT  — left rows survive an empty right
unknown          yes              RIGHT — right rows survive an empty left
unknown          unknown          FULL  — genuinely needed
===============  ===============  ==================================

The only proof of "has rows" available here is a grand-total aggregate:
``SELECT sum(x) FROM t`` emits exactly one row for any ``t``, empty or not.
Emptiness composes along the FROM chain, so a run of keyless FULL joins over
scalar aggregates (q09 joins four) collapses entirely.

The gate is emptiness, never keylessness on its own. A keyless FULL between
relations that might be empty is a genuine outer cartesian — and, historically
here, a planner bug that dropped join keys (see ``_widen_merge_join_keys``).
Rewriting those would silently change row counts and hide the defect.
"""

from __future__ import annotations

from trilogy.core.enums import JoinType
from trilogy.core.models.build import get_grouped_aggregate_wrapper
from trilogy.core.models.execute import CTE, Join, RecursiveCTE, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.processing.condition_utility import is_scalar_condition


def is_single_row(cte: CTE | UnionCTE) -> bool:
    """True when ``cte`` provably emits exactly one row.

    Only an ungrouped aggregate qualifies. A ``UnionCTE`` stacks its branches,
    a LIMIT can truncate to zero, and a HAVING can delete the single aggregate
    row, so all three disqualify. A *scalar* condition renders as WHERE, which
    filters the aggregate's input rather than its output row.
    """
    if not isinstance(cte, CTE) or isinstance(cte, RecursiveCTE):
        return False
    if not cte.group_to_grain or cte.group_concepts or cte.rollup_concepts:
        return False
    if cte.limit is not None:
        return False
    if not any(
        get_grouped_aggregate_wrapper(c) is not None
        and not cte.source_map.get(c.address)
        for c in cte.output_columns
    ):
        return False
    if cte.condition is None:
        return True
    materialized = {address for address, v in cte.source_map.items() if v}
    return is_scalar_condition(cte.condition, materialized=materialized)


def _is_keyless_full(join: object) -> bool:
    return (
        isinstance(join, Join)
        and join.jointype == JoinType.FULL
        and not join.joinkey_pairs
        and join.condition is None
    )


def _narrowed_type(left_has_rows: bool, right_has_rows: bool) -> JoinType | None:
    if left_has_rows and right_has_rows:
        return JoinType.INNER
    if left_has_rows:
        return JoinType.LEFT_OUTER
    if right_has_rows:
        return JoinType.RIGHT_OUTER
    return None


class NarrowKeylessFullJoins(OptimizationRule):
    """Replace ``FULL JOIN ... on 1=1`` with the narrowest equivalent join.

    Walks the FROM chain in render order carrying whether the relation
    accumulated so far provably has rows: the FROM base's own proof to start,
    then updated per join from the type actually emitted. Any other join in the
    chain (keyed, conditioned, unnest) makes the running cardinality unknowable,
    so the carry resets and later joins can only narrow on their right side.
    """

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE) or not cte.render_from_clause or not cte.joins:
            return False, None
        if not any(_is_keyless_full(join) for join in cte.joins):
            return False, None
        base = next(
            (p for p in cte.dependency_nodes() if p.name == cte.base_alias), None
        )
        left_has_rows = base is not None and is_single_row(base)
        changed = False
        for join in cte.joins:
            if not isinstance(join, Join) or not _is_keyless_full(join):
                left_has_rows = False
                continue
            if join.left_cte is not None:
                # An explicit left overrides the running carry for this join.
                left_has_rows = left_has_rows or is_single_row(join.left_cte)
            right_has_rows = is_single_row(join.right_cte)
            narrowed = _narrowed_type(left_has_rows, right_has_rows)
            if narrowed is None:
                left_has_rows = False
                continue
            if join.jointype != narrowed:
                join.jointype = narrowed
                changed = True
                self.log(
                    f"{cte.name}: keyless FULL JOIN with {join.right_cte.name} "
                    f"narrowed to {narrowed.value.upper()} (left has rows="
                    f"{left_has_rows}, right has rows={right_has_rows})"
                )
            # The joined relation has rows whenever a preserved side does.
            left_has_rows = left_has_rows or right_has_rows
        return changed, None
