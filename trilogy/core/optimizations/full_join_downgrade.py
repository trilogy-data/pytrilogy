"""Downgrade FULL joins when the WHERE proves they're stricter than needed.

The asymmetric-nullable rule in `get_join_type` promotes some joins to FULL so
NULL rows on the nullable side aren't lost against the non-nullable side. When
the surrounding WHERE forces a side to contribute non-null data — either on
the join keys themselves or on any column unique to that side — the unmatched
rows that FULL was preserving can never satisfy the filter, so we can
downgrade:

  - filter forces left side non-null AND right side non-null → ``INNER``
  - filter forces only the left side non-null  (drops right-unmatched) → ``LEFT_OUTER``
  - filter forces only the right side non-null (drops left-unmatched)  → ``RIGHT_OUTER``

A predicate "forces a concept non-null in surviving rows" if it can never be
TRUE when that concept is NULL — direct ``IS NOT NULL``, null-propagating
comparisons against literals, or any concept reference inside a null-
propagating expression. ``COALESCE``/``NULLIF``/``CASE``/``COUNT`` are treated
as opaque (they can be non-null even when an arg is NULL), so we don't recurse
into them.
"""

from __future__ import annotations

from trilogy.constants import MagicConstants
from trilogy.core.enums import ComparisonOperator, FunctionType, JoinType
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildFunction,
    BuildParenthetical,
)
from trilogy.core.models.execute import CTE, Join, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.processing.condition_utility import decompose_condition

# Operators whose result is NULL (and therefore not TRUE) when either operand is
# NULL — when one of these atoms is required by an AND chain, both sides'
# concept references must be non-null in surviving rows. (Tuple, not set, since
# ComparisonOperator overrides __eq__ without __hash__.)
_NULL_PROPAGATING_OPS: tuple[ComparisonOperator, ...] = (
    ComparisonOperator.EQ,
    ComparisonOperator.NE,
    ComparisonOperator.LT,
    ComparisonOperator.GT,
    ComparisonOperator.LTE,
    ComparisonOperator.GTE,
    ComparisonOperator.LIKE,
    ComparisonOperator.ILIKE,
    ComparisonOperator.IN,
    ComparisonOperator.NOT_IN,
    ComparisonOperator.CONTAINS,
)

# Functions whose result can be non-null even when an argument is NULL — we
# can't recurse through them to claim the args are non-null.
_NULL_OPAQUE_FUNCTIONS: tuple[FunctionType, ...] = (
    FunctionType.COALESCE,
    FunctionType.NULLIF,
    FunctionType.CASE,
    FunctionType.COUNT,  # COUNT(NULL) = 0, not NULL
    FunctionType.COUNT_DISTINCT,
)


def _is_null_literal(value: object) -> bool:
    return value is None or value is MagicConstants.NULL


def _concepts_in_expression(value: object) -> set[str]:
    """Recursively gather concept addresses inside a null-propagating expression.

    The caller has already established that the expression's result must be
    non-null; this returns every concept whose individual non-nullness is
    therefore implied. Stops at null-opaque functions (coalesce et al.) — those
    can swallow NULL args without their result being NULL, so descending past
    them would over-claim.
    """
    if isinstance(value, BuildConcept):
        return {value.address}
    if isinstance(value, BuildParenthetical):
        return _concepts_in_expression(value.content)  # type: ignore[arg-type]
    if isinstance(value, BuildAggregateWrapper):
        return _concepts_in_expression(value.function)
    if isinstance(value, BuildFunction):
        if value.operator in _NULL_OPAQUE_FUNCTIONS:
            return set()
        addrs: set[str] = set()
        for arg in value.arguments:
            addrs |= _concepts_in_expression(arg)
        return addrs
    return set()


def _proves_non_null(
    atom: BuildComparison | BuildConditional | BuildParenthetical,
) -> set[str]:
    """Concept addresses that this AND-atom forces non-null in surviving rows."""
    if isinstance(atom, BuildParenthetical):
        return _proves_non_null(atom.content)  # type: ignore[arg-type]
    if not isinstance(atom, BuildComparison):
        return set()

    left, right, op = atom.left, atom.right, atom.operator

    if op == ComparisonOperator.IS_NOT:
        # `<expr> IS NOT NULL` — every concept inside the expression must be
        # non-null, modulo null-opaque functions.
        if _is_null_literal(right):
            return _concepts_in_expression(left)
        if _is_null_literal(left):
            return _concepts_in_expression(right)
        return set()

    # `concept IS NULL` specifically wants NULLs — never proves non-null.
    if op == ComparisonOperator.IS:
        return set()

    if op in _NULL_PROPAGATING_OPS:
        # Null-propagating comparison: both sides must evaluate to non-null for
        # the comparison to be TRUE.
        return _concepts_in_expression(left) | _concepts_in_expression(right)

    return set()


def _gather_proofs(
    cond: BuildComparison | BuildConditional | BuildParenthetical,
) -> set[str]:
    return {
        addr for atom in decompose_condition(cond) for addr in _proves_non_null(atom)
    }


def _cte_addresses(cte: CTE | UnionCTE | None) -> set[str]:
    if cte is None:
        return set()
    return {c.address for c in cte.output_columns}


def _side_addresses(
    join: Join, default_left: CTE | UnionCTE
) -> tuple[set[str], set[str]]:
    """Addresses unique to each side of the join. Filters that touch only
    one side are unambiguous about which side they constrain."""
    left = join.left_cte or default_left
    right = join.right_cte
    left_all = _cte_addresses(left)
    right_all = _cte_addresses(right)
    return left_all - right_all, right_all - left_all


def _downgrade(
    join: Join, left_default: CTE | UnionCTE, proofs: set[str]
) -> JoinType | None:
    """Pick the strictest join that still produces the same surviving rows."""
    pairs = join.joinkey_pairs or []
    left_keys = {p.left.address for p in pairs}
    right_keys = {p.right.address for p in pairs}
    left_only, right_only = _side_addresses(join, left_default)

    # The left side is "forced present" when the WHERE references a concept
    # that only exists on the left, OR when every left join key is individually
    # proven non-null. Either case rules out the unmatched rows whose left
    # columns were filled with NULL. Mirror for the right side.
    left_forced = bool(proofs & left_only) or (
        bool(left_keys) and left_keys.issubset(proofs)
    )
    right_forced = bool(proofs & right_only) or (
        bool(right_keys) and right_keys.issubset(proofs)
    )

    if left_forced and right_forced:
        return JoinType.INNER
    # left_forced → filter drops right-unmatched (their left columns are NULL
    # fill, so any non-null check on a left column fails) → LEFT_OUTER drops
    # the same set. Mirror for right_forced.
    if left_forced:
        return JoinType.LEFT_OUTER
    if right_forced:
        return JoinType.RIGHT_OUTER
    return None


def _previous_left(cte: CTE | UnionCTE, idx: int) -> CTE | UnionCTE | None:
    """When a join doesn't carry an explicit left_cte, the LEFT side is the
    accumulated FROM up to that point. For the first join that's whichever
    parent CTE isn't the join's right side; for subsequent joins it's the
    prior join's right CTE."""
    if idx == 0:
        if isinstance(cte, CTE) and cte.joins:
            right_name = getattr(cte.joins[0].right_cte, "name", None)
            for parent in cte.parent_ctes:
                if isinstance(parent, (CTE, UnionCTE)) and parent.name != right_name:
                    return parent
        return None
    return cte.joins[idx - 1].right_cte  # type: ignore[union-attr]


class DowngradeFullJoinOnGuards(OptimizationRule):
    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE) or not cte.condition or not cte.joins:
            return False, None
        if not any(j.jointype == JoinType.FULL for j in cte.joins):
            return False, None

        proofs = _gather_proofs(cte.condition)
        if not proofs:
            return False, None

        changed = False
        for idx, join in enumerate(cte.joins):
            if join.jointype != JoinType.FULL:
                continue
            # The structural left of the first join is the CTE's FROM clause
            # (a sibling parent CTE); subsequent joins accumulate the prior
            # join's right_cte. join.left_cte takes precedence when set.
            default_left = join.left_cte or _previous_left(cte, idx)
            if default_left is None:
                continue
            target = _downgrade(join, default_left, proofs)
            if target is None or target == join.jointype:
                continue
            dropped = {
                JoinType.INNER: "both left- and right-unmatched",
                JoinType.LEFT_OUTER: "right-unmatched",
                JoinType.RIGHT_OUTER: "left-unmatched",
            }[target]
            self.log(
                f"FULL→{target.value} on {cte.name} for join with"
                f" {join.right_cte.name}: WHERE filters out"
                f" {dropped} rows that FULL was preserving"
            )
            join.jointype = target
            changed = True
        return changed, None
