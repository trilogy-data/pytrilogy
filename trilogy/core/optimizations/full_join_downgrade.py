"""Downgrade OUTER joins when the WHERE proves they're stricter than needed.

Outer joins exist to preserve unmatched rows by NULL-padding one side. When the
surrounding WHERE rejects rows where those NULL-padded columns appear (directly
via ``IS NOT NULL`` or via any null-propagating predicate that mentions a
concept on that side), the unmatched rows can never satisfy the filter — so
the OUTER join produces the same surviving rows as a stricter join, and we
can downgrade.

A predicate "forces a concept non-null in surviving rows" if it can never be
TRUE when that concept is NULL — direct ``IS NOT NULL``, null-propagating
comparisons against literals, or any concept reference inside a null-
propagating expression. ``COALESCE``/``NULLIF``/``CASE``/``COUNT`` are treated
as opaque (they can be non-null even when an arg is NULL), so we don't recurse
into them.

Downgrades by current join type:

  - ``FULL``
      both sides forced non-null  → ``INNER``
      only left forced (drops left-unmatched rows that have NULL left-side
      columns) → ``LEFT_OUTER``
      only right forced (drops right-unmatched rows with NULL right-side
      columns) → ``RIGHT_OUTER``
  - ``LEFT_OUTER`` (NULL-padding lives on the right side)
      right forced → ``INNER``
  - ``RIGHT_OUTER`` (NULL-padding lives on the left side)
      left forced → ``INNER``
"""

from __future__ import annotations

from trilogy.core.enums import ComparisonOperator, JoinType
from trilogy.core.models.build import (
    BuildComparison,
    BuildConditional,
    BuildParenthetical,
)
from trilogy.core.models.execute import CTE, Join, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.processing.condition_utility import (
    NULL_PROPAGATING_OPS,
    concepts_implied_non_null,
    decompose_condition,
    is_null_literal,
)

# Joins whose surviving rows can be a strict subset under a null-rejecting
# WHERE. INNER joins already drop both unmatched sides, so they're never
# eligible to be downgraded further.
_OUTER_JOIN_TYPES = (JoinType.FULL, JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER)


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
        if is_null_literal(right):
            return concepts_implied_non_null(left)
        if is_null_literal(left):
            return concepts_implied_non_null(right)
        return set()

    # `concept IS NULL` specifically wants NULLs — never proves non-null.
    if op == ComparisonOperator.IS:
        return set()

    if op in NULL_PROPAGATING_OPS:
        return concepts_implied_non_null(left) | concepts_implied_non_null(right)

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
    current = join.jointype
    if current not in _OUTER_JOIN_TYPES:
        return None

    pairs = join.joinkey_pairs or []
    left_keys = {p.left.address for p in pairs}
    right_keys = {p.right.address for p in pairs}
    left_only, right_only = _side_addresses(join, left_default)

    # The left side is "forced present" when the WHERE references a concept
    # that only exists on the left, OR when every left join key is individually
    # proven non-null. Either case rules out unmatched rows whose left columns
    # were filled with NULL. Mirror for the right side.
    left_forced = bool(proofs & left_only) or (
        bool(left_keys) and left_keys.issubset(proofs)
    )
    right_forced = bool(proofs & right_only) or (
        bool(right_keys) and right_keys.issubset(proofs)
    )

    if current == JoinType.FULL:
        if left_forced and right_forced:
            return JoinType.INNER
        # left_forced → filter drops left-unmatched rows (NULL left columns)
        # → LEFT_OUTER drops the same set. Mirror for right_forced.
        if left_forced:
            return JoinType.LEFT_OUTER
        if right_forced:
            return JoinType.RIGHT_OUTER
        return None

    if current == JoinType.LEFT_OUTER:
        # NULL-padding lives on the right side; right_forced removes the
        # padded rows, leaving only matched rows → INNER.
        if right_forced:
            return JoinType.INNER
        return None

    if current == JoinType.RIGHT_OUTER:
        if left_forced:
            return JoinType.INNER
        return None

    return None


def _previous_left(cte: CTE | UnionCTE, idx: int) -> CTE | UnionCTE | None:
    """When a join doesn't carry an explicit left_cte, the LEFT side is the
    accumulated FROM up to that point. For the first join that's whichever
    parent CTE isn't the join's right side; for subsequent joins it's the
    prior join's right CTE."""
    if idx == 0:
        if isinstance(cte, CTE) and cte.joins:
            first = cte.joins[0]
            if not isinstance(first, Join):
                return None
            right_name = first.right_cte.name
            for parent in cte.parent_ctes:
                if isinstance(parent, (CTE, UnionCTE)) and parent.name != right_name:
                    return parent
        return None
    prior = cte.joins[idx - 1]  # type: ignore[union-attr]
    if not isinstance(prior, Join):
        return None
    return prior.right_cte


class DowngradeFullJoinOnGuards(OptimizationRule):
    """Downgrade FULL/LEFT_OUTER/RIGHT_OUTER joins to a stricter form when the
    enclosing WHERE rejects the unmatched rows the OUTER join was preserving.

    Class name kept for backwards compatibility; the rule covers all three
    OUTER join types now."""

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE) or not cte.condition or not cte.joins:
            return False, None
        if not any(
            isinstance(j, Join) and j.jointype in _OUTER_JOIN_TYPES for j in cte.joins
        ):
            return False, None

        proofs = _gather_proofs(cte.condition)
        if not proofs:
            return False, None

        changed = False
        for idx, join in enumerate(cte.joins):
            if not isinstance(join, Join) or join.jointype not in _OUTER_JOIN_TYPES:
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
                JoinType.INNER: "unmatched",
                JoinType.LEFT_OUTER: "right-unmatched",
                JoinType.RIGHT_OUTER: "left-unmatched",
            }[target]
            self.log(
                f"{join.jointype.value}→{target.value} on {cte.name} for join with"
                f" {join.right_cte.name}: WHERE filters out"
                f" {dropped} rows that the OUTER join was preserving"
            )
            join.jointype = target
            changed = True
        return changed, None
