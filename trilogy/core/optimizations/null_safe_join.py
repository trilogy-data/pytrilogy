"""Downgrade null-safe join keys to plain equality when proven safe.

``extra_align_joins`` (multiselect / MERGE ALIGN) stamps every align join key
with ``Modifier.NULLABLE``, which the dialect renders as ``IS NOT DISTINCT
FROM``. That null-safe form differs from ``=`` only for rows where *both* keys
are NULL. ``UpgradeJoinOnGuards`` can prove such an align INNER but leaves the
now-stale modifier behind. For an INNER join, if either side's key is provably
non-null in its producing CTE the two forms are equivalent, so the modifier is
dead weight (and ``IS NOT DISTINCT FROM`` is the slower, non-hash-joinable
form). Once join types and CTE nullability have settled, strip the redundant
modifier on INNER joins so plain ``=`` is emitted.
"""

from __future__ import annotations

from trilogy.core.enums import JoinType, Modifier
from trilogy.core.models.build import BuildConcept
from trilogy.core.models.execute import CTE, Join, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule


def _equivalent_addrs(concepts: list[BuildConcept]) -> set[str]:
    out: set[str] = set()
    for c in concepts:
        out |= c.equivalent_addresses
    return out


def _proven_non_null(concept: BuildConcept, cte: CTE | UnionCTE) -> bool:
    """True when ``concept`` is sourced from ``cte`` and cannot be NULL there.

    ``UnionCTE`` carries no nullability tracking, so it can never *prove*
    non-null — stay conservative and keep the null-safe form there."""
    if not isinstance(cte, CTE):
        return False
    output = _equivalent_addrs(list(cte.output_columns))
    if concept.equivalent_addresses.isdisjoint(output):
        return False
    nullable = _equivalent_addrs(list(cte.nullable_concepts))
    return concept.equivalent_addresses.isdisjoint(nullable)


class SimplifyNullSafeJoins(OptimizationRule):
    """Strip ``Modifier.NULLABLE`` from INNER join keys with a provably
    non-null side, so the renderer emits ``=`` instead of ``IS NOT DISTINCT
    FROM``.

    Restricted to INNER joins: an OUTER align (e.g. a FULL MERGE/ALIGN) wants
    the null-safe form so unmatched NULL keys group together, and the
    nullability tracking on intermediate projection CTEs under-reports there.
    The reported case is a FULL align that ``UpgradeJoinOnGuards`` proved
    INNER but left a stale ``Modifier.NULLABLE`` behind; once it is INNER the
    null-safe form only adds spurious NULL-vs-NULL matches that ``=`` (with a
    non-null side) provably never produces."""

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE):
            return False, None
        changed = False
        for join in cte.joins or []:
            if (
                not isinstance(join, Join)
                or join.jointype != JoinType.INNER
                or not join.joinkey_pairs
            ):
                continue
            all_pairs_safe = True
            for pair in join.joinkey_pairs:
                safe = _proven_non_null(pair.left, pair.cte) or _proven_non_null(
                    pair.right, join.right_cte
                )
                all_pairs_safe = all_pairs_safe and safe
                if safe and Modifier.NULLABLE in pair.modifiers:
                    pair.modifiers = [
                        m for m in pair.modifiers if m != Modifier.NULLABLE
                    ]
                    changed = True
            if all_pairs_safe and Modifier.NULLABLE in join.modifiers:
                join.modifiers = [m for m in join.modifiers if m != Modifier.NULLABLE]
                changed = True
                self.log(
                    f"{cte.name}: join with {join.right_cte.name} keys provably "
                    "non-null; using = instead of IS NOT DISTINCT FROM"
                )
        return changed, None
