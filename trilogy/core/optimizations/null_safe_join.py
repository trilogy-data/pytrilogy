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

A CTE is proven non-null for a column when (a) build-time
``nullable_concepts`` tracking excludes it, (b) the CTE's own ``condition``
null-rejects it (predicate pushdown migrates downstream ``IS NOT NULL`` /
null-propagating filters into the producer's condition, so this picks them up
without any optimizer looking at the consumer), or (c) for a ``UnionCTE``,
every branch independently proves the column non-null.

The *joining* CTE's own condition also counts: it applies to the joined
output, so the rows a plain ``=`` would drop relative to ``IS NOT DISTINCT
FROM`` (both key sides NULL) are exactly rows the WHERE deletes anyway when it
null-rejects any member of the key's equivalence family.
"""

from __future__ import annotations

from trilogy.core.enums import AggregateGroupingMode, JoinType, Modifier
from trilogy.core.models.build import BuildAggregateWrapper, BuildConcept
from trilogy.core.models.execute import CTE, Join, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.processing.condition_utility import condition_proves_non_null


def _equivalent_addrs(concepts: list[BuildConcept]) -> set[str]:
    out: set[str] = set()
    for c in concepts:
        out |= c.equivalent_addresses
    return out


def _join_pads_null(cte: CTE, addrs: set[str]) -> bool:
    """True when one of ``cte``'s own outer joins NULL-pads any address in
    ``addrs``.

    Used by the parent-walk in ``_proven_non_null`` to refuse a recursive
    proof when a column that's non-null in some upstream producer is
    re-introduced as nullable by ``cte``'s own join. The build-time
    ``nullable_concepts`` already reflects this, so we only have to recurse
    upstream when the local join can't be the source of the nullability."""
    if not cte.joins:
        return False
    for join in cte.joins:
        if not isinstance(join, Join) or join.jointype == JoinType.INNER:
            continue
        if join.jointype in (JoinType.LEFT_OUTER, JoinType.FULL):
            right_outputs = _equivalent_addrs(list(join.right_cte.output_columns))
            if not addrs.isdisjoint(right_outputs):
                return True
        if join.jointype in (JoinType.RIGHT_OUTER, JoinType.FULL):
            if join.left_cte is not None:
                left_outputs = _equivalent_addrs(list(join.left_cte.output_columns))
                if not addrs.isdisjoint(left_outputs):
                    return True
            for pair in join.joinkey_pairs or []:
                if pair.cte is None:
                    continue
                pair_outputs = _equivalent_addrs(list(pair.cte.output_columns))
                if not addrs.isdisjoint(pair_outputs):
                    return True
    return False


def _rollup_injects_null(cte: CTE, addrs: set[str]) -> bool:
    """True when ``cte`` performs a ROLLUP/CUBE/GROUPING SETS that injects NULLs
    into any address in ``addrs`` (its grouping-key dims, or anything marked
    nullable as a result). Like ``_join_pads_null``, this stops the parent-walk
    from proving a rollup key non-null via the upstream source where it *is*
    non-null — the rollup is the source of the NULLs at subtotal rows."""
    has_rollup = any(
        isinstance(c.lineage, BuildAggregateWrapper)
        and c.lineage.grouping != AggregateGroupingMode.STANDARD
        for c in cte.output_columns
    )
    if not has_rollup:
        return False
    return not addrs.isdisjoint(_equivalent_addrs(list(cte.nullable_concepts)))


def _proven_non_null(
    concept: BuildConcept,
    cte: CTE | UnionCTE,
    _visited: frozenset[str] = frozenset(),
) -> bool:
    """True when ``concept`` is sourced from ``cte`` and cannot be NULL there.

    The proof is layered, each layer reading only the local CTE or *upstream*
    parents (never consumers):

    1. ``UnionCTE``: every internal branch must independently prove the
       concept non-null.
    2. Plain CTE: the build-time ``nullable_concepts`` already excludes it
       (no NULL anywhere upstream + no outer join here pads it).
    3. The CTE's own ``condition`` null-rejects it (predicate pushdown will
       have migrated downstream ``IS NOT NULL`` / null-propagating filters
       into the producing CTE's condition).
    4. The CTE pulls the concept from a parent CTE and *every* parent that
       emits it proves it non-null, and no local outer join NULL-pads the
       column. Walking the parent chain catches the common case where a
       UNION ALL's branches each filter ``x is not null`` but the union
       output and projection CTEs in between never re-derived their
       ``nullable_concepts`` from the refined branches.
    """
    if isinstance(cte, UnionCTE):
        branches = list(cte.internal_ctes)
        if not branches:
            return False
        return all(_proven_non_null(concept, branch, _visited) for branch in branches)
    if not isinstance(cte, CTE):
        return False
    output = _equivalent_addrs(list(cte.output_columns))
    if concept.equivalent_addresses.isdisjoint(output):
        return False
    nullable = _equivalent_addrs(list(cte.nullable_concepts))
    if concept.equivalent_addresses.isdisjoint(nullable):
        return True
    if cte.condition is not None:
        proven = condition_proves_non_null(cte.condition)
        if not concept.equivalent_addresses.isdisjoint(proven):
            return True
    if cte.name in _visited:
        return False
    # An outer join — or a ROLLUP/CUBE — here may itself introduce the NULL;
    # don't claim a parent proof if our own node is the source of the nullability.
    if _join_pads_null(cte, concept.equivalent_addresses) or _rollup_injects_null(
        cte, concept.equivalent_addresses
    ):
        return False
    next_visited = _visited | {cte.name}
    contributing = [
        parent
        for parent in cte.parent_ctes
        if not concept.equivalent_addresses.isdisjoint(
            _equivalent_addrs(list(parent.output_columns))
        )
    ]
    if not contributing:
        return False
    return all(
        _proven_non_null(concept, parent, next_visited) for parent in contributing
    )


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
    non-null side) provably never produces.

    Proof sources: each side's producing CTE, plus the joining CTE's own
    condition — a WHERE that null-rejects the key family deletes the both-NULL
    rows the null-safe form would otherwise admit."""

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE):
            return False, None
        changed = False
        local_proofs: set[str] | None = None
        for join in cte.joins or []:
            if (
                not isinstance(join, Join)
                or join.jointype != JoinType.INNER
                or not join.joinkey_pairs
            ):
                continue
            if local_proofs is None:
                local_proofs = (
                    condition_proves_non_null(cte.condition)
                    if cte.condition is not None
                    else set()
                )
            all_pairs_safe = True
            for pair in join.joinkey_pairs:
                safe = (
                    not pair.left.equivalent_addresses.isdisjoint(local_proofs)
                    or not pair.right.equivalent_addresses.isdisjoint(local_proofs)
                    or _proven_non_null(pair.left, pair.cte)
                    or _proven_non_null(pair.right, join.right_cte)
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
