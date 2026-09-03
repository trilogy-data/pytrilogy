"""Downgrade null-safe join keys to plain equality when proven safe.

Align joins stamp every key with ``Modifier.NULLABLE``, rendered as ``IS NOT
DISTINCT FROM``, which differs from ``=`` only when both keys are NULL. On an
INNER join whose key is provably non-null on either side the two forms are
equivalent, and ``IS NOT DISTINCT FROM`` is the slower, non-hash-joinable one.
Runs after join types and CTE nullability have settled.

A CTE proves a column non-null when (a) build-time ``nullable_concepts``
excludes it, (b) the CTE's own ``condition`` null-rejects it (predicate
pushdown migrates downstream null-rejecting filters into the producer), or
(c) for a ``UnionCTE``, every branch independently proves it.

The joining CTE's own condition also counts: it applies to the joined output,
so the both-NULL rows a plain ``=`` would drop are exactly the rows the WHERE
deletes anyway when it null-rejects any member of the key's equivalence family.
"""

from __future__ import annotations

from trilogy.core.enums import JoinType, Modifier
from trilogy.core.models.build import (
    BuildConcept,
    nonstandard_grouping_lineage,
)
from trilogy.core.models.execute import CTE, Join, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import equivalent_addresses
from trilogy.core.processing.condition_utility import condition_proves_non_null


def _join_pads_null(cte: CTE, addrs: set[str]) -> bool:
    """True when one of ``cte``'s own outer joins NULL-pads any address in
    ``addrs``. The parent-walk in ``proven_non_null`` uses this to refuse a
    recursive proof when the local join is itself the source of nullability."""
    if not cte.joins:
        return False
    for join in cte.joins:
        if not isinstance(join, Join) or join.jointype == JoinType.INNER:
            continue
        if join.jointype in (JoinType.LEFT_OUTER, JoinType.FULL):
            right_outputs = equivalent_addresses(list(join.right_cte.output_columns))
            if not addrs.isdisjoint(right_outputs):
                return True
        if join.jointype in (JoinType.RIGHT_OUTER, JoinType.FULL):
            if join.left_cte is not None:
                left_outputs = equivalent_addresses(list(join.left_cte.output_columns))
                if not addrs.isdisjoint(left_outputs):
                    return True
            for pair in join.joinkey_pairs or []:
                if pair.cte is None:
                    continue
                pair_outputs = equivalent_addresses(list(pair.cte.output_columns))
                if not addrs.isdisjoint(pair_outputs):
                    return True
    return False


def _null_padded_sources(cte: CTE) -> set[str]:
    """Names of ``cte``'s sources whose columns its own outer joins NULL-pad."""
    padded: set[str] = set()
    for join in cte.joins or []:
        if not isinstance(join, Join) or join.jointype == JoinType.INNER:
            continue
        if join.jointype in (JoinType.LEFT_OUTER, JoinType.FULL):
            padded.add(join.right_cte.name)
        if join.jointype in (JoinType.RIGHT_OUTER, JoinType.FULL):
            if join.left_cte is not None:
                padded.add(join.left_cte.name)
            for pair in join.joinkey_pairs or []:
                if pair.cte is not None:
                    padded.add(pair.cte.name)
    return padded


def _coalesced_source_non_null(
    cte: CTE, concept: BuildConcept, visited: frozenset[str]
) -> bool:
    """A column drawn from several sources renders as ``COALESCE`` over them, so
    it is non-null as soon as one source the local joins cannot NULL-pad proves
    it non-null, even while another source sits on an outer join's optional
    side."""
    sources: set[str] = set()
    for addr in concept.equivalent_addresses:
        sources.update(cte.source_map.get(addr, []) or [])
    if len(sources) < 2:
        return False
    padded = _null_padded_sources(cte)
    parents = {parent.name: parent for parent in cte.parent_ctes}
    return any(
        name not in padded
        and name in parents
        and proven_non_null(concept, parents[name], visited)
        for name in sources
    )


def _rollup_injects_null(cte: CTE, addrs: set[str]) -> bool:
    """True when ``cte`` performs a ROLLUP/CUBE/GROUPING SETS that injects NULLs
    into any address in ``addrs``. Like ``_join_pads_null``, this stops the
    parent-walk from proving a rollup key non-null via an upstream source: the
    rollup itself is the source of the NULLs at subtotal rows."""
    has_rollup = any(
        nonstandard_grouping_lineage(c) is not None for c in cte.output_columns
    )
    if not has_rollup:
        return False
    return not addrs.isdisjoint(equivalent_addresses(list(cte.nullable_concepts)))


def proven_non_null(
    concept: BuildConcept,
    cte: CTE | UnionCTE,
    _visited: frozenset[str] = frozenset(),
) -> bool:
    """True when ``concept`` is sourced from ``cte`` and cannot be NULL there.

    Each layer reads only the local CTE or upstream parents, never consumers:

    1. ``UnionCTE``: every internal branch must independently prove the
       concept non-null.
    2. Plain CTE: the build-time ``nullable_concepts`` already excludes it.
    3. The CTE's own ``condition`` null-rejects it.
    4. Every parent that emits the concept proves it non-null, and no local
       outer join or rollup NULL-pads the column. Intermediate union and
       projection CTEs do not re-derive ``nullable_concepts`` from refined
       branches, so the parent walk is what recovers that proof.
    """
    if isinstance(cte, UnionCTE):
        branches = list(cte.internal_ctes)
        if not branches:
            return False
        return all(proven_non_null(concept, branch, _visited) for branch in branches)
    if not isinstance(cte, CTE):
        return False
    output = equivalent_addresses(list(cte.output_columns))
    if concept.equivalent_addresses.isdisjoint(output):
        return False
    nullable = equivalent_addresses(list(cte.nullable_concepts))
    if concept.equivalent_addresses.isdisjoint(nullable):
        return True
    if cte.condition is not None:
        proven = condition_proves_non_null(cte.condition)
        if not concept.equivalent_addresses.isdisjoint(proven):
            return True
    if cte.name in _visited:
        return False
    next_visited = _visited | {cte.name}
    # A local outer join or ROLLUP/CUBE may itself introduce the NULL; a parent
    # proof does not apply then.
    if _rollup_injects_null(cte, concept.equivalent_addresses):
        return False
    if _join_pads_null(cte, concept.equivalent_addresses):
        return _coalesced_source_non_null(cte, concept, next_visited)
    contributing = [
        parent
        for parent in cte.parent_ctes
        if not concept.equivalent_addresses.isdisjoint(
            equivalent_addresses(list(parent.output_columns))
        )
    ]
    if not contributing:
        return False
    return all(
        proven_non_null(concept, parent, next_visited) for parent in contributing
    )


class SimplifyNullSafeJoins(OptimizationRule):
    """Strip ``Modifier.NULLABLE`` from INNER join keys with a provably
    non-null side, so the renderer emits ``=`` instead of ``IS NOT DISTINCT
    FROM``.

    Restricted to INNER joins: an OUTER align wants the null-safe form so
    unmatched NULL keys group together, and nullability tracking on
    intermediate projection CTEs under-reports there.

    Proof sources: each side's producing CTE, plus the joining CTE's own
    condition (a WHERE that null-rejects the key family deletes the both-NULL
    rows the null-safe form would otherwise admit)."""

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
                    or proven_non_null(pair.left, pair.cte)
                    or proven_non_null(pair.right, join.right_cte)
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
