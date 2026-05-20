"""Upgrade outer joins whose key value sets are conceptually identical.

A FULL/LEFT/RIGHT OUTER join preserves unmatched rows from one or both sides.
When both sides of every join key pair produce *the same set of key values*,
no rows are ever unmatched — the OUTER form behaves like an INNER, just with
a slower execution plan (FULL can't be hash-joined; LEFT/RIGHT carry NULL-
padding bookkeeping the engine doesn't need). Recognise that and upgrade.

The decision is concept-level, not CTE-structural:

  For each join key pair we compute a ``KeySetDescriptor``:

    * ``source_address`` — the underlying concept the side projects, after
      pseudonym / merge / equivalence resolution (e.g. ``local.customer_id``
      backed by ``store_sales.customer.id`` resolves to the latter).
    * ``filter`` — the AND of every condition applied to the row population
      that produces this concept, walking up the side's parent chain.
    * ``complete_distinct`` — True iff the side projects every distinct
      value of the source under the accumulated filter (it lives on a
      ``GROUP BY`` grain that includes the concept).

  Two descriptors match when source addresses agree, both sides are
  ``complete_distinct``, and the accumulated filters are mutually implied via
  ``condition_implies``. When every pair matches, both sides cover exactly
  the same key tuples — the OUTER preserves no rows an INNER would lose.

The comparison never references CTE identity, so the rule is stable under
optimizer rewrites that inline, rename, or hoist intermediate CTEs.
"""

from __future__ import annotations

from trilogy.core.enums import BooleanOperator, JoinType
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
)
from trilogy.core.models.execute import CTE, Join, QueryDatasource, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.processing.condition_utility import condition_implies

_OUTER_JOIN_TYPES = (JoinType.FULL, JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER)


def _source_address(concept: BuildConcept) -> str:
    """Resolve ``concept`` to a stable underlying address.

    ``canonical_address`` already collapses pseudonym / merge / synonym
    relationships into a single key — concepts that share a canonical
    address represent the same logical column even when their local
    address (the alias the consumer references) differs.
    """
    return concept.canonical_address


def _complete_distinct(concept: BuildConcept, side_cte: CTE | UnionCTE) -> bool:
    """True when ``side_cte`` projects every distinct value of ``concept``
    under its accumulated filter — i.e. the concept lives on a GROUP BY
    grain key here. Cardinality below the grain (a row per group) means
    no two source rows collapse to one — the side's key column carries
    exactly the source's distinct values, modulo the filter."""
    if not isinstance(side_cte, CTE):
        return False
    if not side_cte.group_to_grain:
        return False
    grain_addrs = set(side_cte.grain.components) if side_cte.grain else set()
    if not grain_addrs:
        return False
    keys = {concept.address, concept.canonical_address} | set(concept.pseudonyms)
    keys |= concept.equivalent_addresses
    return bool(grain_addrs & keys)


def _accumulate_filter(
    side_cte: CTE | UnionCTE,
    _visited: frozenset[str] = frozenset(),
) -> BoolExpr | None:
    """AND of every condition applied along ``side_cte``'s parent chain.

    Walks consumer → producer (allowed direction). Returns ``None`` when no
    condition appears anywhere in the chain. Cycle-safe via ``_visited``.

    The filter represents the row population that produces this side's
    rows. For a sibling-rollup case, two sides share the same chain of
    filters; for an independent aggregation (a HAVING-bounded subset, a
    year-restricted slice), the chains diverge and the resulting filter
    expressions won't mutually imply each other.
    """
    if not isinstance(side_cte, CTE):
        # A ``UnionCTE`` produces UNION ALL of its branches — its row
        # population mixes per-branch filters that don't AND together
        # cleanly. Treat as opaque so the equivalence test conservatively
        # fails and we don't upgrade.
        return None
    if side_cte.name in _visited:
        return None
    next_visited = _visited | {side_cte.name}
    parts: list[BoolExpr] = []
    if side_cte.condition is not None:
        parts.append(side_cte.condition)
    for parent in side_cte.parent_ctes:
        sub = _accumulate_filter(parent, next_visited)
        if sub is not None:
            parts.append(sub)
    if not parts:
        return None
    result = parts[0]
    for atom in parts[1:]:
        result = BuildConditional(left=result, right=atom, operator=BooleanOperator.AND)
    return result


def _collect_base_datasources(
    node: BuildDatasource | QueryDatasource | object, out: set[str]
) -> None:
    if isinstance(node, BuildDatasource):
        out.add(node.identifier)
        return
    if isinstance(node, QueryDatasource):
        for child in node.datasources:
            _collect_base_datasources(child, out)


def _leaf_datasource_addresses(side_cte: CTE | UnionCTE) -> frozenset[str]:
    """Identifiers of base tables backing the rows ``side_cte`` projects.

    Two sides projecting the same canonical concept from *different* base
    tables (e.g. ``customer.id`` materialised from ``web_sales`` on one side
    and ``catalog_sales`` on the other) produce different row populations
    even when filters happen to coincide — only the customers who actually
    shopped on that channel show up. The descriptor must distinguish them.

    Walks the side's own ``QueryDatasource`` tree to leaf
    ``BuildDatasource`` nodes, then recurses through ``parent_ctes`` for the
    full upstream coverage. ``UnionCTE`` chains are not unfolded — the
    consumer treats them as a single opaque source via ``parent_ctes``.
    """
    leaves: set[str] = set()
    if isinstance(side_cte, CTE):
        _collect_base_datasources(side_cte.source, leaves)
        for parent in side_cte.parent_ctes:
            leaves |= _leaf_datasource_addresses(parent)
    elif isinstance(side_cte, UnionCTE):
        for branch in side_cte.internal_ctes:
            leaves |= _leaf_datasource_addresses(branch)
    return frozenset(leaves)


def _filters_equivalent(a: BoolExpr | None, b: BoolExpr | None) -> bool:
    """Both filters cover exactly the same surviving rows.

    Uses ``condition_implies`` in both directions — atom-set containment
    handles BETWEEN, IN, SubselectComparison, etc. uniformly. Two ``None``
    filters are trivially equivalent; a one-sided ``None`` is not.
    """
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return condition_implies(a, b) and condition_implies(b, a)


def _pair_key_sets_equivalent(
    left_concept: BuildConcept,
    left_cte: CTE | UnionCTE,
    right_concept: BuildConcept,
    right_cte: CTE | UnionCTE,
) -> bool:
    if _source_address(left_concept) != _source_address(right_concept):
        return False
    if not _complete_distinct(left_concept, left_cte):
        return False
    if not _complete_distinct(right_concept, right_cte):
        return False
    # Same canonical concept can be materialised from different base tables —
    # ``customer.id`` from ``web_sales`` is *not* the same value set as
    # ``customer.id`` from ``catalog_sales`` (only the customers who shopped
    # on that channel). Insist the leaf datasource set matches before
    # claiming the key sets coincide.
    if _leaf_datasource_addresses(left_cte) != _leaf_datasource_addresses(right_cte):
        return False
    return _filters_equivalent(
        _accumulate_filter(left_cte),
        _accumulate_filter(right_cte),
    )


class UpgradeOuterFromKeySetEquivalence(OptimizationRule):
    """Upgrade FULL/LEFT/RIGHT OUTER → INNER when each join key pair has
    identical conceptual value sets on both sides.

    See module docstring for the descriptor model. Catches:

    - The "twin rollup" pattern (TPC-DS q98, q12, q20, q63, q89, q98 ...)
      where one or both sides are GROUP BY rollups of a shared filtered
      source, joined back on the rollup key.
    - Sibling aggregations whose effective WHERE chains mutually imply.

    Skips:

    - Cross-source joins (e.g. ``store_sales`` ↔ ``catalog_sales``):
      source addresses differ, equivalence fails.
    - Year-over-year / channel comparisons where one side carries an extra
      WHERE: filters fail mutual implication.
    - Sides without ``group_to_grain``: cardinality unknown, can't claim
      the side carries every distinct value.
    """

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE):
            return False, None
        changed = False
        for join in cte.joins or []:
            if not isinstance(join, Join):
                continue
            if join.jointype not in _OUTER_JOIN_TYPES:
                continue
            if not join.joinkey_pairs:
                continue
            right_cte = join.right_cte
            if not all(
                _pair_key_sets_equivalent(
                    pair.left, pair.cte, pair.right, right_cte
                )
                for pair in join.joinkey_pairs
            ):
                continue
            original = join.jointype
            join.jointype = JoinType.INNER
            changed = True
            left_name = join.joinkey_pairs[0].cte.name
            self.log(
                f"{cte.name}: {original.value} → INNER on key-set equivalence "
                f"between {left_name} and {right_cte.name}"
            )
        return changed, None
