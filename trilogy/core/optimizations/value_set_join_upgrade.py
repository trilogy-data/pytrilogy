"""Upgrade outer joins whose key value sets are conceptually identical.

A FULL/LEFT/RIGHT OUTER join preserves unmatched rows from one or both sides.
When both sides of every join key pair produce *the same set of key values*,
no rows are ever unmatched — the OUTER form behaves like an INNER, just with
a slower execution plan (FULL can't be hash-joined; LEFT/RIGHT carry NULL-
padding bookkeeping the engine doesn't need). Recognise that and upgrade.

The decision is concept-level — no CTE identity, no physical addresses:

  For each join key pair we compute a ``KeySetDescriptor``:

    * ``source_address`` — the underlying concept the side projects, after
      pseudonym / merge / equivalence resolution. ``canonical_address``
      already collapses these.
    * ``filter`` — the AND of every condition applied to the row population
      that produces this concept, walking up the side's parent chain.
    * ``complete_distinct`` — True iff the side projects every distinct
      value of the *full* concept value space: the concept lives on a
      ``GROUP BY`` grain AND is not marked partial on the side. A partial
      concept represents a subset projection — distinct *within* that
      subset, but not the full concept value space. Partial-ness can be
      stamped by a number of upstream mechanisms (a datasource that's
      partial for a column, a ``MERGE`` aligning a narrow alias into a
      shared concept, ``Modifier.PARTIAL`` on a column assignment, etc.);
      this rule treats them uniformly via the ``partial_concepts`` field
      that propagates up the CTE chain.

  Two descriptors match when source addresses agree, both sides are
  ``complete_distinct``, and the accumulated filters are mutually implied
  via ``condition_implies``. When every pair matches, both sides cover
  exactly the same key tuples — the OUTER preserves no rows an INNER
  would lose.

The comparison never references CTE identity or physical datasource
addresses, so the rule is stable under optimizer rewrites that inline,
rename, hoist, or repartition intermediate CTEs.
"""

from __future__ import annotations

from trilogy.core.enums import JoinType
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
)
from trilogy.core.models.execute import CTE, Join, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.processing.condition_utility import (
    combine_condition_atoms,
    condition_implies,
)

_OUTER_JOIN_TYPES = (JoinType.FULL, JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER)


def _source_address(concept: BuildConcept) -> str:
    """Resolve ``concept`` to a stable underlying address.

    ``canonical_address`` already collapses pseudonym / merge / synonym
    relationships into a single key — concepts that share a canonical
    address represent the same logical column even when their local
    address (the alias the consumer references) differs.
    """
    return concept.canonical_address


def _key_addresses(concept: BuildConcept) -> set[str]:
    return (
        {concept.address, concept.canonical_address}
        | set(concept.pseudonyms)
        | concept.equivalent_addresses
    )


def _complete_distinct(concept: BuildConcept, side_cte: CTE | UnionCTE) -> bool:
    """True when ``side_cte`` projects every distinct value of ``concept``
    *for the concept's full value space*.

    Two conditions:

    1. The concept lives on a GROUP BY grain key here (``group_to_grain``
       with the concept in the grain). Cardinality at the grain means no
       two source rows collapse to one — the side carries exactly the
       source's distinct values modulo the accumulated filter.
    2. The side does NOT mark the concept as *partial*. A partial concept
       is a subset projection — distinct *within* that subset, but not the
       full concept value space. Partial-ness arrives via any of several
       upstream mechanisms (a partial datasource binding, a ``MERGE``
       alignment, a ``Modifier.PARTIAL`` column assignment, …); the
       ``partial_concepts`` field on the CTE propagates that signal
       uniformly, and we read it without caring which mechanism set it.
       Two partial sides may individually be GROUP BY-distinct but their
       subsets don't coincide — never a basis for upgrading an outer join.
    """
    if not isinstance(side_cte, CTE):
        return False
    if not side_cte.group_to_grain:
        return False
    grain_addrs = set(side_cte.grain.components) if side_cte.grain else set()
    if not grain_addrs:
        return False
    keys = _key_addresses(concept)
    if not (grain_addrs & keys):
        return False
    partial_addrs: set[str] = set()
    for partial in side_cte.partial_concepts:
        partial_addrs |= _key_addresses(partial)
    return not (partial_addrs & keys)


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
    return combine_condition_atoms(parts)


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
    - Query-scoped FULL joins (``protected_outer_join_keys``): the two sides
      are independent populations with potentially disjoint key sets, and FULL
      deliberately keeps its key complete (registry-driven, not partial), so
      the complete-distinct test can't see the disjointness. The scoped merge
      collapses both keys onto one canonical address, which would otherwise
      fool the source-address / complete-distinct test into treating two
      distinct populations as the same value space. (LEFT/merge joins carry the
      partial flag and need no protection — the test fails naturally.)
    """

    def __init__(self, protected_outer_join_keys: set[str] | None = None) -> None:
        # Canonical addresses of query-scoped FULL-join keys; joins on these
        # must never be upgraded to INNER (FULL's key stays complete, so the
        # partial-driven checks can't protect it).
        self.protected_outer_join_keys = protected_outer_join_keys or set()

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
            if self.protected_outer_join_keys and any(
                _key_addresses(pair.left) & self.protected_outer_join_keys
                or _key_addresses(pair.right) & self.protected_outer_join_keys
                for pair in join.joinkey_pairs
            ):
                continue
            right_cte = join.right_cte
            if not all(
                _pair_key_sets_equivalent(pair.left, pair.cte, pair.right, right_cte)
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
