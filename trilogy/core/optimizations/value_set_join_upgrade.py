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

from trilogy.core.domain_graph import DomainGraph
from trilogy.core.enums import Derivation, JoinType, Modifier, SourceType
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
)
from trilogy.core.models.execute import CTE, BuildDatasource, Join, UnionCTE
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


def _authoritative_scan(side_cte: CTE | UnionCTE) -> bool:
    """A direct, unfiltered scan of a single datasource. Such a side carries
    its bindings' full value sets — the datasource IS the source of the
    concept's values (a partial binding is rejected by the caller like any
    other partial). Only consulted for EQUAL-declared keys: for undeclared
    keys a non-partial binding is a weaker claim than an author declaration
    (fact FKs are routinely complete-in-schema but value-subsets in data)."""
    if not isinstance(side_cte, CTE):
        return False
    if side_cte.condition is not None:
        return False
    source = side_cte.source
    if source.source_type != SourceType.DIRECT_SELECT:
        return False
    return len(source.datasources) == 1 and isinstance(
        source.datasources[0], BuildDatasource
    )


def _complete_distinct(
    concept: BuildConcept,
    side_cte: CTE | UnionCTE,
    allow_scan_evidence: bool = False,
) -> bool:
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
       Stamps close over the pseudonym/canonical group here: for the
       EQUIVALENCE claim (both sides carry one identical value set), a
       relation-induced stamp anywhere in the group is disqualifying.
    """
    if not isinstance(side_cte, CTE):
        return False
    keys = _key_addresses(concept)
    partial_addrs: set[str] = set()
    for partial in side_cte.partial_concepts:
        partial_addrs |= _key_addresses(partial)
    if partial_addrs & keys:
        return False
    if side_cte.group_to_grain or (
        allow_scan_evidence and _authoritative_scan(side_cte)
    ):
        grain_addrs = set(side_cte.grain.components) if side_cte.grain else set()
        return bool(grain_addrs & keys)
    return False


def _own_coverage_partial(
    concept: BuildConcept, side_cte: CTE, graph: DomainGraph
) -> bool:
    """An exact-address partial stamp that speaks to the side's own coverage
    of ``concept`` — the veto for the DIRECTIONAL (value-completeness) claim.

    The pseudonym closure is wrong twice over here: a scoped join
    pseudonym-links the two sides' key concepts (smearing the subset side's
    stamp onto the superset side), and a relation-induced stamp — a declared
    subset endpoint, stamped by the scoped-join build on the member's own
    rendering — speaks to the RELATION, not to the side's coverage of its own
    concept; an authoritative scan carries all of its concept's values
    regardless of what relations were declared over it. Only an exact-address
    stamp from outside the graph's declared subset endpoints (e.g. an
    authored `~` binding) blocks."""
    subset_endpoints = graph.subset_sources()
    return any(
        p.address == concept.address and p.address not in subset_endpoints
        for p in side_cte.partial_concepts
    )


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


def _key_nullable(concept: BuildConcept, side_cte: CTE | UnionCTE) -> bool:
    """True when ``side_cte`` can emit NULL for ``concept`` — e.g. a ROLLUP/CUBE/
    GROUPING SETS grouping key carries NULL at its subtotal/grand-total rows."""
    if not isinstance(side_cte, CTE):
        return False
    keys = _key_addresses(concept)
    nullable_addrs: set[str] = set()
    for nc in side_cte.nullable_concepts:
        nullable_addrs |= _key_addresses(nc)
    return bool(nullable_addrs & keys)


def _pair_key_sets_equivalent(
    left_concept: BuildConcept,
    left_cte: CTE | UnionCTE,
    right_concept: BuildConcept,
    right_cte: CTE | UnionCTE,
    allow_scan_evidence: bool = False,
) -> bool:
    if _source_address(left_concept) != _source_address(right_concept):
        return False
    if not _complete_distinct(left_concept, left_cte, allow_scan_evidence):
        return False
    if not _complete_distinct(right_concept, right_cte, allow_scan_evidence):
        return False
    return _filters_equivalent(
        _accumulate_filter(left_cte),
        _accumulate_filter(right_cte),
    )


_COMPLETENESS_TRANSFERRING = (Derivation.BASIC, Derivation.ROWSET)


def _complete_values(
    concept: BuildConcept,
    side_cte: CTE | UnionCTE,
    graph: DomainGraph,
) -> bool:
    """The side carries every value of the concept's domain — the superset
    test for directional narrowing. Weaker than ``_complete_distinct``:
    duplicates are allowed (fan-out is a property of the data, not of the
    join type narrowing picks), and a derived concept's domain is the image
    of its inputs' domains, so completeness transfers through BASIC/ROWSET
    lineage — but never through a FILTER, which restricts values. Only
    ``_own_coverage_partial`` stamps veto the claim — relation-induced
    stamps speak to the relation, not the side's coverage."""
    if not isinstance(side_cte, CTE):
        return False
    keys = _key_addresses(concept)
    if not _own_coverage_partial(concept, side_cte, graph):
        # scan evidence is trusted here: this path is only reachable from
        # declaration-gated callers, so scan trust is declaration-gated too
        if side_cte.group_to_grain or _authoritative_scan(side_cte):
            grain_addrs = set(side_cte.grain.components) if side_cte.grain else set()
            if grain_addrs & keys:
                return True
        # a non-partial binding on an authoritative scan carries the concept's
        # full value set even OFF the grain — grain membership only matters
        # for distinctness, which value completeness does not need
        if _authoritative_scan(side_cte) and any(
            concept.address == c.address for c in side_cte.output_columns
        ):
            return True
    if _complete_via_preserved_base(concept, side_cte, graph):
        return True
    # a pure 1:1 passthrough (a rowset translation wrapper: no grouping, no
    # joins, no condition, one parent) preserves its parent's row set, so
    # completeness carries through the projection rename
    if (
        side_cte.condition is None
        and not side_cte.joins
        and len(side_cte.parent_ctes) == 1
        and isinstance(side_cte.parent_ctes[0], CTE)
    ):
        parent = side_cte.parent_ctes[0]
        for parent_concept in parent.output_columns:
            if _key_addresses(parent_concept) & keys and _complete_values(
                parent_concept, parent, graph
            ):
                return True
        # a rowset translation renames the key with NO shared address across
        # the boundary; the wrapper's grain IS the renamed parent grain, so
        # grain membership at matching arity carries the parent's grain
        # completeness through the rename
        grain_addrs = set(side_cte.grain.components) if side_cte.grain else set()
        parent_grain = set(parent.grain.components) if parent.grain else set()
        if (
            grain_addrs & keys
            and parent.group_to_grain
            and parent_grain
            and len(parent_grain) == len(grain_addrs)
            and not any(
                p.address not in graph.subset_sources() for p in parent.partial_concepts
            )
        ):
            return True
    if concept.derivation in _COMPLETENESS_TRANSFERRING:
        args = concept.concept_arguments
        return bool(args) and all(
            _complete_values(arg, side_cte, graph) for arg in args
        )
    return False


def _equal_intersection_complete(
    concept: BuildConcept,
    side_cte: CTE | UnionCTE,
    graph: DomainGraph,
) -> bool:
    """EQUAL-trust evidence only: under an EQUAL declaration all group members
    name ONE value space, so an unfiltered all-INNER zip of complete parents
    joined on this key group stays complete BY DECLARATION — the rows the
    intersection drops are exactly the ones the declaration says don't exist.
    Never used on the proof-based (subset-directional) path."""
    if not isinstance(side_cte, CTE) or side_cte.condition is not None:
        return False
    raw_joins = side_cte.joins or []
    joins = [j for j in raw_joins if isinstance(j, Join)]
    if not joins or len(joins) != len(raw_joins):
        return False
    if any(j.jointype != JoinType.INNER for j in joins):
        return False
    keys = _key_addresses(concept)
    for j in joins:
        for p in j.joinkey_pairs or []:
            if not (_key_addresses(p.left) & keys and _key_addresses(p.right) & keys):
                return False
    parents = [p for p in side_cte.parent_ctes if isinstance(p, CTE)]
    if not parents:
        return False
    for parent in parents:
        if not any(
            _key_addresses(out) & keys
            and (
                _complete_values(out, parent, graph)
                or _equal_intersection_complete(out, parent, graph)
            )
            for out in parent.output_columns
        ):
            return False
    return True


def _complete_via_preserved_base(
    concept: BuildConcept,
    side_cte: CTE | UnionCTE,
    graph: DomainGraph,
) -> bool:
    """A join CTE carries a key's full value set when the key is provided by
    an authoritative scan the CTE's joins only PRESERVE — LEFT_OUTER/FULL
    null-extend the other side, never dropping the provider's rows. Covers
    relation CTEs whose scans are inlined (no parent CTEs to recurse into) —
    e.g. the narrowed subset relation itself, read back at a coarser level.

    A provider on the RIGHT side of a LEFT_OUTER join does not qualify: that
    side's unmatched rows drop, so its value set is filtered by the join."""
    if not isinstance(side_cte, CTE):
        return False
    if side_cte.condition is not None:
        return False
    raw_joins = side_cte.joins or []
    joins = [j for j in raw_joins if isinstance(j, Join)]
    if not joins or len(joins) != len(raw_joins):
        return False
    if any(j.jointype not in (JoinType.LEFT_OUTER, JoinType.FULL) for j in joins):
        return False
    if _own_coverage_partial(concept, side_cte, graph):
        return False
    dropped_ids: set[str] = set()
    for j in joins:
        if j.jointype != JoinType.LEFT_OUTER or j.right_cte is None:
            continue
        dropped_ids.add(j.right_cte.name)
        for ds in getattr(j.right_cte.source, "datasources", []) or []:
            dropped_ids |= {ds.identifier, ds.safe_identifier}
    scan_ids = {
        ident
        for ds in side_cte.source.datasources
        if isinstance(ds, BuildDatasource)
        for ident in (ds.identifier, ds.safe_identifier)
    }
    providers = set(side_cte.source_map.get(concept.address, []) or [])
    return bool((providers - dropped_ids) & scan_ids)


def _side_origins(side_cte: CTE | UnionCTE, group: set[str]) -> set[str]:
    """The origin domain nodes this side carries for a canonical key group:
    the authored addresses of substituted column bindings (threaded forward
    from the build — ``BuildColumnAssignment.origin_address``) whose bound
    concept renders in ``group``, collected across the side's source tree and
    parent chain. Per-COLUMN stamps discriminate where physical datasource
    identity cannot (one table binding several relation endpoints, shared-base
    self-joins reading distinct columns)."""
    out: set[str] = set()

    def walk_source(source) -> None:
        for ds in getattr(source, "datasources", []) or []:
            if isinstance(ds, BuildDatasource):
                for column in ds.columns:
                    if column.origin_address is not None and (
                        _key_addresses(column.concept) & group
                    ):
                        out.add(column.origin_address)
            else:
                walk_source(ds)

    seen: set[str] = set()

    def walk_cte(cte) -> None:
        if not isinstance(cte, CTE) or cte.name in seen:
            return
        seen.add(cte.name)
        walk_source(cte.source)
        for parent in cte.parent_ctes:
            walk_cte(parent)

    walk_cte(side_cte)
    return out


def _declared_partial(concept: BuildConcept, side_cte: CTE | UnionCTE) -> bool:
    """The side marks the key partial — a SUBSET domain declaration (a `~`
    binding, `merge a into ~b`, or a scoped subset/left join) propagated up
    the CTE chain via ``partial_concepts``."""
    if not isinstance(side_cte, CTE):
        return False
    keys = _key_addresses(concept)
    partial_addrs: set[str] = set()
    for partial in side_cte.partial_concepts:
        partial_addrs |= _key_addresses(partial)
    return bool(partial_addrs & keys)


def _proven_subset_of(
    graph: DomainGraph, sub_concept: BuildConcept, sup_concept: BuildConcept
) -> bool:
    """The graph proves the sub side's concept a subset of the sup side's: a
    directed ⊑ path over declared AND structural edges (rowset/filter lineage
    mints the latter; traversal handles chained relations and unconditioned ≡
    hops natively). Deliberately ignores ∦ declarations — an authored ∦ can be
    conservatively wrong in one direction, and a proven ⊑ path makes the
    narrowing row-identical (rule B). The sub side keys on its OWN exact
    address — the two relation endpoints are distinct concepts, one per side,
    so the plain address is side-specific — while the sup side matches through
    its pseudonym/canonical closure, since rendering may have re-addressed
    it."""
    if sub_concept.address == sup_concept.address:
        return False
    for candidate in sorted(_key_addresses(sup_concept)):
        if candidate == sub_concept.address:
            continue
        if graph.proven_subset(sub_concept.address, candidate):
            return True
    return False


def _genuine_partial_stamp(
    sub_concept: BuildConcept,
    sub_cte: CTE,
    sup_cte: CTE | UnionCTE,
    graph: DomainGraph,
) -> bool:
    """A coverage-speaking partial stamp on the sub side that the sup side
    lacks. Stamps at declared-relation subset endpoints speak to the RELATION
    and smear symmetrically across a canonical group; any other stamp is an
    authored coverage fact (a `~` binding: projection ⊑ concept), so its
    one-sided presence proves the subset direction — the author declared BOTH
    sides' relations to the domain (`~` on the sub, a complete binding on the
    sup, verified by ``_complete_values`` after)."""
    keys = _key_addresses(sub_concept)
    subset_endpoints = graph.subset_sources()
    genuine = {
        p.address
        for p in sub_cte.partial_concepts
        if p.address not in subset_endpoints and _key_addresses(p) & keys
    }
    if not genuine:
        return False
    sup_stamps = (
        {p.address for p in sup_cte.partial_concepts}
        if isinstance(sup_cte, CTE)
        else set()
    )
    return bool(genuine - sup_stamps)


def _pair_side_fully_matches(
    sub_concept: BuildConcept,
    sub_cte: CTE | UnionCTE,
    sup_concept: BuildConcept,
    sup_cte: CTE | UnionCTE,
    domain_graph: DomainGraph,
    subset_join_map: dict[str, str],
    scoped_canonical: dict[str, str],
    graph_proof_only: bool = False,
) -> bool:
    """Every row of the subset side finds a partner on the superset side, so a
    join preserving the subset side's unmatched rows preserves nothing.

    Requires SUBSET evidence on the sub side (a lying declaration is an
    author error — narrowing then drops the violating rows, the ruled
    semantics), plus proof the superset side carries the key's full domain
    HERE: complete-distinct with scan evidence trusted because the author
    declared the relation, and a filter-free chain — a *filtered* superset
    side never proves subset-match, since a filter on another column can drop
    domain values asymmetrically.

    The evidence arrives one of two ways: a ⊑ path in the domain graph
    (``_proven_subset_of`` — distinct endpoint concepts, one per side), or a
    ``partial_concepts`` stamp on the sub side, where both endpoints name one
    concept and must share a source address. ``graph_proof_only`` restricts
    to the former: rule-B narrowing through an authored ∦ veto trusts only a
    proven ⊑ path, never the stamp heuristics — ∦ collapses two genuinely
    distinct populations onto one address, exactly what stamps can't see."""
    declared = _proven_subset_of(domain_graph, sub_concept, sup_concept)
    if not declared:
        if graph_proof_only:
            return False
        if _source_address(sub_concept) != _source_address(sup_concept):
            return False
        if not _declared_partial(sub_concept, sub_cte):
            return False
        # SAME-address pair: relation-induced partial stamps land symmetrically
        # when several relations share one canonical group, so those stamps
        # alone cannot say which side is the subset HERE. A GENUINE coverage
        # stamp (a `~` binding, not a relation endpoint) present only on the
        # sub side settles the direction; otherwise this pair is the rendering
        # of a relation whose subset member got substituted onto the pair
        # address — arbitrate by the ORIGIN DOMAIN NODES threaded forward from
        # the build: the subset side carries a declared-subset origin of this
        # group that the superset side does not.
        if sub_concept.address == sup_concept.address and not (
            isinstance(sub_cte, CTE)
            and _genuine_partial_stamp(sub_concept, sub_cte, sup_cte, domain_graph)
        ):
            if not subset_join_map:
                return False
            pair_canon = scoped_canonical.get(sub_concept.address, sub_concept.address)
            group = _key_addresses(sub_concept) | {pair_canon}
            sub_origins = _side_origins(sub_cte, group)
            sup_origins = _side_origins(sup_cte, group)
            if not any(
                s != sub_concept.address
                and scoped_canonical.get(s, s) == pair_canon
                and s in sub_origins
                and s not in sup_origins
                for s in subset_join_map
            ):
                return False
    if not _complete_values(sup_concept, sup_cte, domain_graph):
        return False
    return _accumulate_filter(sup_cte) is None


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
    - Query-scoped FULL/UNION joins (``full_join_keys``): the two sides
      are independent populations with potentially disjoint key sets, and FULL
      deliberately keeps its key complete (registry-driven, not partial), so
      the complete-distinct test can't see the disjointness. The scoped merge
      collapses both keys onto one canonical address, which would otherwise
      fool the source-address / complete-distinct test into treating two
      distinct populations as the same value space. (LEFT/merge joins carry the
      partial flag and need no protection — the test fails naturally.)
      Rule B exception: an authored ∦ can be conservatively wrong in one
      direction. When the graph PROVES a subset direction — a structural ⊑
      path (rowset/filter lineage) into a complete, filter-free superset
      side — the preservation of the sub side is a no-op and the vetoed
      join still narrows DIRECTIONALLY (``_narrow_directionally`` with
      ``graph_proof_only``, never the equivalence upgrade, never the stamp
      heuristics). The ∦ stays declared; unproven pairs keep the veto.

    ``equal_join_keys`` releases that veto for keys whose FULL relation is an
    EQUAL domain DECLARATION (non-partial `merge a into b` — mutual subset,
    docs/subset_union_join_design.md): the canonical collapse then genuinely
    names one value space, so the standard completeness tests apply and the
    join may narrow to INNER. Populated only when
    ``CONFIG.optimizations.narrow_equal_domain_joins`` is on — narrowing
    trusts the declaration; data violating it loses the violating rows.
    """

    def __init__(
        self,
        domain_graph: DomainGraph | None = None,
        narrow_equal_domain_joins: bool = True,
    ) -> None:
        # The statement's declared-edge domain graph (plus author binding
        # facts) — the one source of truth for what relations were authored
        # (docs/domain_graph_design.md). The derived views below are the
        # legacy registry shapes the pass still consumes internally; they
        # dissolve as the predicates migrate to direct graph queries.
        graph = domain_graph or DomainGraph()
        self.domain_graph = graph
        # Canonical addresses of EQUAL-declared keys (non-partial `merge a
        # into b` — mutual subset): the canonical collapse genuinely names one
        # value space, so the completeness tests below legitimately arbitrate
        # and the join may narrow to INNER. Narrowing trusts the declaration;
        # data violating it loses the violating rows.
        self.equal_join_keys = (
            graph.equal_narrowable_keys() if narrow_equal_domain_joins else set()
        )
        # Canonical addresses of ∦-declared (query-scoped FULL/UNION) keys;
        # joins on these must never be upgraded to INNER (FULL's key stays
        # complete, so the partial-driven checks can't protect it).
        self.full_join_keys = graph.outer_relation_keys() - self.equal_join_keys
        # subset side (exact, side-specific address) → superset counterpart
        # for every SUBSET-declared relation; feeds directional narrowing.
        self.subset_join_map = graph.subset_join_map()
        # full member → canonical-group-root map for scoped relations; maps a
        # rendered same-address pair back to its relation group.
        self.scoped_canonical = dict(graph.canonical_map())

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
            if self.full_join_keys and any(
                _key_addresses(pair.left) & self.full_join_keys
                or _key_addresses(pair.right) & self.full_join_keys
                for pair in join.joinkey_pairs
            ):
                # Rule B: an authored ∦ vetoes the equivalence upgrade and the
                # stamp heuristics, but the graph can PROVE a subset direction
                # (structural ⊑ lineage against a complete superset side) —
                # then dropping the sub side's preservation is row-identical
                # and directional narrowing applies. The ∦ stays declared;
                # unproven pairs keep the veto.
                if self._narrow_directionally(
                    cte, join, right_cte, graph_proof_only=True
                ):
                    changed = True
                continue
            if self._upgrade_to_inner(cte, join, right_cte):
                changed = True
                continue
            if self._narrow_directionally(cte, join, right_cte):
                changed = True
        return changed, None

    def _pair_equal_declared(self, pair) -> bool:
        return bool(
            self.equal_join_keys
            and (_key_addresses(pair.left) | _key_addresses(pair.right))
            & self.equal_join_keys
        )

    def _upgrade_to_inner(
        self, cte: CTE, join: Join, right_cte: CTE | UnionCTE
    ) -> bool:
        assert join.joinkey_pairs

        def pair_equal(pair) -> bool:
            equal_declared = self._pair_equal_declared(pair)
            if _pair_key_sets_equivalent(
                pair.left,
                pair.cte,
                pair.right,
                right_cte,
                # Authoritative-scan completeness is only trusted for keys
                # the author DECLARED equal-domain; see _authoritative_scan.
                allow_scan_evidence=equal_declared,
            ):
                return True
            # An EQUAL declaration is itself the cross-concept bridge (the
            # merge collapses two concepts into one value space), so the
            # renamed-grain evidence of the directional machinery applies:
            # both sides carrying every value + equivalent filters is exactly
            # the no-unmatched-rows proof.
            if not equal_declared:
                return False
            graph = self.domain_graph
            return (
                (
                    _complete_values(pair.left, pair.cte, graph)
                    or _equal_intersection_complete(pair.left, pair.cte, graph)
                )
                and (
                    _complete_values(pair.right, right_cte, graph)
                    or _equal_intersection_complete(pair.right, right_cte, graph)
                )
                and _filters_equivalent(
                    _accumulate_filter(pair.cte),
                    _accumulate_filter(right_cte),
                )
            )

        if not all(pair_equal(pair) for pair in join.joinkey_pairs):
            return False
        # A key that is nullable on a side but joined with plain ``=`` (the
        # pair carries no NULLABLE modifier) carries NULL rows the equality
        # never matches — e.g. a ROLLUP subtotal/grand-total key. Upgrading
        # to INNER would silently drop them, so leave the OUTER join. (A
        # null-safe pair, the twin-rollup case, matches NULLs and is safe.)
        # For an EQUAL-DECLARED key both sides name one value space, so the
        # right response is to null-safe the pair — NULL is a valid member
        # and the NULL groups pair — rather than refuse the upgrade.
        for pair in join.joinkey_pairs:
            if pair.is_nullable:
                continue
            if _key_nullable(pair.left, pair.cte) or _key_nullable(
                pair.right, right_cte
            ):
                if self._pair_equal_declared(pair):
                    pair.modifiers = list(pair.modifiers) + [Modifier.NULLABLE]
                else:
                    return False
        original = join.jointype
        join.jointype = JoinType.INNER
        left_name = join.joinkey_pairs[0].cte.name
        self.log(
            f"{cte.name}: {original.value} → INNER on key-set equivalence "
            f"between {left_name} and {right_cte.name}"
        )
        return True

    def _narrow_directionally(
        self,
        cte: CTE,
        join: Join,
        right_cte: CTE | UnionCTE,
        graph_proof_only: bool = False,
    ) -> bool:
        """SUBSET-driven narrowing: preservation of a side that provably has no
        unmatched rows is a no-op, so drop it.

        A side needs preservation only for (a) key values missing from the
        other side — none when it is subset-DECLARED against a proven-complete
        superset side (``_pair_side_fully_matches``) — or (b) NULL-key rows
        with no null-safe partner — none when the pair is null-safe or the
        side proves non-null. FULL narrows to the directional join preserving
        the superset side; a directional join whose preserved side fully
        matches narrows to INNER."""
        assert join.joinkey_pairs

        def right_matches_left() -> bool:
            return all(
                _pair_side_fully_matches(
                    pair.right,
                    right_cte,
                    pair.left,
                    pair.cte,
                    self.domain_graph,
                    self.subset_join_map,
                    self.scoped_canonical,
                    graph_proof_only=graph_proof_only,
                )
                and (pair.is_nullable or not _key_nullable(pair.right, right_cte))
                for pair in join.joinkey_pairs or []
            )

        def left_matches_right() -> bool:
            return all(
                _pair_side_fully_matches(
                    pair.left,
                    pair.cte,
                    pair.right,
                    right_cte,
                    self.domain_graph,
                    self.subset_join_map,
                    self.scoped_canonical,
                    graph_proof_only=graph_proof_only,
                )
                and (pair.is_nullable or not _key_nullable(pair.left, pair.cte))
                for pair in join.joinkey_pairs or []
            )

        original = join.jointype
        target: JoinType | None = None
        if join.jointype == JoinType.FULL:
            if right_matches_left():
                target = JoinType.LEFT_OUTER
            elif left_matches_right():
                target = JoinType.RIGHT_OUTER
        elif join.jointype == JoinType.LEFT_OUTER and left_matches_right():
            target = JoinType.INNER
        elif join.jointype == JoinType.RIGHT_OUTER and right_matches_left():
            target = JoinType.INNER
        if target is None:
            return False
        join.jointype = target
        left_name = join.joinkey_pairs[0].cte.name
        self.log(
            f"{cte.name}: {original.value} → {target.value} on declared-subset "
            f"full-match between {left_name} and {right_cte.name}"
        )
        return True
