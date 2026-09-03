"""Narrow outer joins whose preserved side provably has no unmatched rows.

Two proofs, both concept-level (no CTE identity, no physical addresses, so
the rule is stable under inlining, renaming, hoisting and repartitioning):

* Directional narrowing (``_narrow_directionally``), the workhorse: a side
  needs preservation only for key values the other side lacks, or for
  NULL-key rows with no null-safe partner. When the domain graph proves one
  side a subset of a complete, filter-free superset side (a declared ``?``
  relation, a rowset/filter lineage path, or a ``_relative_key_subset``
  proof), preserving the subset side is a no-op: FULL narrows to the
  directional join preserving the superset side, and a directional join
  whose preserved side fully matches narrows to INNER.

* Equivalence upgrade (``_upgrade_to_inner``), the narrow case: every join
  key pair carries the same value set on both sides, so no row is ever
  unmatched and the OUTER is an INNER. A pair matches when both sides are
  ``complete_distinct`` (the key is on a GROUP BY grain and not partial on
  that side), resolve to the same source address, and their accumulated
  parent-chain filters mutually imply. Under an EQUAL declaration
  (``equal_join_keys``) the weaker ``_complete_values`` test is accepted
  instead of distinctness.

Completeness evidence is always gated by declaration: scan coverage and
grain membership only count for keys the author declared complete, and a
row LIMIT or a FILTER anywhere in a side's chain vetoes the claim.
"""

from __future__ import annotations

from trilogy.core.domain_graph import DomainGraph, ResolvedRelation
from trilogy.core.enums import Derivation, JoinType, Modifier, SourceType
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildRowsetItem,
)
from trilogy.core.models.execute import CTE, BuildDatasource, Join, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.processing.condition_utility import (
    combine_condition_atoms,
    condition_implies,
)
from trilogy.core.processing.join_resolution import (
    OUTER_JOIN_TYPES,
    _padding_sources,
    nulls_are_values,
)


def _source_address(concept: BuildConcept) -> str:
    """Resolve ``concept`` to a stable underlying address: ``canonical_address``
    collapses pseudonym / merge / synonym relationships into a single key, so
    concepts sharing one represent the same logical column even when their
    local alias differs.
    """
    return concept.canonical_address


def _key_addresses(concept: BuildConcept) -> set[str]:
    return (
        {concept.address, concept.canonical_address}
        | set(concept.pseudonyms)
        | concept.equivalent_addresses
    )


def _row_limited(
    side_cte: CTE | UnionCTE, _visited: frozenset[str] = frozenset()
) -> bool:
    """A row LIMIT anywhere in the side's chain truncates its row population,
    so no value-completeness claim survives it. Conservative by design: a
    limit on a sibling branch preserved through outer joins could in
    principle keep another provider complete, but the FULL it leaves behind
    is the correct price for a truncating construct."""
    if not isinstance(side_cte, CTE):
        return False
    if side_cte.limit is not None:
        return True
    if side_cte.name in _visited:
        return False
    next_visited = _visited | {side_cte.name}
    return any(_row_limited(parent, next_visited) for parent in side_cte.parent_ctes)


def _authoritative_scan(side_cte: CTE | UnionCTE) -> bool:
    """A direct, unfiltered scan of a single datasource carries its bindings'
    full value sets (a partial binding is rejected by the caller like any
    other partial). Only consulted for EQUAL-declared keys: for undeclared
    keys a non-partial binding is a weaker claim than an author declaration,
    since fact FKs are routinely complete-in-schema but value-subsets in data."""
    if not isinstance(side_cte, CTE):
        return False
    if side_cte.condition is not None or side_cte.limit is not None:
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
    for the concept's full value space:

    1. The concept lives on a GROUP BY grain key here, so the side carries
       exactly the source's distinct values modulo the accumulated filter.
    2. The side does not mark the concept partial. A partial concept is a
       subset projection, distinct within that subset but not the full value
       space; ``partial_concepts`` propagates that signal uniformly whatever
       upstream mechanism set it. Stamps close over the pseudonym/canonical
       group here: for the equivalence claim (both sides carry one identical
       value set), a relation-induced stamp anywhere in the group is
       disqualifying.
    """
    if not isinstance(side_cte, CTE):
        return False
    if _row_limited(side_cte):
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
    of ``concept``: the veto for the directional (value-completeness) claim.

    The pseudonym closure is wrong here twice over: a scoped join
    pseudonym-links the two sides' key concepts (smearing the subset side's
    stamp onto the superset side), and a relation-induced stamp at a declared
    subset endpoint speaks to the relation, not to the side's coverage of its
    own concept. Only an exact-address stamp from outside the graph's declared
    subset endpoints (an authored `~` binding) blocks.

    A genuine `~` stamp cannot hide at a pseudonym address of the key: build
    substitution re-addresses partial stamps onto the rendered pair address,
    and a `~`-bound alias column never serves as the key's provider. Closing
    over the pseudonym group would instead false-veto a scan that binds the
    key completely alongside a `~` projection of a merged sibling."""
    subset_endpoints = graph.subset_sources()
    return any(
        p.address == concept.address and p.address not in subset_endpoints
        for p in side_cte.partial_concepts
    )


def _rowset_definition_boundary(
    concept: BuildConcept, side_cte: CTE | UnionCTE
) -> bool:
    """``side_cte`` is ``concept``'s own rowset materialization boundary: a
    ROWSET-derived key output here whose parent CTEs do not carry it under its
    own address.

    A rowset boundary is opaque: the output is a freshly-named concept whose
    value set is whatever the body produces. The body WHERE/HAVING defines
    that domain rather than restricting a pre-existing one, so at the rename
    boundary the side carries every value of the concept by construction; a
    `subset` join declaring `a ⊆ rs.k` narrows to the member-dropping LEFT
    exactly as against a plain datasource.

    An external filter on the rowset output lands in a CTE above this boundary
    whose parent still carries the key, so it fails the parent-exclusion test
    and falls through to the normal ``_complete_values`` / filter-free checks.
    Matching is on the concept's own address, not the pseudonym closure: a
    scoped-join merge pseudonym-links the two sides' keys, and the closure
    would let the other side's address leak into the parent-exclusion test. A
    boundary CTE with the body's scans inlined (no parent CTEs) is the same
    boundary; nothing above it can have been folded in unless it filters the
    rowset's own outputs, which the body's pre-rename WHERE never names."""
    if not isinstance(side_cte, CTE):
        return False
    if concept.derivation != Derivation.ROWSET:
        return False
    addr = concept.address
    if not any(out.address == addr for out in side_cte.output_columns):
        return False
    if not side_cte.parent_ctes:
        return not _filters_own_rowset_outputs(concept, side_cte)
    return not any(
        out.address == addr
        for parent in side_cte.parent_ctes
        if isinstance(parent, CTE)
        for out in parent.output_columns
    )


def _filters_own_rowset_outputs(concept: BuildConcept, side_cte: CTE) -> bool:
    """A condition on this CTE references one of the rowset's own output
    handles: an external filter on the materialized rows folded in, not the
    body's definitional WHERE."""
    lineage = concept.lineage
    if not isinstance(lineage, BuildRowsetItem):
        return True
    condition = _accumulate_filter(side_cte)
    if condition is None:
        return False
    derived = set(lineage.rowset.derived_concepts)
    return any(arg.address in derived for arg in condition.concept_arguments)


def _accumulate_filter(
    side_cte: CTE | UnionCTE,
    _visited: frozenset[str] = frozenset(),
) -> BoolExpr | None:
    """AND of every condition applied along ``side_cte``'s parent chain, or
    ``None`` when the chain carries none. Sibling rollups share one chain of
    filters; independent aggregations diverge and their filters do not
    mutually imply.
    """
    if not isinstance(side_cte, CTE):
        # A UnionCTE's row population mixes per-branch filters that do not
        # AND together; treat it as opaque so the equivalence test fails.
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
    """Both filters cover exactly the same surviving rows (mutual
    ``condition_implies``). Two ``None`` filters are trivially equivalent; a
    one-sided ``None`` is not.
    """
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return condition_implies(a, b) and condition_implies(b, a)


def _null_extended_before(cte: CTE, target: Join, member: str) -> bool:
    """Whether ``member``'s columns can be NULL-extended in the rows entering
    ``target``: an earlier outer join in this CTE's FROM chain preserved rows
    where ``member`` has no partner. ``member``'s key is NULL on those rows, a
    plain-equality join never matches them, and ``target``'s preservation is
    load-bearing for exactly those rows even though the member's own row
    population fully matches."""
    extended: set[str] = set()
    joined: set[str] = set()
    for j in cte.joins or []:
        if j is target:
            break
        if not isinstance(j, Join):
            continue
        left_names = {p.cte.name for p in j.joinkey_pairs or [] if p.cte is not None}
        right_name = j.right_cte.name if j.right_cte is not None else None
        if j.jointype is JoinType.LEFT_OUTER and right_name:
            extended.add(right_name)
        elif j.jointype is JoinType.RIGHT_OUTER:
            extended |= joined | left_names
        elif j.jointype is JoinType.FULL:
            extended |= joined | left_names
            if right_name:
                extended.add(right_name)
        joined |= left_names
        if right_name:
            joined.add(right_name)
    return member in extended


def _key_nullable(concept: BuildConcept, side_cte: CTE | UnionCTE) -> bool:
    """True when ``side_cte`` can emit NULL for ``concept`` (a ROLLUP/CUBE/
    GROUPING SETS key carries NULL at its subtotal rows)."""
    if not isinstance(side_cte, CTE):
        return False
    keys = _key_addresses(concept)
    nullable_addrs: set[str] = set()
    for nc in side_cte.nullable_concepts:
        nullable_addrs |= _key_addresses(nc)
    return bool(nullable_addrs & keys)


def _identity(address: str) -> str:
    return address


def _unshared_join_padding(pair, right_cte: CTE | UnionCTE) -> bool:
    """A side whose key can be NULL via outer-join padding carries the join
    image of the key (the values its preserved rows happened to match, a
    subset of the value space), so the two sides' key sets only provably
    coincide when the padding shares provenance. Value NULLs (a `?` column, a
    ROLLUP grouping key) do not subset the non-null values and stay with the
    null-safe machinery."""
    padded = False
    for concept, side in ((pair.left, pair.cte), (pair.right, right_cte)):
        if (
            isinstance(side, CTE)
            and _key_nullable(concept, side)
            and not nulls_are_values(concept, side.source)
        ):
            padded = True
    if not padded:
        return False
    keys = _key_addresses(pair.left) | _key_addresses(pair.right)
    left_pad = (
        _padding_sources(pair.cte.source, keys, _identity)
        if isinstance(pair.cte, CTE)
        else set()
    )
    right_pad = (
        _padding_sources(right_cte.source, keys, _identity)
        if isinstance(right_cte, CTE)
        else set()
    )
    return not (left_pad & right_pad)


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
    """The side carries every value of the concept's domain: the superset
    test for directional narrowing. Weaker than ``_complete_distinct``:
    duplicates are allowed (fan-out is a property of the data, not of the
    join type), and a derived concept's domain is the image of its inputs'
    domains, so completeness transfers through BASIC/ROWSET lineage but never
    through a FILTER. Only ``_own_coverage_partial`` stamps veto the claim; a
    row LIMIT anywhere in the chain vetoes unconditionally."""
    if not isinstance(side_cte, CTE):
        return False
    if _row_limited(side_cte):
        return False
    keys = _key_addresses(concept)
    if not _own_coverage_partial(concept, side_cte, graph):
        # Scan evidence is trusted here because every caller of this path is
        # declaration-gated.
        if side_cte.group_to_grain or _authoritative_scan(side_cte):
            grain_addrs = set(side_cte.grain.components) if side_cte.grain else set()
            if grain_addrs & keys:
                return True
        # A non-partial binding on an authoritative scan carries the full
        # value set even off the grain; grain membership only matters for
        # distinctness, which value completeness does not need.
        if _authoritative_scan(side_cte) and any(
            concept.address == c.address for c in side_cte.output_columns
        ):
            return True
    # A pure 1:1 passthrough (no grouping, joins, or condition, one parent)
    # preserves its parent's row set, so completeness carries through the
    # projection rename.
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
        # A rowset translation renames the key with no shared address across
        # the boundary; the wrapper's grain is the renamed parent grain, so
        # grain membership at matching arity carries the parent's grain
        # completeness through the rename.
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


def _side_origins(side_cte: CTE | UnionCTE, group: set[str]) -> set[str]:
    """The origin domain nodes this side carries for a canonical key group:
    the authored addresses of substituted column bindings
    (``BuildColumnAssignment.origin_address``) whose bound concept renders in
    ``group``, collected across the side's source tree and parent chain.
    Per-column stamps discriminate where physical datasource identity cannot
    (one table binding several relation endpoints, shared-base self-joins
    reading distinct columns)."""
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
    """The side marks the key partial: a subset domain declaration (a `~`
    binding, `merge a into ~b`, or a scoped subset join) propagated up the
    CTE chain via ``partial_concepts``."""
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
    directed subset path over declared and structural edges (rowset/filter
    lineage mints the latter). Deliberately ignores incomparable declarations:
    an authored one can be conservatively wrong in one direction, and a proven
    subset path makes the narrowing row-identical (rule B). The sub side keys
    on its own exact address (the two relation endpoints are distinct
    concepts, one per side) while the sup side matches through its
    pseudonym/canonical closure, since rendering may have re-addressed it.

    That closure is not a free stand-in for the sup side: a scoped
    incomparable merge collapses every member of a chained group onto one
    canonical, so the sup side's pseudonyms include its own siblings,
    independent populations the declaration says are incomparable. Proving
    `sub ⊑ sibling` says nothing about `sub ⊑ sup`, and rule-B narrowing on
    it drops rows present on only one side. Siblings are excluded; sup's own
    address stays, so a genuine subset path through the merge still narrows."""
    if sub_concept.address == sup_concept.address:
        return False
    group = graph.join_key_groups().get(graph.canonical(sup_concept.address), set())
    siblings = (group & graph.coalescing_relation_members()) - {sup_concept.address}
    for candidate in sorted(_key_addresses(sup_concept)):
        if candidate == sub_concept.address or candidate in siblings:
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
    lacks. Stamps at declared-relation subset endpoints speak to the relation
    and smear symmetrically across a canonical group, and a stamp on a ROWSET
    handle is the planner's own relation-driven marking, never an authored
    fact. Any other stamp is an authored coverage fact (a `~` binding), so its
    one-sided presence proves the subset direction: the author declared both
    sides' relations to the domain (`~` on the sub, a complete binding on the
    sup, verified by ``_complete_values`` after)."""
    keys = _key_addresses(sub_concept)
    subset_endpoints = graph.subset_sources()
    genuine = {
        p.address
        for p in sub_cte.partial_concepts
        if p.address not in subset_endpoints
        and p.derivation != Derivation.ROWSET
        and _key_addresses(p) & keys
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

    Requires subset evidence on the sub side (a lying declaration is an
    author error; narrowing then drops the violating rows), plus proof the
    superset side carries the key's full domain here: complete values, with
    scan evidence trusted because the author declared the relation, and a
    filter-free chain, since a filter on another column can drop domain
    values asymmetrically.

    The evidence arrives one of two ways: a subset path in the domain graph
    (``_proven_subset_of``, distinct endpoint concepts, one per side), or a
    ``partial_concepts`` stamp on the sub side, where both endpoints name one
    concept and must share a source address. ``graph_proof_only`` restricts
    to the former: rule-B narrowing through an authored incomparable veto
    trusts only a proven path, never the stamp heuristics, because that veto
    collapses two genuinely distinct populations onto one address, exactly
    what stamps cannot see."""
    declared = _proven_subset_of(domain_graph, sub_concept, sup_concept)
    if not declared:
        if graph_proof_only:
            return False
        if _source_address(sub_concept) != _source_address(sup_concept):
            return False
        if not _declared_partial(sub_concept, sub_cte):
            return False
        # Same-address pair: relation-induced partial stamps land symmetrically
        # when several relations share one canonical group, so they cannot
        # say which side is the subset here. A genuine coverage stamp (a `~`
        # binding) present only on the sub side settles the direction;
        # otherwise arbitrate by origin domain nodes: the subset side carries
        # a declared-subset origin of this group that the superset side lacks.
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
    # A ROWSET superset anchor at its own rename boundary is complete by
    # construction (the body filter defines the domain), so the declared
    # subset side fully matches it. An external filter on the rowset output
    # fails the boundary test and falls through below.
    #
    # Gated on a strict directional subset between the two own addresses: a
    # scoped-join merge collapses every anchor-joined key onto one canonical,
    # so two independent rowsets joined to a common anchor land in each
    # other's pseudonym closure and `_proven_subset_of` would falsely read one
    # sibling as the superset of the other. The own-address relation is
    # UNKNOWN for such siblings and SUBSET only toward the genuine anchor.
    if (
        _rowset_definition_boundary(sup_concept, sup_cte)
        and domain_graph.relation(sub_concept.address, sup_concept.address)
        is ResolvedRelation.SUBSET
    ):
        return True
    if not _complete_values(sup_concept, sup_cte, domain_graph):
        return False
    return _accumulate_filter(sup_cte) is None


def _datasource_ids_for_key(cte: CTE, concept: BuildConcept) -> set[str]:
    providers = set(cte.source_map.get(concept.address, ()))
    return {
        datasource.identifier
        for datasource in cte.source.datasources
        if isinstance(datasource, BuildDatasource)
        and datasource.safe_identifier in providers
        and set(datasource.grain.components) & _key_addresses(concept)
    }


def _cte_contains_datasource(cte: CTE | UnionCTE, identifier: str) -> bool:
    return isinstance(cte, CTE) and any(
        isinstance(datasource, BuildDatasource) and datasource.identifier == identifier
        for datasource in cte.source.datasources
    )


def _provider_joins_preserve_rows(
    cte: CTE,
    provider_ids: set[str],
    graph: DomainGraph,
    subset_join_map: dict[str, str],
    scoped_canonical: dict[str, str],
) -> bool:
    def complete_domain_match(
        sub_concept: BuildConcept,
        sub_cte: CTE | UnionCTE,
        sup_concept: BuildConcept,
        sup_cte: CTE | UnionCTE,
    ) -> bool:
        return (
            isinstance(sup_cte, CTE)
            and _source_address(sub_concept) == _source_address(sup_concept)
            and bool(set(sup_cte.grain.components) & _key_addresses(sup_concept))
            and _complete_values(sub_concept, sub_cte, graph)
            and _complete_values(sup_concept, sup_cte, graph)
            and _accumulate_filter(sup_cte) is None
        )

    for join in cte.joins:
        if not isinstance(join, Join) or join.jointype != JoinType.INNER:
            return False
        if not join.joinkey_pairs:
            return False
        if not all(
            (
                _cte_contains_datasource(pair.cte, source)
                and _pair_side_fully_matches(
                    pair.left,
                    pair.cte,
                    pair.right,
                    join.right_cte,
                    graph,
                    subset_join_map,
                    scoped_canonical,
                )
                or _cte_contains_datasource(pair.cte, source)
                and complete_domain_match(
                    pair.left, pair.cte, pair.right, join.right_cte
                )
                or _cte_contains_datasource(join.right_cte, source)
                and _pair_side_fully_matches(
                    pair.right,
                    join.right_cte,
                    pair.left,
                    pair.cte,
                    graph,
                    subset_join_map,
                    scoped_canonical,
                )
                or _cte_contains_datasource(join.right_cte, source)
                and complete_domain_match(
                    pair.right, join.right_cte, pair.left, pair.cte
                )
            )
            for pair in join.joinkey_pairs
            for source in provider_ids
        ):
            return False
    return bool(provider_ids)


def _relative_key_subset(
    sub_concept: BuildConcept,
    sub_cte: CTE | UnionCTE,
    sup_concept: BuildConcept,
    sup_cte: CTE | UnionCTE,
    graph: DomainGraph,
    subset_join_map: dict[str, str],
    scoped_canonical: dict[str, str],
) -> bool:
    """Whether `sub_cte`'s keys are covered by a filtered key provider."""
    if not isinstance(sub_cte, CTE) or not isinstance(sup_cte, CTE):
        return False
    if _source_address(sub_concept) != _source_address(sup_concept):
        return False
    if not set(sup_cte.grain.components) & _key_addresses(sup_concept):
        return False
    sub_filter = _accumulate_filter(sub_cte)
    sup_filter = _accumulate_filter(sup_cte)
    if sup_filter is not None and (
        sub_filter is None or not condition_implies(sub_filter, sup_filter)
    ):
        return False
    provider_ids = _datasource_ids_for_key(sup_cte, sup_concept)
    if not _provider_joins_preserve_rows(
        sup_cte,
        provider_ids,
        graph,
        subset_join_map,
        scoped_canonical,
    ):
        return False
    if not any(_cte_contains_datasource(sub_cte, source) for source in provider_ids):
        return False
    return any(
        isinstance(join, Join)
        and join.jointype == JoinType.INNER
        and any(
            _key_addresses(pair.left) & _key_addresses(sub_concept)
            and _key_addresses(pair.right) & _key_addresses(sup_concept)
            and (
                _cte_contains_datasource(pair.cte, source)
                or _cte_contains_datasource(join.right_cte, source)
            )
            for pair in join.joinkey_pairs or []
            for source in provider_ids
        )
        for join in sub_cte.joins
    )


class UpgradeOuterFromKeySetEquivalence(OptimizationRule):
    """Upgrade FULL/LEFT/RIGHT OUTER to INNER when each join key pair has
    identical conceptual value sets on both sides, or narrow directionally
    when one side is a proven subset (see module docstring).

    Catches twin rollups (one or both sides GROUP BY rollups of a shared
    filtered source, joined back on the rollup key) and sibling aggregations
    whose effective WHERE chains mutually imply. Skips cross-source joins
    (source addresses differ), sides carrying an extra WHERE (filters fail
    mutual implication), and sides without ``group_to_grain`` (cardinality
    unknown).

    Query-scoped FULL/UNION joins (``full_join_keys``) join two independent
    populations with potentially disjoint key sets, and FULL deliberately
    keeps its key complete rather than partial, so the complete-distinct test
    cannot see the disjointness through the canonical collapse. Rule B
    exception: an authored incomparable declaration can be conservatively
    wrong in one direction. When the graph proves a subset direction (a
    structural path into a complete, filter-free superset side), the vetoed
    join still narrows directionally (``graph_proof_only``, never the
    equivalence upgrade or the stamp heuristics); unproven pairs keep the
    veto.

    ``equal_join_keys`` releases that veto for keys whose FULL relation is an
    EQUAL domain declaration (non-partial `merge a into b`,
    docs/subset_union_join_design.md): the canonical collapse then genuinely
    names one value space, so the standard completeness tests apply and the
    join may narrow to INNER. Populated only when
    ``CONFIG.optimizations.narrow_equal_domain_joins`` is on; narrowing
    trusts the declaration, and data violating it loses the violating rows.
    """

    def __init__(
        self,
        domain_graph: DomainGraph | None = None,
        narrow_equal_domain_joins: bool = True,
    ) -> None:
        # The statement's declared-edge domain graph is the one source of
        # truth for authored relations (docs/domain_graph_design.md); the
        # views below are derived from it.
        graph = domain_graph or DomainGraph()
        self.domain_graph = graph
        # Canonical addresses of EQUAL-declared keys (see class docstring).
        self.equal_join_keys = (
            graph.equal_narrowable_keys() if narrow_equal_domain_joins else set()
        )
        # Canonical addresses of query-scoped FULL/UNION keys; joins on these
        # must never upgrade to INNER (FULL's key stays complete, so the
        # partial-driven checks cannot protect it).
        self.full_join_keys = graph.outer_relation_keys() - self.equal_join_keys
        # Subset side (exact, side-specific address) -> superset counterpart
        # for every SUBSET-declared relation; feeds directional narrowing.
        self.subset_join_map = graph.subset_join_map()
        # Full member -> canonical-group-root map for scoped relations; maps
        # a rendered same-address pair back to its relation group.
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
            if join.jointype not in OUTER_JOIN_TYPES:
                continue
            if not join.joinkey_pairs:
                continue
            right_cte = join.right_cte
            if self.full_join_keys and any(
                _key_addresses(pair.left) & self.full_join_keys
                or _key_addresses(pair.right) & self.full_join_keys
                for pair in join.joinkey_pairs
            ):
                # Rule B: the veto blocks the equivalence upgrade and the
                # stamp heuristics, but a graph-proven subset direction still
                # narrows directionally; unproven pairs keep the veto.
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
                # the author declared equal-domain; see _authoritative_scan.
                allow_scan_evidence=equal_declared,
            ):
                return True
            # An EQUAL declaration collapses two concepts into one value
            # space, so both sides carrying every value plus equivalent
            # filters is exactly the no-unmatched-rows proof.
            if not equal_declared:
                return False
            graph = self.domain_graph
            return (
                _complete_values(pair.left, pair.cte, graph)
                and _complete_values(pair.right, right_cte, graph)
                and _filters_equivalent(
                    _accumulate_filter(pair.cte),
                    _accumulate_filter(right_cte),
                )
            )

        if not all(pair_equal(pair) for pair in join.joinkey_pairs):
            return False
        # A chain member null-extended by an earlier outer join carries rows
        # where its key is absent; equality never matches them, so this join's
        # preservation is load-bearing regardless of value-set equivalence.
        if any(
            _null_extended_before(cte, join, pair.cte.name)
            for pair in join.joinkey_pairs
        ):
            return False
        # A key nullable on a side but joined with plain ``=`` carries NULL
        # rows the equality never matches (a ROLLUP subtotal key); INNER
        # would silently drop them. A null-safe pair matches NULLs and is
        # safe. For an EQUAL-declared key both sides name one value space, so
        # the pair is made null-safe rather than refusing the upgrade.
        for pair in join.joinkey_pairs:
            # Null-safety pairs the NULL groups but says nothing about the
            # non-null values: a join-padded side carries a key image that
            # subsets the value space, so unless the padding shares
            # provenance the equivalence claim is unsound. An EQUAL
            # declaration names one value space and overrides.
            if not self._pair_equal_declared(pair) and _unshared_join_padding(
                pair, right_cte
            ):
                return False
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
        """Subset-driven narrowing: preservation of a side that provably has no
        unmatched rows is a no-op, so drop it.

        A side needs preservation only for (a) key values missing from the
        other side, none when it is subset-declared against a proven-complete
        superset side (``_pair_side_fully_matches``), or (b) NULL-key rows
        with no null-safe partner, none when the pair is null-safe or the
        side proves non-null. FULL narrows to the directional join preserving
        the superset side; a directional join whose preserved side fully
        matches narrows to INNER.

        The sub side's full-match claim is about its own rows; when an
        earlier outer join in this CTE's FROM chain null-extended it, the
        chain carries rows where the sub side is absent, and the target
        join's preservation is load-bearing for exactly those rows
        (``_null_extended_before``)."""
        assert join.joinkey_pairs

        def relative_right(pair) -> bool:
            return not graph_proof_only and _relative_key_subset(
                pair.right,
                right_cte,
                pair.left,
                pair.cte,
                self.domain_graph,
                self.subset_join_map,
                self.scoped_canonical,
            )

        def relative_left(pair) -> bool:
            return not graph_proof_only and _relative_key_subset(
                pair.left,
                pair.cte,
                pair.right,
                right_cte,
                self.domain_graph,
                self.subset_join_map,
                self.scoped_canonical,
            )

        def right_matches_left() -> bool:
            return all(
                (
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
                    or relative_right(pair)
                )
                and (
                    pair.is_nullable
                    or not _key_nullable(pair.right, right_cte)
                    or relative_right(pair)
                    and not _key_nullable(pair.left, pair.cte)
                )
                for pair in join.joinkey_pairs or []
            )

        def left_matches_right() -> bool:
            return all(
                (
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
                    or relative_left(pair)
                )
                and (
                    pair.is_nullable
                    or not _key_nullable(pair.left, pair.cte)
                    or relative_left(pair)
                    and not _key_nullable(pair.right, right_cte)
                )
                and not _null_extended_before(cte, join, pair.cte.name)
                for pair in join.joinkey_pairs or []
            )

        original = join.jointype
        target: JoinType | None = None
        if join.jointype == JoinType.FULL:
            if right_matches_left():
                target = JoinType.LEFT_OUTER
            elif left_matches_right():
                target = JoinType.RIGHT_OUTER
        elif (
            join.jointype == JoinType.LEFT_OUTER
            and left_matches_right()
            or join.jointype == JoinType.RIGHT_OUTER
            and right_matches_left()
        ):
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
