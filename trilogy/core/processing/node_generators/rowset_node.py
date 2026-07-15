"""Rowset node generation.

A rowset is a SCOPE: its outputs are new addresses wrapping a closed body
subplan, while the scoped-join machinery (canonical collapse, pseudonyms, the
domain graph) operates on one flat address space. Anything derived from a
rowset's outputs — a cast join key, a per-side presence probe — therefore has
a well-defined value but no discovery-addressable place: discovery's only
primitive is "source this address set", and "compute X on THIS side, before
the merge" is not an address set. Two rules keep that seam sound; every
join-expression special case in this file is an instance of one of them:

1. OBLIGATION — a rowset node materializes, from its own outputs, every
   concept the outer plan needs from ITS side of a declared relation: its
   member of a derived join-key group, and any presence probe over its
   outputs (`_local_exposure_obligations`, discharged unconditionally in
   `gen_rowset_node`). The completion merge then relates sides by pseudonym
   with no re-sourcing.
2. EXCLUSION — no concept whose only computation path lies inside this node
   may be requested from discovery during its enrichment
   (`_externally_unsourceable`): asking can only re-enter this rowset
   (the q02/q59/q97 recursion family).

The two paths that RELATE this scope to others — enrichment across a derived
join key (`_enrich_via_derived_join_key`) and cross-rowset WHERE handling
(`_apply_cross_rowset_where`) — source the OTHER side, never back through
this one.
"""

from typing import List

from trilogy.constants import logger
from trilogy.core.enums import Derivation, JoinType
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.author import MultiSelectLineage, SelectLineage
from trilogy.core.models.build import (
    BuildConcept,
    BuildRowsetItem,
    BuildRowsetLineage,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.common import unsatisfied_optionals
from trilogy.core.processing.node_generators.presence_probe import (
    is_presence_probe as _is_presence_probe,
)
from trilogy.core.processing.node_generators.presence_probe import (
    member_binding_datasources,
    probe_member_address,
    retain_presence_probes,
)
from trilogy.core.processing.nodes import (
    History,
    MergeNode,
    RowsetNode,
    SelectNode,
    StrategyNode,
)
from trilogy.core.processing.utility import concept_to_relevant_joins, padding
from trilogy.utility import unique

LOGGER_PREFIX = "[GEN_ROWSET_NODE]"


def _producible_addresses(
    node: StrategyNode, *, deep: bool, include_pseudonyms: bool
) -> set[str]:
    """Addresses `node` can produce -- the single reachability primitive behind
    the rowset guards.

    Every dangling-reference bug in this file is one shape: a node advertises (or
    a predicate references) an address no subtree can actually source, so the
    renderer emits an ``INVALID_REFERENCE_BUG`` CTE. The renderer's strict-mode
    guard is the backstop; these gen-time checks bail *early* (fall through to an
    alternative plan) instead of hard-failing at render.

    ``deep`` walks the whole parent subtree (vs the node's own outputs only).
    ``include_pseudonyms`` also counts addresses a concept was collapsed onto -- a
    pseudonym is renderable even though it is not the concept's own address."""
    acc: set[str] = set()
    seen: set[int] = set()
    stack = [node]
    while stack:
        current = stack.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        for c in current.output_concepts:
            acc.add(c.address)
            if include_pseudonyms:
                acc.update(c.pseudonyms)
        if deep:
            stack.extend(current.parents)
    return acc


def _condition_operands_resolved(
    conditions: BuildWhereClause, node: StrategyNode
) -> bool:
    """True when `node` can produce every row-operand the predicate references
    (directly or via a pseudonym). A cross-rowset merge that drops an operand
    must not have the predicate applied -- the renderer would emit a dangling
    INVALID_REFERENCE_BUG CTE for the missing column."""
    available = _producible_addresses(node, deep=False, include_pseudonyms=True)
    return all(r.address in available for r in conditions.row_arguments)


def _scoped_joins_for_rowset(
    scoped_joins: list[tuple[str, str, JoinType]],
    derived_concepts: list[str],
) -> list[tuple[str, str, JoinType]]:
    """A query-scoped `join`/`merge` relates the rowset's *output* to an outer
    concept; it must not be applied inside the rowset's own (independent-scope)
    build. Such a join collapses the outer concept onto the rowset output via
    the merge map/pseudonym — so if the rowset's WHERE references that outer
    concept (e.g. a membership existence feeder), sourcing the feeder redirects
    back to the rowset's own output and the rowset depends on itself (infinite
    recursion). Drop any join referencing a concept this rowset derives."""
    derived = set(derived_concepts)
    return [
        (s, t, jt)
        for (s, t, jt) in scoped_joins
        if s not in derived and t not in derived
    ]


def _pseudonym_bridge_keys(
    outputs: List[BuildConcept], environment: BuildEnvironment
) -> list[tuple[BuildConcept, BuildConcept]]:
    """Pair each rowset-derived FK output with the non-rowset (dim) key it was
    merged/joined onto. A query-scoped `join`/`merge` collapses the FK's address
    onto the dim key, leaving the FK only as a non-rowset pseudonym; offering
    that canonical key lets the rowset be enriched from the dim's datasource."""
    pairs: list[tuple[BuildConcept, BuildConcept]] = []
    for fk in outputs:
        if fk.derivation != Derivation.ROWSET:
            continue
        for address in fk.pseudonyms:
            canonical = environment.concepts.get(address)
            if canonical is not None and canonical.derivation != Derivation.ROWSET:
                pairs.append((fk, canonical))
    return pairs


def _producible_derived_join_keys(
    node: StrategyNode, environment: BuildEnvironment
) -> list[tuple[BuildConcept, str]]:
    """Derived-expression scoped-join keys this rowset node can MATERIALIZE off
    its own outputs, paired with the other side's key address.

    A scoped `join a.grp + 1 = b.grp` lowers the left key to an anonymous BASIC
    concept (`a.grp + 1`) whose pseudonym is the other side's key (`b.grp`). That
    bridge is not a rowset output, so `_pseudonym_bridge_keys` can't see it; and
    sourcing it through discovery would re-enter this same rowset (its parent is
    the rowset's own output) and recurse. But the rowset already produces the
    key's inputs, so it can compute the key locally — exposing a join column the
    merge relates to the other rowset by pseudonym, with no re-sourcing. Returns
    ``(derived key, other-side key address)`` for each such key.

    The enrichment sources the OTHER-side key to pull the other rowset, so that
    key must resolve to a DIFFERENT rowset. An INNER/global merge collapses the
    derived key onto the other side, leaving the other side's own identity intact.
    A LEFT scoped join is the opposite: it collapses the optional side's key ONTO
    this anchor's derived key (substitution), so the "other side" now derives from
    THIS rowset's own output — sourcing it re-enters this rowset and recurses with
    no real relation to the other one. Skip any pairing whose other side is
    producible here; it falls through to the standard path / a clean disconnect."""
    producible = _producible_addresses(node, deep=False, include_pseudonyms=True)
    scoped_keys = (
        environment.domain_graph.outer_relation_keys()
        | environment.domain_graph.left_anchor_keys()
    )
    seen: set[str] = set()
    result: list[tuple[BuildConcept, str]] = []
    for key_addr in scoped_keys:
        key_concept = environment.concepts.get(key_addr)
        if key_concept is None:
            continue
        other_inputs = {a.address for a in key_concept.concept_arguments}
        if other_inputs and other_inputs <= producible:
            # The other side derives from this rowset (LEFT-anchor substitution);
            # sourcing it would re-enter this rowset. Decline.
            continue
        for pseudo in key_concept.pseudonyms:
            derived = environment.concepts.get(pseudo)
            if (
                derived is None
                or derived.derivation != Derivation.BASIC
                or derived.address in seen
            ):
                continue
            inputs = {a.address for a in derived.concept_arguments}
            if inputs and inputs <= producible:
                seen.add(derived.address)
                result.append((derived, key_addr))
    return result


def _rowset_scope_routed(concept: BuildConcept) -> bool:
    """Whether `concept`'s value derives (transitively) from some rowset's
    outputs, i.e. whether a cross-rowset relation can resolve it. A base-model
    concept (e.g. an outer WHERE arg like `s.date.year`) is NOT routed: no
    rowset scope resolves it, so it must never be bundled into a request aimed
    at another rowset — that side would delegate it right back through this
    one. Base residue belongs to the outer loop / the standard bridge path."""
    if concept.derivation == Derivation.ROWSET:
        return True
    stack = list(concept.concept_arguments)
    seen: set[str] = set()
    while stack:
        current = stack.pop()
        if current.address in seen:
            continue
        seen.add(current.address)
        if current.derivation == Derivation.ROWSET:
            return True
        stack.extend(current.concept_arguments)
    return False


def _rowset_scopes(concept: BuildConcept) -> set[str]:
    """Names of the rowsets `concept`'s value (transitively) derives from."""
    scopes: set[str] = set()
    stack = [concept]
    seen: set[str] = set()
    while stack:
        current = stack.pop()
        if current.address in seen:
            continue
        seen.add(current.address)
        if isinstance(current.lineage, BuildRowsetItem):
            scopes.add(current.lineage.rowset.name)
            continue
        stack.extend(current.concept_arguments)
    return scopes


def _local_exposure_obligations(
    node: StrategyNode, environment: BuildEnvironment
) -> list[BuildConcept]:
    """OBLIGATION rule: every concept this rowset node must materialize from
    its OWN outputs for the outer plan to relate or filter its side of a
    declared relation. Purely local computations (BASICs over this node's own
    outputs) — nothing is sourced. Two kinds:

    - Derived-expression join-key members (`next_year.wk - 52`, `cast(k)`) of
      declared outer relations. Coalescing (`full`/`union`) sides meet only at
      the completion merge, which infers joins from the sides' outputs alone —
      an unmaterialized member makes the authored pairing invisible and the
      join silently drops it. (`left`/`subset` resolve via the enrichment
      machinery — `_producible_derived_join_keys` — instead.)
    - Presence probes whose member is an own output. A probe must compute
      BEFORE the coalescing merge (post-merge the member's source is the fused
      group coalesce, never NULL) and matches by exact address, never a
      pseudonym — the group-mate's node carries the member as a pseudonym, and
      computing the probe there would read the wrong side."""
    producible = _producible_addresses(node, deep=False, include_pseudonyms=True)
    own = {c.address for c in node.output_concepts}
    out: list[BuildConcept] = []
    seen: set[str] = set()

    def consider(candidate: BuildConcept | None, within: set[str]) -> None:
        if (
            candidate is None
            or candidate.derivation != Derivation.BASIC
            or candidate.address in seen
            or candidate.address in own
        ):
            return
        inputs = {a.address for a in candidate.concept_arguments}
        if inputs and inputs <= within:
            seen.add(candidate.address)
            out.append(candidate)

    for key_addr in environment.domain_graph.outer_relation_keys():
        key_concept = environment.concepts.get(key_addr)
        if key_concept is None:
            continue
        for candidate_addr in {key_addr, *key_concept.pseudonyms}:
            consider(environment.concepts.get(candidate_addr), producible)
    for concept in environment.concepts.values():
        if _is_presence_probe(concept.address):
            # A probe whose member is datasource-bound (ROOT) belongs to
            # gen_presence_probe_node's pinned scan. Its argument is the
            # canonicalized group key, which an anchor rowset may well output
            # — claiming it here would compute the probe on the wrong side.
            member = probe_member_address(concept.address, environment)
            if member is not None and member_binding_datasources(member, environment):
                continue
            consider(concept, own)
    return out


def _externally_unsourceable(
    x: BuildConcept, coalescing_members: dict[str, set[str]], producible: set[str]
) -> bool:
    """EXCLUSION rule: a concept discovery must never be asked to source on
    behalf of this node. Two tests for one recursion family:

    - registry: a materialized coalescing-join key member (`next_year.wk -
      52`) is a cross-rowset merge key, not an enrichment bridge — each side
      materializes its OWN member and the merge pairs them.
    - structure: any BASIC computed purely from this node's own producible
      addresses (materialized join keys, presence probes) has no external
      source, so sourcing it can only route back through this same rowset."""
    if x.address in coalescing_members:
        return True
    if x.derivation != Derivation.BASIC:
        return False
    inputs = {a.address for a in x.concept_arguments}
    return bool(inputs) and inputs <= producible


def _build_rowset_body_node(
    concept: BuildConcept,
    lineage: BuildRowsetItem,
    select: SelectLineage | MultiSelectLineage,
    history: History,
    depth: int,
) -> StrategyNode:
    """Build (or reuse a cached) node for the rowset body select.

    Cache the parent select-node by rowset name: when an outer SELECT mixes an
    aliased rowset concept (BASIC derivation) with bare rowset references, the
    search loop visits gen_rowset_node twice (once for the rowset priority, once
    via gen_basic_node's parent re-sourcing). The select's structure is
    deterministic per rowset, so memoize the pre-mutation node and hand back a
    copy on cache hit."""
    from trilogy.core.query_processor import get_query_node

    rowset: BuildRowsetLineage = lineage.rowset
    cached = history.rowset_history.get(rowset.name)
    if cached is not None:
        return cached.copy()

    # The rowset body's OWN query-scoped joins live on its SelectLineage; feed
    # them to the inner build (like the union-arm path) so the body applies them
    # — otherwise its datasources come back disconnected. Combine with the outer
    # query's joins (filtered to avoid self-referential recursion).
    own_scoped = list(select.scoped_joins) if isinstance(select, SelectLineage) else []
    scoped_joins = (
        _scoped_joins_for_rowset(
            history.build_caches.scoped_joins, lineage.rowset.derived_concepts
        )
        + own_scoped
    )
    node = get_query_node(
        history.base_environment,
        select,
        history=None,
        scoped_joins=scoped_joins or None,
    )
    if not node:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate parent rowset node for {concept}"
        )
        raise UnresolvableQueryException(
            f"Cannot generate parent select for concept {concept} in rowset {rowset.name}; ensure the rowset is a valid statement."
        )
    history.rowset_history[rowset.name] = node.copy()
    return node


def _collect_advertised_outputs(
    lineage: BuildRowsetItem,
    select: SelectLineage | MultiSelectLineage,
    environment: BuildEnvironment,
    base_node: StrategyNode,
    local_optional: List[BuildConcept],
) -> tuple[list[BuildConcept], list[BuildConcept], list[BuildConcept]]:
    """Resolve the rowset's advertised outputs from its derived concepts.

    Returns ``(rowset_relevant, additional_relevant, scoped_partial)``: the
    rowset's own outputs, any local-optional enrichments already in the body, and
    the subset that source from scoped-partial datasources."""
    enrichment = {x.address for x in local_optional}
    concept_pool = list(environment.concepts.values()) + list(
        environment.alias_origin_lookup.values()
    )
    rowset_outputs = [
        x.address for x in concept_pool if x.address in lineage.rowset.derived_concepts
    ]
    rowset_relevant: list[BuildConcept] = [
        v
        for v in concept_pool
        if v.address in rowset_outputs and v.address not in base_node.hidden_concepts
    ]

    present_map: dict[str, BuildConcept] = {v.address: v for v in rowset_relevant}
    scoped_partial: list[BuildConcept] = []
    for derived_address in lineage.rowset.derived_concepts:
        if derived_address in present_map:
            advertised = present_map[derived_address]
        else:
            collapsed = environment.concepts.get(derived_address)
            if collapsed is None:
                continue
            advertised = collapsed
            if collapsed.address not in present_map:
                rowset_relevant.append(collapsed)
                present_map[collapsed.address] = collapsed
        if derived_address in environment.domain_graph.subset_sources():
            scoped_partial.append(advertised)

    additional_relevant = [
        environment.concepts[x.address]
        for x in select.output_components
        if x.address in enrichment
    ]
    return rowset_relevant, additional_relevant, scoped_partial


def _unhide_referenced_body_locals(
    base_node: StrategyNode,
    advertised: list[BuildConcept],
) -> None:
    """Un-hide body locals that back a referenced rowset concept.

    A rowset output marked hidden (`--`) in the body select still backs a
    publicly-referenced rowset concept (its `BuildRowsetItem.content`). Leaving
    that body-local column hidden means the body QueryDatasource omits it from
    its source map, so the wrapper can't source the rowset output and re-derives
    it from lineage against the already-grouped parent (raw operands gone) →
    INVALID_REFERENCE_BUG. Un-hide any body local that backs a relevant rowset
    concept so it materializes in the body's grouping CTE.

    A coalescing scoped join (`union`/`full`/`subset`) in the body collapses the
    referenced content (`sa.item_sk`) onto ONE canonical output (`ci.item_sk`),
    leaving the content only as that canonical's *pseudonym*; the canonical is
    itself hidden when only the collapsed side was projected. Match on pseudonyms
    too so the body still projects the coalesced key column — otherwise a
    downstream `rowset.<collapsed key>` reference has no source (q64)."""
    referenced_body_locals = {
        item.lineage.content.address
        for item in advertised
        if isinstance(item.lineage, BuildRowsetItem)
    }
    to_unhide = {
        c.address
        for c in base_node.output_concepts
        if c.address in base_node.hidden_concepts
        and (
            c.address in referenced_body_locals
            or (set(c.pseudonyms) & referenced_body_locals)
        )
    }
    if to_unhide:
        base_node.hidden_concepts = base_node.hidden_concepts - to_unhide
        base_node.rebuild_cache()


def _expose_coalesced_key_contents(
    node: RowsetNode,
    base_node: StrategyNode,
    advertised: list[BuildConcept],
) -> None:
    """Expose the authored source address of a coalesced join key downstream.

    A coalescing scoped join (`full`/`subset`/`union`) collapses a join-key group
    (`a.aid = b.bid`) onto ONE canonical body concept (`b.bid`), leaving each
    authored side (`a.aid`) only as a *pseudonym* of that canonical. A rowset
    output projecting `a.aid` carries `a.aid` as its `BuildRowsetItem.content`,
    but the body materializes it solely under the canonical address — so a
    downstream reference to `rs.a.aid` renders its content `a.aid`, finds no
    source-map entry for it, and the renderer emits a Missing-source sentinel.

    Add each such content address as a HIDDEN output so `resolve_concept_map`
    sources it off the body's canonical column via that pseudonym (mirroring the
    single-statement path, where the authored side is itself the projected
    output). `left` joins don't collapse, so their content is a direct body
    output and this is a no-op."""
    base_available: set[str] = set()
    base_pseudonyms: set[str] = set()
    for c in base_node.output_concepts:
        if c.address in base_node.hidden_concepts:
            continue
        base_available.add(c.address)
        base_pseudonyms.update(c.pseudonyms)
    extra: list[BuildConcept] = []
    seen: set[str] = set()
    for item in advertised:
        if not isinstance(item.lineage, BuildRowsetItem):
            continue
        content = item.lineage.content
        if (
            content.address not in base_available
            and content.address in base_pseudonyms
            and content.address not in seen
        ):
            extra.append(content)
            seen.add(content.address)
    if extra:
        node.add_output_concepts(extra, rebuild=False, unhide=False)
        node.hide_output_concepts(extra, rebuild=False)


def _interpose_limit_node(
    base_node: StrategyNode,
    select: SelectLineage | MultiSelectLineage,
    environment: BuildEnvironment,
    depth: int,
) -> StrategyNode:
    """Materialize the body's `limit` (with its ORDER BY) as a dedicated
    passthrough node BETWEEN the body and the translation wrapper.

    The limit must not live on the translation node itself: discovery applies
    outer WHEREs onto that node (they would render pre-limit, changing which
    rows fill the limit), and when the outer statement reuses it as the query
    root its ordering is overwritten by the statement's and the root renders
    without a CTE-level limit. A dedicated node keeps LIMIT+ORDER BY in their
    own CTE; everything downstream is post-limit by construction, and the
    optimizer treats the limited CTE as an opaque boundary."""
    if select.limit is None:
        return base_node
    passthrough = [
        x
        for x in base_node.output_concepts
        if x.address not in base_node.hidden_concepts
    ]
    limit_node = SelectNode(
        input_concepts=passthrough,
        output_concepts=passthrough,
        environment=environment,
        parents=[base_node],
        depth=depth,
        partial_concepts=list(base_node.partial_concepts),
        nullable_concepts=list(base_node.nullable_concepts),
    )
    limit_node.limit = select.limit
    # the ORDER BY the limit selects under was built onto the body root by
    # get_query_node; hoist it here so both render in one SELECT (an inner
    # ORDER BY without a limit carries no semantics and just costs a sort)
    limit_node.ordering = base_node.ordering
    base_node.ordering = None
    base_node.rebuild_cache()
    limit_node.rebuild_cache()
    return limit_node


def _build_translation_node(
    base_node: StrategyNode,
    rowset_relevant: list[BuildConcept],
    additional_relevant: list[BuildConcept],
    scoped_partial: list[BuildConcept],
    select: SelectLineage | MultiSelectLineage,
    environment: BuildEnvironment,
    depth: int,
) -> RowsetNode:
    """Wrap the body node in a translation RowsetNode rather than mutating its
    outputs. The body materializes the rowset-local concepts (`local._rs_*`)
    against its own scoped-join-collapsed env; keeping it as a parent (with those
    locals as this node's inputs) preserves their source mapping, so `rs.*`
    resolves across the query boundary — in particular a collapsed join key whose
    authored source (e.g. `a.aid`) only exists inside the body as the join
    canonical (`b.bid`)."""
    # nullability propagates by ADDRESS between nodes, but a rowset output is a
    # new address wrapping its body content — map through the BuildRowsetItem
    # content (and pseudonyms) so a `?` column's nullability survives the
    # rowset boundary (else a NULL rowset join key stops matching null-safely)
    base_nullable: set[str] = set()
    for c in base_node.nullable_concepts:
        base_nullable.add(c.address)
        base_nullable.update(c.pseudonyms)
    nullable = [
        x
        for x in rowset_relevant + additional_relevant
        if x.address in base_nullable
        or (set(x.pseudonyms) & base_nullable)
        or (
            isinstance(x.lineage, BuildRowsetItem)
            and (
                x.lineage.content.address in base_nullable
                # the content is typically a body-local alias whose pseudonyms
                # carry the authored source address (`local._ra_k` ~ `a.l_key`)
                or (set(x.lineage.content.pseudonyms) & base_nullable)
            )
        )
    ]
    source_node = _interpose_limit_node(base_node, select, environment, depth)
    node = RowsetNode(
        input_concepts=[
            x
            for x in base_node.output_concepts
            if x.address not in base_node.hidden_concepts
        ],
        output_concepts=rowset_relevant + additional_relevant,
        environment=environment,
        parents=[source_node],
        depth=depth,
        partial_concepts=list(base_node.partial_concepts),
        nullable_concepts=nullable,
    )
    # The body WHERE already constrains this scope's rows; advertise it so an
    # outer condition the body implies (q44's redundant `where store.id = 1`
    # over rowsets filtered to store 1) counts as applied instead of demanding
    # an impossible re-application. The independent-scope exemption for
    # UNRELATED outer conditions is untouched.
    node.preexisting_conditions = base_node.preexisting_conditions
    if select.where_clause:
        for item in additional_relevant:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} adding {item} to partial concepts"
            )
            node.partial_concepts.append(item)

    existing_partial = {c.address for c in node.partial_concepts}
    for item in scoped_partial:
        if item.address not in existing_partial:
            node.partial_concepts.append(item)
            existing_partial.add(item.address)

    _expose_coalesced_key_contents(
        node, base_node, rowset_relevant + additional_relevant
    )

    node.rebuild_cache()
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} final output is {[x.address for x in node.output_concepts]} with grain {node.grain}"
    )
    return node


def _apply_cross_rowset_where(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History,
    conditions: BuildWhereClause,
    node: StrategyNode,
) -> StrategyNode | None:
    """Apply a WHERE that compares this rowset's outputs against *other*
    scoped-joined rowsets (q11/q23 period/channel comparisons).

    Those operands aren't in this rowset's outputs; source them alongside this
    rowset's output as one scoped-join merge and apply the predicate to that
    merge. Returning the bare node would silently drop the filter (or strand it
    as an unsatisfiable condition upstream). Returns the merged node, or None to
    fall through to normal enrichment."""
    have = {x.address for x in node.output_concepts}
    condition_targets = [
        environment.concepts[r.address]
        for r in conditions.row_arguments
        if r.address not in have and r.address in environment.concepts
    ]
    # Only intercept when an operand lives in ANOTHER scoped rowset (the q11/q23
    # cross-rowset comparison): those operands can't be pushed down, so we merge
    # the rowsets and apply the predicate post-join. A plain base concept (e.g.
    # an outer `yr=1999` filter feeding an outer aggregate) must instead flow
    # through the normal enrich path so the condition is pushed down into the
    # scan — applying it here would compute the aggregate unfiltered and filter
    # too late. A presence probe is a cross-rowset operand by construction: it
    # wraps another rowset's coalescing join key member and must be evaluated
    # on that side pre-merge, filtered post-merge — the generic path would
    # silently drop it (its rowset node has no joinable non-key output).
    if not any(
        t.derivation == Derivation.ROWSET or _is_presence_probe(t.address)
        for t in condition_targets
    ):
        return None
    # Source the merge against EVERY concept the predicate references, not just
    # the operands missing from this rowset (`condition_targets`). An operand
    # that lives in *this* rowset (e.g. a measure compared against another
    # rowset's measure) is in `node` but NOT in the fresh merge -- the merge is
    # sourced anew, it doesn't inherit `node`'s outputs. Omit it and the applied
    # predicate references a column the merge never produced -> dangling
    # INVALID_REFERENCE_BUG CTE (q64).
    merge_inputs = unique(
        [concept] + local_optional + list(conditions.row_arguments),
        "address",
    )
    merged = source_concepts(
        mandatory_list=merge_inputs,
        environment=environment,
        g=g,
        depth=depth + 1,
        conditions=None,
        history=history,
    )
    # Only apply the predicate if the merge actually produced every operand;
    # otherwise fall through rather than emit a dangling CTE.
    if not (merged and _condition_operands_resolved(conditions, merged)):
        return None
    merged.add_condition(conditions.conditional)
    # A membership predicate (`x in <set>`) needs its existence set sourced as a
    # parent here too -- otherwise the subselect renders against a dangling CTE
    # (INVALID_REFERENCE_BUG). The normal completion-merge path appends this;
    # this cross-rowset branch short-circuits it, so mirror it explicitly.
    if conditions.existence_arguments:
        from trilogy.core.processing.concept_strategies_v3 import (
            append_existence_check,
        )

        append_existence_check(merged, environment, g, conditions, history)
    merged.set_preexisting_conditions(conditions.conditional)
    return merged


def _enrich_via_derived_join_key(
    derived_keys: List[tuple[BuildConcept, str]],
    enrich_remaining: List[BuildConcept],
    local_optional: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History,
    conditions: BuildWhereClause | None,
    node: RowsetNode,
) -> StrategyNode | None:
    """Enrich a rowset across a derived-expression scoped join.

    Materializes each derived join key (`a.grp + 1`) onto this rowset node from
    its own outputs, then sources the still-missing optionals — plus the other
    side's key, so the merge has a real column to join — which pulls the OTHER
    scoped-joined rowset, and merges. The merge relates the two over the key's
    pseudonym (`a.grp + 1` ~ `b.grp`); the other rowset is never re-sourced
    through this one (which would recurse).

    Returns None (fall through to the standard path / clean disconnect) when the
    other side cannot EXPOSE the pseudonym key — e.g. the optional is a filtered
    aggregate grouped by THIS rowset's key, which collapses the other side's join
    column away. Merging then would silently cross-join (`1=1`), so decline and
    let the query fail cleanly rather than return wrong rows."""
    other_keys = [
        environment.concepts[other]
        for _, other in derived_keys
        if other in environment.concepts
    ]
    # Plain-equality co-keys authored in the SAME cross-rowset join (`a.store =
    # b.store and a.period + 10 = b.period`). The merge relates the two rowsets
    # by the derived key's pseudonym only; without also materializing the
    # equality co-keys on the other side, get_node_joins infers a join on the
    # derived key alone and the equality key silently drops -> cross-key fan-out.
    # Source them so both sides expose the co-key (shared canonical address) and
    # the inferred join carries every authored key.
    scoped_keys = (
        environment.domain_graph.outer_relation_keys()
        | environment.domain_graph.left_anchor_keys()
    )
    derived_related = (
        {other for _, other in derived_keys}
        | {key.address for key, _ in derived_keys}
        | {p for key, _ in derived_keys for p in key.pseudonyms}
    )
    co_keys = [
        environment.concepts[a]
        for a in scoped_keys - derived_related
        if a in environment.concepts
    ]
    co_key_addresses = {c.address for c in co_keys} | {
        p for c in co_keys for p in c.pseudonyms
    }
    enrich_node = source_concepts(
        mandatory_list=unique(enrich_remaining + other_keys + co_keys, "address"),
        environment=environment,
        g=g,
        depth=depth + 1,
        conditions=conditions,
        history=history,
    )
    if not enrich_node:
        return None
    # Keep the anchor-side co-key column visible so the inferred join can pair it
    # against the other side (its address shares a canonical with the other side's
    # co-key via the scoped-join pseudonym).
    for x in list(node.output_concepts):
        if x.address in node.hidden_concepts and (
            x.address in co_key_addresses or (set(x.pseudonyms) & co_key_addresses)
        ):
            node.unhide_output_concepts([x])
    exposed: set[str] = set()
    for x in enrich_node.output_concepts:
        if x.address in enrich_node.hidden_concepts:
            continue
        exposed.add(x.address)
        exposed |= set(x.pseudonyms)
    bindable = {other for _, other in derived_keys} | {
        p for key, _ in derived_keys for p in key.pseudonyms
    }
    if not (exposed & bindable):
        return None
    for key, _ in derived_keys:
        node.add_output_concept(key)
    node.rebuild_cache()
    non_hidden = [
        x for x in node.output_concepts if x.address not in node.hidden_concepts
    ]
    non_hidden_enrich = [
        x
        for x in enrich_node.output_concepts
        if x.address not in enrich_node.hidden_concepts
    ]
    return MergeNode(
        input_concepts=unique(non_hidden + non_hidden_enrich, "address"),
        output_concepts=unique(non_hidden + local_optional, "address"),
        environment=environment,
        depth=depth,
        parents=[node, enrich_node],
        preexisting_conditions=conditions.conditional if conditions else None,
    )


def _enrich_via_group_mate_keys(
    enrich_remaining: List[BuildConcept],
    local_optional: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History,
    conditions: BuildWhereClause | None,
    node: RowsetNode,
    coalescing_members: dict[str, set[str]],
    producible: set[str],
) -> StrategyNode | None:
    """Enrich a rowset across a coalescing scoped join whose key groups expose
    no non-rowset bridge canonical (rowset <-> rowset `union`/`full` join, q59).

    Each side of such a join materializes its OWN key-group member and the
    merge pairs the group over the shared canonical, so there is no external
    bridge column to request — the columns to source are the OTHER side's
    members themselves. Sourcing them pulls the other scope directly and never
    routes back through this rowset; a mate computed from this node's own
    outputs (LEFT-anchor substitution) would, and is declined. Returns None to
    fall through to the bare node when a group we expose has no externally
    sourceable mate or the other side cannot expose one — merging then would
    drop an authored key and fan out."""
    node_outputs = {c.address for c in node.output_concepts}
    # Only request mates on the sides this enrichment actually sources: a mate
    # from an UNrequested side re-enters that rowset's own generation, whose
    # symmetric enrichment routes back through this one — unbounded on a
    # three-way coalescing group (a=b=c). The unrequested side pairs with the
    # group at the level that does source it.
    needed_scopes: set[str] = set()
    for c in enrich_remaining:
        needed_scopes |= _rowset_scopes(c)
    mate_groups: list[set[str]] = []
    mates: list[BuildConcept] = []
    for out_c in node.output_concepts:
        group_mates = coalescing_members.get(out_c.address)
        if not group_mates:
            continue
        external: list[BuildConcept] = []
        for mate_addr in sorted(group_mates):
            if mate_addr in node_outputs:
                continue
            mate = environment.concepts.get(mate_addr)
            if mate is None:
                continue
            mate_inputs = {a.address for a in mate.concept_arguments}
            if mate_inputs and mate_inputs <= producible:
                continue
            external.append(mate)
        if not external:
            return None
        needed = [
            m
            for m in external
            if not (scopes := _rowset_scopes(m)) or scopes & needed_scopes
        ]
        if not needed:
            continue
        mate_groups.append(
            {m.address for m in needed} | {p for m in needed for p in m.pseudonyms}
        )
        mates.extend(needed)
    if not mates:
        return None
    # A presence probe over a mate must compute inside the mate's own scope,
    # pre-merge (`_local_exposure_obligations`): post-merge the member reads as
    # the fused group coalesce and the probe can never be NULL. The outer loop
    # can't request it once this merge satisfies the member, so carry it here.
    mate_addresses = {m.address for m in mates}
    mate_probes = [
        c
        for c in environment.concepts.values()
        if _is_presence_probe(c.address)
        and probe_member_address(c.address, environment) in mate_addresses
    ]
    enrich_node = source_concepts(
        mandatory_list=unique(mates + mate_probes + enrich_remaining, "address"),
        environment=environment,
        g=g,
        depth=depth + 1,
        conditions=conditions,
        history=history,
    )
    if not enrich_node:
        return None
    exposed: set[str] = set()
    for x in enrich_node.output_concepts:
        if x.address in enrich_node.hidden_concepts:
            continue
        exposed.add(x.address)
        exposed |= set(x.pseudonyms)
    if any(not (exposed & group) for group in mate_groups):
        return None
    for x in list(node.output_concepts):
        if x.address in node.hidden_concepts and x.address in coalescing_members:
            node.unhide_output_concepts([x])
    non_hidden = [
        x for x in node.output_concepts if x.address not in node.hidden_concepts
    ]
    non_hidden_enrich = [
        x
        for x in enrich_node.output_concepts
        if x.address not in enrich_node.hidden_concepts
    ]
    carried_probes = [
        p for p in mate_probes if p.address in {c.address for c in non_hidden_enrich}
    ]
    return MergeNode(
        input_concepts=unique(non_hidden + non_hidden_enrich, "address"),
        output_concepts=unique(non_hidden + local_optional + carried_probes, "address"),
        environment=environment,
        depth=depth,
        parents=[node, enrich_node],
        preexisting_conditions=conditions.conditional if conditions else None,
    )


def _enrich_rowset_node(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History,
    conditions: BuildWhereClause | None,
    node: RowsetNode,
) -> StrategyNode:
    """Join the rowset node to outer dimensions to satisfy local optionals.

    Sources the join keys (plus any pseudonym bridge keys) alongside the missing
    optionals and merges them back onto the rowset. Returns the bare rowset node
    when no enrichment is possible/required."""
    remaining = unsatisfied_optionals(local_optional, node)
    # A subset-join member among this node's OWN outputs reads as unsatisfied
    # (scoped_partial marks it partial), but it is partial by DECLARATION — the
    # anchor holds the fuller group axis — not by source: no other node produces
    # a fuller copy of a rowset-local column. Sourcing one routes to the anchor
    # rowset, whose enrichment symmetrically requests this member back —
    # unbounded (q59 store/offset-week join). The member is served here; the
    # merge above pairs it with the anchor over the group pseudonym.
    subset_members = environment.domain_graph.subset_sources()
    if subset_members:
        own_outputs = {x.address for x in node.output_concepts}
        remaining = [
            x
            for x in remaining
            if not (
                x.derivation == Derivation.ROWSET
                and x.address in subset_members
                and x.address in own_outputs
            )
        ]
    # A scoped-join key-group member is NOT satisfied through its group-mate
    # pseudonym: the merge between this rowset and the enrichment side joins the
    # two physical columns, so the other side must materialize its OWN member.
    # `unsatisfied_optionals` would drop it here (this node's group-mate output
    # pseudonym-matches it), the enrichment request would omit it, and the
    # inferred join would silently lose that key — cross-producting the rows on
    # whatever keys remain (TPC-DS q59). Re-add it unless this node exposes the
    # member itself.
    group_members = environment.distinct_scoped_join_group_members()
    if group_members:
        node_outputs = {x.address for x in node.output_concepts}
        remaining_addresses = {x.address for x in remaining}
        for optional in local_optional:
            if (
                optional.address in group_members
                and optional.address not in node_outputs
                and optional.address not in remaining_addresses
            ):
                remaining.append(optional)
                remaining_addresses.add(optional.address)
    # An outer WHERE can reference a concept the rowset doesn't produce (e.g. a
    # dimension property reachable only via the declared `join rs.k = dim.key`).
    # That arg must be sourced and the predicate applied even when every SELECT
    # optional is already in the rowset -- otherwise `remaining` is empty, we
    # return the bare node, and the filter is silently dropped (wrong results).
    # Exclude ROWSET-derived operands: a predicate comparing against another
    # rowset is `_apply_cross_rowset_where`'s job (already tried above); enriching
    # toward it here would mis-route into rowset re-sourcing.
    cond_remaining: list[BuildConcept] = []
    if conditions:
        have = _producible_addresses(node, deep=False, include_pseudonyms=True)
        cond_remaining = unique(
            [
                environment.concepts[r.address]
                for r in conditions.row_arguments
                if r.address not in have
                and r.address in environment.concepts
                and environment.concepts[r.address].derivation != Derivation.ROWSET
            ],
            "address",
        )
    if not remaining and not cond_remaining:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no enrichment required for rowset node as all optional {[x.address for x in local_optional]} found or no optional; exiting early."
        )
        return node
    # A scoped join on a DERIVED key (`a.grp + 1 = b.grp`) leaves no rowset-output
    # pseudonym for `_pseudonym_bridge_keys` to find, and sourcing the key via
    # discovery would re-enter this rowset and recurse. Materialize it locally and
    # merge with the other side over its pseudonym. Gated on the shape so the
    # cheaper standard enrichment below is unperturbed for every other rowset.
    #
    # The enrichment relates this scope to the OTHER side of the declared
    # relation, so its request may only carry concepts that relation can resolve
    # (rowset-derived values and computations over them). A base-model concept —
    # typically an outer WHERE arg like `s.date.year` — has no source on the
    # other side; bundling it asks the other rowset to resolve it, whose own
    # enrichment symmetrically delegates it back through this one, unbounded
    # (q02). Base residue is left to the outer loop, and when any condition arg
    # is base-model the conditions can't be discharged here either, so they are
    # withheld from the request and the merge makes no preexisting claim.
    derived_keys = _producible_derived_join_keys(node, environment)
    relation_remaining = [x for x in remaining if _rowset_scope_routed(x)]
    routed_cond = [x for x in cond_remaining if _rowset_scope_routed(x)]
    if derived_keys and relation_remaining:
        merged = _enrich_via_derived_join_key(
            derived_keys,
            unique(relation_remaining + routed_cond, "address"),
            local_optional,
            environment,
            g,
            depth,
            source_concepts,
            history,
            conditions if len(routed_cond) == len(cond_remaining) else None,
            node,
        )
        if merged is not None:
            return merged
    bridge_keys = _pseudonym_bridge_keys(node.output_concepts, environment)
    coalescing_members = environment.distinct_scoped_join_group_mates()
    producible = _producible_addresses(node, deep=False, include_pseudonyms=True)
    possible_joins = concept_to_relevant_joins(
        [
            x
            for x in node.output_concepts
            if x.derivation != Derivation.ROWSET
            and not _externally_unsourceable(x, coalescing_members, producible)
        ]
        + [canonical for _, canonical in bridge_keys]
    )
    if not possible_joins:
        # No bridge canonical at all — the rowset<->rowset coalescing-join case:
        # every group member is a rowset/derived column, so the only join
        # columns are the other side's own members.
        if relation_remaining:
            merged = _enrich_via_group_mate_keys(
                unique(relation_remaining + routed_cond, "address"),
                local_optional,
                environment,
                g,
                depth,
                source_concepts,
                history,
                conditions if len(routed_cond) == len(cond_remaining) else None,
                node,
                coalescing_members,
                producible,
            )
            if merged is not None:
                return merged
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no possible joins for rowset node to get {[x.address for x in local_optional]}; have {[x.address for x in node.output_concepts]}"
        )
        return node
    if any(x.derivation == Derivation.ROWSET for x in possible_joins):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} cannot enrich rowset node with rowset concepts; exiting early"
        )
        return node
    logger.info([x.address for x in possible_joins + local_optional])
    enrich_remaining = unique(remaining + cond_remaining, "address")
    enrich_node: MergeNode = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=possible_joins + enrich_remaining,
        environment=environment,
        g=g,
        depth=depth + 1,
        conditions=conditions,
        history=history,
    )
    if not enrich_node:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate rowset enrichment node for {concept} with optional {local_optional}, returning just rowset node"
        )
        return node

    for x in possible_joins:
        if x.address in node.hidden_concepts:
            node.unhide_output_concepts([x])
    # keep the bridge FK visible on the rowset side so the merge joins on it
    for fk, _ in bridge_keys:
        if fk.address in node.hidden_concepts:
            node.unhide_output_concepts([fk])
    non_hidden = [
        x for x in node.output_concepts if x.address not in node.hidden_concepts
    ]
    non_hidden_enrich = [
        x
        for x in enrich_node.output_concepts
        if x.address not in enrich_node.hidden_concepts
    ]
    # A presence probe the enrichment sourced (a coalescing member's own-side
    # null marker) is read post-merge by the outer WHERE; dropping it forces
    # recomputation off the fused coalesced key, never NULL (TPC-DS q35).
    return MergeNode(
        input_concepts=non_hidden + non_hidden_enrich,
        output_concepts=retain_presence_probes(
            non_hidden + local_optional, non_hidden_enrich
        ),
        environment=environment,
        depth=depth,
        parents=[
            node,
            enrich_node,
        ],
        preexisting_conditions=conditions.conditional if conditions else None,
    )


def gen_rowset_node(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    if not isinstance(concept.lineage, BuildRowsetItem):
        raise SyntaxError(
            f"Invalid lineage passed into rowset fetch, got {type(concept.lineage)}, expected {BuildRowsetItem}"
        )
    lineage: BuildRowsetItem = concept.lineage
    select: SelectLineage | MultiSelectLineage = lineage.rowset.select

    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} rowset derived concepts are {lineage.rowset.derived_concepts}"
    )
    base_node = _build_rowset_body_node(concept, lineage, select, history, depth)

    rowset_relevant, additional_relevant, scoped_partial = _collect_advertised_outputs(
        lineage, select, environment, base_node, local_optional
    )
    _unhide_referenced_body_locals(base_node, rowset_relevant + additional_relevant)
    node = _build_translation_node(
        base_node,
        rowset_relevant,
        additional_relevant,
        scoped_partial,
        select,
        environment,
        depth,
    )

    # Discharge this scope's exposure obligations (module docstring rule 1):
    # derived join-key members and presence probes computable off the node's
    # own outputs, materialized here so the completion merge can relate this
    # side without re-sourcing anything through it.
    obligations = _local_exposure_obligations(node, environment)
    if obligations:
        for obligation in obligations:
            node.add_output_concept(obligation, rebuild=False)
        node.rebuild_cache()

    if conditions:
        merged = _apply_cross_rowset_where(
            concept,
            local_optional,
            environment,
            g,
            depth,
            source_concepts,
            history,
            conditions,
            node,
        )
        if merged is not None:
            return merged

    return _enrich_rowset_node(
        concept,
        local_optional,
        environment,
        g,
        depth,
        source_concepts,
        history,
        conditions,
        node,
    )
