"""v4 root datasource planning.

This replaces the old ROOT hand-off to v3 discovery for the case where a set
of sourced concepts needs connector concepts added before datasource components
can be merged. The connector search still reuses the graph Steiner helper for
now, but component sourcing and final assembly stay in the v4 root planner.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import cast

from trilogy.core import graph as nx
from trilogy.core.enums import Derivation, Granularity, JoinType
from trilogy.core.graph_models import (
    ReferenceGraph,
    concept_to_node,
    prune_sources_for_conditions,
)
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildGrain,
    BuildUnionDatasource,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.aggregate_rollup import (
    _is_additive_aggregate,
    filter_finer_row_args,
    get_additive_rollup_concepts,
)
from trilogy.core.processing.condition_utility import (
    condition_implies,
    merge_conditions,
)
from trilogy.core.processing.node_generators.common import (
    inject_authored_join_key_terminals,
)
from trilogy.core.processing.node_generators.node_merge_node import (
    AMBIGUITY_CHECK_LIMIT,
    detect_ambiguity_and_raise,
    determine_induced_minimal_nodes,
)
from trilogy.core.processing.node_generators.presence_probe import (
    coalescing_axis_group,
    gen_coalescing_axis_node,
    is_presence_probe,
    member_binding_datasources,
    probe_member_address,
)
from trilogy.core.processing.node_generators.select_helpers.condition_routing import (
    covered_conditions,
)
from trilogy.core.processing.node_generators.select_helpers.datasource_injection import (
    get_union_sources,
)
from trilogy.core.processing.node_generators.select_helpers.datasource_nodes import (
    create_select_node,
    create_select_node_candidate,
    finalize_select_node,
)
from trilogy.core.processing.nodes import History, MergeNode, SelectNode, StrategyNode
from trilogy.core.processing.v4_helper.constants import ROW_SHAPE_BARRIER_DERIVATIONS
from trilogy.core.processing.v4_helper.source_policy import (
    STRICT_SOURCE_POLICY,
    SourceAttempt,
    SourcePolicy,
)
from trilogy.utility import unique


@dataclass(frozen=True)
class SourceRequest:
    outputs: list[BuildConcept]
    environment: BuildEnvironment
    graph: ReferenceGraph
    history: History
    conditions: BuildWhereClause | None = None
    depth: int = 0
    source_policy: SourcePolicy = STRICT_SOURCE_POLICY
    # False inside a partial-completion sub-call, so completing a partial output
    # cannot re-enter `_complete_partial_requested` on itself (infinite loop when
    # the concept has no complete source).
    complete_partials: bool = True


@dataclass(frozen=True)
class BridgePlan:
    concepts: list[BuildConcept]
    graph: ReferenceGraph
    # True when the graph added no connector concept and was accepted only
    # because it spans multiple datasources covering the full request — a
    # last-resort join `_direct_source` should get first shot at beating.
    full_cover_fallback: bool = False


def _concept_node_address(node: str) -> str:
    return node.split("~", maxsplit=1)[1].split("@", maxsplit=1)[0]


def _concept_node_grain_addresses(node: str) -> set[str]:
    marker = "@Grain<"
    if marker not in node:
        return set()
    grain = node.split(marker, maxsplit=1)[1].rsplit(">", maxsplit=1)[0]
    return {address for address in grain.split(",") if address}


def _concepts_in_graph(
    graph: ReferenceGraph, environment: BuildEnvironment
) -> list[BuildConcept]:
    """Resolve every concept node the bridge kept to this environment's
    concepts. A Steiner solution can traverse a lineage-bridge node for a
    derived variant minted in ANOTHER build scope (e.g. a rowset body's
    `alias(...)` key built under different scoped joins, reachable through the
    handle's pseudonym web but never registered here). Such a node proves
    connectivity but cannot be requested or planned in this environment —
    resolve what this scope knows and drop the rest."""
    out: list[BuildConcept] = []
    for node in graph.nodes:
        if not node.startswith("c~"):
            continue
        concept = environment.canonical_concepts.get(_concept_node_address(node))
        if concept is not None:
            out.append(concept)
    return out


def _condition_row_concepts(
    conditions: BuildWhereClause | None,
) -> list[BuildConcept]:
    if conditions is None:
        return []
    return unique(list(conditions.row_arguments), "address")


def _requested_concepts(request: SourceRequest) -> list[BuildConcept]:
    return unique(
        request.outputs + _condition_row_concepts(request.conditions),
        "address",
    )


def _concepts_with_grain_keys(
    concepts: list[BuildConcept],
    environment: BuildEnvironment,
) -> list[BuildConcept]:
    expanded: list[BuildConcept] = []
    for concept in concepts:
        expanded.append(concept)
        expanded.extend(
            environment.concepts[address]
            for address in concept.grain.components
            if address in environment.concepts
        )
    return unique(expanded, "address")


def _direct_source(
    request: SourceRequest, attempt: SourceAttempt
) -> StrategyNode | None:
    outputs = unique(
        request.outputs + _condition_row_concepts(request.conditions),
        "address",
    )
    node = request.history.gen_select_node(
        outputs,
        request.environment,
        request.graph,
        request.depth,
        fail_if_not_found=False,
        conditions=request.conditions,
        accept_partial=attempt.accepts_partial,
    )
    if node is None or {c.address for c in outputs} == {
        c.address for c in request.outputs
    }:
        return node
    return SelectNode(
        output_concepts=request.outputs,
        input_concepts=node.output_concepts,
        environment=request.environment,
        parents=[node],
    )


def _condition_arg_lineage_roots(request: SourceRequest) -> list[BuildConcept]:
    """ROOT lineage sources of any *derived* condition row-arg.

    A derived WHERE arg (e.g. `launch_date <- launch_jd`) is dropped by the
    `filter_downstream` Steiner pass, so without its sourceable root in the
    search the datasource that supplies it (`launch_info`) is scanned only for
    join keys and the rendered WHERE references an unscanned column
    (INVALID_REFERENCE). Pull those roots into the bridge search explicitly.

    A row-shape-barrier arg (RECURSIVE/AGGREGATE/WINDOW/...) is deliberately
    NOT inlined this way: pulling its roots lets the renderer recompute it from
    lineage (a RECURSIVE collapses to a single-step CASE), giving wrong rows.
    Such an arg must be sourced through its own node and joined — left to
    `gen_root`'s `_resolve_root_condition_sources` fallback, which the bridge
    triggers by failing to source the arg here."""
    roots: list[BuildConcept] = []
    for concept in _condition_row_concepts(request.conditions):
        if concept.lineage is None:
            continue
        if concept.derivation in ROW_SHAPE_BARRIER_DERIVATIONS:
            continue
        roots.extend(
            source for source in concept.sources if source.derivation == Derivation.ROOT
        )
    return roots


def _search_concepts_for_bridge(request: SourceRequest) -> list[BuildConcept]:
    return _concepts_with_grain_keys(
        inject_authored_join_key_terminals(
            unique(
                _requested_concepts(request) + _condition_arg_lineage_roots(request),
                "address",
            ),
            request.environment,
        ),
        request.environment,
    )


def _single_source_covers_completely(request: SourceRequest) -> bool:
    """One datasource binds every requested concept (and filter column) as a
    non-partial output.

    When such a source exists no join connector is needed, so a bridge that
    "adds" one is routing the requested columns through an unnecessary finer
    source (e.g. base `flight` via its `id` key) instead of the summary table
    that carries them directly. Let `_direct_source`'s grain-aware scoring pick
    the better single source. A partial coverage still needs the bridge (the
    partial key must be upgraded against its dimension), so non-partial is
    required here."""
    requested = {c.canonical_address for c in _requested_concepts(request)}
    for ds in request.environment.datasources.values():
        if not isinstance(ds, BuildDatasource):
            continue
        non_partial = {c.canonical_address for c in ds.output_concepts} - {
            c.canonical_address for c in ds.partial_concepts
        }
        if requested.issubset(non_partial):
            return True
    return False


def _bridge_plan(request: SourceRequest, attempt: SourceAttempt) -> BridgePlan | None:
    if _single_source_covers_completely(request):
        return None
    search_concepts = _search_concepts_for_bridge(request)
    requested = {c.address for c in _requested_concepts(request)}
    # (search_conditions, allow_intersection), most-specific first. The covered
    # option mirrors v3's complete-where retry: when the full condition spans
    # datasources (e.g. `city='USSFO' AND native_status IS NOT NULL`, with city
    # owned by a `complete where` partial and native_status by a joined table),
    # pruning on the full condition disconnects the joined table. Retrying on
    # only the *covered* atoms (those implied by some datasource's
    # `non_partial_for`) with `allow_intersection` keeps the foreign table and
    # promotes the complete-where partial over the full union.
    condition_options: list[tuple[BuildWhereClause | None, bool]]
    if request.conditions is not None:
        condition_options = [(request.conditions, False)]
        covered = covered_conditions(request.conditions, request.environment)
        if covered is not None:
            condition_options.append((covered, True))
        condition_options.append((None, False))
    else:
        condition_options = [(None, False)]
    for filter_downstream in (True, False):
        for search_conditions, allow_intersection in condition_options:
            graph = _resolve_bridge_graph(
                search_concepts,
                request,
                attempt=attempt,
                filter_downstream=filter_downstream,
                search_conditions=search_conditions,
                allow_intersection=allow_intersection,
            )
            if graph is None:
                continue
            bridged = unique(_concepts_in_graph(graph, request.environment), "address")
            # The bridge is only worth preferring over `_direct_source` when it
            # ADDED connector concepts (a superset of what was requested) or a
            # union. A bridged set that is a strict *subset* of `requested` is
            # not a connector — it just means single-row/abstract concepts were
            # excluded from the bridge search (`_resolve_bridge_graph`). Treating
            # that subset as a connector would route the remaining concepts
            # through a single partial datasource, dropping the complete-key
            # merge `_direct_source` would otherwise build (refresh of a
            # count-by-X persist whose output also carries a `data_through`).
            # ...or when it selected a `complete where` partial whose predicate
            # the query implies (an exact-match source). `_direct_source` would
            # otherwise fall through to the full table, since the partial is
            # rejected by the strict attempt and the full condition disconnects
            # the partial's join partner from the graph (q geography exact-match).
            # A multi-datasource graph that covers the full request is likewise
            # a genuine connector even when it adds no new concept — the join
            # key was simply already requested (`st as store` under `merge st
            # into store_id`: sales ⋈ stores on the requested store_id).
            # Flagged as a FALLBACK: `_direct_source` gets first shot, since a
            # request it can satisfy already had a good single plan (q64 join
            # hoist, two_merge window compaction) — the flagged bridge only
            # rescues requests no direct source can cover.
            bridged_addresses = {c.address for c in bridged}
            if (
                (bridged_addresses - requested)
                or _has_union_datasource(graph)
                or _graph_has_condition_matched_partial(
                    graph, request.conditions, request.environment
                )
            ):
                return BridgePlan(concepts=bridged, graph=graph)
            if len(graph.datasources) > 1 and requested <= bridged_addresses:
                return BridgePlan(
                    concepts=bridged, graph=graph, full_cover_fallback=True
                )
    return None


def _graph_has_condition_matched_partial(
    graph: ReferenceGraph,
    conditions: BuildWhereClause | None,
    environment: BuildEnvironment,
) -> bool:
    """A `complete where <c>` partial datasource in `graph` whose predicate the
    query conditions imply — pre-filtered to exactly the requested rows, so it
    is the authoritative (and smaller) source over a full table."""
    if conditions is None:
        return False
    for ds in graph.datasources.values():
        if (
            isinstance(ds, BuildDatasource)
            and ds.non_partial_for is not None
            and condition_implies(
                conditions.conditional, ds.non_partial_for.conditional
            )
        ):
            return True
    return False


def _has_union_datasource(graph: ReferenceGraph) -> bool:
    return any(
        node in graph and isinstance(datasource, BuildUnionDatasource)
        for node, datasource in graph.datasources.items()
    )


def _resolve_bridge_graph(
    concepts: list[BuildConcept],
    request: SourceRequest,
    *,
    attempt: SourceAttempt,
    filter_downstream: bool,
    search_conditions: BuildWhereClause | None,
    allow_intersection: bool = False,
) -> ReferenceGraph | None:
    search_graph = request.graph.copy()
    _inject_union_datasources(search_graph, concepts, request.environment)
    prune_sources_for_conditions(
        search_graph,
        attempt.criteria,
        conditions=search_conditions,
        allow_intersection=allow_intersection,
    )
    # Single-row / abstract-grain concepts (a `<*>` watermark, a constant) join
    # by cross product, never by a key, so they must not drive the connectivity
    # search — otherwise the Steiner walk invents a spurious intermediate join
    # key to attach them and two equal candidates raise a false ambiguity
    # (refresh of a count-by-X persist with a shared `data_through`). They fall
    # through to `_direct_source`, which sources constants as a cross-join.
    bridge_concepts = [
        concept for concept in concepts if concept.granularity != Granularity.SINGLE_ROW
    ]
    node_list = sorted(
        concept_to_node(concept.with_default_grain())
        for concept in bridge_concepts
        if "__preql_internal" not in concept.address
    )
    synonyms: dict[str, str] = {}
    for concept in bridge_concepts:
        for pseudonym in concept.pseudonyms:
            synonyms[pseudonym] = concept.address

    found: list[ReferenceGraph] = []
    reduced_concept_sets: list[set[str]] = []
    requested_addresses = {concept.address for concept in bridge_concepts}
    for _ in range(AMBIGUITY_CHECK_LIMIT):
        try:
            graph = determine_induced_minimal_nodes(
                search_graph,
                node_list,
                filter_downstream=filter_downstream,
                accept_partial=attempt.accepts_partial,
                environment=request.environment,
                synonyms=synonyms,
            )
        except nx.exception.NetworkXNoPath:
            break
        if graph is None or not graph.nodes or not nx.is_weakly_connected(graph):
            break
        bridge_graph = cast(ReferenceGraph, graph)
        all_graph_concepts = _concepts_in_graph(bridge_graph, request.environment)
        new = [
            concept
            for concept in all_graph_concepts
            if concept.address not in requested_addresses
        ]
        found.append(bridge_graph)
        reduced_concept_sets.append(
            {concept.address for concept in new if concept.address not in synonyms}
        )
        if not new:
            break
        for concept in new:
            node = concept_to_node(concept)
            if node in search_graph:
                search_graph.remove_node(node)
    if not found:
        return None
    detect_ambiguity_and_raise(
        bridge_concepts, reduced_concept_sets, request.environment
    )
    return found[0]


def _inject_union_datasources(
    graph: ReferenceGraph,
    concepts: list[BuildConcept],
    environment: BuildEnvironment,
) -> None:
    datasources = [
        datasource
        for datasource in environment.datasources.values()
        if isinstance(datasource, BuildDatasource)
    ]
    union_edges: list[tuple[str, str]] = []
    for datasource_group in get_union_sources(datasources, concepts):
        node_address = "ds~" + "-".join(
            [datasource.name for datasource in datasource_group]
        )
        if node_address in graph.datasources:
            continue
        merged_condition = merge_conditions(
            [
                datasource.non_partial_for.conditional
                for datasource in datasource_group
                if datasource.non_partial_for is not None
            ]
        )
        non_partial_for = (
            BuildWhereClause(conditional=merged_condition)
            if merged_condition is not None
            else None
        )
        graph.datasources[node_address] = BuildUnionDatasource(
            children=datasource_group,
            non_partial_for=non_partial_for,
        )
        common_outputs = set(datasource_group[0].output_concepts)
        for datasource in datasource_group[1:]:
            common_outputs &= set(datasource.output_concepts)
        for concept in common_outputs:
            concept_node = concept_to_node(concept)
            union_edges.append((node_address, concept_node))
            union_edges.append((concept_node, node_address))
    graph.add_edges_from(union_edges)


def _concept_has_non_basic_merge_origin(
    concept: BuildConcept, environment: BuildEnvironment
) -> bool:
    """`concept` is a merge key whose value comes from a non-BASIC (recursive /
    aggregate) origin — its real lineage lives in `alias_origin_lookup` under the
    concept's address or a pseudonym, while `environment.concepts` holds a demoted
    lineage-less ROOT. Such a key is materialized by `_derived_connector_nodes`,
    never a raw scan. A BASIC merge origin (`p_last <- split(p_name)`) computes
    inline on the scan, so it is excluded."""
    for alias in (concept.address, *concept.pseudonyms):
        origin = environment.alias_origin_lookup.get(alias)
        if (
            origin is not None
            and origin.lineage is not None
            and origin.derivation != Derivation.BASIC
        ):
            return True
    return False


def _bridge_has_non_basic_merge(
    plan: BridgePlan, environment: BuildEnvironment
) -> bool:
    """A bridge concept merges with a non-BASIC (recursive/aggregate) origin.

    Such a key is materialized by `_derived_connector_nodes`, not a raw scan, so
    the datasource-registration gap-fill must stand down for that bridge.
    """
    return any(
        _concept_has_non_basic_merge_origin(concept, environment)
        for concept in plan.concepts
    )


def _datasource_nodes_for_bridge(
    request: SourceRequest,
    plan: BridgePlan,
    attempt: SourceAttempt,
) -> list[StrategyNode] | None:
    parents: list[StrategyNode] = []
    bridge_addresses = {concept.address for concept in plan.concepts}
    # A datasource the Steiner tree reached only via the post-pass (a derived
    # merge key routed the walk through the key's reverse-lineage instead of the
    # datasource) is a node in the bridge graph but missing from its
    # `.datasources` registry (rebuilt from the Steiner nodes). Re-point it from
    # the full source graph so the loop scans it. `plan.graph` is this bridge's
    # private copy, so this never disturbs the shared graph or the recursive
    # connector's own bridges (unlike mutating the Steiner helper directly).
    #
    # Skip entirely when a bridge concept merges with a non-BASIC (recursive /
    # aggregate) origin: that key is supplied by `_derived_connector_nodes`, and
    # re-pointing the datasources its subplan consumes lets the bridge scan the
    # merged key directly, stranding the connector (recursive enrichment). A
    # BASIC merge key (`r_last <- split`) is computed inline on the scan, so it
    # is safe.
    if not _bridge_has_non_basic_merge(plan, request.environment):
        covered = {
            concept.address
            for ds in plan.graph.datasources.values()
            if isinstance(ds, BuildDatasource)
            for concept in ds.output_concepts
        }
        for ds_node in sorted(n for n in plan.graph.nodes if n.startswith("ds~")):
            if ds_node in plan.graph.datasources:
                continue
            source_ds = request.graph.datasources.get(ds_node)
            # Union sources are injected separately (`_inject_union_datasources`).
            if not isinstance(source_ds, BuildDatasource):
                continue
            # Only fill a genuine gap: register the missing source iff it provides
            # a bridge concept no already-registered datasource covers. Blindly
            # registering every reachable alternate over-sources a union/semijoin
            # bridge (regresses partial_union_bridge_semijoin).
            provides = {c.address for c in source_ds.output_concepts} & bridge_addresses
            if provides - covered:
                plan.graph.datasources[ds_node] = source_ds
                covered |= provides
    for ds_node in sorted(node for node in plan.graph.datasources):
        concept_nodes = _local_concept_nodes_for_datasource(
            plan.graph,
            ds_node,
            bridge_addresses,
            request.environment,
        )
        concept_nodes.extend(
            _original_datasource_concept_nodes(
                request.graph,
                plan.graph,
                ds_node,
                bridge_addresses,
                request.environment,
            )
        )
        concept_nodes.extend(
            _datasource_grain_concept_nodes(
                plan.graph,
                ds_node,
                concept_nodes,
                request.environment,
            )
        )
        concept_nodes = sorted(set(concept_nodes))
        if not concept_nodes:
            continue
        # Pass the WHERE only to a `complete where` partial the query implies, so
        # `create_select_node_candidate` clears its partial flag (partial_is_full)
        # and applies the predicate on the scan -- otherwise its outputs stay
        # partial and `_complete_partial_requested` joins the full table back in
        # (geography exact-match). Other sources get the condition post-merge.
        ds_obj = plan.graph.datasources.get(ds_node)
        ds_conditions = (
            request.conditions
            if (
                isinstance(ds_obj, BuildDatasource)
                and ds_obj.non_partial_for is not None
                and request.conditions is not None
                and condition_implies(
                    request.conditions.conditional, ds_obj.non_partial_for.conditional
                )
            )
            else None
        )
        candidate = create_select_node_candidate(
            ds_node,
            concept_nodes,
            accept_partial=attempt.accepts_partial,
            g=plan.graph,
            environment=request.environment,
            depth=request.depth + 1,
            conditions=ds_conditions,
        )
        parents.append(
            finalize_select_node(
                candidate,
                environment=request.environment,
                depth=request.depth + 1,
            )
        )
    parents.extend(_derived_connector_nodes(request, plan, parents))
    if not parents:
        return None
    return parents


def _derived_connector_nodes(
    request: SourceRequest,
    plan: BridgePlan,
    datasource_parents: list[StrategyNode],
) -> list[StrategyNode]:
    """Materialize bridge concepts whose source is a *derived* connector.

    The bridge can route through a merged derivation (e.g. a recursive
    `recurse_edge` whose output was `merge`d into a dimension key) that is not a
    real datasource — its real lineage lives in `alias_origin_lookup`, keyed by
    the concept's address or any pseudonym, while `environment.concepts` holds a
    demoted lineage-less ROOT. Such a connector is dropped by the `ds~`-only
    loop above, leaving the concept it provides unsourced (INVALID_REFERENCE).
    Plan each needed connector's true origin and hand it back as a parent;
    `_merge_component_sources` joins it on the pseudonym equivalence.
    """
    covered = {
        c.address for parent in datasource_parents for c in parent.usable_outputs
    }
    if {c.address for c in plan.concepts} <= covered:
        return []
    # Imported lazily: `concept_strategies_v4` imports this module's package.
    from trilogy.core.processing.concept_strategies_v4 import (
        V4History,
        search_concepts,
    )

    env = request.environment
    history = cast(V4History, request.history)
    planned: set[str] = set()
    connectors: list[StrategyNode] = []
    for concept in plan.concepts:
        for alias in (concept.address, *concept.pseudonyms):
            origin = env.alias_origin_lookup.get(alias)
            # Skip non-derived origins, anything a datasource already sources,
            # and any connector currently mid-plan. The last is the re-entry
            # guard: planning a connector recurses to source its own inputs,
            # whose bridge re-routes through the same connector — without the
            # guard that re-injects forever.
            if (
                origin is None
                or origin.lineage is None
                or origin.address in planned
                or origin.address in covered
                or origin.address in history.connectors_in_progress
            ):
                continue
            # Carry the connector's grain keys (e.g. a recursion keyed by `id`
            # must emit `id`, not group it away) so the merge can join the
            # connector back to the consumer on that key. The key must be emitted
            # even when another parent already covers it — it IS the join column;
            # dropping it leaves the merge with no shared key and a 1=1 cross join
            # (hackernews: the recursion's `id` is also the post scan's `id`).
            mandatory = unique(
                [origin]
                + [
                    env.concepts[address]
                    for address in origin.grain.components
                    if address in env.concepts
                ],
                "address",
            )
            history.connectors_in_progress.add(origin.address)
            try:
                info = search_concepts(
                    mandatory_list=mandatory,
                    history=history,
                    environment=env,
                    depth=request.depth + 1,
                    g=request.graph,
                    conditions=[],
                    source_policy=request.source_policy,
                )
            finally:
                history.connectors_in_progress.discard(origin.address)
            if info.strategy_node is not None:
                planned.add(origin.address)
                connectors.append(info.strategy_node)
    return connectors


def _datasource_grain_concept_nodes(
    graph: ReferenceGraph,
    ds_node: str,
    concept_nodes: list[str],
    environment: BuildEnvironment,
) -> list[str]:
    selected_addresses = {
        _concept_node_address(node) for node in concept_nodes if node.startswith("c~")
    }
    datasource = graph.datasources.get(ds_node)
    if datasource is None:
        return []
    grain_addresses: set[str] = set()
    for node in concept_nodes:
        if node.startswith("c~") and isinstance(datasource, BuildUnionDatasource):
            grain_addresses.update(_concept_node_grain_addresses(node))
    for address in selected_addresses:
        concept = environment.concepts.get(address)
        if concept is not None:
            grain_addresses.update(concept.grain.components)
    if not grain_addresses or ds_node not in graph:
        return []
    nodes = [
        neighbor
        for neighbor in graph.neighbors(ds_node)
        if neighbor.startswith("c~")
        and _concept_node_address(neighbor) in grain_addresses
    ]
    node_addresses = {_concept_node_address(node) for node in nodes}
    for address in sorted(grain_addresses - node_addresses):
        concept = environment.concepts.get(address)
        if concept is None or not _datasource_can_output(datasource, address):
            continue
        nodes.append(concept_to_node(concept))
    return nodes


def _datasource_can_output(
    datasource: BuildDatasource | BuildUnionDatasource,
    address: str,
) -> bool:
    if isinstance(datasource, BuildDatasource):
        return any(concept.address == address for concept in datasource.output_concepts)
    return all(
        any(concept.address == address for concept in child.output_concepts)
        for child in datasource.children
    )


def _datasource_renders_derived(
    datasource: BuildDatasource | BuildUnionDatasource | None,
    concept: BuildConcept,
) -> bool:
    """Every ROOT leaf of `concept`'s lineage is an output column of `datasource`.

    A scoped-merged BASIC derived key has one canonical variant per join side
    (`da <- o.amt+1` pseudonym of `db <- c.cost+1`); only the side whose base
    column this scan binds can compute its inline expression. The sibling scan
    produces the other variant and the merge joins them on the pseudonym
    equivalence -- assigning a scan the variant it cannot render emits that
    variant's lineage against a column it lacks (INVALID_REFERENCE).

    A key can also be bound to a scan directly: a derived key merged into a
    physical column (`merge d1 into ~s1`, `s1 <- base+1`) is exposed by the
    `facts` scan as its own `d1` column, so that scan renders it even though its
    lineage root (`base`) lives elsewhere -- check the direct binding first."""
    if datasource is None:
        return False
    if _datasource_can_output(datasource, concept.address):
        return True
    roots = [
        source for source in concept.sources if source.derivation == Derivation.ROOT
    ]
    if not roots:
        return True
    return all(_datasource_can_output(datasource, source.address) for source in roots)


def _datasource_renders_probe(
    datasource: BuildDatasource | BuildUnionDatasource | None,
    address: str,
    environment: BuildEnvironment,
) -> bool:
    """A presence probe pins side identity: post-substitution every key-group
    member's binding shares the canonical address, so lineage-based checks pass
    on BOTH sides of the scoped relation — but the probe is NULL exactly where
    the member's side is absent, so only a scan physically carrying the
    member's authored column (via `origin_address`) may compute it. Computing
    it on the complement side makes the probe never-NULL and the null test a
    silent no-op (the q84/q59 idiom). Non-probe concepts pass through.

    Graph nodes carry the probe's canonical `_virt_func_*` address; the
    `_virt_presence_*` identity (whose hash names the pinned member) lives on
    the resolved concept's own `.address`."""
    concept = environment.canonical_concepts.get(address)
    probe_address = concept.address if concept is not None else address
    if not is_presence_probe(probe_address):
        return True
    if not isinstance(datasource, BuildDatasource):
        return False
    member = probe_member_address(probe_address, environment)
    if member is None:
        return True
    return datasource.name in {
        ds.name for ds in member_binding_datasources(member, environment)
    }


def _original_datasource_concept_nodes(
    source_graph: ReferenceGraph,
    bridge_graph: ReferenceGraph,
    ds_node: str,
    bridge_addresses: set[str],
    environment: BuildEnvironment,
) -> list[str]:
    concept_nodes: list[str] = []
    if ds_node not in source_graph:
        return concept_nodes
    ds_obj = source_graph.datasources.get(ds_node)
    for neighbor in source_graph.neighbors(ds_node):
        if not neighbor.startswith("c~"):
            continue
        address = _concept_node_address(neighbor)
        if address not in bridge_addresses or address not in environment.concepts:
            continue
        if not _datasource_renders_probe(ds_obj, address, environment):
            continue
        if neighbor not in bridge_graph:
            bridge_graph.add_node(neighbor)
            if neighbor in source_graph.concepts:
                bridge_graph.concepts[neighbor] = source_graph.concepts[neighbor]
        if not bridge_graph.has_edge(ds_node, neighbor):
            bridge_graph.add_edge(ds_node, neighbor)
        concept_nodes.append(neighbor)
    return concept_nodes


def _local_concept_nodes_for_datasource(
    graph: ReferenceGraph,
    ds_node: str,
    bridge_addresses: set[str],
    environment: BuildEnvironment,
) -> list[str]:
    datasource = graph.datasources.get(ds_node)
    queue: deque[str] = deque([ds_node])
    seen: set[str] = {ds_node}
    concepts: dict[str, str] = {}
    while queue:
        node = queue.popleft()
        for neighbor in graph.neighbors(node):
            if neighbor in seen:
                continue
            if neighbor.startswith("ds~"):
                continue
            seen.add(neighbor)
            if not neighbor.startswith("c~"):
                continue
            address = _concept_node_address(neighbor)
            canonical = environment.canonical_concepts.get(address)
            # A recursive/aggregate merge key (`recursive_parent` merged into a
            # dimension key) is reachable from this scan only through its
            # reverse-lineage (the scan provides its recursive INPUTS), but its
            # value is materialized by `_derived_connector_nodes`, not the scan.
            # Attach it here only when this datasource genuinely binds it as a
            # column (the property-side re-import scan keyed on it); otherwise the
            # bridge would emit it from the input scan and strand the connector.
            if (
                canonical is not None
                and _concept_has_non_basic_merge_origin(canonical, environment)
                and not (
                    datasource is not None
                    and _datasource_can_output(datasource, address)
                )
            ):
                queue.append(neighbor)
                continue
            # Bridge addresses are keyed by `.address`, but a derived concept's
            # graph node uses its `.canonical_address` (a `_virt_func_*` name) —
            # so a derived merge key (`da <- o.amt+1` merged/joined with
            # `db <- c.cost+1`) is missed unless we also match the node concept's
            # `.address`. Restrict that fallback to a BASIC-derived key this
            # datasource can actually render (every ROOT leaf of its lineage is a
            # bound column): a scoped-merged key exposes one variant per join side
            # (INNER links them as pseudonyms; FULL keeps them distinct to
            # coalesce), and only the side binding the base column may compute the
            # inline expression -- the sibling scan supplies the other variant and
            # the join relates them. Assigning a scan a variant it cannot render
            # emits an unbound column (INVALID_REFERENCE); a recursive/complex
            # merge key must instead come from `_derived_connector_nodes`.
            renders_derived_key = (
                canonical is not None
                and canonical.derivation == Derivation.BASIC
                and canonical.address in bridge_addresses
                and _datasource_renders_derived(datasource, canonical)
            )
            # A datasource-materialized aggregate/window (`customer_order_count`
            # in a summary table) is requested by its `.address` but reaches this
            # scan under its `_virt_agg_*` canonical node -- match the canonical
            # too, but only when the scan physically BINDS it as a column. Without
            # the binding guard a fact scan would emit the aggregate via its
            # reverse-lineage edge (order_id -> count) and recompute it wrongly;
            # with it, only the summary table that owns the column emits it.
            # Restricted to AGGREGATE/WINDOW: a plain root concept already matches
            # via `address in bridge_addresses` (its canonical IS its address), and
            # widening this to every derivation re-sources probe/filter members off
            # the wrong scan (gcat decom_spine).
            renders_materialized_canonical = (
                canonical is not None
                and canonical.derivation in (Derivation.AGGREGATE, Derivation.WINDOW)
                and canonical.address in bridge_addresses
                and datasource is not None
                and _datasource_can_output(datasource, canonical.address)
            )
            if (
                canonical is not None
                and (
                    address in bridge_addresses
                    or renders_derived_key
                    or renders_materialized_canonical
                )
                and _datasource_renders_probe(datasource, address, environment)
            ):
                concepts.setdefault(address, neighbor)
            queue.append(neighbor)
    return sorted(concepts.values())


def _merge_component_sources(
    request: SourceRequest,
    parents: list[StrategyNode],
    output_concepts: list[BuildConcept] | None = None,
) -> StrategyNode | None:
    if not parents:
        return None
    outputs = output_concepts or request.outputs
    inputs = unique(
        [concept for parent in parents for concept in parent.usable_outputs],
        "address",
    )
    if len(parents) == 1:
        parent = parents[0]
        if request.conditions is None and {
            c.address for c in parent.output_concepts
        } == {c.address for c in outputs}:
            return parent
        return SelectNode(
            output_concepts=outputs,
            input_concepts=inputs,
            environment=request.environment,
            parents=[parent],
            conditions=(
                request.conditions.conditional
                if request.conditions is not None
                else None
            ),
        )
    return MergeNode(
        input_concepts=inputs,
        output_concepts=outputs,
        environment=request.environment,
        parents=parents,
        depth=request.depth,
        conditions=(
            request.conditions.conditional if request.conditions is not None else None
        ),
    )


def _complete_partial_requested(
    request: SourceRequest, node: StrategyNode
) -> StrategyNode:
    """Upgrade a requested output that the bridge could only bind *partially*.

    On a strict (non-partial) pass a bridge can still carry a requested concept
    as a partial column -- e.g. the `~vehicle.name` merge key on `launch_info`:
    every launch has one, but the column is not vehicle.name's authoritative
    domain, so it is flagged partial and the final-output guard rejects it. v3's
    sourcing loop completes such a key against its dimension source (`lv_info`)
    and joins; mirror that here. If no *complete* source exists the node is left
    unchanged -- the genuinely-partial case stays for the partial passes / guard.
    """
    requested = {c.address for c in _requested_concepts(request)}
    partial_requested = [c for c in node.partial_concepts if c.address in requested]
    if not partial_requested:
        return node
    partial_addresses = {c.address for c in partial_requested}
    # Carry the WHERE onto the completing dimension when every column it
    # references is one we are completing (e.g. `vehicle.name like '%Falcon%'`);
    # otherwise the unfiltered dimension would re-introduce keys the bridge's
    # filter excluded. If the filter spans other columns the completion source
    # cannot satisfy it, so leave it on the bridge side only.
    completion_conditions = None
    if (
        request.conditions is not None
        and {c.address for c in request.conditions.row_arguments} <= partial_addresses
    ):
        completion_conditions = request.conditions
    completion = plan_source(
        SourceRequest(
            outputs=partial_requested,
            environment=request.environment,
            graph=request.graph,
            history=request.history,
            conditions=completion_conditions,
            depth=request.depth + 1,
            source_policy=STRICT_SOURCE_POLICY,
            complete_partials=False,
        )
    )
    if completion is None:
        return node
    completion_partial = {c.address for c in completion.partial_concepts}
    if any(c.address in completion_partial for c in partial_requested):
        return node
    inputs = unique(
        [c for parent in (completion, node) for c in parent.usable_outputs],
        "address",
    )
    # Anchor the complete (and filtered) dimension and outer-join the bridge, so
    # the requested key is non-partial and every surviving dimension value is
    # kept -- matching v3's `lv_info LEFT JOIN launch_info` shape.
    return MergeNode(
        input_concepts=inputs,
        output_concepts=node.output_concepts,
        environment=request.environment,
        parents=[completion, node],
        depth=request.depth,
    )


def _bridge_parents_cover(parents: list[StrategyNode], required: set[str]) -> bool:
    """Every requested concept is actually emitted by some bridge datasource.

    The graph Steiner solver can route a requested root concept through a
    *derived* concept's lineage (a window/aggregate output that shares a key
    with it), yielding a bridge whose datasource set never sources the concept
    — e.g. `country` (on `users`) "reached" from `posts` via a window keyed by
    `user_id`. `_merge_component_sources` would then promise `country` off a
    posts-only scan and render INVALID_REFERENCE. Reject such a bridge so
    `_direct_source` (which merges both datasources) is used instead."""
    available = {c.address for parent in parents for c in parent.usable_outputs}
    return required <= available


def _finer_filter_rollup_source(request: SourceRequest) -> BuildDatasource | None:
    """A single datasource that can serve an additive-rollup aggregate under a
    filter on a column *finer* than the requested grain.

    The filter splits the requested groups (e.g. `WHERE order_date` below a
    `customer_id` grain), so the only correct plan is to scan a summary table
    that carries both the aggregate and the finer column, push the filter into
    that scan, then SUM-roll to the requested grain. A coarser exact table
    (`agg_by_customer`) can't express the filter — joining its unfiltered count
    to a separately-filtered key list double-counts. We require ONE datasource
    that binds every requested aggregate, holds the requested grain keys, and
    supports the finer filter; otherwise there is no safe pinned source."""
    conditions = request.conditions
    if conditions is None:
        return None
    outputs = request.outputs
    aggregates = [c for c in outputs if c.is_aggregate and _is_additive_aggregate(c)]
    if not aggregates:
        return None
    environment = request.environment
    target_grain = BuildGrain.from_concepts(outputs, environment=environment)
    target_components = set(target_grain.components)
    if not target_components:
        return None
    # Every output must be a rolled aggregate or a target-grain key — a property
    # or other shape would not survive the pinned scan + SUM-roll.
    if any(not c.is_aggregate and c.address not in target_components for c in outputs):
        return None
    finer = filter_finer_row_args(conditions, target_grain, environment.concepts)
    if not finer:
        return None
    finer_canonicals = {c.canonical_address for c in finer}
    datasources = [
        ds for ds in environment.datasources.values() if isinstance(ds, BuildDatasource)
    ]
    matches: list[BuildDatasource] = []
    for ds in datasources:
        ds_canonicals = {c.canonical_address for c in ds.output_concepts}
        ds_addresses = {c.address for c in ds.output_concepts}
        if not finer_canonicals.issubset(ds_canonicals):
            continue
        if not target_components.issubset(ds_addresses):
            continue
        rolled = get_additive_rollup_concepts(
            datasource=ds,
            requested_concepts=list(outputs),
            concepts_by_address=environment.concepts,
            datasources=datasources,
            target_grain=target_grain,
            conditions=conditions,
        )
        rolled_addresses = {c.address for c in rolled}
        if all(agg.address in rolled_addresses for agg in aggregates):
            matches.append(ds)
    if not matches:
        return None
    # Prefer the finest match closest to the requested grain (fewest dropped
    # grain components) for a deterministic, cheapest source.
    matches.sort(key=lambda ds: (len(ds.grain.components), ds.name))
    return matches[0]


def _plan_complete_where_source(request: SourceRequest) -> StrategyNode | None:
    """Scan a `partial ... complete where <c>` datasource when the query's WHERE
    implies `<c>`.

    A partial datasource pre-filtered to `complete where customer_revenue > 100`
    is *complete* for any query whose conditions imply that predicate — every row
    it would otherwise be missing is excluded by the filter anyway. Pinning it
    lets `create_datasource_node` clear the partial flag (`partial_is_full`) and
    treat the predicate as already applied, instead of the planner picking a
    generic summary and then trying to render a HAVING it can't (the requested
    aggregate and the filter's aggregate are canonically equal but differently
    named, so the filter column isn't projected — INVALID_REFERENCE).

    Requires ONE partial datasource whose `non_partial_for` is implied by the
    conditions, that binds every requested output at the requested grain, and
    that carries each filter column (so any extra predicate beyond
    `non_partial_for` is still applied on the scan)."""
    conditions = request.conditions
    if conditions is None:
        return None
    outputs = request.outputs
    environment = request.environment
    target_grain = BuildGrain.from_concepts(outputs, environment=environment)
    target_canonicals = {
        environment.concepts[c].canonical_address
        for c in target_grain.components
        if c in environment.concepts
    }
    output_canonicals = {c.canonical_address for c in outputs}
    condition_canonicals = {
        c.canonical_address
        for c in conditions.row_arguments
        if c.granularity != Granularity.SINGLE_ROW
    }
    matches: list[BuildDatasource] = []
    for ds in environment.datasources.values():
        if not isinstance(ds, BuildDatasource) or ds.non_partial_for is None:
            continue
        # Only datasources exposed as a standalone scan in this graph are
        # addressable here. A union *member* (e.g. `store_sales_unified`) lives
        # in the environment but the graph only carries the union node, so
        # scanning it directly would KeyError -- leave it to the union planner.
        if f"ds~{ds.name}" not in request.graph.datasources:
            continue
        if not condition_implies(
            conditions.conditional, ds.non_partial_for.conditional
        ):
            continue
        ds_canonicals = {c.canonical_address for c in ds.output_concepts}
        if not output_canonicals.issubset(ds_canonicals):
            continue
        # The scan must still apply any residual predicate beyond
        # `non_partial_for`, so its columns must be present -- UNLESS
        # `non_partial_for` also implies the query condition (the two are
        # equivalent). Then the datasource is pre-filtered to exactly the
        # requested rows, there is no residual WHERE, and the filter columns
        # (e.g. `name` for a `complete where name = 'Sarah'` source that only
        # binds customer_id/revenue) need not be bound.
        residual_free = condition_implies(
            ds.non_partial_for.conditional, conditions.conditional
        )
        if not residual_free and not condition_canonicals.issubset(ds_canonicals):
            continue
        ds_grain_canonicals = {
            environment.concepts[c].canonical_address
            for c in ds.grain.components
            if c in environment.concepts
        }
        if ds_grain_canonicals != target_canonicals:
            continue
        matches.append(ds)
    if not matches:
        return None
    matches.sort(key=lambda ds: ds.name)
    ds = matches[0]
    scan_nodes = [concept_to_node(c.with_default_grain()) for c in outputs]
    return create_select_node(
        f"ds~{ds.name}",
        scan_nodes,
        accept_partial=False,
        g=request.graph,
        environment=environment,
        depth=request.depth + 1,
        conditions=conditions,
    )


def _plan_finer_filter_rollup(request: SourceRequest) -> StrategyNode | None:
    ds = _finer_filter_rollup_source(request)
    if ds is None:
        return None
    environment = request.environment
    outputs = list(request.outputs)
    scan_nodes = [concept_to_node(c.with_default_grain()) for c in outputs]
    scan = create_select_node(
        f"ds~{ds.name}",
        scan_nodes,
        accept_partial=True,
        g=request.graph,
        environment=environment,
        depth=request.depth + 1,
        conditions=request.conditions,
    )
    target_components = set(
        BuildGrain.from_concepts(outputs, environment=environment).components
    )
    partial_keys = [c for c in scan.partial_concepts if c.address in target_components]
    if not partial_keys:
        return scan
    # The summary's grain keys are partial (a `~key` join column); complete each
    # against its authoritative dimension and INNER-join, so the filtered scan
    # selects exactly the surviving keys (a LEFT/FULL join would leak keys absent
    # from the filtered aggregate with a NULL count).
    key_node = plan_source(
        SourceRequest(
            outputs=partial_keys,
            environment=environment,
            graph=request.graph,
            history=request.history,
            conditions=None,
            depth=request.depth + 1,
            source_policy=request.source_policy,
        )
    )
    if key_node is None:
        return None
    inputs = unique(
        [c for parent in (scan, key_node) for c in parent.usable_outputs],
        "address",
    )
    return MergeNode(
        input_concepts=inputs,
        output_concepts=outputs,
        environment=environment,
        parents=[scan, key_node],
        depth=request.depth,
        force_join_type=JoinType.INNER,
    )


def _plan_coalescing_axis(request: SourceRequest) -> StrategyNode | None:
    """Bare projection of a coalescing (`full`/`union`) axis: the unified axis
    is the union of member domains, so no single member's scan may satisfy it —
    assemble the mandatory coalesce of every member side (v3's
    `gen_coalescing_axis_node`, reused with a v4 recursion adapter for unbound
    members).

    Deliberately narrow, mirroring the v3 trigger: fires only when EVERY
    requested concept (outputs and filter columns alike) is a key of one
    coalescing group. A request carrying any other column is querying a side or
    already forces the member scans into the bridge, where probe pinning and
    partial-driven join typing assemble the axis population natively."""
    env = request.environment
    requested = _requested_concepts(request)
    canonicals: set[str] = set()
    allowed: set[str] = set()
    for concept in requested:
        found = coalescing_axis_group(concept.address, env)
        if found is None:
            return None
        canonical, group = found
        canonicals.add(canonical)
        allowed |= {canonical, *group}
    if len(canonicals) != 1 or any(c.address not in allowed for c in requested):
        return None

    from trilogy.core.processing.concept_strategies_v4 import (
        V4History,
        search_concepts,
    )

    def _v4_member_source(
        mandatory_list: list[BuildConcept],
        environment: BuildEnvironment,
        g: ReferenceGraph,
        depth: int,
        history: History,
        conditions: BuildWhereClause | None = None,
    ) -> StrategyNode | None:
        info = search_concepts(
            mandatory_list=mandatory_list,
            history=cast(V4History, history),
            environment=environment,
            depth=depth,
            g=g,
            conditions=[conditions] if conditions else [],
        )
        return info.strategy_node

    key_concept = env.concepts.get(next(iter(canonicals)))
    if key_concept is None:
        return None
    axis = gen_coalescing_axis_node(
        key_concept,
        env,
        request.depth + 1,
        g=request.graph,
        source_concepts=_v4_member_source,
        history=request.history,
    )
    if axis is None or request.conditions is None:
        return axis
    return SelectNode(
        output_concepts=list(axis.output_concepts),
        input_concepts=list(axis.output_concepts),
        environment=env,
        parents=[axis],
        conditions=request.conditions.conditional,
    )


def plan_source(request: SourceRequest) -> StrategyNode | None:
    """Source ROOT-level concepts through one v4 path.

    First try the ordinary datasource component planner. If the requested roots
    are split but the graph can prove connector concepts, source each expanded
    component directly and merge them under a v4 node.
    """
    axis = _plan_coalescing_axis(request)
    if axis is not None:
        return axis
    complete_where = _plan_complete_where_source(request)
    if complete_where is not None:
        return complete_where
    pinned_rollup = _plan_finer_filter_rollup(request)
    if pinned_rollup is not None:
        return pinned_rollup
    requested_addresses = {c.address for c in _requested_concepts(request)}
    fallback_bridge: tuple[BridgePlan, SourceAttempt] | None = None
    for attempt in request.source_policy.attempts:
        bridge_plan = _bridge_plan(request, attempt)
        if bridge_plan is not None and bridge_plan.full_cover_fallback:
            # A no-new-concepts multi-source join is a dead-last resort: any
            # plan another path can produce (a direct source at some attempt,
            # the unconditioned retry) wins first — see the fallback block at
            # the bottom.
            if fallback_bridge is None:
                fallback_bridge = (bridge_plan, attempt)
            bridge_plan = None
        if bridge_plan is not None:
            parents = _datasource_nodes_for_bridge(request, bridge_plan, attempt)
            if parents is not None and (
                attempt.accepts_partial
                or _bridge_parents_cover(parents, requested_addresses)
            ):
                merged = _merge_component_sources(
                    request, parents, bridge_plan.concepts
                )
                if (
                    merged is not None
                    and not attempt.accepts_partial
                    and request.complete_partials
                ):
                    merged = _complete_partial_requested(request, merged)
                if merged is not None:
                    return merged
        direct = _direct_source(request, attempt)
        if direct is not None:
            return direct
    if request.conditions is not None:
        outputs = unique(
            request.outputs + _condition_row_concepts(request.conditions),
            "address",
        )
        unfiltered = plan_source(
            SourceRequest(
                outputs=outputs,
                environment=request.environment,
                graph=request.graph,
                history=request.history,
                conditions=None,
                depth=request.depth,
                source_policy=request.source_policy,
            )
        )
        if unfiltered is not None:
            return SelectNode(
                output_concepts=request.outputs,
                input_concepts=unfiltered.output_concepts,
                environment=request.environment,
                parents=[unfiltered],
                conditions=request.conditions.conditional,
            )
    # Dead-last: the no-new-concepts multi-source join. Rescues a request no
    # single source covers and no connector expansion reaches (the two-rowset
    # body's `st as store` under `merge st into store_id`, where the joined
    # key was itself requested) without pre-empting any plan the paths above
    # can produce.
    if fallback_bridge is not None:
        plan, attempt = fallback_bridge
        parents = _datasource_nodes_for_bridge(request, plan, attempt)
        if parents is not None and (
            attempt.accepts_partial
            or _bridge_parents_cover(parents, requested_addresses)
        ):
            merged = _merge_component_sources(request, parents, plan.concepts)
            if (
                merged is not None
                and not attempt.accepts_partial
                and request.complete_partials
            ):
                merged = _complete_partial_requested(request, merged)
            if merged is not None:
                return merged
    return None
