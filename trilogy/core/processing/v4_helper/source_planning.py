"""Root datasource planning.

Handles the case where a set of sourced concepts needs connector concepts added
before datasource components can be merged. The connector search still reuses the graph Steiner helper for
now, but component sourcing and final assembly stay in the v4 root planner.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import cast

from trilogy.constants import logger
from trilogy.core.enums import Derivation, Granularity, JoinType
from trilogy.core.graph_models import ReferenceGraph, concept_to_node
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
from trilogy.core.processing.model_ambiguity import validate_relation_paths
from trilogy.core.processing.node_generators.common import (
    inject_authored_join_key_terminals,
    reinject_common_join_keys_v2,
)
from trilogy.core.processing.node_generators.presence_probe import (
    coalescing_axis_group,
    gen_coalescing_axis_node,
    is_presence_probe,
    member_binding_datasources,
    probe_member_address,
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
from trilogy.core.processing.v4_helper.functional_dependency import build_fd_closure
from trilogy.core.processing.v4_helper.history import V4History
from trilogy.core.processing.v4_helper.network_build import (
    build_source_network,
    connector_join_keys,
)
from trilogy.core.processing.v4_helper.network_model import (
    CONNECTOR_NODE_PREFIX,
    SearchResult,
    SourceNetwork,
)
from trilogy.core.processing.v4_helper.network_search import search_sources
from trilogy.utility import unique


@dataclass(frozen=True)
class SourceRequest:
    outputs: list[BuildConcept]
    environment: BuildEnvironment
    graph: ReferenceGraph
    history: History
    conditions: BuildWhereClause | None = None
    depth: int = 0
    # The ONE partiality constraint left in v4: this request's answer must
    # bind every requested output FULLY. Set only by the partial-completion
    # sub-call — completing a partial output with another partial read would
    # complete nothing. Everywhere else the search prices partiality per
    # binding and no outer mode exists.
    require_full: bool = False
    # False inside a partial-completion sub-call, so completing a partial output
    # cannot re-enter `_complete_partial_requested` on itself (infinite loop when
    # the concept has no complete source).
    complete_partials: bool = True


@dataclass(frozen=True)
class NetworkDecision:
    """What the network search concluded for a request.

    `None` from `_network_source` means the search DECLINED — it found no
    solution, or found one the emitter cannot express. A decision with
    `bridge=None` is not a decline: the search succeeded and the answer is a
    single scan, which `_direct_source` renders (design §4). Keeping the two
    apart is what lets the ladder go: a decline needs another home, a
    single-scan does not."""

    bridge: BridgePlan | None


@dataclass(frozen=True)
class BridgePlan:
    concepts: list[BuildConcept]
    graph: ReferenceGraph
    # Connector aliases the network search CHOSE as join hops. Address coverage
    # must not veto planning these: a merged key's surviving address is bound by
    # the dimension scan and its input keys by the fact scan, so every address
    # looks covered — yet the two scans share no column and only the connector's
    # subplan (e.g. a merged-unnest bridge) can relate them.
    connector_aliases: tuple[str, ...] = ()


def _concept_node_address(node: str) -> str:
    return node.split("~", maxsplit=1)[1].split("@", maxsplit=1)[0]


def _graph_neighbors(graph: ReferenceGraph, node: str) -> set[str]:
    return set(graph.predecessors(node)) | set(graph.successors(node))


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


def _single_source_covers(requested: set[str], environment: BuildEnvironment) -> bool:
    """Some datasource binds every requested address by itself, COMPLETELY.

    Completeness is the whole condition: a `partial`/`complete where` source
    covering the request is one arm of an answer, not the answer, and the grain
    keys are what carry the other arms in (`channel_dim_text_id` over a WEB and
    a CATALOG partial). A partial binding of a single column is the same story
    at column scope.
    """
    for datasource in environment.datasources.values():
        if not isinstance(datasource, BuildDatasource):
            continue
        if datasource.non_partial_for is not None:
            continue
        if requested & {c.address for c in datasource.partial_concepts}:
            continue
        if requested <= {c.address for c in datasource.output_concepts}:
            return True
    return False


def _concepts_with_grain_keys(
    concepts: list[BuildConcept],
    environment: BuildEnvironment,
) -> list[BuildConcept]:
    expanded: list[BuildConcept] = []
    requested_addresses = {concept.address for concept in concepts}
    # A grain key is expanded so the request can REACH a concept living on
    # another source -- the key is the join spine. When one datasource already
    # binds every requested address the cover joins nothing, so no spine is
    # needed and demanding the key only forces in a bridge chain whose columns
    # nothing reads (gcat: a summary binding `org.state_code`/`org.hex` but
    # not `org.code` picked up `launch_info` + `organizations`). Whenever a join
    # IS in play the key stays a terminal: dropping it there does not degrade to
    # a connector, it re-picks the source and pairs on properties instead
    # (tpc_ds aggregates q03).
    keys_are_affordances = _single_source_covers(requested_addresses, environment)
    # A requested aggregate pins the population at its own grain: its axis
    # members join BY THEMSELVES, so their authored host-row keys are not
    # requirements of the request. Expanding them would demand the finer key
    # and force the raw table into a cover that a materialized rollup source
    # answers alone (daily_fact: `select ride_year, ride_month, total_rides`
    # dragging `ride_id` in).
    aggregate_axes: set[str] = set()
    for concept in concepts:
        if concept.derivation == Derivation.AGGREGATE:
            aggregate_axes.update(concept.grain.components)
    for concept in concepts:
        expanded.append(concept)
        # A coalescing (`full`/`union` join) axis canonical inherits the
        # SURVIVING arm's grain, but the unified axis spans every arm's domain:
        # expanding that grain would drag the surviving arm's row key into an
        # arm-scoped request and force the other arm into its cover (a second
        # assembly axis where the final merge already coalesces the arms).
        axis = coalescing_axis_group(concept.address, environment)
        if axis is not None and axis[0] == concept.address:
            continue
        if concept.address in aggregate_axes:
            continue
        for address in concept.grain.components:
            if address not in environment.concepts:
                continue
            if keys_are_affordances and address not in requested_addresses:
                continue
            expanded.append(environment.concepts[address])
    return unique(expanded, "address")


def _direct_source(request: SourceRequest, accept_partial: bool) -> StrategyNode | None:
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
        accept_partial=accept_partial,
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
    concepts = _concepts_with_grain_keys(
        inject_authored_join_key_terminals(
            unique(
                _requested_concepts(request) + _condition_arg_lineage_roots(request),
                "address",
            ),
            request.environment,
        ),
        request.environment,
    )
    # Static model-path validation, BEFORE any source search: an ambiguous
    # relation is a model/request defect the search must never arbitrate.
    validate_relation_paths(request.environment, concepts)
    return concepts


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


def _memoized_search(network: SourceNetwork, history: History) -> SearchResult:
    """The search, memoized for this build request on the network's structural
    signature. The ROOT planner re-asks the same question several times per
    query — the same terminals reached through the condition retry, the
    partial-completion sub-call and the single-scan re-ask — and the search is
    the dominant cost of v4 generation. Nothing build-scoped is stored: the key
    is addresses and node names, the value node names and integers."""
    if not isinstance(history, V4History):
        return search_sources(network)
    key = network.signature()
    cached = history.search_cache.get(key)
    if cached is None:
        cached = search_sources(network)
        history.search_cache[key] = cached
    return cached


def _report_truncation(network: SourceNetwork, result: SearchResult) -> None:
    """A search that ran out of budget must not read as a considered verdict.

    With a solution in hand the plan is valid but need not be cost-minimal.
    Without one the search DECLINED FOR LACK OF BUDGET, which is not the same
    claim as "no solution exists" — yet `plan_source` falls through to
    `_direct_source` and the unconditioned retry identically for both. Until
    branch-and-bound removes the truncation, saying so is the difference between
    a known limitation and a silent one."""
    logger.warning(
        "[v4] source search hit %s over %d candidates for terminals %s: %s",
        result.limit.value if result.limit else "no limit",
        len(network.candidates),
        ",".join(network.terminals),
        (
            "no solution emitted — falling through to the single-scan planners, "
            "which is a guess, not evidence that none exists"
            if result.exhausted
            else "solution kept but it may not be cost-minimal"
        ),
    )


def _network_source(
    request: SourceRequest, defer_single_scan: bool = True
) -> NetworkDecision | None:
    """Stage D, as an adapter: let the network search PICK the sources, and keep the
    existing bridge machinery as the emitter.

    The solution is expressed as a `BridgePlan` — a graph holding exactly the chosen
    datasources plus the concept nodes it binds — so `_datasource_nodes_for_bridge`,
    `_merge_component_sources` and `_complete_partial_requested` are unchanged and every
    §5 carry-over they implement stays in force. That keeps the cutover diff to "who
    chose the sources", which is the only thing under test.
    """
    concepts = _search_concepts_for_bridge(request)
    v4_history = request.history if isinstance(request.history, V4History) else None
    verdict_key: tuple[str, str, bool] | None = None
    if v4_history is not None:
        verdict_key = (
            "-".join(sorted(c.address for c in concepts)),
            str(request.conditions),
            defer_single_scan,
        )
        cached_verdict = v4_history.network_verdicts.get(verdict_key)
        if cached_verdict == "none":
            return None
        if cached_verdict == "defer":
            return NetworkDecision(bridge=None)
    network = build_source_network(
        concepts, request.environment, request.graph, request.conditions
    )
    result = _memoized_search(network, request.history)
    if result.truncated:
        _report_truncation(network, result)
    if result.split:
        # A proof, not a budget: no join-component of the candidate pool holds
        # binders for every terminal, so no connected cover exists and the
        # fall-through is evidence-based — contrast _report_truncation.
        logger.info(
            "[v4] source search declined: terminals %s share no join-component "
            "with the rest of the request — no connected cover exists; falling "
            "through to the single-scan planners",
            ",".join(sorted(result.split)),
        )
    if result.solution is None:
        if v4_history is not None and verdict_key is not None:
            v4_history.network_verdicts[verdict_key] = "none"
        return None
    if (
        defer_single_scan
        and len(result.solution.sources) == 1
        and not network.candidates[result.solution.sources[0]].is_union
        and not any(
            binding.injected
            for binding in network.candidates[
                result.solution.sources[0]
            ].bindings.values()
        )
    ):
        # A one-scan solution is `_direct_source`'s job (design §4): it is the
        # renderer for a single assignment and knows the grain-aware scoring and
        # the force-group the bridge emitter has no reason to apply. Routing it
        # through the bridge drops the GROUP BY that collapses a scan at finer
        # grain than the request. A union stays here — `_direct_source` cannot
        # render one. A solution leaning on an INJECTED binding (a pinned probe
        # the graph never offered) is the exception: `_direct_source`'s
        # graph-scored select cannot see it, only the bridge emitter's
        # `_datasource_renders_probe` path renders it.
        if v4_history is not None and verdict_key is not None:
            v4_history.network_verdicts[verdict_key] = "defer"
        return NetworkDecision(bridge=None)
    graph = request.graph.copy()
    # The network mints union candidates itself and names them with the same
    # convention, so injecting here makes its chosen node addresses resolvable.
    _inject_union_datasources(graph, concepts, request.environment)
    chosen = set(result.solution.sources)
    # A derived-connector choice (`connector~<alias>`) is not a scan: its alias
    # concept rides in `plan.concepts` and `_derived_connector_nodes` plans the
    # `alias_origin_lookup` origin as a parent. Only real scans face the
    # datasource-registry checks.
    connector_aliases = sorted(
        node.removeprefix(CONNECTOR_NODE_PREFIX)
        for node in chosen
        if node.startswith(CONNECTOR_NODE_PREFIX)
    )
    chosen -= {f"{CONNECTOR_NODE_PREFIX}{alias}" for alias in connector_aliases}
    if not chosen <= set(graph.datasources):
        if v4_history is not None and verdict_key is not None:
            v4_history.network_verdicts[verdict_key] = "none"
        return None
    for node in [n for n in graph.datasources if n not in chosen]:
        del graph.datasources[node]
    keep_addresses = set(network.terminals) | result.solution.connectors
    bridge_concepts = [
        concept
        for concept in concepts
        if network.equivalence.get(concept.address, concept.address) in keep_addresses
    ]
    for address in sorted(keep_addresses):
        if any(concept.address == address for concept in bridge_concepts):
            continue
        connector = request.environment.concepts.get(address)
        if connector is not None:
            bridge_concepts.append(connector)
            continue
        # A connector labeled by a canonical (`_virt_*`) address is a derived
        # merge key: each side of the declared equality owns one variant, known
        # only to `canonical_concepts` (the demoted side's real lineage lives in
        # `alias_origin_lookup`). Carry, per CHOSEN source, the concept behind
        # the binding that source actually reads — its own side's variant — so
        # each scan materializes its side and the merge joins them on the
        # pseudonym equivalence (the `renders_derived_key` contract downstream).
        # Never the whole equivalence class: an unread member (gcat's
        # `first_org`, a second declared alias for the same key) would hand the
        # join a column the authored FK already provides, changing the join.
        for node in result.solution.sources:
            binding = network.candidates[node].bindings.get(address)
            if binding is None:
                continue
            resolved = request.environment.concepts.get(
                binding.address
            ) or request.environment.canonical_concepts.get(binding.address)
            if resolved is not None and not any(
                concept.address == resolved.address for concept in bridge_concepts
            ):
                bridge_concepts.append(resolved)
    for alias in connector_aliases:
        concept = request.environment.concepts.get(alias)
        if concept is not None and not any(
            existing.address == alias for existing in bridge_concepts
        ):
            bridge_concepts.append(concept)
    # A requested concept the bridge cannot carry means the bridge is not an
    # answer to THIS request, however good its source pick is: a single-row `<*>`
    # watermark (excluded from the search because it joins by cross product, and
    # the bridge emitter has no cross join) or a rowset output with no backing
    # scan both render as INVALID_REFERENCE. `_direct_source` handles them, and
    # the ladder reaches the same conclusion through its own
    # `requested <= bridged_addresses` gate.
    #
    # No carve-out for the terminals the search drops as DECOMPOSABLE, tempting
    # as it is: "the emitter can compute this inline" is a claim about lineage,
    # and it does not survive a parent that is itself only reachable through a
    # rowset body. Measured — exempting them costs `test_rowset_shape` and buys
    # one request back from the ladder.
    bridged_addresses = {concept.address for concept in bridge_concepts}
    if not {c.address for c in _requested_concepts(request)} <= bridged_addresses:
        return NetworkDecision(bridge=None)

    # Keep concept nodes by ADDRESS, not by `concept_to_node`: a graph concept node
    # carries an `@grain` suffix that need not match the default-grain spelling, and
    # deleting the graph's own node severs the datasource edge that binds it.
    def _keep(node: str) -> bool:
        if node in chosen:
            return True
        if not node.startswith("c~"):
            return False
        address = _concept_node_address(node)
        # `keep_addresses` is in equivalence-class terms; a graph node may spell a
        # non-canonical member of the same class.
        return network.equivalence.get(address, address) in keep_addresses

    # Drop by node rather than taking a subgraph view: the emitter re-points missing
    # datasources into `plan.graph.datasources`, which a frozen view rejects.
    graph.remove_reference_nodes([n for n in list(graph.nodes) if not _keep(n)])
    if not graph.datasources:
        return None
    # §5 carry-over. The ladder gets this from inside `determine_induced_minimal_nodes`,
    # which this path bypasses: a key two chosen sources share must be an edge on BOTH
    # of them, or the side that never materializes its own member drops out of the
    # merge join's equality and the join degrades to the keys that remain (the q05
    # fan-out — `s.return_channel_dim_id` falling out left `cheerful` joined on
    # `s.channel` alone).
    synonyms: dict[str, str] = {}
    for concept in bridge_concepts:
        for pseudonym in concept.pseudonyms:
            synonyms[pseudonym] = concept.address
    reinject_common_join_keys_v2(request.graph, graph, synonyms)
    return NetworkDecision(
        bridge=BridgePlan(
            concepts=bridge_concepts,
            graph=graph,
            connector_aliases=tuple(connector_aliases),
        )
    )


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
    accept_partial: bool,
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
            accept_partial=accept_partial,
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
    if accept_partial and not request.complete_partials:
        parents = _drop_redundant_subset_sources(parents)
    parents.extend(_derived_connector_nodes(request, plan, parents))
    if not parents:
        return None
    return parents


def _drop_redundant_subset_sources(
    parents: list[StrategyNode],
) -> list[StrategyNode]:
    """Drop a permissive bridge source wholly covered by one richer source.

    The caller limits this to aggregate fact-population searches. A key-only
    dimension cannot connect anything the richer fact source does not already
    connect, and completing its domain would add groups outside the aggregate's
    observed population.
    """
    kept: list[StrategyNode] = []
    for index, parent in enumerate(parents):
        outputs = {concept.address for concept in parent.usable_outputs}
        if outputs and any(
            outputs <= {concept.address for concept in other.usable_outputs}
            for other_index, other in enumerate(parents)
            if other_index != index
        ):
            continue
        kept.append(parent)
    return kept


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
    forced = set(plan.connector_aliases)
    if not forced and {c.address for c in plan.concepts} <= covered:
        return []
    # Imported lazily: `concept_strategies_v4` imports this module's package.
    from trilogy.core.processing.concept_strategies_v4 import search_concepts

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
                or (origin.address in covered and alias not in forced)
                or origin.address in history.connectors_in_progress
            ):
                continue
            # Carry the connector's grain keys (e.g. a recursion keyed by `id`
            # must emit `id`, not group it away) so the merge can join the
            # connector back to the consumer on that key. The key must be emitted
            # even when another parent already covers it — it IS the join column;
            # dropping it leaves the merge with no shared key and a 1=1 cross join
            # (hackernews: the recursion's `id` is also the post scan's `id`).
            # An uncovered bridge concept FD-riding the connector's grain must
            # ride the connector too: the datasource gap-fill stands down for
            # non-BASIC merge bridges, so no raw scan will ever supply it
            # (window-key join: `orders.amt` rides the rank connector at oid
            # grain, so the window computes inline on the amt-carrying scan).
            # A connector whose grain offers no axis beyond the merged key emits
            # its input keys instead (see `connector_join_keys`) — the same
            # contract `_connector_candidates` bound when the search picked it.
            # Without them the subplan joins the consumer only on the key it
            # exists to relate, which is no join at all.
            grain_components = set(origin.grain.components) | connector_join_keys(
                alias, origin
            )
            carried = [
                c
                for c in plan.concepts
                if c.address not in covered
                and c.address != origin.address
                and c.address not in grain_components
                and c.grain.components
                and set(c.grain.components) <= grain_components
            ]
            mandatory = unique(
                [origin]
                + [
                    env.concepts[address]
                    for address in sorted(grain_components)
                    if address in env.concepts
                ]
                + carried,
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


def _datasource_rolls_up_to(
    datasource: BuildDatasource | BuildUnionDatasource | None,
    concept: BuildConcept,
    environment: BuildEnvironment,
) -> bool:
    """`datasource` binds an additive aggregate that SUM-rolls up to `concept` at
    `concept`'s own grain — the anonymous-alias analogue of binding it outright.

    A named metric reaches its summary table because query and column share an
    address; an agent-authored `sum(x) as total` shares an address with nothing,
    and its grain-pinned canonical differs from the column's, so only the
    lineage-signature rollup check can relate them."""
    if not isinstance(datasource, BuildDatasource):
        return False
    if not (concept.is_aggregate and _is_additive_aggregate(concept)):
        return False
    return bool(
        get_additive_rollup_concepts(
            datasource=datasource,
            requested_concepts=[concept],
            concepts_by_address=environment.concepts,
            datasources=[
                ds
                for ds in environment.datasources.values()
                if isinstance(ds, BuildDatasource)
            ],
            target_grain=concept.grain,
        )
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
    # `canonical_concepts` keeps one concept per canonical address, so a query
    # alias and an identically-derived named metric (`sum(x) as total` beside a
    # declared `total_x`) collide there and the winner may not be the address the
    # request asked for. Match the aggregate arm below on the canonical instead.
    bridge_canonicals = {
        concept.canonical_address
        for address in bridge_addresses
        if (concept := environment.concepts.get(address)) is not None
    }
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
                and (
                    canonical.address in bridge_addresses
                    or address in bridge_canonicals
                )
                and datasource is not None
                and (
                    _datasource_can_output(datasource, canonical.address)
                    # ...or it binds a finer additive aggregate that rolls up to
                    # it, which is how an anonymous alias reaches a summary table.
                    or _datasource_rolls_up_to(datasource, canonical, environment)
                )
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
    domain, so it is flagged partial and the final-output guard rejects it.
    Complete such a key against its dimension source (`lv_info`) and join. If no
    *complete* source exists the node is left
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
            require_full=True,
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
    # kept -- the `lv_info LEFT JOIN launch_info` shape.
    return MergeNode(
        input_concepts=inputs,
        output_concepts=node.output_concepts,
        environment=request.environment,
        parents=[completion, node],
        depth=request.depth,
    )


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
            require_full=request.require_full,
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
    assemble the mandatory coalesce of every member side.

    Deliberately narrow: fires only when EVERY
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

    # Imported lazily: `concept_strategies_v4` imports this module's package.
    from trilogy.core.processing.concept_strategies_v4 import search_concepts

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


def _uf_find(parent: dict[str, str], node: str) -> str:
    while parent.get(node, node) != node:
        parent[node] = parent.get(parent[node], parent[node])
        node = parent[node]
    return node


def _uf_union(parent: dict[str, str], left: str, right: str) -> None:
    a, b = _uf_find(parent, left), _uf_find(parent, right)
    if a != b:
        lo, hi = sorted((a, b))
        parent[hi] = lo


def _terminal_components(network) -> list[set[str]]:
    """Terminals grouped by join-connectivity of the candidates binding them.
    Empty when some terminal has no binder at all."""
    parent: dict[str, str] = {node: node for node in network.candidates}
    nodes = sorted(network.candidates)
    for index, left in enumerate(nodes):
        for right in nodes[index + 1 :]:
            if network.join_keys(left, right):
                _uf_union(parent, left, right)
    for address in network.terminals:
        binders = network.binders(address)
        for other in binders[1:]:
            _uf_union(parent, binders[0], other)
    groups: dict[str, set[str]] = {}
    for address in network.terminals:
        binders = network.binders(address)
        if not binders:
            return []
        groups.setdefault(_uf_find(parent, binders[0]), set()).add(address)
    return [groups[key] for key in sorted(groups)]


def _lineage_connected(graph: ReferenceGraph, outputs: list[BuildConcept]) -> bool:
    """Every output is reachable from every other in the reference graph,
    lineage edges included. This is the ladder's real criterion for assembling
    a join-disconnected request: the Steiner walk crossed component boundaries
    exactly when a derivation's lineage related them (`overall <- sum(samt) +
    sum(wamt)`), and failed loudly when nothing did — the q75/q64/q35 correct
    disconnects, which must stay errors."""
    targets: list[set[str]] = []
    for concept in outputs:
        matches = {
            node
            for node in graph.nodes
            if node.startswith("c~")
            and _concept_node_address(node)
            in (concept.address, concept.canonical_address)
        }
        if not matches:
            return False
        targets.append(matches)
    frontier = list(targets[0])
    seen = set(frontier)
    while frontier:
        current = frontier.pop()
        for neighbor in _graph_neighbors(graph, current):
            if neighbor not in seen:
                seen.add(neighbor)
                frontier.append(neighbor)
    return all(matches & seen for matches in targets)


def _cross_component_source(request: SourceRequest) -> StrategyNode | None:
    """Assemble a lineage-related but join-disconnected request from its
    components — the `sum(samt) + sum(wamt)` scalar shape: two facts related
    only through a derived expression's lineage, with no key to join on. The
    ladder's Steiner walk answered these by scanning each component and letting
    the aggregate machinery collapse each side before a single-row cross join
    (design §0.1: a disconnected cover is a CROSS PRODUCT, and the merge
    renders exactly that); the network search correctly refuses to call such a
    cover JOINED, so the assembly lives here as an explicit fallback.

    Gated on `_lineage_connected`, which separates "the query itself relates
    these components" from "nothing does" — the latter must stay a loud
    disconnect error."""
    if len(request.outputs) < 2:
        return None
    concepts = _search_concepts_for_bridge(request)
    network = build_source_network(
        concepts, request.environment, request.graph, request.conditions
    )
    groups = _terminal_components(network)
    if len(groups) < 2:
        return None
    rep_to_group: dict[str, int] = {}
    for index, group in enumerate(groups):
        for address in group:
            rep_to_group[address] = index
    grouped: dict[int, list[BuildConcept]] = {}
    for concept in request.outputs:
        representative = network.equivalence.get(concept.address, concept.address)
        group_index = rep_to_group.get(representative)
        if group_index is None:
            return None
        grouped.setdefault(group_index, []).append(concept)
    if len(grouped) < 2:
        return None
    if not _lineage_connected(request.graph, request.outputs):
        return None
    # Lineage-connected is not enough to license a CROSS PRODUCT. The shape this
    # exists for (`sum(samt) + sum(wamt)`) relates two components only through a
    # derived expression: neither side's rows are identified by the other, so
    # each collapses to a scalar and the cross join is the answer. When one
    # component's concepts are FD-determined by another's, the components have a
    # real key relationship and a JOIN on it is mandatory: crossing them
    # multiplies rows instead (a PERSIST of `split` alone drops the `scalar` it
    # is keyed by, and `select split, scalar` then paired every split with every
    # scalar). Refuse, so the caller reports a disconnect rather than silently
    # returning a cartesian.
    component_addresses = [
        frozenset(concept.address for concept in members)
        for members in grouped.values()
    ]
    for index, addresses in enumerate(component_addresses):
        closure = build_fd_closure(
            request.environment, addresses, include_empty_grain=False
        )
        for other_index, other in enumerate(component_addresses):
            if other_index != index and closure & other:
                return None
    parents: list[StrategyNode] = []
    for index in sorted(grouped):
        component = plan_source(
            SourceRequest(
                outputs=grouped[index],
                environment=request.environment,
                graph=request.graph,
                history=request.history,
                conditions=None,
                depth=request.depth + 1,
                require_full=request.require_full,
                complete_partials=request.complete_partials,
            )
        )
        if component is None:
            return None
        parents.append(component)
    return _merge_component_sources(request, parents)


def _emit_bridge(request: SourceRequest, bridge: BridgePlan) -> StrategyNode | None:
    # The search already priced partiality, so there is no escalation to do:
    # render at the request's own permissiveness and let the solution speak.
    parents = _datasource_nodes_for_bridge(request, bridge, not request.require_full)
    if parents is None:
        return None
    merged = _merge_component_sources(request, parents, bridge.concepts)
    if merged is not None and request.complete_partials:
        merged = _complete_partial_requested(request, merged)
    return merged


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
    decision = _network_source(request)
    if decision is not None and decision.bridge is not None:
        merged = _emit_bridge(request, decision.bridge)
        if merged is not None:
            return merged
    # Either the search answered with a single scan (design §4: `_direct_source`
    # is that solution's renderer), or it declined and this is the last read that
    # might still work. The escalation is only "may this read accept a partial
    # binding" — it is not a re-search.
    for accept_partial in ((False,) if request.require_full else (False, True)):
        direct = _direct_source(request, accept_partial)
        if direct is not None:
            return direct
    if decision is not None and decision.bridge is None:
        # The single-scan solution `_direct_source` could not render — a
        # derived output only the bridge's concept-node assembly computes (the
        # persist-refresh watermark shape, where the ladder likewise answered
        # with a one-datasource bridge). Re-ask for the bridge rendering.
        retry = _network_source(request, defer_single_scan=False)
        if retry is not None and retry.bridge is not None:
            merged = _emit_bridge(request, retry.bridge)
            if merged is not None:
                return merged
    if request.conditions is None:
        crossed = _cross_component_source(request)
        if crossed is not None:
            return crossed
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
                require_full=request.require_full,
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
    return None
