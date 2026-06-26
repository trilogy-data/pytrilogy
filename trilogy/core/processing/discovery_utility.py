from typing import TYPE_CHECKING, List

from trilogy.constants import VIRTUAL_CONCEPT_PREFIX, logger
from trilogy.core.enums import (
    Derivation,
    FunctionType,
    Granularity,
    Purpose,
)
from trilogy.core.exceptions import DisconnectedConceptsException
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildDatasource,
    BuildFunction,
    BuildGrain,
    BuildRowsetItem,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import QueryDatasource, UnnestJoin
from trilogy.core.processing.condition_utility import (
    preserved_non_partial_conditions,
)
from trilogy.core.processing.constants import ROOT_DERIVATIONS
from trilogy.core.processing.grain_utility import (
    _grain_coverage_addresses,
    concept_source_address,
)
from trilogy.core.processing.nodes import (
    GroupNode,
    MergeNode,
    MultiSelectMergeNode,
    StrategyNode,
)
from trilogy.core.processing.utility import GroupRequiredResponse
from trilogy.utility import unique

if TYPE_CHECKING:
    from trilogy.core.graph_models import ReferenceGraph


def depth_to_prefix(depth: int) -> str:
    return "\t" * depth


NO_PUSHDOWN_DERIVATIONS: list[Derivation] = ROOT_DERIVATIONS + [
    Derivation.BASIC,
    Derivation.ROWSET,
    Derivation.UNNEST,
]


LOGGER_PREFIX = "[DISCOVERY LOOP]"


def calculate_effective_parent_grain(
    node: QueryDatasource | BuildDatasource,
) -> BuildGrain:
    # calculate the effective grain of the parent node
    # this is the union of all parent grains
    if isinstance(node, QueryDatasource):
        if node.group_required:
            return node.grain
        grain = BuildGrain()
        qds = node
        if not qds.joins:
            base = qds.base_datasource
            if base is not None:
                return base.grain
            return qds.datasources[0].grain
        seen = set()
        for join in qds.joins:
            if isinstance(join, UnnestJoin):
                grain += BuildGrain(components=set([x.address for x in join.concepts]))
                continue
            pairs = join.concept_pairs or []
            for key in pairs:
                left = key.existing_datasource
                logger.debug(f"adding left grain {left.grain} for join key {key.left}")
                grain += left.grain
                seen.add(left.name)
            keys = [key.right for key in pairs]
            join_grain = BuildGrain.from_concepts(keys)
            if join_grain == join.right_datasource.grain:
                logger.debug(f"irrelevant right join {join}, does not change grain")
            else:
                logger.debug(
                    f"join changes grain, adding {join.right_datasource.grain} to {grain}"
                )
                grain += join.right_datasource.grain
            seen.add(join.right_datasource.name)
        for x in qds.datasources:
            # if we haven't seen it, it's still contributing to grain
            # unless used ONLY in a subselect
            # so the existence check is a [bad] proxy for that
            if x.name not in seen and not (
                qds.condition
                and qds.condition.existence_arguments
                and any(
                    [
                        c.address in block
                        for c in x.output_concepts
                        for block in qds.condition.existence_arguments
                    ]
                )
            ):
                logger.debug(f"adding unjoined grain {x.grain} for datasource {x.name}")
                grain += x.grain
        return grain
    else:
        return node.grain or BuildGrain()


def check_if_group_required(
    downstream_concepts: List[BuildConcept],
    parents: list[QueryDatasource | BuildDatasource],
    environment: BuildEnvironment,
    depth: int = 0,
) -> GroupRequiredResponse:
    padding = "\t" * depth
    target_grain = BuildGrain.from_concepts(
        downstream_concepts,
        environment=environment,
    )

    comp_grain = BuildGrain()
    for source in parents:
        comp_grain += calculate_effective_parent_grain(source)

    # dynamically select if we need to group
    # we must avoid grouping if we are already at grain
    if comp_grain.abstract and not target_grain.abstract:
        logger.info(
            f"{padding}{LOGGER_PREFIX} Group requirement check: upstream grain is abstract, cannot determine grouping requirement, assuming group required"
        )
        return GroupRequiredResponse(target_grain, comp_grain, True)
    if comp_grain.issubset(target_grain):

        logger.info(
            f"{padding}{LOGGER_PREFIX} Group requirement check:  {comp_grain}, target: {target_grain}, grain is subset of target, no group node required"
        )
        return GroupRequiredResponse(target_grain, comp_grain, False)
    # Expand target via concept-coverage so a MULTISELECT align identity
    # covers its source keys (e.g. local.customer_id covers customer.id and
    # store_sales.customer.id), and a comp_grain that arrives carrying the
    # source keys does not look like extra grain. When *every* target
    # component is itself an aggregate (e.g. TPC-H Q13 distribution shape:
    # ``select count(x) by Y -> per_y, count(Y) -> dist``), exclude aggregate
    # by-keys from coverage: there is no non-aggregate concept anchoring the
    # output to the by-grain, so an upstream at the by-grain is strictly
    # finer and a regroup is required to roll up to the aggregate's grain.
    target_has_only_aggregates = bool(target_grain.components) and all(
        environment.concepts[address].is_aggregate
        for address in target_grain.components
    )
    target_coverage = _grain_coverage_addresses(
        target_grain,
        environment,
        include_aggregate_by_keys=not target_has_only_aggregates,
    )
    if comp_grain.components.issubset(target_coverage):
        logger.info(
            f"{padding}{LOGGER_PREFIX} Group requirement check:  {comp_grain} covered by target coverage {target_coverage}, no group node required"
        )
        return GroupRequiredResponse(target_grain, comp_grain, False)
    # find out what extra is in the comp grain vs target grain
    difference = [
        environment.concepts[c] for c in (comp_grain - target_grain).components
    ]
    logger.info(
        f"{padding}{LOGGER_PREFIX} Group requirement check: upstream grain: {comp_grain}, desired grain: {target_grain} from, difference {[x.address for x in difference]}"
    )
    for x in difference:
        logger.info(
            f"{padding}{LOGGER_PREFIX} Difference concept {x.address} purpose {x.purpose} keys {x.keys}"
        )

    # if the difference is all unique properties whose keys are in the source grain
    # we can also suppress the group
    if difference and all(
        [
            x.keys
            and all(
                environment.concepts[z].address in comp_grain.components for z in x.keys
            )
            for x in difference
        ]
    ):
        logger.info(
            f"{padding}{LOGGER_PREFIX} Group requirement check: skipped due to unique property validation"
        )
        return GroupRequiredResponse(target_grain, comp_grain, False)
    if difference and all([x.purpose == Purpose.KEY for x in difference]):
        logger.info(
            f"{padding}{LOGGER_PREFIX} checking if downstream is unique properties of key"
        )
        replaced_grain_raw: list[set[str]] = [
            (
                x.keys or set()
                if x.purpose == Purpose.UNIQUE_PROPERTY
                else set([x.address])
            )
            for x in downstream_concepts
            if x.address in target_grain.components
        ]
        replaced_grain = [item for sublist in replaced_grain_raw for item in sublist]
        # if the replaced grain is a subset of the comp grain, we can skip the group
        unique_grain_comp = BuildGrain.from_concepts(
            replaced_grain, environment=environment
        )
        if comp_grain.issubset(unique_grain_comp):
            logger.info(
                f"{padding}{LOGGER_PREFIX} Group requirement check: skipped due to unique property validation"
            )
            return GroupRequiredResponse(target_grain, comp_grain, False)
    logger.info(
        f"{padding}{LOGGER_PREFIX} Checking for grain equivalence for filters and rowsets"
    )
    ngrain = []
    for con in target_grain.components:
        full = environment.concepts[con]
        ngrain.append(concept_source_address(full))
    target_grain2 = BuildGrain.from_concepts(
        ngrain,
        environment=environment,
    )
    if comp_grain.issubset(target_grain2):
        logger.info(
            f"{padding}{LOGGER_PREFIX} Group requirement check: {comp_grain}, {target_grain2}, pre rowset grain is subset of target, no group node required"
        )
        return GroupRequiredResponse(target_grain2, comp_grain, False)

    logger.info(f"{padding}{LOGGER_PREFIX} Group requirement check: group required")
    return GroupRequiredResponse(
        target=target_grain, upstream=comp_grain, required=True
    )


def group_if_required_v2(
    root: StrategyNode,
    final: List[BuildConcept],
    environment: BuildEnvironment,
    where_injected: set[str] | None = None,
    depth: int = 0,
):
    where_injected = where_injected or set()
    targets = [
        x
        for x in root.output_concepts
        if x.address in final or any(c in final for c in x.pseudonyms)
    ]
    # A multiselect align outer is a pure FULL JOIN of pre-aggregated arms at the
    # align-key grain and must never regroup: Keyed off the concrete
    # node type (the multiselect generator emits a MultiSelectMergeNode
    if isinstance(root, MultiSelectMergeNode):
        root.set_output_concepts(targets, change_visibility=False)
        return root
    required = check_if_group_required(
        downstream_concepts=final,
        parents=[root.resolve()],
        environment=environment,
        depth=depth,
    )
    if required.required:
        if isinstance(root, MergeNode):
            root.force_group = True
            root.set_output_concepts(targets, rebuild=False, change_visibility=False)
            root.rebuild_cache()
            return root
        elif isinstance(root, GroupNode) and any(
            x.derivation == Derivation.BASIC for x in root.output_concepts
        ):
            # we need to group this one more time
            pass
        elif isinstance(root, GroupNode):
            if set(x.address for x in final) != set(
                x.address for x in root.output_concepts
            ):
                allowed_outputs = [
                    x
                    for x in root.output_concepts
                    if not (
                        x.address in where_injected
                        and x.address not in (root.required_outputs or set())
                    )
                ]
                if where_injected:
                    logger.info(
                        f"Adjusting group node outputs to remove injected concepts {where_injected}: remaining {allowed_outputs}"
                    )
                root.set_output_concepts(allowed_outputs)
            return root
        return GroupNode(
            output_concepts=targets,
            input_concepts=targets,
            environment=environment,
            parents=[root],
            partial_concepts=root.partial_concepts,
            preexisting_conditions=root.preexisting_conditions,
        )
    elif isinstance(root, GroupNode):

        return root
    else:
        root.set_output_concepts(targets, change_visibility=False)
    return root


def get_upstream_concepts(base: BuildConcept, nested: bool = False) -> set[str]:
    return _upstream_concepts(base, nested, {})


def _upstream_concepts(
    base: BuildConcept, nested: bool, cache: dict[int, set[str]]
) -> set[str]:
    # Lineage DAGs are diamond-shaped: the same concept is reached via many
    # paths, so an unmemoized recursion is exponential. BuildConcepts are
    # immutable during resolution, so memoize the (nested=True) result by
    # identity for the lifetime of the top-level call.
    if nested:
        memoized = cache.get(id(base))
        if memoized is not None:
            return memoized
    upstream: set[str] = set()
    if nested:
        upstream.add(base.address)
    if base.lineage:
        for x in base.lineage.concept_arguments:
            # if it's derived from any value in a rowset, ALL rowset items are
            # upstream. use the rowset's already-namespaced derived_concepts
            # rather than splicing `rowset.name` onto the underlying SELECT's
            # addresses, which would produce nonsense like
            # `deduped.local.group_key` and silently miss real upstreams.
            if x.derivation == Derivation.ROWSET:
                assert isinstance(x.lineage, BuildRowsetItem), type(x.lineage)
                upstream.update(x.lineage.rowset.derived_concepts)
            upstream |= _upstream_concepts(x, True, cache)
    if nested:
        cache[id(base)] = upstream
    return upstream


def evaluate_loop_condition_pushdown(
    mandatory: list[BuildConcept],
    conditions: BuildWhereClause | None,
    depth: int,
    force_no_condition_pushdown: bool,
    forced_pushdown: list[BuildConcept],
) -> BuildWhereClause | None:
    # filter evaluation
    # always pass the filter up when we aren't looking at all filter inputs
    # or there are any non-filter complex types
    if not conditions:
        return None
    # first, check if we *have* to push up conditions above complex derivations
    if forced_pushdown:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Force including conditions to push filtering above complex concepts {forced_pushdown} that are not condition row inputs {conditions.row_arguments} or parent"
        )
        return conditions
    # otherwise, only prevent pushdown
    # (forcing local condition evaluation)
    # only if all condition inputs are here and we only have roots
    should_evaluate_filter_on_this_level_not_push_down = all(
        [x.address in mandatory for x in conditions.row_arguments]
    ) and not any(
        [
            x.derivation not in (ROOT_DERIVATIONS)
            for x in mandatory
            if x.address not in conditions.row_arguments
        ]
    )

    if (
        force_no_condition_pushdown
        or should_evaluate_filter_on_this_level_not_push_down
    ):
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Forcing condition evaluation at this level: all basic_no_agg: {should_evaluate_filter_on_this_level_not_push_down}"
        )
        return None

    return conditions


def generate_candidates_restrictive(
    priority_concept: BuildConcept,
    candidates: list[BuildConcept],
    exhausted: set[str],
    # conditions_exist: bool,
) -> list[BuildConcept]:
    unselected_candidates = [
        x for x in candidates if x.address != priority_concept.address
    ]
    local_candidates = [
        x
        for x in unselected_candidates
        if x.address not in exhausted
        and (
            x.granularity != Granularity.SINGLE_ROW
            or x.derivation == Derivation.CONSTANT
        )
        and x.address not in priority_concept.pseudonyms
        and priority_concept.address not in x.pseudonyms
    ]

    # if it's single row, joins are irrelevant. Fetch without keys.
    if priority_concept.granularity == Granularity.SINGLE_ROW:
        logger.info("Have single row concept, including only other single row optional")
        optional = (
            [
                x
                for x in unselected_candidates
                if x.granularity == Granularity.SINGLE_ROW
                and x.address not in priority_concept.pseudonyms
                and priority_concept.address not in x.pseudonyms
            ]
            if priority_concept.derivation == Derivation.AGGREGATE
            else []
        )
        return optional
    return local_candidates


def get_priority_concept(
    all_concepts: List[BuildConcept],
    attempted_addresses: set[str],
    found_concepts: set[str],
    partial_concepts: set[str],
    depth: int,
    environment: BuildEnvironment | None = None,
) -> BuildConcept:
    # optimized search for missing concepts
    all_concepts_local = all_concepts
    pass_one = sorted(
        [
            c
            for c in all_concepts_local
            if c.address not in attempted_addresses
            and (c.address not in found_concepts or c.address in partial_concepts)
        ],
        key=lambda x: x.address,
    )

    priority = (
        # then multiselects to remove them from scope
        [c for c in pass_one if c.derivation == Derivation.MULTISELECT]
        +
        # union TVFs behave like multiselects (self-contained combined sources)
        [c for c in pass_one if c.derivation == Derivation.TVF_UNION]
        +
        # then rowsets to remove them from scope, as they cannot get partials
        [c for c in pass_one if c.derivation == Derivation.UNION]
        # we should be home-free here
        + [c for c in pass_one if c.derivation == Derivation.BASIC]
        +
        # then rowsets to remove them from scope, as they cannot get partials
        [c for c in pass_one if c.derivation == Derivation.ROWSET]
        +
        # then aggregates to remove them from scope, as they cannot get partials
        [c for c in pass_one if c.derivation == Derivation.AGGREGATE]
        # then windows to remove them from scope, as they cannot get partials
        + [c for c in pass_one if c.derivation == Derivation.WINDOW]
        # then filters to remove them from scope, also cannot get partials
        + [c for c in pass_one if c.derivation == Derivation.FILTER]
        # unnests are weird?
        + [c for c in pass_one if c.derivation == Derivation.UNNEST]
        + [c for c in pass_one if c.derivation == Derivation.RECURSIVE]
        + [c for c in pass_one if c.derivation == Derivation.GROUP_TO]
        + [c for c in pass_one if c.derivation == Derivation.SUBSELECT]
        # roots that are abstract
        + [
            c
            for c in pass_one
            if c.derivation == Derivation.ROOT
            and c.granularity == Granularity.SINGLE_ROW
        ]
        # finally our plain selects
        + [
            c
            for c in pass_one
            if c.derivation == Derivation.ROOT
            and c.granularity != Granularity.SINGLE_ROW
        ]  # and any non-single row constants
        + [c for c in pass_one if c.derivation == Derivation.CONSTANT]
    )

    priority += [c for c in pass_one if c.address not in [x.address for x in priority]]
    final = []
    # if any thing is derived from another concept
    # get the derived copy first as this will usually resolve cleaner.
    # get_upstream_concepts(c) depends only on c, so collect the union once
    # rather than recomputing it per (x, c) pair.
    all_upstream: set[str] = set()
    for c in priority:
        all_upstream |= get_upstream_concepts(c)
    for x in priority:
        if x.address in all_upstream:
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} delaying fetch of {x.address} as parent of another concept"
            )
            continue
        final.append(x)
    # then append anything we didn't get
    for x2 in priority:
        if x2 not in final:
            final.append(x2)
    if final:
        return final[0]
    subgraphs = (
        disconnected_components(environment, all_concepts)
        if environment is not None
        else []
    )
    if len(subgraphs) > 1:
        raise DisconnectedConceptsException(
            format_disconnected_subgraphs_error(subgraphs),
            subgraphs=[[c.address for c in group] for group in subgraphs],
        )
    raise DisconnectedConceptsException(
        format_unresolved_concepts_error(all_concepts, found_concepts),
        subgraphs=[[c.address for c in all_concepts]],
    )


def _crossjoinable(concept: BuildConcept) -> bool:
    """Single-row / constant concepts cross-join into any component, so they
    never cause a disconnection (mirrors calculate_graph_relevance)."""
    return (
        concept.granularity == Granularity.SINGLE_ROW
        or concept.derivation == Derivation.CONSTANT
    )


def _anchor_nodes(concept: BuildConcept) -> List[str]:
    """Reference-graph nodes that tie a concept into the model graph: its own
    node, its default-grain node, and its direct source args' default-grain
    nodes (derived concepts may not carry their own node, but their sources do).
    """
    from trilogy.core.graph_models import concept_to_node

    nodes = [
        concept_to_node(concept),
        concept_to_node(concept.with_default_grain()),
    ]
    for arg in concept.concept_arguments:
        if isinstance(arg, BuildConcept):
            nodes.append(concept_to_node(arg.with_default_grain()))
    return nodes


def _aggregate_grain_only_parents(
    environment: BuildEnvironment,
) -> dict[str, set[str]]:
    """Map each aggregate concept address -> the addresses of its grain-only
    ``by`` keys: grain components that are NOT functional inputs of the measure.

    An aggregate can be regrouped to any grain, so an edge from it to its ``by``
    key is not a join relationship and must not bridge otherwise-unconnected
    components. ``add_concept`` adds a graph edge for every ``concept_argument``,
    and an aggregate's ``concept_arguments`` include its ``by`` keys — so without
    this, ``sum(web.measure) by store.county`` would connect the (separate) web
    and store subgraphs through ``store.county``. Mirrors the
    "aggregate up to an arbitrary grain can be joined in later" rule in
    ``calculate_graph_relevance``.
    """
    out: dict[str, set[str]] = {}
    for c in environment.concepts.values():
        if not isinstance(c, BuildConcept):
            continue
        if c.derivation != Derivation.AGGREGATE or not c.grain.components:
            continue
        if isinstance(c.lineage, BuildAggregateWrapper):
            measure = c.lineage.function.concept_arguments
        elif c.lineage is not None:
            measure = c.lineage.concept_arguments
        else:
            measure = []
        measure_addrs = {a.address for a in measure if isinstance(a, BuildConcept)}
        grain_only = set(c.grain.components) - measure_addrs
        if grain_only:
            out[c.address] = grain_only
    return out


def _island_rowsets(g: "ReferenceGraph", cg) -> None:
    """Mutate the undirected connectivity copy ``cg`` so each rowset becomes an
    island: a rowset is a materialized result, so from outside it you can reach
    only its declared outputs through an explicit scoped join/merge — you cannot
    navigate into its derivation to recover the base concepts it was computed
    from. So sever every edge crossing a rowset boundary, then reconnect (a) a
    rowset's own outputs to each other and (b) outputs related across rowsets by a
    scoped-join pseudonym.

    Without this, a property keyed on a base concept (e.g. ``store_id.name``) looks
    falsely reachable from a rowset whose key was *renamed* off that base concept
    (``select store_id as sk_a``): the global graph connects ``store_id`` to the
    rowset through that internal derivation, so a genuine scoped-join disconnection
    (the join group is ``{sk_a, sk_b}``, not ``store_id``) is masked and surfaces
    as the generic unresolvable error instead of a named subgraph split.
    """
    members_by_rowset: dict[str, list[str]] = {}
    nodes_by_address: dict[str, list[str]] = {}
    rowset_nodes: set[str] = set()
    for node, concept in g.concepts.items():
        if concept.derivation != Derivation.ROWSET:
            continue
        rowset_nodes.add(node)
        nodes_by_address.setdefault(concept.address, []).append(node)
        if isinstance(concept.lineage, BuildRowsetItem):
            members_by_rowset.setdefault(concept.lineage.rowset.name, []).append(node)

    if not rowset_nodes:
        return

    island = rowset_nodes | {
        n for n in cg.nodes if isinstance(n, str) and n.startswith("rowset~")
    }
    cg.remove_edges_from(
        [(u, v) for u, v in cg.edges if (u in island) != (v in island)]
    )

    # reconnect each rowset's own outputs via a synthetic hub
    for name, members in members_by_rowset.items():
        hub = f"rowset_island~{name}"
        cg.add_node(hub)
        cg.add_edges_from((hub, m) for m in members if m in cg)

    # reconnect outputs related across rowsets by a scoped-join pseudonym
    for node in rowset_nodes:
        concept = g.concepts[node]
        for pseudonym in concept.pseudonyms:
            if pseudonym == concept.address:
                continue
            for other in nodes_by_address.get(pseudonym, []):
                if node in cg and other in cg:
                    cg.add_edge(node, other)


def disconnected_components(
    environment: BuildEnvironment,
    concepts: List[BuildConcept],
    g: "ReferenceGraph | None" = None,
    island_rowsets: bool = True,
) -> List[List[BuildConcept]]:
    """Partition concepts by true join reachability: two concepts share a group
    iff their reference-graph nodes are in the same weakly-connected component
    (i.e. some join / FK / merge path relates them). >1 group means a genuinely
    unconnected set — a real missing join/merge, not merely a grain conflict.

    Pass the resolution's graph as ``g`` to reuse it; otherwise one is built from
    ``environment``. Crossjoinable (single-row/constant) concepts are skipped.
    Aggregate grain-only ``by`` edges are dropped first (see
    ``_aggregate_grain_only_parents``) so a regroupable aggregate never bridges
    two otherwise-disconnected models through its grouping key.

    ``island_rowsets`` controls rowset islanding (see ``_island_rowsets``): when
    set, a base concept reachable only by navigating into a rowset's derivation is
    not treated as a real join path. This is correct as a *post-failure* message
    refiner (the v3 path, where discovery has already failed independently), but as
    a *pre-check gate* it false-positives on legitimate rowset join-backs (a base
    key that IS a rowset output, or a concept DERIVED from one) — so the v4
    pre-gate disables it and lets discovery decide. Defaults to the v3 behaviour.
    """
    from trilogy.core import graph as gx
    from trilogy.core.env_processor import generate_graph

    g = g if g is not None else generate_graph(environment)

    # Compute connectivity on an undirected copy so we can drop aggregate
    # grain-only edges without mutating the shared resolution graph.
    cg = g.to_undirected()
    grain_only = _aggregate_grain_only_parents(environment)
    if grain_only:
        for node, concept in g.concepts.items():
            keys = grain_only.get(concept.address)
            if not keys or node not in cg:
                continue
            for neighbor in list(gx.neighbors(cg, node)):
                neighbor_concept = g.concepts.get(neighbor)
                if neighbor_concept is not None and neighbor_concept.address in keys:
                    cg.remove_edge(node, neighbor)

    if island_rowsets:
        _island_rowsets(g, cg)

    comp_of: dict[str, int] = {}
    for i, component in enumerate(gx.connected_components(cg)):
        for node in component:
            comp_of[node] = i

    # concept -> the component id it resolves into; a concept whose nodes are
    # absent from the graph gets a synthetic per-address component so it surfaces
    # rather than silently vanishing.
    buckets: dict[object, List[BuildConcept]] = {}
    for concept in concepts:
        if _crossjoinable(concept):
            continue
        cid: object | None = None
        for node in _anchor_nodes(concept):
            if node in comp_of:
                cid = comp_of[node]
                break
        if cid is None:
            cid = f"orphan::{concept.address}"
        buckets.setdefault(cid, []).append(concept)

    groups = [sorted(grp, key=lambda c: c.address) for grp in buckets.values()]
    return sorted(groups, key=lambda grp: min(c.address for c in grp))


def raise_if_disconnected(
    environment: BuildEnvironment,
    concepts: List[BuildConcept],
    g: "ReferenceGraph | None" = None,
    island_rowsets: bool = True,
) -> None:
    """Raise the typed subgraph error when ``concepts`` span >1 unconnected
    reference-graph component (a real missing join/merge). Crossjoinable
    (single-row/constant) concepts are skipped, so valid cross-joins still pass.
    See ``disconnected_components`` for ``island_rowsets``."""
    subgraphs = disconnected_components(
        environment, concepts, g, island_rowsets=island_rowsets
    )
    if len(subgraphs) > 1:
        raise DisconnectedConceptsException(
            format_disconnected_subgraphs_error(subgraphs),
            subgraphs=[[c.address for c in group] for group in subgraphs],
        )


def _is_global_aggregate_gate(
    group: List[BuildConcept], output_addresses: set[str]
) -> bool:
    """True when a disconnected subgraph is a pure WHERE aggregate gate rather than
    a missing join: every member is an aggregate row-arg (not an output) at a grain
    absent from the outputs. Such a condition is a global filter gate — the planner
    bridges it via the gate's grain and cross-joins/dedups the (constant) outputs,
    matching v3 (e.g. `where sum(x) by name < ... select <const>`). A disconnected
    raw-column arg (`where bv > 0`) implies a row-level correlation that genuinely
    needs a join, so it is NOT a gate and must still raise."""
    return all(
        c.address not in output_addresses and c.derivation == Derivation.AGGREGATE
        for c in group
    )


def raise_if_disconnected_for(
    outputs: List[BuildConcept],
    conditions: "BuildWhereClause | None",
    environment: BuildEnvironment,
    g: "ReferenceGraph | None" = None,
    island_rowsets: bool = True,
) -> None:
    """Connectivity gate for a select's required concepts (its outputs plus any
    WHERE row args): raise the typed subgraph error when they span unconnected
    reference-graph components. Crossjoinable (single-row/constant) concepts are
    skipped by ``disconnected_components``, so e.g. two ungrouped scalar aggregates
    still resolve via cross-join. Shared verbatim by the top-level select and
    nested rowset inner selects — rowset discovery is recursive query discovery, so
    the connectivity diagnostic must be identical. See ``disconnected_components``
    for ``island_rowsets`` (the v4 pre-gate passes ``False``)."""
    concepts = list(outputs)
    output_addresses = {c.address for c in concepts}
    if conditions:
        concepts += [c for c in conditions.row_arguments if c.address not in seen]
    raise_if_disconnected(environment, concepts, g, island_rowsets=island_rowsets)


def format_disconnected_subgraphs_error(
    subgraphs: List[List[BuildConcept]],
) -> str:
    def render(group: List[BuildConcept]) -> str:
        addrs = sorted(c.address for c in group)
        # drop internal _virt_* scaffolding, but keep raw if that empties a group
        cleaned = [a for a in addrs if VIRTUAL_CONCEPT_PREFIX not in a]
        return "{" + ", ".join(cleaned or addrs) + "}"

    rendered = "; ".join(render(group) for group in subgraphs)
    return (
        "Discovery error: cannot merge all concepts into one connected query. "
        f"The requested concepts split into {len(subgraphs)} disconnected "
        f"subgraphs: {rendered}. Are you missing a join or merge statement to "
        "relate them?"
    )


def format_unresolved_concepts_error(
    all_concepts: List[BuildConcept], found_concepts: set[str]
) -> str:
    """Terminal-fallback message when discovery exhausts its candidates without
    building one connected source. Unlike the >1-subgraph case the model graph
    looks connected, so we can't name subgraphs — but the likely cause is still a
    missing join/merge to relate concepts across models. List what we did and
    didn't source, dropping internal `_virt_*` scaffolding."""
    requested = {c.address for c in all_concepts}

    def clean(addresses: set[str]) -> list[str]:
        return sorted(a for a in addresses if VIRTUAL_CONCEPT_PREFIX not in a)

    sourced = clean(found_concepts & requested)
    unresolved = clean(requested - found_concepts)

    def fmt(items: list[str]) -> str:
        return "{" + ", ".join(items) + "}"

    if unresolved:
        detail = f"Sourced: {fmt(sourced)}; still unresolved: {fmt(unresolved)}"
    else:
        # everything resolved individually but couldn't be combined
        detail = f"Sourced individually but not joinable from model: {fmt(sourced)}"
    return (
        "Discovery error: couldn't source all these concepts into one query; you "
        "may need a join or merge to relate them across models. " + detail
    )


def is_pushdown_aliased_concept(c: BuildConcept) -> bool:
    return (
        isinstance(c.lineage, BuildFunction)
        and c.lineage.operator == FunctionType.ALIAS
        and isinstance(c.lineage.arguments[0], BuildConcept)
        and c.lineage.arguments[0].derivation not in NO_PUSHDOWN_DERIVATIONS
    )


def get_inputs_that_require_pushdown(
    conditions: BuildWhereClause | None, mandatory: list[BuildConcept]
) -> list[BuildConcept]:
    if not conditions:
        return []
    return [
        x
        for x in mandatory
        if x.address not in conditions.row_arguments
        and (
            x.derivation not in NO_PUSHDOWN_DERIVATIONS
            or is_pushdown_aliased_concept(x)
        )
    ]


def _resolve_condition_disposition(
    conditions: BuildWhereClause | None,
    original_conditions: BuildWhereClause | None,
    remaining: list[BuildConcept],
    materialized_canonical: set[str],
    force_conditions: bool,
    force_pushdown_to_complex_input: bool,
    environment: BuildEnvironment | None,
    depth: int,
) -> tuple[bool, BuildWhereClause | None]:
    """Decide whether to inject condition row args and what conditions to push down.

    Returns (inject_row_args, routing_conditions).
    """
    all_materialized_roots = (
        bool(remaining)
        and conditions is not None
        and all(
            x.derivation == Derivation.ROOT
            and x.granularity != Granularity.SINGLE_ROW
            and x.canonical_address in materialized_canonical
            for x in remaining
        )
    )

    if all_materialized_roots:
        assert conditions is not None
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} All remaining mandatory concepts are materialized roots, injecting condition inputs into candidate list"
        )
        routing = (
            preserved_non_partial_conditions(conditions, environment)
            if environment
            else None
        )
        return True, routing
    elif conditions and force_conditions:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} condition evaluation at this level forced"
        )
        routing = conditions if force_pushdown_to_complex_input else None
        if routing is None and original_conditions and environment:
            routing = preserved_non_partial_conditions(original_conditions, environment)
        return True, routing
    else:
        # No consumption — recover routing atoms if conditions were already consumed
        if conditions is None and original_conditions and environment:
            return False, preserved_non_partial_conditions(
                original_conditions, environment
            )
        return False, conditions


def get_loop_iteration_targets(
    mandatory: list[BuildConcept],
    conditions: BuildWhereClause | None,
    attempted: set[str],
    force_conditions: bool,
    found: set[str],
    partial: set[str],
    depth: int,
    materialized_canonical: set[str],
    environment: BuildEnvironment | None = None,
) -> tuple[BuildConcept, List[BuildConcept], BuildWhereClause | None]:
    # objectives
    # 1. if we have complex types; push any conditions further up until we only have roots
    # 2. if we only have roots left, push all condition inputs into the candidate list
    # 3. from the final candidate list, select the highest priority concept to attempt next
    force_pushdown_to_complex_input = False

    pushdown_targets = get_inputs_that_require_pushdown(conditions, mandatory)
    if pushdown_targets:
        force_pushdown_to_complex_input = True
    # a list of all non-materialized concepts, or all concepts
    # if a pushdown is required
    all_concepts_local: list[BuildConcept] = [
        x
        for x in mandatory
        if force_pushdown_to_complex_input
        or (x.canonical_address not in materialized_canonical)
        # keep Root/Constant
        or x.derivation in (Derivation.ROOT, Derivation.CONSTANT)
    ]
    remaining_concrete = [x for x in mandatory if x.address not in all_concepts_local]

    for x in remaining_concrete:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX}  Adding materialized concept {x.address} as root instead of derived."
        )
        all_concepts_local.append(x.with_materialized_source())

    remaining = [x for x in all_concepts_local if x.address not in attempted]
    original_conditions = conditions
    conditions = evaluate_loop_condition_pushdown(
        mandatory=all_concepts_local,
        conditions=conditions,
        depth=depth,
        force_no_condition_pushdown=force_conditions,
        forced_pushdown=pushdown_targets,
    )
    local_all = [*all_concepts_local]

    inject_row_args, conditions = _resolve_condition_disposition(
        conditions=conditions,
        original_conditions=original_conditions,
        remaining=remaining,
        materialized_canonical=materialized_canonical,
        force_conditions=force_conditions,
        force_pushdown_to_complex_input=force_pushdown_to_complex_input,
        environment=environment,
        depth=depth,
    )
    if inject_row_args and original_conditions:
        local_all = unique(
            list(original_conditions.row_arguments) + remaining,
            "address",
        )

    priority_concept = get_priority_concept(
        all_concepts=local_all,
        attempted_addresses=attempted,
        found_concepts=found,
        partial_concepts=partial,
        depth=depth,
        environment=environment,
    )

    # A `by`-partitioned aggregate injected purely because it appears in the
    # outer WHERE (i.e. not in the caller's mandatory outputs) is a scoped
    # scalar subquery: its denominator is its own `by` grain, not the outer
    # WHERE. Treat it as a scalar for partial-satisfaction so outer routing
    # atoms do NOT propagate into its parent sourcing — otherwise a datasource
    # whose `non_partial_for` matches the outer filter can be pulled in to
    # satisfy the routing, bringing foreign join keys that silently restrict
    # the aggregate's input rows.
    mandatory_addresses = {c.address for c in mandatory}
    if (
        inject_row_args
        and isinstance(priority_concept.lineage, BuildAggregateWrapper)
        and priority_concept.lineage.by
        and priority_concept.address not in mandatory_addresses
    ):
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} priority {priority_concept.address} "
            f"is a filter-scalar by-aggregate; dropping outer conditions for its scope"
        )
        conditions = None

    # A condition can only be evaluated where all its inputs exist. When the
    # priority we are about to build is itself a *derived* row-argument of the
    # condition (e.g. a named `auto f1 <- a between x and y` flag used in both
    # `where (f1 or f2)` and a `? f1` operand), routing the condition into its
    # build is circular: sourcing the flag's parents puts the flag back in the
    # mandatory list via the condition's row args, re-forcing forever. Build the
    # flag first; the condition is applied at this level's completion instead.
    # (An aggregate grouped *by* a derived row-arg is fine — it isn't the
    # row-arg itself, so it never matches here and keeps normal pushdown.)
    if (
        conditions
        and priority_concept.derivation not in ROOT_DERIVATIONS
        and priority_concept.address in {c.address for c in conditions.row_arguments}
    ):
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} priority {priority_concept.address} "
            f"is a derived condition input; not routing the condition into its build"
        )
        conditions = None

    optional = generate_candidates_restrictive(
        priority_concept=priority_concept,
        candidates=local_all,
        exhausted=attempted,
    )
    return priority_concept, optional, conditions
