from trilogy.constants import logger
from trilogy.core import graph as nx
from trilogy.core.enums import (
    Derivation,
    Granularity,
    JoinType,
    Purpose,
)
from trilogy.core.graph_models import (
    ReferenceGraph,
    SearchCriteria,
    concept_to_node,
    prune_sources_for_aggregates,
    prune_sources_for_conditions,
)
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildUnionDatasource,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.aggregate_rollup import (
    _is_additive_aggregate,
    get_additive_rollup_concepts,
)
from trilogy.core.processing.condition_utility import merge_conditions
from trilogy.core.processing.discovery_validation import (
    ValidationResult,
    validate_stack,
)
from trilogy.core.processing.node_generators.common import reinject_common_join_keys_v2
from trilogy.core.processing.node_generators.select_helpers.condition_routing import (
    covered_conditions,
)
from trilogy.core.processing.node_generators.select_helpers.datasource_injection import (
    get_union_sources,
)
from trilogy.core.processing.node_generators.select_helpers.datasource_nodes import (
    create_datasource_node,
    create_select_node,
    create_union_datasource,
)
from trilogy.core.processing.node_generators.select_helpers.source_scoring import (
    deduplicate_datasources,
    get_graph_partial_nodes,
    get_materialization_score,
    resolve_subgraphs,
    score_datasource_node,
    subgraph_is_complete,
)
from trilogy.core.processing.nodes import (
    ConstantNode,
    MergeNode,
    StrategyNode,
)
from trilogy.core.processing.utility import padding
from trilogy.utility import unique

LOGGER_PREFIX = "[GEN_ROOT_MERGE_NODE]"
__all__ = [
    "SearchCriteria",
    "create_datasource_node",
    "create_pruned_concept_graph",
    "create_select_node",
    "create_union_datasource",
    "gen_select_merge_node",
    "get_graph_partial_nodes",
    "get_materialization_score",
    "resolve_subgraphs",
    "score_datasource_node",
]


def create_pruned_concept_graph(
    g: ReferenceGraph,
    all_concepts: list[BuildConcept],
    datasources: list[BuildDatasource],
    criteria: SearchCriteria,
    conditions: BuildWhereClause | None = None,
    depth: int = 0,
    allow_intersection: bool = False,
) -> ReferenceGraph | None:
    orig_g = g
    g = g.copy()
    union_options = get_union_sources(datasources, all_concepts)
    concepts_by_address = {c.address: c for c in orig_g.concepts.values()}
    for node_address, datasource in list(g.datasources.items()):
        if not isinstance(datasource, BuildDatasource):
            continue
        for concept in get_additive_rollup_concepts(
            datasource=datasource,
            requested_concepts=all_concepts,
            concepts_by_address=concepts_by_address,
            datasources=datasources,
            conditions=conditions,
        ):
            cnode = concept_to_node(concept)
            g.concepts[cnode] = concept
            g.add_node(cnode)
            g.add_edge(node_address, cnode)
            g.add_edge(cnode, node_address)

    for ds_list in union_options:
        node_address = "ds~" + "-".join([x.name for x in ds_list])
        _merged = merge_conditions(
            [
                x.non_partial_for.conditional
                for x in ds_list
                if x.non_partial_for is not None
            ]
        )
        reduced_non_partial_for = (
            BuildWhereClause(conditional=_merged) if _merged is not None else None
        )
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} injecting potentially relevant union datasource {node_address} with non_partial_for {reduced_non_partial_for} from children {[x.name for x in ds_list]}"
        )
        common: set[BuildConcept] = set.intersection(
            *[set(x.output_concepts) for x in ds_list]
        )
        g.datasources[node_address] = BuildUnionDatasource(
            children=ds_list, non_partial_for=reduced_non_partial_for
        )
        for c in common:
            cnode = concept_to_node(c)
            g.add_edge(node_address, cnode)
            g.add_edge(cnode, node_address)

    prune_sources_for_conditions(
        g,
        criteria,
        conditions,
        allow_intersection,
        {c.canonical_address for c in all_concepts},
    )
    prune_sources_for_aggregates(g, all_concepts, logger, orig_g=orig_g)

    target_addresses = {c.canonical_address for c in all_concepts}
    concepts: dict[str, BuildConcept] = orig_g.concepts
    relevant_concepts_pre = {
        n: x.canonical_address
        for n in g.nodes()
        # filter out synonyms
        if (x := concepts.get(n, None)) and x.canonical_address in target_addresses
    }
    relevant_concepts: list[str] = list(relevant_concepts_pre.keys())
    partial = get_graph_partial_nodes(g, conditions)

    if criteria == SearchCriteria.FULL_ONLY:
        datasource_map = orig_g.datasources
        to_remove = [
            edge
            for edge in g.edges
            if (edge[0] in datasource_map and edge[1] in partial.get(edge[0], []))
            or (edge[1] in datasource_map and edge[0] in partial.get(edge[1], []))
        ]
        g.remove_edges_from(to_remove)

    g_edges = set(g.edges)
    relevant_datasets = [
        n for n in g.datasources if any((n, x) in g_edges for x in relevant_concepts)
    ]
    logger.info(f"Relevant datasets after pruning: {relevant_datasets}")

    relevant_datasets = deduplicate_datasources(
        relevant_datasets, relevant_concepts, g_edges, g.datasources, depth, partial
    )

    keep = set(relevant_datasets)
    keep.update(relevant_concepts)
    g.remove_nodes_from([n for n in g.nodes() if n not in keep])

    synonyms: dict[str, str] = {}
    for c in all_concepts:
        for xc in c.pseudonyms:
            synonyms[xc] = c.address
    reinject_common_join_keys_v2(orig_g, g, synonyms, add_joins=True)

    subgraphs = [
        s
        for s in nx.connected_components(g.to_undirected())
        if subgraph_is_complete(s, target_addresses, relevant_concepts_pre, g)
    ]
    if not subgraphs:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} cannot resolve root graph - no subgraphs after node prune"
        )
        return None

    if len(subgraphs) != 1:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} cannot resolve root graph - subgraphs are split - have {len(subgraphs)} from {subgraphs}"
        )
        return None

    # add back any relevant edges that might have been partially filtered
    relevant = set(relevant_concepts + relevant_datasets)
    for edge in orig_g.edges():
        if edge[0] in relevant and edge[1] in relevant:
            g.add_edge(edge[0], edge[1])

    if not any(n.startswith("ds~") for n in g.nodes):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} cannot resolve root graph - No datasource nodes found"
        )
        return None

    return g


def _source_concepts_via_graph(
    concepts: list[BuildConcept],
    g: ReferenceGraph,
    environment: BuildEnvironment,
    depth: int,
    accept_partial: bool = False,
    conditions: BuildWhereClause | None = None,
    allow_intersection: bool = False,
    filter_conditions: BuildWhereClause | None = None,
) -> list[StrategyNode]:
    """Run the pruned-graph → subgraph → SelectNode pipeline for a list of concepts.

    conditions: used for graph pruning (which datasources to keep).
    filter_conditions: if set, used for WHERE application in SelectNodes instead of conditions.
    """
    select_conditions = (
        filter_conditions if filter_conditions is not None else conditions
    )
    attempts = [SearchCriteria.FULL_ONLY]
    if accept_partial:
        attempts.append(SearchCriteria.PARTIAL_UNSCOPED)
        attempts.append(SearchCriteria.PARTIAL_INCLUDING_SCOPED)
    pruned: ReferenceGraph | None = None
    sub_nodes: dict[str, list[str]] = {}
    for attempt in attempts:
        pruned = create_pruned_concept_graph(
            g,
            concepts,
            criteria=attempt,
            conditions=conditions,
            datasources=list(environment.datasources.values()),
            depth=depth,
            allow_intersection=allow_intersection,
        )
        if not pruned:
            continue
        sub_nodes = resolve_subgraphs(
            pruned,
            relevant=concepts,
            criteria=attempt,
            conditions=select_conditions,
            depth=depth,
        )
        break
    if not pruned:
        return []
    return [
        node
        for k, subgraph in sub_nodes.items()
        if (
            node := create_select_node(
                k,
                subgraph,
                g=pruned,
                accept_partial=accept_partial,
                environment=environment,
                depth=depth,
                conditions=select_conditions,
            )
        )
        is not None
    ]


def gen_select_merge_node(
    all_concepts: list[BuildConcept],
    g: ReferenceGraph,
    environment: BuildEnvironment,
    depth: int,
    accept_partial: bool = False,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    abstract_props = [
        c
        for c in all_concepts
        if c.grain.abstract
        and c.purpose == Purpose.PROPERTY
        and c.granularity == Granularity.SINGLE_ROW
        and c.address in environment.materialized_concepts
    ]
    constants = [c for c in all_concepts if c.derivation == Derivation.CONSTANT]
    excluded = {c.address for c in abstract_props} | {c.address for c in constants}
    normals = [c for c in all_concepts if c.address not in excluded]
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} generating select merge node for normals: {normals}, abstract_props: {abstract_props}, constants: {constants}, conditions: {conditions}"
    )
    only_abstract = not normals and not constants and abstract_props
    only_constant = not normals and not abstract_props and constants
    if only_abstract:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} only abstract-grain property inputs ({abstract_props}), sourcing each independently"
        )
        abstract_nodes = [
            n
            for p in abstract_props
            for n in _source_concepts_via_graph(
                [p], g, environment, depth + 1, accept_partial, conditions
            )
        ]
        # all found
        if len(abstract_nodes) < len(abstract_props):
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} not all abstract properties could be sourced, cannot generate select node."
            )
            return None
        if not abstract_nodes:
            return None
        if len(abstract_nodes) == 1:
            return abstract_nodes[0]
        return MergeNode(
            output_concepts=abstract_props,
            input_concepts=abstract_props,
            environment=environment,
            depth=depth,
            parents=abstract_nodes,
            preexisting_conditions=None,
        )

    elif only_constant:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} only constant inputs to discovery ({constants}), returning constant node directly"
        )
        for x in constants:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} {x} {x.lineage} {x.derivation}"
            )
        if conditions:
            if not all(
                x.derivation == Derivation.CONSTANT for x in conditions.row_arguments
            ):
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} conditions being passed in to constant node {conditions}, but not all concepts are constants, cannot generate select node."
                )
                return None
            else:
                constants += conditions.row_arguments

        return ConstantNode(
            output_concepts=constants,
            input_concepts=[],
            environment=environment,
            parents=[],
            depth=depth,
            partial_concepts=[],
            force_group=False,
            conditions=conditions.conditional if conditions else None,
        )
    parents: list[StrategyNode] = []
    if normals:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} searching for root source graph for concepts {[c.address for c in all_concepts]} and conditions {conditions}"
        )
        parents = _source_concepts_via_graph(
            normals, g, environment, depth, accept_partial, conditions
        )
        if not parents and conditions:
            # Retry with only "covered" condition atoms (those implied by some datasource's
            # non_partial_for) for graph pruning. Foreign datasources (e.g. tree_enrichment
            # when filtering by city='USSFO') are kept via the intersection check since the
            # condition is guaranteed to be applied by the owning partial datasource.
            # The original conditions are still passed as filter_conditions so that
            # per-datasource WHERE clauses (e.g. tree_category='deciduous') are preserved.
            covered = covered_conditions(conditions, environment)
            if covered:
                parents = _source_concepts_via_graph(
                    normals,
                    g,
                    environment,
                    depth,
                    accept_partial,
                    covered,
                    allow_intersection=True,
                    filter_conditions=conditions,
                )
        if not parents:
            logger.info(f"{padding(depth)}{LOGGER_PREFIX} no covering graph found.")
            return None

    if constants:
        parents.append(
            ConstantNode(
                output_concepts=constants,
                input_concepts=[],
                environment=environment,
                parents=[],
                depth=depth,
                partial_concepts=[],
                force_group=False,
                preexisting_conditions=conditions.conditional if conditions else None,
            )
        )

    for p in abstract_props:
        abstract_nodes = _source_concepts_via_graph(
            [p], g, environment, depth + 1, accept_partial, conditions
        )
        if not abstract_nodes:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} no source found for abstract property {p}, cannot generate select node."
            )
            return None
        parents.extend(abstract_nodes)

    if len(parents) == 1:
        candidate: StrategyNode = parents[0]
    else:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Multiple parent DS nodes resolved - {[type(x) for x in parents]}, wrapping in merge"
        )

        preexisting_conditions = None
        force_join_type: JoinType | None = None
        if conditions and all(
            x.preexisting_conditions
            and x.preexisting_conditions == conditions.conditional
            for x in parents
        ):
            preexisting_conditions = conditions.conditional
        elif conditions:
            # Filter applied at one parent (e.g. a partial-aggregate rollup)
            # plus pure-enumerator joins (single-key dimension tables added by
            # prune_sources_for_aggregates upgrade): the conditioned parent
            # already carries the merge's full output set, so the merge
            # inherits its conditions. Force INNER joins so unmatched
            # enumerator rows (which never went through the WHERE) don't leak
            # NULL-filter rows into the result.
            output_addrs = {c.address for c in all_concepts}
            for parent in parents:
                if (
                    parent.preexisting_conditions == conditions.conditional
                    and output_addrs.issubset(
                        {c.address for c in parent.usable_outputs}
                    )
                ):
                    preexisting_conditions = conditions.conditional
                    force_join_type = JoinType.INNER
                    break

        # When the merge's joined grain (e.g. customer_id from agg + dim) is
        # finer than the outer target's grain (e.g. region) — and the target
        # is reachable from the merge grain via property-of-key — the merge
        # needs to SUM-roll additive aggregates up to the target grain.
        # Mark force_group + rollup_concepts so the renderer emits SUM and
        # GROUP BY at the merge level.
        additive_aggs = [
            c for c in all_concepts if c.is_aggregate and _is_additive_aggregate(c)
        ]
        rollup_at_merge: list[BuildConcept] = []
        force_merge_group: bool | None = None
        if additive_aggs and len(additive_aggs) == sum(
            1 for c in all_concepts if c.is_aggregate
        ):
            merge_components: set[str] = set()
            for parent_node in parents:
                pg = parent_node.grain
                if pg and pg.components:
                    merge_components.update(pg.components)
            target_components = {c.address for c in all_concepts if not c.is_aggregate}
            unreached = target_components - merge_components
            unreached_via_property = bool(unreached) and all(
                (concept := environment.concepts.get(tc)) is not None
                and concept.purpose == Purpose.PROPERTY
                and concept.keys
                and concept.keys.issubset(merge_components)
                for tc in unreached
            )
            if (
                merge_components
                and target_components
                and target_components != merge_components
                and unreached_via_property
            ):
                rollup_at_merge = additive_aggs
                force_merge_group = True

        candidate = MergeNode(
            output_concepts=all_concepts,
            input_concepts=normals + abstract_props,
            environment=environment,
            depth=depth,
            parents=parents,
            preexisting_conditions=preexisting_conditions,
            force_join_type=force_join_type,
            force_group=force_merge_group,
            rollup_concepts=rollup_at_merge or None,
        )

    if conditions:
        completion_mandatory = unique(
            all_concepts + list(conditions.row_arguments), "address"
        )
        complete, _, _, _, _ = validate_stack(
            environment,
            [candidate],
            all_concepts,
            completion_mandatory,
            conditions=conditions,
            accept_partial=accept_partial,
        )
        if complete != ValidationResult.COMPLETE:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} candidate validation state was {complete}; returning None"
            )
            return None

    return candidate
