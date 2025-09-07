from itertools import combinations
from typing import Callable, List, Optional

import networkx as nx
from networkx.algorithms import approximation as ax

from trilogy.constants import logger
from trilogy.core.enums import Derivation
from trilogy.core.exceptions import AmbiguousRelationshipResolutionException
from trilogy.core.graph_models import (
    ReferenceGraph,
    concept_to_node,
    prune_sources_for_conditions,
)
from trilogy.core.models.build import (
    BuildConcept,
    BuildConditional,
    BuildGrain,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import History, MergeNode, StrategyNode
from trilogy.core.processing.utility import padding
from trilogy.utility import unique

LOGGER_PREFIX = "[GEN_MERGE_NODE]"
AMBIGUITY_CHECK_LIMIT = 20


def filter_pseudonyms_for_source(
    ds_graph: nx.DiGraph, node: str, pseudonyms: set[tuple[str, str]]
):
    to_remove = set()
    for edge in ds_graph.edges:
        if edge in pseudonyms:
            lengths = {}
            for n in edge:
                try:
                    lengths[n] = nx.shortest_path_length(ds_graph, node, n)
                except nx.NetworkXNoPath:
                    lengths[n] = 999
            to_remove.add(max(lengths, key=lambda x: lengths.get(x, 0)))
    for node in to_remove:
        ds_graph.remove_node(node)


def extract_address(node: str):
    return node.split("~")[1].split("@")[0]


def extract_concept(node: str, env: BuildEnvironment):
    if node in env.alias_origin_lookup:
        return env.alias_origin_lookup[node]
    return env.concepts[node]


def filter_unique_graphs(graphs: list[list[str]]) -> list[list[str]]:
    unique_graphs: list[set[str]] = []

    # sort graphs from largest to smallest
    graphs.sort(key=lambda x: len(x), reverse=True)
    for graph in graphs:
        if not any(set(graph).issubset(x) for x in unique_graphs):
            unique_graphs.append(set(graph))

    return [list(x) for x in unique_graphs]


def extract_ds_components(
    g: nx.DiGraph, nodelist: list[str], pseudonyms: set[tuple[str, str]]
) -> list[list[str]]:
    graphs = []
    for node in g.nodes:
        if node.startswith("ds~"):
            local = g.copy()
            filter_pseudonyms_for_source(local, node, pseudonyms)
            ds_graph: nx.DiGraph = nx.ego_graph(local, node, radius=10).copy()
            graphs.append(
                [
                    extract_address(x)
                    for x in ds_graph.nodes
                    if not str(x).startswith("ds~")
                ]
            )
    # if we had no ego graphs, return all concepts
    if not graphs:
        return [[extract_address(node) for node in nodelist]]
    graphs = filter_unique_graphs(graphs)
    for node in nodelist:
        parsed = extract_address(node)
        if not any(parsed in x for x in graphs):
            graphs.append([parsed])
    return graphs


def prune_and_merge(
    G: nx.DiGraph,
    keep_node_lambda: Callable[[str], bool],
) -> nx.DiGraph:
    """
    Prune nodes of one type and create direct connections between remaining nodes.

    Args:
        G: NetworkX graph
        keep_node_type: The node type to keep
        node_type_attr: Attribute name that stores node type

    Returns:
        New graph with only keep_node_type nodes and merged connections
    """
    # Get nodes to keep
    nodes_to_keep = [n for n in G.nodes if keep_node_lambda(n)]
    # Create new graph
    new_graph = G.subgraph(nodes_to_keep).copy()

    # Find paths between nodes to keep through removed nodes
    nodes_to_remove = [n for n in G.nodes() if n not in nodes_to_keep]

    for node_pair in combinations(nodes_to_keep, 2):
        n1, n2 = node_pair

        # Check if there's a path through removed nodes
        try:
            path = nx.shortest_path(G, n1, n2)
            # If path exists and goes through nodes we're removing
            if len(path) > 2 or any(node in nodes_to_remove for node in path[1:-1]):
                new_graph.add_edge(n1, n2)
        except nx.NetworkXNoPath:
            continue

    return new_graph


def reinject_common_join_keys_v2(
    G: ReferenceGraph,
    final: nx.DiGraph,
    nodelist: list[str],
    synonyms: set[str] = set(),
) -> bool:
    # when we've discovered a concept join, for each pair of ds nodes
    # check if they have more keys in common
    # and inject those to discovery as join conditions
    def is_ds_node(n: str) -> bool:
        return n.startswith("ds~")

    ds_graph = prune_and_merge(final, is_ds_node)
    injected = False
    for datasource in ds_graph.nodes:
        node1 = G.datasources[datasource]
        neighbors = nx.all_neighbors(ds_graph, datasource)
        for neighbor in neighbors:
            node2 = G.datasources[neighbor]
            common_concepts = set(
                x.concept.address for x in node1.columns
            ).intersection(set(x.concept.address for x in node2.columns))
            concrete_concepts = [
                x.concept for x in node1.columns if x.concept.address in common_concepts
            ]
            reduced = BuildGrain.from_concepts(concrete_concepts).components
            existing_addresses = set()
            for concrete in concrete_concepts:
                logger.info(
                    f"looking at column {concrete.address} with pseudonyms {concrete.pseudonyms}"
                )
                cnode = concept_to_node(concrete.with_default_grain())
                if cnode in final.nodes:
                    existing_addresses.add(concrete.address)
                    continue
            for concrete in concrete_concepts:
                if concrete.address in synonyms:
                    continue
                if concrete.address not in reduced:
                    continue
                # skip anything that is already in the graph pseudonyms
                if any(x in concrete.pseudonyms for x in existing_addresses):
                    continue
                cnode = concept_to_node(concrete.with_default_grain())
                final.add_edge(datasource, cnode)
                final.add_edge(neighbor, cnode)
                logger.info(
                    f"{LOGGER_PREFIX} reinjecting common join key {cnode} between {datasource} and {neighbor}"
                )
                injected = True
    return injected


def determine_induced_minimal_nodes(
    G: ReferenceGraph,
    nodelist: list[str],
    environment: BuildEnvironment,
    filter_downstream: bool,
    accept_partial: bool = False,
    synonyms: set[str] = set(),
) -> nx.DiGraph | None:
    H: nx.Graph = nx.to_undirected(G).copy()
    nodes_to_remove = []
    for node, lookup in G.concepts.items():
        # inclusion of aggregates can create ambiguous node relation chains
        # there may be a better way to handle this
        # can be revisited if we need to connect a derived synonym based on an aggregate
        if lookup.derivation in (
            Derivation.CONSTANT,
            Derivation.AGGREGATE,
            Derivation.FILTER,
        ):
            nodes_to_remove.append(node)
        # purge a node if we're already looking for all it's parents
        if filter_downstream and lookup.derivation not in (Derivation.ROOT,):
            nodes_to_remove.append(node)
    if nodes_to_remove:
        # logger.debug(f"Removing nodes {nodes_to_remove} from graph")
        H.remove_nodes_from(nodes_to_remove)
    isolates = list(nx.isolates(H))
    if isolates:
        # logger.debug(f"Removing isolates {isolates} from graph")
        H.remove_nodes_from(isolates)

    zero_out = list(x for x in H.nodes if G.out_degree(x) == 0 and x not in nodelist)
    while zero_out:
        # logger.debug(f"Removing zero out nodes {zero_out} from graph")
        H.remove_nodes_from(zero_out)
        zero_out = list(
            x for x in H.nodes if G.out_degree(x) == 0 and x not in nodelist
        )
    try:
        paths = nx.multi_source_dijkstra_path(H, nodelist)
        # logger.debug(f"Paths found for {nodelist}")
    except nx.exception.NodeNotFound:
        # logger.debug(f"Unable to find paths for {nodelist}- {str(e)}")
        return None
    path_removals = list(x for x in H.nodes if x not in paths)
    if path_removals:
        # logger.debug(f"Removing paths {path_removals} from graph")
        H.remove_nodes_from(path_removals)
    # logger.debug(f"Graph after path removal {H.nodes}")
    sG: nx.Graph = ax.steinertree.steiner_tree(H, nodelist).copy()
    if not sG.nodes:
        logger.debug(f"No Steiner tree found for nodes {nodelist}")
        return None
    # logger.debug(f"Steiner tree found for nodes {nodelist} {sG.nodes}")
    final: nx.DiGraph = nx.subgraph(G, sG.nodes).copy()

    for edge in G.edges:
        if edge[1] in final.nodes and edge[0].startswith("ds~"):
            ds_name = extract_address(edge[0])
            ds = environment.datasources[ds_name]
            concept = environment.concepts[extract_address(edge[1])]
            if concept.address in [x.address for x in ds.partial_concepts]:
                if not accept_partial:
                    continue
            final.add_edge(*edge)

    reinject_common_join_keys_v2(G, final, nodelist, synonyms)

    # all concept nodes must have a parent
    if not all(
        [
            final.in_degree(node) > 0
            for node in final.nodes
            if node.startswith("c~") and node in nodelist
        ]
    ):
        missing = [
            node
            for node in final.nodes
            if node.startswith("c~") and final.in_degree(node) == 0
        ]
        logger.debug(f"Skipping graph for {nodelist} as no in_degree {missing}")
        return None

    if not all([node in final.nodes for node in nodelist]):
        missing = [node for node in nodelist if node not in final.nodes]
        logger.debug(
            f"Skipping graph for initial list {nodelist} as missing nodes {missing} from final graph {final.nodes}"
        )
        return None
    logger.debug(f"Found final graph {final.nodes}")
    return final


def canonicalize_addresses(
    reduced_concept_set: set[str], environment: BuildEnvironment
) -> set[str]:
    """
    Convert a set of concept addresses to their canonical form.
    This is necessary to ensure that we can compare concepts correctly,
    especially when dealing with aliases or pseudonyms.
    """
    return set(
        environment.concepts[x].address if x in environment.concepts else x
        for x in reduced_concept_set
    )


def detect_ambiguity_and_raise(
    all_concepts: list[BuildConcept],
    reduced_concept_sets_raw: list[set[str]],
    environment: BuildEnvironment,
) -> None:
    final_candidates: list[set[str]] = []
    common: set[str] = set()
    # find all values that show up in every join_additions
    reduced_concept_sets = [
        canonicalize_addresses(x, environment) for x in reduced_concept_sets_raw
    ]
    for ja in reduced_concept_sets:
        if not common:
            common = ja
        else:
            common = common.intersection(ja)
        if all(set(ja).issubset(y) for y in reduced_concept_sets):
            final_candidates.append(ja)
    if not final_candidates:
        filtered_paths = [x.difference(common) for x in reduced_concept_sets]
        raise AmbiguousRelationshipResolutionException(
            message=f"Multiple possible concept injections found for {[x.address for x in all_concepts]}, got {' or '.join([str(x) for x in reduced_concept_sets])}",
            parents=filtered_paths,
        )


def has_synonym(concept: BuildConcept, others: list[list[BuildConcept]]) -> bool:
    return any(
        c.address in concept.pseudonyms or concept.address in c.pseudonyms
        for sublist in others
        for c in sublist
    )


def filter_relevant_subgraphs(
    subgraphs: list[list[BuildConcept]],
) -> list[list[BuildConcept]]:
    return [
        subgraph
        for subgraph in subgraphs
        if len(subgraph) > 1
        or (
            len(subgraph) == 1
            and not has_synonym(subgraph[0], [x for x in subgraphs if x != subgraph])
        )
    ]


def filter_duplicate_subgraphs(
    subgraphs: list[list[BuildConcept]], environment
) -> list[list[BuildConcept]]:
    seen: list[set[str]] = []

    for graph in subgraphs:
        seen.append(
            canonicalize_addresses(set([x.address for x in graph]), environment)
        )
    final = []
    # sometimes w can get two subcomponents that are the same
    # due to alias resolution
    # if so, drop any that are strict subsets.
    for graph in subgraphs:
        logger.info(f"Checking graph {graph} for duplicates in {seen}")
        set_x = canonicalize_addresses(set([x.address for x in graph]), environment)
        if any([set_x.issubset(y) and set_x != y for y in seen]):
            continue
        final.append(graph)
    return final


def resolve_weak_components(
    all_concepts: List[BuildConcept],
    environment: BuildEnvironment,
    environment_graph: ReferenceGraph,
    filter_downstream: bool = True,
    accept_partial: bool = False,
    search_conditions: BuildWhereClause | None = None,
) -> list[list[BuildConcept]] | None:
    break_flag = False
    found = []
    search_graph = environment_graph.copy()
    prune_sources_for_conditions(
        search_graph, accept_partial, conditions=search_conditions
    )
    reduced_concept_sets: list[set[str]] = []

    # loop through, removing new nodes we find
    # to ensure there are not ambiguous discovery paths
    # (if we did not care about raising ambiguity errors, we could just use the first one)
    count = 0
    node_list = sorted(
        [
            concept_to_node(c.with_default_grain())
            for c in all_concepts
            if "__preql_internal" not in c.address
        ]
    )
    synonyms: set[str] = set()
    for x in all_concepts:
        synonyms = synonyms.union(x.pseudonyms)
    # from trilogy.hooks.graph_hook import GraphHook
    # GraphHook().query_graph_built(search_graph, highlight_nodes=[concept_to_node(c.with_default_grain()) for c in all_concepts if "__preql_internal" not in c.address])
    while break_flag is not True:
        count += 1
        if count > AMBIGUITY_CHECK_LIMIT:
            break_flag = True
        try:
            g = determine_induced_minimal_nodes(
                search_graph,
                node_list,
                filter_downstream=filter_downstream,
                accept_partial=accept_partial,
                environment=environment,
                synonyms=synonyms,
            )

            if not g or not g.nodes:
                break_flag = True
                continue
            if not nx.is_weakly_connected(g):
                break_flag = True
                continue
            # from trilogy.hooks.graph_hook import GraphHook
            # GraphHook().query_graph_built(g, highlight_nodes=[concept_to_node(c.with_default_grain()) for c in all_concepts if "__preql_internal" not in c.address])
            all_graph_concepts = [
                extract_concept(extract_address(node), environment)
                for node in g.nodes
                if node.startswith("c~")
            ]
            new = [x for x in all_graph_concepts if x.address not in all_concepts]

            if not new:
                break_flag = True
            # remove our new nodes for the next search path
            for n in new:
                node = concept_to_node(n)
                if node in search_graph:
                    search_graph.remove_node(node)
            # TODO: figure out better place for debugging
            # from trilogy.hooks.graph_hook import GraphHook
            # GraphHook().query_graph_built(g, highlight_nodes=[concept_to_node(c.with_default_grain()) for c in all_concepts if "__preql_internal" not in c.address])
            found.append(g)
            new_addresses = set([x.address for x in new if x.address not in synonyms])
            reduced_concept_sets.append(new_addresses)

        except nx.exception.NetworkXNoPath:
            break_flag = True
        if g and not g.nodes:
            break_flag = True
    if not found:
        return None

    detect_ambiguity_and_raise(all_concepts, reduced_concept_sets, environment)

    # take our first one as the actual graph
    g = found[0]

    subgraphs: list[list[BuildConcept]] = []
    # components = nx.strongly_connected_components(g)
    node_list = [x for x in g.nodes if x.startswith("c~")]
    components = extract_ds_components(g, node_list, environment_graph.pseudonyms)
    logger.debug(f"Extracted components {components} from {node_list}")
    for component in components:
        # we need to take unique again as different addresses may map to the same concept
        sub_component = unique(
            # sorting here is required for reproducibility
            # todo: we should sort in an optimized order
            [extract_concept(x, environment) for x in sorted(component)],
            "address",
        )
        if not sub_component:
            continue
        subgraphs.append(sub_component)
    final = filter_duplicate_subgraphs(subgraphs, environment)
    return final
    # return filter_relevant_subgraphs(subgraphs)


def subgraphs_to_merge_node(
    concept_subgraphs: list[list[BuildConcept]],
    depth: int,
    all_concepts: List[BuildConcept],
    environment,
    g,
    source_concepts,
    history,
    conditions,
    output_concepts: List[BuildConcept],
    search_conditions: BuildWhereClause | None = None,
    enable_early_exit: bool = True,
):
    parents: List[StrategyNode] = []
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} fetching subgraphs {[[c.address for c in subgraph] for subgraph in concept_subgraphs]}"
    )
    for graph in concept_subgraphs:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} fetching subgraph {[c.address for c in graph]}"
        )

        parent: StrategyNode | None = source_concepts(
            mandatory_list=graph,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
            # conditions=search_conditions,
        )
        if not parent:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} Unable to instantiate target subgraph"
            )
            return None
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} finished subgraph fetch for {[c.address for c in graph]}, have parent {type(parent)} w/ {[c.address for c in parent.output_concepts]}"
        )
        parents.append(parent)
    input_c = []
    output_c = []
    for x in parents:
        for y in x.usable_outputs:
            input_c.append(y)
            if y in output_concepts:
                output_c.append(y)
            elif any(y.address in c.pseudonyms for c in output_concepts) or any(
                c.address in y.pseudonyms for c in output_concepts
            ):
                output_c.append(y)

    if len(parents) == 1 and enable_early_exit:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} only one parent node, exiting early w/ {[c.address for c in parents[0].output_concepts]}"
        )
        return parents[0]
    rval = MergeNode(
        input_concepts=unique(input_c, "address"),
        output_concepts=output_c,
        environment=environment,
        parents=parents,
        depth=depth,
        # hidden_concepts=[]
        # conditions=conditions,
        # conditions=search_conditions.conditional,
        # preexisting_conditions=search_conditions.conditional,
        # node_joins=[]
    )
    return rval


def gen_merge_node(
    all_concepts: List[BuildConcept],
    g: nx.DiGraph,
    environment: BuildEnvironment,
    depth: int,
    source_concepts,
    accept_partial: bool = False,
    history: History | None = None,
    conditions: BuildConditional | None = None,
    search_conditions: BuildWhereClause | None = None,
) -> Optional[MergeNode]:
    if search_conditions:
        all_search_concepts = unique(
            all_concepts + list(search_conditions.row_arguments), "address"
        )
    else:
        all_search_concepts = all_concepts
    all_search_concepts = sorted(all_search_concepts, key=lambda x: x.address)
    break_set = set([x.address for x in all_search_concepts])
    for filter_downstream in [True, False]:
        weak_resolve = resolve_weak_components(
            all_search_concepts,
            environment,
            g,
            filter_downstream=filter_downstream,
            accept_partial=accept_partial,
            search_conditions=search_conditions,
        )
        if not weak_resolve:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} wasn't able to resolve graph through intermediate concept injection with accept_partial {accept_partial}, filter_downstream {filter_downstream}"
            )
            continue

        log_graph = [[y.address for y in x] for x in weak_resolve]
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Was able to resolve graph through weak component resolution - final graph {log_graph}"
        )
        for flat in log_graph:
            if set(flat) == break_set:
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} expanded concept resolution was identical to search resolution; breaking to avoid recursion error."
                )
                return None
        return subgraphs_to_merge_node(
            weak_resolve,
            depth=depth,
            all_concepts=all_search_concepts,
            environment=environment,
            g=g,
            source_concepts=source_concepts,
            history=history,
            conditions=conditions,
            search_conditions=search_conditions,
            output_concepts=all_concepts,
        )
    return None
