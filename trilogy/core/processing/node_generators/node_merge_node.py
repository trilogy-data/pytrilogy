from typing import List, Optional

from trilogy.core.models import Concept, Environment, Conditional
from trilogy.core.processing.nodes import MergeNode, History, StrategyNode
import networkx as nx
from trilogy.core.graph_models import concept_to_node
from trilogy.core.processing.utility import PathInfo
from trilogy.constants import logger
from trilogy.utility import unique
from trilogy.core.exceptions import AmbiguousRelationshipResolutionException
from trilogy.core.processing.utility import padding
from networkx.algorithms import approximation as ax
from trilogy.core.enums import PurposeLineage


LOGGER_PREFIX = "[GEN_MERGE_NODE]"
AMBIGUITY_CHECK_LIMIT = 20


def filter_pseudonyms_for_source(ds_graph: nx.DiGraph, node: str):
    to_remove = set()

    for edge in ds_graph.edges:
        if ds_graph.edges[edge].get("pseudonym", False):
            lengths = {}
            for n in edge:
                lengths[n] = nx.shortest_path_length(ds_graph, node, n)
            to_remove.add(max(lengths, key=lambda x: lengths.get(x, 0)))
    for node in to_remove:
        ds_graph.remove_node(node)


def extract_address(node: str):
    return node.split("~")[1].split("@")[0]


def extract_concept(node: str, env: Environment):
    if node in env.alias_origin_lookup:
        return env.alias_origin_lookup[node]
    return env.concepts[node]


def filter_unique_graphs(graphs: list[list[str]]) -> list[list[str]]:
    unique_graphs: list[set[str]] = []
    for graph in graphs:
        if not any(set(graph).issubset(x) for x in unique_graphs):
            unique_graphs.append(set(graph))

    return [list(x) for x in unique_graphs]


def extract_ds_components(g: nx.DiGraph, nodelist: list[str]) -> list[list[str]]:
    graphs = []
    for node in g.nodes:
        if node.startswith("ds~"):
            local = g.copy()
            filter_pseudonyms_for_source(local, node)
            ds_graph: nx.DiGraph = nx.ego_graph(local, node, radius=10).copy()
            graphs.append(
                [
                    extract_address(x)
                    for x in ds_graph.nodes
                    if not str(x).startswith("ds~")
                ]
            )

    graphs = filter_unique_graphs(graphs)
    for node in nodelist:
        parsed = extract_address(node)
        if not any(parsed in x for x in graphs):
            graphs.append([parsed])
    return graphs


def determine_induced_minimal_nodes(
    G: nx.DiGraph,
    nodelist: list[str],
    environment: Environment,
    filter_downstream: bool,
    accept_partial: bool = False,
) -> nx.DiGraph | None:
    H: nx.Graph = nx.to_undirected(G).copy()
    nodes_to_remove = []
    concepts = nx.get_node_attributes(G, "concept")
    for node in G.nodes:
        if concepts.get(node):
            lookup = concepts[node]
            if lookup.derivation not in (PurposeLineage.BASIC, PurposeLineage.ROOT):
                nodes_to_remove.append(node)
            elif lookup.derivation == PurposeLineage.BASIC and G.out_degree(node) == 0:
                nodes_to_remove.append(node)
            # purge a node if we're already looking for all it's parents
            elif filter_downstream and lookup.derivation == PurposeLineage.BASIC:
                nodes_to_remove.append(node)

    H.remove_nodes_from(nodes_to_remove)

    H.remove_nodes_from(list(nx.isolates(H)))

    zero_out = list(x for x in H.nodes if G.out_degree(x) == 0 and x not in nodelist)
    while zero_out:
        H.remove_nodes_from(zero_out)
        zero_out = list(
            x for x in H.nodes if G.out_degree(x) == 0 and x not in nodelist
        )
    try:
        paths = nx.multi_source_dijkstra_path(H, nodelist)
    except nx.exception.NodeNotFound:
        return None
    H.remove_nodes_from(list(x for x in H.nodes if x not in paths))
    sG: nx.Graph = ax.steinertree.steiner_tree(H, nodelist).copy()
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
    # all concept nodes must have a parent

    if not all(
        [final.in_degree(node) > 0 for node in final.nodes if node.startswith("c~")]
    ):
        return None
    if not all([node in final.nodes for node in nodelist]):
        return None
    return final


def detect_ambiguity_and_raise(all_concepts, reduced_concept_sets) -> None:
    final_candidates: list[set[str]] = []
    common: set[str] = set()
    # find all values that show up in every join_additions
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


def resolve_weak_components(
    all_concepts: List[Concept],
    environment: Environment,
    environment_graph: nx.DiGraph,
    filter_downstream: bool = True,
    accept_partial: bool = False,
) -> list[list[Concept]] | None:

    break_flag = False
    found = []
    search_graph = environment_graph.copy()
    reduced_concept_sets: list[set[str]] = []

    # loop through, removing new nodes we find
    # to ensure there are not ambiguous loops
    # (if we did not care about raising ambiguity errors, we could just use the first one)
    count = 0
    node_list = [
        concept_to_node(c.with_default_grain())
        for c in all_concepts
        if "__preql_internal" not in c.address
    ]
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
            )

            if not g or not g.nodes:
                break_flag = True
                continue
            if not nx.is_weakly_connected(g):
                break_flag = True
                continue

            all_graph_concepts = [
                extract_concept(extract_address(node), environment)
                for node in g.nodes
                if node.startswith("c~")
            ]
            new = [
                x
                for x in all_graph_concepts
                if x.address not in [y.address for y in all_concepts]
            ]

            new_addresses = set([x.address for x in new])
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
            reduced_concept_sets.append(new_addresses)

        except nx.exception.NetworkXNoPath:
            break_flag = True
        if g and not g.nodes:
            break_flag = True
    if not found:
        return None

    detect_ambiguity_and_raise(all_concepts, reduced_concept_sets)

    # take our first one as the actual graph
    g = found[0]

    subgraphs: list[list[Concept]] = []
    # components = nx.strongly_connected_components(g)
    components = extract_ds_components(g, node_list)
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
    return subgraphs


def subgraphs_to_merge_node(
    concept_subgraphs: list[list[Concept]],
    depth: int,
    all_concepts: List[Concept],
    environment,
    g,
    source_concepts,
    history,
    conditions,
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
    for x in parents:
        for y in x.output_concepts:
            input_c.append(y)

    return MergeNode(
        input_concepts=unique(input_c, "address"),
        output_concepts=[x for x in all_concepts],
        environment=environment,
        g=g,
        parents=parents,
        depth=depth,
        conditions=conditions,
        # node_joins=[]
    )


def gen_merge_node(
    all_concepts: List[Concept],
    g: nx.DiGraph,
    environment: Environment,
    depth: int,
    source_concepts,
    accept_partial: bool = False,
    history: History | None = None,
    conditions: Conditional | None = None,
) -> Optional[MergeNode]:
    join_candidates: List[PathInfo] = []

    # inject new concepts into search, and identify if two dses can reach there
    if not join_candidates:
        for filter_downstream in [True, False]:
            weak_resolve = resolve_weak_components(
                all_concepts,
                environment,
                g,
                filter_downstream=filter_downstream,
                accept_partial=accept_partial,
            )
            if weak_resolve:
                log_graph = [[y.address for y in x] for x in weak_resolve]
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} Was able to resolve graph through weak component resolution - final graph {log_graph}"
                )
                return subgraphs_to_merge_node(
                    weak_resolve,
                    depth=depth,
                    all_concepts=all_concepts,
                    environment=environment,
                    g=g,
                    source_concepts=source_concepts,
                    history=history,
                    conditions=conditions,
                )
    if not join_candidates:
        return None
    join_additions: list[set[str]] = []
    for candidate in join_candidates:
        join_additions.append(candidate.reduced_concepts)

    common: set[str] = set()
    final_candidates: list[set[str]] = []
    # find all values that show up in every join_additions
    for ja in join_additions:
        if not common:
            common = ja
        else:
            common = common.intersection(ja)
        if all(ja.issubset(y) for y in join_additions):
            final_candidates.append(ja)

    if not final_candidates:
        filtered_paths = [x.difference(common) for x in join_additions]
        raise AmbiguousRelationshipResolutionException(
            f"Ambiguous concept join resolution fetching {[x.address for x in all_concepts]} - unique values in possible paths = {filtered_paths}. Include an additional concept to disambiguate",
            join_additions,
        )
    if not join_candidates:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} No additional join candidates could be found"
        )
        return None
    shortest: PathInfo = sorted(
        [x for x in join_candidates if x.reduced_concepts in final_candidates],
        key=lambda x: len(x.reduced_concepts),
    )[0]
    logger.info(f"{padding(depth)}{LOGGER_PREFIX} final path is {shortest.paths}")

    return subgraphs_to_merge_node(
        shortest.concept_subgraphs,
        depth=depth,
        all_concepts=all_concepts,
        environment=environment,
        g=g,
        source_concepts=source_concepts,
        history=history,
        conditions=conditions,
    )
    # parents = []
    # for graph in shortest.concept_subgraphs:
    #     logger.info(
    #         f"{padding(depth)}{LOGGER_PREFIX} fetching subgraph {[c.address for c in graph]}"
    #     )
    #     parent = source_concepts(
    #         mandatory_list=graph,
    #         environment=environment,
    #         g=g,
    #         depth=depth + 1,
    #         history=history,
    #     )
    #     if not parent:
    #         logger.info(
    #             f"{padding(depth)}{LOGGER_PREFIX} Unable to instantiate target subgraph"
    #         )
    #         return None
    #     logger.info(
    #         f"{padding(depth)}{LOGGER_PREFIX} finished subgraph fetch for {[c.address for c in graph]}, have parent {type(parent)}"
    #     )
    #     parents.append(parent)

    # return MergeNode(
    #     input_concepts=[
    #         environment.concepts[x]
    #         for x in shortest.reduced_concepts
    #         if environment.concepts[x].derivation != PurposeLineage.MERGE
    #     ],
    #     output_concepts=[
    #         x for x in all_concepts if x.derivation != PurposeLineage.MERGE
    #     ],
    #     environment=environment,
    #     g=g,
    #     parents=parents,
    #     depth=depth,
    #     conditions=conditions,
    # )
