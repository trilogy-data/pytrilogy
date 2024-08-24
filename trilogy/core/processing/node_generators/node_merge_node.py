from typing import List, Optional

from trilogy.core.models import Concept, Environment, Datasource, Conditional
from trilogy.core.processing.nodes import MergeNode, History, StrategyNode
import networkx as nx
from trilogy.core.graph_models import concept_to_node, datasource_to_node
from trilogy.core.env_processor import generate_adhoc_graph
from trilogy.core.processing.utility import PathInfo
from trilogy.constants import logger
from trilogy.utility import unique
from trilogy.core.exceptions import AmbiguousRelationshipResolutionException
from trilogy.core.processing.utility import padding
from trilogy.core.processing.graph_utils import extract_mandatory_subgraphs
from collections import defaultdict

LOGGER_PREFIX = "[GEN_MERGE_NODE]"


def filter_pseudonyms_for_source(ds_graph: nx.DiGraph, node: str):
    to_remove = set()

    for edge in ds_graph.edges:
        if ds_graph.edges[edge].get("pseudonym", False):
            lengths = {}
            for n in edge:
                lengths[n] = nx.shortest_path_length(ds_graph, node, n)
            to_remove.add(max(lengths, key=lengths.get))
    for node in to_remove:
        ds_graph.remove_node(node)
    

def reduce_path_concepts(paths:dict[str,list[str]], g:nx.DiGraph) -> set[str]:
    concept_nodes: List[Concept] = []
    # along our path, find all the concepts required
    for _, value in paths.items():
        concept_nodes += [g.nodes[v]["concept"] for v in value if v.startswith("c~")]
    final: List[Concept] = unique(concept_nodes, "address")
    return set([x.address for x in final])


def identify_ds_join_paths(
    all_concepts: List[Concept],
    g,
    datasource: Datasource,
    accept_partial: bool = False,
    fail: bool = False,
) -> PathInfo | None:
    all_found = True
    any_direct_found = False
    paths:dict[str, list[str]] = {}
    for bitem in all_concepts:
        item = bitem.with_default_grain()
        target_node = concept_to_node(item)
        try:
            path = nx.shortest_path(
                g,
                source=datasource_to_node(datasource),
                target=target_node,
            )
            paths[target_node] = path
            if sum([1 for x in path if x.startswith("ds~")]) == 1:
                any_direct_found = True
        except nx.exception.NodeNotFound:
            # TODO: support Verbose logging mode configuration and reenable these
            all_found = False
            if fail:
                raise
            return None
        except nx.exception.NetworkXNoPath:
            all_found = False
            if fail:
                raise
            return None
    if all_found and any_direct_found:
        partial = [
            c.concept
            for c in datasource.columns
            if not c.is_complete
            and c.concept.address in [x.address for x in all_concepts]
        ]
        if partial and not accept_partial:
            return None

        return PathInfo(
            paths=paths,
            datasource=datasource,
            reduced_concepts=reduce_path_concepts(paths, g),
            concept_subgraphs=extract_mandatory_subgraphs(paths, g),
        )
    return None


def extract_address(node: str):
    return node.split("~")[1].split("@")[0]


def extract_concept(node: str, env: Environment):
    if node in env.alias_origin_lookup:
        return env.alias_origin_lookup[node]
    return env.concepts[node]


def filter_unique_graphs(graphs: list[list[str]]) -> list[list[str]]:
    unique_graphs = []
    for graph in graphs:
        if not any(set(graph).issubset(x) for x in unique_graphs):
            unique_graphs.append(set(graph))

    return [list(x) for x in unique_graphs]


def extract_ds_components(g: nx.DiGraph) -> list[list[str]]:
    graphs = []
    for node in g.nodes:
        if node.startswith("ds~"):
            ds_graph: nx.DiGraph = nx.ego_graph(g, node, radius=10)
            filter_pseudonyms_for_source(ds_graph, node)
            graphs.append(
                [
                    extract_address(x)
                    for x in ds_graph.nodes
                    if not str(x).startswith("ds~")
                ]
            )
    graphs = filter_unique_graphs(graphs)
    return graphs


def set_to_key(s: set[str]):
    return ",".join(sorted(s))

def determine_induced_minimal_nodes(G:nx.DiGraph, nodelist):
    H:nx.Graph = nx.to_undirected(G).copy()

    from networkx.algorithms import approximation as ax

    H.remove_nodes_from(list(nx.isolates(H)))

    H.remove_nodes_from(list(x for x in H.nodes if G.out_degree(x) == 0 and x not in nodelist))
    # 
    paths = nx.multi_source_dijkstra_path(H, nodelist)
    H.remove_nodes_from(list(x for x in H.nodes if x not in paths))
    sG:nx.Graph = ax.steinertree.steiner_tree(H, nodelist).copy()
    final:nx.DiGraph = nx.subgraph(G, sG.nodes).copy()
    for edge in G.edges:
        if edge[1] in final.nodes and edge[0].startswith('ds~'):
            # sG.add_node(edge[0])
            final.add_edge(*edge)
    print(list(final.nodes))
    return final


def resolve_weak_components(
    all_concepts: List[Concept], environment: Environment, environment_graph: nx.DiGraph
) -> list[list[Concept]] | None:
    candidate_subgraph_arrays: list[list[list[Concept]]] = []
    # for x in list(environment.concepts.values())+list(environment.alias_origin_lookup.values()):
    #     # network = [x] + list(x.pseudonyms.values())
    #     c_targets = [*all_concepts, x]
    # g = generate_adhoc_graph(
    #     concepts=[*all_concepts, x],
    #     datasources=list(environment.datasources.values()),
    #     restrict_to_listed=True,
    # )
    try:
        g = determine_induced_minimal_nodes(environment_graph, [concept_to_node(c) for c in all_concepts if "__preql_internal" not in c.address])
    except nx.exception.NetworkXNoPath:
        return None
    if not g.nodes:
        return None
    weak = nx.is_weakly_connected(g)
    print('subgraph')
    print(weak)
    print(list(g.nodes()))
    # if x.address == 'store_sales.date.id':
    # from trilogy.hooks.graph_hook import GraphHook
    # GraphHook().query_graph_built(g)
    
    if weak:
        print('subgraph')
        print(weak)
        print(list(g.nodes()))
    subgraphs: list[Concept] = []
    # components = nx.strongly_connected_components(g)
    components = extract_ds_components(g)
    for component in components:
        # we need to take unique again as different addresses may map to the same concept
        sub_component = unique(
            [extract_concept(x, environment) for x in component], "address"
        )
        if not sub_component:
            continue
        subgraphs.append(sub_component)
    if subgraphs:
        candidate_subgraph_arrays.append(subgraphs)

    reduced_concept_sets: list[set[str]] = []

    reduced_concept_mapping: dict[set[str], list[list[list[Concept]]]] = defaultdict(
        list
    )

    for subgraph_array in candidate_subgraph_arrays:
        concepts: list[Concept] = []
        for y in subgraph_array:
            concepts += y
        unique_concepts = set([x.address for x in concepts])
        reduced_concept_sets.append(unique_concepts)
        # we could have multiple subgraphs with same output concepts
        # we'll pick one at random later
        reduced_concept_mapping[set_to_key(unique_concepts)].append(subgraph_array)

    # we found no additional concepts via weak enrichment
    if not reduced_concept_sets:
        return None

    common: set[str] = set()

    final_candidates: list[set[str]] = []

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
            message=f"Multiple weak components found for {[x.address for x in all_concepts]}, got {reduced_concept_sets}",
            parents=filtered_paths,
        )

    final = final_candidates[0]
    return reduced_concept_mapping[set_to_key(final)][0]


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

    for graph in concept_subgraphs:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} fetching subgraph {[c.address for c in graph]}"
        )
        parent = source_concepts(
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
            f"{padding(depth)}{LOGGER_PREFIX} finished subgraph fetch for {[c.address for c in graph]}, have parent {type(parent)}"
        )
        # raise ValueError(f"{padding(depth)}{LOGGER_PREFIX} finished subgraph fetch for {[c.address for c in graph]}, have parent {type(parent)}")
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
    # anchor on datasources
    final_all_concepts = []
    for x in all_concepts:
        final_all_concepts.append(x)
    for datasource in environment.datasources.values():
        path = identify_ds_join_paths(final_all_concepts, g, datasource, accept_partial)
        if path and path.reduced_concepts:
            join_candidates.append(path)

    # inject new concepts into search, and identify if two dses can reach there
    if not join_candidates:
        weak_resolve = resolve_weak_components(all_concepts, environment, g)
        if weak_resolve:
            log_graph = [[y.address for y in x] for x in weak_resolve]
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} Was able to resolve graph through weak component resolution - final graph {log_graph}"
            )
            raise ValueError(f"{padding(depth)}{LOGGER_PREFIX} Was able to resolve graph through weak component resolution - final graph {log_graph}")
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
