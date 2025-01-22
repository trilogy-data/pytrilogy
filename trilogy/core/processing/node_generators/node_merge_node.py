from typing import List, Optional

import networkx as nx
from networkx.algorithms import approximation as ax

from trilogy.constants import logger
from trilogy.core.enums import Derivation
from trilogy.core.exceptions import AmbiguousRelationshipResolutionException
from trilogy.core.graph_models import concept_to_node
from trilogy.core.models.build import BuildConcept, BuildConditional, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import History, MergeNode, StrategyNode
from trilogy.core.processing.utility import padding
from trilogy.utility import unique

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


def extract_concept(node: str, env: BuildEnvironment):
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
    # if we had no ego graphs, return all concepts
    if not graphs:
        return [[extract_address(node) for node in nodelist]]
    graphs = filter_unique_graphs(graphs)
    for node in nodelist:
        parsed = extract_address(node)
        if not any(parsed in x for x in graphs):
            graphs.append([parsed])
    return graphs


def determine_induced_minimal_nodes(
    G: nx.DiGraph,
    nodelist: list[str],
    environment: BuildEnvironment,
    filter_downstream: bool,
    accept_partial: bool = False,
) -> nx.DiGraph | None:
    H: nx.Graph = nx.to_undirected(G).copy()
    nodes_to_remove = []
    concepts = nx.get_node_attributes(G, "concept")

    for node in G.nodes:
        if concepts.get(node):
            lookup: BuildConcept = concepts[node]
            if lookup.derivation in (Derivation.CONSTANT,):
                nodes_to_remove.append(node)
            # purge a node if we're already looking for all it's parents
            if filter_downstream and lookup.derivation not in (Derivation.ROOT,):
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
    except nx.exception.NodeNotFound as e:
        logger.debug(f"Unable to find paths for {nodelist}- {str(e)}")
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
            f"Skipping graph for {nodelist} as missing nodes {missing} from {final.nodes}"
        )
        return None
    logger.debug(f"Found final graph {final.nodes}")
    return final


def detect_ambiguity_and_raise(
    all_concepts: list[BuildConcept], reduced_concept_sets: list[set[str]]
) -> None:
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
    subgraphs: list[list[BuildConcept]],
) -> list[list[BuildConcept]]:
    seen: list[set[str]] = []

    for graph in subgraphs:
        seen.append(set([x.address for x in graph]))
    final = []
    # sometimes w can get two subcomponents that are the same
    # due to alias resolution
    # if so, drop any that are strict subsets.
    for graph in subgraphs:
        set_x = set([x.address for x in graph])
        if any([set_x.issubset(y) and set_x != y for y in seen]):
            continue
        final.append(graph)
    return final


def resolve_weak_components(
    all_concepts: List[BuildConcept],
    environment: BuildEnvironment,
    environment_graph: nx.DiGraph,
    filter_downstream: bool = True,
    accept_partial: bool = False,
) -> list[list[BuildConcept]] | None:
    break_flag = False
    found = []
    search_graph = environment_graph.copy()
    reduced_concept_sets: list[set[str]] = []

    # loop through, removing new nodes we find
    # to ensure there are not ambiguous discovery paths
    # (if we did not care about raising ambiguity errors, we could just use the first one)
    count = 0
    node_list = [
        concept_to_node(c.with_default_grain())
        for c in all_concepts
        if "__preql_internal" not in c.address
    ]
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
            new = [x for x in all_graph_concepts if x.address not in all_concepts]

            new_addresses = set([x.address for x in new if x.address not in synonyms])

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

    subgraphs: list[list[BuildConcept]] = []
    # components = nx.strongly_connected_components(g)
    node_list = [x for x in g.nodes if x.startswith("c~")]
    components = extract_ds_components(g, node_list)
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
    final = filter_duplicate_subgraphs(subgraphs)
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
    for x in parents:
        for y in x.output_concepts:
            input_c.append(y)
    if len(parents) == 1 and enable_early_exit:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} only one parent node, exiting early w/ {[c.address for c in parents[0].output_concepts]}"
        )
        return parents[0]
    return MergeNode(
        input_concepts=unique(input_c, "address"),
        output_concepts=output_concepts,
        environment=environment,
        parents=parents,
        depth=depth,
        # hidden_concepts=[]
        # conditions=conditions,
        # conditions=search_conditions.conditional,
        # preexisting_conditions=search_conditions.conditional,
        # node_joins=[]
    )


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
    for filter_downstream in [True, False]:
        weak_resolve = resolve_weak_components(
            all_search_concepts,
            environment,
            g,
            filter_downstream=filter_downstream,
            accept_partial=accept_partial,
        )
        if not weak_resolve:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} wasn't able to resolve graph through intermediate concept injection with accept_partial {accept_partial}, filter_downstream {filter_downstream}"
            )
        else:
            log_graph = [[y.address for y in x] for x in weak_resolve]
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} Was able to resolve graph through weak component resolution - final graph {log_graph}"
            )
            for flat in log_graph:
                if set(flat) == set([x.address for x in all_search_concepts]):
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

    # one concept handling may need to be kicked to alias
    if len(all_search_concepts) == 1:
        concept = all_search_concepts[0]
        for v in concept.pseudonyms:
            test = subgraphs_to_merge_node(
                [[concept, environment.alias_origin_lookup[v]]],
                g=g,
                all_concepts=[concept],
                environment=environment,
                depth=depth,
                source_concepts=source_concepts,
                history=history,
                conditions=conditions,
                enable_early_exit=False,
                search_conditions=search_conditions,
                output_concepts=[concept],
            )
            if test:
                return test
    return None
