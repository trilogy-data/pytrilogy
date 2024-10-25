from typing import List, Optional

from trilogy.core.models import (
    Concept,
    Environment,
    Grain,
    Datasource,
    WhereClause,
    LooseConceptList,
)
from trilogy.core.processing.nodes import (
    MergeNode,
    StrategyNode,
    GroupNode,
    ConstantNode,
    SelectNode,
)
import networkx as nx
from trilogy.core.graph_models import concept_to_node
from trilogy.constants import logger
from trilogy.core.processing.utility import padding
from trilogy.core.enums import PurposeLineage

LOGGER_PREFIX = "[GEN_ROOT_MERGE_NODE]"


def extract_address(node: str):
    return node.split("~")[1].split("@")[0]


def get_graph_partial_nodes(
    g: nx.DiGraph, conditions: WhereClause | None
) -> dict[str, list[str]]:
    datasources: dict[str, Datasource] = nx.get_node_attributes(g, "datasource")
    partial: dict[str, list[str]] = {}
    for node in g.nodes:
        if node in datasources:
            ds = datasources[node]
            partial[node] = [concept_to_node(c) for c in ds.partial_concepts]
            if ds.non_partial_for and conditions == ds.non_partial_for:
                partial[node] = []

    return partial


def get_graph_grain_length(g: nx.DiGraph) -> dict[str, int]:
    datasources: dict[str, Datasource] = nx.get_node_attributes(g, "datasource")
    partial: dict[str, int] = {}
    for node in g.nodes:
        if node in datasources:
            partial[node] = len(datasources[node].grain.components)
    return partial


def create_pruned_concept_graph(
    g: nx.DiGraph,
    all_concepts: List[Concept],
    accept_partial: bool = False,
    conditions: WhereClause | None = None,
) -> nx.DiGraph:
    orig_g = g
    g = g.copy()
    target_addresses = set([c.address for c in all_concepts])
    concepts: dict[str, Concept] = nx.get_node_attributes(orig_g, "concept")
    datasources: dict[str, Datasource] = nx.get_node_attributes(orig_g, "datasource")
    relevant_concepts_pre = {
        n: x.address
        for n in g.nodes()
        # filter out synonyms
        if (x := concepts.get(n, None)) and x.address in target_addresses
    }
    relevant_concepts: list[str] = list(relevant_concepts_pre.keys())
    relevent_datasets: list[str] = []
    if not accept_partial:
        partial = {}
        partial = get_graph_partial_nodes(g, conditions)
        to_remove = []
        for edge in g.edges:
            if (
                edge[0] in datasources
                and (pnodes := partial.get(edge[0], []))
                and edge[1] in pnodes
            ):
                to_remove.append(edge)
            if (
                edge[1] in datasources
                and (pnodes := partial.get(edge[1], []))
                and edge[0] in pnodes
            ):
                to_remove.append(edge)
        for edge in to_remove:
            g.remove_edge(*edge)
    for n in g.nodes():
        if not n.startswith("ds~"):
            continue
        actual_neighbors = [
            x for x in relevant_concepts if x in (nx.all_neighbors(g, n))
        ]
        if actual_neighbors:
            relevent_datasets.append(n)

    # for injecting extra join concepts that are shared between datasets
    # use the original graph, pre-partial pruning
    for n in orig_g.nodes:
        # readd ignoring grain
        # we want to join inclusive of all concepts
        roots: dict[str, set[str]] = {}
        if n.startswith("c~") and n not in relevant_concepts:
            root = n.split("@")[0]
            neighbors = roots.get(root, set())
            for neighbor in nx.all_neighbors(orig_g, n):
                if neighbor in relevent_datasets:
                    neighbors.add(neighbor)
            if len(neighbors) > 1:
                relevant_concepts.append(n)
            roots[root] = set()
    g.remove_nodes_from(
        [
            n
            for n in g.nodes()
            if n not in relevent_datasets and n not in relevant_concepts
        ]
    )

    subgraphs = list(nx.connected_components(g.to_undirected()))
    if not subgraphs:
        return None
    if subgraphs and len(subgraphs) != 1:
        return None
    # add back any relevant edges that might have been partially filtered
    relevant = set(relevant_concepts + relevent_datasets)
    for edge in orig_g.edges():
        if edge[0] in relevant and edge[1] in relevant:
            g.add_edge(edge[0], edge[1])

    return g


def resolve_subgraphs(
    g: nx.DiGraph, conditions: WhereClause | None
) -> dict[str, list[str]]:
    datasources = [n for n in g.nodes if n.startswith("ds~")]
    subgraphs: dict[str, list[str]] = {
        ds: list(set(list(nx.all_neighbors(g, ds)))) for ds in datasources
    }
    partial_map = get_graph_partial_nodes(g, conditions)
    grain_length = get_graph_grain_length(g)
    concepts: dict[str, Concept] = nx.get_node_attributes(g, "concept")
    non_partial_map = {
        ds: [concepts[c].address for c in subgraphs[ds] if c not in partial_map[ds]]
        for ds in datasources
    }
    concept_map = {
        ds: [concepts[c].address for c in subgraphs[ds]] for ds in datasources
    }
    pruned_subgraphs = {}
    for key, nodes in subgraphs.items():
        value = non_partial_map[key]
        all_concepts = concept_map[key]
        is_subset = False
        matches = set()
        # Compare current list with other lists
        for other_key, other_all_concepts in concept_map.items():
            other_value = non_partial_map[other_key]
            # needs to be a subset of non partial and a subset of all
            if (
                key != other_key
                and set(value).issubset(set(other_value))
                and set(all_concepts).issubset(set(other_all_concepts))
            ):
                if len(value) < len(other_value):
                    is_subset = True
                    logger.debug(
                        f"Dropping subgraph {key} with {value} as it is a subset of {other_key} with {other_value}"
                    )
                    break
                elif len(value) == len(other_value) and len(all_concepts) == len(
                    other_all_concepts
                ):
                    matches.add(other_key)
                    matches.add(key)
        if matches:
            is_subset = key is not min(matches, key=lambda x: (grain_length[x], x))
        if not is_subset:
            pruned_subgraphs[key] = nodes
    return pruned_subgraphs


def create_select_node(
    ds_name: str,
    subgraph: list[str],
    accept_partial: bool,
    g,
    environment: Environment,
    depth: int,
) -> StrategyNode:
    ds_name = ds_name.split("~")[1]
    all_concepts = [
        environment.concepts[extract_address(c)] for c in subgraph if c.startswith("c~")
    ]

    all_lcl = LooseConceptList(concepts=all_concepts)
    if all([c.derivation == PurposeLineage.CONSTANT for c in all_concepts]):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} All concepts {[x.address for x in all_concepts]} are constants, returning constant node"
        )
        return ConstantNode(
            output_concepts=all_concepts,
            input_concepts=[],
            environment=environment,
            g=g,
            parents=[],
            depth=depth,
            # no partial for constants
            partial_concepts=[],
            force_group=False,
        )

    datasource = environment.datasources[ds_name]
    target_grain = Grain(components=all_concepts)
    force_group = False
    if not datasource.grain.issubset(target_grain):
        force_group = True
    partial_concepts = [
        c.concept
        for c in datasource.columns
        if not c.is_complete and c.concept in all_lcl
    ]
    partial_lcl = LooseConceptList(concepts=partial_concepts)
    nullable_concepts = [
        c.concept for c in datasource.columns if c.is_nullable and c.concept in all_lcl
    ]
    nullable_lcl = LooseConceptList(concepts=nullable_concepts)

    bcandidate: StrategyNode = SelectNode(
        input_concepts=[c.concept for c in datasource.columns],
        output_concepts=all_concepts,
        environment=environment,
        g=g,
        parents=[],
        depth=depth,
        partial_concepts=[c for c in all_concepts if c in partial_lcl],
        nullable_concepts=[c for c in all_concepts if c in nullable_lcl],
        accept_partial=accept_partial,
        datasource=datasource,
        grain=Grain(components=all_concepts),
        conditions=datasource.where.conditional if datasource.where else None,
    )

    # we need to nest the group node one further
    if force_group is True:
        candidate: StrategyNode = GroupNode(
            output_concepts=all_concepts,
            input_concepts=all_concepts,
            environment=environment,
            g=g,
            parents=[bcandidate],
            depth=depth,
            partial_concepts=bcandidate.partial_concepts,
            nullable_concepts=bcandidate.nullable_concepts,
        )
    else:
        candidate = bcandidate
    return candidate


def gen_select_merge_node(
    all_concepts: List[Concept],
    g: nx.DiGraph,
    environment: Environment,
    depth: int,
    accept_partial: bool = False,
    conditions: WhereClause | None = None,
) -> Optional[StrategyNode]:
    non_constant = [c for c in all_concepts if c.derivation != PurposeLineage.CONSTANT]
    constants = [c for c in all_concepts if c.derivation == PurposeLineage.CONSTANT]
    if not non_constant and constants:
        return ConstantNode(
            output_concepts=constants,
            input_concepts=[],
            environment=environment,
            g=g,
            parents=[],
            depth=depth,
            partial_concepts=[],
            force_group=False,
        )
    for attempt in [False, True]:
        pruned_concept_graph = create_pruned_concept_graph(
            g, non_constant, attempt, conditions
        )
        if pruned_concept_graph:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} found covering graph w/ partial flag {attempt}"
            )
            break

    if not pruned_concept_graph:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no covering graph found {attempt}"
        )
        return None

    sub_nodes = resolve_subgraphs(pruned_concept_graph, conditions)

    logger.info(f"{padding(depth)}{LOGGER_PREFIX} fetching subgraphs {sub_nodes}")
    parents = [
        create_select_node(
            k,
            subgraph,
            g=g,
            accept_partial=accept_partial,
            environment=environment,
            depth=depth,
        )
        for k, subgraph in sub_nodes.items()
    ]
    if not parents:
        return None

    if constants:
        parents.append(
            ConstantNode(
                output_concepts=constants,
                input_concepts=[],
                environment=environment,
                g=g,
                parents=[],
                depth=depth,
                partial_concepts=[],
                force_group=False,
            )
        )

    if len(parents) == 1:
        return parents[0]
    return MergeNode(
        output_concepts=all_concepts,
        input_concepts=non_constant,
        environment=environment,
        g=g,
        depth=depth,
        parents=parents,
    )
