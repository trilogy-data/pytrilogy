from functools import reduce
from typing import List, Optional

import networkx as nx

from trilogy.constants import logger
from trilogy.core.enums import Derivation
from trilogy.core.graph_models import concept_to_node
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildGrain,
    BuildWhereClause,
    LooseBuildConceptList,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.select_helpers.datasource_injection import (
    get_union_sources,
)
from trilogy.core.processing.nodes import (
    ConstantNode,
    GroupNode,
    MergeNode,
    SelectNode,
    StrategyNode,
)
from trilogy.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_ROOT_MERGE_NODE]"


def extract_address(node: str):
    return node.split("~")[1].split("@")[0]


def get_graph_partial_nodes(
    g: nx.DiGraph, conditions: BuildWhereClause | None
) -> dict[str, list[str]]:
    datasources: dict[str, BuildDatasource | list[BuildDatasource]] = (
        nx.get_node_attributes(g, "datasource")
    )
    partial: dict[str, list[str]] = {}
    for node in g.nodes:
        if node in datasources:
            ds = datasources[node]
            if not isinstance(ds, list):

                if ds.non_partial_for and conditions == ds.non_partial_for:
                    partial[node] = []
                    continue
                partial[node] = [concept_to_node(c) for c in ds.partial_concepts]
                ds = [ds]
            # assume union sources have no partial
            else:
                partial[node] = []

    return partial


def get_graph_exact_match(
    g: nx.DiGraph, conditions: BuildWhereClause | None
) -> set[str]:
    datasources: dict[str, BuildDatasource | list[BuildDatasource]] = (
        nx.get_node_attributes(g, "datasource")
    )
    exact: set[str] = set()
    for node in g.nodes:
        if node in datasources:
            ds = datasources[node]
            if not isinstance(ds, list):
                if ds.non_partial_for and conditions == ds.non_partial_for:
                    exact.add(node)
                    continue
            else:
                continue

    return exact


def get_graph_grains(g: nx.DiGraph) -> dict[str, list[str]]:
    datasources: dict[str, BuildDatasource | list[BuildDatasource]] = (
        nx.get_node_attributes(g, "datasource")
    )
    grain_length: dict[str, list[str]] = {}
    for node in g.nodes:
        if node in datasources:
            base: set[str] = set()
            lookup = datasources[node]
            if not isinstance(lookup, list):
                lookup = [lookup]
            assert isinstance(lookup, list)
            grain_length[node] = reduce(
                lambda x, y: x.union(y.grain.components), lookup, base  # type: ignore
            )
    return grain_length


def create_pruned_concept_graph(
    g: nx.DiGraph,
    all_concepts: List[BuildConcept],
    datasources: list[BuildDatasource],
    accept_partial: bool = False,
    conditions: BuildWhereClause | None = None,
    depth: int = 0,
) -> nx.DiGraph:
    orig_g = g
    g = g.copy()
    union_options = get_union_sources(datasources, all_concepts)
    for ds_list in union_options:
        node_address = "ds~" + "-".join([x.name for x in ds_list])
        common: set[BuildConcept] = set.intersection(
            *[set(x.output_concepts) for x in ds_list]
        )
        g.add_node(node_address, datasource=ds_list)
        for c in common:
            g.add_edge(node_address, concept_to_node(c))

    target_addresses = set([c.address for c in all_concepts])
    concepts: dict[str, BuildConcept] = nx.get_node_attributes(orig_g, "concept")
    datasource_map: dict[str, BuildDatasource | list[BuildDatasource]] = (
        nx.get_node_attributes(orig_g, "datasource")
    )
    relevant_concepts_pre = {
        n: x.address
        for n in g.nodes()
        # filter out synonyms
        if (x := concepts.get(n, None)) and x.address in target_addresses
    }
    # from trilogy.hooks.graph_hook import GraphHook
    # GraphHook().query_graph_built(g)
    relevant_concepts: list[str] = list(relevant_concepts_pre.keys())
    relevent_datasets: list[str] = []
    if not accept_partial:
        partial = {}
        partial = get_graph_partial_nodes(g, conditions)
        to_remove = []
        for edge in g.edges:
            if (
                edge[0] in datasource_map
                and (pnodes := partial.get(edge[0], []))
                and edge[1] in pnodes
            ):
                to_remove.append(edge)
            if (
                edge[1] in datasource_map
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
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} cannot resolve root graph - no subgraphs after node prune"
        )
        return None
    # from trilogy.hooks.graph_hook import GraphHook
    # GraphHook().query_graph_built(g)
    if subgraphs and len(subgraphs) != 1:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} cannot resolve root graph - subgraphs are split - have {len(subgraphs)} from {subgraphs}"
        )
        return None
    # add back any relevant edges that might have been partially filtered
    relevant = set(relevant_concepts + relevent_datasets)
    for edge in orig_g.edges():
        if edge[0] in relevant and edge[1] in relevant:
            g.add_edge(edge[0], edge[1])
    # if we have no ds nodes at all, for non constant, we can't find it
    if not any([n.startswith("ds~") for n in g.nodes]):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} cannot resolve root graph - No datasource nodes found"
        )
        return None
    return g


def resolve_subgraphs(
    g: nx.DiGraph, relevant: list[BuildConcept], conditions: BuildWhereClause | None
) -> dict[str, list[str]]:
    """When we have multiple distinct subgraphs within our matched
    nodes that can satisfy a query, resolve which one of those we should
    ultimately ues.
    This should generally return one subgraph for each
    unique set of sub concepts that can be referenced,
    discarding duplicates.
    Duplicate subgraphs will be resolved based on which
    ones are most 'optimal' to use, a hueristic
    that can evolve in the future but is currently based on
    cardinality."""
    datasources = [n for n in g.nodes if n.startswith("ds~")]
    subgraphs: dict[str, list[str]] = {
        ds: list(set(list(nx.all_neighbors(g, ds)))) for ds in datasources
    }
    partial_map = get_graph_partial_nodes(g, conditions)
    exact_map = get_graph_exact_match(g, conditions)
    grain_length = get_graph_grains(g)
    concepts: dict[str, BuildConcept] = nx.get_node_attributes(g, "concept")
    non_partial_map = {
        ds: [concepts[c].address for c in subgraphs[ds] if c not in partial_map[ds]]
        for ds in datasources
    }
    concept_map = {
        ds: [concepts[c].address for c in subgraphs[ds]] for ds in datasources
    }
    pruned_subgraphs = {}

    def score_node(input: str):
        logger.debug(f"scoring node {input}")
        grain = grain_length[input]
        # first - go for lowest grain
        # but if the object we want is in the grain, treat that as "free"
        # ex - pick source with grain(product_id) over grain(order_id)
        # when going for product_id
        score = (
            len(list(grain)) - sum([1 for x in concept_map[input] if x in grain]),
            # then check if it's an exact condition match
            0 if input in exact_map else 0.5,
            # last, number of concepts
            len(subgraphs[input]),
            input,
        )
        logger.debug(score)
        return score

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
                elif len(value) == len(other_value) and len(all_concepts) == len(
                    other_all_concepts
                ):
                    matches.add(other_key)
                    matches.add(key)
        if matches and not is_subset:
            min_node = min(matches, key=score_node)
            logger.debug(f"minimum source score is {min_node}")
            is_subset = key is not min(matches, key=score_node)
        if not is_subset:
            pruned_subgraphs[key] = nodes

    final_nodes: set[str] = set([n for v in pruned_subgraphs.values() for n in v])
    relevant_concepts_pre = {
        n: x.address
        for n in g.nodes()
        # filter out synonyms
        if (x := concepts.get(n, None)) and x.address in relevant
    }
    for node in final_nodes:
        keep = True
        if node.startswith("c~") and node not in relevant_concepts_pre:
            keep = (
                sum(
                    [
                        1 if node in sub_nodes else 0
                        for _, sub_nodes in pruned_subgraphs.items()
                    ]
                )
                > 1
            )
        if not keep:
            logger.debug(f"Pruning node {node} as irrelevant after subgraph resolution")
            pruned_subgraphs = {
                k: [n for n in v if n != node] for k, v in pruned_subgraphs.items()
            }

    return pruned_subgraphs


def create_datasource_node(
    datasource: BuildDatasource,
    all_concepts: List[BuildConcept],
    accept_partial: bool,
    environment: BuildEnvironment,
    depth: int,
    conditions: BuildWhereClause | None = None,
) -> tuple[StrategyNode, bool]:
    logger.info(all_concepts)
    target_grain = BuildGrain.from_concepts(all_concepts, environment=environment)
    force_group = False
    if not datasource.grain.issubset(target_grain):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX}_DS_NODE Select node must be wrapped in group, {datasource.grain} not subset of target grain {target_grain}"
        )
        force_group = True
    else:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX}_DS_NODE Select node grain {datasource.grain} is subset of target grain {target_grain}, no group required"
        )
    if not datasource.grain.components:
        force_group = True
    partial_concepts = [
        c.concept
        for c in datasource.columns
        if not c.is_complete and c.concept.address in all_concepts
    ]
    partial_lcl = LooseBuildConceptList(concepts=partial_concepts)
    nullable_concepts = [
        c.concept
        for c in datasource.columns
        if c.is_nullable and c.concept.address in all_concepts
    ]
    nullable_lcl = LooseBuildConceptList(concepts=nullable_concepts)
    partial_is_full = conditions and (conditions == datasource.non_partial_for)

    datasource_conditions = datasource.where.conditional if datasource.where else None

    return (
        SelectNode(
            input_concepts=[c.concept for c in datasource.columns],
            output_concepts=all_concepts,
            environment=environment,
            parents=[],
            depth=depth,
            partial_concepts=(
                [] if partial_is_full else [c for c in all_concepts if c in partial_lcl]
            ),
            nullable_concepts=[c for c in all_concepts if c in nullable_lcl],
            accept_partial=accept_partial,
            datasource=datasource,
            grain=datasource.grain,
            conditions=datasource_conditions,
            preexisting_conditions=(
                conditions.conditional if partial_is_full and conditions else None
            ),
        ),
        force_group,
    )


def create_select_node(
    ds_name: str,
    subgraph: list[str],
    accept_partial: bool,
    g,
    environment: BuildEnvironment,
    depth: int,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode:

    all_concepts = [
        environment.concepts[extract_address(c)] for c in subgraph if c.startswith("c~")
    ]

    if all([c.derivation == Derivation.CONSTANT for c in all_concepts]):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} All concepts {[x.address for x in all_concepts]} are constants, returning constant node"
        )
        return ConstantNode(
            output_concepts=all_concepts,
            input_concepts=[],
            environment=environment,
            parents=[],
            depth=depth,
            # no partial for constants
            partial_concepts=[],
            force_group=False,
            preexisting_conditions=conditions.conditional if conditions else None,
        )

    datasource: dict[str, BuildDatasource | list[BuildDatasource]] = (
        nx.get_node_attributes(g, "datasource")[ds_name]
    )
    if isinstance(datasource, BuildDatasource):
        bcandidate, force_group = create_datasource_node(
            datasource,
            all_concepts,
            accept_partial,
            environment,
            depth,
            conditions=conditions,
        )

    elif isinstance(datasource, list):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} generating union node parents with condition {conditions}"
        )
        from trilogy.core.processing.nodes.union_node import UnionNode

        force_group = False
        parents = []
        for x in datasource:
            subnode, fg = create_datasource_node(
                x,
                all_concepts,
                accept_partial,
                environment,
                depth,
                conditions=conditions,
            )
            parents.append(subnode)
            force_group = force_group or fg
        logger.info(f"{padding(depth)}{LOGGER_PREFIX} generating union node")
        bcandidate = UnionNode(
            output_concepts=all_concepts,
            input_concepts=all_concepts,
            environment=environment,
            parents=parents,
            depth=depth,
        )
    else:
        raise ValueError(f"Unknown datasource type {datasource}")

    # we need to nest the group node one further
    if force_group is True:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} source requires group before consumption."
        )
        candidate: StrategyNode = GroupNode(
            output_concepts=all_concepts,
            input_concepts=all_concepts,
            environment=environment,
            parents=[bcandidate],
            depth=depth,
            partial_concepts=bcandidate.partial_concepts,
            nullable_concepts=bcandidate.nullable_concepts,
            preexisting_conditions=bcandidate.preexisting_conditions,
            force_group=force_group,
        )
    else:

        candidate = bcandidate
    return candidate


def gen_select_merge_node(
    all_concepts: List[BuildConcept],
    g: nx.DiGraph,
    environment: BuildEnvironment,
    depth: int,
    accept_partial: bool = False,
    conditions: BuildWhereClause | None = None,
) -> Optional[StrategyNode]:
    non_constant = [c for c in all_concepts if c.derivation != Derivation.CONSTANT]
    constants = [c for c in all_concepts if c.derivation == Derivation.CONSTANT]
    if not non_constant and constants:
        return ConstantNode(
            output_concepts=constants,
            input_concepts=[],
            environment=environment,
            parents=[],
            depth=depth,
            partial_concepts=[],
            force_group=False,
            preexisting_conditions=conditions.conditional if conditions else None,
        )
    for attempt in [False, True]:
        pruned_concept_graph = create_pruned_concept_graph(
            g,
            non_constant,
            accept_partial=attempt,
            conditions=conditions,
            datasources=list([x for x in environment.datasources.values()]),
            depth=depth,
        )
        if pruned_concept_graph:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} found covering graph w/ partial flag {attempt}"
            )
            break

    if not pruned_concept_graph:
        logger.info(f"{padding(depth)}{LOGGER_PREFIX} no covering graph found.")
        return None

    sub_nodes = resolve_subgraphs(
        pruned_concept_graph, relevant=non_constant, conditions=conditions
    )

    logger.info(f"{padding(depth)}{LOGGER_PREFIX} fetching subgraphs {sub_nodes}")

    parents = [
        create_select_node(
            k,
            subgraph,
            g=pruned_concept_graph,
            accept_partial=accept_partial,
            environment=environment,
            depth=depth,
            conditions=conditions,
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
                parents=[],
                depth=depth,
                partial_concepts=[],
                force_group=False,
                preexisting_conditions=conditions.conditional if conditions else None,
            )
        )

    if len(parents) == 1:
        return parents[0]
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} Multiple parent DS nodes resolved - {[type(x) for x in parents]}, wrapping in merge"
    )

    preexisting_conditions = None
    if conditions and all(
        [
            x.preexisting_conditions
            and x.preexisting_conditions == conditions.conditional
            for x in parents
        ]
    ):
        preexisting_conditions = conditions.conditional
    base = MergeNode(
        output_concepts=all_concepts,
        input_concepts=non_constant,
        environment=environment,
        depth=depth,
        parents=parents,
        preexisting_conditions=preexisting_conditions,
    )

    return base
