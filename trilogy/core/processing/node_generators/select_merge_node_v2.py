from functools import reduce, lru_cache
from typing import TYPE_CHECKING, List, Optional, Dict, Set, Tuple

import networkx as nx

from trilogy.constants import logger
from trilogy.core.enums import Derivation
from trilogy.core.graph_models import (
    concept_to_node,
    get_graph_exact_match,
    prune_sources_for_conditions,
)
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

if TYPE_CHECKING:
    from trilogy.core.processing.nodes.union_node import UnionNode

LOGGER_PREFIX = "[GEN_ROOT_MERGE_NODE]"


# Cache for expensive string operations
@lru_cache(maxsize=1024)
def extract_address(node: str) -> str:
    """Cached version of address extraction from node string."""
    return node.split("~")[1].split("@")[0]


@lru_cache(maxsize=1024)
def get_node_type(node: str) -> str:
    """Extract node type prefix efficiently."""
    if "~" in node:
        return node.split("~")[0] + "~"
    return ""


class GraphAttributeCache:
    """Cache for expensive NetworkX attribute operations."""

    def __init__(self, graph: nx.DiGraph):
        self.graph = graph
        self._datasources = None
        self._concepts = None
        self._ds_nodes = None
        self._concept_nodes = None

    @property
    def datasources(self) -> Dict:
        if self._datasources is None:
            self._datasources = nx.get_node_attributes(self.graph, "datasource")
        return self._datasources

    @property
    def concepts(self) -> Dict:
        if self._concepts is None:
            self._concepts = nx.get_node_attributes(self.graph, "concept")
        return self._concepts

    @property
    def ds_nodes(self) -> Set[str]:
        if self._ds_nodes is None:
            self._ds_nodes = {n for n in self.graph.nodes if n.startswith("ds~")}
        return self._ds_nodes

    @property
    def concept_nodes(self) -> Set[str]:
        if self._concept_nodes is None:
            self._concept_nodes = {n for n in self.graph.nodes if n.startswith("c~")}
        return self._concept_nodes


def get_graph_partial_nodes(
    g: nx.DiGraph,
    conditions: BuildWhereClause | None,
    cache: GraphAttributeCache = None,
) -> dict[str, list[str]]:
    """Optimized version with caching and early returns."""
    if cache is None:
        cache = GraphAttributeCache(g)

    datasources = cache.datasources
    partial: dict[str, list[str]] = {}

    for node in cache.ds_nodes:  # Only iterate over datasource nodes
        if node not in datasources:
            continue

        ds = datasources[node]
        if not isinstance(ds, list):
            # Early return for non-partial nodes
            if ds.non_partial_for and conditions == ds.non_partial_for:
                partial[node] = []
                continue
            partial[node] = [concept_to_node(c) for c in ds.partial_concepts]
        else:
            # Union sources have no partial
            partial[node] = []

    return partial


def get_graph_grains(
    g: nx.DiGraph, cache: GraphAttributeCache = None
) -> dict[str, list[str]]:
    """Optimized version using set.update() instead of reduce with union."""
    if cache is None:
        cache = GraphAttributeCache(g)

    datasources = cache.datasources
    grain_length: dict[str, list[str]] = {}

    for node in cache.ds_nodes:  # Only iterate over datasource nodes
        if node not in datasources:
            continue

        lookup = datasources[node]
        if not isinstance(lookup, list):
            lookup = [lookup]

        # Optimized set building - avoid reduce and intermediate sets
        components = set()
        for item in lookup:
            components.update(item.grain.components)
        grain_length[node] = components

    return grain_length


def subgraph_is_complete(
    nodes: list[str], targets: set[str], mapping: dict[str, str], g: nx.DiGraph
) -> bool:
    """Optimized with early returns and reduced iterations."""
    # Early return check for target presence
    mapped = set()
    for n in nodes:
        mapped.add(mapping.get(n, n))
        # Early return if we've found all targets
        if len(mapped) >= len(targets) and targets.issubset(mapped):
            break

    if not targets.issubset(mapped):
        return False

    # Check datasource edges more efficiently
    has_ds_edge = {k: False for k in targets}

    # Early return optimization - stop checking once all targets have edges
    found_count = 0
    for n in nodes:
        if not n.startswith("c~"):
            continue

        concept_key = mapping.get(n, n)
        if concept_key in has_ds_edge and not has_ds_edge[concept_key]:
            # Check for datasource neighbor
            for neighbor in nx.neighbors(g, n):
                if neighbor.startswith("ds~"):
                    has_ds_edge[concept_key] = True
                    found_count += 1
                    break

            # Early return if all targets have datasource edges
            if found_count == len(targets):
                return True

    return all(has_ds_edge.values())


def create_pruned_concept_graph(
    g: nx.DiGraph,
    all_concepts: List[BuildConcept],
    datasources: list[BuildDatasource],
    accept_partial: bool = False,
    conditions: BuildWhereClause | None = None,
    depth: int = 0,
) -> nx.DiGraph:
    """Optimized version with caching and batch operations."""
    orig_g = g
    orig_cache = GraphAttributeCache(orig_g)

    g = g.copy()
    union_options = get_union_sources(datasources, all_concepts)

    # Batch edge additions for union sources
    edges_to_add = []
    for ds_list in union_options:
        node_address = "ds~" + "-".join([x.name for x in ds_list])
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} injecting potentially relevant union datasource {node_address}"
        )
        common: set[BuildConcept] = set.intersection(
            *[set(x.output_concepts) for x in ds_list]
        )
        g.add_node(node_address, datasource=ds_list)

        # Collect edges for batch addition
        for c in common:
            c_node = concept_to_node(c)
            edges_to_add.extend([(node_address, c_node), (c_node, node_address)])

    # Batch add all edges at once
    if edges_to_add:
        g.add_edges_from(edges_to_add)

    prune_sources_for_conditions(g, accept_partial, conditions)

    # Create cache for the modified graph
    g_cache = GraphAttributeCache(g)

    target_addresses = set([c.address for c in all_concepts])
    concepts = orig_cache.concepts
    datasource_map = orig_cache.datasources

    # Optimized filtering with early termination
    relevant_concepts_pre = {}
    for n in g_cache.concept_nodes:  # Only iterate over concept nodes
        if n in concepts:
            concept = concepts[n]
            if concept.address in target_addresses:
                relevant_concepts_pre[n] = concept.address

    relevant_concepts: list[str] = list(relevant_concepts_pre.keys())
    relevent_datasets: list[str] = []

    if not accept_partial:
        partial = get_graph_partial_nodes(g, conditions, g_cache)
        edges_to_remove = []

        # Collect edges to remove
        for edge in g.edges:
            if edge[0] in datasource_map and edge[0] in partial:
                if edge[1] in partial[edge[0]]:
                    edges_to_remove.append(edge)
            if edge[1] in datasource_map and edge[1] in partial:
                if edge[0] in partial[edge[1]]:
                    edges_to_remove.append(edge)

        # Batch remove edges
        if edges_to_remove:
            g.remove_edges_from(edges_to_remove)

    # Find relevant datasets more efficiently
    relevant_concepts_set = set(relevant_concepts)
    for n in g_cache.ds_nodes:  # Only iterate over datasource nodes
        # Check if any relevant concepts are neighbors
        if any(
            neighbor in relevant_concepts_set for neighbor in nx.all_neighbors(g, n)
        ):
            relevent_datasets.append(n)

    # Handle additional join concepts
    roots: dict[str, set[str]] = {}
    for n in orig_cache.concept_nodes:  # Only iterate over concept nodes
        if n not in relevant_concepts:
            root = n.split("@")[0]
            neighbors = roots.get(root, set())
            for neighbor in nx.all_neighbors(orig_g, n):
                if neighbor in relevent_datasets:
                    neighbors.add(neighbor)
            if len(neighbors) > 1:
                relevant_concepts.append(n)
            roots[root] = neighbors

    # Remove irrelevant nodes
    nodes_to_keep = set(relevent_datasets + relevant_concepts)
    nodes_to_remove = [n for n in g.nodes() if n not in nodes_to_keep]
    if nodes_to_remove:
        g.remove_nodes_from(nodes_to_remove)

    # Check subgraphs
    subgraphs = list(nx.connected_components(g.to_undirected()))
    subgraphs = [
        s
        for s in subgraphs
        if subgraph_is_complete(list(s), target_addresses, relevant_concepts_pre, g)
    ]

    if not subgraphs:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} cannot resolve root graph - no subgraphs after node prune"
        )
        return None

    if subgraphs and len(subgraphs) != 1:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} cannot resolve root graph - subgraphs are split - have {len(subgraphs)} from {subgraphs}"
        )
        return None

    # Add back relevant edges - batch operation
    relevant = set(relevant_concepts + relevent_datasets)
    edges_to_add = []
    for edge in orig_g.edges():
        if edge[0] in relevant and edge[1] in relevant and not g.has_edge(*edge):
            edges_to_add.append(edge)
    if edges_to_add:
        g.add_edges_from(edges_to_add)

    # Early return check
    if not any(n.startswith("ds~") for n in g.nodes):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} cannot resolve root graph - No datasource nodes found"
        )
        return None

    return g


def resolve_subgraphs(
    g: nx.DiGraph,
    relevant: list[BuildConcept],
    accept_partial: bool,
    conditions: BuildWhereClause | None,
    depth: int = 0,
) -> dict[str, list[str]]:
    """Optimized version with caching and reduced iterations."""
    cache = GraphAttributeCache(g)
    datasources = list(cache.ds_nodes)

    # Build subgraphs more efficiently
    subgraphs: dict[str, list[str]] = {}
    for ds in datasources:
        # Use set to avoid duplicates from the start
        subgraphs[ds] = list(set(nx.all_neighbors(g, ds)))

    partial_map = get_graph_partial_nodes(g, conditions, cache)
    exact_map = get_graph_exact_match(g, accept_partial, conditions)
    grain_length = get_graph_grains(g, cache)
    concepts = cache.concepts

    # Pre-compute concept addresses for all datasources
    non_partial_map = {}
    concept_map = {}
    for ds in datasources:
        ds_concepts = subgraphs[ds]
        partial_concepts = set(partial_map.get(ds, []))

        non_partial_map[ds] = [
            concepts[c].address
            for c in ds_concepts
            if c in concepts and c not in partial_concepts
        ]
        concept_map[ds] = [concepts[c].address for c in ds_concepts if c in concepts]

    pruned_subgraphs = {}

    def score_node(input: str) -> tuple:
        """Optimized scoring function."""
        logger.debug(f"{padding(depth)}{LOGGER_PREFIX} scoring node {input}")
        grain = grain_length[input]
        concept_addresses = concept_map[input]

        # Calculate score components
        grain_score = len(grain) - sum(1 for x in concept_addresses if x in grain)
        exact_match_score = 0 if input in exact_map else 0.5
        concept_count = len(subgraphs[input])

        score = (grain_score, exact_match_score, concept_count, input)
        logger.debug(f"{padding(depth)}{LOGGER_PREFIX} node {input} has score {score}")
        return score

    # Optimize subset detection with early termination
    for key in subgraphs:
        value = non_partial_map[key]
        all_concepts = concept_map[key]
        is_subset = False
        matches = set()

        # Early termination optimization
        value_set = set(value)
        all_concepts_set = set(all_concepts)

        for other_key in concept_map:
            if key == other_key:
                continue

            other_value = non_partial_map[other_key]
            other_all_concepts = concept_map[other_key]

            # Quick check before detailed comparison
            if len(value) > len(other_value) or len(all_concepts) > len(
                other_all_concepts
            ):
                continue

            other_value_set = set(other_value)
            other_all_concepts_set = set(other_all_concepts)

            if value_set.issubset(other_value_set) and all_concepts_set.issubset(
                other_all_concepts_set
            ):
                if len(value) < len(other_value):
                    is_subset = True
                    logger.debug(
                        f"{padding(depth)}{LOGGER_PREFIX} Dropping subgraph {key} with {value} as it is a subset of {other_key} with {other_value}"
                    )
                    break  # Early termination
                elif len(value) == len(other_value) and len(all_concepts) == len(
                    other_all_concepts
                ):
                    matches.add(other_key)
                    matches.add(key)

        if matches and not is_subset:
            min_node = min(matches, key=score_node)
            logger.debug(
                f"{padding(depth)}{LOGGER_PREFIX} minimum source score is {min_node}"
            )
            is_subset = key != min_node

        if not is_subset:
            pruned_subgraphs[key] = subgraphs[key]

    # Final node pruning - optimized
    final_nodes: set[str] = set()
    for v in pruned_subgraphs.values():
        final_nodes.update(v)

    relevant_concepts_pre = {
        n: concepts[n].address
        for n in cache.concept_nodes
        if n in concepts and concepts[n].address in relevant
    }

    # Count node occurrences once
    node_counts = {}
    for node in final_nodes:
        if node.startswith("c~") and node not in relevant_concepts_pre:
            node_counts[node] = sum(
                1 for sub_nodes in pruned_subgraphs.values() if node in sub_nodes
            )

    # Filter nodes based on counts
    nodes_to_remove = {node for node, count in node_counts.items() if count <= 1}

    if nodes_to_remove:
        for key in pruned_subgraphs:
            pruned_subgraphs[key] = [
                n for n in pruned_subgraphs[key] if n not in nodes_to_remove
            ]
            logger.debug(
                f"{padding(depth)}{LOGGER_PREFIX} Pruning nodes {nodes_to_remove} as irrelevant after subgraph resolution"
            )

    return pruned_subgraphs


def create_datasource_node(
    datasource: BuildDatasource,
    all_concepts: List[BuildConcept],
    accept_partial: bool,
    environment: BuildEnvironment,
    depth: int,
    conditions: BuildWhereClause | None = None,
) -> tuple[StrategyNode, bool]:

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

    # Optimized concept filtering using sets
    all_concept_addresses = {c.address for c in all_concepts}

    partial_concepts = [
        c.concept
        for c in datasource.columns
        if not c.is_complete and c.concept.address in all_concept_addresses
    ]

    partial_lcl = LooseBuildConceptList(concepts=partial_concepts)

    nullable_concepts = [
        c.concept
        for c in datasource.columns
        if c.is_nullable and c.concept.address in all_concept_addresses
    ]

    nullable_lcl = LooseBuildConceptList(concepts=nullable_concepts)
    partial_is_full = conditions and (conditions == datasource.non_partial_for)

    datasource_conditions = datasource.where.conditional if datasource.where else None
    rval = SelectNode(
        input_concepts=[c.concept for c in datasource.columns],
        output_concepts=sorted(all_concepts, key=lambda x: x.address),
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
    )
    return (
        rval,
        force_group,
    )


def create_union_datasource(
    datasource: list[BuildDatasource],
    all_concepts: List[BuildConcept],
    accept_partial: bool,
    environment: BuildEnvironment,
    depth: int,
    conditions: BuildWhereClause | None = None,
) -> tuple["UnionNode", bool]:
    from trilogy.core.processing.nodes.union_node import UnionNode

    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} generating union node parents with condition {conditions}"
    )
    force_group = False
    parents = []
    for x in datasource:
        subnode, fg = create_datasource_node(
            x,
            all_concepts,
            accept_partial,
            environment,
            depth + 1,
            conditions=conditions,
        )
        parents.append(subnode)
        force_group = force_group or fg
    logger.info(f"{padding(depth)}{LOGGER_PREFIX} returning union node")
    return (
        UnionNode(
            output_concepts=all_concepts,
            input_concepts=all_concepts,
            environment=environment,
            parents=parents,
            depth=depth,
            partial_concepts=[],
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

    # Use cached extraction
    all_concepts = [
        environment.concepts[extract_address(c)] for c in subgraph if c.startswith("c~")
    ]

    # Early return for all constants
    if all(c.derivation == Derivation.CONSTANT for c in all_concepts):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} All concepts {[x.address for x in all_concepts]} are constants, returning constant node"
        )
        return ConstantNode(
            output_concepts=all_concepts,
            input_concepts=[],
            environment=environment,
            parents=[],
            depth=depth,
            partial_concepts=[],
            force_group=False,
            preexisting_conditions=conditions.conditional if conditions else None,
        )

    cache = GraphAttributeCache(g)
    datasource = cache.datasources[ds_name]

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
        bcandidate, force_group = create_union_datasource(
            datasource,
            all_concepts,
            accept_partial,
            environment,
            depth,
            conditions=conditions,
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
            depth=depth + 1,
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
    # Early separation of constants and non-constants
    non_constant = []
    constants = []
    for c in all_concepts:
        if c.derivation == Derivation.CONSTANT:
            constants.append(c)
        else:
            non_constant.append(c)

    # Early return for all constants
    if not non_constant and constants:
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
                    f"{padding(depth)}{LOGGER_PREFIX} conditions being passed in to constant node {conditions}, but not all concepts are constants."
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

    attempts = [False]
    if accept_partial:
        attempts.append(True)

    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} searching for root source graph for concepts {[c.address for c in all_concepts]} and conditions {conditions}"
    )

    pruned_concept_graph = None
    for attempt in attempts:
        pruned_concept_graph = create_pruned_concept_graph(
            g,
            non_constant,
            accept_partial=attempt,
            conditions=conditions,
            datasources=list(environment.datasources.values()),
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
        pruned_concept_graph,
        relevant=non_constant,
        accept_partial=accept_partial,
        conditions=conditions,
        depth=depth,
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
        x.preexisting_conditions and x.preexisting_conditions == conditions.conditional
        for x in parents
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
