from functools import reduce
from typing import TYPE_CHECKING, List, Optional

import networkx as nx

from trilogy.constants import logger
from trilogy.core.enums import AddressType, Derivation
from trilogy.core.graph_models import (
    ReferenceGraph,
    concept_to_node,
    get_graph_exact_match,
    prune_sources_for_aggregates,
    prune_sources_for_conditions,
)
from trilogy.core.models.build import (
    Address,
    BuildConcept,
    BuildDatasource,
    BuildGrain,
    BuildUnionDatasource,
    BuildWhereClause,
    CanonicalBuildConceptList,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.common import reinject_common_join_keys_v2
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


def extract_address(node: str):
    return node.split("~")[1].split("@")[0]


def get_graph_partial_nodes(
    g: ReferenceGraph, conditions: BuildWhereClause | None
) -> dict[str, list[str]]:
    partial: dict[str, list[str]] = {}
    for node, ds in g.datasources.items():

        if ds.non_partial_for and conditions == ds.non_partial_for:
            partial[node] = []
            continue
        partial[node] = [concept_to_node(c) for c in ds.partial_concepts]
    return partial


def get_graph_partial_canonical(
    g: ReferenceGraph, conditions: BuildWhereClause | None
) -> dict[str, set[str]]:
    partial: dict[str, set[str]] = {}
    for node, ds in g.datasources.items():

        if ds.non_partial_for and conditions == ds.non_partial_for:
            partial[node] = set()
            continue
        partial[node] = set(c.canonical_address for c in ds.partial_concepts)
    return partial


def get_graph_grains(g: ReferenceGraph) -> dict[str, list[str]]:
    grain_length: dict[str, list[str]] = {}
    for node, lookup in g.datasources.items():

        base: set[str] = set()
        if not isinstance(lookup, list):
            flookup = [lookup]
        else:
            flookup = lookup
        assert isinstance(flookup, list)
        grain_length[node] = reduce(
            lambda x, y: x.union(y.grain.components), flookup, base  # type: ignore
        )
    return grain_length


def get_materialization_score(address: Address | AddressType | str) -> int:
    """Score datasource by materialization level. Lower is better (more materialized).

    - 0: TABLE - fully materialized in the database
    - 1: Static files (CSV, TSV, PARQUET) - data files that need to be read
    - 2: Dynamic sources (QUERY, SQL) - queries that need to be executed
    - 3: Executable scripts (PYTHON_SCRIPT) - scripts that need to run
    """
    if isinstance(address, str):
        return 0
    elif isinstance(address, AddressType):
        address_type = address
    else:
        address_type = address.type
    if address_type == AddressType.TABLE:
        return 0
    if address_type in (AddressType.CSV, AddressType.TSV, AddressType.PARQUET):
        return 1
    if address_type in (AddressType.QUERY, AddressType.SQL):
        return 2
    if address_type == AddressType.PYTHON_SCRIPT:
        return 3
    return 2


def deduplicate_datasources(
    datasets: list[str],
    relevant_concepts: list[str],
    g_edges: set[tuple[str, str]],
    datasources: dict[str, "BuildDatasource | BuildUnionDatasource"],
    depth: int = 0,
    partial_map: dict[str, list[str]] = {},
) -> list[str]:
    """Prune duplicate datasources that have identical relevant concept bindings.

    When multiple datasources provide the exact same set of relevant concepts,
    keep only the most materialized one to avoid false join key injection.
    """
    # Map each datasource to its set of relevant concept bindings
    ds_to_concepts: dict[str, frozenset[str]] = {}
    for ds in datasets:
        ds_concepts = frozenset(
            c for c in relevant_concepts if (ds, c) in g_edges or (c, ds) in g_edges
        )
        ds_to_concepts[ds] = ds_concepts

    # Group datasources by their concept bindings
    concept_to_ds: dict[frozenset[str], list[str]] = {}
    for ds, concepts_set in ds_to_concepts.items():
        if concepts_set not in concept_to_ds:
            concept_to_ds[concepts_set] = []
        concept_to_ds[concepts_set].append(ds)

    # For each group of duplicates, keep only one (prefer more materialized)
    deduplicated: list[str] = []
    for concepts_set, ds_list in concept_to_ds.items():
        if len(ds_list) == 1:
            deduplicated.append(ds_list[0])
        else:
            # Pick the best one by materialization score
            def get_mat_score(ds_name: str) -> tuple[int, int, str]:
                partial_count = len(
                    [x for x in partial_map.get(ds_name, []) if x in relevant_concepts]
                )
                ds = datasources.get(ds_name)
                if ds is None:
                    return (partial_count, 2, ds_name)
                elif isinstance(ds, BuildDatasource):
                    return (
                        partial_count,
                        get_materialization_score(ds.address),
                        ds_name,
                    )
                elif isinstance(ds, BuildUnionDatasource):
                    return (
                        partial_count,
                        max(
                            get_materialization_score(child.address)
                            for child in ds.children
                        ),
                        ds_name,
                    )
                return (partial_count, 2, ds_name)

            best_ds = min(ds_list, key=get_mat_score)
            deduplicated.append(best_ds)
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} Pruned down duplicate datasources list {ds_list}, keeping {best_ds}"
            )

    return deduplicated


def score_datasource_node(
    node: str,
    datasources: dict[str, "BuildDatasource | BuildUnionDatasource"],
    grain_map: dict[str, list[str]],
    concept_map: dict[str, set[str]],
    exact_map: set[str],
    subgraphs: dict[str, list[str]],
) -> tuple[int, int, float, int, str]:
    """Score a datasource node for selection priority. Lower score = higher priority.

    Returns tuple of:
    - materialization_score: 0 (table) to 3 (python script)
    - grain_score: effective grain size (lower is better)
    - exact_match_score: 0 if exact condition match, 0.5 otherwise
    - concept_count: number of concepts (tiebreaker)
    - node_name: alphabetic tiebreaker
    """
    ds = datasources.get(node)

    # materialization score
    if ds is None:
        mat_score = 2
    elif isinstance(ds, BuildDatasource):
        mat_score = get_materialization_score(ds.address)
    elif isinstance(ds, BuildUnionDatasource):
        mat_score = max(
            get_materialization_score(child.address) for child in ds.children
        )
    else:
        mat_score = 2

    # grain score
    grain = grain_map[node]
    grain_score = len(list(grain)) - sum([1 for x in concept_map[node] if x in grain])

    # exact match
    exact_score = 0 if node in exact_map else 0.5

    # concept count
    concept_count = len(subgraphs[node])

    return (mat_score, grain_score, exact_score, concept_count, node)


def subgraph_is_complete(
    nodes: list[str], targets: set[str], mapping: dict[str, str], g: nx.DiGraph
) -> bool:
    # Check if all targets are present in mapped nodes
    mapped = {mapping.get(n, n) for n in nodes}
    if not targets.issubset(mapped):
        missing = targets - mapped
        logger.debug(
            f"Subgraph {nodes} is not complete, missing targets {missing} - mapped {mapped}"
        )
        return False

    # Check if at least one concept node has a datasource edge
    has_ds_edge = {target: False for target in targets}

    for node in nodes:
        if node.startswith("c~"):
            mapped_node = mapping.get(node, node)
            if mapped_node in targets and not has_ds_edge[mapped_node]:
                # Only check neighbors if we haven't found a ds edge for this mapped node yet
                if any(
                    neighbor.startswith("ds~") for neighbor in nx.neighbors(g, node)
                ):
                    has_ds_edge[mapped_node] = True

    return all(has_ds_edge.values())


def create_pruned_concept_graph(
    g: ReferenceGraph,
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
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} injecting potentially relevant union datasource {node_address}"
        )
        common: set[BuildConcept] = set.intersection(
            *[set(x.output_concepts) for x in ds_list]
        )
        g.datasources[node_address] = BuildUnionDatasource(children=ds_list)
        for c in common:
            cnode = concept_to_node(c)
            g.add_edge(node_address, cnode)
            g.add_edge(cnode, node_address)
    prune_sources_for_conditions(g, accept_partial, conditions)
    prune_sources_for_aggregates(g, all_concepts, logger)
    target_addresses = set([c.canonical_address for c in all_concepts])
    concepts: dict[str, BuildConcept] = orig_g.concepts
    datasource_map: dict[str, BuildDatasource | BuildUnionDatasource] = (
        orig_g.datasources
    )
    relevant_concepts_pre = {
        n: x.canonical_address
        for n in g.nodes()
        # filter out synonyms
        if (x := concepts.get(n, None)) and x.canonical_address in target_addresses
    }

    relevant_concepts: list[str] = list(relevant_concepts_pre.keys())
    relevent_datasets: list[str] = []
    partial = get_graph_partial_nodes(g, conditions)
    if not accept_partial:
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

    g_edges = set(g.edges)
    for n in g.datasources:
        if any((n, x) in g_edges for x in relevant_concepts):
            relevent_datasets.append(n)
            continue
    logger.info(f"Relevant datasets after pruning: {relevent_datasets}")

    # Prune duplicate datasources BEFORE join concept injection
    relevent_datasets = deduplicate_datasources(
        relevent_datasets, relevant_concepts, g_edges, g.datasources, depth, partial
    )

    g.remove_nodes_from(
        [
            n
            for n in g.nodes()
            if n not in relevent_datasets and n not in relevant_concepts
        ]
    )
    # from trilogy.hooks.graph_hook import GraphHook
    # GraphHook().query_graph_built(g)
    subgraphs = list(nx.connected_components(g.to_undirected()))
    subgraphs = [
        s
        for s in subgraphs
        if subgraph_is_complete(s, target_addresses, relevant_concepts_pre, g)
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
    # add back any relevant edges that might have been partially filtered
    # Inject extra join concepts that are shared between datasets
    synonyms: set[str] = set()
    for c in all_concepts:
        synonyms.update(c.pseudonyms)
    reinject_common_join_keys_v2(orig_g, g, synonyms, add_joins=True)
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


# def deduplicate_nodes(subgraph: nx.DiGraph, nodes: list[str], partial_map: dict[str, list[str]], depth: int) -> list[str]:
#     """
#     Remove duplicate datasource nodes that are connected to the same concepts
#     and have the same partial state, keeping the one with the most unique concepts.

#     Args:
#         subgraph: NetworkX DiGraph containing the nodes and edges
#         nodes: List of node names to deduplicate
#         partial_map: Map of datasource to partial nodes

#     Returns:
#         List of deduplicated node names
#     """
#     # Filter for datasource nodes only
#     ds_nodes = [node for node in nodes if node.startswith("ds~")]
#     non_ds_nodes = [node for node in nodes if not node.startswith("ds~")]

#     if len(ds_nodes) <= 1:
#         return nodes  # No deduplication needed

#     # Build a map of each datasource to its connected concepts and partial state
#     ds_info = {}

#     for ds_node in ds_nodes:
#         # Get connected concept nodes (nodes starting with "c~")
#         connected_concepts = set()
#         for neighbor in subgraph.neighbors(ds_node):
#             if neighbor.startswith("c~"):
#                 connected_concepts.add(neighbor)

#         # Get partial state for this datasource
#         partial_state = tuple(sorted(partial_map.get(ds_node, [])))

#         ds_info[ds_node] = {
#             'concepts': connected_concepts,
#             'partial_state': partial_state
#         }

#     # Find datasources to remove (those that are subsets of others)
#     nodes_to_remove = set()
#     logger.info('LOOK HERE')
#     logger.info(ds_info)
#     for ds_a, info_a in ds_info.items():
#         for ds_b, info_b in ds_info.items():
#             if ds_a != ds_b and ds_a not in nodes_to_remove:
#                 # Check if ds_a is a subset of ds_b (same partial state and concepts are subset)
#                 if (info_a['partial_state'] == info_b['partial_state'] and
#                     info_a['concepts'].issubset(info_b['concepts']) and
#                     len(info_a['concepts']) < len(info_b['concepts'])):
#                     # ds_a connects to fewer concepts than ds_b, so remove ds_a
#                     nodes_to_remove.add(ds_a)
#                 elif (info_a['partial_state'] == info_b['partial_state'] and
#                       info_a['concepts'] == info_b['concepts']):
#                     # Exact same concepts and partial state - keep one arbitrarily
#                     # (keep the lexicographically smaller one for consistency)
#                     if ds_a > ds_b:
#                         nodes_to_remove.add(ds_a)

#     # Keep datasource nodes that weren't marked for removal
#     logger.info(f"{padding(depth)}{LOGGER_PREFIX} Removing duplicate datasource nodes: {nodes_to_remove}")
#     deduplicated_ds_nodes = [ds for ds in ds_nodes if ds not in nodes_to_remove]

#     # Return deduplicated datasource nodes plus all non-datasource nodes
#     return deduplicated_ds_nodes + non_ds_nodes


def filter_pseudonym_duplicates(
    concepts: list[BuildConcept], relevant: list[BuildConcept]
) -> list[BuildConcept]:
    """Filter out concepts whose pseudonyms are also in the list, keeping the one in relevant."""
    relevant_addrs = {c.address for c in relevant}
    concept_addrs = {c.address for c in concepts}
    to_remove: set[str] = set()
    for c in concepts:
        for p_addr in c.pseudonyms:
            if p_addr in concept_addrs:
                c_in_relevant = c.address in relevant_addrs
                p_in_relevant = p_addr in relevant_addrs
                if p_in_relevant and not c_in_relevant:
                    to_remove.add(c.address)
                    break
                elif c_in_relevant and not p_in_relevant:
                    to_remove.add(p_addr)
    return [c for c in concepts if c.address not in to_remove]


def resolve_subgraphs(
    g: ReferenceGraph,
    relevant: list[BuildConcept],
    accept_partial: bool,
    conditions: BuildWhereClause | None,
    depth: int = 0,
) -> dict[str, list[str]]:
    """When we have multiple distinct subgraphs within our matched
    nodes that can satisfy a query, resolve which one of those we should
    ultimately ues.
    This should generally return one subgraph for each
    unique set of sub concepts that can be referenced,
    discarding duplicates.
    Duplicate subgraphs will be resolved based on which
    ones are most 'optimal' to use, a hueristic
    that can evolve in the future but is currently based on datasource
    cardinality."""
    datasources = [n for n in g.nodes if n.startswith("ds~")]
    canonical_relevant = set([c.canonical_address for c in relevant])
    canonical_map = {c.canonical_address: c.address for c in relevant}
    concepts: dict[str, BuildConcept] = g.concepts
    subgraphs: dict[str, list[str]] = {
        ds: list(set(list(nx.all_neighbors(g, ds)))) for ds in datasources
    }
    # filter pseudonym duplicates from each subgraph, keeping concept in relevant
    for ds in subgraphs:
        ds_concepts = [concepts[n] for n in subgraphs[ds] if n in concepts]
        filtered = filter_pseudonym_duplicates(ds_concepts, relevant)
        filtered_nodes = {concept_to_node(c) for c in filtered}
        subgraphs[ds] = [
            n for n in subgraphs[ds] if n not in concepts or n in filtered_nodes
        ]
    partial_canonical = get_graph_partial_canonical(g, conditions)
    exact_map = get_graph_exact_match(g, accept_partial, conditions)
    grain_length = get_graph_grains(g)
    non_partial_map = {
        ds: set(
            [
                concepts[c].canonical_address
                for c in subgraphs[ds]
                if concepts[c].canonical_address not in partial_canonical[ds]
            ]
        )
        for ds in datasources
    }
    concept_map = {
        ds: set([concepts[c].canonical_address for c in subgraphs[ds]])
        for ds in datasources
    }
    pruned_subgraphs = {}

    def score_node(input: str):
        logger.debug(f"{padding(depth)}{LOGGER_PREFIX} scoring node {input}")
        score = score_datasource_node(
            input, g.datasources, grain_length, concept_map, exact_map, subgraphs
        )
        logger.debug(f"{padding(depth)}{LOGGER_PREFIX} node {input} has score {score}")
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
                and value.issubset(other_value)
                and all_concepts.issubset(other_all_concepts)
            ):
                if len(value) < len(other_value):
                    is_subset = True
                    logger.info(
                        f"{padding(depth)}{LOGGER_PREFIX} Dropping subgraph {key} with {value} as it is a subset of {other_key} with {other_value}"
                    )
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
            is_subset = key is not min(matches, key=score_node)
        if not is_subset:
            pruned_subgraphs[key] = nodes

    final_nodes: set[str] = set([n for v in pruned_subgraphs.values() for n in v])
    relevant_concepts_pre = {
        n: x.canonical_address
        for n in g.nodes()
        # filter out synonyms
        if (x := concepts.get(n, None)) and x.canonical_address in canonical_relevant
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
            logger.debug(
                f"{padding(depth)}{LOGGER_PREFIX} Pruning node {node} as irrelevant after subgraph resolution"
            )
            pruned_subgraphs = {
                canonical_map.get(k, k): [n for n in v if n != node]
                for k, v in pruned_subgraphs.items()
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

    target_grain = BuildGrain.from_concepts(all_concepts, environment=environment)
    # datasource grain may have changed since reference graph creation
    datasource_grain = BuildGrain.from_concepts(
        datasource.grain.components, environment=environment
    )
    # datasource_grain = datasource.grain
    force_group = False
    if not datasource_grain.issubset(target_grain):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX}_DS_NODE Select node must be wrapped in group, {datasource_grain} not subset of target grain {target_grain} from {all_concepts}"
        )
        force_group = True
    else:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX}_DS_NODE Select node grain {datasource_grain} is subset of target grain {target_grain}, no group required"
        )
    if not datasource_grain.components:
        force_group = True
    partial_concepts = [
        c.concept
        for c in datasource.columns
        if not c.is_complete and c.concept.address in all_concepts
    ]

    partial_lcl = CanonicalBuildConceptList(concepts=partial_concepts)
    nullable_concepts = [
        c.concept
        for c in datasource.columns
        if c.is_nullable and c.concept.address in all_concepts
    ]

    nullable_lcl = CanonicalBuildConceptList(concepts=nullable_concepts)
    partial_is_full = conditions and (conditions == datasource.non_partial_for)

    datasource_conditions = datasource.where.conditional if datasource.where else None
    all_inputs = [c.concept for c in datasource.columns]
    canonical_all = CanonicalBuildConceptList(concepts=all_inputs)

    # if we're binding via a canonical address association, add it here
    for x in all_concepts:
        if x not in all_inputs and x in canonical_all:
            all_inputs.append(x)

    rval = SelectNode(
        input_concepts=all_inputs,
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
    datasource: BuildUnionDatasource,
    all_concepts: List[BuildConcept],
    accept_partial: bool,
    environment: BuildEnvironment,
    depth: int,
    conditions: BuildWhereClause | None = None,
) -> tuple["UnionNode", bool]:
    from trilogy.core.processing.nodes.union_node import UnionNode

    datasources = datasource.children
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} generating union node parents with condition {conditions}"
    )
    force_group = False
    parents = []
    for x in datasources:
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
    g: ReferenceGraph,
    environment: BuildEnvironment,
    depth: int,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode:
    all_concepts = [
        environment.canonical_concepts[extract_address(c)]
        for c in subgraph
        if c.startswith("c~")
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

    datasource: BuildDatasource | BuildUnionDatasource = g.datasources[ds_name]

    if isinstance(datasource, BuildDatasource):
        bcandidate, force_group = create_datasource_node(
            datasource,
            all_concepts,
            accept_partial,
            environment,
            depth,
            conditions=conditions,
        )

    elif isinstance(datasource, BuildUnionDatasource):
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
    non_constant = [c for c in all_concepts if c.derivation != Derivation.CONSTANT]
    constants = [c for c in all_concepts if c.derivation == Derivation.CONSTANT]
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} generating select merge node for {all_concepts}"
    )
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
                [x.derivation == Derivation.CONSTANT for x in conditions.row_arguments]
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
    attempts = [
        False,
    ]
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
