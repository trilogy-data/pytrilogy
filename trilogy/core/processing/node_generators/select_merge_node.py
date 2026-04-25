from typing import TYPE_CHECKING

from trilogy.constants import logger
from trilogy.core import graph as nx
from trilogy.core.enums import (
    AddressType,
    BooleanOperator,
    Derivation,
    Granularity,
    Purpose,
)
from trilogy.core.graph_models import (
    ReferenceGraph,
    SearchCriteria,
    concept_to_node,
    datasource_has_filter_sensitive_aggregate,
    get_graph_exact_match,
    prune_sources_for_aggregates,
    prune_sources_for_conditions,
)
from trilogy.core.models.build import (
    Address,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildGrain,
    BuildParenthetical,
    BuildUnionDatasource,
    BuildWhereClause,
    CanonicalBuildConceptList,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import (
    condition_implies,
    decompose_condition,
    is_scalar_condition,
    merge_conditions,
    merge_conditions_and_dedup,
)
from trilogy.core.processing.discovery_validation import (
    ValidationResult,
    validate_stack,
)
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
from trilogy.utility import unique

if TYPE_CHECKING:
    from trilogy.core.processing.nodes.union_node import UnionNode

LOGGER_PREFIX = "[GEN_ROOT_MERGE_NODE]"


def _condition_atom_addresses(
    atom: BuildComparison | BuildConditional | BuildParenthetical,
) -> set[str]:
    return {
        c.canonical_address
        for c in atom.row_arguments
        if c.derivation != Derivation.CONSTANT
    }


def extract_address(node: str) -> str:
    return node.split("~")[1].split("@")[0]


def get_graph_partial_nodes(
    g: ReferenceGraph, conditions: BuildWhereClause | None
) -> dict[str, list[str]]:
    partial: dict[str, list[str]] = {}
    for node, ds in g.datasources.items():
        if (
            ds.non_partial_for
            and conditions
            and condition_implies(
                conditions.conditional, ds.non_partial_for.conditional
            )
        ):
            partial[node] = []
        else:
            partial[node] = [concept_to_node(c) for c in ds.partial_concepts]
    return partial


def get_graph_partial_canonical(
    g: ReferenceGraph, conditions: BuildWhereClause | None
) -> dict[str, set[str]]:
    partial: dict[str, set[str]] = {}
    for node, ds in g.datasources.items():
        if (
            ds.non_partial_for
            and conditions
            and condition_implies(
                conditions.conditional, ds.non_partial_for.conditional
            )
        ):
            partial[node] = set()
        else:
            partial[node] = {c.canonical_address for c in ds.partial_concepts}
    return partial


def get_graph_grains(g: ReferenceGraph) -> dict[str, set[str]]:
    return {node: ds.grain.components for node, ds in g.datasources.items()}


def get_materialization_score(
    address: Address | AddressType | str, is_filtered: bool = False
) -> float:
    """Score datasource by materialization level. Lower is better (more materialized).

    - 0: TABLE - fully materialized in the database
    - 1: Static files (CSV, TSV, PARQUET) - data files that need to be read
    - 2: Dynamic sources (QUERY, SQL) - queries that need to be executed
    - 3: Executable scripts (PYTHON_SCRIPT) - scripts that need to run
    """
    base = -0.1 if is_filtered else 0.0

    if isinstance(address, str):
        return base
    address_type = address if isinstance(address, AddressType) else address.type
    if address_type == AddressType.TABLE:
        return base
    if address_type in (AddressType.CSV, AddressType.TSV, AddressType.PARQUET):
        return base + 1.0
    if address_type in (AddressType.QUERY, AddressType.SQL):
        return base + 2.0
    if address_type == AddressType.PYTHON_SCRIPT:
        return base + 3.0
    return base + 2.0


def _ds_mat_score(
    ds_name: str,
    datasources: dict[str, "BuildDatasource | BuildUnionDatasource"],
    relevant_concepts: list[str],
    partial_map: dict[str, list[str]],
) -> tuple[int, float, str]:
    partial_count = sum(
        1 for x in partial_map.get(ds_name, []) if x in relevant_concepts
    )
    ds = datasources.get(ds_name)
    if ds is None:
        return (partial_count, 2, ds_name)
    if isinstance(ds, BuildDatasource):
        return (
            partial_count,
            get_materialization_score(
                ds.address, True if ds.non_partial_for else False
            ),
            ds_name,
        )
    if isinstance(ds, BuildUnionDatasource):
        return (
            partial_count,
            max(
                get_materialization_score(
                    child.address, True if ds.non_partial_for else False
                )
                + 0.11
                for child in ds.children
            ),
            ds_name,
        )
    return (partial_count, 2, ds_name)


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


def deduplicate_datasources(
    datasets: list[str],
    relevant_concepts: list[str],
    g_edges: set[tuple[str, str]],
    datasources: dict[str, "BuildDatasource | BuildUnionDatasource"],
    depth: int = 0,
    partial_map: dict[str, list[str]] | None = None,
) -> list[str]:
    """Prune duplicate datasources that have identical relevant concept bindings.

    When multiple datasources provide the exact same set of relevant concepts,
    keep only the most materialized one to avoid false join key injection.
    """
    if partial_map is None:
        partial_map = {}

    # Group datasources by their set of relevant concept bindings in one pass
    concept_to_ds: dict[frozenset[str], list[str]] = {}
    for ds in datasets:
        key = frozenset(
            c for c in relevant_concepts if (ds, c) in g_edges or (c, ds) in g_edges
        )
        concept_to_ds.setdefault(key, []).append(ds)

    deduplicated: list[str] = []
    for ds_list in concept_to_ds.values():
        if len(ds_list) == 1:
            deduplicated.append(ds_list[0])
        else:
            best_ds = min(
                ds_list,
                key=lambda n: _ds_mat_score(
                    n, datasources, relevant_concepts, partial_map
                ),
            )
            deduplicated.append(best_ds)
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} Pruned down duplicate datasources list {ds_list}, keeping {best_ds}"
            )
    return deduplicated


def score_datasource_node(
    node: str,
    datasources: dict[str, "BuildDatasource | BuildUnionDatasource"],
    grain_map: dict[str, set[str]],
    concept_map: dict[str, set[str]],
    exact_map: set[str],
    subgraphs: dict[str, list[str]],
) -> tuple[float, int, float, int, str]:
    """Score a datasource node for selection priority. Lower score = higher priority.

    Returns tuple of:
    - materialization_score: 0 (table) to 3 (python script)
    - grain_score: effective grain size (lower is better)
    - exact_match_score: 0 if exact condition match, 0.5 otherwise
    - concept_count: number of concepts (tiebreaker)
    - node_name: alphabetic tiebreaker
    """
    ds = datasources.get(node)
    if ds is None:
        mat_score = 2.0
    elif isinstance(ds, BuildDatasource):
        mat_score = get_materialization_score(ds.address)
    elif isinstance(ds, BuildUnionDatasource):
        mat_score = max(
            get_materialization_score(child.address) for child in ds.children
        )
    else:
        mat_score = 2.0

    grain = grain_map[node]
    grain_score = len(grain) - sum(1 for x in concept_map[node] if x in grain)
    exact_score = 0 if node in exact_map else 0.5
    concept_count = len(subgraphs[node])
    return (mat_score, grain_score, exact_score, concept_count, node)


def _score_node(
    node: str,
    datasources: dict[str, BuildDatasource | BuildUnionDatasource],
    grain_length: dict[str, set[str]],
    concept_map: dict[str, set[str]],
    exact_map: set[str],
    subgraphs: dict[str, list[str]],
    depth: int,
) -> tuple[float, int, float, int, str]:
    logger.debug(f"{padding(depth)}{LOGGER_PREFIX} scoring node {node}")
    score = score_datasource_node(
        node, datasources, grain_length, concept_map, exact_map, subgraphs
    )
    logger.debug(f"{padding(depth)}{LOGGER_PREFIX} node {node} has score {score}")
    return score


def subgraph_is_complete(
    nodes: set[str] | list[str],
    targets: set[str],
    mapping: dict[str, str],
    g: nx.DiGraph,
) -> bool:
    mapped = {mapping.get(n, n) for n in nodes}
    if not targets.issubset(mapped):
        missing = targets - mapped
        logger.debug(
            f"Subgraph {nodes} is not complete, missing targets {missing} - mapped {mapped}"
        )
        return False

    has_ds_edge = {target: False for target in targets}
    for node in nodes:
        if node.startswith("c~"):
            mapped_node = mapping.get(node, node)
            if mapped_node in targets and not has_ds_edge[mapped_node]:
                if any(
                    neighbor.startswith("ds~") for neighbor in nx.neighbors(g, node)
                ):
                    has_ds_edge[mapped_node] = True

    return all(has_ds_edge.values())


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
    prune_sources_for_aggregates(g, all_concepts, logger)

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
    criteria: SearchCriteria,
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
    canonical_relevant = {c.canonical_address for c in relevant}
    canonical_map = {c.canonical_address: c.address for c in relevant}
    concepts: dict[str, BuildConcept] = g.concepts
    subgraphs: dict[str, list[str]] = {
        ds: sorted(set(nx.all_neighbors(g, ds))) for ds in datasources
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
    exact_map = get_graph_exact_match(
        g, criteria, conditions, allow_filter_application=False
    )
    grain_length = get_graph_grains(g)

    # compute concept_map and non_partial_map in one pass over subgraphs
    concept_map: dict[str, set[str]] = {}
    non_partial_map: dict[str, set[str]] = {}
    for ds in datasources:
        all_addrs = {
            concepts[c].canonical_address for c in subgraphs[ds] if c in concepts
        }
        concept_map[ds] = all_addrs
        non_partial_map[ds] = all_addrs - partial_canonical[ds]

    pruned_subgraphs = {}

    def _scorer(n: str) -> tuple[float, int, float, int, str]:
        return _score_node(
            n, g.datasources, grain_length, concept_map, exact_map, subgraphs, depth
        )

    for key, nodes in subgraphs.items():
        value = non_partial_map[key]
        all_concepts = concept_map[key]
        is_subset = False
        matches = set()
        for other_key, other_all_concepts in concept_map.items():
            other_value = non_partial_map[other_key]
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
            min_node = min(matches, key=_scorer)
            logger.debug(
                f"{padding(depth)}{LOGGER_PREFIX} minimum source score is {min_node}"
            )
            is_subset = key is not min_node
        if not is_subset:
            pruned_subgraphs[key] = nodes

    final_nodes = {n for v in pruned_subgraphs.values() for n in v}
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
                sum(1 for _, sub_nodes in pruned_subgraphs.items() if node in sub_nodes)
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
    all_concepts: list[BuildConcept],
    accept_partial: bool,
    environment: BuildEnvironment,
    depth: int,
    conditions: BuildWhereClause | None = None,
    injected_conditions: (
        BuildComparison | BuildConditional | BuildParenthetical | None
    ) = None,
) -> tuple[StrategyNode, bool]:

    target_grain = BuildGrain.from_concepts(all_concepts, environment=environment)
    # datasource grain may have changed since reference graph creation
    datasource_grain = BuildGrain.from_concepts(
        datasource.grain.components, environment=environment
    )
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
        force_group = any(
            x.granularity != Granularity.SINGLE_ROW for x in datasource.output_concepts
        )
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
    partial_is_full = (
        conditions
        and datasource.non_partial_for
        and condition_implies(
            conditions.conditional, datasource.non_partial_for.conditional
        )
    )

    datasource_conditions = datasource.where.conditional if datasource.where else None
    if injected_conditions and datasource_conditions:
        datasource_conditions = datasource_conditions + injected_conditions
    elif injected_conditions:
        datasource_conditions = injected_conditions

    if conditions:
        ds_outputs = {c.canonical_address for c in datasource.output_concepts}
        covered_atoms: set[str] = set()
        if partial_is_full and datasource.non_partial_for:
            covered_atoms = {
                str(atom)
                for atom in decompose_condition(datasource.non_partial_for.conditional)
            }
        for atom in decompose_condition(conditions.conditional):
            if (
                str(atom) in covered_atoms
                or atom.existence_arguments
                or not is_scalar_condition(atom)
            ):
                continue
            if _condition_atom_addresses(atom).issubset(ds_outputs):
                datasource_conditions = (
                    merge_conditions_and_dedup(atom, datasource_conditions)
                    if datasource_conditions
                    else atom
                )

    all_inputs = [c.concept for c in datasource.columns]
    canonical_all = CanonicalBuildConceptList(concepts=all_inputs)

    # if we're binding via a canonical address association, add it here
    for x in all_concepts:
        if x not in all_inputs and x in canonical_all:
            all_inputs.append(x)

    # additional single row check
    satisfies_conditions = not datasource_has_filter_sensitive_aggregate(
        datasource, conditions
    ) and all(
        x.granularity == Granularity.SINGLE_ROW for x in datasource.output_concepts
    )
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} creating select node for datasource {datasource.name} with conditions {datasource_conditions}, "
        f"partial_is_full {partial_is_full}, satisfies_conditions {satisfies_conditions}, "
        f"force_group {force_group}"
    )
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
            # partial_is_full only means non_partial_for conditions are satisfied;
            # any extra conditions in the query must still be applied externally.
            datasource.non_partial_for.conditional
            if partial_is_full and datasource.non_partial_for and conditions
            else (
                conditions.conditional if satisfies_conditions and conditions else None
            )
        ),
    )
    return (
        rval,
        force_group,
    )


def create_union_datasource(
    datasource: BuildUnionDatasource,
    all_concepts: list[BuildConcept],
    accept_partial: bool,
    environment: BuildEnvironment,
    depth: int,
    conditions: BuildWhereClause | None = None,
) -> tuple["UnionNode", bool]:
    from trilogy.core.processing.condition_utility import filter_union_children
    from trilogy.core.processing.nodes.union_node import UnionNode

    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} generating union node parents with condition {conditions}"
    )

    effective: list[
        tuple[
            BuildDatasource,
            BuildComparison | BuildConditional | BuildParenthetical | None,
        ]
    ]
    if conditions:
        qcond = conditions.conditional
        non_partial_map = {
            child.name: child.non_partial_for for child in datasource.children
        }
        kept = filter_union_children(non_partial_map, qcond)
        for child in datasource.children:
            if child.name not in kept:
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} dropping {child.name}: "
                    f"non_partial_for {child.non_partial_for!r} mutually exclusive with {qcond!r}"
                )
        effective = [
            (child, kept[child.name])
            for child in datasource.children
            if child.name in kept
        ]
        if len(effective) < len(datasource.children):
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} reduced union from {len(datasource.children)} "
                f"to {len(effective)} branch(es)"
            )
    else:
        effective = [(child, None) for child in datasource.children]

    force_group = False
    parents = []
    for child, injected_cond in effective:
        subnode, fg = create_datasource_node(
            child,
            all_concepts,
            accept_partial,
            environment,
            depth + 1,
            injected_conditions=injected_cond,
        )
        parents.append(subnode)
        force_group = force_group or fg
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} returning union node with {len(parents)} branch(es)"
    )
    return (
        UnionNode(
            output_concepts=all_concepts,
            input_concepts=all_concepts,
            environment=environment,
            parents=parents,
            depth=depth,
            partial_concepts=[],
            preexisting_conditions=conditions.conditional if conditions else None,
        ),
        force_group,
    )


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


def _covered_conditions(
    conditions: BuildWhereClause, environment: BuildEnvironment
) -> BuildWhereClause | None:
    """Return condition atoms covered by some datasource's complete_where (non_partial_for).

    These are "owned" by a partial datasource, so foreign datasources (like tree_enrichment
    when the condition is city='USSFO') can safely ignore them — they will be applied via
    the owning datasource's non_partial_for mechanism.
    """
    atoms = decompose_condition(conditions.conditional)
    atom_str_map = {str(a): a for a in atoms}
    preserved = []
    seen: set[str] = set()
    for ds in environment.datasources.values():
        if not isinstance(ds, BuildDatasource) or not ds.non_partial_for:
            continue
        if not condition_implies(
            conditions.conditional, ds.non_partial_for.conditional
        ):
            continue
        for np_atom in decompose_condition(ds.non_partial_for.conditional):
            key = str(np_atom)
            if key in atom_str_map and key not in seen:
                preserved.append(atom_str_map[key])
                seen.add(key)
    if not preserved:
        return None
    cond = preserved[0]
    for a in preserved[1:]:
        cond = BuildConditional(left=cond, right=a, operator=BooleanOperator.AND)
    return BuildWhereClause(conditional=cond)


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
    parents = []
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
            covered = _covered_conditions(conditions, environment)
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
        if conditions and all(
            x.preexisting_conditions
            and x.preexisting_conditions == conditions.conditional
            for x in parents
        ):
            preexisting_conditions = conditions.conditional

        candidate = MergeNode(
            output_concepts=all_concepts,
            input_concepts=normals + abstract_props,
            environment=environment,
            depth=depth,
            parents=parents,
            preexisting_conditions=preexisting_conditions,
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
