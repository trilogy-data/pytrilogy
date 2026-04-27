from trilogy.constants import logger
from trilogy.core import graph as nx
from trilogy.core.enums import AddressType
from trilogy.core.graph_models import (
    ReferenceGraph,
    SearchCriteria,
    concept_to_node,
    get_graph_exact_match,
)
from trilogy.core.models.build import (
    Address,
    BuildConcept,
    BuildDatasource,
    BuildUnionDatasource,
    BuildWhereClause,
)
from trilogy.core.processing.condition_utility import condition_implies
from trilogy.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_ROOT_MERGE_NODE]"


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
    """Score datasource by materialization level. Lower is better."""
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


def deduplicate_datasources(
    datasets: list[str],
    relevant_concepts: list[str],
    g_edges: set[tuple[str, str]],
    datasources: dict[str, "BuildDatasource | BuildUnionDatasource"],
    depth: int = 0,
    partial_map: dict[str, list[str]] | None = None,
) -> list[str]:
    """Prune duplicate datasources that have identical relevant concept bindings."""
    if partial_map is None:
        partial_map = {}

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
    """Score a datasource node for selection priority. Lower score wins."""
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


def filter_pseudonym_duplicates(
    concepts: list[BuildConcept], relevant: list[BuildConcept]
) -> list[BuildConcept]:
    """Filter out concepts whose pseudonyms are also in the list."""
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
    """Resolve competing datasource subgraphs to the preferred source set."""
    datasources = sorted(n for n in g.nodes if n.startswith("ds~"))
    canonical_relevant = {c.canonical_address for c in relevant}
    canonical_map = {c.canonical_address: c.address for c in relevant}
    concepts: dict[str, BuildConcept] = g.concepts
    subgraphs: dict[str, list[str]] = {
        ds: sorted(set(nx.all_neighbors(g, ds))) for ds in datasources
    }
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

    concept_map: dict[str, set[str]] = {}
    non_partial_map: dict[str, set[str]] = {}
    for ds in datasources:
        all_addrs = {concepts[c].canonical_address for c in subgraphs[ds]}
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
            is_subset = key != min_node
        if not is_subset:
            pruned_subgraphs[key] = nodes

    final_nodes = {n for v in pruned_subgraphs.values() for n in v}
    relevant_concepts_pre = {
        n: x.canonical_address
        for n in g.nodes()
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
