from typing import Iterable

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
from trilogy.core.processing.condition_utility import (
    condition_implies,
    condition_proves_non_null,
    condition_required_addresses,
    decompose_condition,
)
from trilogy.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_ROOT_MERGE_NODE]"


def _structural_partial_concepts(
    ds: "BuildDatasource | BuildUnionDatasource",
) -> list[BuildConcept]:
    """Column-level (``~``) partials, which survive a satisfied complete-where.

    For a union these are the children's unhealed intrinsic partials (see
    ``union_unhealed_partial_addresses``) — a union of subset-covering bindings
    is still subset-covering, so it must not score as complete against a rival
    union whose children bind the same keys fully.
    """
    return ds.column_level_partial_concepts or []


def membership_complete_grain_keys(
    ds: "BuildDatasource | BuildUnionDatasource",
    datasources: "Iterable[BuildDatasource | BuildUnionDatasource]",
    conditions: BuildWhereClause | None,
) -> set[str]:
    """Canonical addresses of ``ds``'s ``~`` GRAIN keys that the query WHERE
    proves complete for this result.

    A ``~`` grain key (``store_returns.~item.id``) covers only a subset of the
    key's universe (return rows), so the planner otherwise keeps a sibling fact
    (``store_sales``, keys complete) to anchor the full grain and re-fetches the
    key as "complete" downstream. But if the WHERE proves-non-null a concept that
    ``ds`` provides and its same-grain siblings do NOT (``return_channel_dim_id``),
    every surviving row is a ``ds`` row (implicit ``complete where``) — the keys
    cover the whole result population, so they are non-partial *for this query*.

    Two call sites share this one proof: source selection drops it from the
    partial map (so the redundant sibling falls out via the ordinary subset
    rule), and the datasource node drops it from ``partial_concepts`` (so the
    discovery loop doesn't see the key come back partial and re-source it).

    Empty unless BOTH a ``~`` grain key exists AND the membership proof holds, so
    every other query is unaffected.
    """
    if conditions is None or not isinstance(ds, BuildDatasource):
        return set()
    grain = set(ds.grain.components)
    grain_keys = {
        c.canonical_address
        for c in _structural_partial_concepts(ds)
        if c.canonical_address in grain
    }
    if not grain_keys:
        return set()
    # A ~ key is only worth "completing" if a same-grain sibling supplies it
    # non-structurally — i.e. there is an anchor we'd otherwise pull in. With no
    # such sibling the key is genuinely partial (nothing covers its full
    # universe) and must stay partial. `sibling_outputs` also gives the concepts
    # a WHERE proof can key off: one exclusive to `ds` proves the surviving rows
    # are `ds`'s rows.
    sibling_outputs: set[str] = set()
    sibling_completes: set[str] = set()
    for other in datasources:
        if (
            not isinstance(other, BuildDatasource)
            or other.name == ds.name
            or set(other.grain.components) != grain
        ):
            continue
        sibling_outputs |= {c.address for c in other.output_concepts}
        other_structural = {
            c.canonical_address for c in _structural_partial_concepts(other)
        }
        sibling_completes |= {
            c.canonical_address for c in other.output_concepts
        } - other_structural
    grain_keys &= sibling_completes
    if not grain_keys:
        return set()
    exclusive = {c.address for c in ds.output_concepts} - sibling_outputs
    proven = condition_proves_non_null(conditions.conditional)
    return grain_keys if proven & exclusive else set()


def get_graph_partial_nodes(
    g: ReferenceGraph, conditions: BuildWhereClause | None
) -> dict[str, list[str]]:
    partial: dict[str, list[str]] = {}
    for node, ds in g.datasources.items():
        complete_keys = membership_complete_grain_keys(
            ds, g.datasources.values(), conditions
        )
        if (
            ds.non_partial_for
            and conditions
            and condition_implies(
                conditions.conditional, ds.non_partial_for.conditional
            )
        ):
            # Condition satisfies the DS's complete-where, so the implicit
            # table-level partial stamp goes away — but column-level ~ partials
            # are structural and must survive.
            partial[node] = [
                concept_to_node(c)
                for c in _structural_partial_concepts(ds)
                if c.canonical_address not in complete_keys
            ]
        else:
            partial[node] = [
                concept_to_node(c)
                for c in ds.partial_concepts
                if c.canonical_address not in complete_keys
            ]
    return partial


def get_graph_partial_canonical(
    g: ReferenceGraph, conditions: BuildWhereClause | None
) -> dict[str, set[str]]:
    partial: dict[str, set[str]] = {}
    for node, ds in g.datasources.items():
        complete_keys = membership_complete_grain_keys(
            ds, g.datasources.values(), conditions
        )
        if (
            ds.non_partial_for
            and conditions
            and condition_implies(
                conditions.conditional, ds.non_partial_for.conditional
            )
        ):
            partial[node] = {
                c.canonical_address for c in _structural_partial_concepts(ds)
            } - complete_keys
        else:
            partial[node] = {
                c.canonical_address for c in ds.partial_concepts
            } - complete_keys
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


def prune_dominated_datasources(
    datasets: list[str],
    relevant_concepts_pre: dict[str, str],
    g: ReferenceGraph,
    partial_map: dict[str, list[str]],
    authored_key_scope: set[str],
    depth: int = 0,
) -> list[str]:
    """Drop datasources whose relevant-concept bindings fall entirely inside an
    authored join pairing's key scope (canonical + member FK keys) AND are a
    strict subset of a single peer's, with no partial advantage.

    Such a source contributes no requested content; it only adds an alternative
    side-path to the authored pairing through UNREQUESTED shared columns, which
    re-pairs the preserving join and can null a row's own dimension enrichment
    (the projected-authored-key NULL-group bug). The scope restriction keeps
    ordinary fact-side FK carriers alive — a source bound to non-pair concepts
    may bridge peers at row grain through unrequested keys (q05's
    partial-FK-to-dim shape), a role binding sets can't see. A dominated source
    that is the only bridge holding the kept graph together also stays
    (connectivity guard)."""
    if len(datasets) < 2 or not authored_key_scope:
        return datasets
    g_edges = set(g.edges)
    bindings: dict[str, frozenset[str]] = {}
    partial_bindings: dict[str, set[str]] = {}
    for ds in datasets:
        bindings[ds] = frozenset(
            canonical
            for node, canonical in relevant_concepts_pre.items()
            if (ds, node) in g_edges or (node, ds) in g_edges
        )
        partial_bindings[ds] = {
            relevant_concepts_pre[node]
            for node in partial_map.get(ds, [])
            if node in relevant_concepts_pre
        }

    def dominated_by(ds: str, other: str) -> bool:
        return (
            bindings[ds] < bindings[other]
            and (partial_bindings[other] & bindings[ds]) <= partial_bindings[ds]
        )

    kept = list(datasets)
    candidates = sorted(
        (
            ds
            for ds in datasets
            if bindings[ds] <= authored_key_scope
            and any(dominated_by(ds, o) for o in datasets)
        ),
        key=lambda ds: (len(bindings[ds]), ds),
    )
    if not candidates:
        return kept
    keep_nodes = set(kept) | set(relevant_concepts_pre)
    induced = nx.Graph()
    induced.add_nodes_from(keep_nodes)
    induced.add_edges_from(
        (a, b) for a, b in g_edges if a in keep_nodes and b in keep_nodes
    )
    targets = set(relevant_concepts_pre.values())

    def covers_all_targets() -> bool:
        # mirrors the downstream single-complete-subgraph criterion: one
        # component must reach every requested canonical through a datasource
        for comp in nx.connected_components(induced):
            if not any(n in bindings for n in comp):
                continue
            covered = {
                relevant_concepts_pre[n] for n in comp if n in relevant_concepts_pre
            }
            if covered >= targets:
                return True
        return False

    guard_active = covers_all_targets()
    for ds in candidates:
        if not any(dominated_by(ds, o) for o in kept if o != ds):
            continue
        removed_edges = [(ds, n) for n in induced.neighbors(ds)]
        induced.remove_node(ds)
        if guard_active and not covers_all_targets():
            induced.add_node(ds)
            induced.add_edges_from(removed_edges)
            continue
        kept.remove(ds)
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Pruned dominated datasource {ds} "
            f"(bindings {sorted(bindings[ds])} subset of a kept peer)"
        )
    return kept


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


def _condition_atoms_sourceable_by_datasource(
    ds: BuildDatasource | BuildUnionDatasource | None,
    conditions: BuildWhereClause | None,
) -> set[str]:
    if conditions is None or not isinstance(ds, BuildDatasource):
        return set()
    outputs = {c.canonical_address for c in ds.output_concepts}
    return {
        str(atom)
        for atom in decompose_condition(conditions.conditional)
        if condition_required_addresses(atom).issubset(outputs)
    }


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
    condition_atom_map = {
        ds: _condition_atoms_sourceable_by_datasource(g.datasources.get(ds), conditions)
        for ds in datasources
    }

    concept_map: dict[str, set[str]] = {}
    non_partial_map: dict[str, set[str]] = {}
    for ds in datasources:
        # Only consider concepts in the requested set when judging coverage.
        # A DS that exposes extra non-relevant concepts (e.g. a returns DS that
        # also surfaces return_channel_dim_id when the query never asks for it)
        # would otherwise look like it "uniquely covers" something — and never
        # be recognized as a subset of a broader DS that the query does need.
        all_addrs = {
            concepts[c].canonical_address for c in subgraphs[ds]
        } & canonical_relevant
        concept_map[ds] = all_addrs
        non_partial_map[ds] = all_addrs - partial_canonical[ds]

    pruned_subgraphs = {}

    # Datasource-level join adjacency via shared concept nodes (join keys),
    # including structural keys that are NOT in the requested set. Used to keep
    # a datasource that is the sole bridge to another datasource from being
    # dropped as a mere concept-subset of a peer that can't reach it.
    ds_join_nodes = {
        ds: {n for n in subgraphs[ds] if n in concepts} for ds in datasources
    }

    def _bridge_edges(ds: str) -> set[tuple[str, str]]:
        # (other datasource, the join-key node shared with it). Keyed by join key,
        # not just by target: an FK bridge reaches a fact via a fine key (oid)
        # while a dimension peer reaches the same fact only via a coarse enum key
        # (channel). Comparing whole targets treats those as equivalent and drops
        # the FK bridge; comparing (target, key) edges keeps the distinction.
        mine = ds_join_nodes[ds]
        return {
            (o, n) for o in datasources if o != ds for n in (mine & ds_join_nodes[o])
        }

    # Datasources that uniquely provide some requested concept — pruning a join
    # path to one of these would lose data, so a bridge to it is load-bearing.
    # A bridge to a datasource whose concepts are all available elsewhere is a
    # redundant alternative path and stays prunable (avoids re-introducing an
    # ambiguous relationship).
    _provider_count: dict[str, int] = {}
    for _ds in datasources:
        for _c in concept_map[_ds]:
            _provider_count[_c] = _provider_count.get(_c, 0) + 1
    sole_providers = {
        ds
        for ds in datasources
        if any(_provider_count[c] == 1 for c in concept_map[ds])
    }

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
                if not condition_atom_map[key].issubset(condition_atom_map[other_key]):
                    continue
                # `key` may share fewer *requested* concepts than `other_key`
                # yet still be the only join path to a datasource that uniquely
                # supplies a needed concept (a dimension bridging a fact to a
                # transitive attribute). Dropping it then severs that join and
                # fans the result out, so keep it. A bridge to a datasource that
                # is redundant elsewhere is NOT protected (it would re-introduce
                # an ambiguous alternative path).
                other_edges = _bridge_edges(other_key)
                key_only_bridges = {
                    o
                    for (o, n) in _bridge_edges(key)
                    if o != other_key
                    and o in sole_providers
                    and (o, n) not in other_edges
                }
                if key_only_bridges:
                    continue
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
