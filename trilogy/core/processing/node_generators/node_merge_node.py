from typing import cast

from trilogy.constants import logger
from trilogy.core import graph as nx
from trilogy.core.enums import Derivation, FunctionType
from trilogy.core.exceptions import AmbiguousRelationshipResolutionException
from trilogy.core.graph import approximation as ax
from trilogy.core.graph_models import (
    ReferenceGraph,
    SearchCriteria,
    concept_to_node,
    prune_sources_for_conditions,
)
from trilogy.core.models.build import (
    BuildConcept,
    BuildConditional,
    BuildFunction,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import (
    combine_condition_atoms,
    decompose_condition,
    preserved_non_partial_conditions,
)
from trilogy.core.processing.node_generators.common import (
    authored_join_pair_candidates,
    inject_authored_join_key_terminals,
    reinject_common_join_keys_v2,
)
from trilogy.core.processing.nodes import History, MergeNode, StrategyNode
from trilogy.core.processing.rowset_islanding import (
    extract_address,
    island_rowsets_for_weak_merge,
)
from trilogy.core.processing.utility import padding
from trilogy.utility import unique

LOGGER_PREFIX = "[GEN_MERGE_NODE]"
AMBIGUITY_CHECK_LIMIT = 20
EGO_RADIUS = 10


def filter_pseudonyms_for_source(
    ds_graph: nx.DiGraph, node: str, pseudonyms: set[tuple[str, str]]
):
    to_remove = set()
    for edge in ds_graph.edges:
        if edge in pseudonyms:
            lengths = {}
            for n in edge:
                try:
                    lengths[n] = nx.shortest_path_length(ds_graph, node, n)
                except nx.NetworkXNoPath:
                    lengths[n] = 999
            to_remove.add(max(lengths, key=lambda x: lengths.get(x, 0)))
    for node in to_remove:
        ds_graph.remove_node(node)


def extract_concept(node: str, env: BuildEnvironment):
    # removing this as part of canonical mapping
    # if node in env.alias_origin_lookup:
    #     return env.alias_origin_lookup[node]
    return env.canonical_concepts[node]


def filter_unique_graphs(graphs: list[list[str]]) -> list[list[str]]:
    unique_graphs: list[set[str]] = []

    # sort graphs from largest to smallest
    graphs.sort(key=lambda x: len(x), reverse=True)
    for graph in graphs:
        if not any(set(graph).issubset(x) for x in unique_graphs):
            unique_graphs.append(set(graph))

    return [list(x) for x in unique_graphs]


def extract_ds_components(
    g: nx.DiGraph,
    nodelist: list[str],
    base: ReferenceGraph,
    scoped_pairs: list[tuple[str, str]] | None = None,
) -> list[list[str]]:
    graphs = []
    component_ds: list[str] = []
    # from trilogy.hooks.graph_hook import GraphHook
    # GraphHook().query_graph_built(g, highlight_nodes=nodelist)
    for node in sorted(g.nodes):
        if not node.startswith("ds~"):
            continue
        local = g.copy()
        filter_pseudonyms_for_source(local, node, base.pseudonyms)
        ds_graph: nx.DiGraph = nx.ego_graph(local, node, radius=EGO_RADIUS).copy()
        graphs.append(
            sorted(
                extract_address(x) for x in ds_graph.nodes if str(x).startswith("c~")
            )
        )
        component_ds.append(node)
    # if we had no ego graphs, return all concepts
    if not graphs:
        return [[extract_address(node) for node in nodelist]]
    # AUTHORED merged-key mates only (scoped join / merge declarations). The
    # base graph's pseudonym edges also link role-aliased imports of one model
    # (`pos_address` vs `current_address`) — those are distinct roles, and
    # matching through them cross-pollutes components until unrelated
    # dimensions fuse into one 1=1-joined subgraph (q64).
    authored_mates: dict[str, set[str]] = {}
    for aa, bb in scoped_pairs or []:
        if aa != bb:
            authored_mates.setdefault(aa, set()).add(bb)
            authored_mates.setdefault(bb, set()).add(aa)
    for node in nodelist:
        parsed = extract_address(node)
        # A merged key reached through a pseudonym edge (a scoped-join mate on
        # another source) can miss the components that bind it: the minimal
        # tree keeps only the cheapest path to the canonical, so a side's own
        # member never enters its ego graph. Each such side must still
        # materialize ITS member — otherwise its parent is dedup-swallowed (or
        # planned without the key), the equality drops out of the merge join,
        # and the remaining-key join fans out (q05 union arm subset-joining
        # web_returns→web_sales on the full composite grain). A target found
        # in NO component attaches by exact address wherever bound (else it
        # becomes a singleton subgraph); a target with authored mates also
        # attaches each mate to the components binding it.
        mates = authored_mates.get(parsed, set())
        attached = any(parsed in x for x in graphs)
        if attached and not mates:
            continue
        candidates = mates if attached else {parsed} | mates
        for i, ds_node in enumerate(component_ds):
            ds = base.datasources.get(ds_node)
            if ds is None:
                continue
            for col in ds.columns:
                if col.concept.address in candidates:
                    if col.concept.address not in graphs[i]:
                        graphs[i] = sorted(set(graphs[i]) | {col.concept.address})
                    attached = True
        if not attached:
            graphs.append([parsed])
    graphs = filter_unique_graphs(graphs)
    return graphs


def determine_induced_minimal_nodes(
    G: ReferenceGraph,
    nodelist: list[str],
    environment: BuildEnvironment,
    filter_downstream: bool,
    accept_partial: bool = False,
    synonyms: dict[str, str] = {},
) -> nx.DiGraph | None:
    # to_undirected already returns a fresh graph with independent rust core
    # and attrs; the extra .copy() is double work.
    H: nx.Graph = nx.to_undirected(G)
    nodelist_set = set(nodelist)

    # Add weights to edges based on target node's derivation type. The
    # default weight is 1 (used by _weight_triples when `weight` key is
    # missing), so we only write to _edge_attrs for the rare BASIC non
    # ATTR_ACCESS case — avoids a per-edge __setitem__ through the layered
    # edge-view API, which dominated this function's cost at ~874k calls
    # per adhoc_perf run.
    g_concepts = G.concepts
    H_edge_attrs = H._edge_attrs  # type: ignore[attr-defined]
    for edge in G.edges():
        target = edge[1]
        target_lookup = g_concepts.get(target)
        if (
            target_lookup is not None
            and target_lookup.derivation == Derivation.BASIC
            and not (
                isinstance(target_lookup.lineage, BuildFunction)
                and target_lookup.lineage.operator == FunctionType.ATTR_ACCESS
            )
        ):
            # H is undirected so the edge key is (min, max) order.
            left, right = edge[0], target
            key = (left, right) if left <= right else (right, left)
            attrs = H_edge_attrs.get(key)
            if attrs is None:
                H_edge_attrs[key] = {"weight": 50}
            else:
                attrs["weight"] = 50

    nodes_to_remove = []
    derivations_to_remove = (
        Derivation.CONSTANT,
        Derivation.AGGREGATE,
        Derivation.FILTER,
    )
    # Mandatory concepts that are datasource-materialized reach this search
    # promoted "as root instead of derived" (a summary table binding
    # `count(x) by k` makes that aggregate a directly selectable column), so
    # their nodes must survive the derivation purge below — otherwise the
    # dijkstra seed list references a deleted node and the search dies with
    # NodeNotFound. Exempt every grain variant of those canonicals; other
    # aggregate/filter nodes stay purged to avoid ambiguous relation chains.
    materialized = environment.materialized_canonical_concepts
    mandatory_materialized = {
        canonical
        for n in nodelist
        if (c := g_concepts.get(n)) is not None
        and (canonical := c.canonical_address) in materialized
    }
    for node, lookup in g_concepts.items():
        # inclusion of aggregates can create ambiguous node relation chains
        # there may be a better way to handle this
        # can be revisited if we need to connect a derived synonym based on an aggregate
        if lookup.derivation in derivations_to_remove:
            if lookup.canonical_address in mandatory_materialized:
                continue
            nodes_to_remove.append(node)
        # purge a node if we're already looking for all it's parents — but
        # keep BASIC concepts that are also directly bound to a datasource
        # column. Their binding is a valid source path on par with ROOT
        # (decomposing into the lineage parents may strand them when the
        # parents can't be co-sourced with other required dimensions). ROWSET
        # outputs are also exempt: a rowset is sourced as one opaque unit (its
        # internals are not navigable parents), so it anchors a join like ROOT.
        elif (
            filter_downstream
            and lookup.derivation not in (Derivation.ROOT, Derivation.ROWSET)
            and lookup.canonical_address
            not in environment.materialized_canonical_concepts
        ):
            nodes_to_remove.append(node)
    if nodes_to_remove:
        # logger.debug(f"Removing nodes {nodes_to_remove} from graph")
        H.remove_nodes_from(nodes_to_remove)
    isolates = list(nx.isolates(H))
    if isolates:
        # logger.debug(f"Removing isolates {isolates} from graph")
        H.remove_nodes_from(isolates)

    zero_out = [x for x in H.nodes if G.out_degree(x) == 0 and x not in nodelist_set]
    while zero_out:
        logger.debug(f"Removing zero out nodes {zero_out} from graph")
        H.remove_nodes_from(zero_out)
        zero_out = [
            x for x in H.nodes if G.out_degree(x) == 0 and x not in nodelist_set
        ]
    try:
        # Use weight attribute for Dijkstra pathfinding
        paths = nx.multi_source_dijkstra_path(H, nodelist, weight="weight")
        # logger.debug(f"Paths found for {nodelist} {paths}")
    except nx.exception.NodeNotFound as e:
        logger.debug(f"Unable to find paths for {nodelist}- {e!s}")
        return None
    path_removals = list(x for x in H.nodes if x not in paths)
    if path_removals:
        # logger.debug(f"Removing paths {path_removals} from graph")
        H.remove_nodes_from(path_removals)
    # logger.debug(f"Graph after path removal {H.nodes}")
    # steiner_tree/subgraph already return fresh graphs with independent cores.
    sG: nx.Graph = ax.steinertree.steiner_tree(H, nodelist, weight="weight")
    if not sG.nodes:
        logger.debug(f"No Steiner tree found for nodes {nodelist}")
        return None

    logger.debug(f"Steiner tree found for nodes {nodelist} {sG.nodes}")
    final = cast(ReferenceGraph, nx.subgraph(G, sG.nodes))

    final_nodes = set(final.nodes)
    for edge in G.edges:
        if edge[1] in final_nodes and edge[0].startswith("ds~"):
            ds = G.datasources[edge[0]]
            concept = environment.canonical_concepts[extract_address(edge[1])]
            if not accept_partial:
                partial_addresses = {x.address for x in ds.partial_concepts}
                if concept.address in partial_addresses:
                    continue
            final.add_edge(*edge)

    # readd concepts that need to be in the output for proper discovery
    reinject_common_join_keys_v2(G, final, synonyms)

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
            f"Skipping graph for initial list {nodelist} as missing nodes {missing} from final graph {final.nodes}"
        )

        return None
    logger.debug(f"Found final graph {final.nodes}")
    return final


def canonicalize_addresses(
    reduced_concept_set: set[str], environment: BuildEnvironment
) -> set[str]:
    """
    Convert a set of concept addresses to their canonical form.
    This is necessary to ensure that we can compare concepts correctly,
    especially when dealing with aliases or pseudonyms.
    """
    return set(
        environment.concepts[x].address if x in environment.concepts else x
        for x in reduced_concept_set
    )


def detect_ambiguity_and_raise(
    all_concepts: list[BuildConcept],
    reduced_concept_sets_raw: list[set[str]],
    environment: BuildEnvironment,
) -> None:
    final_candidates: list[set[str]] = []
    common: set[str] = set()
    # find all values that show up in every join_additions
    reduced_concept_sets = [
        canonicalize_addresses(x, environment) for x in reduced_concept_sets_raw
    ]
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
            message=f"Multiple possible concept additions (intermediate join keys) found to resolve {[x.address for x in all_concepts]}, have {' or '.join([str(x) for x in reduced_concept_sets])}. Different paths are is: {filtered_paths}",
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


def inject_property_key_terminals(
    all_concepts: list[BuildConcept],
    environment: BuildEnvironment,
) -> list[BuildConcept]:
    """Force the key of each requested property into the resolution as a mandatory
    terminal, but only when that key is itself a foreign key bound at a finer grain
    (i.e. the key has its own keys). Without it the minimal-tree search can bridge
    the property's dimension straight to the anchor on a coarser shared grain
    component (a non-unique key → fan-out) instead of routing through the
    intermediate datasource that carries the key. Base entity keys (no keys of
    their own) sit directly on the anchor, so forcing them only perturbs the plan;
    only materialized keys can anchor a join."""
    existing = {c.address for c in all_concepts}
    grain_keys = environment.domain_graph.sole_grain_keys()
    additions: list[BuildConcept] = []
    for c in all_concepts:
        # A concept that is itself a 1:1 dimension grain key is directly
        # sourceable at its own grain; its declared `keys` are an FK-path
        # artifact. Promoting them routes through the coarser entity that
        # carries it (customer for customer.address.id) and fans the
        # enrichment out by that entity's population. Leave it (q64).
        if c.address in grain_keys:
            continue
        for key_addr in c.keys or set():
            if key_addr in existing:
                continue
            key = environment.concepts.get(key_addr)
            if (
                key is None
                or not key.keys
                or key.canonical_address
                not in environment.materialized_canonical_concepts
            ):
                continue
            existing.add(key_addr)
            additions.append(key)
    if not additions:
        return all_concepts
    return unique(all_concepts + additions, "address")


def resolve_weak_components(
    all_concepts: list[BuildConcept],
    environment: BuildEnvironment,
    environment_graph: ReferenceGraph,
    filter_downstream: bool = True,
    accept_partial: bool = False,
    search_conditions: BuildWhereClause | None = None,
) -> list[list[BuildConcept]] | None:
    # order matters: property-key promotion must not see (and further promote)
    # the authored-join terminals injected below
    all_concepts = inject_property_key_terminals(all_concepts, environment)
    all_concepts = inject_authored_join_key_terminals(all_concepts, environment)
    break_flag = False
    found = []
    search_graph = environment_graph.copy()
    prune_sources_for_conditions(
        search_graph,
        (
            SearchCriteria.PARTIAL_INCLUDING_SCOPED
            if accept_partial
            else SearchCriteria.FULL_ONLY
        ),
        conditions=search_conditions,
    )
    island_rowsets_for_weak_merge(search_graph, all_concepts)
    reduced_concept_sets: list[set[str]] = []

    count = 0
    node_list = sorted(
        [
            concept_to_node(c.with_default_grain())
            for c in all_concepts
            if "__preql_internal" not in c.address
        ]
    )
    synonyms: dict[str, str] = {}
    for c in all_concepts:
        for x in c.pseudonyms:
            synonyms[x] = c.address
    # from trilogy.hooks.graph_hook import GraphHook
    # GraphHook().query_graph_built(search_graph, highlight_nodes=[concept_to_node(c.with_default_grain()) for c in all_concepts if "__preql_internal" not in c.address])

    # loop through, removing new nodes we find
    # to ensure there are not ambiguous discovery paths
    # (if we did not care about raising ambiguity errors, we could just use the first one)
    while break_flag is not True:
        g: nx.DiGraph | None = None
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
                synonyms=synonyms,
            )

            if not g or not g.nodes:
                break_flag = True
                continue
            if not nx.is_weakly_connected(g):
                break_flag = True
                continue
            # from trilogy.hooks.graph_hook import GraphHook
            # GraphHook().query_graph_built(g, highlight_nodes=[concept_to_node(c.with_default_grain()) for c in all_concepts if "__preql_internal" not in c.address])
            all_graph_concepts = [
                extract_concept(extract_address(node), environment)
                for node in g.nodes
                if node.startswith("c~")
            ]
            new = [x for x in all_graph_concepts if x.address not in all_concepts]

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
            new_addresses = set([x.address for x in new if x.address not in synonyms])
            reduced_concept_sets.append(new_addresses)

        except nx.exception.NetworkXNoPath:
            break_flag = True
        if g and not g.nodes:
            break_flag = True
    if not found:
        return None

    detect_ambiguity_and_raise(all_concepts, reduced_concept_sets, environment)

    # take our first one as the actual graph
    g = found[0]

    subgraphs: list[list[BuildConcept]] = []
    # components = nx.strongly_connected_components(g)
    node_list = [x for x in g.nodes if x.startswith("c~")]
    authored_pairs = [
        pair_addrs
        for pair in authored_join_pair_candidates(environment)
        for pair_addrs in (
            (pair.left.address, pair.right.address),
            (pair.left.address, pair.canonical.address),
            (pair.right.address, pair.canonical.address),
        )
    ]
    components = extract_ds_components(
        g, node_list, environment_graph, scoped_pairs=authored_pairs
    )
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
    fgraphs = []
    for subgraph in subgraphs:
        # don't re-enter an unresolvable root when injecting concepts.
        resolvable = [
            x
            for x in subgraph
            if not (
                x.derivation == Derivation.ROOT
                and x.canonical_address
                not in environment.materialized_canonical_concepts
            )
        ]
        if not resolvable:
            logger.info(f"Subgraph {subgraph} is not resolvable, skipping")
            continue
        fgraphs.append(subgraph)
    return fgraphs


def subgraphs_to_merge_node(
    concept_subgraphs: list[list[BuildConcept]],
    depth: int,
    all_concepts: list[BuildConcept],
    environment,
    g,
    source_concepts,
    history,
    conditions,
    output_concepts: list[BuildConcept],
    search_conditions: BuildWhereClause | None = None,
    filter_conditions: BuildWhereClause | None = None,
    enable_early_exit: bool = True,
):

    parents: list[StrategyNode] = []
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} fetching subgraphs {[[c.address for c in subgraph] for subgraph in concept_subgraphs]}"
    )
    for graph in concept_subgraphs:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} fetching subgraph {[c.address for c in graph]}"
        )

        subgraph_addrs = {c.address for c in graph}
        applicable_conditions = (
            filter_conditions if filter_conditions is not None else search_conditions
        )
        subgraph_conditions = (
            _conditions_for_subgraph(applicable_conditions, subgraph_addrs)
            if applicable_conditions
            else None
        )
        parent: StrategyNode | None = source_concepts(
            mandatory_list=graph,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
            conditions=subgraph_conditions,
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
    output_c = []
    for x in parents:
        for y in x.usable_outputs:
            input_c.append(y)
            if y in output_concepts or any(y.address in c.pseudonyms for c in output_concepts) or any(
                c.address in y.pseudonyms for c in output_concepts
            ):
                output_c.append(y)

    if len(parents) == 1 and enable_early_exit:

        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} only one parent node, exiting early w/ {[c.address for c in parents[0].output_concepts]}"
        )
        parent = parents[0]
        return parent

    rval = MergeNode(
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
    return rval


def _conditions_for_subgraph(
    conditions: BuildWhereClause,
    subgraph_addrs: set[str],
) -> BuildWhereClause | None:
    """Return condition atoms whose concepts are all present in the subgraph."""
    atoms = decompose_condition(conditions.conditional)
    relevant = [
        a
        for a in atoms
        if all(c.address in subgraph_addrs for c in a.concept_arguments)
    ]
    cond = combine_condition_atoms(relevant)
    if cond is None:
        return None
    return BuildWhereClause(conditional=cond)


def gen_merge_node(
    all_concepts: list[BuildConcept],
    g: ReferenceGraph,
    environment: BuildEnvironment,
    depth: int,
    source_concepts,
    accept_partial: bool = False,
    history: History | None = None,
    conditions: BuildConditional | None = None,
    search_conditions: BuildWhereClause | None = None,
) -> MergeNode | None:

    # we do not actually APPLY these conditions anywhere
    # though we could look at doing that as an optimization
    # it's important to include them so the base discovery loop that was generating
    # the merge node can then add them automatically
    # so we should not return a node with preexisting conditions
    if search_conditions:
        all_search_concepts = unique(
            all_concepts + list(search_conditions.row_arguments), "address"
        )
        # Preserve only atoms that are satisfied by a datasource's complete_where so
        # that partial-datasource exact-match resolution works inside each subgraph.
        # Extra condition concepts are still in all_search_concepts as projections.
        effective_search_conditions = preserved_non_partial_conditions(
            search_conditions, environment
        )
    else:
        effective_search_conditions = None
        all_search_concepts = all_concepts
    all_search_concepts = sorted(all_search_concepts, key=lambda x: x.address)
    # Constants have no datasource edges and are purged from the search graph
    # before pathfinding; a constant in the mandatory nodelist makes dijkstra
    # die with NodeNotFound and kills the whole merge. Bucket them out of the
    # connectivity search (as the root merge does) — they stay in the output
    # list and render inline.
    searchable_concepts = [
        c for c in all_search_concepts if c.derivation != Derivation.CONSTANT
    ]
    if not searchable_concepts:
        # pure-constant requests are the root merge's job
        return None
    break_set = set([x.address for x in searchable_concepts])
    # Skip condition pruning only when conditions are "owned" by a partial datasource;
    # otherwise retain normal pruning so regular WHERE conditions still gate resolution.
    base_search_conditions = (
        None if effective_search_conditions is not None else search_conditions
    )
    # Pruning by a non-partial-owned WHERE is only an optimization to prefer a
    # source that already satisfies it — the predicate is still applied on top by
    # the caller (this node is returned without preexisting conditions). So if
    # pruning disconnects the graph (it strands a join-partner dimension that
    # lacks the condition's columns — e.g. a date dim needed only as a
    # post-aggregate gate, under a fact filtered on item), fall back to an
    # unpruned pass: inject the join key, source everything, let the caller
    # filter. The condition stays live for the aggregate's own input upstream;
    # this only stops the gate dimension's merge from over-pruning.
    condition_attempts: list[BuildWhereClause | None] = (
        [base_search_conditions]
        if base_search_conditions is None
        else [base_search_conditions, None]
    )
    attempts = [(sc, fd) for sc in condition_attempts for fd in (True, False)]
    for resolved_search_conditions, filter_downstream in attempts:
        weak_resolve = resolve_weak_components(
            searchable_concepts,
            environment,
            g,
            filter_downstream=filter_downstream,
            accept_partial=accept_partial,
            search_conditions=resolved_search_conditions,
        )
        if not weak_resolve:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} wasn't able to resolve graph through intermediate concept injection with accept_partial {accept_partial}, filter_downstream {filter_downstream}, pruning {resolved_search_conditions is not None}"
            )
            continue

        log_graph = [[y.address for y in x] for x in weak_resolve]
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Was able to resolve graph through weak component resolution - final graph {log_graph}"
        )
        for flat in log_graph:
            if set(flat) == break_set:
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
            search_conditions=effective_search_conditions,
            filter_conditions=search_conditions,
            output_concepts=all_concepts,
        )
    return None
