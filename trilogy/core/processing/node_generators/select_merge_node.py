from trilogy.constants import logger
from trilogy.core import graph as nx
from trilogy.core.enums import (
    Derivation,
    Granularity,
    JoinType,
    Purpose,
)
from trilogy.core.graph_models import (
    ReferenceGraph,
    SearchCriteria,
    concept_to_node,
    prune_sources_for_aggregates,
    prune_sources_for_conditions,
)
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildGrain,
    BuildUnionDatasource,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.aggregate_rollup import (
    _is_additive_aggregate,
    get_additive_rollup_concepts,
)
from trilogy.core.processing.condition_utility import (
    combine_condition_atoms,
    condition_implies,
    condition_required_addresses,
    decompose_condition,
    is_scalar_condition,
    merge_conditions,
)
from trilogy.core.processing.discovery_validation import (
    ValidationResult,
    validate_stack,
)
from trilogy.core.processing.node_generators.common import reinject_common_join_keys_v2
from trilogy.core.processing.node_generators.select_helpers.condition_routing import (
    ConditionExpression,
    covered_conditions,
)
from trilogy.core.processing.node_generators.select_helpers.datasource_injection import (
    get_union_sources,
)
from trilogy.core.processing.node_generators.select_helpers.datasource_nodes import (
    SourceNodeCandidate,
    create_datasource_node,
    create_select_node,
    create_select_node_candidate,
    create_union_datasource,
    finalize_select_node,
)
from trilogy.core.processing.node_generators.select_helpers.source_scoring import (
    deduplicate_datasources,
    get_graph_partial_nodes,
    get_materialization_score,
    resolve_subgraphs,
    score_datasource_node,
    subgraph_is_complete,
)
from trilogy.core.processing.nodes import (
    ConstantNode,
    MergeNode,
    StrategyNode,
)
from trilogy.core.processing.utility import padding
from trilogy.utility import unique

LOGGER_PREFIX = "[GEN_ROOT_MERGE_NODE]"
__all__ = [
    "SearchCriteria",
    "create_datasource_node",
    "create_pruned_concept_graph",
    "create_select_node",
    "create_union_datasource",
    "gen_select_merge_node",
    "get_graph_partial_nodes",
    "get_materialization_score",
    "resolve_subgraphs",
    "score_datasource_node",
]


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
    concepts_by_address = {c.address: c for c in orig_g.concepts.values()}
    target_grain = BuildGrain.from_concepts(all_concepts)
    for node_address, datasource in list(g.datasources.items()):
        if not isinstance(datasource, BuildDatasource):
            continue
        for concept in get_additive_rollup_concepts(
            datasource=datasource,
            requested_concepts=all_concepts,
            concepts_by_address=concepts_by_address,
            datasources=datasources,
            target_grain=target_grain,
            conditions=conditions,
        ):
            cnode = concept_to_node(concept)
            g.concepts[cnode] = concept
            g.add_node(cnode)
            g.add_edge(node_address, cnode)
            g.add_edge(cnode, node_address)

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
    prune_sources_for_aggregates(g, all_concepts, logger, orig_g=orig_g)

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
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} Relevant datasets after pruning: {relevant_datasets}"
    )

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
    orig_concepts = list(concepts)
    sourceable_condition_atoms = (
        _sourceable_condition_atoms(conditions, environment) if conditions else []
    )
    concept_attempts = [orig_concepts]
    if sourceable_condition_atoms:
        # Bring the filter columns into the graph so they surface as parent
        # outputs; when deferring (below) the WHERE is reapplied once at the
        # merge instead of being pushed into each subselect.
        augmented = unique(
            orig_concepts
            + _condition_source_concepts(sourceable_condition_atoms, environment),
            "address",
        )
        if {c.address for c in augmented} != {c.address for c in orig_concepts}:
            concept_attempts.append(augmented)
    search_attempts = [SearchCriteria.FULL_ONLY]
    if accept_partial:
        search_attempts.append(SearchCriteria.PARTIAL_UNSCOPED)
        search_attempts.append(SearchCriteria.PARTIAL_INCLUDING_SCOPED)
    for concepts in concept_attempts:
        # Only defer when this is the primary pruning pass. The partial-datasource
        # path uses filter_conditions and must still push per-source WHEREs.
        defer_conditions_to_merge = (
            filter_conditions is None
            and conditions is not None
            and _conditions_deferrable_to_merge(orig_concepts, conditions, environment)
        )
        select_conditions = (
            filter_conditions if filter_conditions is not None else conditions
        )
        pruned: ReferenceGraph | None = None
        sub_nodes: dict[str, list[str]] = {}
        for attempt in search_attempts:
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
            continue
        # On the augmented attempt the concept set was widened with filter-only
        # columns; the originally requested concepts still define grain so a
        # finer source gets regrouped instead of fanning the request out.
        requested_concepts = orig_concepts if concepts is not orig_concepts else None
        candidates = [
            create_select_node_candidate(
                k,
                subgraph,
                g=pruned,
                accept_partial=accept_partial,
                environment=environment,
                depth=depth,
                conditions=select_conditions,
            )
            for k, subgraph in sub_nodes.items()
        ]
        covering_candidates = [
            candidate
            for candidate in candidates
            if _candidate_satisfies_request(candidate, orig_concepts, select_conditions)
        ]
        if covering_candidates:
            best = min(
                covering_candidates,
                key=lambda c: (
                    c.group_source_count,
                    len(c.node.usable_outputs),
                    str(c.node),
                ),
            )
            return [
                finalize_select_node(
                    best,
                    environment=environment,
                    depth=depth,
                    requested_concepts=requested_concepts,
                )
            ]
        if conditions and len(sub_nodes) > 1:
            trial = [
                create_select_node_candidate(
                    k,
                    subgraph,
                    g=pruned,
                    accept_partial=accept_partial,
                    environment=environment,
                    depth=depth,
                    conditions=None,
                )
                for k, subgraph in sub_nodes.items()
            ]
            # Only defer for a flat star: every source must be a single ungrouped
            # scan, so the merge can apply the WHERE across the joined rowset.
            safe = all(t.group_source_count == 0 and not t.force_group for t in trial)
            if defer_conditions_to_merge and safe:
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} conditions sourceable by "
                    f"components; deferring WHERE to merge across {len(sub_nodes)} "
                    "sources rather than pushing into each subselect"
                )
                candidates = trial
            else:
                defer_conditions_to_merge = False
                grouped_candidates = [
                    filtered
                    for filtered, unfiltered in zip(candidates, trial, strict=True)
                    if unfiltered.group_source_count > 0 or unfiltered.force_group
                ]
                if grouped_candidates:
                    mixed = [
                        (
                            unfiltered
                            if unfiltered.group_source_count == 0
                            and not unfiltered.force_group
                            else filtered
                        )
                        for filtered, unfiltered in zip(candidates, trial, strict=True)
                    ]
                    pushed = _condition_atoms_applied_by_candidates(grouped_candidates)
                    remaining = combine_condition_atoms(
                        [
                            atom
                            for atom in decompose_condition(conditions.conditional)
                            if atom not in pushed
                        ]
                    )
                    if remaining and _condition_can_apply_after_merge(mixed, remaining):
                        logger.info(
                            f"{padding(depth)}{LOGGER_PREFIX} progressively routing WHERE; "
                            "grouped sources keep applicable atoms and flat sources defer "
                            "remaining atoms to the merge"
                        )
                        candidates = mixed
        if select_conditions and not _candidates_route_conditions(
            candidates, select_conditions
        ):
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} candidates cannot route WHERE "
                f"{select_conditions}; trying next concept set"
            )
            continue
        group_source_count = sum(c.group_source_count for c in candidates)
        grouped_condition_deferred = any(
            c.force_group and c.conditions_deferred for c in candidates
        )
        defer_group = (
            len(candidates) > 1
            and group_source_count == 1
            and grouped_condition_deferred
        )
        if len(candidates) > 1 and group_source_count > 1:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} keeping source groups before merge; "
                f"{group_source_count} grouped source branches would be joined."
            )
        return [
            finalize_select_node(
                candidate,
                environment=environment,
                depth=depth,
                defer_group=defer_group,
                requested_concepts=requested_concepts,
            )
            for candidate in candidates
        ]
    return []


def _conditions_can_be_sourced_by_components(
    concepts: list[BuildConcept],
    conditions: BuildWhereClause,
    environment: BuildEnvironment,
) -> bool:
    return len(_sourceable_condition_atoms(conditions, environment)) == len(
        decompose_condition(conditions.conditional)
    )


def _sourceable_condition_atoms(
    conditions: BuildWhereClause,
    environment: BuildEnvironment,
) -> list[ConditionExpression]:
    datasources = [
        ds
        for ds in environment.datasources.values()
        if isinstance(ds, BuildDatasource)
        and not ds.non_partial_for
        and not any(c.is_aggregate for c in ds.output_concepts)
    ]
    if not datasources:
        return []
    available: set[str] = set()
    for ds in datasources:
        partial = {c.canonical_address for c in ds.partial_concepts}
        available.update(
            c.canonical_address
            for c in ds.output_concepts
            if c.canonical_address not in partial
        )
    sourceable = []
    for atom in decompose_condition(conditions.conditional):
        if any(arg for group in atom.existence_arguments for arg in group):
            continue
        if not is_scalar_condition(atom):
            continue
        if condition_required_addresses(atom).issubset(available):
            sourceable.append(atom)
    return sourceable


def _conditions_deferrable_to_merge(
    concepts: list[BuildConcept],
    conditions: BuildWhereClause,
    environment: BuildEnvironment,
) -> bool:
    """Whether the WHERE can be merged-then-reapplied rather than pushed per source.

    This is only the early routing check: every atom must be coverable by a
    complete, non-aggregate source. The caller still builds conditionless trial
    candidates and only accepts deferral for flat source scans.
    """
    return _conditions_can_be_sourced_by_components(concepts, conditions, environment)


def _condition_source_concepts(
    atoms: list[ConditionExpression],
    environment: BuildEnvironment,
) -> list[BuildConcept]:
    concepts = [c for atom in atoms for c in atom.row_arguments]
    seen = {c.address for c in concepts}
    for concept in list(concepts):
        for key in concept.keys or []:
            if key in seen:
                continue
            key_concept = environment.concepts.get(key)
            if key_concept is None:
                continue
            concepts.append(key_concept)
            seen.add(key)
    return concepts


def _node_condition_atoms(node: StrategyNode) -> list[ConditionExpression]:
    atoms: list[ConditionExpression] = []
    for expr in (node.conditions, node.preexisting_conditions):
        if expr is not None:
            atoms.extend(decompose_condition(expr))
    return atoms


def _condition_can_apply_after_node_merge(
    nodes: list[StrategyNode],
    condition: ConditionExpression,
) -> bool:
    if not is_scalar_condition(condition):
        return False
    if any(
        arg
        for atom in decompose_condition(condition)
        for group in atom.existence_arguments
        for arg in group
    ):
        return False
    available = {c.canonical_address for node in nodes for c in node.usable_outputs}
    return condition_required_addresses(condition).issubset(available)


def _condition_atoms_applied_by_candidates(
    candidates: list[SourceNodeCandidate],
) -> list[ConditionExpression]:
    return [a for c in candidates for a in _node_condition_atoms(c.node)]


def _condition_can_apply_after_merge(
    candidates: list[SourceNodeCandidate],
    condition: ConditionExpression,
) -> bool:
    return _condition_can_apply_after_node_merge(
        [c.node for c in candidates], condition
    )


def _candidates_route_conditions(
    candidates: list[SourceNodeCandidate],
    conditions: BuildWhereClause,
) -> bool:
    pushed = _condition_atoms_applied_by_candidates(candidates)
    remaining = combine_condition_atoms(
        [
            atom
            for atom in decompose_condition(conditions.conditional)
            if atom not in pushed
        ]
    )
    return remaining is None or _condition_can_apply_after_merge(candidates, remaining)


def _candidate_satisfies_request(
    candidate: SourceNodeCandidate,
    requested: list[BuildConcept],
    conditions: BuildWhereClause | None,
) -> bool:
    if not conditions or candidate.node.preexisting_conditions is None:
        return False
    if not condition_implies(
        candidate.node.preexisting_conditions, conditions.conditional
    ):
        return False
    requested_addresses = {c.canonical_address for c in requested}
    output_addresses = {c.canonical_address for c in candidate.node.usable_outputs}
    partial_addresses = {c.canonical_address for c in candidate.node.partial_concepts}
    return requested_addresses.issubset(output_addresses - partial_addresses)


def _parents_apply_condition_atoms(
    parents: list[StrategyNode],
    conditions: BuildWhereClause,
) -> bool:
    if not parents:
        return False
    parent_atoms = [_node_condition_atoms(parent) for parent in parents]
    for atom in decompose_condition(conditions.conditional):
        if any(arg for group in atom.existence_arguments for arg in group):
            return False
        if not all(atom in atoms for atoms in parent_atoms):
            return False
    return True


def _condition_remaining_after_parents(
    parents: list[StrategyNode],
    conditions: BuildWhereClause,
) -> ConditionExpression | None:
    parent_atoms = [a for parent in parents for a in _node_condition_atoms(parent)]
    return combine_condition_atoms(
        [
            atom
            for atom in decompose_condition(conditions.conditional)
            if atom not in parent_atoms
        ]
    )


def _condition_can_apply_after_parent_merge(
    parents: list[StrategyNode],
    condition: ConditionExpression,
) -> bool:
    return _condition_can_apply_after_node_merge(parents, condition)


def _merge_condition_routing(
    parents: list[StrategyNode],
    output_concepts: list[BuildConcept],
    conditions: BuildWhereClause | None,
) -> tuple[ConditionExpression | None, ConditionExpression | None, JoinType | None]:
    if conditions is None:
        return None, None, None
    condition = conditions.conditional
    if all(
        x.preexisting_conditions and x.preexisting_conditions == condition
        for x in parents
    ):
        return condition, None, None
    if _parents_apply_condition_atoms(parents, conditions):
        merge_condition = (
            condition
            if _condition_can_apply_after_parent_merge(parents, condition)
            else None
        )
        return condition, merge_condition, None
    remaining_conditions = _condition_remaining_after_parents(parents, conditions)
    if remaining_conditions and _condition_can_apply_after_parent_merge(
        parents, remaining_conditions
    ):
        return condition, remaining_conditions, None
    if remaining_conditions is None:
        output_addrs = {c.address for c in output_concepts}
        for parent in parents:
            if parent.preexisting_conditions is None:
                continue
            if not (
                parent.preexisting_conditions == condition
                or condition_implies(parent.preexisting_conditions, condition)
            ):
                continue
            parent_addrs = {c.address for c in parent.usable_outputs}
            if parent_addrs and parent_addrs.issubset(output_addrs):
                return condition, None, JoinType.INNER

    # Filter applied at one parent (e.g. a partial-aggregate rollup) plus pure
    # enumerator joins: the conditioned parent already carries the merge output
    # set, so unmatched enumerator rows should not leak into the result.
    output_addrs = {c.address for c in output_concepts}
    for parent in parents:
        if parent.preexisting_conditions == condition and output_addrs.issubset(
            {c.address for c in parent.usable_outputs}
        ):
            return condition, None, JoinType.INNER
    return None, None, None


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
    parents: list[StrategyNode] = []
    if normals:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} searching for root source graph for concepts {[c.address for c in all_concepts]} and conditions {conditions}"
        )
        parents = _source_concepts_via_graph(
            normals, g, environment, depth, accept_partial, conditions
        )
        if not parents and conditions:
            if _conditions_can_be_sourced_by_components(
                normals, conditions, environment
            ):
                augmented = unique(
                    normals
                    + _condition_source_concepts(
                        decompose_condition(conditions.conditional), environment
                    ),
                    "address",
                )
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} retrying source graph with "
                    "condition inputs; WHERE atoms are covered by component sources."
                )
                parents = _source_concepts_via_graph(
                    augmented,
                    g,
                    environment,
                    depth,
                    accept_partial,
                    conditions,
                )
        if not parents and conditions:
            # Retry with only "covered" condition atoms (those implied by some datasource's
            # non_partial_for) for graph pruning. Foreign datasources (e.g. tree_enrichment
            # when filtering by city='USSFO') are kept via the intersection check since the
            # condition is guaranteed to be applied by the owning partial datasource.
            # The original conditions are still passed as filter_conditions so that
            # per-datasource WHERE clauses (e.g. tree_category='deciduous') are preserved.
            covered = covered_conditions(conditions, environment)
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

    if len(parents) == 1 and not constants:
        candidate: StrategyNode = parents[0]
    else:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Multiple parent DS nodes resolved - {[type(x) for x in parents]}, wrapping in merge"
        )

        preexisting_conditions = None
        force_join_type: JoinType | None = None
        merge_conditions = None
        preexisting_conditions, merge_conditions, force_join_type = (
            _merge_condition_routing(parents, all_concepts, conditions)
        )

        # When the merge's joined grain (e.g. customer_id from agg + dim) is
        # finer than the outer target's grain (e.g. region) — and the target
        # is reachable from the merge grain via property-of-key — the merge
        # needs to SUM-roll additive aggregates up to the target grain.
        # Mark force_group + rollup_concepts so the renderer emits SUM and
        # GROUP BY at the merge level.
        additive_aggs = [
            c for c in all_concepts if c.is_aggregate and _is_additive_aggregate(c)
        ]
        rollup_at_merge: list[BuildConcept] = []
        force_merge_group: bool | None = None
        if additive_aggs and len(additive_aggs) == sum(
            1 for c in all_concepts if c.is_aggregate
        ):
            merge_components: set[str] = set()
            for parent_node in parents:
                pg = parent_node.grain
                if pg and pg.components:
                    merge_components.update(pg.components)
            target_components = {c.address for c in all_concepts if not c.is_aggregate}
            unreached = target_components - merge_components
            unreached_via_property = bool(unreached) and all(
                (concept := environment.concepts.get(tc)) is not None
                and concept.purpose == Purpose.PROPERTY
                and concept.keys
                and concept.keys.issubset(merge_components)
                for tc in unreached
            )
            if (
                merge_components
                and target_components
                and target_components != merge_components
                and unreached_via_property
            ):
                rollup_at_merge = additive_aggs
                force_merge_group = True

        candidate = MergeNode(
            output_concepts=all_concepts,
            input_concepts=unique(
                normals
                + abstract_props
                + (list(merge_conditions.row_arguments) if merge_conditions else []),
                "address",
            ),
            environment=environment,
            depth=depth,
            parents=parents,
            conditions=merge_conditions,
            preexisting_conditions=preexisting_conditions,
            force_join_type=force_join_type,
            force_group=force_merge_group,
            rollup_concepts=rollup_at_merge or None,
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
