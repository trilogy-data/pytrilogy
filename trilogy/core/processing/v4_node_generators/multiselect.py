"""MULTISELECT generator: a merge/align of independently planned arms.

Not reachable through `dispatch.build_node` — a multiselect is never a group in
the group graph. `concept_strategies_v4._search_concepts` intercepts a request
the multiselect produces in full and routes it here; a rowset-wrapped one is
planned inside `rowset.resolve_rowset`.
"""

from collections import defaultdict

from trilogy.core.enums import JoinType, Modifier, Purpose
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import (
    BuildConcept,
    BuildMultiSelectLineage,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import ConceptPair
from trilogy.core.processing.condition_utility import combine_where_clauses
from trilogy.core.processing.nodes import MergeNode, NodeJoin, StrategyNode
from trilogy.core.processing.v4_helper.history import V4History

from .condition_sources import resolve_and_inject_condition
from .nested_select import plan_align_arms


def extra_align_joins(
    base: BuildMultiSelectLineage,
    environment: BuildEnvironment,
    parents: list[StrategyNode],
) -> list[NodeJoin]:
    """Build the FULL-JOIN chain that aligns multiselect rowset CTEs.

    For N parent CTEs, emit N-1 joins anchored on the first parent. The Nth
    parent's join binds its aligned concepts against EVERY prior parent (not
    just the anchor). This matters for ROLLUP-style cascades where coarser
    levels emit NULLs for some aligned columns: with only the anchor in the
    ON clause, the grand-total CTE's (NULL, NULL) row would `IS NOT DISTINCT
    FROM` the anchor's NULLs and get absorbed into per-channel rows. Binding
    against every prior parent (rendered as `coalesce(prior1, prior2, ...) =
    rightN` by `_build_joinkeys`) keeps the level-N row distinct.
    """
    node_merge_concept_map: dict[StrategyNode, list[BuildConcept]] = defaultdict(list)
    for align in base.align.items:
        jc = environment.concepts[align.aligned_concept]
        if jc.purpose == Purpose.CONSTANT:
            continue
        for node in parents:
            for item in align.concepts:
                if item in node.output_lcl:
                    node_merge_concept_map[node].append(jc)

    relevant = list(node_merge_concept_map.keys())
    if len(relevant) < 2:
        return []

    anchor = relevant[0]
    output: list[NodeJoin] = []
    for i in range(1, len(relevant)):
        right = relevant[i]
        priors = relevant[:i]
        right_concepts = [
            c
            for c in node_merge_concept_map[right]
            if any(c in node_merge_concept_map[p] for p in priors)
        ]
        concept_pairs: list[ConceptPair] = []
        for c in right_concepts:
            for prior in priors:
                if c not in node_merge_concept_map[prior]:
                    continue
                concept_pairs.append(
                    ConceptPair(
                        left=c,
                        right=c,
                        existing_datasource=prior.resolve(),
                        modifiers=[Modifier.NULLABLE],
                    )
                )
        output.append(
            NodeJoin(
                left_node=anchor,
                right_node=right,
                concepts=right_concepts,
                concept_pairs=concept_pairs or None,
                join_type=JoinType.FULL,
                modifiers=[Modifier.NULLABLE],
            )
        )
    return output


def gen_multiselect(
    ms_concept: BuildConcept,
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    history: V4History,
    conditions: list[BuildWhereClause],
) -> StrategyNode | None:
    """Plan a top-level multiselect (merge/align).

    Each arm is recursively planned by the v4 searcher (mirroring how rowsets
    recurse per branch), then the arms are stitched together with one FULL
    join per extra arm on the alignment concepts. The outer WHERE is a
    post-join filter."""
    lineage = ms_concept.lineage
    assert isinstance(lineage, BuildMultiSelectLineage)

    plans = plan_align_arms(lineage, history, depth, "multiselect arm")
    if plans is None:
        return None

    arm_nodes: list[StrategyNode] = []
    for plan in plans:
        arm_node = plan.node
        # Expose each arm's alignment key under the merge concept's address so
        # `extra_align_joins` can bind the arms together on it.
        for out in list(arm_node.output_concepts):
            merge_name = lineage.get_merge_concept(out)
            if merge_name:
                arm_node.output_concepts.append(environment.concepts[merge_name])
        arm_node.rebuild_cache()
        arm_nodes.append(arm_node)

    node_joins = extra_align_joins(lineage, environment, arm_nodes)
    merged_outputs = [
        c
        for arm in arm_nodes
        for c in arm.output_concepts
        if c.address not in (arm.hidden_concepts or set())
    ]
    node: StrategyNode = MergeNode(
        input_concepts=merged_outputs,
        output_concepts=merged_outputs,
        environment=environment,
        depth=depth,
        parents=arm_nodes,
        node_joins=node_joins,
    )

    # An outer WHERE can reference concepts from both arms, so it can only be
    # applied above the merge.
    combined = combine_where_clauses(conditions)
    if combined is not None:
        node = resolve_and_inject_condition(
            node,
            combined,
            list(mandatory_list),
            environment=environment,
            graph=g,
            history=history,
            depth=depth,
        )

    node.set_output_concepts(list(mandatory_list), rebuild=False)
    node.rebuild_cache()
    return node
