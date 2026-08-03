"""MULTISELECT generator: a merge/align of independently planned arms.

Not reachable through `dispatch.build_node` — a multiselect is never a group in
the group graph. `concept_strategies_v4._search_concepts` intercepts a request
the multiselect produces in full and routes it here; a rowset-wrapped one is
planned inside `rowset.resolve_rowset`.
"""

from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import (
    BuildConcept,
    BuildMultiSelectLineage,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import combine_where_clauses
from trilogy.core.processing.node_generators.multiselect_node import extra_align_joins
from trilogy.core.processing.nodes import MergeNode, StrategyNode
from trilogy.core.processing.v4_helper.history import V4History

from .condition_sources import resolve_and_inject_condition
from .nested_select import plan_align_arms


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
    post-join filter. Same shape as the v3 multiselect generator, but the
    per-arm recursion goes through v4 rather than v3's `get_query_node`."""
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
