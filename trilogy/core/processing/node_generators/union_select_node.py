from typing import List

from trilogy.constants import logger
from trilogy.core.models.build import (
    BuildConcept,
    BuildGrain,
    BuildUnionSelectLineage,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import (
    History,
    MergeNode,
    StrategyNode,
    UnionNode,
)
from trilogy.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_UNION_SELECT_NODE]"


def gen_union_select_node(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Build a relational `union(...)` TVF: a column-positional row stack.

    Sources each arm independently (like the multiselect generator) but stacks
    them with a `UnionNode` (SQL UNION ALL) instead of FULL-joining on align
    keys. Each arm projects its i-th column onto the shared union output so the
    stacked SELECTs line up by column."""
    from trilogy.core.query_processor import get_query_node

    if not isinstance(concept.lineage, BuildUnionSelectLineage):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate union node for {concept}"
        )
        return None
    lineage: BuildUnionSelectLineage = concept.lineage

    # Canonical output order = align-item order; every arm exposes exactly these.
    ordered_outputs = [
        environment.concepts[item.aligned_concept] for item in lineage.align.items
    ]

    arm_nodes: List[StrategyNode] = []
    for select in lineage.selects:
        snode: StrategyNode = get_query_node(history.base_environment, select, history)
        if not snode:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} could not source union arm "
                f"{select.output_components}"
            )
            return None
        # Expose each arm's i-th column under the shared union output, then hide
        # the per-arm internal columns so the rendered SELECT emits only the
        # union outputs (k, v) — sourced from the hidden columns via find_source.
        arm_own = [c.address for c in snode.output_concepts]
        for x in list(snode.output_concepts):
            merge_name = lineage.get_merge_concept(x)
            if merge_name:
                snode.output_concepts.append(environment.concepts[merge_name])
        snode.hidden_concepts = set(arm_own)
        snode.rebuild_cache()
        arm_nodes.append(snode)

    union_node = UnionNode(
        input_concepts=list(ordered_outputs),
        output_concepts=list(ordered_outputs),
        environment=environment,
        parents=arm_nodes,
        preexisting_conditions=conditions.conditional if conditions else None,
    )
    union_node.grain = BuildGrain.from_concepts(
        union_node.output_concepts, environment=environment
    )
    union_node.rebuild_cache()

    # Wrap in a pass-through projection so a consumer (e.g. gen_rowset_node) can
    # re-project the outputs onto rowset handles without mutating the UnionNode,
    # whose add_output_concepts propagates to the arm parents (which cannot
    # source the handle addresses).
    node: StrategyNode = MergeNode(
        input_concepts=list(ordered_outputs),
        output_concepts=list(ordered_outputs),
        environment=environment,
        depth=depth,
        parents=[union_node],
    )
    node.grain = BuildGrain.from_concepts(node.output_concepts, environment=environment)
    node.rebuild_cache()

    if not local_optional or all(
        x.address in [y.address for y in node.output_concepts] for x in local_optional
    ):
        return node

    enrich_node = source_concepts(
        mandatory_list=list(ordered_outputs) + local_optional,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        conditions=conditions,
    )
    if not enrich_node:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} could not enrich union node; "
            "returning base"
        )
        return node

    return MergeNode(
        input_concepts=enrich_node.output_concepts + node.output_concepts,
        output_concepts=node.output_concepts + local_optional,
        environment=environment,
        depth=depth,
        parents=[node, enrich_node],
    )
