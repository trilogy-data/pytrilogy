"""TVF_UNION generator: a relational `union(...)`/`except(...)`/`intersect(...)`.

Sibling of `multiselect`: same per-arm recursion, but the arms are stacked
column-positionally rather than joined on an alignment key. Intercepted in
`concept_strategies_v4._search_concepts`, not reachable through dispatch.
"""

from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import (
    BuildConcept,
    BuildUnionSelectLineage,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import SelectNode, StrategyNode, UnionNode
from trilogy.core.processing.v4_helper.history import V4History

from .nested_select import plan_align_arms


def gen_union_select(
    union_concept: BuildConcept,
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    history: V4History,
    conditions: list[BuildWhereClause],
) -> StrategyNode | None:
    """Plan a relational `union(...)`/`except(...)`/`intersect(...)` TVF: a
    column-positional row stack.

    Each arm is planned independently (same arm recursion as a multiselect),
    then each arm projects its i-th column onto the shared output concept and
    the arms are combined with a `UnionNode` carrying the lineage's set
    operator (UNION ALL / EXCEPT / INTERSECT), not joined. Arm order is
    preserved; for EXCEPT it is semantic (left-fold)."""
    lineage = union_concept.lineage
    assert isinstance(lineage, BuildUnionSelectLineage)

    # Canonical output order = align-item order; every arm must expose exactly
    # these concepts, in this order, so the UNION columns line up.
    ordered_outputs = [
        environment.concepts[item.aligned_concept] for item in lineage.align.items
    ]

    plans = plan_align_arms(lineage, history, depth, "union arm")
    if plans is None:
        return None

    arm_nodes: list[StrategyNode] = []
    for plan in plans:
        arm_node = plan.node
        # Expose each arm's i-th column under the shared union output, in the
        # canonical align order: a UNION ALL stacks positionally, so every
        # arm's projection must list the outputs in the same order regardless
        # of its own column order. A scoped join inside the arm can emit the
        # authored key under its partner's address, so resolve through the
        # partner/pseudonym (`find_source` recovers the physical column at
        # render). The per-arm internal columns are hidden.
        arm_cols = list(arm_node.output_concepts)
        produced = {
            lineage.get_merge_concept_resolved(out) for out in arm_node.output_concepts
        }
        exposed = [m for m in ordered_outputs if m.address in produced]
        # Re-expose via a rename-only SELECT over the arm rather than mutating
        # the arm node's outputs: a GROUP arm would otherwise re-derive its
        # grain from the union outputs and group by its own aggregate, while a
        # SELECT node never emits a GROUP BY. Dropping the extra projection when
        # it is a true passthrough is CollapseSingleParent's job, not the
        # planner's.
        rename = SelectNode(
            input_concepts=arm_cols,
            output_concepts=arm_cols + exposed,
            environment=environment,
            depth=depth,
            parents=[arm_node],
            hidden_concepts={c.address for c in arm_cols},
        )
        rename.rebuild_cache()
        arm_nodes.append(rename)

    node: StrategyNode = UnionNode(
        input_concepts=list(ordered_outputs),
        output_concepts=list(ordered_outputs),
        environment=environment,
        depth=depth,
        parents=arm_nodes,
        set_operator=lineage.operator,
    )
    node.set_output_concepts(list(mandatory_list), rebuild=False)
    node.rebuild_cache()
    return node
