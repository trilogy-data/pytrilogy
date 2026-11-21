"""Rollup TVF generator: a relational `rollup`/`cube`/`grouping sets`.

Sibling of `union_select` with one arm: the arm select computes every grouping
set, and its columns are re-exposed as the relation's own outputs. Intercepted
in `concept_strategies_v4._search_concepts`, not reachable through dispatch.
"""

from trilogy.core.enums import Purpose
from trilogy.core.models.build import (
    BuildConcept,
    BuildGrain,
    BuildRollupSelectLineage,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import SelectNode, StrategyNode
from trilogy.core.processing.v4_helper.history import V4History

from .nested_select import plan_align_arms


def gen_rollup_select(
    rollup_concept: BuildConcept,
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    history: V4History,
) -> StrategyNode | None:
    lineage = rollup_concept.lineage
    assert isinstance(lineage, BuildRollupSelectLineage)
    plans = plan_align_arms(lineage, history, depth, "rollup arm")
    if plans is None:
        return None
    arm_node = plans[0].node
    # the arm's columns alone: a grouping key it does not project stays below
    arm_cols = [
        out
        for out in arm_node.output_concepts
        if out.address not in arm_node.hidden_concepts
        and lineage.get_merge_concept_resolved(out) is not None
    ]
    produced = {lineage.get_merge_concept_resolved(out) for out in arm_cols}
    demanded = {c.address for c in mandatory_list}
    exposed = [
        environment.concepts[item.aligned_concept]
        for item in lineage.align.items
        if item.aligned_concept in produced and item.aligned_concept in demanded
    ]
    outputs = [
        environment.concepts[item.aligned_concept] for item in lineage.align.items
    ]
    # a rename-only SELECT, as for a union arm: the arm keeps its own grouping.
    # Its rows are the relation's, one per identity, demanded or not.
    node = SelectNode(
        input_concepts=arm_cols,
        output_concepts=arm_cols + exposed,
        environment=environment,
        depth=depth,
        parents=[arm_node],
        hidden_concepts={c.address for c in arm_cols},
        grain=BuildGrain(
            components={c.address for c in outputs if c.purpose == Purpose.KEY}
        ),
    )
    node.rebuild_cache()
    return node
