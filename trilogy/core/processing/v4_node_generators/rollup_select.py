"""Rollup TVF generator: a relational `rollup`/`cube`/`grouping sets`.

Sibling of `union_select` with one arm: the arm select computes every grouping
set, and its columns are re-exposed as the relation's own outputs. Intercepted
in `concept_strategies_v4._search_concepts`, not reachable through dispatch.

The arm is planned as a standard select. Its one ROLLUP/CUBE/GROUPING SETS
pass is the select's, set here on the arm's build environment: the planner
co-sources every aggregate at the pass's keys and the group computing them
runs it.
"""

from trilogy.core.enums import Purpose
from trilogy.core.models.author import (
    AggregateWrapper,
    FunctionCallWrapper,
    SelectLineage,
)
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildGrain,
    BuildGroupingSets,
    BuildRollupSelectLineage,
    BuildSelectLineage,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.environment import Environment
from trilogy.core.processing.nodes import SelectNode, StrategyNode
from trilogy.core.processing.v4_helper.history import V4History

from .nested_select import build_nested_select, plan_align_arms


def _bare(address: str, arm: SelectLineage, environment: Environment) -> bool:
    authored = arm.local_concepts.get(address) or environment.concepts.get(address)
    lineage = authored.lineage if authored is not None else None
    content = lineage.content if isinstance(lineage, FunctionCallWrapper) else lineage
    return isinstance(content, AggregateWrapper) and not content.by


def arm_grouping_sets(
    arm: SelectLineage,
    built: BuildSelectLineage,
    environment: Environment,
    build_environment: BuildEnvironment,
) -> BuildGroupingSets:
    """The arm's one pass, in its built concepts. Its keys are the `by` the
    factory gave a bare aggregate (the spec's keys, or the select's grain
    where the spec infers them); parsing refuses an arm with none."""
    spec = arm.grouping
    assert spec is not None
    by = next(
        c.lineage.by
        for c in built.output_components
        if isinstance(c.lineage, BuildAggregateWrapper)
        and _bare(c.address, arm, environment)
    )

    def lookup(address: str) -> BuildConcept:
        return built.local_concepts.get(address) or build_environment.concepts[address]

    return BuildGroupingSets(
        mode=spec.mode,
        by=list(by),
        sets=[[lookup(ref.address) for ref in s] for s in spec.grouping_sets],
    )


def gen_rollup_select(
    rollup_concept: BuildConcept,
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    history: V4History,
) -> StrategyNode | None:
    lineage = rollup_concept.lineage
    assert isinstance(lineage, BuildRollupSelectLineage)
    arm = lineage.selects[0]
    built, build_env, _, _ = build_nested_select(arm, history)
    assert isinstance(built, BuildSelectLineage)
    build_env.grouping = arm_grouping_sets(
        arm, built, history.base_environment, build_env
    )
    plan = plan_align_arms(lineage, history, depth, "rollup arm")
    if plan is None:
        return None
    arm_node = plan[0].node
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
