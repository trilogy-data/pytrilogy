from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import GroupNode, StrategyNode

from .common import parent_outputs_needed


def gen_group_to(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """`group_to(...)` always materializes the grain change — force a GROUP
    BY even when the parent is already at the target grain."""
    return GroupNode(
        output_concepts=outputs,
        input_concepts=parent_outputs_needed(outputs, parents, conditions),
        environment=environment,
        parents=parents,
        conditions=conditions.conditional if conditions else None,
        preexisting_conditions=(
            preexisting_conditions.conditional if preexisting_conditions else None
        ),
        force_group=True,
    )
