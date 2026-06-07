from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import (
    ConstantNode,
    MergeNode,
    SelectNode,
    StrategyNode,
)

from .common import outputs_with_parent_grain_keys, parent_outputs_needed


def gen_basic(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Projection of derived basic expressions over already-built parents.

    Zero parents → ConstantNode. One parent → SelectNode (just projection).
    Multiple parents → MergeNode, which auto-joins on shared output concepts
    (typically the common grain). A SelectNode here would render with no
    join and emit `INVALID_REFERENCE_BUG_<...>` for the unjoined parent."""
    pre = preexisting_conditions.conditional if preexisting_conditions else None
    if not parents:
        return ConstantNode(
            input_concepts=[],
            output_concepts=outputs,
            environment=environment,
            preexisting_conditions=pre,
        )
    full_outputs = outputs_with_parent_grain_keys(outputs, parents)
    inputs = parent_outputs_needed(full_outputs, parents, conditions)
    if len(parents) == 1:
        return SelectNode(
            input_concepts=inputs,
            output_concepts=full_outputs,
            environment=environment,
            parents=parents,
            conditions=conditions.conditional if conditions else None,
            preexisting_conditions=pre,
        )
    return MergeNode(
        input_concepts=inputs,
        output_concepts=full_outputs,
        environment=environment,
        parents=parents,
        conditions=conditions.conditional if conditions else None,
        preexisting_conditions=pre,
    )
