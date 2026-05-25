from typing import List

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import (
    ConstantNode,
    MergeNode,
    SelectNode,
    StrategyNode,
)

from .common import parent_outputs_needed


def gen_basic(
    outputs: List[BuildConcept],
    parents: List[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Projection of derived basic expressions over already-built parents.

    Zero parents → ConstantNode. One parent → SelectNode (just projection).
    Multiple parents → MergeNode, which auto-joins on shared output concepts
    (typically the common grain). A SelectNode here would render with no
    join and emit `INVALID_REFERENCE_BUG_<...>` for the unjoined parent."""
    if not parents:
        return ConstantNode(
            input_concepts=[],
            output_concepts=outputs,
            environment=environment,
        )
    inputs = parent_outputs_needed(outputs, parents, conditions)
    if len(parents) == 1:
        return SelectNode(
            input_concepts=inputs,
            output_concepts=outputs,
            environment=environment,
            parents=parents,
            conditions=conditions.conditional if conditions else None,
        )
    return MergeNode(
        input_concepts=inputs,
        output_concepts=outputs,
        environment=environment,
        parents=parents,
        conditions=conditions.conditional if conditions else None,
    )
