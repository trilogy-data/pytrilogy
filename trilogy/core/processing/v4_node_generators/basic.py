from typing import List

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import ConstantNode, SelectNode, StrategyNode

from .common import parent_outputs_needed


def gen_basic(
    outputs: List[BuildConcept],
    parents: List[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Projection of derived basic expressions over already-built parents.
    If no parents (no upstream concepts), the basics must be constant."""
    if not parents:
        return ConstantNode(
            input_concepts=[],
            output_concepts=outputs,
            environment=environment,
        )
    return SelectNode(
        input_concepts=parent_outputs_needed(outputs, parents, conditions),
        output_concepts=outputs,
        environment=environment,
        parents=parents,
        conditions=conditions.conditional if conditions else None,
    )
