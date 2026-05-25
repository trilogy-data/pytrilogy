from typing import List

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import ConstantNode, StrategyNode


def gen_constant(
    outputs: List[BuildConcept],
    parents: List[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """A constant has no inputs by definition. Parents (if any) are ignored."""
    return ConstantNode(
        input_concepts=[],
        output_concepts=outputs,
        environment=environment,
        conditions=conditions.conditional if conditions else None,
    )
