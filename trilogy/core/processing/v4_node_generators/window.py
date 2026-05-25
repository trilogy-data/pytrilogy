from typing import List

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import StrategyNode, WindowNode

from .common import parent_outputs_needed


def gen_window(
    outputs: List[BuildConcept],
    parents: List[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Window functions (LEAD/LAG/RANK/...) over an already-built parent.
    Conditions are inherited as `preexisting_conditions` from the parent;
    WindowNode does not take a `conditions` arg."""
    return WindowNode(
        input_concepts=parent_outputs_needed(outputs, parents, conditions),
        output_concepts=outputs,
        environment=environment,
        parents=parents,
        preexisting_conditions=conditions.conditional if conditions else None,
    )
