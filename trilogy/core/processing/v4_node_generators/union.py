from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import StrategyNode, UnionNode

from .common import parent_outputs_needed


def gen_union(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Stack parent outputs into a UNION ALL. Each parent contributes one
    arm; the union node is responsible for column-aligning them. UnionNode
    has no `conditions` arg, so new-at-this-group atoms collapse into
    `preexisting_conditions` alongside the inherited ones."""
    new = conditions.conditional if conditions else None
    pre = preexisting_conditions.conditional if preexisting_conditions else None
    combined = new if pre is None else (pre if new is None else (new + pre))
    return UnionNode(
        input_concepts=parent_outputs_needed(outputs, parents, conditions),
        output_concepts=outputs,
        environment=environment,
        parents=parents,
        preexisting_conditions=combined,
    )
