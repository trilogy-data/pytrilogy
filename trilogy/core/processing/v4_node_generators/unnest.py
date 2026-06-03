from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import StrategyNode, UnnestNode

from .common import parent_outputs_needed


def gen_unnest(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Unnest an array-valued parent column into rows. The unnested concepts
    are passed as `unnest_concepts`; everything in `outputs` that isn't
    unnested is a pass-through projection."""
    inputs = parent_outputs_needed(outputs, parents, conditions)
    unnest_concepts = [c for c in outputs if c.lineage is not None]
    return UnnestNode(
        unnest_concepts=unnest_concepts,
        input_concepts=inputs,
        output_concepts=outputs,
        environment=environment,
        parents=parents,
    )
