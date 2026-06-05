from trilogy.core.enums import Derivation
from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import StrategyNode, UnnestNode

from .common import parent_outputs_needed, passthrough_if_materialized


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
    passthrough = passthrough_if_materialized(
        outputs, parents, environment, conditions, preexisting_conditions
    )
    if passthrough is not None:
        return passthrough
    parent_addrs = {c.address for parent in parents for c in parent.output_concepts}
    outputs = [
        c
        for c in outputs
        if c.derivation == Derivation.UNNEST or c.address in parent_addrs
    ]
    inputs = parent_outputs_needed(outputs, parents, conditions)
    unnest_concepts = [c for c in outputs if c.derivation == Derivation.UNNEST]
    return UnnestNode(
        unnest_concepts=unnest_concepts,
        input_concepts=inputs,
        output_concepts=outputs,
        environment=environment,
        parents=parents,
    )
