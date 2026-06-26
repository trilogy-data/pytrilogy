from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import (
    ConstantNode,
    FilterNode,
    GroupNode,
    StrategyNode,
)


def gen_constant(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """A constant has no inputs by definition, so parents are normally ignored.

    The exception is an EXISTS-style gate: a WHERE whose row args are non-constant
    columns this group doesn't produce (e.g. `where sum(x) by k < ... select <const>`
    — an aggregate gate disconnected from a constant output). A bare ConstantNode
    can't source those columns, so the predicate renders as an unresolved reference.
    When a parent supplies the gate's inputs, filter that parent by the gate and
    group the constants to their own (empty) grain, so the result is 0 or 1 rows —
    the boolean "did any row satisfy the gate". Mirrors v3, which sources the
    constant SELECT from the gate's scan and applies the predicate as a HAVING."""
    if conditions and parents:
        parent_outputs = [o for p in parents for o in p.output_concepts]
        parent_addresses = {c.address for c in parent_outputs}
        gate_inputs = [
            r
            for r in conditions.conditional.row_arguments
            if r.address in parent_addresses
        ]
        if gate_inputs:
            seen: set[str] = set()
            passthrough: list[BuildConcept] = []
            for o in parent_outputs:
                if o.address not in seen:
                    seen.add(o.address)
                    passthrough.append(o)
            gate = FilterNode(
                input_concepts=passthrough,
                output_concepts=passthrough,
                environment=environment,
                parents=parents,
                conditions=conditions.conditional,
                preexisting_conditions=(
                    preexisting_conditions.conditional
                    if preexisting_conditions
                    else None
                ),
            )
            return GroupNode(
                output_concepts=list(outputs),
                input_concepts=[],
                environment=environment,
                parents=[gate],
                force_group=True,
            )
    return ConstantNode(
        input_concepts=[],
        output_concepts=outputs,
        environment=environment,
        conditions=conditions.conditional if conditions else None,
        preexisting_conditions=(
            preexisting_conditions.conditional if preexisting_conditions else None
        ),
    )
