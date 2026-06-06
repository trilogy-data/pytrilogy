"""ROOT generator: pick or join datasources for requested concepts."""

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import decompose_condition
from trilogy.core.processing.nodes import History, StrategyNode
from trilogy.core.processing.v4_helper.condition_injection import (
    ConditionSources,
    inject_condition_at_node,
    split_existence_atoms,
)
from trilogy.core.processing.v4_helper.source_planning import SourceRequest, plan_source
from trilogy.core.processing.v4_helper.source_policy import (
    STRICT_SOURCE_POLICY,
    SourcePolicy,
)


def gen_root(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    *,
    history: History,
    g,
    source_policy: SourcePolicy = STRICT_SOURCE_POLICY,
) -> StrategyNode | None:
    """Source ROOT concepts through the v4 source planner.

    Existence-bearing atoms (`x IN <subselect>`) are applied in a wrapper so
    the existence feeder remains a side-channel parent rather than being pulled
    into the row stream.
    """
    row_conditions, existence_conditions = split_existence_atoms(conditions)

    inner_outputs: list[BuildConcept] = list(outputs)
    if existence_conditions is not None:
        seen = {c.address for c in inner_outputs}
        for atom in decompose_condition(existence_conditions.conditional):
            for arg in atom.row_arguments:
                if arg.address not in seen:
                    inner_outputs.append(arg)
                    seen.add(arg.address)

    node = plan_source(
        SourceRequest(
            outputs=inner_outputs,
            environment=environment,
            graph=g,
            history=history,
            conditions=row_conditions,
            source_policy=source_policy,
        )
    )
    if node is None or existence_conditions is None:
        return node

    return inject_condition_at_node(
        node,
        existence_conditions,
        list(outputs),
        environment=environment,
        sources=ConditionSources(),
        input_concepts=list(node.output_concepts),
        combine_existing=False,
    )
