from typing import List

from trilogy.core.enums import ComparisonOperator, Derivation
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildFunction,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.recursive_node import GATING_CONCEPT
from trilogy.core.processing.nodes import RecursiveNode, StrategyNode


def gen_recursive(
    outputs: List[BuildConcept],
    parents: List[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Recursive CTE (`recurse_edge(seed, next)`) over an already-built parent.

    Emits a `RecursiveNode` whose UNION feeds the recursion plus a gating
    concept, then a wrapper that keeps only the gated rows. Mirrors v3
    `gen_recursive_node`, but the recursion's seed/next columns come from the
    pre-built parent the group graph supplied rather than a fresh search."""
    recursive = next((o for o in outputs if o.derivation == Derivation.RECURSIVE), None)
    if recursive is None or not isinstance(recursive.lineage, BuildFunction):
        return None
    arguments = recursive.lineage.concept_arguments
    node_outputs = [recursive, *arguments, GATING_CONCEPT]
    base = RecursiveNode(
        input_concepts=list(arguments),
        output_concepts=node_outputs,
        environment=environment,
        parents=list(parents) if (arguments or parents) else [],
    )
    # The recursion materializes a UNION; keep only the gated rows.
    return StrategyNode(
        input_concepts=node_outputs,
        output_concepts=node_outputs,
        environment=environment,
        parents=[base],
        conditions=BuildComparison(
            left=GATING_CONCEPT, right=True, operator=ComparisonOperator.IS
        ),
    )
