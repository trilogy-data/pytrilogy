from trilogy.constants import DEFAULT_NAMESPACE, RECURSIVE_GATING_CONCEPT
from trilogy.core.enums import ComparisonOperator, Derivation, Purpose
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildFunction,
    BuildGrain,
    BuildWhereClause,
    DataType,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import merge_conditions_and_dedup
from trilogy.core.processing.nodes import MergeNode, RecursiveNode, StrategyNode

GATING_CONCEPT = BuildConcept(
    name=RECURSIVE_GATING_CONCEPT,
    canonical_name=RECURSIVE_GATING_CONCEPT,
    namespace=DEFAULT_NAMESPACE,
    grain=BuildGrain(),
    build_is_aggregate=False,
    datatype=DataType.BOOL,
    purpose=Purpose.KEY,
    derivation=Derivation.BASIC,
)


def gen_recursive(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Recursive CTE (`recurse_edge(seed, next)`) over an already-built parent.

    Emits a `RecursiveNode` whose UNION feeds the recursion plus a gating
    concept, then a wrapper that keeps only the gated rows. The recursion's
    seed/next columns come from the pre-built parent the group graph supplied
    rather than a fresh search."""
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
    gate = BuildComparison(
        left=GATING_CONCEPT, right=True, operator=ComparisonOperator.IS
    )
    walk_outputs = [recursive, *arguments]
    # A WHERE hosted here filters the FINISHED walk, never its edge set: the
    # parent stays unfiltered so every walk reaches its terminal row.
    walk_conditions = conditions.conditional if conditions else None
    carried = {c.address for c in walk_outputs}
    joined_back = walk_conditions is not None and any(
        c.address not in carried for c in walk_conditions.row_arguments
    )
    # The recursion materializes a UNION; keep only the gated rows. The gate is
    # internal to the recursion and not an environment concept, so it stops here.
    walk = StrategyNode(
        input_concepts=node_outputs,
        output_concepts=walk_outputs,
        environment=environment,
        parents=[base],
        # A subset merge (`merge a into ~b`) declares the walk's roots only part
        # of b's domain, as a partial column binding would.
        partial_concepts=(
            [recursive]
            if recursive.address in environment.domain_graph.subset_sources()
            else []
        ),
        conditions=(
            merge_conditions_and_dedup(walk_conditions, gate)
            if walk_conditions is not None and not joined_back
            else gate
        ),
    )
    if not joined_back:
        return walk
    # The atom reads columns of the seed row the walk does not carry: join the
    # walk back to its parent on the seed row and filter there.
    return MergeNode(
        input_concepts=walk_outputs,
        output_concepts=walk_outputs,
        environment=environment,
        parents=[walk, *parents],
        partial_concepts=walk.partial_concepts,
        conditions=walk_conditions,
    )
