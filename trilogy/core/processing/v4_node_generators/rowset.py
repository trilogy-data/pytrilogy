from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import History, StrategyNode
from trilogy.core.processing.v4_helper.source_policy import (
    STRICT_SOURCE_POLICY,
    SourcePolicy,
)


def gen_rowset(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
    *,
    history: History,
    g: ReferenceGraph,
    source_policy: SourcePolicy = STRICT_SOURCE_POLICY,
) -> StrategyNode | None:
    """Boundary node for a rowset reference. The rowset's inner select is a
    self-contained sub-query, so — like ROOT — this generator ignores the
    group graph's pre-built parents and plans the inner recursively through
    v4 (`resolve_rowset`), mirroring v3's `gen_rowset_node`/`get_query_node`.
    Enrichment joins back to the outer query are handled by the outer group
    graph / FINAL merge, not here."""
    from trilogy.core.processing.concept_strategies_v4 import V4History, resolve_rowset

    if not outputs or not isinstance(history, V4History):
        return None
    return resolve_rowset(
        outputs, environment, depth=0, g=g, history=history, conditions=conditions
    )
