"""ROOT generator: pick (or join) the datasources that produce the requested
concepts. Delegates to v3 because datasource search/join planning is the one
piece the v4 prototype hasn't replaced yet — the per-node `add_output_concept`
and datasource scoring lives there."""

from typing import List

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing import concept_strategies_v3 as v3
from trilogy.core.processing.nodes import History, StrategyNode


def gen_root(
    outputs: List[BuildConcept],
    parents: List[StrategyNode],  # always empty for ROOT; kept for uniform sig
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    *,
    history: History,
    g,
) -> StrategyNode | None:
    """Try the History-cached single-datasource select first; if no single
    datasource covers `outputs`, fall back to v3's cross-datasource search."""
    node = history.gen_select_node(
        outputs, environment, g, depth=0, conditions=conditions
    )
    if node is not None:
        return node
    return v3.search_concepts(
        mandatory_list=outputs,
        history=history,
        environment=environment,
        depth=1,
        g=g,
        conditions=conditions,
    )
