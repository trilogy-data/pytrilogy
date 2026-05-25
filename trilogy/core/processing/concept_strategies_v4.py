"""v4 discovery: a three-stage prototype planner.

    Stage 1 (concept_graph): walk lineage from the mandatory list back to
    roots, producing a per-concept DAG with depth labels (d0/d1/d*).

    Stage 2 (group_graph): collapse compatible concepts into shared-scan
    groups, inject filter clauses at the furthest-upstream group that can
    serve them, and append a FINAL sink.

    Stage 3 (strategy_builder): walk groups topologically, dispatching each
    one to its derivation factory and stitching parent groups through a
    source-concepts callback.

The stage implementations live in `v4_helper/`; this file is just the public
API and the History cache wiring.
"""

from typing import List

from trilogy.constants import logger
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.discovery_utility import (
    LOGGER_PREFIX,
    depth_to_prefix,
)
from trilogy.core.processing.nodes import History
from trilogy.core.processing.v4_helper import (
    FINAL_NODE_ID,
    ROW_SHAPE_BARRIER_DERIVATIONS,
    BuildInfo,
    build_concept_graph,
    build_group_graph,
    build_strategy_node,
)

__all__ = [
    "BuildInfo",
    "FINAL_NODE_ID",
    "History",
    "ROW_SHAPE_BARRIER_DERIVATIONS",
    "search_concepts",
]


def _search_concepts(
    mandatory_list: List[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    history: History,
    conditions: list[BuildWhereClause],
    accept_partial: bool = False,
) -> BuildInfo:
    concept_graph = build_concept_graph(mandatory_list, environment, conditions)
    group_graph = build_group_graph(concept_graph, conditions)
    strategy_node = build_strategy_node(
        group_graph, mandatory_list, environment, g, history
    )
    return BuildInfo(
        concept_graph=concept_graph,
        group_graph=group_graph,
        strategy_node=strategy_node,
    )


def search_concepts(
    mandatory_list: List[BuildConcept],
    history: History,
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    accept_partial: bool = False,
    conditions: list[BuildWhereClause] | None = None,
) -> BuildInfo:
    """Run the v4 planner against `mandatory_list` under `conditions`. Cached
    per `(mandatory_list, accept_partial, conditions)` via `history`."""
    conditions = conditions or []
    hist = history.get_history(
        search=mandatory_list, accept_partial=accept_partial, conditions=conditions
    )
    if hist is not False:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Returning search node from "
            f"history ({'exists' if hist is not None else 'does not exist'}) for "
            f"{[c.address for c in mandatory_list]} with accept_partial {accept_partial}"
        )
        assert not isinstance(hist, bool)
        return hist

    result = _search_concepts(
        mandatory_list,
        environment,
        depth=depth,
        g=g,
        accept_partial=accept_partial,
        history=history,
        conditions=conditions,
    )
    # a node may be mutated after being cached; always store a copy
    history.search_to_history(
        mandatory_list,
        accept_partial,
        result.copy() if result else None,
        conditions=conditions,
    )
    return result
