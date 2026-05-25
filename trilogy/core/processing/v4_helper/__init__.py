"""Helpers behind `concept_strategies_v4.search_concepts`.

Each stage of discovery lives in its own module:

    constants         — derivation classifications, FINAL sentinel
    models            — BuildInfo result bundle + internal GroupBucket
    concept_graph     — stage 1: build the concept-lineage DAG
    group_graph       — stage 2: collapse concepts into co-projectable groups
    strategy_builder  — stage 3: walk groups and emit a StrategyNode tree
    factory_dispatch  — Derivation → gen_X_node registry used by stage 3
"""

from .concept_graph import build_concept_graph, classify_depth
from .constants import (
    FINAL_NODE_ID,
    GROUPING_DERIVATIONS,
    ROW_SHAPE_BARRIER_DERIVATIONS,
)
from .factory_dispatch import build_node_for_group
from .group_graph import build_group_graph
from .models import BuildInfo, GroupBucket
from .strategy_builder import build_strategy_node, combine_clauses

__all__ = [
    "BuildInfo",
    "FINAL_NODE_ID",
    "GROUPING_DERIVATIONS",
    "GroupBucket",
    "ROW_SHAPE_BARRIER_DERIVATIONS",
    "build_concept_graph",
    "build_group_graph",
    "build_node_for_group",
    "build_strategy_node",
    "classify_depth",
    "combine_clauses",
]
