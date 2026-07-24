"""Helpers behind `concept_strategies_v4.search_concepts`.

Each stage of discovery lives in its own module:

    constants         — derivation classifications, FINAL sentinel
    models            — BuildInfo result bundle + internal GroupBucket
    concept_graph     — stage 1: build the concept-lineage DAG
    group_graph       — stage 2: collapse concepts into co-projectable groups
    strategy_builder  — stage 3: walk groups and emit a StrategyNode tree
"""

from .concept_graph import build_concept_graph, classify_depth
from .constants import (
    FINAL_NODE_ID,
    GROUPING_DERIVATIONS,
    ROW_SHAPE_BARRIER_DERIVATIONS,
)
from .group_graph import build_group_graph
from .models import (
    BuildInfo,
    ConceptAttrs,
    FinalAssemblyContract,
    FinalContributorContract,
    GroupAttrs,
    GroupBucket,
    GroupInputContract,
    InputChannel,
)
from .strategy_builder import build_strategy_node

__all__ = [
    "FINAL_NODE_ID",
    "GROUPING_DERIVATIONS",
    "ROW_SHAPE_BARRIER_DERIVATIONS",
    "BuildInfo",
    "ConceptAttrs",
    "FinalAssemblyContract",
    "FinalContributorContract",
    "GroupAttrs",
    "GroupBucket",
    "GroupInputContract",
    "InputChannel",
    "build_concept_graph",
    "build_group_graph",
    "build_strategy_node",
    "classify_depth",
]
