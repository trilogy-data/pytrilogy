"""Helpers behind `concept_strategies_v4.search_concepts`.

Each stage of discovery lives in its own module:

    constants         : derivation classifications, FINAL sentinel
    history           : V4History, the per-request cache bundle
    models            : BuildInfo result bundle + internal GroupBucket
    concept_graph     : stage 1: build the concept-lineage DAG
    group_graph       : stage 2: collapse concepts into co-projectable groups
    strategy_builder  : stage 3: walk groups and emit a StrategyNode tree

Source selection for one group is its own stack, layered so that only the
bottom of it reads build models:

    network_model       : the vocabulary: candidates, covers, obligations, cost
    network_build       : stage A: label the network (the ONLY build-model reader)
    network_coalescing  : stage A: presence probes and union-join axis families
    network_topology    : how a cover hangs together; shared by the two stages
    network_obligations : what a partial cover still owes
    network_search      : stages B/C: enumerate, reduce, cost, choose
    source_planning     : turns the chosen sources into StrategyNodes
"""

from .concept_graph import build_concept_graph, classify_depth
from .constants import (
    FINAL_NODE_ID,
    GROUPING_DERIVATIONS,
    ROW_SHAPE_BARRIER_DERIVATIONS,
)
from .group_graph import build_group_graph
from .history import V4History
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
    "V4History",
    "build_concept_graph",
    "build_group_graph",
    "build_strategy_node",
    "classify_depth",
]
