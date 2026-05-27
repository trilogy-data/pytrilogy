from dataclasses import dataclass, field

import networkx as nx

from trilogy.core.processing.nodes import StrategyNode


@dataclass
class BuildInfo:
    """Result bundle for discovery: the raw concept graph, the grouped graph,
    and the materialized StrategyNode produced by walking the group graph."""

    concept_graph: nx.DiGraph = field(default_factory=nx.DiGraph)
    group_graph: nx.DiGraph = field(default_factory=nx.DiGraph)
    strategy_node: StrategyNode | None = None

    def copy(self) -> "BuildInfo":
        return BuildInfo(
            concept_graph=self.concept_graph.copy(),
            group_graph=self.group_graph.copy(),
            strategy_node=self.strategy_node.copy() if self.strategy_node else None,
        )


@dataclass
class GroupBucket:
    """In-flight working state for one group while we're assembling
    `group_graph`. Once all groups are populated, fields are unpacked onto the
    final nx node as attributes.

    ``label`` is the sub-graph this bucket belongs to. Empty string is
    the outer query; non-empty (e.g. ``"q5_results"``) is a rowset's
    inner sub-graph. The grouping pipeline partitions per-label so
    inner and outer BASICs at compatible grain don't merge into one
    bucket and form a group-level cycle through the rowset boundary."""

    depth_label: str
    derivation: str
    grain_components: frozenset[str]
    primary_members: list[str] = field(default_factory=list)
    secondary_members: list[str] = field(default_factory=list)
    member_depths: dict[str, str] = field(default_factory=dict)
    label: str = ""
