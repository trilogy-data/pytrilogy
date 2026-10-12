from dataclasses import dataclass, field
from itertools import islice

from trilogy.core.models.build import (
    get_canonical_pseudonyms,
)
from trilogy.core.models.environment import Environment

from .base_node import NodeJoin, StrategyNode
from .filter_node import FilterNode
from .group_node import GroupNode
from .merge_node import MergeNode
from .recursive_node import RecursiveNode
from .select_node_v2 import ConstantNode, RowsetNode, SelectNode
from .subselect_node import SubselectNode
from .union_node import UnionNode
from .unnest_node import UnnestNode
from .window_node import WindowNode


@dataclass
class BuildCaches:
    """Factory build caches, threaded through every get_query_node call in a
    resolution. All are keyed on grain/lineage/address identity, so reuse
    is correct across sub-selects sharing the same base environment."""

    canonical_build_cache: dict = field(default_factory=dict)
    grain_build_cache: dict = field(default_factory=dict)
    datasource_build_cache: dict = field(default_factory=dict)
    # Context-free environment materializations (EnvBaseline), keyed by the
    # scoped-join tuple they were built under; nested arms replay only their
    # overlay's delta against these. Scoped to this resolution like every
    # other cache here.
    env_baselines: dict = field(default_factory=dict)
    pseudonym_map: dict | None = None
    # `len(environment.concepts.data)` the pseudonym map was last folded up
    # to; concepts registered since are folded in by `sync_pseudonym_map`.
    pseudonym_concept_count: int = 0
    # Build-scoped joins for this resolution, as
    # (source_address, target_address, JoinType). Applied during the build and
    # shared so every sub-select (rowsets, multiselect arms) inherits them.
    scoped_joins: list = field(default_factory=list)

    def sync_pseudonym_map(self, environment: Environment) -> dict:
        """The canonical pseudonym map for `environment`, kept current across
        concept additions without a rebuild: a concept added since the last
        sync contributes only its own entry (`get_canonical_pseudonyms` roots
        a concept's pseudonyms under its own address, and registering an
        alias never touches the source concept), so folding the tail of the
        concept dict in is the whole update. Deletions and rebinds evict the
        bundle before this runs."""
        data = environment.concepts.data
        if self.pseudonym_map is None:
            self.pseudonym_map = get_canonical_pseudonyms(environment)
        elif len(data) != self.pseudonym_concept_count:
            for key, concept in islice(
                data.items(), self.pseudonym_concept_count, None
            ):
                entry = self.pseudonym_map.setdefault(concept.address, set())
                entry.add(key)
                entry.update(concept.pseudonyms)
        self.pseudonym_concept_count = len(data)
        return self.pseudonym_map


__all__ = [
    "BuildCaches",
    "ConstantNode",
    "FilterNode",
    "GroupNode",
    "MergeNode",
    "NodeJoin",
    "RecursiveNode",
    "RowsetNode",
    "SelectNode",
    "StrategyNode",
    "SubselectNode",
    "UnionNode",
    "UnnestNode",
    "WindowNode",
]
