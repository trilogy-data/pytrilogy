from contextlib import contextmanager
from dataclasses import dataclass, field

from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.author import Concept
from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.environment import Environment

from .base_node import NodeJoin, StrategyNode, WhereSafetyNode
from .filter_node import FilterNode
from .group_node import GroupNode
from .merge_node import MergeNode, MultiSelectMergeNode
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

    build_cache: dict = field(default_factory=dict)
    canonical_build_cache: dict = field(default_factory=dict)
    grain_build_cache: dict = field(default_factory=dict)
    datasource_build_cache: dict = field(default_factory=dict)
    pseudonym_map: dict | None = None
    # Build-scoped joins for this resolution, as
    # (source_address, target_address, JoinType). Applied during the build and
    # shared so every sub-select (rowsets, multiselect arms) inherits them.
    scoped_joins: list = field(default_factory=list)


@dataclass
class History:
    base_environment: Environment
    local_base_concepts: dict[str, Concept] = field(default_factory=dict)
    history: dict[str, StrategyNode | None] = field(default_factory=dict)
    select_history: dict[str, StrategyNode | None] = field(default_factory=dict)
    rowset_history: dict[str, StrategyNode | None] = field(default_factory=dict)
    started: dict[str, int] = field(default_factory=dict)
    # Root sets whose merge expansion is mid-flight; balanced add/discard (see
    # merge_in_progress) so it only blocks nested re-entry, unlike `started`.
    merge_in_progress_keys: set[str] = field(default_factory=set)
    # Coalescing axes whose all-member assembly is mid-flight: sourcing a
    # member SIDE re-enters discovery for that member, which must resolve the
    # side alone rather than re-assembling the axis (balanced add/discard).
    coalescing_axis_in_progress: set[str] = field(default_factory=set)
    # The statement's WHERE, stashed at source_query_concepts entry: the
    # discovery loop deliberately sources sub-requests with conditions=None,
    # but presence-probe axis assembly needs statement-level truth (does the
    # WHERE provably reject NULL probes?) to skip complement-side scans.
    statement_conditions: "BuildWhereClause | None" = None
    build_caches: BuildCaches = field(default_factory=BuildCaches)

    def _concepts_to_lookup(
        self,
        search: list[BuildConcept],
        accept_partial: bool,
        conditions: BuildWhereClause | None = None,
    ) -> str:
        base = sorted([c.address for c in search])
        if conditions:
            return "-".join(base) + str(accept_partial) + str(conditions)
        return "-".join(base) + str(accept_partial)

    def search_to_history(
        self,
        search: list[BuildConcept],
        accept_partial: bool,
        output: StrategyNode | None,
        conditions: BuildWhereClause | None = None,
    ):
        self.history[
            self._concepts_to_lookup(search, accept_partial, conditions=conditions)
        ] = output
        self.log_end(
            search,
            accept_partial=accept_partial,
            conditions=conditions,
        )

    def get_history(
        self,
        search: list[BuildConcept],
        conditions: BuildWhereClause | None = None,
        accept_partial: bool = False,
        parent_key: str = "",
    ) -> StrategyNode | None | bool:
        key = self._concepts_to_lookup(
            search,
            accept_partial,
            conditions,
        )
        if parent_key and parent_key == key:
            raise ValueError(
                f"Parent key {parent_key} is the same as the current key {key}"
            )
        if key in self.history:
            node = self.history[key]
            if node:
                return node.copy()
            return node
        return False

    def log_start(
        self,
        search: list[BuildConcept],
        accept_partial: bool = False,
        conditions: BuildWhereClause | None = None,
    ):
        key = self._concepts_to_lookup(
            search,
            accept_partial=accept_partial,
            conditions=conditions,
        )
        if key in self.started:
            self.started[key] += 1
        else:
            self.started[key] = 1
        if self.started[key] > 5:
            raise UnresolvableQueryException(
                f"Was unable to resolve datasources to serve this query from model; unresolvable set was {search}. You may be querying unrelated concepts."
            )

    def log_end(
        self,
        search: list[BuildConcept],
        accept_partial: bool = False,
        conditions: BuildWhereClause | None = None,
    ):
        key = self._concepts_to_lookup(
            search,
            accept_partial=accept_partial,
            conditions=conditions,
        )
        if key in self.started:
            del self.started[key]

    def check_started(
        self,
        search: list[BuildConcept],
        accept_partial: bool = False,
        conditions: BuildWhereClause | None = None,
    ):
        return (
            self._concepts_to_lookup(
                search,
                accept_partial,
                conditions=conditions,
            )
            in self.started
        )

    @contextmanager
    def merge_in_progress(
        self,
        search: list[BuildConcept],
        accept_partial: bool = False,
        conditions: BuildWhereClause | None = None,
    ):
        """Mark a root set's merge expansion in-flight; yields True if it was
        already in-flight (caller should skip to break the recursion)."""
        key = self._concepts_to_lookup(search, accept_partial, conditions=conditions)
        if key in self.merge_in_progress_keys:
            yield True
            return
        self.merge_in_progress_keys.add(key)
        try:
            yield False
        finally:
            self.merge_in_progress_keys.discard(key)

    def gen_select_node(
        self,
        concepts: list[BuildConcept],
        environment: BuildEnvironment,
        g,
        depth: int,
        fail_if_not_found: bool = False,
        accept_partial: bool = False,
        conditions: BuildWhereClause | None = None,
    ) -> StrategyNode | None:
        from trilogy.core.processing.node_generators.select_node import gen_select_node

        fingerprint = self._concepts_to_lookup(
            concepts,
            accept_partial,
            conditions=conditions,
        )
        if fingerprint in self.select_history:
            rval = self.select_history[fingerprint]
            if rval:
                # all nodes must be copied before returning
                return rval.copy()
            return rval
        gen = gen_select_node(
            concepts,
            environment,
            g,
            depth + 1,
            fail_if_not_found=fail_if_not_found,
            accept_partial=accept_partial,
            conditions=conditions,
        )
        self.select_history[fingerprint] = gen
        if gen:
            return gen.copy()
        return gen


__all__ = [
    "BuildCaches",
    "ConstantNode",
    "FilterNode",
    "GroupNode",
    "History",
    "MergeNode",
    "MultiSelectMergeNode",
    "NodeJoin",
    "RecursiveNode",
    "RowsetNode",
    "SelectNode",
    "StrategyNode",
    "SubselectNode",
    "UnionNode",
    "UnnestNode",
    "WhereSafetyNode",
    "WindowNode",
]
