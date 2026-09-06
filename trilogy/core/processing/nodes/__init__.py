from dataclasses import dataclass, field

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
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

    build_cache: dict = field(default_factory=dict)
    canonical_build_cache: dict = field(default_factory=dict)
    grain_build_cache: dict = field(default_factory=dict)
    datasource_build_cache: dict = field(default_factory=dict)
    # Context-free environment materializations (EnvBaseline), keyed by the
    # scoped-join tuple they were built under; nested arms replay only their
    # overlay's delta against these. Scoped to this resolution like every
    # other cache here.
    env_baselines: dict = field(default_factory=dict)
    pseudonym_map: dict | None = None
    # Build-scoped joins for this resolution, as
    # (source_address, target_address, JoinType). Applied during the build and
    # shared so every sub-select (rowsets, multiselect arms) inherits them.
    scoped_joins: list = field(default_factory=list)


@dataclass
class History:
    base_environment: Environment
    select_history: dict[str, StrategyNode | None] = field(default_factory=dict)
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
