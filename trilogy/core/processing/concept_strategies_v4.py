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

from dataclasses import dataclass, field
from typing import List

import networkx as nx

from trilogy.constants import logger
from trilogy.core.enums import BooleanOperator
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import (
    BuildConcept,
    BuildConditional,
    BuildMultiSelectLineage,
    BuildWhereClause,
    Factory,
    get_canonical_pseudonyms,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.discovery_utility import (
    LOGGER_PREFIX,
    depth_to_prefix,
)
from trilogy.core.processing.node_generators.multiselect_node import extra_align_joins
from trilogy.core.processing.nodes import History, MergeNode, SelectNode, StrategyNode
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
    "V4History",
    "search_concepts",
]


@dataclass
class V4History(History):
    """History fork for the v4 discovery prototype. The inherited StrategyNode
    cache still serves the v3 sub-searches v4 dispatches into; this fork adds a
    parallel, correctly-typed cache for the BuildInfo bundles v4 returns."""

    build_history: dict[str, BuildInfo | None] = field(default_factory=dict)

    def _v4_key(
        self,
        search: list[BuildConcept],
        accept_partial: bool,
        conditions: list[BuildWhereClause],
    ) -> str:
        base = "-".join(sorted(c.address for c in search)) + str(accept_partial)
        return base + str(conditions) if conditions else base

    def get_build_history(
        self,
        search: list[BuildConcept],
        accept_partial: bool,
        conditions: list[BuildWhereClause],
    ) -> BuildInfo | None | bool:
        key = self._v4_key(search, accept_partial, conditions)
        if key in self.build_history:
            node = self.build_history[key]
            return node.copy() if node else node
        return False

    def build_to_history(
        self,
        search: list[BuildConcept],
        accept_partial: bool,
        output: BuildInfo | None,
        conditions: list[BuildWhereClause],
    ) -> None:
        self.build_history[self._v4_key(search, accept_partial, conditions)] = output


def _resolve_multiselect(
    ms_concept: BuildConcept,
    mandatory_list: List[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    history: "V4History",
    conditions: list[BuildWhereClause],
) -> BuildInfo:
    """Plan a top-level multiselect (merge/align).

    Each arm is recursively planned by the v4 searcher (mirroring how rowsets
    recurse per branch), then the arms are stitched together with one FULL
    join per extra arm on the alignment concepts. The outer WHERE is a
    post-join filter. Same shape as the v3 multiselect generator, but the
    per-arm recursion goes through v4 rather than v3's `get_query_node`."""
    lineage = ms_concept.lineage
    assert isinstance(lineage, BuildMultiSelectLineage)

    # Arms are stored as AUTHOR SelectLineage (built lazily during discovery);
    # build each one against the author environment to recover its outputs and
    # WHERE, then plan those through v4.
    author_env = history.base_environment
    caches = history.build_caches
    if caches.pseudonym_map is None:
        caches.pseudonym_map = get_canonical_pseudonyms(author_env)
    factory = Factory(
        environment=author_env,
        build_cache=caches.build_cache,
        canonical_build_cache=caches.canonical_build_cache,
        grain_build_cache=caches.grain_build_cache,
        pseudonym_map=caches.pseudonym_map,
    )

    def _empty() -> BuildInfo:
        return BuildInfo(
            concept_graph=nx.DiGraph(),
            group_graph=nx.DiGraph(),
            group_attrs={},
            strategy_node=None,
        )

    arm_nodes: list[StrategyNode] = []
    for arm in lineage.selects:
        built_arm = factory.build(arm)
        arm_conditions = [built_arm.where_clause] if built_arm.where_clause else []
        arm_info = search_concepts(
            mandatory_list=list(built_arm.output_components),
            history=history,
            environment=environment,
            depth=depth + 1,
            g=g,
            conditions=arm_conditions,
        )
        arm_node = arm_info.strategy_node
        if arm_node is None:
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} multiselect arm "
                f"{[c.address for c in built_arm.output_components]} did not resolve"
            )
            return _empty()
        # Expose each arm's alignment key under the merge concept's address so
        # `extra_align_joins` can bind the arms together on it.
        for out in list(arm_node.output_concepts):
            merge_name = lineage.get_merge_concept(out)
            if merge_name:
                arm_node.output_concepts.append(environment.concepts[merge_name])
        arm_node.rebuild_cache()
        arm_nodes.append(arm_node)

    node_joins = extra_align_joins(lineage, environment, arm_nodes)
    merged_outputs = [
        c
        for arm in arm_nodes
        for c in arm.output_concepts
        if c.address not in (arm.hidden_concepts or set())
    ]
    node: StrategyNode = MergeNode(
        input_concepts=merged_outputs,
        output_concepts=merged_outputs,
        environment=environment,
        depth=depth,
        parents=arm_nodes,
        node_joins=node_joins,
    )

    # Outer WHERE (e.g. q46 `customer.address.city != bought_city`) references
    # concepts from both arms, so it can only be applied above the merge.
    if conditions:
        combined = conditions[0].conditional
        for extra in conditions[1:]:
            combined = BuildConditional(
                left=combined, right=extra.conditional, operator=BooleanOperator.AND
            )
        node = SelectNode(
            output_concepts=list(mandatory_list),
            input_concepts=list(node.usable_outputs),
            parents=[node],
            environment=environment,
            conditions=combined,
        )

    node.set_output_concepts(list(mandatory_list))
    node.rebuild_cache()
    return BuildInfo(
        concept_graph=nx.DiGraph(),
        group_graph=nx.DiGraph(),
        group_attrs={},
        strategy_node=node,
    )


def _search_concepts(
    mandatory_list: List[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    history: "V4History",
    conditions: list[BuildWhereClause],
    accept_partial: bool = False,
) -> BuildInfo:
    # A top-level multiselect (merge/align) isn't a single source graph — its
    # arms are independent sub-plans joined on the alignment concept. Resolve
    # each arm through v4 and stitch them, rather than trying to source both
    # arms' columns from one (unjoinable) root scan.
    ms_concept = next(
        (c for c in mandatory_list if isinstance(c.lineage, BuildMultiSelectLineage)),
        None,
    )
    if ms_concept is not None:
        return _resolve_multiselect(
            ms_concept, mandatory_list, environment, depth, g, history, conditions
        )
    concept_graph, concept_attrs = build_concept_graph(
        mandatory_list, environment, conditions
    )
    group_graph, group_attrs = build_group_graph(
        concept_graph, concept_attrs, conditions, mandatory_list
    )
    strategy_node = build_strategy_node(
        group_graph, group_attrs, mandatory_list, environment, g, history
    )
    return BuildInfo(
        concept_graph=concept_graph,
        group_graph=group_graph,
        group_attrs=group_attrs,
        concept_attrs=concept_attrs,
        strategy_node=strategy_node,
    )


def search_concepts(
    mandatory_list: List[BuildConcept],
    history: V4History,
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    accept_partial: bool = False,
    conditions: list[BuildWhereClause] | None = None,
) -> BuildInfo:
    """Run the v4 planner against `mandatory_list` under `conditions`. Cached
    per `(mandatory_list, accept_partial, conditions)` via `history`."""
    conditions = conditions or []
    hist = history.get_build_history(
        search=mandatory_list, accept_partial=accept_partial, conditions=conditions
    )
    if hist is not False:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Returning search node from "
            f"history ({'exists' if hist is not None else 'does not exist'}) for "
            f"{[c.address for c in mandatory_list]} with accept_partial {accept_partial}"
        )
        assert isinstance(hist, BuildInfo)
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
    history.build_to_history(
        mandatory_list,
        accept_partial,
        result.copy(),
        conditions=conditions,
    )
    return result
