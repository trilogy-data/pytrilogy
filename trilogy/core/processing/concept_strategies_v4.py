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
from trilogy.core.env_processor import generate_graph
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.author import MultiSelectLineage, SelectLineage
from trilogy.core.models.build import (
    BuildConcept,
    BuildConditional,
    BuildGrain,
    BuildMultiSelectLineage,
    BuildRowsetItem,
    BuildSelectLineage,
    BuildWhereClause,
    Factory,
    get_canonical_pseudonyms,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import strip_tautological_not_null
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
from trilogy.utility import unique

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


def _factory_for_history(history: "V4History") -> Factory:
    author_env = history.base_environment
    caches = history.build_caches
    if caches.pseudonym_map is None:
        caches.pseudonym_map = get_canonical_pseudonyms(author_env)
    return Factory(
        environment=author_env,
        build_cache=caches.build_cache,
        canonical_build_cache=caches.canonical_build_cache,
        grain_build_cache=caches.grain_build_cache,
        pseudonym_map=caches.pseudonym_map,
    )


def _protected_addresses(
    statement: BuildSelectLineage | BuildMultiSelectLineage,
) -> set[str]:
    protected: set[str] = set()
    for component in statement.output_components:
        protected.add(component.address)
        protected.add(component.canonical_address)
    order_by = statement.order_by
    if order_by is not None:
        for item in order_by.items:
            for arg in item.concept_arguments:
                protected.add(arg.address)
                protected.add(arg.canonical_address)
    return protected


def _build_nested_select(
    select: SelectLineage | MultiSelectLineage,
    history: "V4History",
) -> tuple[
    BuildSelectLineage | BuildMultiSelectLineage,
    BuildEnvironment,
    BuildWhereClause | None,
]:
    """Build and materialize one nested select in its own build environment."""
    author_env = history.base_environment
    caches = history.build_caches
    factory = _factory_for_history(history)
    built: BuildSelectLineage | BuildMultiSelectLineage = factory.build(select)
    build_env = author_env.materialize_for_select(
        built.local_concepts,
        build_cache=caches.build_cache,
        pseudonym_map=factory.pseudonym_map,
        grain_build_cache=caches.grain_build_cache,
        canonical_build_cache=caches.canonical_build_cache,
        datasource_build_cache=caches.datasource_build_cache,
    )
    where_clause = strip_tautological_not_null(
        built.where_clause, build_env, _protected_addresses(built)
    )
    return built, build_env, where_clause


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

    def _empty() -> BuildInfo:
        return BuildInfo(
            concept_graph=nx.DiGraph(),
            group_graph=nx.DiGraph(),
            group_attrs={},
            strategy_node=None,
        )

    arm_nodes: list[StrategyNode] = []
    for arm in lineage.selects:
        built_arm, arm_env, arm_where = _build_nested_select(arm, history)
        arm_conditions = [arm_where] if arm_where else []
        arm_info = search_concepts(
            mandatory_list=list(built_arm.output_components),
            history=history,
            environment=arm_env,
            depth=depth + 1,
            g=generate_graph(arm_env),
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


def resolve_rowset(
    outputs: List[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    history: "V4History",
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Plan a rowset boundary node by recursively planning its inner select
    through v4, then projecting that producer under the outer handle addresses.

    The rowset's inner select is a self-contained sub-query (the same shape v3
    planned via `get_query_node`): we build its author lineage against the base
    environment, materialize a FRESH build environment + graph for it (the
    outer environment classifies the inner's concepts under rowset aliasing —
    a plain root reads back as `derivation=rowset` there — so reusing it
    mis-buckets the inner plan; q14's nested rowsets), plan its outputs + WHERE
    through `search_concepts`, apply the inner HAVING as a post-aggregate
    filter, then re-expose the producer's columns under the outer rowset
    handles. Each handle is a ROWSET concept whose `lineage.content` is the
    inner column it wraps — the renderer emits the handle as that content, so
    the boundary is a thin projection whose inputs are the content columns the
    inner producer supplies.

    `outputs` are all the same rowset (the rowset grouping rule buckets one
    rowset's handles together), but a recursive nested-rowset search can hand a
    bucket of plain roots here; bail to None so the caller treats it as a
    normal group rather than asserting."""
    rowset_outputs = [o for o in outputs if isinstance(o.lineage, BuildRowsetItem)]
    if not rowset_outputs:
        return None
    lineage = rowset_outputs[0].lineage
    assert isinstance(lineage, BuildRowsetItem)
    select: SelectLineage | MultiSelectLineage = lineage.rowset.select

    built, inner_env, inner_where = _build_nested_select(select, history)
    inner_g = generate_graph(inner_env)

    inner_info = search_concepts(
        mandatory_list=list(built.output_components),
        history=history,
        environment=inner_env,
        depth=depth + 1,
        g=inner_g,
        conditions=[inner_where] if inner_where else [],
    )
    inner_node = inner_info.strategy_node
    if inner_node is None:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} rowset {lineage.rowset.name} "
            f"inner select did not resolve"
        )
        return None

    # Inner HAVING is a post-aggregate filter over the inner producer (mirrors
    # the HAVING wrap in `get_query_node`).
    having = getattr(built, "having_clause", None)
    if having is not None:
        base = inner_node
        # A HAVING can compare against a SEPARATE scalar rowset
        # (q14 `bucket_sum_l0 > avg_sales.average_sales`). Plan those external
        # args on their own and cross-join them in — folding them into the main
        # search would pull the scalar into this rowset's grain and mis-bucket
        # it. Each is a self-contained sub-plan, joined on its (empty) grain.
        produced_addrs = {o.address for o in inner_node.output_concepts}
        extra_args = [
            c for c in having.concept_arguments if c.address not in produced_addrs
        ]
        if extra_args:
            extra_info = search_concepts(
                mandatory_list=extra_args,
                history=history,
                environment=inner_env,
                depth=depth + 1,
                g=inner_g,
                conditions=[],
            )
            if extra_info.strategy_node is not None:
                merged_outputs = unique(
                    list(inner_node.output_concepts)
                    + list(extra_info.strategy_node.output_concepts),
                    "address",
                )
                base = MergeNode(
                    input_concepts=merged_outputs,
                    output_concepts=merged_outputs,
                    environment=inner_env,
                    parents=[inner_node, extra_info.strategy_node],
                )
        combined = (
            BuildConditional(
                left=base.conditions,
                right=having.conditional,
                operator=BooleanOperator.AND,
            )
            if base.conditions
            else having.conditional
        )
        inner_node = SelectNode(
            output_concepts=list(built.output_components),
            input_concepts=list(base.usable_outputs),
            parents=[base],
            environment=inner_env,
            partial_concepts=list(base.partial_concepts),
            conditions=combined,
        )

    # Expose the demanded handles plus any rowset-derived handle that carries a
    # PSEUDONYM — a cross-rowset merge (`merge X.a into Y.b`, q44/q54) links its
    # two boundaries on the merged keys via the canonical-pseudonym map in
    # `get_node_joins`, and those keys are rarely selected by the outer query,
    # so projecting only the demanded handles would drop them and the FINAL
    # merge would degrade to a `1=1` cross product. Pseudonyms are exactly what
    # a `merge into` produces, so they single out the join keys without
    # over-projecting unrelated internals — e.g. a rowset-wrapped multiselect's
    # bare align inputs (q64 `item_sk_99`, no pseudonyms) must NOT leak out, or
    # the outer FINAL has an output no parent can source.
    produced = {o.address: o for o in inner_node.output_concepts}
    derived = lineage.rowset.derived_concepts
    demanded = {o.address for o in rowset_outputs}
    handle_pool = list(environment.concepts.values()) + list(
        environment.alias_origin_lookup.values()
    )
    handles: list[BuildConcept] = []
    inputs: list[BuildConcept] = []
    seen: set[str] = set()
    for handle in [*rowset_outputs, *handle_pool]:
        hlineage = handle.lineage
        if handle.address in seen or handle.address not in derived:
            continue
        if not isinstance(hlineage, BuildRowsetItem):
            continue
        if hlineage.content.address not in produced:
            continue
        if handle.address not in demanded and not handle.pseudonyms:
            continue
        seen.add(handle.address)
        handles.append(handle)
        inputs.append(produced[hlineage.content.address])

    # A rowset wrapping a multiselect (q64): an aligned handle's content is the
    # multiselect concept (e.g. `s_name`), which the renderer resolves via
    # `find_source` — it needs the arm concepts (`s_name_99`/`s_name_00`) in the
    # SAME CTE's outputs. They're not handles, so carry them as HIDDEN outputs
    # of this boundary; the aligned value is then materialized here and outer
    # CTEs just reference the column (mirrors v3 `gen_rowset_node`, whose node
    # kept the arm concepts as hidden outputs).
    hidden: set[str] = set()
    if isinstance(built, BuildMultiSelectLineage):
        handle_addrs = {h.address for h in handles}
        for item in built.align.items:
            for arm in item.concepts:
                if arm.address in produced and arm.address not in handle_addrs:
                    arm_concept = produced[arm.address]
                    handles.append(arm_concept)
                    inputs.append(arm_concept)
                    hidden.add(arm_concept.address)

    boundary = SelectNode(
        output_concepts=handles,
        input_concepts=inputs,
        parents=[inner_node],
        environment=inner_env,
        # Grain over the outer handles (mirrors v3 `gen_rowset_node`): lets the
        # FINAL merge join two rowsets on their shared/pseudonym grain key
        # instead of cross-joining when the boundary exposes no grain.
        grain=BuildGrain.from_concepts([h for h in handles if h.address not in hidden]),
        hidden_concepts=hidden,
    )
    # A filter the group graph injected at this boundary is a consumer-side
    # predicate over the rowset's rows — e.g. a multiselect arm's per-arm
    # `marital != ...` over the row-projection rowset it reads (q64). The inner
    # plan didn't apply it (it's not part of the rowset's own select), so apply
    # it here over the materialized rows.
    if conditions is not None:
        boundary = SelectNode(
            output_concepts=list(handles),
            input_concepts=list(boundary.usable_outputs),
            parents=[boundary],
            environment=inner_env,
            conditions=conditions.conditional,
            grain=boundary.grain,
            hidden_concepts=hidden,
        )
    return boundary


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
    datasource_columns = [
        frozenset(c.address for c in ds.output_concepts)
        for ds in environment.datasources.values()
    ]
    group_graph, group_attrs = build_group_graph(
        concept_graph, concept_attrs, conditions, mandatory_list, datasource_columns
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
