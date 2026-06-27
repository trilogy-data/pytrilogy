"""v4 discovery: a staged planner with explicit phase boundaries.

    Stage 1 (concept_graph): walk lineage from the mandatory list and
    conditions back to roots, producing a per-concept DAG with depth labels
    (d0/d1/d*) and typed row/existence edges. This is concept demand only.

    Stage 2 (group_graph): collapse compatible concepts into groups that can
    be sourced together, inject filter clauses at the furthest-upstream group
    that can serve them, compute group IO, and append a FINAL sink.

    Stage 3 (strategy_builder + source_planning): materialize each group in
    topological order. ROOT groups pick concrete datasource plans here; other
    groups dispatch to their derivation generators.

    Stage 4 (_assemble_final_node): zip the materialized groups into the final
    query node, carrying join keys only as needed and deduping to the requested
    output grain.

The stage implementations live in `v4_helper/`; this file is just the public
API and the History cache wiring.
"""

from dataclasses import dataclass, field

from trilogy.constants import logger
from trilogy.core import graph as nx
from trilogy.core.enums import BooleanOperator, Derivation
from trilogy.core.env_processor import generate_graph
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.author import MultiSelectLineage, SelectLineage
from trilogy.core.models.build import (
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildGrain,
    BuildMultiSelectLineage,
    BuildRowsetItem,
    BuildSelectLineage,
    BuildUnionSelectLineage,
    BuildWhereClause,
    Factory,
    get_canonical_pseudonyms,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.aggregate_rollup import (
    _conditions_supported,
    _is_additive_aggregate,
    get_additive_rollup_concepts,
)
from trilogy.core.processing.condition_utility import condition_implies
from trilogy.core.processing.discovery_utility import (
    LOGGER_PREFIX,
    depth_to_prefix,
    raise_if_disconnected_for,
)
from trilogy.core.processing.node_generators.multiselect_node import extra_align_joins
from trilogy.core.processing.nodes import (
    History,
    MergeNode,
    SelectNode,
    StrategyNode,
    UnionNode,
)
from trilogy.core.processing.v4_helper import (
    FINAL_NODE_ID,
    ROW_SHAPE_BARRIER_DERIVATIONS,
    BuildInfo,
    build_concept_graph,
    build_group_graph,
    build_strategy_node,
)
from trilogy.core.processing.v4_helper.condition_injection import (
    ConditionSources,
    inject_condition_at_node,
)
from trilogy.core.processing.v4_helper.source_policy import (
    ROWSET_SOURCE_POLICY,
    STRICT_SOURCE_POLICY,
    SourcePolicy,
    source_policy_from_legacy_accept_partial,
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
    # Derived-connector origin addresses currently mid-plan, used by the root
    # source planner to break the self-referential bridge recursion (a merged
    # recursive connector whose own input search re-routes through it).
    connectors_in_progress: set[str] = field(default_factory=set)

    def _v4_key(
        self,
        search: list[BuildConcept],
        source_policy: SourcePolicy,
        conditions: list[BuildWhereClause],
    ) -> str:
        base = "-".join(sorted(c.address for c in search)) + source_policy.cache_key
        return base + str(conditions) if conditions else base

    def get_build_history(
        self,
        search: list[BuildConcept],
        source_policy: SourcePolicy,
        conditions: list[BuildWhereClause],
    ) -> BuildInfo | None | bool:
        key = self._v4_key(search, source_policy, conditions)
        if key in self.build_history:
            node = self.build_history[key]
            return node.copy() if node else node
        return False

    def build_to_history(
        self,
        search: list[BuildConcept],
        source_policy: SourcePolicy,
        output: BuildInfo | None,
        conditions: list[BuildWhereClause],
    ) -> None:
        self.build_history[self._v4_key(search, source_policy, conditions)] = output


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
        scoped_joins=caches.scoped_joins,
    )


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
        scoped_joins=caches.scoped_joins,
    )
    return built, build_env, built.where_clause


def _resolve_multiselect(
    ms_concept: BuildConcept,
    mandatory_list: list[BuildConcept],
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
        arm_g = generate_graph(arm_env)
        arm_info = search_concepts(
            mandatory_list=list(built_arm.output_components),
            history=history,
            environment=arm_env,
            depth=depth + 1,
            g=arm_g,
            conditions=arm_conditions,
        )
        arm_node = arm_info.strategy_node
        if arm_node is None:
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} multiselect arm "
                f"{[c.address for c in built_arm.output_components]} did not resolve"
            )
            return _empty()
        # A per-arm HAVING is a post-aggregate filter over that arm's producer
        # (e.g. `count_distinct(ticket) as c having c > 1000`). The top-level
        # `_get_query_node_v4` HAVING wrap doesn't reach into arms, so apply it
        # here -- mirroring the inner-HAVING handling in `resolve_rowset`.
        arm_having = built_arm.having_clause
        if arm_having is not None:
            arm_node = _resolve_and_inject_condition(
                arm_node,
                arm_having,
                list(built_arm.output_components),
                environment=arm_env,
                graph=arm_g,
                history=history,
                depth=depth,
                partial_concepts=list(arm_node.partial_concepts),
            )
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
        node = _resolve_and_inject_condition(
            node,
            BuildWhereClause(conditional=combined),
            list(mandatory_list),
            environment=environment,
            graph=g,
            history=history,
            depth=depth,
        )

    node.set_output_concepts(list(mandatory_list))
    node.rebuild_cache()
    return BuildInfo(
        concept_graph=nx.DiGraph(),
        group_graph=nx.DiGraph(),
        group_attrs={},
        strategy_node=node,
    )


def _resolve_union_select(
    union_concept: BuildConcept,
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    history: "V4History",
    conditions: list[BuildWhereClause],
) -> BuildInfo:
    """Plan a relational `union(...)`/`except(...)`/`intersect(...)` TVF: a
    column-positional row stack.

    Each arm is planned independently (same arm recursion as a multiselect),
    then each arm projects its i-th column onto the shared output concept and
    the arms are combined with a `UnionNode` carrying the lineage's set
    operator (UNION ALL / EXCEPT / INTERSECT) — not joined. Arm order is
    preserved; for EXCEPT it is semantic (left-fold)."""
    lineage = union_concept.lineage
    assert isinstance(lineage, BuildUnionSelectLineage)

    def _empty() -> BuildInfo:
        return BuildInfo(
            concept_graph=nx.DiGraph(),
            group_graph=nx.DiGraph(),
            group_attrs={},
            strategy_node=None,
        )

    # Canonical output order = align-item order; every arm must expose exactly
    # these concepts, in this order, so the UNION columns line up.
    ordered_outputs = [
        environment.concepts[item.aligned_concept] for item in lineage.align.items
    ]

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
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} union arm "
                f"{[c.address for c in built_arm.output_components]} did not resolve"
            )
            return _empty()
        # Expose each arm's i-th column under the shared union output, then hide
        # the per-arm internal columns so the rendered SELECT emits only the
        # union outputs — sourced from the hidden columns via find_source.
        arm_own = [c.address for c in arm_node.output_concepts]
        for out in list(arm_node.output_concepts):
            merge_name = lineage.get_merge_concept(out)
            if merge_name:
                arm_node.output_concepts.append(environment.concepts[merge_name])
        arm_node.hidden_concepts = set(arm_own)
        arm_node.rebuild_cache()
        arm_nodes.append(arm_node)

    node: StrategyNode = UnionNode(
        input_concepts=list(ordered_outputs),
        output_concepts=list(ordered_outputs),
        environment=environment,
        depth=depth,
        parents=arm_nodes,
        set_operator=lineage.operator,
    )
    node.set_output_concepts(list(mandatory_list))
    node.rebuild_cache()
    return BuildInfo(
        concept_graph=nx.DiGraph(),
        group_graph=nx.DiGraph(),
        group_attrs={},
        strategy_node=node,
    )


def _resolve_condition_sources(
    node: StrategyNode,
    condition: BuildWhereClause,
    environment: BuildEnvironment,
    graph: ReferenceGraph,
    history: "V4History",
    depth: int,
) -> ConditionSources:
    """Resolve condition row inputs and existence inputs without mixing them."""
    sources = ConditionSources()
    produced_addrs = {o.address for o in node.usable_outputs}
    row_args = unique(
        [c for c in condition.row_arguments if c.address not in produced_addrs],
        "address",
    )
    if row_args:
        row_info = search_concepts(
            mandatory_list=row_args,
            history=history,
            environment=environment,
            depth=depth + 1,
            g=graph,
            conditions=[],
        )
        if row_info.strategy_node is None:
            raise ValueError(
                "Could not resolve condition row arguments "
                f"{[c.address for c in row_args]}"
            )
        sources.row_concepts = row_args
        sources.row_parents.append(row_info.strategy_node)

    seen_existence_addrs: set[str] = set()
    seen_parent_ids: set[int] = set()
    for arg_group in condition.existence_arguments or ():
        existence_args = unique(list(arg_group), "address")
        if not existence_args:
            continue
        ex_info = search_concepts(
            mandatory_list=existence_args,
            history=history,
            environment=environment,
            depth=depth + 1,
            g=graph,
            conditions=[],
        )
        if ex_info.strategy_node is None:
            raise ValueError(
                "Could not resolve condition existence arguments "
                f"{[c.address for c in existence_args]}"
            )
        for concept in existence_args:
            if concept.address not in seen_existence_addrs:
                seen_existence_addrs.add(concept.address)
                sources.existence_concepts.append(concept)
        if id(ex_info.strategy_node) not in seen_parent_ids:
            seen_parent_ids.add(id(ex_info.strategy_node))
            sources.existence_parents.append(ex_info.strategy_node)
    return sources


def _resolve_and_inject_condition(
    node: StrategyNode,
    condition: BuildWhereClause,
    output_concepts: list[BuildConcept],
    environment: BuildEnvironment,
    graph: ReferenceGraph,
    history: "V4History",
    depth: int,
    *,
    partial_concepts: list[BuildConcept] | None = None,
    grain: BuildGrain | None = None,
    hidden_concepts: set[str] | None = None,
) -> StrategyNode:
    sources = _resolve_condition_sources(
        node, condition, environment, graph, history, depth
    )
    return inject_condition_at_node(
        node,
        condition,
        output_concepts,
        environment,
        sources,
        partial_concepts=partial_concepts,
        grain=grain,
        hidden_concepts=hidden_concepts,
    )


def resolve_rowset(
    outputs: list[BuildConcept],
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

    # The inner select is its own resolution scope; if its required concepts span
    # unconnected models (a grain-only `by` edge does NOT bridge them), surface
    # the typed subgraph error rather than silently cross-joining inside the CTE.
    raise_if_disconnected_for(
        list(built.output_components),
        inner_where,
        inner_env,
        inner_g,
        # v4 pre-gate: see query_processor._raise_if_disconnected.
        island_rowsets=False,
    )

    inner_info = search_concepts(
        mandatory_list=list(built.output_components),
        history=history,
        environment=inner_env,
        depth=depth + 1,
        g=inner_g,
        source_policy=ROWSET_SOURCE_POLICY,
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
    having = built.having_clause
    if having is not None:
        inner_node = _resolve_and_inject_condition(
            inner_node,
            having,
            list(built.output_components),
            environment=inner_env,
            graph=inner_g,
            history=history,
            depth=depth,
            partial_concepts=list(inner_node.partial_concepts),
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
    condition_row_addresses = (
        {c.address for c in conditions.row_arguments}
        if conditions is not None
        else set()
    )
    for handle in [*rowset_outputs, *handle_pool]:
        hlineage = handle.lineage
        if handle.address in seen or handle.address not in derived:
            continue
        if not isinstance(hlineage, BuildRowsetItem):
            continue
        if hlineage.content.address not in produced:
            continue
        if (
            handle.address not in demanded
            and handle.address not in condition_row_addresses
            and not handle.pseudonyms
        ):
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

    boundary: StrategyNode = SelectNode(
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
        condition_outputs = [
            h
            for h in handles
            if h.address not in condition_row_addresses
            or h.address in demanded
            or h.address in hidden
            or h.pseudonyms
        ]
        boundary = _resolve_and_inject_condition(
            boundary,
            conditions,
            list(condition_outputs),
            environment=inner_env,
            graph=inner_g,
            history=history,
            depth=depth,
            grain=boundary.grain,
            hidden_concepts=hidden,
        )
    return boundary


def _combine_conditions(
    conditions: list[BuildWhereClause],
) -> BuildWhereClause | None:
    if not conditions:
        return None
    combined = conditions[0].conditional
    for extra in conditions[1:]:
        combined = BuildConditional(
            left=combined, right=extra.conditional, operator=BooleanOperator.AND
        )
    return BuildWhereClause(conditional=combined)


def _datasource_materializes(
    concept: BuildConcept,
    ds: BuildDatasource,
    where: BuildWhereClause | None,
    environment: BuildEnvironment,
) -> bool:
    """A datasource materializes `concept` iff it binds a COMPLETE column whose
    canonical address matches — name-independent: a differently-named column with
    the same underlying expression (`sum(x)` vs a bound `total`) satisfies it —
    and can express the query's row-narrowing conditions.

    Partialness is relative to the query, via the same `condition_implies` rule
    source-planning's `partial_is_full` uses. Two partial mechanisms:
    - Population (`complete where X`, `ds.non_partial_for`): the table holds only
      the X-subset of rows, so it's a complete source only when the query implies
      X. A `~key` merge column (`merge orid into ~orid_2`) is the degenerate case —
      intrinsically one row per key, missing values that never appear as a key,
      with no `non_partial_for` to recover it — so it never qualifies.
    - Column (`Modifier.PARTIAL`): a `partial ... complete where X` table's columns
      are individually partial but become complete once the query implies X.

    Matching on `ds.columns` (genuine bindings), not `output_concepts`, is
    deliberate: the latter includes merge-pseudonym-expanded entries that hide the
    real PARTIAL marker."""
    if not _conditions_supported(ds, where, environment.concepts):
        return False
    partial_covered = bool(
        where
        and ds.non_partial_for
        and condition_implies(where.conditional, ds.non_partial_for.conditional)
    )
    if ds.non_partial_for is not None and not partial_covered:
        return False
    return any(
        col.concept.canonical_address == concept.canonical_address
        and (col.is_complete or partial_covered)
        for col in ds.columns
    )


def _materialized_root_addresses(
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    conditions: list[BuildWhereClause],
) -> frozenset[str]:
    """Demanded derived concepts that a datasource materializes directly — a
    precomputed / pre-aggregated summary table or a persisted derived column.
    Stage 1 treats these as ROOT scans so v4 reads the table instead of
    re-deriving from base.

    Eligibility is one rule (`_datasource_materializes`): a datasource binds the
    concept's canonical expression as a COMPLETE column (or a partial one the
    query's conditions complete) and can express the conditions. EXACT-grain
    AGGREGATE/BASIC additionally require `ds.grain == target_grain` so the scan's
    row multiplicity matches; an UNNEST is exempt (a persisted unnest table's
    declared grain is the coarser key, understating its per-value rows).

    Additive rollup: an additive AGGREGATE (sum/count) that no datasource has at
    the exact grain, but a *finer*-grain table binds, is also treated as a root
    scanned from that finer table — `_group_to_grain_if_required` then
    re-aggregates it to the target grain (`sum(finer.col)`)."""
    if not mandatory_list:
        return frozenset()
    target_grain = BuildGrain.from_concepts(mandatory_list)
    where = _combine_conditions(conditions)
    datasources = [
        ds for ds in environment.datasources.values() if isinstance(ds, BuildDatasource)
    ]
    out: set[str] = set()
    for concept in mandatory_list:
        # Short-circuit only derivations a datasource row fully reproduces: a
        # precomputed AGGREGATE/scalar BASIC, or an UNNEST a table persists
        # directly. The other row-shaping derivations (ROWSET/RECURSIVE/FILTER/
        # WINDOW/...) generate or drop rows a scan wouldn't reproduce; enabling
        # them would each need its own population-vs-conditions validation.
        if concept.derivation == Derivation.UNNEST:
            # No grain-equality gate: a `persist ... from select key, unnest_val`
            # table declares the coarser key grain but physically holds one row
            # per unnest value, so the scan reproduces them. The merge-onto-key
            # shape is excluded by the partial-column check in the predicate.
            if any(
                _datasource_materializes(concept, ds, where, environment)
                for ds in datasources
            ):
                out.add(concept.address)
            continue
        if concept.derivation not in (Derivation.AGGREGATE, Derivation.BASIC):
            continue
        is_aggregate = concept.derivation == Derivation.AGGREGATE
        # EXACT: a datasource at the target grain materializes the concept.
        exact = False
        if concept.canonical_address in environment.materialized_canonical_concepts:
            for ds in datasources:
                if ds.grain != target_grain:
                    continue
                if _datasource_materializes(concept, ds, where, environment):
                    out.add(concept.address)
                    exact = True
                    break
        if exact or not (is_aggregate and _is_additive_aggregate(concept)):
            continue
        # ROLLUP: a finer-grain table binds the same named concept (matched by
        # address, since the finer instance has a different grain canonical), and
        # the additive aggregate can be SUM-rolled up to the target grain.
        #
        # Marking the concept a root lets source-planning pick a table binding
        # it. `get_additive_rollup_concepts` below is passed the filter and only
        # matches a datasource whose grain can express it (`_conditions_supported`):
        # a group-level filter (constant within a target-grain group) matches any
        # coarser/exact table, while a finer filter (`order_date` below a
        # `customer_id` grain) only matches a finer summary that carries the
        # column — `plan_source._plan_finer_filter_rollup` then pins that table,
        # pushes the filter pre-aggregation, and SUM-rolls. A coarser table is
        # never matched under a finer filter, so post-rollup decoupling can't
        # double-count.
        for ds in datasources:
            if concept.address not in {c.address for c in ds.output_concepts}:
                continue
            rolled = get_additive_rollup_concepts(
                datasource=ds,
                requested_concepts=mandatory_list,
                concepts_by_address=environment.concepts,
                datasources=datasources,
                target_grain=target_grain,
                conditions=where,
            )
            if any(r.address == concept.address for r in rolled):
                out.add(concept.address)
                break
    return frozenset(out)


def _build_from_graph(
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    history: "V4History",
    conditions: list[BuildWhereClause],
    source_policy: SourcePolicy,
    materialized_roots: frozenset[str],
) -> BuildInfo:
    concept_graph, concept_attrs, concept_edges = build_concept_graph(
        mandatory_list, environment, conditions, materialized_roots
    )
    datasource_columns = [
        frozenset(c.address for c in ds.output_concepts)
        for ds in environment.datasources.values()
    ]
    (
        group_graph,
        group_edges,
        group_attrs,
        merged_group_graph,
        merged_group_edges,
    ) = build_group_graph(
        concept_graph,
        concept_edges,
        concept_attrs,
        conditions,
        mandatory_list,
        datasource_columns,
        environment=environment,
        return_merged_graph=True,
    )
    strategy_node = build_strategy_node(
        group_graph,
        group_edges,
        group_attrs,
        mandatory_list,
        environment,
        g,
        history,
        source_policy=source_policy,
    )
    return BuildInfo(
        concept_graph=concept_graph,
        merged_group_graph=merged_group_graph,
        group_graph=group_graph,
        group_attrs=group_attrs,
        concept_attrs=concept_attrs,
        concept_edges=concept_edges,
        merged_group_edges=merged_group_edges,
        group_edges=group_edges,
        strategy_node=strategy_node,
    )


def _search_concepts(
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    history: "V4History",
    conditions: list[BuildWhereClause],
    source_policy: SourcePolicy = STRICT_SOURCE_POLICY,
) -> BuildInfo:
    # A top-level multiselect (merge/align) isn't a single source graph — its
    # arms are independent sub-plans joined on the alignment concept. Resolve
    # each arm through v4 and stitch them, rather than trying to source both
    # arms' columns from one (unjoinable) root scan.
    # A relational union TVF is a column-positional row stack (UNION), not a
    # key-join. Its lineage subclasses BuildMultiSelectLineage, so check it
    # first and route to the union combiner.
    union_concept = next(
        (c for c in mandatory_list if isinstance(c.lineage, BuildUnionSelectLineage)),
        None,
    )
    if union_concept is not None:
        return _resolve_union_select(
            union_concept, mandatory_list, environment, depth, g, history, conditions
        )
    ms_concept = next(
        (c for c in mandatory_list if isinstance(c.lineage, BuildMultiSelectLineage)),
        None,
    )
    if ms_concept is not None:
        return _resolve_multiselect(
            ms_concept, mandatory_list, environment, depth, g, history, conditions
        )
    # Prefer a precomputed/summary datasource for any demanded aggregate it
    # materializes at grain. If treating those as roots can't be sourced (the
    # summary doesn't combine with the rest of the query), fall back to the
    # derive-from-base plan — mirrors v3 trying the direct source first.
    materialized_roots = _materialized_root_addresses(
        mandatory_list, environment, conditions
    )
    info = _build_from_graph(
        mandatory_list,
        environment,
        depth,
        g,
        history,
        conditions,
        source_policy,
        materialized_roots,
    )
    if materialized_roots and info.strategy_node is None:
        info = _build_from_graph(
            mandatory_list,
            environment,
            depth,
            g,
            history,
            conditions,
            source_policy,
            frozenset(),
        )
    return info


def search_concepts(
    mandatory_list: list[BuildConcept],
    history: V4History,
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    accept_partial: bool = False,
    source_policy: SourcePolicy | None = None,
    conditions: list[BuildWhereClause] | None = None,
) -> BuildInfo:
    """Run the v4 planner against `mandatory_list` under `conditions`. Cached
    per `(mandatory_list, source_policy, conditions)` via `history`."""
    conditions = conditions or []
    active_policy = source_policy or source_policy_from_legacy_accept_partial(
        accept_partial
    )
    hist = history.get_build_history(
        search=mandatory_list,
        source_policy=active_policy,
        conditions=conditions,
    )
    if hist is not False:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Returning search node from "
            f"history ({'exists' if hist is not None else 'does not exist'}) for "
            f"{[c.address for c in mandatory_list]} with source_policy "
            f"{active_policy.cache_key}"
        )
        assert isinstance(hist, BuildInfo)
        return hist

    result = _search_concepts(
        mandatory_list,
        environment,
        depth=depth,
        g=g,
        source_policy=active_policy,
        history=history,
        conditions=conditions,
    )
    # a node may be mutated after being cached; always store a copy
    history.build_to_history(
        mandatory_list,
        active_policy,
        result.copy(),
        conditions=conditions,
    )
    return result
