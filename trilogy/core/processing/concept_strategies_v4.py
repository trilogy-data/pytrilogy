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
from trilogy.core.processing.node_generators.presence_probe import (
    is_presence_probe,
    probe_member_address,
)
from trilogy.core.processing.node_generators.rowset_node import (
    _interpose_limit_node,
    _scoped_joins_for_rowset,
)
from trilogy.core.processing.nodes import (
    BuildCaches,
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
    exclude_derived: list[str] | None = None,
) -> tuple[
    BuildSelectLineage | BuildMultiSelectLineage,
    BuildEnvironment,
    BuildWhereClause | None,
]:
    """Build and materialize one nested select in its own build environment.

    A nested select can carry its OWN query-scoped joins (a rowset body
    ``with rs as inner join a.aid = b.bid select ...``) that the outer resolution
    never saw. Those joins live on ``SelectLineage.scoped_joins`` and must be fed
    to BOTH the factory (so the joined keys build to one canonical) and the build
    env (so the graph bridges the two datasources) -- otherwise the body builds
    with no join, its datasources come back as separate components, and the
    read-back raises a misleading DisconnectedConceptsException for a join that is
    in fact present inside the rowset (surfaced on enriched TPC-DS q64).

    ``exclude_derived`` carries a rowset body's own derived concepts: an OUTER
    query-scoped join referencing them (``subset join a.store = b.store``)
    relates this rowset's output to its sibling and must not be applied inside
    the body's independent scope (v3's `_scoped_joins_for_rowset`) — the body
    would canonicalize its own output onto the cross-rowset group and source it
    back through itself."""
    author_env = history.base_environment
    caches = history.build_caches
    nested_scoped = select.scoped_joins if isinstance(select, SelectLineage) else []
    outer_scoped = _scoped_joins_for_rowset(caches.scoped_joins, exclude_derived or [])
    scoped_joins = outer_scoped + [j for j in nested_scoped if j not in outer_scoped]
    if caches.pseudonym_map is None:
        caches.pseudonym_map = get_canonical_pseudonyms(author_env)
    # The shared build caches are keyed on address/grain identity alone, which
    # is only correct while every build in the resolution applies the SAME
    # scoped joins — a join changes what an address builds to (canonical
    # collapse + pseudonym stamping). When this body carries its OWN joins the
    # outer resolution never saw, entries the outer scope cached are wrong
    # here (an outer-built join key comes back with no pseudonym link to its
    # body mate, so the inner aggregate detaches from its grouping key and
    # FINAL cross-joins ON 1=1); build this scope with fresh caches. The
    # converse (outer joins EXCLUDED here via `exclude_derived`) keeps the
    # shared caches: boundary pairing reads the outer join's pseudonym stamps
    # off them (subset_presence_probe rowset pairs).
    if any(j not in caches.scoped_joins for j in scoped_joins):
        caches = BuildCaches(
            pseudonym_map=caches.pseudonym_map, scoped_joins=scoped_joins
        )
    factory = Factory(
        environment=author_env,
        build_cache=caches.build_cache,
        canonical_build_cache=caches.canonical_build_cache,
        grain_build_cache=caches.grain_build_cache,
        pseudonym_map=caches.pseudonym_map,
        scoped_joins=scoped_joins,
    )
    built: BuildSelectLineage | BuildMultiSelectLineage = factory.build(select)
    build_env = author_env.materialize_for_select(
        built.local_concepts,
        build_cache=caches.build_cache,
        pseudonym_map=factory.pseudonym_map,
        grain_build_cache=caches.grain_build_cache,
        canonical_build_cache=caches.canonical_build_cache,
        datasource_build_cache=caches.datasource_build_cache,
        scoped_joins=scoped_joins,
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
        # Re-expose each arm's columns under the shared union outputs, in the
        # canonical align order — a UNION ALL stacks positionally, so every
        # arm's projection must list the outputs in the same order regardless
        # of its own column order. A scoped join inside the arm can emit the
        # authored key under its partner's address, so resolve through the
        # partner/pseudonym (`find_source` recovers the physical column at
        # render). The per-arm internal columns are hidden.
        produced = {
            lineage.get_merge_concept_resolved(out) for out in arm_node.output_concepts
        }
        for merged in ordered_outputs:
            if merged.address in produced:
                arm_node.output_concepts.append(merged)
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
        feeder = row_info.strategy_node
        # The standalone feeder plan hides its own grain keys at its FINAL
        # layer (non-mandatory there), but hidden outputs are invisible to
        # downstream join inference — the merge back onto `node` degrades to
        # a cartesian. Un-hide any key the consumer also carries so the pair
        # joins keyed (a keyless feeder, e.g. a `by *` global, still
        # cross-joins).
        shared_hidden = {
            o.address for o in feeder.output_concepts if o.address in produced_addrs
        } & set(feeder.hidden_concepts)
        if shared_hidden:
            feeder.hidden_concepts = set(feeder.hidden_concepts) - shared_hidden
            feeder.rebuild_cache()
        sources.row_concepts = row_args
        sources.row_parents.append(feeder)

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
        # An existence feeder is side-channel-only: slice its outputs to the
        # subselect columns (its mandatory contract). The nested plan can come
        # back carrying its predicate args as extra row outputs (`max_total`
        # for a HAVING membership), and any of those shared with the consumer
        # promotes the feeder to a row-join candidate in MergeNode resolution —
        # a spurious value-join whose grain then leaks the plan-local virt
        # across the rowset boundary. v3 renders the feeder as the bare
        # subselect column; match it.
        ex_node = ex_info.strategy_node
        existence_addrs = {c.address for c in existence_args}
        sliced = [o for o in ex_node.output_concepts if o.address in existence_addrs]
        if sliced and len(sliced) < len(ex_node.output_concepts):
            ex_node.set_output_concepts(sliced)
        for concept in existence_args:
            if concept.address not in seen_existence_addrs:
                seen_existence_addrs.add(concept.address)
                sources.existence_concepts.append(concept)
        if id(ex_node) not in seen_parent_ids:
            seen_parent_ids.add(id(ex_node))
            sources.existence_parents.append(ex_node)
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
        # A probe-only demand (the presence-count shape: the boundary's sole
        # contract output is a member's presence probe): recover the boundary
        # through the probe's member handle, which the obligation pass below
        # then materializes alongside the probe.
        for concept in outputs:
            if not is_presence_probe(concept.address):
                continue
            member_addr = probe_member_address(concept.address, environment)
            member = environment.concepts.get(member_addr) if member_addr else None
            if member is not None and isinstance(member.lineage, BuildRowsetItem):
                rowset_outputs = [member]
                break
    if not rowset_outputs:
        return None
    lineage = rowset_outputs[0].lineage
    assert isinstance(lineage, BuildRowsetItem)
    select: SelectLineage | MultiSelectLineage = lineage.rowset.select

    built, inner_env, inner_where = _build_nested_select(
        select, history, exclude_derived=lineage.rowset.derived_concepts
    )
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

    # The body's LIMIT (with the ORDER BY it selects under) defines the
    # rowset's row set; materialize it as a dedicated post-body node exactly
    # like v3 (`_interpose_limit_node`) so outer WHEREs stay post-limit and
    # the boundary treats the limited rows as opaque.
    if select.limit is not None:
        inner_node.ordering = built.order_by
        inner_node.rebuild_cache()
        inner_node = _interpose_limit_node(inner_node, select, inner_env, depth)

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
    # Usable (non-hidden) outputs only: a hidden inner column (a grain key the
    # inner FINAL masked) has no source-map entry in the rendered CTE, so a
    # boundary input mapped to it dangles at render time.
    produced = {o.address: o for o in inner_node.usable_outputs}
    # A coalescing scoped join (`full`/`subset`/`union`) collapses the join-key
    # group (`a.aid = b.bid`) onto ONE canonical body column, leaving the
    # authored side only as a pseudonym of that canonical — so a demanded
    # handle's content (`a.aid`) has no produced entry of its own. Re-expose
    # the content on the inner producer (sourced off the canonical column via
    # the pseudonym) so the boundary can materialize the handle; mirrors v3
    # `_expose_coalesced_key_sources`.
    produced_by_pseudonym: dict[str, BuildConcept] = {}
    for out in produced.values():
        for pseudonym in out.pseudonyms:
            produced_by_pseudonym.setdefault(pseudonym, out)
    coalesced_contents: list[BuildConcept] = []
    for demanded_handle in outputs:
        dlineage = demanded_handle.lineage
        if not isinstance(dlineage, BuildRowsetItem):
            continue
        content = dlineage.content
        if content.address in produced or content.address not in produced_by_pseudonym:
            continue
        if content.address not in {c.address for c in coalesced_contents}:
            coalesced_contents.append(content)
    if coalesced_contents:
        inner_node.add_output_concepts(coalesced_contents)
        produced.update({c.address: c for c in coalesced_contents})
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

    # A plain rowset's GRAIN keys (e.g. `id`) are the shared join keys back to the
    # outer query and sibling rowsets, but they're plain roots — not
    # `BuildRowsetItem` handles — so the loop above skips them. Expose any the inner
    # producer supplies so they enter the boundary grain below and the FINAL merge
    # joins on them; otherwise a shared-key rowset with no `merge into` pseudonym
    # degrades to a `1=1` cross product -> cartesian (test_rowset_alias_name_
    # collision: 3 rows -> 27). Mirrors v3 `gen_rowset_node` carrying the inner
    # select's demanded output_components (`additional_relevant`). Multiselect
    # grains are align concepts handled separately below, so scope to plain selects.
    #
    # Only for an UNFILTERED rowset: a WHERE/HAVING makes its key-set a proper
    # subset of the base domain, so advertising the key would let the cover step
    # satisfy the outer bare key FROM the filtered rowset and drop the unfiltered
    # source (rowset_outer_addition: odd orders must survive NULL-extended via a
    # LEFT add, not be inner-joined away). A filtered rowset stays a separate
    # outer-added contributor.
    #
    # Plain ROW-projection rowsets only: an AGGREGATE rowset's grain is its
    # grouping key, which the producer renames to the handle (`dept_totals` groups
    # by `dept` and renders it as `_dept_totals_department`), so the raw grain key
    # isn't a separately renderable column — exposing it makes assembly demand a
    # `local.dept` no CTE projects (query-structure syntax example). A plain
    # projection's grain key (`id`) IS a passthrough column, safe to expose.
    if (
        isinstance(built, BuildSelectLineage)
        and built.where_clause is None
        and built.having_clause is None
        and not any(
            o.derivation == Derivation.AGGREGATE for o in built.output_components
        )
    ):
        handle_addrs = {h.address for h in handles}
        for key_addr in built.grain.components:
            if key_addr in produced and key_addr not in handle_addrs:
                key_concept = produced[key_addr]
                handles.append(key_concept)
                inputs.append(key_concept)

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

    # OBLIGATION (v3 `_local_exposure_obligations`): a presence probe over one
    # of this rowset's handles must be computed HERE, pre-merge — post-merge
    # the member reads as the fused group coalesce, never NULL. The probe is a
    # BASIC over the handle, so it renders inline in the boundary SELECT once
    # its member handle is materialized; expose the member as a hidden output
    # when the outer query didn't demand it directly.
    handle_addrs = {h.address for h in handles}
    for probe in outputs:
        if probe.address in handle_addrs or not is_presence_probe(probe.address):
            continue
        member_addr = probe_member_address(probe.address, environment)
        if member_addr is None or member_addr not in derived:
            continue
        member_handle = environment.concepts.get(member_addr)
        if member_handle is None:
            continue
        if member_addr not in handle_addrs and isinstance(
            member_handle.lineage, BuildRowsetItem
        ):
            if member_handle.lineage.content.address not in produced:
                continue
            handles.append(member_handle)
            inputs.append(produced[member_handle.lineage.content.address])
            handle_addrs.add(member_addr)
            hidden.add(member_addr)
        handles.append(probe)
        handle_addrs.add(probe.address)

    # OBLIGATION: a DERIVED relation member (`union join cur.wk + 53 = fut.wk`)
    # is a BASIC over THIS boundary's handles with no producer group of its own
    # — materialize it here so the completion merge can pair it with its
    # authored mate instead of cross-joining ON 1=1 (v3 bundles the derived key
    # into the rowset body select). OUTER (full/union) relations only: a
    # directional (left/subset) relation resolves through the scoped-merge
    # collapse, which substitutes the derived key into the other side's grain
    # and computes it in a downstream projection — materializing it here too
    # displaces that path and widens the authored LEFT to FULL
    # (scoped_derived_rowset_join_matrix). A directional relation's SUBSET
    # SOURCE member is the exception: it pairs by its own value (`left join
    # ta.s = nb.s and nb.w = ta.w + 52`, mixed anchors composing to FULL), so
    # its own side still materializes it. Side identity is structural: every
    # lineage arg must already be a handle of this boundary.
    outer_relation_keys = environment.domain_graph.outer_relation_keys()
    subset_source_members = environment.domain_graph.subset_sources()
    for canonical, members in environment.scoped_join_key_groups.items():
        outer_relation = canonical in outer_relation_keys
        for member_addr in {canonical, *members}:
            if not outer_relation and member_addr not in subset_source_members:
                continue
            if member_addr in handle_addrs or is_presence_probe(member_addr):
                continue
            member_concept = environment.concepts.get(member_addr)
            if (
                member_concept is None
                # address mismatch = the scoped-merge collapse substituted this
                # member to the other side's derivation; that path owns it
                or member_concept.address != member_addr
                or member_concept.lineage is None
                or isinstance(member_concept.lineage, BuildRowsetItem)
            ):
                continue
            arg_addrs = {a.address for a in member_concept.concept_arguments}
            if not arg_addrs:
                continue
            # An arg the outer query never demanded is not a handle yet — but if
            # it IS one of this rowset's own handles the boundary can still
            # materialize it (hidden), which is the only way the derived key
            # gets a producer at all (`subset join ftr.ws - 53 = cur.ws` never
            # projects `ftr.ws`; without this the completion merge has no axis
            # and cross-joins ON 1=1).
            pending: list[tuple[BuildConcept, BuildConcept]] = []
            for arg_addr in sorted(arg_addrs - handle_addrs):
                arg_handle = environment.concepts.get(arg_addr)
                if (
                    arg_addr not in derived
                    or arg_handle is None
                    or not isinstance(arg_handle.lineage, BuildRowsetItem)
                    or arg_handle.lineage.content.address not in produced
                ):
                    break
                pending.append(
                    (arg_handle, produced[arg_handle.lineage.content.address])
                )
            else:
                for arg_handle, arg_input in pending:
                    handles.append(arg_handle)
                    inputs.append(arg_input)
                    handle_addrs.add(arg_handle.address)
                handles.append(member_concept)
                handle_addrs.add(member_addr)

    # A handle that is a declared-subset SOURCE (`subset join rs.k = anchor.k`)
    # spans only the subset side's domain: mark it partial so join resolution
    # anchors the complete side and LEFT-joins this boundary (v3's
    # `scoped_partial` in `_collect_advertised_outputs`) instead of INNER-
    # narrowing the anchor to the intersection. Restricted to relations whose
    # OTHER members are also rowset handles (the rowset-pair matrix): a mixed
    # root↔rowset relation resolves through binding substitution, and marking
    # the rowset side there re-routed a boundary measure onto the root scan
    # (conflicting-filter year-over-year join re-derived `cnt_2000` row-wise).
    subset_sources = environment.domain_graph.subset_sources()

    def _mates_all_rowset(address: str) -> bool:
        mates: set[str] = set()
        for canonical, members in environment.scoped_join_key_groups.items():
            if address in members:
                mates |= (members | {canonical}) - {address}
        mate_concepts = [environment.concepts.get(m) for m in mates]
        return bool(mate_concepts) and all(
            m is not None and m.derivation == Derivation.ROWSET for m in mate_concepts
        )

    scoped_partial = [
        h
        for h in handles
        if h.address in subset_sources
        and isinstance(h.lineage, BuildRowsetItem)
        and _mates_all_rowset(h.address)
    ]
    # nullability propagates by ADDRESS between nodes, but a rowset handle is a
    # new address wrapping its body content — map through the BuildRowsetItem
    # content (and pseudonyms) so a `?` column's nullability survives the
    # boundary (else a NULL rowset join key stops matching null-safely). Mirrors
    # v3 `_build_translation_node`, but restricted to KEY-like handles (the
    # boundary's grain and scoped-relation members): those are the handles that
    # become join keys and need the null-safe pairing. A nullable non-key
    # property handle stays unstamped — v4's FINAL re-pairing join would
    # otherwise render it `is not distinct from` alongside the keys that
    # already pair the rows (q29's item_desc, a hash-join-defeating no-op).
    base_nullable: set[str] = set()
    for c in inner_node.nullable_concepts:
        base_nullable.add(c.address)
        base_nullable.update(c.pseudonyms)
    boundary_grain = BuildGrain.from_concepts(
        [
            h
            for h in handles
            if h.address not in hidden and not is_presence_probe(h.address)
        ]
    )
    # A multiselect/union boundary's rows are at the FULL align grain even
    # when only a subset of its outputs is demanded (`subset join x =
    # all_combos.b` never projects `ch`): the projection does NOT dedup, so
    # stamping the demanded subset as the grain overclaims uniqueness and a
    # downstream aggregate elides its GROUP BY over the per-arm fan
    # (union_reproject direct-RHS: sum fanned to two rows of 10 instead of
    # re-aggregating to 20). Stamp the grain over every align output instead.
    if isinstance(built, BuildMultiSelectLineage):
        full_handles = [
            handle_concept
            for addr in sorted(derived)
            if (handle_concept := environment.concepts.get(addr)) is not None
        ]
        if full_handles:
            boundary_grain = BuildGrain.from_concepts(full_handles)
    key_like = set(boundary_grain.components) | {
        addr
        for canonical, members in environment.scoped_join_key_groups.items()
        for addr in (canonical, *members)
    }
    nullable_handles = [
        h
        for h in handles
        if (h.address in key_like or (set(h.pseudonyms) & key_like))
        and (
            h.address in base_nullable
            or (set(h.pseudonyms) & base_nullable)
            or (
                isinstance(h.lineage, BuildRowsetItem)
                and (
                    h.lineage.content.address in base_nullable
                    or (set(h.lineage.content.pseudonyms) & base_nullable)
                )
            )
        )
    ]
    boundary: StrategyNode = SelectNode(
        output_concepts=handles,
        input_concepts=inputs,
        parents=[inner_node],
        environment=inner_env,
        # Grain over the outer handles (mirrors v3 `gen_rowset_node`): lets the
        # FINAL merge join two rowsets on their shared/pseudonym grain key
        # instead of cross-joining when the boundary exposes no grain. Probes
        # are per-key presence markers, not part of the boundary's row grain.
        grain=boundary_grain,
        hidden_concepts=hidden,
        partial_concepts=scoped_partial,
        nullable_concepts=nullable_handles,
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
    partial_covered = bool(
        where
        and ds.non_partial_for
        and condition_implies(where.conditional, ds.non_partial_for.conditional)
    )
    # When the persisted population is EXACTLY the query's desired rows (the query
    # `where` and the datasource's `non_partial_for` are mutually implied), the
    # condition is already applied by materialization -- the datasource needs no
    # column to re-express it. A `persist ... from select derived where cat = 1`
    # drops the filter key (only the derived column is stored), so it can't pass
    # `_conditions_supported`, but its baked-in population already satisfies a
    # `... where cat = 1` query. Otherwise the datasource must express the
    # (residual) condition itself.
    population_is_exact = bool(
        partial_covered
        and where
        and ds.non_partial_for
        and condition_implies(ds.non_partial_for.conditional, where.conditional)
    )
    if not population_is_exact and not _conditions_supported(
        ds, where, environment.concepts
    ):
        return False
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
