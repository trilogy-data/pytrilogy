"""Planning one nested select — a rowset body, a merge arm, a union TVF arm.

Every nested select is a self-contained sub-query, so all three consumers need
the same sequence: build it in its own scope, gate connectivity, search its
outputs, then apply the post-aggregate HAVING and the body LIMIT that belong to
the select itself. Only what the consumer does with the resulting producer
differs — project it under rowset handles, FULL-join it to sibling arms, or
stack it. Keeping the sequence here is what stops the three from drifting apart.
"""

from dataclasses import dataclass

from trilogy.constants import logger
from trilogy.core.enums import JoinType
from trilogy.core.env_processor import generate_graph
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.author import MultiSelectLineage, SelectLineage
from trilogy.core.models.build import (
    BuildMultiSelectLineage,
    BuildSelectLineage,
    BuildWhereClause,
    Factory,
    get_canonical_pseudonyms,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.discovery_utility import (
    LOGGER_PREFIX,
    depth_to_prefix,
    raise_if_disconnected_for,
)
from trilogy.core.processing.nodes import BuildCaches, SelectNode, StrategyNode
from trilogy.core.processing.v4_helper.history import V4History

from .common import search_parent
from .condition_sources import resolve_and_inject_condition


def _scoped_joins_for_rowset(
    scoped_joins: list[tuple[str, str, JoinType]],
    derived_concepts: list[str],
) -> list[tuple[str, str, JoinType]]:
    """A query-scoped `join`/`merge` relates the rowset's *output* to an outer
    concept; it must not be applied inside the rowset's own (independent-scope)
    build. Such a join collapses the outer concept onto the rowset output via
    the merge map/pseudonym — so if the rowset's WHERE references that outer
    concept (e.g. a membership existence feeder), sourcing the feeder redirects
    back to the rowset's own output and the rowset depends on itself (infinite
    recursion). Drop any join referencing a concept this rowset derives."""
    derived = set(derived_concepts)
    return [
        (s, t, jt)
        for (s, t, jt) in scoped_joins
        if s not in derived and t not in derived
    ]


def _interpose_limit_node(
    base_node: StrategyNode,
    select: SelectLineage | MultiSelectLineage,
    environment: BuildEnvironment,
    depth: int,
) -> StrategyNode:
    """Materialize the body's `limit` (with its ORDER BY) as a dedicated
    passthrough node BETWEEN the body and the translation wrapper.

    The limit must not live on the translation node itself: discovery applies
    outer WHEREs onto that node (they would render pre-limit, changing which
    rows fill the limit), and when the outer statement reuses it as the query
    root its ordering is overwritten by the statement's and the root renders
    without a CTE-level limit. A dedicated node keeps LIMIT+ORDER BY in their
    own CTE; everything downstream is post-limit by construction, and the
    optimizer treats the limited CTE as an opaque boundary."""
    if select.limit is None:
        return base_node
    passthrough = [
        x
        for x in base_node.output_concepts
        if x.address not in base_node.hidden_concepts
    ]
    limit_node = SelectNode(
        input_concepts=passthrough,
        output_concepts=passthrough,
        environment=environment,
        parents=[base_node],
        depth=depth,
        partial_concepts=list(base_node.partial_concepts),
        nullable_concepts=list(base_node.nullable_concepts),
    )
    limit_node.limit = select.limit
    # the ORDER BY the limit selects under was built onto the body root by
    # get_query_node; hoist it here so both render in one SELECT (an inner
    # ORDER BY without a limit carries no semantics and just costs a sort)
    limit_node.ordering = base_node.ordering
    base_node.ordering = None
    base_node.rebuild_cache()
    limit_node.rebuild_cache()
    return limit_node


@dataclass
class NestedPlan:
    """A nested select planned to a producer node, with the scope it resolved in."""

    node: StrategyNode
    built: BuildSelectLineage | BuildMultiSelectLineage
    environment: BuildEnvironment
    graph: ReferenceGraph


def build_nested_select(
    select: SelectLineage | MultiSelectLineage,
    history: V4History,
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
    in fact present inside the rowset.

    ``exclude_derived`` carries a rowset body's own derived concepts: an OUTER
    query-scoped join referencing them (``subset join a.store = b.store``)
    relates this rowset's output to its sibling and must not be applied inside
    the body's independent scope (see `_scoped_joins_for_rowset`) — the body
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
    # Materialized as baseline + overlay delta: the context-free build of the
    # whole environment under these scoped joins is computed once per
    # resolution (per join set) and each arm replays only the units its own
    # overlay actually changes — measured 0-10 of ~1500 on the nested-heavy
    # corpus queries. `materialize_for_select` is the reference spelling the
    # delta must stay byte-equivalent to (test_nested_env_delta.py).
    baseline_key = author_env.materialize_join_key(scoped_joins)
    baseline = caches.env_baselines.get(baseline_key)
    if baseline is None:
        baseline = author_env.materialize_baseline(
            build_cache=caches.build_cache,
            pseudonym_map=factory.pseudonym_map,
            grain_build_cache=caches.grain_build_cache,
            canonical_build_cache=caches.canonical_build_cache,
            datasource_build_cache=caches.datasource_build_cache,
            scoped_joins=scoped_joins,
        )
        caches.env_baselines[baseline_key] = baseline
    build_env = author_env.materialize_delta(
        baseline,
        built.local_concepts,
        build_cache=caches.build_cache,
        pseudonym_map=factory.pseudonym_map,
        grain_build_cache=caches.grain_build_cache,
        canonical_build_cache=caches.canonical_build_cache,
        datasource_build_cache=caches.datasource_build_cache,
        scoped_joins=scoped_joins,
    )
    return built, build_env, built.where_clause


def plan_nested_select(
    select: SelectLineage | MultiSelectLineage,
    history: V4History,
    depth: int,
    label: str,
    exclude_derived: list[str] | None = None,
    hide_from_connectivity: list[str] | None = None,
) -> NestedPlan | None:
    """Plan one nested select to a producer node. See the module docstring."""
    # `exclude_derived` also filters this scope's scoped joins, so the
    # connectivity set is tracked separately -- widening the join filter to the
    # inherited set would drop joins a body legitimately carries.
    inherited = history.nested_exclusions
    hidden = inherited | frozenset(hide_from_connectivity or exclude_derived or ())
    built, env, where = build_nested_select(select, history, exclude_derived)
    graph = generate_graph(env)

    # The nested select resolves on its own; if its required concepts span
    # unconnected models (a grain-only `by` edge does NOT bridge them), surface
    # the typed subgraph error rather than silently cross-joining inside it.
    # `hidden` keeps the enclosing construct's own outputs out of that judgement.
    raise_if_disconnected_for(
        list(built.output_components),
        where,
        env,
        graph,
        # v4 pre-gate: see query_processor._raise_if_disconnected.
        island_rowsets=False,
        excluded_addresses=hidden,
    )

    # A nested select's own `then where` stages ride its built lineage; thread
    # them so a staged rowset body / multiselect arm keeps staged semantics.
    staged = (
        built.where_clauses
        if isinstance(built, BuildSelectLineage) and len(built.where_clauses) > 1
        else None
    )
    # Constructs nested inside this select inherit the hidden set.
    history.nested_exclusions = hidden
    try:
        node = search_parent(
            list(built.output_components),
            env,
            history,
            graph,
            depth=depth + 1,
            conditions=[where] if where else [],
            staged_conditions=staged,
        )
    finally:
        history.nested_exclusions = inherited
    if node is None:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} {label} "
            f"{[c.address for c in built.output_components]} did not resolve"
        )
        return None

    # HAVING is a post-aggregate filter over this select's own producer; the
    # top-level `_get_query_node_v4` wrap only sees the outer query.
    having = built.having_clause
    if having is not None:
        node = resolve_and_inject_condition(
            node,
            having,
            list(built.output_components),
            environment=env,
            graph=graph,
            history=history,
            depth=depth,
            partial_concepts=list(node.partial_concepts),
        )

    # The body's LIMIT (with the ORDER BY it selects under) defines its row set;
    # materialize it as a dedicated node so outer filters stay post-limit and
    # consumers treat the limited rows as opaque.
    if select.limit is not None:
        node.ordering = built.order_by
        # `resolve`, not `rebuild_cache`: the ordering set here is hoisted onto
        # the limit node and this node is re-resolved without it, so all a
        # rebuild would buy is the nullability sync the limit node reads.
        node.resolve()
        node = _interpose_limit_node(node, select, env, depth)

    return NestedPlan(node=node, built=built, environment=env, graph=graph)


def plan_align_arms(
    lineage: BuildMultiSelectLineage,
    history: V4History,
    depth: int,
    label: str,
) -> list[NestedPlan] | None:
    """Plan every arm of a merge/union construct. None if any arm fails.

    An arm must not reach connectivity through the align outputs it feeds, for
    the same reason a rowset body must not reach through its handles."""
    align_outputs = [item.aligned_concept for item in lineage.align.items]
    plans: list[NestedPlan] = []
    for arm in lineage.selects:
        plan = plan_nested_select(
            arm, history, depth, label, hide_from_connectivity=align_outputs
        )
        if plan is None:
            return None
        plans.append(plan)
    return plans
