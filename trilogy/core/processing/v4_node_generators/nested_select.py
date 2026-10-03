"""Planning one nested select: a rowset body, a merge arm, a union TVF arm.

Every nested select is a self-contained sub-query, so all three consumers need
the same sequence: build it in its own scope, gate connectivity, search its
outputs, then apply the post-aggregate HAVING and the body LIMIT that belong to
the select itself. Only what the consumer does with the resulting producer
differs: project it under rowset handles, FULL-join it to sibling arms, or
stack it. Keeping the sequence here is what stops the three from drifting apart.
"""

from dataclasses import dataclass, replace

from trilogy.constants import logger
from trilogy.core.domain_graph import EdgeScope
from trilogy.core.enums import JoinType
from trilogy.core.env_processor import generate_graph
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.author import MultiSelectLineage, SelectLineage
from trilogy.core.models.build import (
    BuildMultiSelectLineage,
    BuildRowsetLineage,
    BuildSelectLineage,
    BuildWhereClause,
    Factory,
    scope_tagged_joins,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.environment import Environment
from trilogy.core.processing.discovery_utility import (
    LOGGER_PREFIX,
    depth_to_prefix,
    raise_if_disconnected_for,
)
from trilogy.core.processing.nodes import BuildCaches, SelectNode, StrategyNode
from trilogy.core.processing.statement_scope import scope_statement
from trilogy.core.processing.v4_helper.history import NestedBuildKey, V4History

from .common import search_parent
from .condition_sources import resolve_and_inject_condition


def _inherited_joins(
    scoped_joins: list[tuple[str, str, JoinType]],
    environment: Environment,
    derived_concepts: list[str],
) -> list[tuple[str, str, JoinType]]:
    """The enclosing resolution's joins a nested select builds under: only the
    environment's global merges. A statement's own joins relate ITS concepts
    and never reach into a nested scope: a body built under its reader's join
    would read that reader as its own source.
    A global merge naming one of a rowset's own derived concepts is dropped
    too: the body would canonicalize its output onto the merge group and source
    it back through itself."""
    derived = set(derived_concepts)
    return [
        (s, t, jt)
        for (s, t, jt), scope in scope_tagged_joins(scoped_joins, environment)
        if scope is EdgeScope.GLOBAL and s not in derived and t not in derived
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
    env (so the graph bridges the two datasources); otherwise the body builds
    with no join, its datasources come back as separate components, and the
    read-back raises a misleading DisconnectedConceptsException for a join that is
    in fact present inside the rowset.

    Of the enclosing resolution's joins only global merges apply here (see
    `_inherited_joins`); ``exclude_derived`` carries a rowset body's own derived
    concepts, which no inherited merge may name."""
    author_env = history.base_environment
    caches = history.build_caches
    nested_scoped = select.scoped_joins if isinstance(select, SelectLineage) else []
    outer_scoped = _inherited_joins(
        caches.scoped_joins, author_env, exclude_derived or []
    )
    scoped_joins = outer_scoped + [j for j in nested_scoped if j not in outer_scoped]
    # A rowset body is built for its witness and again for its plan; both
    # read one build env. A hit skips the pseudonym sync: the author env gains
    # no concept while a statement resolves, and planning mutates no build-env
    # state but `span_scope`, which it restores.
    key: NestedBuildKey = (
        id(select),
        tuple(exclude_derived or ()),
        tuple(scoped_joins),
    )
    cached = history.nested_builds.get(key)
    if cached is not None:
        return cached[1]
    caches.sync_pseudonym_map(author_env)
    # The shared build caches are keyed on address/grain identity alone, which
    # is only correct while every build in the resolution applies the SAME
    # scoped joins; a join changes what an address builds to (canonical
    # collapse + pseudonym stamping). When this body carries its OWN joins the
    # outer resolution never saw, entries the outer scope cached are wrong
    # here (an outer-built join key comes back with no pseudonym link to its
    # body mate, so the inner aggregate detaches from its grouping key and
    # FINAL cross-joins ON 1=1); build this scope with fresh caches. The
    # converse (statement joins not inherited) keeps the concept caches, as
    # boundary pairing reads the outer join's pseudonym stamps off them, but
    # not the datasources: built under the statement's joins, their columns
    # collapse onto its merge group and the body reads its own consumer.
    if any(j not in caches.scoped_joins for j in scoped_joins):
        caches = BuildCaches(
            pseudonym_map=caches.pseudonym_map,
            pseudonym_concept_count=caches.pseudonym_concept_count,
            scoped_joins=scoped_joins,
        )
    elif set(scoped_joins) != set(caches.scoped_joins):
        caches = replace(caches, datasource_build_cache={}, scoped_joins=scoped_joins)
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
    # overlay actually changes. `materialize_for_select` is the reference
    # spelling the delta must stay byte-equivalent to.
    baseline = author_env.shared_baseline(
        caches.env_baselines,
        build_cache=caches.build_cache,
        pseudonym_map=factory.pseudonym_map,
        grain_build_cache=caches.grain_build_cache,
        canonical_build_cache=caches.canonical_build_cache,
        datasource_build_cache=caches.datasource_build_cache,
        scoped_joins=scoped_joins,
    )
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
    # This select is its own plan: its WHERE completes `~` bindings and rules
    # out partitions over ITS references, not the enclosing statement's.
    scope_statement(build_env, select, author_env, built)
    result = (built, build_env, built.where_clause)
    history.nested_builds[key] = (select, result)
    return result


def _nested_graph(env: BuildEnvironment, history: V4History) -> ReferenceGraph:
    """The env's graph, generated once: a body built for its witness and its
    plan shares one build env."""
    cached = history.nested_graphs.get(id(env))
    if cached is None:
        cached = history.nested_graphs[id(env)] = (env, generate_graph(env))
    return cached[1]


def plan_nested_select(
    select: SelectLineage | MultiSelectLineage,
    history: V4History,
    depth: int,
    label: str,
    exclude_derived: list[str] | None = None,
    hide_from_connectivity: list[str] | None = None,
    owned_spans: frozenset[str] = frozenset(),
    rowset: BuildRowsetLineage | None = None,
) -> NestedPlan | None:
    """Plan one nested select to a producer node. See the module docstring.

    ``owned_spans`` are the spans of this select's regions whose rows the
    consumer holds itself (a rowset read beside its own region domain): the
    select, and every plan under it, is built not to extend them."""
    # `exclude_derived` also filters this scope's scoped joins, so the
    # connectivity set is tracked separately; widening the join filter to the
    # inherited set would drop joins a body legitimately carries.
    inherited = history.nested_exclusions
    hidden = inherited | frozenset(hide_from_connectivity or exclude_derived or ())
    built, env, where = build_nested_select(select, history, exclude_derived)
    graph = _nested_graph(env, history)

    # The nested select resolves on its own; if its required concepts span
    # unconnected models (a grain-only `by` edge does NOT bridge them), surface
    # the typed subgraph error rather than silently cross-joining inside it.
    # `hidden` keeps the enclosing construct's own outputs out of that judgement.
    raise_if_disconnected_for(
        list(built.output_components),
        where,
        env,
        graph,
        excluded_addresses=hidden,
        scope=rowset,
    )

    # A nested select's own `then where` stages ride its built lineage; thread
    # them so a staged rowset body / multiselect arm keeps staged semantics.
    staged = (
        built.where_clauses or None if isinstance(built, BuildSelectLineage) else None
    )
    # Constructs nested inside this select inherit the hidden set. The owned
    # spans are this select's own: a construct nested inside it starts over.
    history.nested_exclusions = hidden
    outer_owned, history.owned_spans = history.owned_spans, owned_spans
    # The hidden set covers the body search alone; the owned spans cover the
    # HAVING sub-plan too, whose connectivity check must see this select's
    # own outputs.
    try:
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

        # HAVING is a post-aggregate filter over this select's own producer;
        # the top-level `_get_query_node_v4` wrap only sees the outer query.
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
    finally:
        history.owned_spans = outer_owned

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
