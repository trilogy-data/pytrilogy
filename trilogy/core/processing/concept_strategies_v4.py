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

The stage implementations live in `v4_helper/`; the per-derivation node
builders (including the nested-select constructs — rowset, multiselect, union
TVF) live in `v4_node_generators/`. This file is just the public API, the
materialized-root pre-pass, and the History cache wiring.
"""

from trilogy.constants import logger
from trilogy.core import graph as nx
from trilogy.core.enums import Derivation
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildFunction,
    BuildGrain,
    BuildMultiSelectLineage,
    BuildUnionSelectLineage,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.aggregate_rollup import (
    _conditions_supported,
    _datasource_has_matching_additive_aggregate,
    _is_additive_aggregate,
    get_additive_rollup_concepts,
)
from trilogy.core.processing.condition_utility import (
    combine_where_clauses,
    condition_implies,
)
from trilogy.core.processing.discovery_utility import (
    LOGGER_PREFIX,
    depth_to_prefix,
    raise_if_filter_disconnected,
)
from trilogy.core.processing.nodes import History, StrategyNode
from trilogy.core.processing.v4_helper import (
    FINAL_NODE_ID,
    ROW_SHAPE_BARRIER_DERIVATIONS,
    BuildInfo,
    V4History,
    build_concept_graph,
    build_group_graph,
    build_strategy_node,
)
from trilogy.core.processing.v4_node_generators.multiselect import gen_multiselect
from trilogy.core.processing.v4_node_generators.union_select import gen_union_select

__all__ = [
    "FINAL_NODE_ID",
    "ROW_SHAPE_BARRIER_DERIVATIONS",
    "BuildInfo",
    "History",
    "V4History",
    "append_existence_check",
    "search_concepts",
]


def append_existence_check(
    node: StrategyNode,
    environment: BuildEnvironment,
    graph: ReferenceGraph,
    where: BuildWhereClause,
    history: V4History,
    conditions: BuildWhereClause | None = None,
) -> None:
    """Source each `x in (<set>)` RHS of `where` as an independent subselect and
    wire it onto `node` as an existence parent. Idempotent: a set already among
    the node's inputs/existence concepts is skipped.

    Used for HAVING-derived membership, which is planned after the output tree
    (a WHERE membership gets its existence edges inside the concept graph).

    Each subselect is gated for connectivity first. It is an independent
    resolution scope, so its concepts (with any FILTER's hidden condition
    concepts surfaced) must be connected on their own — otherwise the feeder
    plans as a cross join and the membership silently filters nothing. The gate
    has to run up front: the planner will happily assemble such a cross join
    rather than fail, so there is no unresolvable result to diagnose after.

    Rowset islanding stays ON here, unlike the WHERE-membership pre-gate in
    `query_processor`. A HAVING membership filters the statement's OUTPUTS, so
    its subselect reads rowset outputs (`r.wk`) across the boundary, where the
    rowset is genuinely opaque and islanding IS the diagnostic
    (`test_q02_filter_rowset_output_by_out_of_grain_concept_clean_error`). A
    WHERE-scope RHS is resolved against the base model instead, where a key that
    happens to be a rowset output is a legitimate join-back and islanding
    false-positives."""
    if not where.existence_arguments:
        return
    already_sourced = {c.address for c in node.input_concepts} | {
        c.address for c in node.existence_concepts
    }
    for subselect in where.existence_arguments:
        if not subselect:
            continue
        if all(x.address in already_sourced for x in subselect):
            logger.info(
                f"{LOGGER_PREFIX} existence clause inputs already found {[str(c) for c in subselect]}"
            )
            continue
        logger.info(
            f"{LOGGER_PREFIX} fetching existence clause inputs {[str(c) for c in subselect]}"
        )
        raise_if_filter_disconnected(list(subselect), environment, graph)
        # A HAVING-derived membership subselect (`conditions` set) is this
        # query's own post-aggregation semijoin: the query WHERE must be
        # pushed pre-aggregate into its aggregate inputs, exactly as it is on
        # the output path — else the membership recomputes the aggregate over
        # the unfiltered universe and its value never matches the filtered
        # output (q44 silent-empty). A user `x in (select ...)` RHS is an
        # independent set (no conditions) and stays unfiltered.
        parent = search_concepts(
            mandatory_list=[*subselect],
            history=history,
            environment=environment,
            depth=0,
            g=graph,
            conditions=[conditions] if conditions else [],
        ).strategy_node
        assert parent, "Could not resolve existence clause"
        node.add_parents([parent])
        logger.info(f"{LOGGER_PREFIX} found {[str(c) for c in subselect]}")
        node.add_existence_concepts([*subselect])


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
    re-aggregates it to the target grain (`sum(finer.col)`).

    Condition row-args are candidates too (EXACT branch only): a WHERE over a
    materialized aggregate (`where customer_revenue > 100` beside its summary
    table) reads the table and filters it, instead of re-deriving from base —
    and without the mark the atom's arg is invisible to the root cluster's
    satisfiability check, silently dropping the WHERE. The rollup branch is
    mandatory-only: filtering a finer scan by a rolled-up value pre-aggregation
    would filter the wrong rows."""
    if not mandatory_list:
        return frozenset()
    target_grain = BuildGrain.from_concepts(mandatory_list)
    where = combine_where_clauses(conditions)
    datasources = [
        ds for ds in environment.datasources.values() if isinstance(ds, BuildDatasource)
    ]
    mandatory_addresses = {c.address for c in mandatory_list}
    condition_args_by_address: dict[str, BuildConcept] = {}
    for clause in conditions:
        for arg in clause.row_arguments:
            if arg.address not in mandatory_addresses:
                condition_args_by_address.setdefault(arg.address, arg)
    out: set[str] = set()
    candidates = mandatory_list + list(condition_args_by_address.values())
    # An unbound bare KEY whose pseudonym origin recomposes it (`merge
    # composite_id_alt into composite_id`, alt <- concat(first, second)) is
    # sourced by substituting the origin during the graph walk — so the
    # origin's own args are lineage intermediates the walk must be able to
    # stop at. Without the mark, an arg that is bound-but-derived (`first <-
    # split(composite_id)`, also a physical column) walks its authored lineage
    # back into the unbound key and the cluster dead-ends (circular aliasing
    # inverse). Direct args only; each still passes the materializes + grain
    # gates below.
    ds_bound_addresses = {c.address for ds in datasources for c in ds.output_concepts}
    for concept in list(candidates):
        if concept.lineage is not None or concept.address in ds_bound_addresses:
            continue
        for pseudonym in (concept.address, *sorted(concept.pseudonyms)):
            origin = environment.alias_origin_lookup.get(pseudonym)
            if origin is None or not isinstance(origin.lineage, BuildFunction):
                continue
            for arg in origin.lineage.concept_arguments:
                if isinstance(arg, BuildConcept):
                    candidates.append(environment.concepts.get(arg.address) or arg)
    seen_candidates: set[str] = set()
    for concept in candidates:
        if concept.address in seen_candidates:
            continue
        seen_candidates.add(concept.address)
        condition_only = concept.address not in mandatory_addresses
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
            if not condition_only and any(
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
        if (
            exact
            or condition_only
            or not (is_aggregate and _is_additive_aggregate(concept))
        ):
            continue
        # ROLLUP: a finer-grain table binds the same aggregate expression and it
        # can be SUM-rolled up to the target grain. Neither address nor canonical
        # address can gate this: the finer instance is grain-pinned to a
        # different canonical, and an agent-authored alias (`sum(ss.price) as
        # total`) never shares an address with the table's bound column. The gate
        # is therefore the *lineage signature* — (operator, sorted canonical arg
        # addresses) — which is exactly what the rollup matcher below re-checks.
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
            if not _datasource_has_matching_additive_aggregate(ds, concept):
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
    g: ReferenceGraph,
    history: V4History,
    conditions: list[BuildWhereClause],
    materialized_roots: frozenset[str],
    complete_partials: bool,
    staged_conditions: list[BuildWhereClause] | None = None,
) -> BuildInfo:
    concept_graph, concept_attrs, concept_edges = build_concept_graph(
        mandatory_list,
        environment,
        conditions,
        materialized_roots,
        staged_conditions=staged_conditions,
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
        staged_conditions=staged_conditions,
    )
    # `build_strategy_node` scopes each group's extent routing on the shared
    # environment; a rowset body planned mid-build recurses through here, so
    # restore whatever the outer plan had rather than leaving it cleared.
    outer_extent_free = environment.extent_free_spans
    try:
        strategy_node = build_strategy_node(
            group_graph,
            group_edges,
            group_attrs,
            mandatory_list,
            environment,
            g,
            history,
            complete_partials=complete_partials,
            staged_conditions=staged_conditions,
        )
    finally:
        environment.extent_free_spans = outer_extent_free
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


def _own_build_of(
    mandatory_list: list[BuildConcept],
    lineage_type: type[BuildMultiSelectLineage],
) -> BuildConcept | None:
    """The demanded concept whose `lineage_type` build produces EVERY mandatory
    concept, if any.

    A request that merely REFERENCES a member beside outer derivations (an
    ORDER BY carrying a grouped-away union column next to aggregates over the
    rowset) is NOT the construct's own build — intercepting it would stamp the
    outer outputs onto the union/merge node itself. The graph path plans that
    case with the construct as a boundary group instead."""
    for concept in mandatory_list:
        lineage = concept.lineage
        if not isinstance(lineage, lineage_type):
            continue
        covered = lineage.derived_concepts | {
            c.address for c in lineage.output_components
        }
        if all(c.address in covered for c in mandatory_list):
            return concept
    return None


def _combined_build_info(node: StrategyNode | None) -> BuildInfo:
    """Wrap a node the arm combiners built directly. They plan no concept or
    group graph — each arm is its own sub-plan — so the diagnostic graphs the
    stages normally populate are empty."""
    return BuildInfo(
        concept_graph=nx.DiGraph(),
        group_graph=nx.DiGraph(),
        group_attrs={},
        strategy_node=node,
    )


def _search_concepts(
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    history: V4History,
    conditions: list[BuildWhereClause],
    complete_partials: bool,
    staged_conditions: list[BuildWhereClause] | None = None,
) -> BuildInfo:
    # A top-level multiselect (merge/align) isn't a single source graph — its
    # arms are independent sub-plans joined on the alignment concept. Resolve
    # each arm through v4 and stitch them, rather than trying to source both
    # arms' columns from one (unjoinable) root scan.
    # A relational union TVF is a column-positional row stack (UNION), not a
    # key-join. Its lineage subclasses BuildMultiSelectLineage, so check it
    # first and route to the union combiner.
    union_concept = _own_build_of(mandatory_list, BuildUnionSelectLineage)
    if union_concept is not None:
        return _combined_build_info(
            gen_union_select(
                union_concept,
                mandatory_list,
                environment,
                depth,
                g,
                history,
                conditions,
            )
        )
    ms_concept = _own_build_of(mandatory_list, BuildMultiSelectLineage)
    if ms_concept is not None:
        return _combined_build_info(
            gen_multiselect(
                ms_concept, mandatory_list, environment, depth, g, history, conditions
            )
        )
    # Prefer a precomputed/summary datasource for any demanded aggregate it
    # materializes at grain. If treating those as roots can't be sourced (the
    # summary doesn't combine with the rest of the query), fall back to the
    # derive-from-base plan: try the direct source first.
    materialized_roots = _materialized_root_addresses(
        mandatory_list, environment, conditions
    )
    info = _build_from_graph(
        mandatory_list,
        environment,
        g,
        history,
        conditions,
        materialized_roots,
        complete_partials,
        staged_conditions,
    )
    if materialized_roots and info.strategy_node is None:
        info = _build_from_graph(
            mandatory_list,
            environment,
            g,
            history,
            conditions,
            frozenset(),
            complete_partials,
            staged_conditions,
        )
    return info


def search_concepts(
    mandatory_list: list[BuildConcept],
    history: V4History,
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    conditions: list[BuildWhereClause] | None = None,
    complete_partials: bool = True,
    staged_conditions: list[BuildWhereClause] | None = None,
) -> BuildInfo:
    """Run the v4 planner against `mandatory_list` under `conditions`. Cached
    per `(mandatory_list, conditions)` via `history`.

    ``staged_conditions`` carries the statement's ordered `then where` stages
    (address-aligned with the combined `conditions` clause); earlier stages'
    row atoms become input filters on later stages' cross-row computations.

    The network search prices partial bindings per binding. ``complete_partials``
    controls whether requested partial keys are subsequently completed against
    their authoritative datasources."""
    conditions = conditions or []
    hist = history.get_build_history(
        search=mandatory_list,
        conditions=conditions,
        complete_partials=complete_partials,
        staged_conditions=staged_conditions,
    )
    if hist is not False:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Returning search node from "
            f"history ({'exists' if hist is not None else 'does not exist'}) for "
            f"{[c.address for c in mandatory_list]}"
        )
        assert isinstance(hist, BuildInfo)
        return hist

    result = _search_concepts(
        mandatory_list,
        environment,
        depth=depth,
        g=g,
        history=history,
        conditions=conditions,
        complete_partials=complete_partials,
        staged_conditions=staged_conditions,
    )
    # a node may be mutated after being cached; always store a copy
    history.build_to_history(
        mandatory_list,
        result.copy(),
        conditions=conditions,
        complete_partials=complete_partials,
        staged_conditions=staged_conditions,
    )
    return result
