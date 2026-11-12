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
builders (including the nested-select constructs: rowset, multiselect, union
TVF) live in `v4_node_generators/`. This file is just the public API, the
materialized-root pre-pass, and the V4History cache wiring.
"""

import logging
from collections.abc import Collection, Sequence
from contextlib import nullcontext

from trilogy.constants import logger
from trilogy.core import graph as nx
from trilogy.core.enums import Derivation, Granularity
from trilogy.core.exceptions import UnbuiltGroupException
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildDatasource,
    BuildFunction,
    BuildGrain,
    BuildMultiSelectLineage,
    BuildUnionDatasource,
    BuildUnionSelectLineage,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment, SpanScope
from trilogy.core.processing import plan_trace
from trilogy.core.processing.aggregate_rollup import (
    _concepts_reachable,
    _conditions_supported,
    _datasource_has_matching_rollup_aggregate,
    _is_rollable_aggregate,
    at_own_grain,
    coarse_gate,
    filter_finer_row_args,
    holds_every_kept_row,
    joinable_property,
    keyed_aggregate,
    source_rollup_concepts,
    sourced_arguments,
    total_gate_beside_rows,
)
from trilogy.core.processing.condition_utility import (
    combine_where_clauses,
    condition_implies,
    is_scalar_condition,
)
from trilogy.core.processing.discovery_utility import (
    LOGGER_PREFIX,
    depth_to_prefix,
    raise_if_filter_disconnected,
)
from trilogy.core.processing.node_generators.select_node import root_is_unsourced
from trilogy.core.processing.nodes import StrategyNode
from trilogy.core.processing.relation_rollup import (
    relation_rollup_concepts,
    relation_targets,
)
from trilogy.core.processing.utility import walk_lineage
from trilogy.core.processing.v4_helper import (
    FINAL_NODE_ID,
    ROW_SHAPE_BARRIER_DERIVATIONS,
    BuildInfo,
    V4History,
    build_concept_graph,
    build_group_graph,
    build_strategy_node,
)
from trilogy.core.processing.v4_helper.concept_graph import (
    derived_key,
    upstream_concepts,
)
from trilogy.core.processing.v4_helper.functional_dependency import (
    build_fd_closure,
    build_fd_determines,
)
from trilogy.core.processing.v4_helper.region_domains import undemanded_spans
from trilogy.core.processing.v4_helper.strategy_builder import under_span_scope
from trilogy.core.processing.v4_node_generators.multiselect import gen_multiselect
from trilogy.core.processing.v4_node_generators.rowset_witness import (
    statement_keyspace,
)
from trilogy.core.processing.v4_node_generators.union_select import gen_union_select

__all__ = [
    "FINAL_NODE_ID",
    "ROW_SHAPE_BARRIER_DERIVATIONS",
    "BuildInfo",
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
    concepts surfaced) must be connected on their own; otherwise the feeder
    plans as a cross join and the membership silently filters nothing. The gate
    has to run up front: the planner will happily assemble such a cross join
    rather than fail, so there is no unresolvable result to diagnose after.

    Rowset islanding stays ON here, unlike the WHERE-membership pre-gate in
    `query_processor`. A HAVING membership filters the statement's OUTPUTS, so
    its subselect reads rowset outputs across the boundary, where the rowset
    is genuinely opaque and islanding IS the diagnostic. A WHERE-scope RHS is
    resolved against the base model instead, where a key that happens to be a
    rowset output is a legitimate join-back and islanding false-positives."""
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
        # the output path, else the membership recomputes the aggregate over
        # the unfiltered universe and its value never matches the filtered
        # output. A user `x in (select ...)` RHS is an independent set (no
        # conditions) and stays unfiltered.
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
    canonical address matches (name-independent: a differently-named column with
    the same underlying expression, `sum(x)` vs a bound `total`, satisfies it)
    and can express the query's row-narrowing conditions.

    Partialness is relative to the query, via the same `condition_implies` rule
    source-planning's `partial_is_full` uses. Two partial mechanisms:
    - Population (`complete where X`, `ds.non_partial_for`): the table holds only
      the X-subset of rows, so it's a complete source only when the query implies
      X. A `~key` merge column (`merge orid into ~orid_2`) is the degenerate case
      (intrinsically one row per key, missing values that never appear as a key,
      with no `non_partial_for` to recover it), so it never qualifies.
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
    # condition is already applied by materialization; the datasource needs no
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


def _scan_rows_at_grain(
    ds: BuildDatasource | BuildUnionDatasource,
    target_grain: BuildGrain,
    environment: BuildEnvironment,
) -> bool:
    """One scan row per `target_grain` tuple: the datasource's declared grain,
    or that grain plus components it functionally determines, which are
    constant per scan row. A merge is how a query grain outgrows a table that
    still answers it: `merge org.state_code into state.code` respells a
    property of `org.code` as a KEY, and every request naming it sits at
    `Grain<org.code, state.code>` over a `Grain<org.code>` table."""
    if ds.grain == target_grain:
        return True
    if ds.grain.abstract or not ds.grain.issubset(target_grain):
        return False
    return all(
        build_fd_determines(
            environment, ds.grain.components, component, include_empty_grain=False
        )
        for component in target_grain.components - ds.grain.components
    )


def _lineage_sourceable(
    concept: BuildConcept, environment: BuildEnvironment, seen: set[str]
) -> bool:
    """Some plan can produce `concept`: a datasource binds it, or every leaf
    of its lineage is a sourced root."""
    if concept.canonical_address in environment.materialized_canonical_concepts:
        return True
    if concept.lineage is None:
        return not root_is_unsourced(concept, environment)
    if concept.address in seen:
        return True
    seen.add(concept.address)
    return all(
        _lineage_sourceable(
            environment.concepts.get(arg.address) or arg, environment, seen
        )
        for arg in concept.lineage.concept_arguments
    )


def _lineage_closure(
    concept: BuildConcept, environment: BuildEnvironment
) -> list[BuildConcept]:
    return walk_lineage(
        [concept], resolve=lambda arg: environment.concepts.get(arg.address) or arg
    )


# Derivations a stored column reproduces row for row. The rest (FILTER/UNNEST/
# ROWSET/RECURSIVE/...) generate or drop rows and stay with their own gates.
_COLUMN_READABLE = frozenset(
    {
        Derivation.ROOT,
        Derivation.CONSTANT,
        Derivation.BASIC,
        Derivation.AGGREGATE,
        Derivation.WINDOW,
    }
)


def _conditions_cannot_move(
    concept: BuildConcept,
    derivations: set[Derivation],
    ds: BuildDatasource | BuildUnionDatasource,
    where: BuildWhereClause | None,
    environment: BuildEnvironment,
) -> bool:
    """The query's conditions leave the value `ds` stores unchanged. A
    row-level value always survives: a filter removes rows, never rewrites
    them. A WINDOW never does, since any filter reshapes its partition. An
    AGGREGATE does when the table sits at the aggregate's own grain, where
    every condition the table can express is constant within a group; stored
    below that grain (a customer total denormalized onto orders), a filter on
    `order_date` is expressible yet splits the groups the column summed."""
    if not where or not derivations & {Derivation.AGGREGATE, Derivation.WINDOW}:
        return True
    if Derivation.WINDOW in derivations:
        return False
    return _scan_rows_at_grain(ds, concept.grain, environment)


def _union_materializes(
    concept: BuildConcept,
    union: BuildUnionDatasource,
    where: BuildWhereClause | None,
    environment: BuildEnvironment,
) -> bool:
    """A stacked family stores `concept` when it holds every kept row and
    every arm binds the column and can express the conditions; the stack heals
    the arms' partial columns (`union_unhealed_partial_addresses`)."""
    if not holds_every_kept_row(union, where):
        return False
    unhealed = union.column_level_partial_addresses
    return all(
        _conditions_supported(arm, where, environment.concepts)
        and any(
            col.concept.canonical_address == concept.canonical_address
            and col.concept.address not in unhealed
            for col in arm.columns
        )
        for arm in union.children
    )


def _lineage_derivations(
    concept: BuildConcept, environment: BuildEnvironment
) -> set[Derivation]:
    return {c.derivation for c in _lineage_closure(concept, environment)}


def _column_reproduces(
    concept: BuildConcept,
    environment: BuildEnvironment,
    datasources: Sequence[BuildDatasource],
    where: BuildWhereClause | None,
) -> bool:
    derivations = _lineage_derivations(concept, environment)
    if not derivations <= _COLUMN_READABLE:
        return False
    if not where or not derivations & {Derivation.AGGREGATE, Derivation.WINDOW}:
        return True
    return any(
        _conditions_cannot_move(concept, derivations, ds, where, environment)
        and _datasource_materializes(concept, ds, where, environment)
        for ds in datasources
    )


def _binding_only_roots(
    candidates: list[BuildConcept],
    environment: BuildEnvironment,
    datasources: Sequence[BuildDatasource],
    where: BuildWhereClause | None,
) -> set[str]:
    """Bound derived concepts, anywhere in the candidates' lineage, that cannot
    be re-derived: `short_e_name <- coalesce(_short_e_name, short_name)` bound
    as a column beside a `_short_e_name` no table binds. The column is the
    concept's only source, so it reads like any bound property: at every
    grain, and as an input to other derivations.

    No grain gate is needed. A canonical address pins an aggregate's grain, so
    a column match already means the stored value is this query's value; the
    scan's multiplicity is then source planning's concern, as for any root.
    What a column cannot absorb is a condition (`_conditions_cannot_move`)."""
    out: set[str] = set()
    seen: set[str] = set()
    stack = list(candidates)
    while stack:
        concept = stack.pop()
        if concept.address in seen or concept.lineage is None:
            continue
        seen.add(concept.address)
        args = [
            environment.concepts.get(arg.address) or arg
            for arg in concept.lineage.concept_arguments
        ]
        if (
            concept.canonical_address in environment.materialized_canonical_concepts
            and not all(_lineage_sourceable(arg, environment, set()) for arg in args)
            and _column_reproduces(concept, environment, datasources, where)
        ):
            out.add(concept.address)
            continue
        # A rowset's content is planned by its own search. Elsewhere the walk
        # follows the graph's own edges, so a grain key a consumer reads a
        # stored aggregate on (`sum(n)` over `n <- count(id) by cls`) is
        # judged like a lineage argument.
        if concept.derivation != Derivation.ROWSET:
            stack.extend(
                environment.concepts.get(c.address) or c
                for c in upstream_concepts(concept, environment)
            )
    return out


def materialized_root_addresses(
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    conditions: list[BuildWhereClause],
    datasources: Sequence[BuildDatasource],
    unions: Sequence[BuildUnionDatasource] = (),
) -> frozenset[str]:
    """Demanded derived concepts that a datasource materializes directly: a
    precomputed / pre-aggregated summary table or a persisted derived column.
    Stage 1 treats these as ROOT scans so v4 reads the table instead of
    re-deriving from base.

    Eligibility is one rule (`_datasource_materializes`): a datasource binds the
    concept's canonical expression as a COMPLETE column (or a partial one the
    query's conditions complete) and can express the conditions. EXACT-grain
    AGGREGATE/BASIC additionally require one scan row per target-grain tuple
    (`_scan_rows_at_grain`) so the scan's row multiplicity matches; an UNNEST is
    exempt (a persisted unnest table's declared grain is the coarser key,
    understating its per-value rows).

    Binding-only: a bound derived concept with no sourceable lineage has no
    derive-from-base plan to prefer, so it is a root wherever the lineage walk
    meets it (`_binding_only_roots`).

    Rollup: a rollable AGGREGATE (`ROLLUP_REAGGREGATES`) that no datasource has
    at the exact grain, but a *finer*-grain table binds, is also treated as a
    root scanned from that finer table; `_group_to_grain_if_required` then
    re-aggregates it to the target grain (`sum(finer.col)`, `max(finer.col)`).

    Condition row-args are candidates too (EXACT branch only): a WHERE over a
    materialized aggregate (`where customer_revenue > 100` beside its summary
    table) reads the table and filters it, instead of re-deriving from base,
    and without the mark the atom's arg is invisible to the root cluster's
    satisfiability check, silently dropping the WHERE. The rollup branch is
    mandatory-only: filtering a finer scan by a rolled-up value pre-aggregation
    would filter the wrong rows."""
    if not mandatory_list:
        return frozenset()
    target_grain = BuildGrain.from_concepts(mandatory_list)
    where = combine_where_clauses(conditions)
    mandatory_addresses = {c.address for c in mandatory_list}
    condition_args_by_address: dict[str, BuildConcept] = {}
    for clause in conditions:
        for arg in clause.row_arguments:
            if arg.address not in mandatory_addresses:
                condition_args_by_address.setdefault(arg.address, arg)
    candidates = mandatory_list + list(condition_args_by_address.values())
    # An unbound bare KEY whose pseudonym origin recomposes it (`merge
    # composite_id_alt into composite_id`, alt <- concat(first, second)) is
    # sourced by substituting the origin during the graph walk, so the
    # origin's own args are lineage intermediates the walk must be able to
    # stop at. Without the mark, an arg that is bound-but-derived (`first <-
    # split(composite_id)`, also a physical column) walks its authored lineage
    # back into the unbound key and the cluster dead-ends. Direct args only;
    # each still passes the materializes + grain gates below.
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
    out = _binding_only_roots(candidates, environment, datasources, where)
    row_grain = _expression_reduced_grain(
        _keyed_grain(target_grain, environment), environment
    )
    rolled_roots: set[str] = set()
    rollup_sources: list[BuildDatasource] = []
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
        exact = _exactly_materialized(
            concept, target_grain, where, environment, datasources, unions
        )
        if exact:
            out.add(concept.address)
        # A rolled root joins the target-grain scan, so an aggregate at another
        # grain (`count(x) by *` beside per-city rows) plans as its own group.
        if (
            exact
            or condition_only
            or not (is_aggregate and _is_rollable_aggregate(concept))
            or not at_own_grain(concept, row_grain, environment.concepts)
        ):
            continue
        # ROLLUP: a finer-grain table binds the same aggregate expression and it
        # can be rolled up to the target grain. Neither address nor canonical
        # address can gate this: the finer instance is grain-pinned to a
        # different canonical, and an agent-authored alias (`sum(ss.price) as
        # total`) never shares an address with the table's bound column. The gate
        # is therefore the *lineage signature* (operator, sorted canonical arg
        # addresses), which is exactly what the rollup matcher below re-checks.
        #
        # Marking the concept a root lets source-planning pick a table binding
        # it. `get_rollup_concepts` below is passed the filter and only
        # matches a datasource whose grain can express it (`_conditions_supported`):
        # a group-level filter (constant within a target-grain group) matches any
        # coarser/exact table, while a finer filter (`order_date` below a
        # `customer_id` grain) only matches a finer summary that carries the
        # column; `plan_source._plan_finer_filter_rollup` then pins that table,
        # pushes the filter pre-aggregation, and rolls up. A coarser table is
        # never matched under a finer filter, so post-rollup decoupling can't
        # double-count.
        sources = _rollup_sources(
            concept,
            mandatory_list,
            target_grain,
            where,
            environment,
            datasources,
            unions,
        )
        if sources:
            rolled_roots.add(concept.address)
            rollup_sources.extend(sources)
    out |= {
        key.address
        for key in _aggregated_derived_keys(mandatory_list, environment)
        if target_grain.components <= key.grain.components
        and _exactly_materialized(
            key, key.grain, where, environment, datasources, unions
        )
    }
    out |= _stored_coarser_roots(
        candidates, target_grain, out | rolled_roots, where, environment, datasources
    )
    nested_exact, nested = _nested_rollup_roots(
        mandatory_list, out | rolled_roots, where, environment, datasources, unions
    )
    out |= nested_exact
    rolled_roots |= nested.keys()
    rollup_sources.extend(ds for sources in nested.values() for ds in sources)
    if rolled_roots and not _finer_filter_reaches_rollup(
        mandatory_list, rolled_roots, rollup_sources, target_grain, where, environment
    ):
        return frozenset(out)
    return frozenset(
        out
        | rolled_roots
        | _related_unnest_roots(
            mandatory_list, rolled_roots, target_grain, where, environment, datasources
        )
        | _summary_bound_expressions(
            mandatory_list
            + list(condition_args_by_address.values())
            + _rolled_grain_columns(rolled_roots, environment),
            rollup_sources,
        )
    )


def _stored_coarser_roots(
    candidates: list[BuildConcept],
    target_grain: BuildGrain,
    roots: set[str],
    where: BuildWhereClause | None,
    environment: BuildEnvironment,
    datasources: Sequence[BuildDatasource],
) -> set[str]:
    """Values a table stores at their own grain, which the query's rows
    determine (`slope` by `g` beside `id`, or inside `count(id ? resid < 1)`):
    joined from that table on its grain, not recomputed from the finer rows
    and joined back on the same keys."""
    reachable = build_fd_closure(
        environment, target_grain.components, include_empty_grain=False
    )
    found: set[str] = set()
    seen: set[str] = set()
    stack = list(candidates)
    while stack:
        popped = stack.pop()
        concept = environment.concepts.get(popped.address) or popped
        if concept.address in seen or concept.address in roots:
            continue
        seen.add(concept.address)
        if _stored_at_own_grain(concept, reachable, where, environment, datasources):
            found.add(concept.address)
        elif concept.lineage is not None:
            stack.extend(concept.lineage.concept_arguments)
    return found


def _stored_at_own_grain(
    concept: BuildConcept,
    reachable: Collection[str],
    where: BuildWhereClause | None,
    environment: BuildEnvironment,
    datasources: Sequence[BuildDatasource],
) -> bool:
    grain = concept.grain.components if concept.grain else set()
    return (
        concept.derivation in (Derivation.AGGREGATE, Derivation.BASIC)
        and bool(grain)
        and _recomputes_aggregate(concept, environment)
        and all(key in reachable for key in grain)
        and _exactly_materialized(
            concept, concept.grain, where, environment, datasources, ()
        )
    )


def _recomputes_aggregate(concept: BuildConcept, environment: BuildEnvironment) -> bool:
    """Re-deriving `concept` aggregates rows no table stores the result of;
    a row expression over stored inputs is re-derived from them instead."""
    stored = environment.materialized_canonical_concepts
    return concept.derivation == Derivation.AGGREGATE or any(
        c.derivation == Derivation.AGGREGATE and c.canonical_address not in stored
        for c in walk_lineage(
            [concept],
            descend=lambda c: c is concept or c.canonical_address not in stored,
            resolve=lambda c: environment.concepts.get(c.address) or c,
        )
    )


def _aggregated_derived_keys(
    mandatory_list: list[BuildConcept], environment: BuildEnvironment
) -> list[BuildConcept]:
    """Derived keys a demanded aggregate reads as its own input rows
    (`count(cls)` over `key cls <- ...`): their values, not their inputs' rows,
    so a table at the key's grain answers them without a re-derivation."""
    return [
        key
        for concept in mandatory_list
        if concept.derivation == Derivation.AGGREGATE and concept.lineage
        for arg in concept.lineage.concept_arguments
        if derived_key(key := environment.concepts.get(arg.address) or arg)
    ]


def _related_unnest_roots(
    mandatory_list: list[BuildConcept],
    rolled_roots: set[str],
    target_grain: BuildGrain,
    where: BuildWhereClause | None,
    environment: BuildEnvironment,
    datasources: Sequence[BuildDatasource],
) -> set[str]:
    """An unnested element the summary's key relates to, when every demanded
    aggregate rolls through that relation: read beside the rolled aggregates
    (`relation_rollup_concepts`), not expanded over their rows."""
    aggregates = {c.address for c in mandatory_list if c.is_aggregate}
    if where is not None or not aggregates or not aggregates <= rolled_roots:
        return set()
    for ds in datasources:
        rolled = relation_rollup_concepts(
            ds, mandatory_list, environment, datasources, target_grain
        )
        if aggregates <= {c.address for c in rolled}:
            return {
                address
                for address in relation_targets(ds, target_grain, environment)
                if environment.concepts[address].derivation == Derivation.UNNEST
            }
    return set()


def _exactly_materialized(
    concept: BuildConcept,
    grain: BuildGrain,
    where: BuildWhereClause | None,
    environment: BuildEnvironment,
    datasources: Sequence[BuildDatasource],
    unions: Sequence[BuildUnionDatasource],
) -> bool:
    """A table or stacked family with one row per `grain` tuple stores
    `concept`, and the conditions leave the stored value unchanged."""
    if concept.canonical_address not in environment.materialized_canonical_concepts:
        return False
    derivations = _lineage_derivations(concept, environment)
    sources: list[BuildDatasource | BuildUnionDatasource] = [*datasources, *unions]
    return any(
        _scan_rows_at_grain(source, grain, environment)
        and _conditions_cannot_move(concept, derivations, source, where, environment)
        and (
            _datasource_materializes(concept, source, where, environment)
            if isinstance(source, BuildDatasource)
            else _union_materializes(concept, source, where, environment)
        )
        for source in sources
    )


def _rolled_grain_columns(
    rolled_roots: set[str], environment: BuildEnvironment
) -> list[BuildConcept]:
    """The columns a rolled aggregate groups by (`plant_year` of `count(x) by
    species, plant_year`): read from its summary, not rebuilt from the facts
    and joined back onto it."""
    return [
        environment.concepts.get(c.address) or c
        for address in sorted(rolled_roots)
        if isinstance(
            (lineage := environment.concepts[address].lineage), BuildAggregateWrapper
        )
        for c in lineage.by
    ]


def _summary_bound_expressions(
    mandatory_list: list[BuildConcept],
    rollup_sources: list[BuildDatasource],
) -> set[str]:
    """Scalar demands a rollup's summary stores (`year(plant_date)` in its
    grain) are read from it beside the rolled aggregate; rebuilt from the fact
    rows they would join those rows back onto the summary and fan it out. A
    stored column answers its value under any spelling (`plant_date.year`,
    `year(plant_date) as y`), and a scalar over one (`plant_year::int`, a
    rename) reads it from the summary. So does a WHERE over one: a function of
    the summary's grain filters its rows before the re-sum."""
    bound = {c.canonical_address for ds in rollup_sources for c in ds.output_concepts}
    return {
        concept.address
        for concept in walk_lineage(
            mandatory_list,
            descend=lambda c: c.derivation == Derivation.BASIC
            and c.canonical_address not in bound,
        )
        if concept.derivation == Derivation.BASIC and concept.canonical_address in bound
    }


def _keyed_grain(
    grain: BuildGrain, environment: BuildEnvironment, windows: bool = False
) -> BuildGrain:
    """`grain` with each aggregate component read as its own grain: a select of
    `sum(x) by k` alone has the aggregate as its grain, and `k` as its rows.
    With `windows`, a window keyed at a grain is read the same way: it is
    evaluated once per row of that grain and adds none."""
    components: set[str] = set()
    for address in grain.components:
        concept = environment.concepts.get(address)
        if concept is not None and (
            concept.is_aggregate
            or (
                windows
                and concept.derivation == Derivation.WINDOW
                and concept.grain.components
                and not concept.grain.abstract
            )
        ):
            components |= set(concept.grain.components)
        else:
            components.add(address)
    return BuildGrain(components=components)


def _expression_reduced_grain(
    grain: BuildGrain, environment: BuildEnvironment
) -> BuildGrain:
    """`grain` without components that are scalar expressions of the others
    (`case bucket when ... end` beside `bucket`): constant within each group of
    the rest, they add no rows an aggregate at the rest's grain would split."""
    components = set(grain.components)
    for address in sorted(grain.components):
        rest = components - {address}
        if rest and _scalar_of(
            address, build_fd_closure(environment, rest), environment
        ):
            components = rest
    return BuildGrain(components=components)


def _scalar_of(
    address: str, closure: frozenset[str], environment: BuildEnvironment
) -> bool:
    """A BASIC expression whose arguments sit in `closure`, directly or through
    other BASIC expressions (`case b2 ...` over `b2 <- concat(bucket, '!')`)."""
    concept = environment.concepts.get(address)
    if concept is None or concept.derivation != Derivation.BASIC:
        return False
    return all(
        arg.address in closure or _scalar_of(arg.address, closure, environment)
        for arg in concept.concept_arguments
    )


def scan_filtered_roots(
    mandatory_list: list[BuildConcept],
    materialized_roots: frozenset[str],
    conditions: list[BuildWhereClause],
    environment: BuildEnvironment,
) -> frozenset[str]:
    """Materialized aggregate roots read under a WHERE finer than their own
    grain: the WHERE must apply on their scan (`ConceptAttrs.scan_filtered`)."""
    where = combine_where_clauses(conditions)
    if where is None or not materialized_roots:
        return frozenset()
    return frozenset(
        concept.address
        for concept in walk_lineage(
            mandatory_list, descend=lambda c: c.address not in materialized_roots
        )
        if concept.address in materialized_roots
        and concept.derivation == Derivation.AGGREGATE
        and filter_finer_row_args(where, concept.grain, environment.concepts)
    )


def _rollup_sources(
    concept: BuildConcept,
    requested: list[BuildConcept],
    target_grain: BuildGrain,
    where: BuildWhereClause | None,
    environment: BuildEnvironment,
    datasources: Sequence[BuildDatasource],
    unions: Sequence[BuildUnionDatasource] = (),
) -> list[BuildDatasource]:
    """The finer summaries `concept` re-aggregates from at `target_grain`. A
    stacked family serves it when every arm rolls it up: the arms hold
    disjoint rows (`Stack.stacked_summary_sum_exact`), so its arms are the
    summaries read."""
    out: list[BuildDatasource] = []
    sources: list[BuildDatasource | BuildUnionDatasource] = [*datasources, *unions]
    gated = _coarse_gate_beside_row_columns(where, target_grain)
    total = total_gate_beside_rows(where)
    for source in sources:
        # shapes the joined plan declines: the search that takes over reads a
        # keyed gate off the summary at the target's grain
        if isinstance(source, BuildDatasource) and (
            (gated and _keyed_gate(where, source, environment))
            or (total and _keyed_gate(where, source, environment, coarse=False))
            or _unsourced_beside_dimension_gate(where, source, environment)
        ):
            continue
        if not holds_every_kept_row(source, where):
            continue
        if isinstance(source, BuildDatasource) and not (
            _datasource_has_matching_rollup_aggregate(
                source, concept, environment.concepts
            )
        ):
            continue
        rolled = source_rollup_concepts(
            datasource=source,
            requested_concepts=requested,
            concepts_by_address=environment.concepts,
            datasources=datasources,
            target_grain=target_grain,
            conditions=where,
            joined=True,
        )
        # under a WHERE the group graph filters the list's parent rows, not a
        # rolled root, so a relation rollup would drop it
        if isinstance(source, BuildDatasource) and where is None:
            rolled = rolled + relation_rollup_concepts(
                source, requested, environment, datasources, target_grain
            )
        if any(r.address == concept.address for r in rolled):
            out.extend(
                source.children
                if isinstance(source, BuildUnionDatasource)
                else [source]
            )
    return out


def _keyed_gate(
    where: BuildWhereClause | None,
    source: BuildDatasource,
    environment: BuildEnvironment,
    coarse: bool = True,
) -> bool:
    """The WHERE reads an aggregate keyed on `source`'s rows; with `coarse`,
    one coarser than its grain (`sum(x) by city` beside `(city, species)`),
    whose value is a re-sum the joined plan computes in its own group."""
    test = coarse_gate if coarse else keyed_aggregate
    return where is not None and any(
        test(c, source, environment.concepts) for c in where.row_arguments
    )


def _unsourced_beside_dimension_gate(
    where: BuildWhereClause | None,
    source: BuildDatasource,
    environment: BuildEnvironment,
) -> bool:
    """A gate grouped by a dimension's column (`sum(x) by city, tree_form`)
    beside one at the summary's grain it does not store (`avg(x) by city,
    species`): the joined plan cannot source the latter, and the search that
    takes over joins the former onto the latter's keys alone
    (`Rollup.gate_joined_on_part_of_its_grain_misfilters`)."""
    if where is None:
        return False
    gates = [c for c in where.row_arguments if c.is_aggregate]
    grain = set(source.grain.components)
    dimension = {
        c.address
        for c in gates
        if keyed_aggregate(c, source, environment.concepts)
        and not set(c.grain.components) <= grain
    }
    return bool(dimension) and any(
        not (
            _is_rollable_aggregate(c)
            and _datasource_has_matching_rollup_aggregate(
                source, c, environment.concepts
            )
        )
        and set(c.grain.components) == grain
        for c in gates
    )


def _coarse_gate_beside_row_columns(
    where: BuildWhereClause | None, target_grain: BuildGrain
) -> bool:
    """A row column beside the gate that the target does not group by
    (`species != 'Tilia'` at `tree_form`) widens the pinned request past the
    target, and the search that takes over re-aggregates the gate at it."""
    return where is not None and any(
        not c.is_aggregate
        and c.granularity != Granularity.SINGLE_ROW
        and c.address not in target_grain.components
        for c in where.row_arguments
    )


def _nested_rollup_roots(
    mandatory_list: list[BuildConcept],
    roots: set[str],
    where: BuildWhereClause | None,
    environment: BuildEnvironment,
    datasources: Sequence[BuildDatasource],
    unions: Sequence[BuildUnionDatasource] = (),
) -> tuple[set[str], dict[str, list[BuildDatasource]]]:
    """rollable aggregates inside a demand (`count(x) by k` under a window,
    `sum(x) * 2`) that a summary stores at their own grain, under the same
    WHERE, and those a finer summary rolls up to it (with its sources)."""
    mandatory = {c.address for c in mandatory_list}
    exact: set[str] = set()
    out: dict[str, list[BuildDatasource]] = {}
    rows = _nested_row_grains(mandatory_list, roots, environment)
    for concept in walk_lineage(
        mandatory_list,
        descend=lambda c: c.address not in roots,
        resolve=lambda c: environment.concepts.get(c.address) or c,
        partials=environment.partial_sources,
    ):
        if (
            concept.address in mandatory
            or concept.derivation != Derivation.AGGREGATE
            or not _is_rollable_aggregate(concept)
            or not all(
                at_own_grain(concept, grain, environment.concepts)
                for grain in rows[concept.address]
            )
        ):
            continue
        if _exactly_materialized(
            concept, concept.grain, where, environment, datasources, unions
        ):
            exact.add(concept.address)
            continue
        sources = _rollup_sources(
            concept, [concept], concept.grain, where, environment, datasources, unions
        )
        if sources:
            out[concept.address] = sources
    return exact, out


def _nested_row_grains(
    mandatory_list: list[BuildConcept],
    roots: set[str],
    environment: BuildEnvironment,
) -> dict[str, list[BuildGrain]]:
    """Each concept under the demand -> the grains of the rows it is read
    beside. An aggregate's arguments are read on their own rows; any other
    derivation (a window, an expression) reads its arguments on the rows it is
    evaluated at, so a coarser aggregate under it shares a finer scan."""
    out: dict[str, list[BuildGrain]] = {}
    seen: set[tuple[str, frozenset[str]]] = set()
    visited: set[tuple[str, frozenset[str]]] = set()
    grains: dict[frozenset[str], BuildGrain] = {}
    top = {c.address: c for c in mandatory_list}
    stack = [(concept, top) for concept in mandatory_list]
    while stack:
        concept, beside = stack.pop()
        beside_key = frozenset(beside)
        if (concept.address, beside_key) in visited:
            continue
        visited.add((concept.address, beside_key))
        grain = grains.get(beside_key)
        if grain is None:
            grain = _keyed_grain(
                BuildGrain.from_concepts(beside.values()), environment, windows=True
            )
            grains[beside_key] = grain
        key = (concept.address, frozenset(grain.components))
        if key in seen:
            continue
        seen.add(key)
        out.setdefault(concept.address, []).append(grain)
        if concept.lineage is None or concept.address in roots:
            continue
        # read as `_nested_rollup_roots` walks: what the plan reads
        args = {
            c.address: environment.concepts.get(c.address) or c
            for c in sourced_arguments(concept, environment.partial_sources)
        }
        below = args if concept.derivation == Derivation.AGGREGATE else beside | args
        stack.extend((arg, below) for arg in args.values())
    return out


def _finer_filter_reaches_rollup(
    mandatory_list: list[BuildConcept],
    rolled_roots: set[str],
    rollup_sources: list[BuildDatasource],
    target_grain: BuildGrain,
    where: BuildWhereClause | None,
    environment: BuildEnvironment,
) -> bool:
    """A filter finer than the target grain is only applied to a rolled-up root
    when the root shares one scan with the filter columns
    (`_plan_finer_filter_rollup`). The other demands ride that scan, so each
    must be a function of the summary's rows: an aggregate the summary does not
    carry, or a column below its grain, needs the fact rows instead."""
    if not filter_finer_row_args(where, target_grain, environment.concepts):
        return True
    demands = [c for c in mandatory_list if c.address not in rolled_roots]
    assert where is not None
    # an atom over a stored aggregate filters only the pinned scan's own rows,
    # so every other demand must be a column of that scan, or a dimension's
    # column keyed on its grain (`_plan_joined_filter_rollup` joins it on)
    if not is_scalar_condition(where.conditional):
        return any(_rides_summary_scan(demands, ds) for ds in rollup_sources)
    reached = walk_lineage(
        [c for c in mandatory_list if c.address not in rolled_roots],
        descend=lambda c: c.address not in rolled_roots,
        resolve=lambda c: environment.concepts.get(c.address) or c,
        partials=environment.partial_sources,
    )
    leaves = [c for c in reached if c.lineage is None]
    if BuildGrain.from_concepts([*mandatory_list, *leaves]) in (
        target_grain,
        _keyed_grain(target_grain, environment, windows=True),
    ):
        return True
    if any(
        c.derivation == Derivation.AGGREGATE
        and c.address not in rolled_roots
        and not _aggregates_rolled_groups(c, rolled_roots, rollup_sources, environment)
        for c in reached
    ):
        return False
    return any(
        _concepts_reachable(
            ds,
            _summary_leaves(demands, rolled_roots, ds, environment),
            environment.concepts,
        )
        for ds in rollup_sources
    )


def _rides_summary_scan(demands: list[BuildConcept], ds: BuildDatasource) -> bool:
    bound = {c.canonical_address for c in ds.output_concepts}
    return all(
        c.canonical_address in bound or joinable_property(c, ds) for c in demands
    )


def _aggregates_rolled_groups(
    concept: BuildConcept,
    rolled_roots: set[str],
    rollup_sources: list[BuildDatasource],
    environment: BuildEnvironment,
) -> bool:
    """An aggregate over the rolled groups (`count(species ? count(tree_id)
    by species > 0)`) reads one row per group, so it rides the pinned scan,
    as does a re-aggregate of a rolled partial (`count(tree_id) by *` beside
    the per-species count). One over the summary's raw rows would count each
    group once per row it rolls up, and one the summary stores a partial of
    must be rolled itself."""
    if concept.address not in environment.partial_sources and any(
        _datasource_has_matching_rollup_aggregate(ds, concept) for ds in rollup_sources
    ):
        return False
    return any(
        c.address in rolled_roots
        for c in walk_lineage(
            [concept],
            resolve=lambda c: environment.concepts.get(c.address) or c,
            partials=environment.partial_sources,
        )
    )


def _summary_leaves(
    demands: list[BuildConcept],
    rolled_roots: set[str],
    datasource: BuildDatasource,
    environment: BuildEnvironment,
) -> list[BuildConcept]:
    """The columns `datasource` must supply for `demands`: a derived concept the
    summary binds (`year(plant_date)` stored at its grain) is read, not rebuilt
    from its finer inputs."""
    bound = {c.canonical_address for c in datasource.output_concepts}
    reached = walk_lineage(
        demands,
        descend=lambda c: c.address not in rolled_roots
        and c.canonical_address not in bound,
        resolve=lambda c: environment.concepts.get(c.address) or c,
        partials=environment.partial_sources,
    )
    return [c for c in reached if c.lineage is None or c.canonical_address in bound]


def _build_from_graph(
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    g: ReferenceGraph,
    history: V4History,
    conditions: list[BuildWhereClause],
    materialized_roots: frozenset[str],
    complete_partials: bool,
    staged_conditions: list[BuildWhereClause] | None = None,
    depth: int = 0,
) -> BuildInfo:
    with (
        plan_trace.plan_scope(
            f"plan: {', '.join(c.address for c in mandatory_list)}",
            depth,
            outputs=plan_trace.addresses(mandatory_list),
            conditions=[plan_trace.expression(c) for c in conditions],
        )
        if plan_trace.active()
        else nullcontext()
    ):
        return _build_from_graph_traced(
            mandatory_list,
            environment,
            g,
            history,
            conditions,
            materialized_roots,
            complete_partials,
            staged_conditions,
            depth,
        )


# named in `plan_trace._ORIGIN_ROOTS`: rename both together
def _build_from_graph_traced(
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    g: ReferenceGraph,
    history: V4History,
    conditions: list[BuildWhereClause],
    materialized_roots: frozenset[str],
    complete_partials: bool,
    staged_conditions: list[BuildWhereClause] | None,
    depth: int,
) -> BuildInfo:
    if plan_trace.active():
        plan_trace.record(
            "requested concepts",
            plan_trace.RequestStep(
                concepts=[plan_trace.concept(c) for c in mandatory_list],
                conditions=[plan_trace.expression(c) for c in conditions],
                staged_conditions=[
                    plan_trace.expression(c) for c in staged_conditions or []
                ],
                materialized_roots=sorted(materialized_roots),
                complete_partials=complete_partials,
                span_scope=plan_trace.span_scope(environment.span_scope),
                environment=plan_trace.environment(environment, g.scope.datasources),
            ),
        )
    concept_graph, concept_attrs, concept_edges = build_concept_graph(
        mandatory_list,
        environment,
        conditions,
        materialized_roots,
        staged_conditions=staged_conditions,
        datasources=g.scope.datasources,
        scan_filtered_roots=scan_filtered_roots(
            mandatory_list, materialized_roots, conditions, environment
        ),
    )
    if plan_trace.active():
        plan_trace.record(
            "concept graph",
            plan_trace.ConceptGraphStep(
                graph=plan_trace.graph(concept_graph, concept_edges, concept_attrs),
                concepts=plan_trace.graph_concepts(environment, concept_attrs),
            ),
        )
    keyspace = statement_keyspace(
        concept_attrs, mandatory_list, environment, conditions, history, g.scope
    )
    if len(keyspace.regions) > 1 and logger.isEnabledFor(logging.INFO):
        logger.info(
            "%s%s keyspace: %s",
            depth_to_prefix(depth),
            LOGGER_PREFIX,
            keyspace.describe(),
        )
    if plan_trace.active():
        plan_trace.record(
            "keyspace", plan_trace.KeyspaceStep(keyspace=plan_trace.keyspace(keyspace))
        )
    group_graph, group_edges, group_attrs = build_group_graph(
        concept_graph,
        concept_edges,
        concept_attrs,
        conditions,
        mandatory_list,
        environment=environment,
        datasources=g.scope.datasources,
        staged_conditions=staged_conditions,
        keyspace=keyspace,
    )
    # a region nothing demands is not a row of the statement: no join of
    # this plan extends its spans
    scope = SpanScope(
        keyspace=keyspace,
        owned=history.owned_spans,
        unextended=undemanded_spans(keyspace, concept_attrs, environment),
    )
    strategy_node: StrategyNode | None = None
    unbuilt_reason: str | None = None
    # a rowset body planned mid-build recurses through here, so the outer
    # plan's scope is restored rather than cleared
    try:
        with under_span_scope(environment, scope):
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
                depth=depth,
            )
    except UnbuiltGroupException as exc:
        unbuilt_reason = str(exc)
    if plan_trace.active():
        plan_trace.record(
            "strategy node",
            plan_trace.StrategyStep(node=plan_trace.strategy_node(strategy_node)),
        )
    return BuildInfo(
        concept_graph=concept_graph,
        group_graph=group_graph,
        group_attrs=group_attrs,
        concept_attrs=concept_attrs,
        concept_edges=concept_edges,
        group_edges=group_edges,
        strategy_node=strategy_node,
        keyspace=keyspace,
        unbuilt_reason=unbuilt_reason,
    )


def _own_build_of(
    mandatory_list: list[BuildConcept],
    lineage_type: type[BuildMultiSelectLineage],
) -> BuildConcept | None:
    """The demanded concept whose `lineage_type` build produces EVERY mandatory
    concept, if any.

    A request that merely REFERENCES a member beside outer derivations (an
    ORDER BY carrying a grouped-away union column next to aggregates over the
    rowset) is NOT the construct's own build: intercepting it would stamp the
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
    group graph (each arm is its own sub-plan), so the diagnostic graphs the
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
    # A top-level multiselect (merge/align) isn't a single source graph; its
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
    materialized_roots = materialized_root_addresses(
        mandatory_list,
        environment,
        conditions,
        g.scope.datasources,
        [ds for ds in g.datasources.values() if isinstance(ds, BuildUnionDatasource)],
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
        depth=depth,
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
            depth=depth,
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
        if plan_trace.active():
            plan_trace.record(
                "returned from history",
                plan_trace.HistoryHitStep(
                    concepts=plan_trace.addresses(mandatory_list),
                    conditions=[plan_trace.expression(c) for c in conditions],
                    exists=hist.strategy_node is not None,
                ),
            )
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
