from trilogy.core.enums import Derivation, Purpose
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildFilterItem,
    BuildGrain,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import (
    combine_condition_atoms,
    is_scalar_condition,
)
from trilogy.core.processing.nodes import FilterNode, StrategyNode
from trilogy.core.processing.v4_helper.functional_dependency import (
    build_fd_determines,
)

from .common import parent_outputs_needed


def _has_concept_existence(where: BuildWhereClause) -> bool:
    """True only for a REAL subselect arg (`x in <other column/select>`), one
    whose existence side carries concepts. A literal IN-list (`month in (1,2,3,4)`)
    is also modeled as a subselect comparison but has no existence concepts, so it
    is a plain scalar predicate safe to push into a WHERE."""
    return any(arg for tup in (where.existence_arguments or ()) for arg in tup)


def shared_filter_predicate(concepts: list[BuildConcept]) -> BuildWhereClause | None:
    """The one predicate every filter concept among `concepts` is gated on, or
    None. Distinct predicates are fused conditional columns (`price ? channel =
    'STORE'`, `price ? channel = 'WEB'`), each its own CASE over the shared
    scan: AND-ing them into one WHERE would null out every row. A predicate
    with an existence arg needs its subselect source wired as a side parent,
    which no WHERE push does."""
    distinct: dict[str, BuildWhereClause] = {}
    for c in concepts:
        if isinstance(c.lineage, BuildFilterItem):
            distinct.setdefault(str(c.lineage.where.conditional), c.lineage.where)
    if len(distinct) != 1:
        return None
    where = next(iter(distinct.values()))
    return None if _has_concept_existence(where) else where


def statement_filter_population(
    mandatory_list: list[BuildConcept],
) -> BuildWhereClause | None:
    """When every output a statement shows is a filter value over one
    predicate, a NULL row is one nothing would keep: `gen_filter` pushes the
    predicate into its WHERE, and the keyspace and pin-heal read it as the
    statement's own, so a region those rows are absent on is emptied and the
    `~` it would pad for is healed, never padded back."""
    if not all(isinstance(c.lineage, BuildFilterItem) for c in mandatory_list):
        return None
    return shared_filter_predicate(mandatory_list)


def _entity_grain(outputs: list[BuildConcept]) -> set[str]:
    """The entity keys the outputs are functions of: a property stands for its
    keys, whether or not they are projected beside it (`select name, late_name`
    is at customer grain), anything else for itself."""
    grain: set[str] = set()
    for o in outputs:
        if o.purpose == Purpose.PROPERTY and o.keys:
            grain |= set(o.keys)
        else:
            grain.add(o.address)
    return grain


def gen_filter(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
    intrinsic_filter_pushdown: bool = True,
    existence_source: bool = False,
    collapse_to_grain: bool = True,
) -> StrategyNode | None:
    """Project filter concepts over already-built parents.

    A filter concept (`filter X where COND`) is a VALUE: `X` where COND holds,
    NULL elsewhere. It narrows only itself, never a sibling's rows, so it
    renders as the per-row `CASE WHEN COND THEN X ELSE NULL` (`select
    customer_id, late_name` keeps every customer). The one shape whose rows it
    does narrow is a statement showing nothing but filter values over one
    predicate, where a NULL row is one nothing would keep: the caller says so
    (`intrinsic_filter_pushdown`, since the group's own outputs carry keys the
    statement never shows) and the predicate is pushed into this node's WHERE
    over the parents as built, so an aggregate it also reads keeps its own
    population (`customer_id ? count(order_id) by customer_id > 1 and
    product_name = 'Mouse'` counts EVERY order). Pushed when the predicate is a
    plain scalar, or an aggregate predicate whose every referenced concept is
    already a parent output (a precomputed column, never a re-aggregation).

    Pass through every parent output as well as the filter's own primaries: a
    downstream consumer (an aggregate that needs a grain key) can then reach
    back through the filter. The optimizer prunes unused columns later. An
    ``existence_source`` filter is a semijoin RHS (`pcid in store_buyers`) whose
    only consumer reads the set value: no pass-through, and its predicate is
    always pushed, since a CASE would put a NULL member in the set and `not in`
    over a NULL member matches nothing.

    A CASE over predicate inputs the outputs' grain does not determine fans a
    property out into {content, NULL}, one row per input row (`filter name
    where undelivered` beside `customer_id`: one row per order). A keyed
    filter groups to the grain so the renderer collapses it (`CTE.filter_collapses_to_grain`), the
    way the same filter renders when computed beside its content; the
    pass-through then keeps only what that grain determines. Not when an
    aggregate consumes it (``collapse_to_grain`` False): `count(line_no ?
    commit_date < receipt_date)` counts the per-row CASE, and the aggregate
    pushes a sole predicate into its own WHERE."""
    pass_through: list[BuildConcept] = []
    seen = {c.address for c in outputs}
    if not existence_source:
        for parent in parents:
            for output in parent.output_concepts:
                if output.address not in seen:
                    pass_through.append(output)
                    seen.add(output.address)
    full_outputs = list(outputs) + pass_through

    filter_lineages = [
        o.lineage for o in outputs if isinstance(o.lineage, BuildFilterItem)
    ]
    intrinsic: BoolExpr | None = None
    where = (
        shared_filter_predicate(outputs)
        if intrinsic_filter_pushdown or existence_source
        else None
    )
    if where is not None:
        parent_outputs = {c.address for p in parents for c in p.output_concepts}
        agg_args = [
            r for r in where.row_arguments if r.derivation == Derivation.AGGREGATE
        ]
        if is_scalar_condition(where.conditional) or (
            agg_args and all(r.address in parent_outputs for r in where.row_arguments)
        ):
            intrinsic = where.conditional

    grain: BuildGrain | None = None
    if filter_lineages and intrinsic is None and collapse_to_grain:
        entity_grain = _entity_grain(outputs)
        collapsible = all(
            bool(o.keys) and set(o.keys or ()) <= entity_grain
            for o in outputs
            if isinstance(o.lineage, BuildFilterItem)
        )
        finer = any(
            not build_fd_determines(environment, entity_grain, r.address)
            for lineage in filter_lineages
            for r in lineage.where.row_arguments
        )
        if collapsible and finer:
            grain = BuildGrain(components=entity_grain)
            full_outputs = list(outputs) + [
                o
                for o in pass_through
                if o.grain is not None and set(o.grain.components) <= entity_grain
            ]

    combined = combine_condition_atoms(
        [c for c in (conditions.conditional if conditions else None, intrinsic) if c]
    )
    combined_clause = BuildWhereClause(conditional=combined) if combined else None
    return FilterNode(
        input_concepts=parent_outputs_needed(full_outputs, parents, combined_clause),
        output_concepts=full_outputs,
        environment=environment,
        parents=parents,
        conditions=combined,
        force_group=grain is not None,
        grain=grain,
        preexisting_conditions=(
            preexisting_conditions.conditional if preexisting_conditions else None
        ),
    )
