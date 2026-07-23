from trilogy.core.enums import Derivation
from trilogy.core.models.build import (
    BuildConcept,
    BuildFilterItem,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import (
    combine_condition_atoms,
    is_scalar_condition,
)
from trilogy.core.processing.nodes import FilterNode, StrategyNode

from .common import parent_outputs_needed


def _has_concept_existence(where: BuildWhereClause) -> bool:
    """True only for a REAL subselect arg (`x in <other column/select>`) — one
    whose existence side carries concepts. A literal IN-list (`month in (1,2,3,4)`)
    is also modeled as a subselect comparison but has no existence concepts, so it
    is a plain scalar predicate safe to push into a WHERE."""
    return any(arg for tup in (where.existence_arguments or ()) for arg in tup)


def gen_filter(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
    intrinsic_filter_pushdown: bool = True,
    existence_source: bool = False,
) -> StrategyNode | None:
    """Apply a row filter over already-built parents. Filter doesn't reshape
    rows or remove columns — it just selects a subset of rows. So pass
    through every parent output as well as the filter's own primaries; any
    downstream consumer (e.g. an aggregate that needs a grain key) can then
    reach back through the filter without us having to predict its needs at
    build time. The optimizer will prune unused columns later.

    An ``existence_source`` filter is a semijoin RHS (`pcid in store_buyers`):
    its only consumer is the IN-subselect, which reads just the set value. Drop
    the pass-through so the filter's own concept is the sole output — then its
    single predicate pushes into a real WHERE (a pruned sub-query) below, instead
    of a row-preserving CASE over carried scan columns."""
    pass_through: list[BuildConcept] = []
    seen = {c.address for c in outputs}
    if not existence_source:
        for parent in parents:
            for output in parent.output_concepts:
                if output.address not in seen:
                    pass_through.append(output)
                    seen.add(output.address)
    full_outputs = list(outputs) + pass_through

    # A filter concept's own predicate (`filter X where COND`) restricts rows
    # when the concept is fetched as its own group — push it down from the
    # value-only `CASE WHEN COND THEN X ELSE NULL` projection into this node's
    # WHERE. (When the filter instead rides alongside a broader dimension whose
    # rows must survive, the planner folds it into a basic projection and never
    # routes here.) Push only when ALL of:
    #   * the group's filter outputs share a SINGLE distinct predicate. Multiple
    #     distinct predicates mean fused conditional-aggregate columns (e.g.
    #     `price ? channel=STORE`, `price ? channel=WEB`) that each render as
    #     their own CASE WHEN over the shared scan — AND-ing the mutually
    #     exclusive predicates into one WHERE would null out every row.
    #   * the predicate is a plain scalar with no existence/semijoin arg: a
    #     subselect needs its existence source wired as a side parent (which
    #     this generator doesn't do) and an aggregate predicate needs a HAVING.
    #   * every NON-filter output is a grain key of the filter's CONTENT (the
    #     concept being filtered) and not the content itself. Surviving rows
    #     still carry valid grain keys, so a key like `product_id` beside
    #     `name ? product_id%2=0` is safe. But a sibling that IS the content
    #     (selecting raw `order_id` beside `filter order_id where ...`, or
    #     q61's raw `ext_sales_price` beside `price ? promo`) or one finer than
    #     the content's grain (`order_id` beside `filter store_id where
    #     order_id%2=0`) must keep all rows — render those as a CASE WHEN by
    #     not pushing.
    # Then AND the single predicate with any injected query-level condition.
    filter_lineages = [
        o.lineage for o in outputs if isinstance(o.lineage, BuildFilterItem)
    ]
    content_args: set[str] = set()
    content_grain: set[str] = set()
    for lineage in filter_lineages:
        for arg in lineage.content_concept_arguments:
            content_args.add(arg.address)
            if arg.grain:
                content_grain |= set(arg.grain.components)
    pushable_siblings = content_grain - content_args
    non_filter_addrs = {
        o.address for o in outputs if not isinstance(o.lineage, BuildFilterItem)
    }
    distinct: dict[str, BuildWhereClause] = {}
    for lineage in filter_lineages:
        distinct.setdefault(str(lineage.where.conditional), lineage.where)
    intrinsic_atoms = []
    if (
        intrinsic_filter_pushdown
        and len(distinct) == 1
        and non_filter_addrs <= pushable_siblings
    ):
        where = next(iter(distinct.values()))
        cond = where.conditional
        if not _has_concept_existence(where) and is_scalar_condition(cond):
            intrinsic_atoms = [cond]

    # Aggregate predicate (`filter X where count(...) by ... > 1`): can't push as
    # a re-derived row WHERE, but when the parent already exposes the aggregate as
    # a precomputed column the predicate is a row filter over the joined stream.
    # Push it referencing that column instead of rendering a
    # `CASE WHEN agg THEN X ELSE NULL` projection, which leaks non-matching rows as
    # NULL groups. The aggregate need not be at the content's own grain — it may be
    # a coarser value bridged in through a fact (`product_name ? count(order) by
    # customer > 1` filters product rows by their customers' counts); applying the
    # predicate as a WHERE before the output dedup is still a correct row filter.
    # Gated: single predicate, no existence/semijoin arg, every referenced concept
    # already a parent output (so we never re-aggregate), and the predicate is
    # constant across the rows of every non-filter sibling — i.e. the grain the
    # predicate varies over (`pred_grain`, the union of its row args' grains) is a
    # subset of each sibling's grain. When it is, WHERE and the preserving CASE
    # agree, so pushing to WHERE just drops non-matching rows the dedup would drop
    # anyway (`customer ? count(order) by customer > 1`: pred at customer grain,
    # sibling customer). When the predicate reaches a FINER grain than a sibling
    # (`customer ? count > 1 and product_name = 'Mouse', customer`: pred spans
    # customer×product, sibling customer) a single customer can have both matching
    # and non-matching product rows — WHERE would drop the customer entirely, so we
    # must leave a preserving CASE. A sole filter output (no siblings) always
    # pushes.
    if not intrinsic_atoms and intrinsic_filter_pushdown and len(distinct) == 1:
        where = next(iter(distinct.values()))
        cond = where.conditional
        parent_outputs = {c.address for p in parents for c in p.output_concepts}
        agg_args = [
            r for r in where.row_arguments if r.derivation == Derivation.AGGREGATE
        ]
        pred_grain: set[str] = set()
        for r in where.row_arguments:
            if r.grain:
                pred_grain |= set(r.grain.components)
        siblings_constant = all(
            (sib.grain is not None) and pred_grain <= set(sib.grain.components)
            for sib in (environment.concepts[a] for a in non_filter_addrs)
        )
        if (
            not _has_concept_existence(where)
            and agg_args
            and all(r.address in parent_outputs for r in where.row_arguments)
            and siblings_constant
        ):
            intrinsic_atoms = [cond]

    combined = conditions.conditional if conditions else None
    if intrinsic_atoms:
        intrinsic = combine_condition_atoms(intrinsic_atoms)
        combined = combine_condition_atoms(
            [c for c in (combined, intrinsic) if c is not None]
        )

    combined_clause = BuildWhereClause(conditional=combined) if combined else None
    return FilterNode(
        input_concepts=parent_outputs_needed(full_outputs, parents, combined_clause),
        output_concepts=full_outputs,
        environment=environment,
        parents=parents,
        conditions=combined,
        preexisting_conditions=(
            preexisting_conditions.conditional if preexisting_conditions else None
        ),
    )
