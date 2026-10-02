from trilogy.core.models.build import (
    BuildConcept,
    BuildFilterItem,
    BuildGrain,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import (
    combine_condition_atoms,
)
from trilogy.core.processing.grain_utility import rows_unique_at_outputs
from trilogy.core.processing.nodes import FilterNode, StrategyNode
from trilogy.core.processing.v4_helper.functional_dependency import (
    build_fd_determines,
)
from trilogy.core.processing.v4_helper.keyspace import entity_keys
from trilogy.core.processing.v4_helper.projection import filter_row_predicate

from .common import parent_outputs_needed


def _rows_unique_at_set(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
) -> bool:
    """Whether the parents' rows are already one per set value. A filter
    emits its content column, so uniqueness is judged on that."""
    grains = [parent.grain for parent in parents]
    if any(grain is None for grain in grains):
        return False
    joined = BuildGrain(
        components=set().union(*(set(grain.components) for grain in grains if grain))
    )
    nullable = [c for parent in parents for c in parent.nullable_concepts]
    emitted = [
        (
            o.lineage.content
            if isinstance(o.lineage, BuildFilterItem)
            and isinstance(o.lineage.content, BuildConcept)
            else o
        )
        for o in outputs
    ]
    return rows_unique_at_outputs(joined, emitted, nullable, environment)


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

    A filter concept (`X ? COND`) is a VALUE: `X` where COND holds, NULL
    elsewhere, rendered as a per-row CASE that narrows no sibling's rows. The
    predicate moves into this node's WHERE only when the caller says the rows
    may narrow (`intrinsic_filter_pushdown`: the statement shows nothing but
    this predicate's filter values) or for a semijoin RHS (`existence_source`,
    where a CASE's NULL member would make `not in` match nothing).

    Parent outputs pass through so a consumer can reach back through the
    filter. A keyed filter whose predicate reads finer than its outputs'
    entity grain groups to that grain (the renderer MAX-collapses it), else the
    CASE fans a property into {content, NULL} per input row; not under an
    aggregate consumer (`collapse_to_grain`), which counts the per-row CASE.
    """
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
    intrinsic = filter_row_predicate(
        outputs, parents, intrinsic_filter_pushdown or existence_source
    )

    grain: BuildGrain | None = None
    if filter_lineages and intrinsic is None and collapse_to_grain:
        # projected or not, a property stands for its keys: `select name,
        # late_name` is at customer grain
        entity_grain: set[str] = set().union(
            *(entity_keys(o.address, environment) or {o.address} for o in outputs)
        )
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
    if (
        existence_source
        and grain is None
        and not _rows_unique_at_set(outputs, parents, environment)
    ):
        # a semijoin RHS is a set: emit it at the set's own grain, not the
        # scan's (`week_seq ? date in (...)` reads a date-grain scan)
        grain = BuildGrain(components={o.address for o in outputs})

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
