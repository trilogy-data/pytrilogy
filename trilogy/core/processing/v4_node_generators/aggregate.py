from trilogy.core.enums import Derivation
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildGrain,
    BuildWhereClause,
    get_grouped_aggregate_wrapper,
    nonstandard_grouping_lineage,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import GroupNode, StrategyNode

from .common import parent_outputs_needed

_ROW_PRESERVING_AGGREGATE_INPUT_DERIVATIONS = {
    Derivation.ROOT,
    Derivation.BASIC,
    Derivation.FILTER,
}


def _add_render_inputs(
    concept: BuildConcept,
    input_concepts: list[BuildConcept],
    input_addresses: set[str],
    available_by_address: dict[str, BuildConcept],
    seen: set[str] | None = None,
) -> None:
    if concept.address in input_addresses:
        return
    seen = seen or set()
    if concept.address in seen:
        return
    seen.add(concept.address)
    available = available_by_address.get(concept.address)
    if available is not None:
        input_concepts.append(available)
        input_addresses.add(available.address)
        return
    if concept.lineage is None:
        return
    for arg in concept.lineage.concept_arguments:
        _add_render_inputs(
            arg, input_concepts, input_addresses, available_by_address, seen
        )


def _abstract_output_grain(
    outputs: list[BuildConcept], environment: BuildEnvironment
) -> bool:
    """Outputs at abstract grain are a single-row (`by *`) aggregate: it has
    no join key and is broadcast via a keyless FULL join. Surfacing a
    scoped-join key would re-grain it to that key, and the renderer's
    grain-match collapse would then silently strip the aggregate (q23)."""
    return BuildGrain.from_concepts(outputs, environment).abstract


def _renders_nonstandard_grouping(
    outputs: list[BuildConcept], upstream: set[str]
) -> bool:
    """A node that renders `GROUP BY ROLLUP/CUBE/GROUPING SETS` takes its
    GROUP BY from the aggregate wrapper's `by` list verbatim, so any other
    column surfaced on it is a bare, ungrouped projection (q05). The key still
    reaches join inference from the other side; this side keeps only its
    grouping keys."""
    return any(
        (wrapper := get_grouped_aggregate_wrapper(c)) is not None
        and wrapper.grouping.nulls_grouping_keys
        and c.address not in upstream
        for c in outputs
    )


def _splits_aggregate_groups(
    outputs: list[BuildConcept], member: str, environment: BuildEnvironment
) -> bool:
    """Whether surfacing `member` on an aggregating node adds a GROUP BY key.

    The rows ARE the groups, so a key the grain does not determine splits
    every one of them: the aggregate values come out finer and the final
    projection emits one row per split instead of re-aggregating. The key
    still reaches join inference from the other side of the relation."""
    # Local import: v4_helper imports this package.
    from trilogy.core.processing.v4_helper.functional_dependency import (
        build_fd_determines,
    )

    if not any(c.is_aggregate for c in outputs):
        return False
    axis = {c.address for c in outputs if not c.is_aggregate}
    # A mate of a grain component in the same authored key group names the
    # SAME axis, so surfacing it renames a key rather than adding one.
    for canonical, members in environment.scoped_join_key_groups.items():
        group = {canonical, *members}
        if group & axis:
            axis |= group
    return member not in axis and not build_fd_determines(
        environment, axis, member, include_empty_grain=False
    )


def outputs_with_scoped_join_mates(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
) -> list[BuildConcept]:
    """Surface every member of an authored coalescing join-key group
    (`union join a.k = b.k`) that a parent carries visibly.

    Join inference pairs the sides' visible outputs, so an aggregate carrying
    `ticket` as its join-back axis while its parent binds the mate
    `r.r_ticket` under a pseudonym would lose that key from the merge and
    cross-product the rows on whatever keys remain (q59, q17)."""
    group_mates = environment.distinct_scoped_join_group_mates()
    if not group_mates or _abstract_output_grain(outputs, environment):
        return outputs
    upstream = {c.address for parent in parents for c in parent.output_concepts}
    if _renders_nonstandard_grouping(outputs, upstream):
        return outputs
    present = {c.address for c in outputs}
    visible = {
        c.address
        for parent in parents
        for c in parent.output_concepts
        if c.address not in parent.hidden_concepts
    }
    result = list(outputs)
    for member in group_mates:
        if member in present or member not in visible:
            continue
        if _splits_aggregate_groups(outputs, member, environment):
            continue
        concept = environment.concepts.get(member)
        if concept is not None:
            result.append(concept)
    return result


def gen_aggregate(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """GROUP BY at the outputs' shared grain over already-built parents.

    Forces a real GROUP source_type when any output has non-standard
    grouping (ROLLUP/CUBE/GROUPING_SETS) — the GroupNode's grain-match
    shortcut would otherwise drop the GROUP BY entirely, losing the
    subtotal rows the rollup adds (q14)."""
    has_non_standard_grouping = any(
        nonstandard_grouping_lineage(c) is not None for c in outputs
    )
    outputs = outputs_with_scoped_join_mates(outputs, parents, environment)
    input_concepts = parent_outputs_needed(outputs, parents, conditions)
    input_addresses = {concept.address for concept in input_concepts}
    available_by_address = {
        concept.address: concept
        for parent in parents
        for concept in parent.output_concepts
    }

    for output in outputs:
        if not isinstance(output.lineage, BuildAggregateWrapper):
            continue
        for arg in output.lineage.function.arguments:
            if (
                isinstance(arg, BuildConcept)
                and arg.derivation in _ROW_PRESERVING_AGGREGATE_INPUT_DERIVATIONS
            ):
                _add_render_inputs(
                    arg, input_concepts, input_addresses, available_by_address
                )

    return GroupNode(
        output_concepts=outputs,
        input_concepts=input_concepts,
        environment=environment,
        parents=parents,
        conditions=conditions.conditional if conditions else None,
        preexisting_conditions=(
            preexisting_conditions.conditional if preexisting_conditions else None
        ),
        force_group=True if has_non_standard_grouping else None,
    )
