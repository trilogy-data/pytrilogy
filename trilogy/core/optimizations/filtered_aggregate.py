from dataclasses import replace

from trilogy.core.enums import FunctionType, JoinType
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildFilterItem,
)
from trilogy.core.models.execute import CTE, Join, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import append_condition
from trilogy.core.processing.condition_utility import condition_proves_non_null

NULL_IGNORING_AGGREGATES = {
    FunctionType.AVG,
    FunctionType.COUNT,
    FunctionType.MAX,
    FunctionType.MIN,
    FunctionType.SUM,
}


def _filtered_aggregate(
    concept: BuildConcept,
) -> tuple[BuildConcept, BuildFilterItem] | None:
    lineage = concept.lineage
    if not isinstance(lineage, BuildAggregateWrapper):
        return None
    if len(lineage.function.arguments) != 1:
        return None
    argument = lineage.function.arguments[0]
    if not isinstance(argument, BuildConcept) or not isinstance(
        argument.lineage, BuildFilterItem
    ):
        return None
    if not isinstance(argument.lineage.content, BuildConcept):
        return None
    return argument, argument.lineage


def _remove_filter(
    cte: CTE, concept: BuildConcept, filtered: BuildConcept, item: BuildFilterItem
) -> None:
    """Swap ``concept`` in ``cte`` for a copy aggregating the unfiltered content;
    the concept itself is shared with every other holder of it."""
    assert isinstance(concept.lineage, BuildAggregateWrapper)
    assert isinstance(item.content, BuildConcept)
    function = replace(
        concept.lineage.function,
        arguments=[
            item.content if argument is filtered else argument
            for argument in concept.lineage.function.arguments
        ],
    )
    unfiltered = replace(concept, lineage=replace(concept.lineage, function=function))
    cte.output_columns = [
        unfiltered if c.address == concept.address else c for c in cte.output_columns
    ]
    cte.source.output_concepts = [
        unfiltered if c.address == concept.address else c
        for c in cte.source.output_concepts
    ]


def _global_rollup_ignores_null_groups(
    consumer: CTE, aggregate_addresses: set[str]
) -> bool:
    if (
        not consumer.group_to_grain
        or consumer.group_concepts
        or consumer.condition is not None
    ):
        return False
    outputs = [
        concept
        for concept in consumer.output_columns
        if not consumer.source_map.get(concept.address)
    ]
    if not outputs:
        return False
    for concept in outputs:
        lineage = concept.lineage
        if (
            not isinstance(lineage, BuildAggregateWrapper)
            or lineage.function.operator not in NULL_IGNORING_AGGREGATES
        ):
            return False
        arguments = {
            argument.address
            for argument in lineage.function.arguments
            if isinstance(argument, BuildConcept)
        }
        if not arguments or not arguments.issubset(aggregate_addresses):
            return False
    return True


class PushFilteredAggregateInput(OptimizationRule):
    """Apply a common aggregate filter before grouping when empty groups are rejected."""

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE) or not cte.group_to_grain:
            return False, None
        aggregates = [
            concept
            for concept in cte.output_columns
            if concept.is_aggregate and not cte.source_map.get(concept.address)
        ]
        if not aggregates:
            return False, None
        matches = [_filtered_aggregate(concept) for concept in aggregates]
        if any(match is None for match in matches):
            return False, None
        filtered = [match for match in matches if match is not None]
        predicate = filtered[0][1].where.conditional
        if any(item.where.conditional != predicate for _, item in filtered[1:]):
            return False, None
        consumers = inverse_map.get(cte.name, [])
        aggregate_addresses = {concept.address for concept in aggregates}
        self_rejects_empty_groups = cte.condition is not None and bool(
            aggregate_addresses.intersection(condition_proves_non_null(cte.condition))
        )
        if not self_rejects_empty_groups and (
            not consumers
            or any(
                not isinstance(consumer, CTE)
                or (
                    (
                        consumer.condition is None
                        or not aggregate_addresses.intersection(
                            condition_proves_non_null(consumer.condition)
                        )
                    )
                    and not _global_rollup_ignores_null_groups(
                        consumer, aggregate_addresses
                    )
                )
                for consumer in consumers
            )
        ):
            return False, None
        required = {
            argument.address
            for _, item in filtered
            for argument in [
                *item.content_concept_arguments,
                *item.where.row_arguments,
            ]
        }
        if not required or any(not cte.source_map.get(address) for address in required):
            return False, None
        for concept, (argument, item) in zip(aggregates, filtered):
            _remove_filter(cte, concept, argument, item)
        cte.condition = append_condition(cte.condition, predicate)
        return True, None


def _filtered_count(
    concept: BuildConcept,
) -> tuple[BuildConcept, BuildFilterItem] | None:
    """As `_filtered_aggregate`, restricted to COUNT."""
    lineage = concept.lineage
    if not isinstance(lineage, BuildAggregateWrapper):
        return None
    if lineage.function.operator != FunctionType.COUNT:
        return None
    return _filtered_aggregate(concept)


def _reads_source(
    cte: CTE, concept: BuildConcept, source: str, seen: frozenset[str] = frozenset()
) -> bool:
    """True when rendering ``concept`` in ``cte`` reaches a column of ``source``.

    Follows ``CTE.render_binding``: an outer-join key renders coalesced over its
    whole class, a bound column reads its own sources, and an unbound one reads
    through its lineage or through a pseudonym sibling this CTE does bind."""
    if concept.address in seen:
        return False
    seen = seen | {concept.address}
    for member in cte.outer_join_key_class(concept.address):
        if _reads_source(cte, member, source, seen):
            return True
    sources = cte.source_map.get(concept.address) or []
    if sources:
        return source in sources
    lineage = concept.lineage
    if lineage is not None:
        return any(
            _reads_source(cte, argument, source, seen)
            for argument in lineage.concept_arguments
        )
    return any(
        _reads_source(cte, other, source, seen)
        for other in cte.output_columns
        if other.canonical_address == concept.canonical_address
        or other.address in concept.pseudonyms
        or concept.address in other.pseudonyms
    )


def _other_right_readers(cte: CTE, aggregate: BuildConcept, source: str) -> bool:
    """True when anything but ``aggregate`` reads ``source``.

    Moving the filter onto the ON predicate leaves the count alone but stops a
    rejected right row from matching, so every other column read from that side
    renders NULL where it used to render the row's value."""
    readers = [
        concept
        for concept in cte.output_columns
        if concept.address != aggregate.address
    ]
    if cte.condition is not None:
        readers.extend(cte.condition.concept_arguments)
    for item in cte.order_by.items if cte.order_by else []:
        readers.extend(item.concept_arguments)
    return any(_reads_source(cte, concept, source) for concept in readers)


class PushFilteredCountIntoJoin(OptimizationRule):
    """Move a sole filtered COUNT onto its LEFT JOIN's ON predicate."""

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        # ungrouped, the COUNT renders per row, and a predicate moved onto the
        # ON changes which rows there are, not just the count
        if not isinstance(cte, CTE) or not cte.group_to_grain:
            return False, None
        joins = [join for join in cte.joins if isinstance(join, Join)]
        aggregates = [concept for concept in cte.output_columns if concept.is_aggregate]
        if len(joins) != 1 or len(aggregates) != 1:
            return False, None
        join = joins[0]
        if join.jointype != JoinType.LEFT_OUTER:
            return False, None
        match = _filtered_count(aggregates[0])
        if match is None:
            return False, None
        filtered, item = match
        right_source = cte.source_key_for(join.right_cte)
        required = {
            argument.address
            for argument in [
                *item.content_concept_arguments,
                *item.where.row_arguments,
            ]
        }
        if not required or any(
            right_source not in cte.source_map.get(address, ()) for address in required
        ):
            return False, None
        if _other_right_readers(cte, aggregates[0], right_source):
            return False, None
        _remove_filter(cte, aggregates[0], filtered, item)
        join.condition = append_condition(join.condition, item.where.conditional)
        return True, None
