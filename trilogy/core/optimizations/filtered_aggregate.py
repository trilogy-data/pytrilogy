from dataclasses import replace

from trilogy.core.enums import FunctionType
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildFilterItem,
)
from trilogy.core.models.execute import CTE, UnionCTE
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
    concept: BuildConcept, filtered: BuildConcept, item: BuildFilterItem
) -> None:
    assert isinstance(concept.lineage, BuildAggregateWrapper)
    assert isinstance(item.content, BuildConcept)
    function = replace(
        concept.lineage.function,
        arguments=[
            item.content if argument is filtered else argument
            for argument in concept.lineage.function.arguments
        ],
    )
    concept.lineage = replace(concept.lineage, function=function)


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
            _remove_filter(concept, argument, item)
        cte.condition = append_condition(cte.condition, predicate)
        return True, None
