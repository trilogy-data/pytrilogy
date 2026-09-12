from trilogy.core.enums import FunctionType, JoinType
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildFilterItem,
)
from trilogy.core.models.execute import CTE, Join, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.filtered_aggregate import (
    _filtered_aggregate,
    _remove_filter,
)
from trilogy.core.optimizations.utils import append_condition


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


class PushFilteredCountIntoJoin(OptimizationRule):
    """Move a sole filtered COUNT onto its LEFT JOIN's ON predicate."""

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE):
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
        replacement = aggregates[0]
        _remove_filter(replacement, filtered, item)
        cte.output_columns = [
            replacement if concept is aggregates[0] else concept
            for concept in cte.output_columns
        ]
        cte.source.output_concepts = [
            replacement if concept.address == replacement.address else concept
            for concept in cte.source.output_concepts
        ]
        join.condition = append_condition(join.condition, item.where.conditional)
        return True, None
