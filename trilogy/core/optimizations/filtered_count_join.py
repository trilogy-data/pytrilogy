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
