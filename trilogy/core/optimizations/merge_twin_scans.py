"""Read one table once where two CTEs scan it alike.

Two scans of one datasource under one condition, at one grain, ungrouped and
unjoined, read the same rows. The planner names a scan before predicate
pushdown moves a WHERE onto it, so a scan filtered at plan time and one
filtered by pushdown can survive side by side, each projecting the columns
its own consumers read. The second folds into the first, which carries both
projections.
"""

from __future__ import annotations

from trilogy.core.models.execute import CTE, DatasourceCTE, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import repoint_consumers


def _signature(cte: DatasourceCTE) -> tuple | None:
    if (
        cte.joins
        or cte.parent_ctes
        or cte.inlined_parents
        or cte.group_to_grain
        or cte.limit is not None
        or cte.order_by
    ):
        return None
    return (
        cte.datasource.identifier,
        str(cte.condition),
        frozenset(cte.grain.components),
        frozenset(c.address for c in cte.partial_concepts),
        cte.base_name_override,
        cte.base_alias_override,
    )


def _inlined_anywhere(name: str, consumers: list[CTE | UnionCTE]) -> bool:
    return any(
        isinstance(c, CTE) and any(p.name == name for p in c.inlined_parents)
        for c in consumers
    )


def _absorb(keep: DatasourceCTE, drop: DatasourceCTE) -> None:
    present = {c.address for c in keep.output_columns}
    for column in drop.output_columns:
        if column.address in present:
            continue
        keep.output_columns.append(column)
        keep.source_map.setdefault(column.address, drop.source_map[column.address])
        keep.source.output_concepts.append(column)
        if column.address in drop.source.source_map:
            keep.source.source_map.setdefault(
                column.address, drop.source.source_map[column.address]
            )
    nullable = {c.address for c in keep.nullable_concepts}
    keep.nullable_concepts += [
        c for c in drop.nullable_concepts if c.address not in nullable
    ]
    keep.hidden_concepts = keep.hidden_concepts & drop.hidden_concepts


class MergeTwinScans(OptimizationRule):
    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, DatasourceCTE) or cte.name not in inverse_map:
            return False, None
        signature = _signature(cte)
        if signature is None or _inlined_anywhere(cte.name, inverse_map[cte.name]):
            return False, None
        for consumers in inverse_map.values():
            for consumer in consumers:
                for twin in consumer.dependency_nodes():
                    if (
                        twin is cte
                        or not isinstance(twin, DatasourceCTE)
                        or twin.name == cte.name
                        or twin.name not in inverse_map
                        or _signature(twin) != signature
                        or _inlined_anywhere(twin.name, inverse_map[twin.name])
                    ):
                        continue
                    self.log(f"Folding scan {cte.name} into its twin {twin.name}")
                    _absorb(twin, cte)
                    repoint_consumers(cte, twin, inverse_map)
                    return True, {cte.name: twin.name}
        return False, None
