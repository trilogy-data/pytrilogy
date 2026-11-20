"""Read one table once where two CTEs scan it alike.

Two scans of one datasource under one condition, at one grain, ungrouped and
unjoined, read the same rows. The planner names a scan before predicate
pushdown moves a WHERE onto it, so a scan filtered at plan time and one
filtered by pushdown can survive side by side, each projecting the columns
its own consumers read. The second folds into the first, which carries both
projections.
"""

from __future__ import annotations

from trilogy.core.constants import ALL_ROWS_ADDRESS
from trilogy.core.models.build import BuildConcept
from trilogy.core.models.execute import CTE, DatasourceCTE, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import repoint_consumers


def _read(cte: DatasourceCTE) -> tuple | None:
    """What a bare scan reads: one table under one condition."""
    if (
        cte.joins
        or cte.parent_ctes
        or cte.inlined_parents
        or cte.limit is not None
        or cte.order_by
    ):
        return None
    return (
        cte.datasource.identifier,
        str(cte.condition),
        frozenset(c.address for c in cte.partial_concepts),
        cte.base_name_override,
        cte.base_alias_override,
    )


def _signature(cte: DatasourceCTE) -> tuple | None:
    read = _read(cte)
    if read is None or cte.group_to_grain:
        return None
    return (*read, frozenset(cte.grain.components))


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


def _group_reads(group: DatasourceCTE) -> list[BuildConcept] | None:
    """The table columns `group` reads, when each output is one of them or
    computed directly from them; None when one is read another way (a stored
    aggregate bound under another address)."""
    reads = [
        c for c in group.datasource.output_concepts if group.source_map.get(c.address)
    ]
    names = {c.address for c in reads}
    for output in group.output_columns:
        if output.address in names:
            continue
        if (
            not {a.address for a in output.concept_arguments} - {ALL_ROWS_ADDRESS}
            <= names
        ):
            return None
    return reads


def _rebase_group(
    group: DatasourceCTE, rows: DatasourceCTE, reads: list[BuildConcept]
) -> None:
    """`group` aggregates the rows `rows` already reads: it groups `rows`
    instead of scanning the table again. The shared condition stays on `rows`."""
    present = {c.address for c in rows.output_columns}
    for column in reads:
        rows.hidden_concepts.discard(column.address)
        if column.address in present:
            continue
        rows.output_columns.append(column)
        rows.source.output_concepts.append(column)
        rows.source_map[column.address] = [rows.datasource.safe_identifier]
        rows.source.source_map[column.address] = {rows.datasource}
    # The driver's CTE list holds this object, so it becomes a plain CTE in
    # place rather than being replaced.
    group.__class__ = CTE  # type: ignore[assignment]
    del group.datasource
    group.parent_ctes = [rows]
    group.condition = None
    group.source.condition = None
    group.existence_source_map = {}
    group.source.datasources = [rows.source]
    group.source.base_datasource = None
    group.source_map = {c.address: [rows.name] for c in reads}
    group.source.source_map = {c.address: {rows.source} for c in reads}
    group.base_name_override = rows.name
    group.base_alias_override = rows.name


class MergeTwinScans(OptimizationRule):
    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, DatasourceCTE) or cte.name not in inverse_map:
            return False, None
        if cte.group_to_grain:
            return self._group_over_rows(cte, inverse_map)
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

    def _group_over_rows(
        self, cte: DatasourceCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        read = _read(cte)
        reads = _group_reads(cte)
        if (
            read is None
            or reads is None
            # a re-sum reads stored partials through its own source map
            or cte.rollup_concepts
            or cte.partial_sources
            or _inlined_anywhere(cte.name, inverse_map[cte.name])
        ):
            return False, None
        for consumers in inverse_map.values():
            for consumer in consumers:
                for rows in consumer.dependency_nodes():
                    if (
                        not isinstance(rows, DatasourceCTE)
                        or rows.name not in inverse_map
                        or _signature(rows) is None
                        or _read(rows) != read
                        or _inlined_anywhere(rows.name, inverse_map[rows.name])
                    ):
                        continue
                    self.log(f"Grouping {cte.name} over its twin rows {rows.name}")
                    _rebase_group(cte, rows, reads)
                    inverse_map[rows.name].append(cte)
                    return True, None
        return False, None
