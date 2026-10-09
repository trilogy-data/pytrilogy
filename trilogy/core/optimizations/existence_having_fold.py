"""Fold an existence scan into the aggregate it is joined to, as a HAVING term.

A consumer ``C`` INNER-joins an aggregate ``A`` grouped on key ``K`` to a row
scan ``R`` on ``K``. ``R`` reads the source ``P`` that ``A`` groups (a shared
parent CTE, or the same inlined table), filtered by a WHERE ``W``, plus a
dimension ``D`` keyed on ``K`` for the columns ``C`` wants. ``R`` then asks only one thing of ``P``: does ``K`` have a row passing ``W``.
``A`` already scans those rows, so

    count(CASE WHEN W THEN 1 END) > 0

in ``A``'s HAVING answers it, and ``R`` reads ``D`` alone. The aggregate itself
must not see ``W``: a flat WHERE never filters an aggregate it compares.

Sound when:

- ``A`` reads every row ``R`` would: ``A``'s joins only pad, and its own WHERE
  is either restated in ``W`` or reads ``K`` alone, which the join on ``K``
  applies anyway.
- each of ``R``'s joins other than ``D`` feeds ``W`` alone and, when INNER, is
  one whose row ``W`` cannot pass without (``W`` proves one of its columns
  non-null); ``A`` reads every column ``W`` does.
- ``C`` reads only ``K`` and ``D``'s columns off ``R``, and either ``R`` is
  unique on ``K`` or ``C`` deduplicates; ``R`` now has one row per ``K``.
"""

from __future__ import annotations

from trilogy.core.enums import (
    ComparisonOperator,
    Derivation,
    FunctionType,
    JoinType,
    Purpose,
)
from trilogy.core.models.build import (
    BoolExpr,
    BuildCaseWhen,
    BuildComparison,
    BuildConcept,
    BuildDatasource,
    BuildFunction,
    BuildParenthetical,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.execute import CTE, DatasourceCTE, Join, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import append_condition, is_sole_consumer
from trilogy.core.processing.condition_utility import (
    condition_proves_non_null,
    decompose_condition,
)


def _single_pair_inner(join: Join) -> bool:
    return (
        join.jointype == JoinType.INNER
        and not join.has_predicate
        and len(join.joinkey_pairs or []) == 1
    )


def _keyed_on(cte: CTE, key: BuildConcept) -> bool:
    grain = set(cte.grain.components)
    return bool(grain) and grain <= key.equivalent_addresses


def _reads_from(cte: CTE, name: str) -> set[str]:
    return {a for a, sources in cte.source_map.items() if name in sources}


def _consumed_from(consumer: CTE, name: str, key: BuildConcept) -> set[str]:
    """What `consumer` actually reads off `name`: its outputs and condition
    (its source map still lists columns pruning has not hidden yet)."""
    read = {c.address for c in consumer.output_columns}
    if consumer.condition is not None:
        read |= {c.address for c in consumer.condition.row_arguments}
    return (_reads_from(consumer, name) & read) | {key.address}


def _inlined_lookup(rows: CTE, join: Join) -> BuildDatasource | None:
    """The raw datasource an inlined join side renders from; `rows`' source map
    names its columns by that datasource's alias."""
    if join.right_cte not in rows.inlined_parents:
        return None
    lookup = join.right_cte
    assert isinstance(lookup, DatasourceCTE)
    for datasource in rows.source.datasources:
        if (
            isinstance(datasource, BuildDatasource)
            and datasource.identifier == lookup.datasource.identifier
        ):
            return datasource
    return None


def _join_side_name(rows: CTE, join: Join) -> str | None:
    raw = _inlined_lookup(rows, join)
    return raw.safe_identifier if raw is not None else None


def _existence_term(condition: BoolExpr) -> BuildComparison:
    comparison = (
        condition.content if isinstance(condition, BuildParenthetical) else condition
    )
    flag = BuildFunction(
        operator=FunctionType.CASE,
        arguments=[BuildCaseWhen(comparison=comparison, expr=1)],  # type: ignore[arg-type]
        output_data_type=DataType.INTEGER,
        output_purpose=Purpose.PROPERTY,
    )
    count = BuildFunction(
        operator=FunctionType.COUNT,
        arguments=[flag],
        output_data_type=DataType.INTEGER,
        output_purpose=Purpose.METRIC,
    )
    return BuildComparison(left=count, right=0, operator=ComparisonOperator.GT)


def _aggregate_reads_all(
    aggregate: CTE, key: BuildConcept, row_atoms: list[str]
) -> bool:
    if any(
        isinstance(j, Join) and j.jointype != JoinType.LEFT_OUTER
        for j in aggregate.joins
    ):
        return False
    if aggregate.semi_join_filters or aggregate.existence_source_map:
        return False
    where = aggregate.condition_placement.where
    if where is None:
        return True
    return all(
        str(atom) in row_atoms
        or {c.address for c in atom.row_arguments} <= key.equivalent_addresses
        for atom in decompose_condition(where)
    )


def _dimension_join(rows: CTE, key: BuildConcept) -> Join | None:
    found = None
    for join in rows.joins:
        if not isinstance(join, Join) or join.left_cte is not None:
            return None
        if _single_pair_inner(join) and (
            (join.joinkey_pairs or [])[0].left.equivalent_addresses
            & key.equivalent_addresses
        ):
            if found is not None:
                return None
            found = join
    return found


def _same_base(rows: CTE, aggregate: CTE, a_key: BuildConcept) -> bool:
    """`rows`' one non-joined source is where `aggregate` reads its key: the
    shared parent CTE, or the same raw table each inlined."""
    sides = {_join_side_name(rows, j) for j in rows.joins if isinstance(j, Join)}
    base = {name for names in rows.source_map.values() for name in names} - sides
    return (
        len(base) == 1
        and base == set(aggregate.source_map.get(a_key.address, []))
        and {p.name for p in rows.parent_ctes} <= base
    )


def _filter_only_joins_droppable(
    rows: CTE, dimension: Join, consumed: set[str], condition: BoolExpr
) -> bool:
    proven = condition_proves_non_null(condition)
    for join in rows.joins:
        if join is dimension:
            continue
        assert isinstance(join, Join)
        name = _join_side_name(rows, join)
        if name is None or join.left_cte is not None or join.has_predicate:
            return False
        reads = _reads_from(rows, name)
        if reads & consumed:
            return False
        if join.jointype == JoinType.INNER and not reads & proven:
            return False
        if join.jointype not in (JoinType.INNER, JoinType.LEFT_OUTER):
            return False
    return True


def _evaluable_in(aggregate: CTE, condition: BoolExpr) -> bool:
    return all(
        aggregate.source_map.get(c.address) or c.derivation == Derivation.CONSTANT
        for c in condition.row_arguments
    )


def _rebase_on_dimension(rows: CTE, dimension: Join, key: BuildConcept) -> None:
    raw = _inlined_lookup(rows, dimension)
    assert raw is not None
    alias = raw.safe_identifier
    keep = _reads_from(rows, alias) | key.equivalent_addresses
    rows.source_map = {
        a: ([alias] if a in key.equivalent_addresses else sources)
        for a, sources in rows.source_map.items()
        if a in keep
    }
    rows.joins = []
    rows.condition = None
    rows.source.condition = None
    rows.parent_ctes = []
    rows.inlined_parents = [dimension.right_cte]  # type: ignore[list-item]
    rows.source.joins = []
    rows.source.datasources = [raw]
    rows.source.base_datasource = raw
    rows.source.source_map = {a: {raw} for a in rows.source_map}
    rows.base_name_override = None
    rows.base_alias_override = alias
    if set(raw.grain.components) <= key.equivalent_addresses:
        rows.group_to_grain = False


class FoldExistenceIntoAggregate(OptimizationRule):
    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE):
            return False, None
        for join in cte.joins:
            if not (isinstance(join, Join) and _single_pair_inner(join)):
                continue
            pair = (join.joinkey_pairs or [])[0]
            sides = [(pair.cte.name, pair.left), (join.right_cte.name, pair.right)]
            for (a_name, a_key), (r_name, r_key) in (sides, sides[::-1]):
                if self._fold(cte, a_name, r_name, a_key, r_key, inverse_map):
                    return True, None
        return False, None

    def _fold(
        self,
        consumer: CTE,
        a_name: str,
        r_name: str,
        a_key: BuildConcept,
        r_key: BuildConcept,
        inverse_map: dict[str, list[CTE | UnionCTE]],
    ) -> bool:
        parents = {p.name: p for p in consumer.parent_ctes}
        aggregate, rows = parents.get(a_name), parents.get(r_name)
        if not (
            isinstance(aggregate, CTE)
            and isinstance(rows, CTE)
            and not isinstance(rows, DatasourceCTE)
            and aggregate.group_to_grain
            and _keyed_on(aggregate, a_key)
            and rows.condition is not None
            and not rows.semi_join_filters
            and not rows.existence_source_map
            and rows.condition_placement.having is None
            and rows.condition_placement.qualify is None
            and is_sole_consumer(consumer, aggregate, inverse_map)
            and is_sole_consumer(consumer, rows, inverse_map)
        ):
            return False
        condition = rows.condition
        dimension = _dimension_join(rows, r_key)
        dim_name = _join_side_name(rows, dimension) if dimension else None
        if (
            dimension is None
            or dim_name is None
            or not _same_base(rows, aggregate, a_key)
        ):
            return False
        consumed = _consumed_from(consumer, rows.name, r_key)
        dim_reads = _reads_from(rows, dim_name)
        if not consumed <= dim_reads | r_key.equivalent_addresses:
            return False
        if not (
            _keyed_on(rows, r_key)
            or (
                consumer.group_to_grain
                and all(
                    consumer.source_map.get(c.address) for c in consumer.output_columns
                )
            )
        ):
            return False
        row_atoms = [str(a) for a in decompose_condition(condition)]
        if not (
            _aggregate_reads_all(aggregate, a_key, row_atoms)
            and _evaluable_in(aggregate, condition)
            and _filter_only_joins_droppable(rows, dimension, consumed, condition)
        ):
            return False
        self.log(f"{rows.name}'s existence test folds into {aggregate.name}'s HAVING")
        term = _existence_term(condition)
        aggregate.condition = append_condition(aggregate.condition, term)
        _rebase_on_dimension(rows, dimension, r_key)
        return True
