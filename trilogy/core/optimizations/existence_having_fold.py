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

from dataclasses import replace

from trilogy.core.enums import (
    ComparisonOperator,
    Derivation,
    FunctionType,
    JoinType,
    Modifier,
    Purpose,
)
from trilogy.core.models.build import (
    BoolExpr,
    BuildAggregateWrapper,
    BuildCaseWhen,
    BuildComparison,
    BuildConcept,
    BuildDatasource,
    BuildFilterItem,
    BuildFunction,
    BuildParamaterizedConceptReference,
    BuildParenthetical,
    BuildSubselectComparison,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.execute import (
    CTE,
    DatasourceCTE,
    Join,
    UnionCTE,
    pair_matches_nulls,
)
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import append_condition, is_sole_consumer
from trilogy.core.processing.condition_utility import (
    condition_proves_non_null,
    decompose_condition,
)


def _single_pair_inner(join: Join) -> bool:
    return (
        join.join_type == JoinType.INNER
        and not join.has_predicate
        and len(join.pairs or []) == 1
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
    if join.right not in rows.inlined_parents:
        return None
    lookup = join.right
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


NULL_PROPAGATING = {
    FunctionType.ADD,
    FunctionType.SUBTRACT,
    FunctionType.MULTIPLY,
    FunctionType.DIVIDE,
}
NULL_IGNORING_AGGREGATES = {
    FunctionType.SUM,
    FunctionType.COUNT,
    FunctionType.AVG,
    FunctionType.MIN,
    FunctionType.MAX,
}


def _same_value(left: object, right: object) -> bool:
    left, right = (
        v.concept if isinstance(v, BuildParamaterizedConceptReference) else v
        for v in (left, right)
    )
    if isinstance(left, BuildConcept) or isinstance(right, BuildConcept):
        return (
            isinstance(left, BuildConcept)
            and isinstance(right, BuildConcept)
            and left.address == right.address
        )
    return left == right


def _equality(atom: object) -> tuple[str, object] | None:
    if (
        type(atom) is BuildComparison
        and atom.operator == ComparisonOperator.EQ
        and isinstance(atom.left, BuildConcept)
    ):
        return atom.left.address, atom.right
    return None


def _implies(conjunct: object, atom: BoolExpr) -> bool:
    """`conjunct` true forces `atom` true: the same atom, or `col = v` against
    `col = v` / `col in (.., v, ..)`."""
    if str(conjunct) == str(atom):
        return True
    equality = _equality(conjunct)
    if equality is None:
        return False
    column, value = equality
    if isinstance(atom, BuildSubselectComparison):
        return (
            atom.operator == ComparisonOperator.IN
            and isinstance(atom.left, BuildConcept)
            and atom.left.address == column
            and isinstance(atom.right, (tuple, list))
            and any(_same_value(value, v) for v in atom.right)
        )
    other = _equality(atom)
    return other is not None and other[0] == column and _same_value(value, other[1])


def _null_unless(expr: object, atom: BoolExpr) -> bool:
    """`expr` is NULL on every row where `atom` is not true."""
    if isinstance(expr, BuildConcept):
        return _null_unless(expr.lineage, atom)
    if isinstance(expr, BuildFilterItem):
        return any(
            _implies(c, atom) for c in decompose_condition(expr.where.conditional)
        ) or _null_unless(expr.content, atom)
    if isinstance(expr, BuildFunction) and expr.operator in NULL_PROPAGATING:
        return any(_null_unless(arg, atom) for arg in expr.arguments)
    return False


def _aggregates_ignore(aggregate: CTE, condition: BoolExpr) -> bool:
    """Every aggregate `aggregate` computes reads NULL off a row failing
    `condition`, so filtering its input by `condition` changes no value."""
    atoms = decompose_condition(condition)
    computed = [
        c
        for c in aggregate.output_columns
        if not aggregate.source_map.get(c.address)
        and isinstance(c.lineage, BuildAggregateWrapper)
    ]
    return bool(computed) and all(
        c.lineage.function.operator in NULL_IGNORING_AGGREGATES
        and len(c.lineage.function.arguments) == 1
        and all(_null_unless(c.lineage.function.arguments[0], a) for a in atoms)
        for c in computed
        if isinstance(c.lineage, BuildAggregateWrapper)
    )


def _aggregate_reads_all(
    aggregate: CTE, key: BuildConcept, row_atoms: list[str]
) -> bool:
    if any(
        isinstance(j, Join) and j.join_type != JoinType.LEFT_OUTER
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
        if not isinstance(join, Join) or join.left is not None:
            return None
        if (
            join.join_type in (JoinType.INNER, JoinType.LEFT_OUTER)
            and not join.has_predicate
            and len(join.pairs or []) == 1
            and (join.pairs or [])[0].left.equivalent_addresses
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
        if name is None or join.left is not None or join.has_predicate:
            return False
        reads = _reads_from(rows, name)
        if reads & consumed:
            return False
        if join.join_type == JoinType.INNER and not reads & proven:
            return False
        if join.join_type not in (JoinType.INNER, JoinType.LEFT_OUTER):
            return False
    return True


def _evaluable_in(aggregate: CTE, condition: BoolExpr) -> bool:
    return all(
        aggregate.source_map.get(c.address) or c.derivation == Derivation.CONSTANT
        for c in condition.row_arguments
    )


def _rebase_on_dimension(
    rows: CTE, dimension: Join, key: BuildConcept, aggregate: CTE
) -> None:
    """An INNER dimension holds exactly the keys the consumer's join keeps, so
    `rows` reads it alone. A LEFT one also yields keys it lacks (and a NULL
    key), so `rows` reads the aggregate's keys padded by it."""
    raw = _inlined_lookup(rows, dimension)
    assert raw is not None
    alias = raw.safe_identifier
    padded = dimension.join_type == JoinType.LEFT_OUTER
    key_source = aggregate.name if padded else alias
    keep = _reads_from(rows, alias) | key.equivalent_addresses
    rows.source_map = {
        a: ([key_source] if a in key.equivalent_addresses else sources)
        for a, sources in rows.source_map.items()
        if a in keep
    }
    rows.condition = None
    rows.source.condition = None
    rows.inlined_parents = [dimension.right]  # type: ignore[list-item]
    if padded:
        pair = (dimension.pairs or [])[0]
        rows.joins = [
            replace(
                dimension,
                pairs=[replace(pair, node=aggregate)],
            )
        ]
        rows.parent_ctes = [aggregate]
        rows.source.datasources = [aggregate.source, raw]
        rows.source.base_datasource = None
        rows.source.source_map = {
            a: {aggregate.source if sources == [aggregate.name] else raw}
            for a, sources in rows.source_map.items()
        }
        rows.base_name_override = aggregate.name
        rows.base_alias_override = aggregate.name
        if set(raw.grain.components) <= key.equivalent_addresses:
            rows.group_to_grain = False
        return
    rows.joins = []
    rows.parent_ctes = []
    rows.source.datasources = [raw]
    rows.source.base_datasource = raw
    rows.source.source_map = {a: {raw} for a in rows.source_map}
    rows.base_name_override = None
    rows.base_alias_override = alias
    if set(raw.grain.components) <= key.equivalent_addresses:
        rows.group_to_grain = False


def _tighten_rejected_padding(aggregate: CTE, condition: BoolExpr) -> None:
    """A LEFT join whose padded rows the moved WHERE now rejects is INNER, which
    lets a dimension it filters sink into a union below."""
    proven = condition_proves_non_null(condition)
    for join in aggregate.joins:
        if not (isinstance(join, Join) and join.join_type == JoinType.LEFT_OUTER):
            continue
        raw = _inlined_lookup(aggregate, join)
        if raw is None or not _reads_from(aggregate, raw.safe_identifier) & proven:
            continue
        join.join_type = JoinType.INNER


def _read_through_rows(
    consumer: CTE, join: Join, aggregate: CTE, rows: CTE, key: BuildConcept
) -> bool:
    """`rows`, rebased on `aggregate` padded by a dimension unique on the key,
    has exactly one row per aggregate row: the consumer's join back onto the
    aggregate pairs each row with itself, so `rows` carries the aggregate's
    columns and the consumer reads it alone. A plain `=` join also drops the
    aggregate's NULL key, so it collapses only when that key cannot be NULL."""
    if rows.group_to_grain or any(j is not join for j in consumer.joins):
        return False
    pair = (join.pairs or [])[0]
    where = aggregate.condition_placement.where
    if not (
        pair_matches_nulls(pair)
        or Modifier.NULLABLE in join.modifiers
        or (
            where is not None
            and condition_proves_non_null(where) & key.equivalent_addresses
        )
    ):
        return False
    carried = {
        a for a, sources in consumer.source_map.items() if aggregate.name in sources
    }
    present = {c.address for c in rows.output_columns}
    for column in aggregate.output_columns:
        if column.address in carried and column.address not in present:
            rows.output_columns.append(column)
            rows.source.output_concepts.append(column)
            rows.source_map[column.address] = [aggregate.name]
            rows.source.source_map[column.address] = {aggregate.source}
            rows.hidden_concepts.discard(column.address)
    consumer.joins = []
    consumer.parent_ctes = [rows]
    consumer.source.datasources = [rows.source]
    consumer.source.base_datasource = None
    consumer.source_map = {
        a: ([rows.name] if sources else sources)
        for a, sources in consumer.source_map.items()
    }
    consumer.source.source_map = {
        a: ({rows.source} if sources else sources)
        for a, sources in consumer.source.source_map.items()
    }
    consumer.base_name_override = rows.name
    consumer.base_alias_override = rows.name
    return True


def _join_dimension_directly(consumer: CTE, rows: CTE, dimension: Join) -> None:
    """An INNER dimension holds exactly the keys the consumer's join keeps, and
    the consumer reads only its columns off `rows`: the consumer joins the
    dimension's own datasource CTE in `rows`' place, which datasource inlining
    then folds into the consumer."""
    lookup = dimension.right
    for join in consumer.joins:
        if not isinstance(join, Join):
            continue
        if join.right is rows:
            join.right = lookup
        if join.left is rows:
            join.left = lookup
        join.pairs = [
            (replace(pair, node=lookup) if pair.node is rows else pair)
            for pair in join.pairs or []
        ]
    consumer.parent_ctes = [lookup if p is rows else p for p in consumer.parent_ctes]
    consumer.source_map = {
        a: [lookup.name if name == rows.name else name for name in sources]
        for a, sources in consumer.source_map.items()
    }
    source = consumer.source
    source.datasources = [
        lookup.source if d is rows.source else d for d in source.datasources
    ]
    source.source_map = {
        a: {lookup.source if d is rows.source else d for d in sources}
        for a, sources in source.source_map.items()
    }
    if source.base_datasource is rows.source:
        source.base_datasource = lookup.source
    if consumer.base_name_override == rows.name:
        consumer.base_name_override = lookup.name
    if consumer.base_alias_override == rows.name:
        consumer.base_alias_override = lookup.name


class FoldExistenceIntoAggregate(OptimizationRule):
    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE):
            return False, None
        for join in cte.joins:
            if not (isinstance(join, Join) and _single_pair_inner(join)):
                continue
            pair = (join.pairs or [])[0]
            sides = [(pair.node.name, pair.left), (join.right.name, pair.right)]
            for (a_name, a_key), (r_name, r_key) in (sides, sides[::-1]):
                if self._fold(cte, join, a_name, r_name, a_key, r_key, inverse_map):
                    return True, None
        return False, None

    def _fold(
        self,
        consumer: CTE,
        join: Join,
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
        # an aggregate blind to the rows W drops may filter its input instead,
        # which also leaves only keys with a passing row
        if _aggregates_ignore(aggregate, condition):
            aggregate.condition = append_condition(aggregate.condition, condition)
            _tighten_rejected_padding(aggregate, condition)
        else:
            aggregate.condition = append_condition(
                aggregate.condition, _existence_term(condition)
            )
        if dimension.join_type == JoinType.INNER:
            _join_dimension_directly(consumer, rows, dimension)
            return True
        _rebase_on_dimension(rows, dimension, r_key, aggregate)
        _read_through_rows(consumer, join, aggregate, rows, r_key)
        return True
