"""Exhaustiveness matrix for the aggregate-wrapper tree walk in
``select_finalize``.

``_collect_rollup_wrappers`` / ``_collect_standard_grouping_wrappers`` must find a
ROLLUP / ``grouping()`` aggregate however deeply it is nested in an expression —
inside ``coalesce(...)``, a ``case``, a ``def`` macro, a window, a filter, a list
literal, etc. The walk is a hand-written dispatch over the ``Expr`` union, so it
rots silently when a new composite node type is added. These tests pin every
container type AND guard the ``Expr`` union itself against drift: add a new member
and ``test_expr_union_exhaustively_covered`` fails until it is handled here and in
``_child_exprs``.

History: a ``def`` macro wrapper (``coalesce(@rollup_agg(x), 0)``) was missed
because ``FunctionCallWrapper`` had no branch — q05 burned 4.45M tokens / 75
iterations on that single gap before it was found.
"""

from __future__ import annotations

import datetime
import typing

import pytest

from trilogy.core.enums import (
    AggregateGroupingMode,
    BooleanOperator,
    ComparisonOperator,
    FunctionType,
    Purpose,
    WindowType,
)
from trilogy.core.models.author import (
    AggregateWrapper,
    Between,
    CaseElse,
    CaseWhen,
    Comparison,
    ConceptRef,
    Conditional,
    Expr,
    FilterItem,
    Function,
    FunctionCallWrapper,
    MagicConstants,
    NavigationWindowItem,
    NumberingWindowItem,
    Parenthetical,
    SubselectItem,
    WhereClause,
)
from trilogy.core.models.core import DataType, ListWrapper, MapWrapper, TupleWrapper
from trilogy.core.having_normalization import (
    _child_exprs,
    _collect_aggregate_wrappers,
    _collect_standard_grouping_wrappers,
)

_DT = DataType.FLOAT
_REF = ConceptRef(address="local.x")


def _collect_rollup_wrappers(node) -> list[AggregateWrapper]:
    return _collect_aggregate_wrappers(
        node,
        lambda n: isinstance(n, AggregateWrapper)
        and n.grouping != AggregateGroupingMode.STANDARD,
    )


def _rollup() -> AggregateWrapper:
    return AggregateWrapper(
        function=Function(
            operator=FunctionType.SUM,
            output_datatype=_DT,
            output_purpose=Purpose.METRIC,
            arguments=[_REF],
        ),
        grouping=AggregateGroupingMode.ROLLUP,
    )


def _grouping() -> AggregateWrapper:
    return AggregateWrapper(
        function=Function(
            operator=FunctionType.GROUPING,
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.METRIC,
            arguments=[_REF],
        ),
        grouping=AggregateGroupingMode.STANDARD,
    )


def _where() -> WhereClause:
    return WhereClause(
        conditional=Comparison(left=_REF, right=0, operator=ComparisonOperator.GT)
    )


# Each composite Expr container -> a builder that wraps a sentinel inside it.
_WRAPPERS: dict[type, typing.Callable[[Expr], Expr]] = {
    Function: lambda s: Function(
        operator=FunctionType.COALESCE,
        output_datatype=_DT,
        output_purpose=Purpose.METRIC,
        arguments=[s, 0],
    ),
    AggregateWrapper: lambda s: AggregateWrapper(
        function=Function(
            operator=FunctionType.SUM,
            output_datatype=_DT,
            output_purpose=Purpose.METRIC,
            arguments=[s],
        )
    ),
    Comparison: lambda s: Comparison(left=s, right=0, operator=ComparisonOperator.GT),
    Conditional: lambda s: Conditional(
        left=s, right=_REF, operator=BooleanOperator.AND
    ),
    Between: lambda s: Between(left=s, low=0, high=0),
    Parenthetical: lambda s: Parenthetical(content=s),
    CaseWhen: lambda s: CaseWhen(
        comparison=Comparison(left=_REF, right=0, operator=ComparisonOperator.GT),
        expr=s,
    ),
    CaseElse: lambda s: CaseElse(expr=s),
    FunctionCallWrapper: lambda s: FunctionCallWrapper(
        content=s, name="m", args=[_REF]
    ),
    TupleWrapper: lambda s: TupleWrapper(val=(s,), type=_DT),
    ListWrapper: lambda s: ListWrapper([s], type=_DT),
    MapWrapper: lambda s: MapWrapper(
        {"k": s}, key_type=DataType.STRING, value_type=_DT
    ),
    FilterItem: lambda s: FilterItem(content=s, where=_where()),
    NavigationWindowItem: lambda s: NavigationWindowItem(
        type=WindowType.LAG, content=s, order_by=[]
    ),
    NumberingWindowItem: lambda s: NumberingWindowItem(
        type=WindowType.RANK, arguments=[s], order_by=[]
    ),
}

# Leaves carry no nested expression; SubselectItem is descended (its `content`)
# but that content is always a ConceptRef leaf, so it cannot itself hold a nested
# aggregate — covered by the walk, excluded from the wrap-and-find matrix.
_LEAF_TYPES = {
    MagicConstants,
    bool,
    int,
    str,
    float,
    datetime.date,
    datetime.datetime,
    ConceptRef,
}
_COMPOSITE_TYPES = set(_WRAPPERS) | {SubselectItem}


@pytest.mark.parametrize("container", list(_WRAPPERS), ids=lambda c: c.__name__)
def test_rollup_found_in_every_container(container):
    sentinel = _rollup()
    assert sentinel in _collect_rollup_wrappers(_WRAPPERS[container](sentinel))


@pytest.mark.parametrize("container", list(_WRAPPERS), ids=lambda c: c.__name__)
def test_grouping_found_in_every_container(container):
    sentinel = _grouping()
    assert sentinel in _collect_standard_grouping_wrappers(
        _WRAPPERS[container](sentinel)
    )


def test_deeply_nested_rollup_found():
    # coalesce(case when .. then @macro(sum(x) by rollup), 0) — the q05 shape.
    sentinel = _rollup()
    macro = FunctionCallWrapper(content=sentinel, name="m", args=[_REF])
    case = CaseElse(expr=macro)
    paren = Parenthetical(content=case)
    coalesce = Function(
        operator=FunctionType.COALESCE,
        output_datatype=_DT,
        output_purpose=Purpose.METRIC,
        arguments=[paren, 0],
    )
    assert _collect_rollup_wrappers(coalesce) == [sentinel]


def test_rollup_walk_ignores_standard_aggregate():
    # A plain STANDARD sum is not a rollup; grouping() is not collected as rollup.
    assert _collect_rollup_wrappers(_grouping()) == []
    plain = AggregateWrapper(
        function=Function(
            operator=FunctionType.SUM,
            output_datatype=_DT,
            output_purpose=Purpose.METRIC,
            arguments=[_REF],
        )
    )
    assert _collect_rollup_wrappers(plain) == []


def test_subselect_content_is_descended():
    # SubselectItem can't hold a nested aggregate (content is a ConceptRef) but
    # must still be descended so it is not a silent dead-end in the walk.
    assert _REF in list(_child_exprs(SubselectItem(content=_REF)))


def test_expr_union_exhaustively_covered():
    members = set(typing.get_args(Expr))
    expected = _LEAF_TYPES | _COMPOSITE_TYPES
    missing = members - expected
    extra = expected - members
    assert members == expected, (
        "The `Expr` union changed. Add the new type to `_child_exprs` (and a "
        "builder here if it can contain a nested aggregate), then to `_LEAF_TYPES` "
        f"or `_COMPOSITE_TYPES`. unhandled-new={missing} stale={extra}"
    )
