"""The aggregates a window orders by, pinned to the tuple it ranks.

A bare aggregate in a window's ORDER BY inherits the grain of where it is
evaluated, and a window is evaluated per its arguments and partition
(`Windows.order_aggregate_per_partition`). That holds wherever the aggregate
sits in the order expression (`sum(y) / count(id)`) and however it is spelled:
`auto s <- sum(y); ... order by s` means what `order by sum(y)` means. The
parser's grain rule (`_reads_aggregate`) and the build
(`_window_order_by_items`) both read the expression this rewrite returns, so
they agree on which windows still inherit the select's grain.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace as dc_replace
from typing import Any

from trilogy.core.enums import AggregateGroupingMode, ConceptSource, FunctionClass
from trilogy.core.models.author import (
    AggregateGrouping,
    AggregateWrapper,
    CaseElse,
    CaseSimpleWhen,
    CaseWhen,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    Function,
    NavigationWindowItem,
    NumberingWindowItem,
    Parenthetical,
    UndefinedConcept,
)

Pin = Callable[[AggregateWrapper | Function], Any]
Lookup = Callable[[str], Concept | None]

_AGGREGATES = frozenset(FunctionClass.AGGREGATE_FUNCTIONS.value)


def window_anchors(window: NumberingWindowItem | NavigationWindowItem) -> list:
    """The tuple a window ranks: its arguments (a navigation window's operand)
    and partition. Empty when it has none to pin to."""
    if isinstance(window, NumberingWindowItem):
        return [*window.arguments, *window.over] if window.arguments else []
    if isinstance(window.content, (ConceptRef, Concept)):
        return [window.content, *window.over]
    return []


def is_bare_aggregate(expr: Any) -> bool:
    """An aggregate that takes the grain of where it is evaluated."""
    if isinstance(expr, AggregateWrapper):
        return not expr.by and expr.grouping == AggregateGroupingMode.STANDARD
    return isinstance(expr, Function) and expr.operator in _AGGREGATES


def pin_to(anchors: list) -> Pin:
    def pin(expr: AggregateWrapper | Function) -> AggregateWrapper:
        function = expr.function if isinstance(expr, AggregateWrapper) else expr
        return AggregateWrapper(function=function, by=list(anchors))

    return pin


def pin_to_spec(spec: AggregateGrouping) -> Pin:
    """Under a select's ROLLUP/CUBE/GROUPING SETS the order's aggregates
    compute in that one grouping pass."""

    def pin(expr: AggregateWrapper | Function) -> AggregateWrapper:
        function = expr.function if isinstance(expr, AggregateWrapper) else expr
        return AggregateWrapper(
            function=function,
            by=list(spec.by),
            grouping=spec.mode,
            grouping_sets=[list(g) for g in spec.grouping_sets],
        )

    return pin


def pin_order_aggregates(
    expr: Any, pin: Pin, lookup: Lookup, _seen: frozenset[str] = frozenset()
) -> Any:
    """`expr` with every bare aggregate it reads replaced by `pin(aggregate)`.
    A name defined outside the select whose lineage reads one is replaced by
    that pinned lineage; any other name, and anything outside a scalar
    expression, is kept."""
    if is_bare_aggregate(expr):
        return pin(expr)
    if isinstance(expr, AggregateWrapper):
        return expr
    if isinstance(expr, (ConceptRef, Concept)):
        if expr.address in _seen:
            return expr
        concept = lookup(expr.address)
        # a select's own output keeps the one value the select projects
        if (
            concept is None
            or isinstance(concept, UndefinedConcept)
            or concept.metadata.concept_source == ConceptSource.SELECT
        ):
            return expr
        lineage = concept.lineage
        pinned = pin_order_aggregates(lineage, pin, lookup, _seen | {expr.address})
        return expr if pinned is lineage else pinned

    def walk(child: Any) -> Any:
        return pin_order_aggregates(child, pin, lookup, _seen)

    if isinstance(expr, Function):
        arguments = [walk(a) for a in expr.arguments]
        if all(a is b for a, b in zip(arguments, expr.arguments)):
            return expr
        return dc_replace(expr, arguments=arguments)
    if isinstance(expr, Parenthetical):
        content = walk(expr.content)
        return expr if content is expr.content else dc_replace(expr, content=content)
    if isinstance(expr, (Comparison, Conditional)):
        left, right = walk(expr.left), walk(expr.right)
        if left is expr.left and right is expr.right:
            return expr
        return dc_replace(expr, left=left, right=right)
    if isinstance(expr, CaseWhen):
        comparison, result = walk(expr.comparison), walk(expr.expr)
        if comparison is expr.comparison and result is expr.expr:
            return expr
        return dc_replace(expr, comparison=comparison, expr=result)
    if isinstance(expr, CaseSimpleWhen):
        value, result = walk(expr.value_expr), walk(expr.expr)
        if value is expr.value_expr and result is expr.expr:
            return expr
        return dc_replace(expr, value_expr=value, expr=result)
    if isinstance(expr, CaseElse):
        result = walk(expr.expr)
        return expr if result is expr.expr else dc_replace(expr, expr=result)
    return expr
