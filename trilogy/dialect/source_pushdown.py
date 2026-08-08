"""Serialize a CTE's own limit and condition into script-datasource flags.

This is a *serializer*, not an optimizer. It does not decide what a script
scan should be filtered by -- the planner already put a ``condition`` and a
``limit`` on the node. All that happens here is expressing those in the
contract's grammar (see ``trilogy.io.contract``), and declining when they
cannot be expressed.

Declining is always safe for filters: the rendered SQL keeps its own WHERE, so
a pushed predicate is redundant and skipping one costs only the optimization.

A limit is different -- it is not redundant, because truncating the source
changes which rows exist. It rides along only when every one of these holds:

- the condition serialized **completely**, or there is none. A source that
  truncates to N rows and then has a leftover predicate applied returns fewer
  than N. (Same rule the script side applies in ``contract.effective_pushdown``.)
- the ``order_by`` serialized completely, or there is none. ``LIMIT`` renders
  after ``ORDER BY``, so the ordering has to travel *with* the limit -- then the
  source returns the same top N. An ordering that cannot travel keeps the limit
  home rather than blocking it outright.
- the CTE has no joins and no parents. A join can drop rows, so SQL would have
  had to read past N to produce N.
- the CTE is not grouping. N groups is not N input rows.

Ties are the one place this is visibly non-deterministic: ``order by state
limit 5`` over duplicate states has no single right answer, and the source's
five need not be SQL's five. That is SQL's own non-determinism, but it does mean
pushdown can change *which* equally-valid rows come back.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from trilogy.core.enums import BooleanOperator, ComparisonOperator, Ordering
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildParenthetical,
)
from trilogy.io.contract import OPERATORS, Filter, Sort, SourceRequest

# Only the plain directions travel. Every nulls-first/last variant changes which
# rows survive a LIMIT, and would have to be matched against pyarrow's null
# placement rather than assumed.
ASCENDING_ORDERS = frozenset({Ordering.ASCENDING})
DESCENDING_ORDERS = frozenset({Ordering.DESCENDING})

if TYPE_CHECKING:
    from trilogy.core.models.build import BuildDatasource
    from trilogy.core.models.execute import CTE

# The contract's own operator list, minus the word-spelled ones. A filter is
# rendered unspaced (``state=CA``) so it survives shell splitting as a single
# token; ``state like C%`` could not. This is a limit of the wire encoding, not
# a judgement about which predicates are worth pushing.
PUSHABLE_OPERATORS = frozenset(
    op for op in OPERATORS if not any(c.isalpha() or c == " " for c in op)
)

# Characters that would be mangled by, or could escape, the transport: a SQL
# string literal, then a shell command line, then shlex.split.
_UNSAFE_VALUE_CHARACTERS = set("'\"\\`$&|;<>()[]{}*?!#\n\r\t ")


def _operator_spelling(operator: ComparisonOperator | str) -> str | None:
    spelling = operator.value if isinstance(operator, ComparisonOperator) else operator
    return spelling if spelling in PUSHABLE_OPERATORS else None


def _is_transport_safe(rendered: str) -> bool:
    """Whether a rendered filter survives SQL-literal + shell + shlex intact.

    ``args`` is embedded in a SQL string literal, concatenated into a shell
    command (a ``shellfs`` pipe, or a ``cmd.exe`` call on Windows) and then
    split with ``shlex``. Rather than escape correctly for all three, push only
    values that need no escaping -- keeping the rendered SQL readable, which is
    worth more here than covering every literal.
    """
    return bool(rendered) and not (set(rendered) & _UNSAFE_VALUE_CHARACTERS)


def _literal(value: Any) -> str | None:
    """Render a literal for the wire, or None if it cannot be spelled."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, str):
        return value
    return None


def _conjuncts(condition: Any) -> list[Any] | None:
    """Flatten a top-level AND, or None if the shape is not a conjunction.

    The contract's filter list *is* a conjunction, so an OR has no faithful
    serialization and reports itself as unrepresentable rather than dropping a
    side.
    """
    if isinstance(condition, BuildParenthetical):
        return _conjuncts(condition.content)
    if isinstance(condition, BuildConditional):
        if condition.operator != BooleanOperator.AND:
            return None
        left, right = _conjuncts(condition.left), _conjuncts(condition.right)
        if left is None or right is None:
            return None
        return left + right
    return [condition]


def _column_for(datasource: BuildDatasource, concept: BuildConcept) -> str | None:
    from trilogy.core.models.execute import _datasource_column_for_concept

    alias = _datasource_column_for_concept(datasource, concept)
    return alias if isinstance(alias, str) else None


def _filter_from(
    comparison: BuildComparison, datasource: BuildDatasource
) -> Filter | None:
    operator = _operator_spelling(comparison.operator)
    if operator is None:
        return None
    left, right = comparison.left, comparison.right
    # Only `column <op> literal`; the mirrored form would need the operator
    # flipped, which is easy to get wrong for no additional coverage.
    if not isinstance(left, BuildConcept) or isinstance(right, BuildConcept):
        return None
    column = _column_for(datasource, left)
    if column is None or not _is_transport_safe(column):
        return None
    rendered = _literal(right)
    if rendered is None or not _is_transport_safe(rendered):
        return None
    return Filter(column, operator, right)


def serialize_condition(
    condition: Any, datasource: BuildDatasource
) -> tuple[tuple[Filter, ...], bool]:
    """``(filters, complete)`` for a condition. ``complete`` means every part
    of it is carried by ``filters``, which is what a limit may ride on."""
    if condition is None:
        return (), True
    conjuncts = _conjuncts(condition)
    if conjuncts is None:
        return (), False
    filters: list[Filter] = []
    complete = True
    for conjunct in conjuncts:
        found = (
            _filter_from(conjunct, datasource)
            if isinstance(conjunct, BuildComparison)
            else None
        )
        if found is None:
            complete = False
            continue
        filters.append(found)
    return tuple(filters), complete


def serialize_order_by(
    order_by: Any, datasource: BuildDatasource
) -> tuple[tuple[Sort, ...], bool]:
    """``(sorts, complete)`` for an ORDER BY.

    An ordering only travels intact if *every* key does -- a partial sort is a
    different ordering, not a weaker one.
    """
    if order_by is None:
        return (), True
    sorts: list[Sort] = []
    for item in order_by.items:
        if not isinstance(item.expr, BuildConcept):
            return (), False
        column = _column_for(datasource, item.expr)
        if column is None or not _is_transport_safe(column):
            return (), False
        descending = item.order in DESCENDING_ORDERS
        if not descending and item.order not in ASCENDING_ORDERS:
            # nulls-first/last variants change which rows a LIMIT keeps.
            return (), False
        sorts.append(Sort(column, descending))
    return tuple(sorts), True


def _limit_is_safe(cte: CTE, condition_complete: bool, order_complete: bool) -> bool:
    """A limit is not redundant -- it changes which rows exist -- so it rides
    along only when everything that decides *which* rows travels with it."""
    if cte.limit is None or not (condition_complete and order_complete):
        return False
    # A join can drop rows and grouping collapses them, so in both cases SQL
    # would have had to read past N inputs to produce N outputs.
    return not cte.joins and not cte.parent_ctes and not cte.group_to_grain


def request_for_cte(cte: CTE, datasource: BuildDatasource) -> SourceRequest | None:
    """The contract request this CTE's own condition, ordering and limit imply."""
    filters, condition_complete = serialize_condition(cte.condition, datasource)
    sorts, order_complete = serialize_order_by(cte.order_by, datasource)
    if not _limit_is_safe(cte, condition_complete, order_complete):
        # Without the limit an ordering buys the source nothing, and sorting
        # costs it its streaming.
        sorts, limit = (), None
    else:
        limit = cte.limit
    if not filters and limit is None:
        return None
    return SourceRequest(filters=filters, order_by=sorts, limit=limit)


def render_args(request: SourceRequest | None) -> str:
    """The command line for a request, as the ``uv_run`` macro's ``args``."""
    if request is None:
        return ""
    parts: list[str] = []
    for predicate in request.filters:
        rendered = _literal(predicate.value)
        if rendered is None:
            continue
        # Quoted because the operator glyphs `<` and `>` are redirects to the
        # POSIX shell that runs the shellfs pipe -- unquoted, `--filter id>30`
        # reaches the script as `id` plus a file named `30`. Single quotes, not
        # double: the Windows transport wraps the whole arg string in double
        # quotes for cmd.exe. Column and value are transport-safe, so the token
        # can never carry a quote of its own.
        parts.append(f"--filter '{predicate.column}{predicate.op}{rendered}'")
    if request.order_by:
        # Must accompany the limit: without it the source truncates an
        # arbitrary N rows rather than the top N.
        parts.append("--order-by " + ",".join(s.render() for s in request.order_by))
    if request.limit is not None:
        parts.append(f"--limit {request.limit}")
    return " ".join(parts)
