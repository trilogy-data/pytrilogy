"""Bind a table-function address's column arguments from a CTE's condition.

``address fn(latitude, longitude)`` declares that each scan of the datasource
is one call, and that ``latitude`` and ``longitude`` are inputs rather than
outputs: the query must pin each with ``= <literal>``. That literal becomes
the call's argument and is projected back as the column, so the planner's own
WHERE (which is kept) holds trivially and the concept stays joinable.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.enums import BooleanOperator, ComparisonOperator
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildParenthetical,
)
from trilogy.core.models.datasource import Address
from trilogy.dialect.source_pushdown import _column_for, _literal


class MissingSourceArgumentException(UnresolvableQueryException):
    pass


def _and_atoms(condition: Any) -> list[Any]:
    if isinstance(condition, BuildParenthetical):
        return _and_atoms(condition.content)
    if (
        isinstance(condition, BuildConditional)
        and condition.operator == BooleanOperator.AND
    ):
        return _and_atoms(condition.left) + _and_atoms(condition.right)
    return [condition]


def pinned_values(condition: Any, datasource: BuildDatasource) -> dict[str, list[Any]]:
    """Datasource column -> the literals ``column = literal`` atoms pin it to."""
    pinned: dict[str, list[Any]] = defaultdict(list)
    if condition is None:
        return pinned
    for atom in _and_atoms(condition):
        if not isinstance(atom, BuildComparison) or atom.operator != (
            ComparisonOperator.EQ
        ):
            continue
        for side, value in ((atom.left, atom.right), (atom.right, atom.left)):
            if not isinstance(side, BuildConcept) or _literal(value) is None:
                continue
            column = _column_for(datasource, side)
            if column is not None and value not in pinned[column]:
                pinned[column].append(value)
    return pinned


def bound_values(
    address: Address, condition: Any, datasource: BuildDatasource | None
) -> dict[str, Any]:
    """Each bound column's single pinned literal; raise naming the first that
    has none (or more than one)."""
    pinned = pinned_values(condition, datasource) if datasource else {}
    bound: dict[str, Any] = {}
    for column in address.bound_columns:
        values = pinned.get(column, [])
        if len(values) != 1:
            raise MissingSourceArgumentException(
                _message(address, column, values, datasource)
            )
        bound[column] = values[0]
    return bound


def _spelling(concept: BuildConcept) -> str:
    """The concept as a query would write it."""
    if concept.namespace == DEFAULT_NAMESPACE:
        return concept.name
    return concept.address


def _message(
    address: Address,
    column: str,
    values: list[Any],
    datasource: BuildDatasource | None,
) -> str:
    concept = column
    name = "a datasource"
    if datasource is not None:
        name = f"Datasource '{datasource.name}'"
        concept = next(
            (_spelling(c.concept) for c in datasource.columns if c.alias == column),
            column,
        )
    if values:
        return (
            f"{name} calls {address.location} once per scan, but the query pins"
            f" `{concept}` to {len(values)} values"
            f" ({', '.join(str(v) for v in values)}). Filter it to one."
        )
    return (
        f"{name} is the table function {address.location}, which needs"
        f" `{concept}` as an argument. Add a `where {concept} = <literal>`"
        " filter to the query."
    )
