"""Render Table elements to markdown and HTML."""

from __future__ import annotations

from decimal import Decimal
from html import escape
from typing import Any, List

from trilogy.rendering.base import prettify_label
from trilogy.rendering.rich_types import format_value, is_numeric
from trilogy.report.document import Table


def _header(name: str) -> str:
    """Humanize a column name for display, matching chart axis labels."""
    return prettify_label(name) or name


def _padded_types(table: Table) -> List[Any]:
    types = list(table.column_types)
    types += [None] * (len(table.columns) - len(types))
    return types


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def _numeric_columns(table: Table, types: List[Any]) -> List[bool]:
    """A column is numeric if its rich type says so, or all its values are."""
    flags: List[bool] = []
    for index in range(len(table.columns)):
        datatype = types[index]
        if datatype is not None and is_numeric(datatype):
            flags.append(True)
            continue
        values = [
            row[index]
            for row in table.rows
            if index < len(row) and row[index] is not None
        ]
        flags.append(bool(values) and all(_is_number(v) for v in values))
    return flags


def table_to_html(table: Table) -> str:
    types = _padded_types(table)
    numeric = _numeric_columns(table, types)
    head = "".join(
        (
            f'<th class="num">{escape(_header(c))}</th>'
            if numeric[i]
            else f"<th>{escape(_header(c))}</th>"
        )
        for i, c in enumerate(table.columns)
    )
    body = ""
    for row in table.rows:
        cells = ""
        for index in range(len(table.columns)):
            value = row[index] if index < len(row) else None
            text = escape(format_value(value, types[index]))
            cells += (
                f'<td class="num">{text}</td>' if numeric[index] else f"<td>{text}</td>"
            )
        body += f"<tr>{cells}</tr>"
    return (
        f'<table class="report-table"><thead><tr>{head}</tr></thead>'
        f"<tbody>{body}</tbody></table>"
    )


def table_to_markdown(table: Table) -> str:
    if not table.columns:
        return ""
    types = _padded_types(table)
    numeric = _numeric_columns(table, types)
    header = "| " + " | ".join(_header(c) for c in table.columns) + " |"
    divider = (
        "| "
        + " | ".join("---:" if numeric[i] else "---" for i in range(len(numeric)))
        + " |"
    )
    rows: List[str] = []
    for row in table.rows:
        cells = [
            format_value(row[i] if i < len(row) else None, types[i]).replace("|", "\\|")
            for i in range(len(table.columns))
        ]
        rows.append("| " + " | ".join(cells) + " |")
    return "\n".join([header, divider, *rows])
