"""Document model and markdown parsing for Trilogy reports."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from trilogy.core.enums import ChartType


@dataclass
class Prose:
    """A run of raw markdown text."""

    markdown: str


@dataclass
class TrilogyBlock:
    """A fenced ```trilogy code block, prior to execution."""

    code: str


@dataclass
class RowBlock:
    """A `:::row ... :::` container; children render side by side."""

    segments: list[Segment]


@dataclass
class Table:
    """A rendered select result.

    `column_types` is parallel to `columns`; each entry is the column's
    datatype (which may be a rich/trait type) or None when unknown.
    """

    columns: list[str]
    rows: list[list[Any]]
    column_types: list[Any] = field(default_factory=list)


@dataclass
class Chart:
    """A rendered chart statement; `chart` is an Altair chart object.

    `chart_type` is the first layer's type, used to drive layout decisions.
    """

    chart: Any
    chart_type: ChartType | None = None


@dataclass
class ErrorBox:
    """An execution error surfaced inline in the report."""

    message: str


@dataclass
class RenderedRow:
    """An executed RowBlock; elements render side by side."""

    elements: list[RenderedElement]


# Segments are the raw parse output; elements are what backends render.
Segment = Prose | TrilogyBlock | RowBlock
RenderedElement = Prose | Table | Chart | ErrorBox | RenderedRow

_FENCE_OPEN = re.compile(r"^\s*(`{3,}|~{3,})\s*([^`]*)$")
_ROW_OPEN = {":::row", "::: row"}
_ROW_CLOSE = ":::"


def _is_close(line: str, fence: str) -> bool:
    stripped = line.strip()
    return len(stripped) >= len(fence) and set(stripped) == {fence[0]}


def _is_trilogy(info: str) -> bool:
    tokens = info.split()
    return bool(tokens) and tokens[0].lower() == "trilogy"


def parse_markdown(text: str) -> list[Segment]:
    """Split markdown into prose, fenced ```trilogy blocks, and :::row containers.

    Non-trilogy fenced blocks (```python, etc.) are preserved verbatim as prose.
    """
    segments: list[Segment] = []
    row: list[Segment] | None = None
    prose: list[str] = []
    fence: str | None = None
    info = ""
    body: list[str] = []

    def flush_prose() -> None:
        if any(line.strip() for line in prose):
            target = segments if row is None else row
            target.append(Prose("\n".join(prose).strip("\n")))
        prose.clear()

    for line in text.splitlines():
        if fence is None:
            stripped = line.strip()
            if row is None and stripped in _ROW_OPEN:
                flush_prose()
                row = []
                continue
            if row is not None and stripped == _ROW_CLOSE:
                flush_prose()
                segments.append(RowBlock(row))
                row = None
                continue
            match = _FENCE_OPEN.match(line)
            if match:
                fence, info, body = match.group(1), match.group(2).strip(), []
            else:
                prose.append(line)
            continue
        if _is_close(line, fence):
            if _is_trilogy(info):
                flush_prose()
                (segments if row is None else row).append(TrilogyBlock("\n".join(body)))
            else:
                prose.extend([f"{fence}{info}", *body, fence])
            fence, info, body = None, "", []
        else:
            body.append(line)

    if fence is not None:  # unterminated fence: keep content as prose
        prose.extend([f"{fence}{info}", *body])
    flush_prose()
    if row is not None:  # unterminated :::row: emit what we collected
        segments.append(RowBlock(row))
    return segments
