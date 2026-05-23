"""Priority-based pretty-printing helpers.

A `Doc` is a sequence of string fragments and `Break` candidates. Each break
carries a priority and an indent delta. When the rendered line would exceed
``width``, the highest-priority breaks are activated first; all siblings at
that priority break together (cross-line consistency). Lower-priority breaks
are activated only if the line still doesn't fit.

This mirrors common pretty-printing approaches (Wadler, Prettier) but stays
small and string-friendly so existing string-returning renderers can opt in
without a full rewrite.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Union


@dataclass(frozen=True)
class Break:
    priority: int
    indent: int = 0  # extra indent levels on the post-break line
    flat: str = " "  # what to emit when this break is NOT activated


DocPart = Union[str, Break]


def _flat(parts: Iterable[DocPart]) -> str:
    return "".join(p if isinstance(p, str) else p.flat for p in parts)


def _max_priority(parts: Iterable[DocPart]) -> int | None:
    pris = [p.priority for p in parts if isinstance(p, Break)]
    return max(pris) if pris else None


def render(
    parts: list[DocPart],
    *,
    width: int,
    col: int,
    indent_unit: str = "    ",
) -> str:
    """Render ``parts`` into a string, breaking to fit ``width``.

    ``col`` is the column the first character lands at (callers should
    typically pass the current indent length). Subsequent lines after an
    activated break are prefixed with ``col + indent * indent_unit``.
    """
    flat = _flat(parts)
    # Pre-rendered newlines (e.g. nested constructs already wrapped) force
    # us to break out — the flat form is no longer single-line.
    if "\n" not in flat and col + len(flat) <= width:
        return flat

    pri = _max_priority(parts)
    if pri is None:
        return flat

    # Split at every break with priority == pri.
    chunks: list[list[DocPart]] = [[]]
    chunk_indents: list[int] = [0]
    for p in parts:
        if isinstance(p, Break) and p.priority == pri:
            chunks.append([])
            chunk_indents.append(p.indent)
        else:
            chunks[-1].append(p)

    pieces: list[str] = []
    for i, chunk in enumerate(chunks):
        chunk_col = col + chunk_indents[i] * len(indent_unit)
        rendered = render(chunk, width=width, col=chunk_col, indent_unit=indent_unit)
        if i == 0:
            pieces.append(rendered)
        else:
            pieces.append("\n" + " " * chunk_col + rendered)
    return "".join(pieces)
