"""Execute the trilogy blocks of a parsed report document."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from trilogy.core.enums import ChartType
from trilogy.core.statements.execute import ProcessedQuery
from trilogy.dialect.enums import Dialects
from trilogy.dialect.results import ChartResult
from trilogy.engine import ResultProtocol
from trilogy.report.document import (
    Chart,
    ErrorBox,
    RenderedElement,
    RenderedRow,
    RowBlock,
    Segment,
    Table,
    TrilogyBlock,
)


def _chart_type(result: ChartResult) -> ChartType | None:
    layers = result.statement.layers
    return layers[0].layer_type if layers else None


def _column_types(processed: Any, columns: list[str]) -> list[Any]:
    """Datatypes for each output column, taken from the processed query."""
    if not isinstance(processed, ProcessedQuery):
        return [None] * len(columns)
    output = processed.output_columns
    if len(output) == len(columns):
        return [col.datatype for col in output]
    by_name: dict[str, Any] = {}
    for col in output:
        by_name.setdefault(col.safe_address, col.datatype)
        by_name.setdefault(col.address, col.datatype)
    return [by_name.get(name) for name in columns]


def _to_element(
    processed: Any, result: ResultProtocol | None
) -> RenderedElement | None:
    if result is None:
        return None
    if isinstance(result, ChartResult):
        if result.chart is None:
            return ErrorBox(
                "Chart rendering requires altair; install pytrilogy[report]."
            )
        return Chart(result.chart, chart_type=_chart_type(result))
    columns = [str(key) for key in result.keys()]
    rows = [list(row) for row in result.fetchall()]
    return Table(
        columns=columns,
        rows=rows,
        column_types=_column_types(processed, columns),
    )


def run_block(executor: Any, block: TrilogyBlock) -> list[RenderedElement]:
    """Execute one trilogy block; surface any failure as an inline ErrorBox."""
    elements: list[RenderedElement] = []
    try:
        for processed in executor.parse_text_generator(block.code):
            element = _to_element(processed, executor.execute_statement(processed))
            if element is not None:
                elements.append(element)
    except Exception as exc:  # report errors inline rather than aborting the render
        elements.append(ErrorBox(f"{type(exc).__name__}: {exc}"))
    return elements


def _run_segment(executor: Any, segment: Segment) -> list[RenderedElement]:
    if isinstance(segment, TrilogyBlock):
        return run_block(executor, segment)
    if isinstance(segment, RowBlock):
        children: list[RenderedElement] = []
        for child in segment.segments:
            children.extend(_run_segment(executor, child))
        return [RenderedRow(children)]
    rendered: list[RenderedElement] = [segment]  # Prose passes through
    return rendered


def run_document(
    segments: list[Segment],
    working_path: Path,
    executor: Any | None = None,
    chart_theme: str | None = None,
) -> list[RenderedElement]:
    """Run every trilogy block against one shared executor so declarations persist.

    Defaults to an in-memory DuckDB executor; callers may pass a pre-built
    executor (e.g. one wired to the dialect in a trilogy.toml) to run the report
    against a configured warehouse instead. ``chart_theme`` pins the executor's
    chart theme to the report's resolved theme so theme-baked marks (headline
    text) match the surrounding page."""
    if executor is None:
        executor = Dialects.DUCK_DB.default_executor(working_path=working_path)
    if chart_theme is not None:
        executor.chart_theme = chart_theme
    if not executor.connected:
        executor.connect()
    elements: list[RenderedElement] = []
    for segment in segments:
        elements.extend(_run_segment(executor, segment))
    return elements
