"""Markdown report rendering: prose mixed with embedded Trilogy blocks.

A report is a standard markdown file. Fenced ```trilogy blocks are executed;
select statements become tables and chart statements become charts. The
`render_report` entrypoint produces a polished artifact (HTML, PNG, ...).
"""

from pathlib import Path
from typing import Any, Union

from trilogy.rendering.theme import DEFAULT_THEME, Theme, get_theme
from trilogy.report.backends import get_backend
from trilogy.report.document import parse_markdown
from trilogy.report.runner import run_document


def render_report(
    source: Union[str, Path],
    output_format: str = "png",
    output_path: Union[str, Path, None] = None,
    theme: Union[str, Theme] = DEFAULT_THEME,
    executor: Any | None = None,
) -> Path:
    """Render a markdown report file to the requested format, returning the output path.

    ``executor`` overrides the default in-memory DuckDB engine so a report can
    run against a configured warehouse (see the CLI's trilogy.toml handling)."""
    source = Path(source)
    resolved_theme = get_theme(theme) if isinstance(theme, str) else theme
    backend = get_backend(output_format)
    target = (
        Path(output_path)
        if output_path is not None
        else source.with_suffix("." + backend.extension)
    )
    segments = parse_markdown(source.read_text(encoding="utf-8"))
    elements = run_document(segments, working_path=source.parent, executor=executor)
    backend.render(elements, target, resolved_theme)
    return target


__all__ = ["render_report"]
