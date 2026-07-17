"""Render command for Trilogy CLI."""

from pathlib import Path as PathlibPath

import click
from click import Choice, Path, argument, option, pass_context
from click.exceptions import Exit

from trilogy.executor import Executor
from trilogy.rendering.theme import DEFAULT_THEME, THEMES
from trilogy.report import render_report
from trilogy.report.backends import available_formats


def _report_executor(
    ctx: click.Context, report_dir: PathlibPath, config_path: str | None
) -> Executor | None:
    """Build an executor from a trilogy.toml (explicit --config or one discovered
    next to the report). Returns None when no engine is configured, letting the
    report fall back to the default in-memory DuckDB."""
    from trilogy.scripts.common import create_executor, get_runtime_config

    override = PathlibPath(config_path) if config_path else None
    config = get_runtime_config(report_dir, override)
    if not config.engine_dialect:
        return None
    return create_executor(
        param=(),
        directory=report_dir,
        conn_args=(),
        edialect=config.engine_dialect,
        debug=ctx.obj["DEBUG"] if ctx.obj else False,
        config=config,
        debug_file=ctx.obj.get("DEBUG_FILE") if ctx.obj else None,
    )


@argument("input", type=Path(exists=True, dir_okay=False))
@option(
    "--to",
    "output_format",
    type=Choice(available_formats()),
    default="png",
    help="Output format for the rendered report.",
)
@option(
    "--theme",
    type=Choice(sorted(THEMES)),
    default=DEFAULT_THEME.name,
    help="Visual theme (font + colors).",
)
@option(
    "--out",
    "-o",
    "output",
    type=Path(),
    default=None,
    help="Output file path. Defaults to the input path with the format's extension.",
)
@option(
    "--config",
    "config_path",
    type=Path(exists=True),
    default=None,
    help="Path to trilogy.toml. Defaults to one discovered next to the report; "
    "its [engine] dialect targets the report at a configured warehouse instead "
    "of the built-in in-memory DuckDB.",
)
@pass_context
def render(
    ctx: click.Context,
    input: str,
    output_format: str,
    theme: str,
    output: str | None,
    config_path: str | None,
) -> None:
    """Render a Trilogy markdown report to an image or HTML file.

    Executes embedded ```trilogy code blocks: chart statements become charts
    and select statements become tables.
    """
    from trilogy.scripts.display import emit_event, is_json_mode, print_error

    executor = _report_executor(ctx, PathlibPath(input).parent, config_path)
    try:
        result = render_report(
            input,
            output_format=output_format,
            output_path=PathlibPath(output) if output else None,
            theme=theme,
            executor=executor,
        )
    except Exception as e:  # surface a clean CLI error
        print_error(str(e))
        raise Exit(1)
    if is_json_mode():
        emit_event("rendered", input=input, output=str(result), format=output_format)
    else:
        click.secho(f"Rendered report -> {result}", fg="green")
