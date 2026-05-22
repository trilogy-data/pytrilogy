"""Render command for Trilogy CLI."""

from pathlib import Path as PathlibPath

import click
from click import Choice, Path, argument, option
from click.exceptions import Exit

from trilogy.rendering.theme import DEFAULT_THEME, THEMES
from trilogy.report import render_report
from trilogy.report.backends import available_formats


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
def render(input: str, output_format: str, theme: str, output: str | None) -> None:
    """Render a Trilogy markdown report to an image or HTML file.

    Executes embedded ```trilogy code blocks: chart statements become charts
    and select statements become tables.
    """
    from trilogy.scripts.display import print_error

    try:
        result = render_report(
            input,
            output_format=output_format,
            output_path=PathlibPath(output) if output else None,
            theme=theme,
        )
    except Exception as e:  # surface a clean CLI error
        print_error(str(e))
        raise Exit(1)
    click.secho(f"Rendered report -> {result}", fg="green")
