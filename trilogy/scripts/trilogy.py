import sys

import click

from trilogy.scripts.click_utils import IGNORE_UNKNOWN, LazyGroup

# Force UTF-8 stdio so non-ASCII output (e.g. the ``↳`` description marker
# from ``file list``) renders on Windows consoles whose default codepage is
# cp1252. Best-effort: a NotImplementedError or AttributeError just means
# we're on a platform where the streams aren't reconfigurable, and we leave
# them alone.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    except (AttributeError, ValueError, NotImplementedError):
        pass


def get_version() -> str:
    from trilogy import __version__

    return __version__


def _version_callback(ctx: click.Context, _param: click.Parameter, value: bool) -> None:
    if value:
        click.echo(f"v{get_version()}")
        ctx.exit()


AGENT_NOTICE = """
NOTE FOR AI AGENTS (Claude, Gemini, GPT, etc.): For comprehensive usage documentation,
run 'trilogy agent-info' to get a detailed AGENTS.md-style guide with all
commands, options, and usage examples.
"""

LAZY_SUBCOMMANDS: dict[str, tuple[str, str, dict | None]] = {
    "agent": ("trilogy.scripts.agent", "agent", None),
    "agent-info": ("trilogy.scripts.agent_info", "agent_info", None),
    "database": ("trilogy.scripts.database", "database", None),
    "explore": ("trilogy.scripts.explore", "explore", None),
    "file": ("trilogy.scripts.file", "file", None),
    "fmt": ("trilogy.scripts.fmt", "fmt", IGNORE_UNKNOWN),
    "ingest": ("trilogy.scripts.ingest", "ingest", IGNORE_UNKNOWN),
    "init": ("trilogy.scripts.init", "init", None),
    "integration": ("trilogy.scripts.testing", "integration", IGNORE_UNKNOWN),
    "plan": ("trilogy.scripts.plan", "plan", None),
    "public": ("trilogy.scripts.public", "public", None),
    "refresh": ("trilogy.scripts.refresh", "refresh", IGNORE_UNKNOWN),
    "render": ("trilogy.scripts.render", "render", None),
    "run": ("trilogy.scripts.run", "run", IGNORE_UNKNOWN),
    "serve": ("trilogy.scripts.serve", "serve", None),
    "unit": ("trilogy.scripts.testing", "unit", IGNORE_UNKNOWN),
}


@click.group(cls=LazyGroup, lazy_subcommands=LAZY_SUBCOMMANDS, epilog=AGENT_NOTICE)
@click.option(
    "--version",
    is_flag=True,
    is_eager=True,
    expose_value=False,
    callback=_version_callback,
    help="Show version and exit.",
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
    help="Enable debug mode (show tracebacks on errors)",
)
@click.option(
    "--debug-file",
    default=None,
    required=False,
    help="Write SQL debug output to the specified file path",
)
@click.pass_context
def cli(ctx: click.Context, debug: bool, debug_file: str | None):
    """Trilogy CLI - A beautiful data productivity tool."""
    ctx.ensure_object(dict)
    ctx.obj["DEBUG"] = debug or bool(debug_file)
    ctx.obj["DEBUG_FILE"] = debug_file

    if ctx.obj["DEBUG"]:
        from trilogy.scripts.display import show_debug_mode

        show_debug_mode()


if __name__ == "__main__":
    cli()
