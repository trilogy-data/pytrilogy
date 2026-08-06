import io
import sys

import click

from trilogy.scripts.click_utils import IGNORE_UNKNOWN, LazyGroup

UTF8_ALIASES = ("utf8", "utf8sig")


def _force_utf8_stdio() -> None:
    """Force UTF-8 stdio so non-ASCII output (e.g. the ``↳`` description marker
    from ``file list``) survives a narrow codepage -- a cp1252 Windows console,
    or stdout redirected to a file under a cp1252 locale.

    Streams that already speak UTF-8 are left strictly alone. ``reconfigure``
    resets the error handler to ``strict`` whenever ``errors`` isn't passed, and
    a capture stream (pytest's, click's ``CliRunner``) is a UTF-8 wrapper opened
    with ``errors="replace"`` over a buffer that also collects whatever raw bytes
    subprocesses wrote to the captured fd. Flipping it to strict makes the first
    such byte poison every capture read for the rest of the session."""
    for stream in (sys.stdout, sys.stderr):
        if not isinstance(stream, io.TextIOWrapper):
            continue
        if stream.encoding.lower().replace("-", "").replace("_", "") in UTF8_ALIASES:
            continue
        try:
            stream.reconfigure(encoding="utf-8", errors=stream.errors)
        except ValueError:
            pass


_force_utf8_stdio()


def get_version() -> str:
    from trilogy import __version__

    return __version__


def _version_callback(ctx: click.Context, _param: click.Parameter, value: bool) -> None:
    if value:
        click.echo(f"v{get_version()}")
        ctx.exit()


AGENT_NOTICE = """
NOTE FOR AI AGENTS (Claude, Gemini, GPT, etc.): Run `trilogy agent-info` for a
compact documentation directory, then call the drilldown matching your task.
"""

LAZY_SUBCOMMANDS: dict[str, tuple[str, str, dict | None]] = {
    "agent": ("trilogy.scripts.agent", "agent", None),
    "agent-info": ("trilogy.scripts.agent_info", "agent_info", None),
    "cloud": ("trilogy.scripts.cloud", "cloud", None),
    "database": ("trilogy.scripts.database", "database", None),
    "env": ("trilogy.scripts.env_commands", "env", None),
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
    "source": ("trilogy.scripts.source", "source", None),
    "state": ("trilogy.scripts.state", "state", IGNORE_UNKNOWN),
    "state-merge": ("trilogy.scripts.state", "state_merge", None),
    "unit": ("trilogy.scripts.testing", "unit", IGNORE_UNKNOWN),
}

# Alternate spellings of a command, resolved to the canonical entry above.
COMMAND_ALIASES: dict[str, str] = {
    "import": "ingest",
}


@click.group(
    cls=LazyGroup,
    lazy_subcommands=LAZY_SUBCOMMANDS,
    aliases=COMMAND_ALIASES,
    epilog=AGENT_NOTICE,
)
@click.option(
    "--version",
    is_flag=True,
    is_eager=True,
    expose_value=False,
    callback=_version_callback,
    help="Show version and exit.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["rich", "json"]),
    default=None,
    help=(
        "Output format. 'rich' (default) renders human-friendly tables and "
        "panels; 'json' emits a stream of newline-delimited JSON events with "
        "no formatting (parity on information, for agents/pipelines). Overrides "
        "the TRILOGY_OUTPUT_FORMAT env var."
    ),
)
@click.option(
    "--agent",
    "agent_mode",
    is_flag=True,
    default=None,
    help=(
        "Declare that a program, not a person, is reading this output. "
        "Conditions a human would only be warned about — chiefly a run that "
        "executed nothing — become errors, so a no-op cannot pass as a "
        "success. Overrides the TRILOGY_AGENT_MODE env var."
    ),
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
def cli(
    ctx: click.Context,
    output_format: str | None,
    agent_mode: bool | None,
    debug: bool,
    debug_file: str | None,
):
    """Trilogy CLI - A beautiful data productivity tool."""
    # display_core, not the display hub: this runs for EVERY command, and the
    # hub pulls the execution/refresh/validation renderers most never use.
    from trilogy.scripts.display_core import (
        is_agent_mode,
        is_json_mode,
        set_agent_mode,
        set_output_format,
    )

    # The flags override the env-derived defaults; when absent the env values
    # (set transparently by the agent subprocess) stand.
    set_output_format(output_format)
    set_agent_mode(agent_mode)

    ctx.ensure_object(dict)
    ctx.obj["DEBUG"] = debug or bool(debug_file)
    ctx.obj["DEBUG_FILE"] = debug_file
    ctx.obj["OUTPUT_FORMAT"] = "json" if is_json_mode() else "rich"
    ctx.obj["AGENT_MODE"] = is_agent_mode()

    if ctx.obj["DEBUG"]:
        from trilogy.scripts.display import show_debug_mode

        show_debug_mode()


if __name__ == "__main__":
    cli()
