import click

from trilogy.scripts.click_utils import IGNORE_UNKNOWN, LazyGroup


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
    "fmt": ("trilogy.scripts.fmt", "fmt", IGNORE_UNKNOWN),
    "ingest": ("trilogy.scripts.ingest", "ingest", IGNORE_UNKNOWN),
    "init": ("trilogy.scripts.init", "init", None),
    "integration": ("trilogy.scripts.testing", "integration", IGNORE_UNKNOWN),
    "plan": ("trilogy.scripts.plan", "plan", None),
    "refresh": ("trilogy.scripts.refresh", "refresh", IGNORE_UNKNOWN),
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
    default=None,
    required=False,
    help="Enable debug mode with output to specified file path",
)
@click.pass_context
def cli(ctx: click.Context, debug: str | None):
    """Trilogy CLI - A beautiful data productivity tool."""
    ctx.ensure_object(dict)
    ctx.obj["DEBUG"] = debug

    if debug:
        from trilogy.scripts.display import show_debug_mode

        show_debug_mode()


if __name__ == "__main__":
    cli()
