from click import group, option, pass_context

from trilogy.scripts.agent import agent
from trilogy.scripts.display import set_rich_mode, show_debug_mode
from trilogy.scripts.fmt import fmt
from trilogy.scripts.ingest import ingest
from trilogy.scripts.init import init
from trilogy.scripts.run import run
from trilogy.scripts.serve import serve
from trilogy.scripts.testing import integration, unit

set_rich_mode = set_rich_mode

# Default parallelism level
DEFAULT_PARALLELISM = 2


@group()
@option("--debug", default=False, help="Enable debug mode")
@pass_context
def cli(ctx, debug: bool):
    """Trilogy CLI - A beautiful data productivity tool."""
    ctx.ensure_object(dict)
    ctx.obj["DEBUG"] = debug

    if debug:
        show_debug_mode()


# Add commands to the group
cli.command("init")(init)
cli.command("ingest", context_settings=dict(ignore_unknown_options=True))(ingest)
cli.command("fmt", context_settings=dict(ignore_unknown_options=True))(fmt)
cli.command(
    "unit",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)(unit)
cli.command(
    "integration",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)(integration)
cli.command(
    "run",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)(run)
cli.command("agent")(agent)
cli.command("serve")(serve)


if __name__ == "__main__":
    cli()
