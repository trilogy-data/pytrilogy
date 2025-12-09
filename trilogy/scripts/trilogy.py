from click import group, option, pass_context

from trilogy.scripts.display import set_rich_mode, show_debug_mode
from trilogy.scripts.fmt import fmt
from trilogy.scripts.run import run
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
cli.command("fmt", context_settings=dict(ignore_unknown_options=True))(fmt)
cli.command(
    "integration",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)(integration)
cli.command(
    "unit",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)(unit)
cli.command(
    "run",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)(run)


if __name__ == "__main__":
    cli()
