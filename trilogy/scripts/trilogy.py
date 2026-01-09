import importlib

import click


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


class LazyGroup(click.Group):
    """A click Group that lazily loads subcommands."""

    def __init__(
        self,
        *args,
        lazy_subcommands: dict[str, tuple[str, str, dict | None]] | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        # Each entry: cmd_name -> (module_path, attr, context_settings)
        self._lazy_subcommands: dict[str, tuple[str, str, dict | None]] = (
            lazy_subcommands or {}
        )
        self._loaded_commands: dict[str, click.Command] = {}

    def list_commands(self, ctx: click.Context) -> list[str]:
        base = super().list_commands(ctx)
        lazy = sorted(self._lazy_subcommands.keys())
        return base + lazy

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        if cmd_name in self._lazy_subcommands:
            if cmd_name not in self._loaded_commands:
                module_path, attr, context_settings = self._lazy_subcommands[cmd_name]
                module = importlib.import_module(module_path)
                func = getattr(module, attr)
                # Wrap with click.command() to create a proper Command object
                cmd = click.command(cmd_name, context_settings=context_settings)(func)
                self._loaded_commands[cmd_name] = cmd
            return self._loaded_commands[cmd_name]
        return super().get_command(ctx, cmd_name)


IGNORE_UNKNOWN = {"ignore_unknown_options": True}

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
