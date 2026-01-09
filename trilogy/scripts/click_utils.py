"""Click utilities and helpers for the CLI."""

import importlib

import click


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
                cmd = click.command(cmd_name, context_settings=context_settings)(func)
                self._loaded_commands[cmd_name] = cmd
            return self._loaded_commands[cmd_name]
        return super().get_command(ctx, cmd_name)


IGNORE_UNKNOWN = {"ignore_unknown_options": True}
