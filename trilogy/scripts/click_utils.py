"""Click utilities and helpers for the CLI."""

import importlib

import click

# Group-level flags that Click can only consume BEFORE the subcommand. We
# hoist them in ``LazyGroup.parse_args`` so ``trilogy run --debug query.preql``
# and ``trilogy --debug run query.preql`` are both accepted (the post-
# subcommand form reads more naturally).
_HOIST_FLAGS: dict[str, bool] = {
    "--debug": False,  # bool flag, no value
    "--debug-file": True,  # consumes the next argv as value
}


def _hoist_group_flags(argv: list[str], subcommands: set[str]) -> list[str]:
    """Move group-level flags to before the subcommand if found after it."""
    if not argv:
        return argv
    sub_idx = next(
        (
            i
            for i, tok in enumerate(argv)
            if not tok.startswith("-") and tok in subcommands
        ),
        None,
    )
    if sub_idx is None:
        return argv
    head = list(argv[: sub_idx + 1])
    tail: list[str] = []
    i = sub_idx + 1
    while i < len(argv):
        tok = argv[i]
        flag = tok.split("=", 1)[0]
        if flag in _HOIST_FLAGS:
            takes_value = _HOIST_FLAGS[flag]
            if "=" in tok or not takes_value:
                head.insert(sub_idx, tok)
                sub_idx += 1
                i += 1
            else:
                head.insert(sub_idx, tok)
                sub_idx += 1
                if i + 1 < len(argv):
                    head.insert(sub_idx, argv[i + 1])
                    sub_idx += 1
                    i += 2
                else:
                    i += 1
        else:
            tail.append(tok)
            i += 1
    return head + tail


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

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        subcommands = set(self._lazy_subcommands.keys()) | set(self.commands.keys())
        args = _hoist_group_flags(list(args), subcommands)
        return super().parse_args(ctx, args)

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        if cmd_name in self._lazy_subcommands:
            if cmd_name not in self._loaded_commands:
                module_path, attr, context_settings = self._lazy_subcommands[cmd_name]
                module = importlib.import_module(module_path)
                func = getattr(module, attr)
                if isinstance(func, click.Command):
                    func.name = cmd_name
                    self._loaded_commands[cmd_name] = func
                else:
                    cmd = click.command(cmd_name, context_settings=context_settings)(
                        func
                    )
                    self._loaded_commands[cmd_name] = cmd
            return self._loaded_commands[cmd_name]
        return super().get_command(ctx, cmd_name)


IGNORE_UNKNOWN = {"ignore_unknown_options": True}


def validate_dialect(dialect: str | None, subcommand: str) -> None:
    """Raise UsageError if dialect looks like a misplaced flag or path.

    ``--debug`` and ``--debug-file`` are hoisted to the group level by
    ``trilogy._hoist_group_flags`` at argv-preprocess time, so they're
    accepted on either side of the subcommand. Anything else that starts
    with ``-`` in the dialect slot is genuinely misplaced.
    """
    if not dialect:
        return
    if dialect.startswith("-"):
        raise click.UsageError(
            f"'{dialect}' is not a valid dialect. "
            "The dialect argument comes after the input file and any options.\n"
            f"  Try: trilogy {subcommand} <input> [<dialect>]"
        )
    if (
        dialect.endswith(".preql")
        or "/" in dialect
        or "\\" in dialect
        or dialect == "."
    ):
        raise click.UsageError(
            f"'{dialect}' looks like a file path, not a dialect. "
            "The dialect argument comes AFTER the input file.\n"
            f"  Try: trilogy {subcommand} {dialect} <dialect>"
        )
