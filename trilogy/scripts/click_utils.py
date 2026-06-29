"""Click utilities and helpers for the CLI."""

import importlib

import click

# Fallback group-level flags for callers of ``_hoist_group_flags`` that don't
# pass an explicit map. ``LazyGroup.parse_args`` derives the real map from the
# group's declared options (see ``derive_hoist_map``) so the set can never
# drift out of sync with the actual ``--`` options on the group.
_HOIST_FLAGS: dict[str, bool] = {
    "--debug": False,  # bool flag, no value
    "--debug-file": True,  # consumes the next argv as value
    "--format": True,  # consumes the next argv as value
}


def derive_hoist_map(params: list[click.Parameter]) -> dict[str, bool]:
    """Map ``--long-opt`` -> takes_value for every group option.

    Driving the hoist set off the group's declared params (rather than a
    hand-maintained list) means a newly added group option is hoistable with
    no extra bookkeeping. Eager, value-suppressed options like ``--version``
    are skipped: they're consumed before any subcommand and have no value to
    carry onto the subcommand's positionals.
    """
    hoist: dict[str, bool] = {}
    for p in params:
        if not isinstance(p, click.Option):
            continue
        if p.is_eager and not p.expose_value:
            continue
        takes_value = not p.is_flag and not p.count and p.nargs == 1
        for opt in (*p.opts, *p.secondary_opts):
            if opt.startswith("--"):
                hoist[opt] = takes_value
    return hoist


def _hoist_group_flags(
    argv: list[str],
    subcommands: set[str],
    hoist: dict[str, bool] | None = None,
) -> list[str]:
    """Move group-level flags to before the subcommand if found after it."""
    if hoist is None:
        hoist = _HOIST_FLAGS
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
    saw_separator = False
    while i < len(argv):
        tok = argv[i]
        # Everything after a bare ``--`` is positional payload (e.g. a query
        # body) and must never be hoisted, even if it spells ``--format``.
        if tok == "--":
            saw_separator = True
            tail.append(tok)
            i += 1
            continue
        flag = tok.split("=", 1)[0]
        if not saw_separator and flag in hoist:
            takes_value = hoist[flag]
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
        hoist = derive_hoist_map(self.get_params(ctx))
        args = _hoist_group_flags(list(args), subcommands, hoist)
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


def misplaced_group_value_hint(
    value: str, ctx: click.Context | None, subcommand: str
) -> str | None:
    """If ``value`` is a choice belonging to a group-level option, explain the
    likely misplacement.

    Triggered when a positional (e.g. the ``run`` dialect) ends up holding what
    was meant as a group option's argument — typically ``--format json`` placed
    after the subcommand without the ``--format`` token surviving. Returns a
    one-line hint, or ``None`` if ``value`` matches no group option choice.
    """
    parent = ctx.parent if ctx is not None else None
    group = parent.command if parent is not None else None
    if parent is None or not isinstance(group, click.Command):
        return None
    for p in group.get_params(parent):
        if not isinstance(p, click.Option) or not isinstance(p.type, click.Choice):
            continue
        if value in p.type.choices:
            opt = next((o for o in p.opts if o.startswith("--")), p.name)
            return (
                f"(If you meant the {opt} option, it must come before the "
                f"subcommand or with its flag: trilogy {opt} {value} "
                f"{subcommand} ...)"
            )
    return None
