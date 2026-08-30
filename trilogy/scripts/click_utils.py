"""Click utilities and helpers for the CLI."""

import importlib
from collections.abc import Callable
from copy import copy

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
        aliases: dict[str, str] | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        # Each entry: cmd_name -> (module_path, attr, context_settings)
        self._lazy_subcommands: dict[str, tuple[str, str, dict | None]] = (
            lazy_subcommands or {}
        )
        # alias -> canonical command name; the alias is a full command in its
        # own right (listed, hoisted, invocable), just loaded from the target.
        self._aliases: dict[str, str] = aliases or {}
        self._loaded_commands: dict[str, click.Command] = {}

    def list_commands(self, ctx: click.Context) -> list[str]:
        base = super().list_commands(ctx)
        lazy = sorted(set(self._lazy_subcommands) | set(self._aliases))
        return base + lazy

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        subcommands = (
            set(self._lazy_subcommands) | set(self.commands) | set(self._aliases)
        )
        hoist = derive_hoist_map(self.get_params(ctx))
        args = _hoist_group_flags(list(args), subcommands, hoist)
        return super().parse_args(ctx, args)

    def _load_lazy(self, name: str) -> click.Command:
        if name not in self._loaded_commands:
            module_path, attr, context_settings = self._lazy_subcommands[name]
            module = importlib.import_module(module_path)
            func = getattr(module, attr)
            if isinstance(func, click.Command):
                func.name = name
                self._loaded_commands[name] = func
            else:
                self._loaded_commands[name] = click.command(
                    name, context_settings=context_settings
                )(func)
        return self._loaded_commands[name]

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        target = self._aliases.get(cmd_name, cmd_name)
        if target not in self._lazy_subcommands:
            return super().get_command(ctx, cmd_name)
        if cmd_name not in self._loaded_commands:
            # An alias copies the built command rather than re-wrapping the
            # function: click.command() consumes the function's pending params,
            # so a second wrap would produce a command with no options at all.
            # Renaming in place is equally wrong — both names would resolve to
            # the one module-level object.
            alias = copy(self._load_lazy(target))
            alias.name = cmd_name
            alias.short_help = f"Alias for `{target}`."
            self._loaded_commands[cmd_name] = alias
        return self._loaded_commands[cmd_name]


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


def report_options(fn: Callable) -> Callable:
    """``--report-file`` / ``--run-id``: the machine-readable execution report
    contract, shared by every command that can emit one."""
    fn = click.option(
        "--run-id",
        "run_id",
        default=None,
        help=(
            "Correlation id stamped on every report record "
            "(env: TRILOGY_RUN_ID; default: generated)."
        ),
    )(fn)
    return click.option(
        "--report-file",
        "report_file",
        type=click.Path(),
        default=None,
        help=(
            "Append a machine-readable JSONL execution report to this path "
            "(env: TRILOGY_REPORT_FILE). One JSON object per line; see "
            "trilogy.execution.report for the record contract."
        ),
    )(fn)


def state_file_option(fn: Callable) -> Callable:
    """``--state-input`` / ``--state-file`` / ``--state-partition``: the
    persisted-state round trip for run/refresh — read recorded asset state in,
    write the post-execution state back out, optionally scoped to the partitions
    this invocation owned."""
    fn = click.option(
        "--state-max-partitions",
        "state_max_partitions",
        default=None,
        help=(
            "Slices per datasource a written snapshot may carry (env: "
            "TRILOGY_STATE_MAX_PARTITIONS). Unset carries every slice; `0` "
            "carries none and reports only the summary counts. Set it only if "
            "whatever reads the file has a payload budget — partition_summary "
            "stays exact either way, and stale slices are kept first, so a "
            "trimmed list is still the backfill queue."
        ),
    )(fn)
    fn = click.option(
        "--state-partition",
        "state_partition",
        multiple=True,
        default=(),
        help=(
            "Partition id this run owns, e.g. `order_date=2024-01-03` (repeatable; "
            "env: TRILOGY_STATE_PARTITION, comma-separated). Scopes --state-file "
            "to those slices and marks it a partial delta, so concurrent "
            "per-partition workers produce snapshots that `trilogy state-merge` "
            "can fold together without clobbering each other."
        ),
    )(fn)
    fn = click.option(
        "--state-input",
        "state_input",
        type=click.Path(exists=True),
        default=None,
        help=(
            "Seed asset state from a snapshot written by an earlier "
            "--state-file / `trilogy state` (env: TRILOGY_STATE_INPUT). "
            "Managed assets recorded there are trusted instead of re-probed; "
            "roots are always re-read live. Matched by physical address."
        ),
    )(fn)
    return click.option(
        "--state-file",
        "state_file",
        type=click.Path(),
        default=None,
        help=(
            "Write a post-execution state snapshot (watermarks, staleness, "
            "column mappings) as JSON to this path (env: TRILOGY_STATE_FILE). "
            "Runs a full state probe after execution; failures warn but never "
            "change the exit code."
        ),
    )(fn)


def dry_run_option(help: str) -> Callable[[Callable], Callable]:
    """``--dry-run`` / ``-n``: describe the writes instead of performing them.

    One decorator so every command that offers a dry run spells it the same
    way -- same short alias, same ``dry_run`` destination. Only the sentence
    naming *what* would be written varies, because that is the only part that
    legitimately differs between compiling SQL, writing model files and
    pushing a bundle."""

    def decorate(fn: Callable) -> Callable:
        return click.option(
            "--dry-run",
            "-n",
            "dry_run",
            is_flag=True,
            default=False,
            help=help,
        )(fn)

    return decorate
